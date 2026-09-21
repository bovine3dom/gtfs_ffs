export RequestScheduler, scheduler_stats

struct RouterBusy <: Exception end

mutable struct RequestScheduler
    lock::ReentrantLock
    changed::Threads.Condition
    total::Int
    capacity::Tuple{Int,Int}
    pending_limit::Tuple{Int,Int}
    memory_limit::Tuple{Int,Int}
    output_limit::Tuple{Int,Int}
    queues::Tuple{Vector{Any},Vector{Any}}
    cpu::Vector{Int}
    memory::Vector{Int}
    output::Vector{Int}
    turn::Int
end

function RequestScheduler(; workers::Integer=Threads.nthreads(:default),
        max_pending::Integer=128, memory_bytes::Integer=8*1024^3,
        output_bytes::Integer=min(memory_bytes, 1024^3))
    1 <= workers <= Threads.nthreads(:default) || throw(ArgumentError("workers must fit the default thread pool"))
    0 <= max_pending < typemax(Int) || throw(ArgumentError("max_pending must be nonnegative and below typemax(Int)"))
    0 < memory_bytes <= typemax(Int) && 0 < output_bytes <= typemax(Int) ||
        throw(ArgumentError("memory budgets must be positive Int values"))
    short = cld(workers, 4)
    split(bytes) = workers == 1 ? (Int(bytes), Int(bytes)) :
        (Int(fld(Int128(bytes) * short, workers)), Int(bytes - fld(Int128(bytes) * short, workers)))
    reserve = min(max_pending, max(1, cld(max_pending, 8)))
    guard = ReentrantLock()
    RequestScheduler(guard, Threads.Condition(guard), Int(workers),
        (Int(short), Int(max(1, workers - short))), (Int(reserve), Int(max_pending - reserve)),
        split(memory_bytes), split(output_bytes), (Any[], Any[]), zeros(Int, 2), zeros(Int, 2), zeros(Int, 2), 1)
end

function scheduler_stats(s::RequestScheduler)
    lock(s.lock) do
        (; workers=Tuple(s.cpu), memory_bytes=Tuple(s.memory), output_bytes=Tuple(s.output),
            pending=length.(s.queues), capacity=s.capacity, pending_limit=s.pending_limit,
            memory_limit=s.memory_limit, output_limit=s.output_limit)
    end
end

mutable struct ComputeLease
    scheduler::RequestScheduler
    lane::Int
    workers::Int
    bytes::Int
    wait_ns::UInt64
end

function _acquire_compute!(lease::ComputeLease, lane, bytes_per_worker; max_workers::Integer=typemax(Int))
    max_workers > 0 || throw(ArgumentError("max_workers must be positive"))
    s = lease.scheduler
    workers = min(max_workers, lane == 1 ? 1 : cld(s.capacity[2], 2))
    bytes_per_worker <= s.memory_limit[lane] ||
        throw(PopulationMemoryError(UInt128(bytes_per_worker), s.memory_limit[lane]))
    workers = min(workers, div(s.memory_limit[lane], max(1, bytes_per_worker)))
    bytes = workers * bytes_per_worker
    extra = bytes - lease.bytes
    started = time_ns()
    lock(s.lock) do
        queue = s.queues[lane]
        entry = (; lease, workers, bytes, started)
        fits(e) = s.cpu[lane] + e.workers <= s.capacity[lane] && sum(s.cpu) + e.workers <= s.total &&
            s.memory[lane] + e.bytes - e.lease.bytes <= s.memory_limit[lane] &&
            (s.total != 1 || (sum(s.memory) + e.bytes - e.lease.bytes <= s.memory_limit[lane] &&
                (e.lease.bytes > 0 || s.turn == lane || isempty(s.queues[3 - lane]))))
        # A request that already owns memory must be able to resume and release it.
        eligible(e) = fits(e) && (e === first(queue) || e.lease.bytes > 0 ||
            time_ns() - first(queue).started < 1_000_000_000)
        if !isempty(queue) || !fits(entry)
            length(queue) < s.pending_limit[lane] || throw(RouterBusy())
            push!(queue, entry)
            try
                while findfirst(eligible, queue) != findfirst(e -> e.lease === lease, queue)
                    wait(s.changed)
                end
            finally
                deleteat!(queue, findfirst(e -> e.lease === lease, queue))
                notify(s.changed; all=true)
            end
        end
        s.cpu[lane] += workers
        s.memory[lane] += extra
        lease.lane, lease.workers, lease.bytes = lane, workers, bytes
        lease.wait_ns += time_ns() - started
    end
    return lease
end

function _release_compute!(lease::ComputeLease; memory=true)
    iszero(lease.workers) && iszero(lease.bytes) && return
    s = lease.scheduler
    lock(s.lock) do
        s.cpu[lease.lane] -= lease.workers
        memory && (s.memory[lease.lane] -= lease.bytes)
        s.turn = 3 - lease.lane
        lease.workers = 0
        memory && (lease.bytes = 0)
        notify(s.changed; all=true)
    end
end

function _workspace_wait(lease::ComputeLease)
    lane, bytes = lease.lane, div(lease.bytes, lease.workers)
    workers = lease.workers
    started = time_ns()
    # Keep scratch memory charged while the pool waits, but return CPU slots.
    _release_compute!(lease; memory=false)
    return function ()
        lease.wait_ns += time_ns() - started
        _acquire_compute!(lease, lane, bytes; max_workers=workers)
    end
end

struct OutputLease
    scheduler::RequestScheduler
    lane::Int
    bytes::Int
end

function _release(lease::OutputLease)
    lock(lease.scheduler.lock) do
        lease.scheduler.output[lease.lane] -= lease.bytes
    end
end

# Reserve before Arrow allocates. Do not wait for slow clients with CPU slots held.
function _with_output(f, request, lease::ComputeLease, rows, encoding; columns=8)
    s, lane = lease.scheduler, lease.lane
    estimate = UInt128(rows) * (encoding == "string" ? 96 + 16columns : 32 + 16columns) + 65536
    estimate <= s.output_limit[lane] || throw(PopulationMemoryError(estimate, s.output_limit[lane]))
    bytes = Int(estimate)
    lock(s.lock) do
        s.output[lane] + bytes <= s.output_limit[lane] &&
            (s.total != 1 || sum(s.output) + bytes <= s.output_limit[lane]) || throw(RouterBusy())
        s.output[lane] += bytes
    end
    token = OutputLease(s, lane, bytes)
    retained = false
    try
        response = f()
        actual = length(response.body) + 4 # WebSocket request ID prefix.
        actual <= bytes || throw(PopulationMemoryError(UInt128(actual), bytes))
        scope = get(request.context, :router_admission, nothing)
        if scope isa _ResponseAdmission
            lock(s.lock) do
                s.output[lane] -= bytes - actual
            end
            token = OutputLease(s, lane, actual)
            push!(scope.leases, token)
            retained = true
        end
        return response
    finally
        retained || _release(token)
    end
end

function _with_scheduled(f, request, s::RequestScheduler, lane, bytes; max_workers::Integer=typemax(Int))
    lease = ComputeLease(s, lane, 0, 0, UInt64(0))
    try
        _acquire_compute!(lease, lane, bytes; max_workers)
        response = f(lease)
        HTTP.setheader(response, "X-Router-Queue-Wait-Ms" => string(lease.wait_ns / 1e6))
        return response
    catch error
        error isa RouterBusy && return _busy_response()
        error isa PopulationMemoryError || rethrow()
        return HTTP.Response(422, [_response_headers(); "Content-Type" => "text/plain"],
            "request exceeds configured routing memory budget")
    finally
        _release_compute!(lease)
    end
end

_short_query(window, budget, walk) = window == 0 && budget <= 10_800_000 && walk <= 3_600_000
