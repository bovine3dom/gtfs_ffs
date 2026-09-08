@inline function _window_position(i::Int32, batch::Int32, shift::Int32)
    offset = i - Int32(1)
    return shift >= 0 ? ((offset & (batch - Int32(1))) + Int32(1), (offset >> shift) + Int32(1)) :
                        (offset % batch + Int32(1), offset ÷ batch + Int32(1))
end

@kernel function window_init!(previous, active, starts, source::Int32,
                             batch::Int32, count::Int32, shift::Int32)
    i = @index(Global, Linear)
    q, v = _window_position(Int32(i), batch, shift)
    live = q <= count && v == source
    @inbounds previous[i] = live ? starts[q] : INF
    @inbounds active[i] = UInt32(live)
end

@kernel function window_prepare!(previous, next, next_active, changed)
    i = @index(Global, Linear)
    if i <= length(previous)
        @inbounds next[i] = previous[i]
        @inbounds next_active[i] = UInt32(0)
    end
    if i <= length(changed)
        @inbounds changed[i] = UInt32(0)
    end
end

@kernel function window_transit!(previous, next, active, next_active, changed,
                                edge_from, edge_to, schedule_ptr, departure, arrival,
                                cutoffs, connections, record::Bool, batch::Int32, shift::Int32)
    i = @index(Global, Linear)
    q, edge = _window_position(Int32(i), batch, shift)
    workgroup = @index(Group, Linear)
    @inbounds begin
        from, to = edge_from[edge], edge_to[edge]
        tail = q + batch * (from - Int32(1))
        if from != to && active[tail] != 0
            ready = previous[tail]
            connection = next_connection(schedule_ptr, departure, arrival, edge,
                                         ready, cutoffs[q])
            if record
                connections[i] = connection
            end
            if connection != 0
                candidate = (ready ÷ PERIOD) * PERIOD + arrival[connection]
                target = q + batch * (to - Int32(1))
                old, _ = Atomix.@atomic next[target] min candidate
                if candidate < old
                    Atomix.@atomic next_active[target] |= UInt32(1)
                    Atomix.@atomic changed[workgroup] |= UInt32(1)
                end
            end
        end
    end
end

"""Batched window workspace sharing its parent's uploaded graph; serialize calls."""
struct WindowKernelRouter{R,U,I}
    parent::R
    batch_size::Int
    check_every::Int
    previous::U
    next::U
    active::U
    next_active::U
    changed::U
    starts::U
    cutoffs::U
    connections::I
    host_starts::Vector{UInt32}
    host_cutoffs::Vector{UInt32}
    host_changed::Vector{UInt32}
    host_labels::Vector{UInt32}
    host_connections::Vector{Int32}
end

function WindowKernelRouter(parent::KernelRouter; batch_size::Integer=64,
                            check_every::Integer=4)
    1 <= batch_size <= 256 || throw(ArgumentError("batch_size must be between 1 and 256"))
    1 <= check_every <= 32 || throw(ArgumentError("check_every must be between 1 and 32"))
    graph, backend = parent.graph, parent.backend
    b = Int(batch_size)
    v, e = length(graph.h3), length(graph.edge_to)
    max(v, e) <= typemax(Int32) ÷ b ||
        throw(ArgumentError("batched graph indices exceed Int32"))
    n = b * v
    c = isnothing(graph.distance_km) ? 1 : b * e
    changes = max(1, cld(b * e, 256))
    return WindowKernelRouter(parent, b, Int(check_every),
        KA.allocate(backend, UInt32, n), KA.allocate(backend, UInt32, n),
        KA.allocate(backend, UInt32, n), KA.allocate(backend, UInt32, n),
        KA.allocate(backend, UInt32, changes), KA.allocate(backend, UInt32, b),
        KA.allocate(backend, UInt32, b), KA.allocate(backend, Int32, c),
        zeros(UInt32, b), zeros(UInt32, b), zeros(UInt32, changes),
        Vector{UInt32}(undef, n), Vector{Int32}(undef, c))
end

WindowKernelRouter(graph::Graph, backend; kwargs...) =
    WindowKernelRouter(KernelRouter(graph, backend); kwargs...)

function route_window_kernel!(router::WindowKernelRouter, origin::UInt64,
                              departure_ms::Integer, budget_ms::Integer,
                              window_ms::Integer; step_ms::Integer=60_000,
                              window_mode=:mean_intersection, distance_mode="itinerary")
    track = _distance_mode(distance_mode) == :itinerary
    parent = router.parent
    graph, backend = parent.graph, parent.backend
    started = time_ns()
    plan = _window_plan(graph, origin, departure_ms, budget_ms, window_ms; step_ms)
    planning_s = (time_ns() - started) / 1e9
    acc = _window_accumulator(graph, plan, track; window_mode)
    groups = length(plan.groups)
    batches = rounds = 0
    device_s = download_s = host_replay_s = aggregation_s = 0.0
    b, v, e = router.batch_size, length(graph.h3), length(graph.edge_to)
    shift = ispow2(b) ? Int32(trailing_zeros(b)) : Int32(-1)
    record = track && !isnothing(graph.distance_km)
    labels = fill(INF, v)
    distances = record ? Vector{Float64}(undef, v) : nothing
    seen = record ? Vector{UInt32}(undef, v) : UInt32[]
    connections = record ? reshape(router.host_connections, b, e) : nothing

    if e == 0
        for group in plan.groups
            first, _ = group
            labels[plan.source] = UInt32(plan.ready + first * plan.step)
            if record
                fill!(distances, NaN)
                distances[plan.source] = 0.0
            end
            _accumulate_window!(acc, plan, group, labels, distances)
        end
    else
        init = window_init!(backend, 256)
        prepare = window_prepare!(backend, 256)
        transit = window_transit!(backend, 256)
        GC.@preserve router begin
            for base in 1:b:groups
                started = time_ns()
                count = min(b, groups - base + 1)
                fill!(router.host_starts, 0)
                fill!(router.host_cutoffs, 0)
                for q in 1:count
                    first, n = plan.groups[base + q - 1]
                    router.host_starts[q] = UInt32(plan.ready + first * plan.step)
                    router.host_cutoffs[q] = UInt32(plan.ready + (first + n - 1) * plan.step + plan.budget)
                end
                KA.copyto!(backend, router.starts, router.host_starts)
                KA.copyto!(backend, router.cutoffs, router.host_cutoffs)
                previous, next = router.previous, router.next
                active, next_active = router.active, router.next_active
                init(previous, active, router.starts, plan.source, Int32(b), Int32(count), shift;
                     ndrange=b * v)
                batches += 1
                # V-1 improving rounds plus one unchanged round suffice for FIFO paths.
                for round in 1:v
                    prepare(previous, next, next_active, router.changed; ndrange=max(b * v, length(router.host_changed)))
                    transit(previous, next, active, next_active, router.changed,
                        parent.edge_from, parent.edge_to, parent.schedule_ptr,
                        parent.departure, parent.arrival, router.cutoffs,
                        router.connections, record, Int32(b), shift; ndrange=b * e)
                    previous, next = next, previous
                    active, next_active = next_active, active
                    rounds += 1
                    if round % router.check_every == 0 || round == v
                        KA.copyto!(backend, router.host_changed, router.changed)
                        KA.synchronize(backend)
                        all(iszero, router.host_changed) && break
                        round == v && error("batched transit routing did not converge within $v rounds")
                    end
                end
                device_s += (time_ns() - started) / 1e9
                started = time_ns()
                KA.copyto!(backend, router.host_labels, previous)
                if record
                    KA.copyto!(backend, router.host_connections, router.connections)
                end
                KA.synchronize(backend)
                download_s += (time_ns() - started) / 1e9
                for q in 1:count
                    started = time_ns()
                    for vertex in 1:v
                        labels[vertex] = router.host_labels[q + b * (vertex - 1)]
                    end
                    if record
                        # Read only edges reached by replay, not every edge for every query.
                        _replay_distances!(distances, seen, graph, plan.source,
                            router.host_starts[q], router.host_cutoffs[q], labels, view(connections, q, :))
                    end
                    host_replay_s += (time_ns() - started) / 1e9
                    started = time_ns()
                    _accumulate_window!(acc, plan, plan.groups[base + q - 1], labels, distances)
                    aggregation_s += (time_ns() - started) / 1e9
                end
            end
        end
    end
    return _finish_window(acc, plan; searches=groups, full_searches=groups, origin, cells=graph.h3,
        repair_searches=0, batches, rounds, planning_s, device_s, download_s, host_replay_s, aggregation_s,
        backend=backend isa KA.CPU ? "ka_cpu_batched" : "gpu_batched")
end
