mutable struct TripWorkspace{K}
    ids::Dict{K,UInt32}
    keys::Vector{K}
    arrival::Vector{UInt32}
    distance::Vector{Float64}
    settled::Vector{UInt64}
    heads::Vector{UInt32}
    indexed_pending::BitVector
    pending_index::Dict{UInt64,UInt32}
    event_time::Vector{UInt32}
    event_next::Vector{UInt32}
    event_prev::Vector{UInt32}
    event_state::Vector{UInt32}
    event_mask::Vector{UInt64}
    free_events::Vector{UInt32}
    queue::UInt32RadixHeap{UInt32}
    seen_trip::Dict{UInt32,Int}
    dominance::Dict{UInt128,UInt32}
    best::Vector{UInt32}
    walk::Vector{UInt32}
    transferred::Vector{UInt32}
end

TripWorkspace{K}() where K = TripWorkspace(Dict{K,UInt32}(), K[], UInt32[], Float64[],
    UInt64[], UInt32[], BitVector(), Dict{UInt64,UInt32}(), UInt32[], UInt32[], UInt32[], UInt32[], UInt64[], UInt32[], UInt32RadixHeap{UInt32}(),
    Dict{UInt32,Int}(), Dict{UInt128,UInt32}(), UInt32[], UInt32[], UInt32[])

mutable struct TripWorkspacePool
    lock::ReentrantLock
    idle::Vector{Tuple{Any,Int}}
    bytes::Int
    limit::Int
end
TripWorkspacePool(; limit=256*1024^2) = begin
    limit >= 0 || throw(ArgumentError("trip workspace cache limit must be nonnegative"))
    TripWorkspacePool(ReentrantLock(), Tuple{Any,Int}[], 0, limit)
end
const TRIP_WORKSPACES = let
    mib = parse(Int, get(ENV, "ROUTER_TRIP_WORKSPACE_CACHE_MIB", "256"))
    0 <= mib <= div(typemax(Int),1024^2) || throw(ArgumentError("invalid trip workspace cache limit"))
    TripWorkspacePool(; limit=mib*1024^2)
end

function _reset_trip_workspace!(ws, nodes, lanes)
    empty!(ws.ids); empty!(ws.keys); empty!(ws.arrival); empty!(ws.distance)
    empty!(ws.settled); empty!(ws.heads); empty!(ws.event_state); empty!(ws.event_mask)
    empty!(ws.event_time); empty!(ws.event_next); empty!(ws.event_prev)
    empty!(ws.indexed_pending); empty!(ws.pending_index)
    empty!(ws.free_events); empty!(ws.seen_trip); empty!(ws.dominance)
    foreach(empty!, ws.queue.buckets)
    empty!(ws.queue.scratch)
    ws.queue.last = UInt32(0); ws.queue.count = 0
    for array in (ws.best, ws.walk, ws.transferred)
        resize!(array, Base.checked_mul(nodes,lanes))
        fill!(array, INF)
    end
    ws
end

function _with_trip_workspace(f, ::Type{K}, nodes, lanes; pool=TRIP_WORKSPACES) where K
    ws = lock(pool.lock) do
        slot = findlast(entry -> entry[1] isa TripWorkspace{K}, pool.idle)
        isnothing(slot) && return TripWorkspace{K}()
        workspace, bytes = splice!(pool.idle,slot)
        pool.bytes -= bytes
        workspace::TripWorkspace{K}
    end
    try
        _reset_trip_workspace!(ws,nodes,lanes)
        return f(ws)
    finally
        # This bounds retained scratch, not the live working set of active queries.
        bytes = Base.summarysize(ws)
        lock(pool.lock) do
            if bytes <= pool.limit
                while pool.bytes + bytes > pool.limit
                    _, old_bytes = popfirst!(pool.idle)
                    pool.bytes -= old_bytes
                end
                push!(pool.idle,(ws,bytes))
                pool.bytes += bytes
            end
        end
    end
end

@inline function _trip_id!(ws::TripWorkspace{K}, key::K) where K
    get!(ws.ids,key) do
        length(ws.keys) < typemax(UInt32) || throw(ArgumentError("too many trip states"))
        push!(ws.keys,key)
        if K === UInt64
            push!(ws.arrival,INF)
            push!(ws.distance,NaN)
        else
            push!(ws.settled,UInt64(0))
            push!(ws.heads,UInt32(0))
            push!(ws.indexed_pending,false)
        end
        UInt32(length(ws.keys))
    end
end

@inline function _trip_label(ws, key)
    id = get(ws.ids,key,UInt32(0))
    id == 0 ? INF : ws.arrival[id]
end

@inline _pending_key(id,time) = (UInt64(id) << 32) | UInt64(time)

function _pending_event(ws,id,time,stats)
    ws.indexed_pending[id] && return get(ws.pending_index,_pending_key(id,time),UInt32(0))
    event = ws.heads[id]
    probes = 0
    while event != 0
        probes += 1
        ws.event_time[event] == time && break
        if probes == 32
            # Do not repeatedly scan long chains on dense or long-horizon queries.
            cursor = ws.heads[id]
            while cursor != 0
                ws.pending_index[_pending_key(id,ws.event_time[cursor])] = cursor
                cursor = ws.event_next[cursor]
            end
            ws.indexed_pending[id] = true
            event = get(ws.pending_index,_pending_key(id,time),UInt32(0))
            break
        end
        event = ws.event_next[event]
    end
    if !isnothing(stats)
        stats.pending_event_probes += probes
        stats.pending_probe_peak = max(stats.pending_probe_peak,probes)
    end
    event
end
