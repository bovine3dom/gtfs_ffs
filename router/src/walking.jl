function _walking_limit(max_walk_s::Integer)
    0 <= max_walk_s <= MAX_BUDGET_MS ÷ 1000 ||
        throw(ArgumentError("max_walk_s must be an integer from 0 to 604800"))
    return UInt32(max_walk_s * 1000)
end

const WalkingGeometryEntry = Tuple{UInt32,Vector{WalkingNeighbor}}

# Request-private: all sharing topologies must use the same index and hop limit.
struct WalkingGeometryCache
    lock::ReentrantLock
    entries::Dict{Tuple{Bool,UInt64},Tuple{ReentrantLock,Base.RefValue{WalkingGeometryEntry}}}
end
WalkingGeometryCache() = WalkingGeometryCache(ReentrantLock(),
    Dict{Tuple{Bool,UInt64},Tuple{ReentrantLock,Base.RefValue{WalkingGeometryEntry}}}())

struct WalkingTopology
    index::WalkingIndex
    limit::UInt32
    neighbors::Dict{UInt64,WalkingGeometryEntry}
    coverage::Dict{UInt64,WalkingGeometryEntry}
    shared::Union{Nothing,WalkingGeometryCache}
end
WalkingTopology(index::WalkingIndex, limit::Integer, shared::Union{Nothing,WalkingGeometryCache}=nothing) =
    WalkingTopology(index, UInt32(limit), Dict{UInt64,WalkingGeometryEntry}(),
                    Dict{UInt64,WalkingGeometryEntry}(), shared)

Base.@constprop :aggressive function _walking_hops(topology, origin; geographic=false, limit=topology.limit)
    iszero(limit) && return WalkingNeighbor[]
    prepared = topology.index.prepared
    if !isnothing(prepared) && limit <= prepared.limit
        u = get(prepared.node_id, origin, Int32(0))
        if u != 0
            packed = geographic ? prepared.geographic : prepared.graph
            return WalkingRange(packed, packed.offsets[u], packed.offsets[u + 1] - packed.offsets[u])
        end
    end
    cache = geographic ? topology.coverage : topology.neighbors
    entry = get(cache, origin, nothing)
    if isnothing(entry) || entry[1] < limit
        shared = topology.shared
        if isnothing(shared)
            entry = (UInt32(limit), geographic ? walking_cells(topology.index, origin, limit) :
                                                walking_neighbors(topology.index, origin, limit))
        else
            entry_lock, published = lock(shared.lock) do
                get!(shared.entries, (geographic, origin)) do
                    (ReentrantLock(), Ref((UInt32(0), WalkingNeighbor[])))
                end
            end
            # Never hold the registry lock during geometry computation or entry waits.
            entry = lock(entry_lock) do
                if published[][1] < limit
                    hops = geographic ? walking_cells(topology.index, origin, limit) :
                                        walking_neighbors(topology.index, origin, limit)
                    # Published vectors stay read-only, including older local snapshots.
                    published[] = (UInt32(limit), hops)
                end
                published[]
            end
        end
        # A larger cached radius serves smaller cutoffs; callers filter durations.
        cache[origin] = entry
    end
    return entry[2]
end

_walking_node(graph, cell::UInt64) = graph.node_id[cell]
_walking_node(graph, cell::Int32) = cell

"""Walking-aware CPU reference, returning sorted reachable H3 cells, arrivals and km."""
function route_walking(graph::Graph, origin::UInt64, departure_ms::Integer, budget_ms::Integer;
                       max_walk_s::Integer=3600, walking_index::Union{Nothing,WalkingIndex}=nothing,
                       distance_mode="itinerary")
    mode = _distance_mode(distance_mode)
    ready, cutoff = query_times(graph, origin, departure_ms, budget_ms)
    limit = min(_walking_limit(max_walk_s), budget_ms)
    index = isnothing(walking_index) ? WalkingIndex(graph) : walking_index
    topology = WalkingTopology(index, limit)
    result = _walking_route_at(graph, topology, origin, ready, cutoff, mode == :itinerary)
    return mode == :itinerary ? result : merge(result, (distance_km=_od_distances(origin, result.h3),))
end

function _walking_route_at(graph, topology, origin, ready::UInt32, cutoff::UInt32, track_distance=true)
    topology.index.resolution == graph.resolution && topology.index.cells == graph.h3 ||
        throw(ArgumentError("walking index does not match graph"))
    n = length(graph.h3)
    arrival, eligible = fill(INF, n), fill(INF, n)
    distance = track_distance ? fill(NaN, n) : nothing
    eligible_distance = track_distance ? fill(NaN, n) : nothing
    # State 0 boards transit; state 1 starts a walk. Only transit updates both.
    queue = BinaryMinHeap{Tuple{UInt32,Int32,Int}}()
    source = get(graph.node_id, origin, Int32(0))
    if source != 0
        arrival[source] = eligible[source] = ready
        track_distance && (distance[source] = eligible_distance[source] = 0.0)
        push!(queue, (ready, source, 0), (ready, source, 1))
    else
        for hop in _walking_hops(topology, origin)
            hop.duration_ms <= min(topology.limit, cutoff - ready) || continue
            v = _walking_node(graph, hop.cell)
            arrival[v] = ready + hop.duration_ms
            track_distance && (distance[v] = hop.distance_km)
            push!(queue, (arrival[v], v, 0))
        end
    end
    while !isempty(queue)
        time, u, state = pop!(queue)
        time == (state == 0 ? arrival[u] : eligible[u]) || continue
        if state == 0
            for edge in graph.out_ptr[u]:(graph.out_ptr[u + 1] - Int32(1))
                connection = next_connection(graph.schedule_ptr, graph.departure, graph.arrival, edge, time, cutoff)
                connection == 0 && continue
                candidate = (time ÷ PERIOD) * PERIOD + graph.arrival[connection]
                v = graph.edge_to[edge]
                candidate < eligible[v] || continue
                if track_distance
                    km = isnothing(graph.distance_km) ? NaN : distance[u] + graph.distance_km[connection]
                    isinf(km) && throw(ArgumentError("accumulated route distance is not finite"))
                    eligible_distance[v] = km
                end
                eligible[v] = candidate
                push!(queue, (candidate, v, 1))
                if candidate < arrival[v]
                    arrival[v] = candidate
                    track_distance && (distance[v] = km)
                    push!(queue, (candidate, v, 0))
                end
            end
        else
            for hop in _walking_hops(topology, graph.h3[u])
                hop.duration_ms <= min(topology.limit, cutoff - time) || continue
                candidate, v = time + hop.duration_ms, _walking_node(graph, hop.cell)
                candidate < arrival[v] || continue
                if track_distance
                    km = eligible_distance[u] + hop.distance_km
                    isinf(km) && throw(ArgumentError("accumulated route distance is not finite"))
                    distance[v] = km
                end
                arrival[v] = candidate
                push!(queue, (candidate, v, 0))
            end
        end
    end
    return _walking_result(graph, topology, origin, ready, cutoff, arrival, eligible,
                           distance, eligible_distance)
end

function _walking_result(graph, topology, origin, ready, cutoff, arrival, eligible,
                         ::Nothing, ::Nothing)
    result = Dict{UInt64,UInt32}(origin => ready)
    for v in eachindex(arrival)
        arrival[v] <= cutoff && (result[graph.h3[v]] = arrival[v])
    end
    for u in 0:length(graph.h3)
        if u == 0
            haskey(graph.node_id, origin) && continue
            cell, time = origin, ready
        else
            eligible[u] <= cutoff || continue
            cell, time = graph.h3[u], eligible[u]
        end
        limit = min(topology.limit, cutoff - time)
        for hop in _walking_hops(topology, cell; geographic=true, limit)
            hop.duration_ms <= limit || continue
            candidate = time + hop.duration_ms
            candidate < get(result, hop.cell, INF) && (result[hop.cell] = candidate)
        end
    end
    h3 = sort!(collect(keys(result)))
    return (; h3, arrival=UInt32[result[h] for h in h3])
end

function _walking_result(graph, topology, origin, ready, cutoff, arrival, eligible,
                         distance, eligible_distance)
    source = get(graph.node_id, origin, Int32(0))
    result = Dict{UInt64,Tuple{UInt32,Float64}}(origin => (ready, 0.0))
    for v in eachindex(arrival)
        arrival[v] <= cutoff || continue
        result[graph.h3[v]] = (arrival[v], distance[v])
    end
    # Geographic-only cells are terminal destinations, never a second walking step.
    for u in 0:length(graph.h3)
        if u == 0
            source == 0 || continue
            cell, time, km = origin, ready, 0.0
        else
            eligible[u] <= cutoff || continue
            cell, time, km = graph.h3[u], eligible[u], eligible_distance[u]
        end
        for hop in _walking_hops(topology, cell; geographic=true, limit=min(topology.limit, cutoff - time))
            hop.duration_ms <= min(topology.limit, cutoff - time) || continue
            candidate = time + hop.duration_ms
            previous = get(result, hop.cell, (INF, NaN))
            candidate < previous[1] || continue
            total_km = km + hop.distance_km
            isinf(total_km) && throw(ArgumentError("accumulated route distance is not finite"))
            result[hop.cell] = (candidate, total_km)
        end
    end
    cells = sort!(collect(keys(result)))
    return (h3=cells, arrival=UInt32[result[h][1] for h in cells],
            distance_km=Float64[result[h][2] for h in cells])
end
