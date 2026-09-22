function _walking_limit(max_walk_ms::Integer)
    0 <= max_walk_ms <= MAX_TIME_MS ||
        throw(ArgumentError("max_walk_ms must be below UInt32 arrival INF"))
    return UInt32(max_walk_ms)
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
                       max_walk_ms::Integer=3_600_000, walking_index::Union{Nothing,WalkingIndex}=nothing,
                       distance_mode="itinerary", stats=nothing)
    mode = _distance_mode(distance_mode)
    ready, cutoff = query_times(graph, origin, departure_ms, budget_ms)
    limit = min(_walking_limit(max_walk_ms), budget_ms)
    index = isnothing(walking_index) ? WalkingIndex(graph) : walking_index
    topology = WalkingTopology(index, limit)
    result = _walking_route_at(graph, topology, origin, ready, cutoff, mode == :itinerary, stats)
    return mode == :itinerary ? result : merge(result, (distance_km=_od_distances(origin, result.h3),))
end

function _walking_route_at(graph, topology, origin, ready::UInt32, cutoff::UInt32, track_distance=true, stats=nothing)
    isnothing(graph.trip_id) || return _walking_route_trip_at(graph, topology, origin, ready, cutoff, track_distance, stats)
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

function _walking_route_trip_at(graph, topology, origin, ready::UInt32, cutoff::UInt32, track_distance=true, stats=nothing)
    topology.index.resolution == graph.resolution && topology.index.cells == graph.h3 ||
        throw(ArgumentError("walking index does not match graph"))
    labels = Dict{UInt64,UInt32}()
    transit_best = fill(INF, length(graph.h3))
    walk_best = fill(INF, length(graph.h3))
    state_distance = track_distance ? Dict{UInt64,Float64}() : nothing
    queue = UInt32RadixHeap{UInt64}()
    seen_trip = zeros(Int32, maximum(graph.trip_id; init=UInt32(0)))
    generation = Int32(0)
    function enqueue!(time, node, trip, is_walk, km)
        key = _trip_state_key(node, trip, is_walk)
        old = get(labels, key, INF)
        if time >= old
            isnothing(stats) || (stats.state_dominance_discards += 1)
            return
        end
        labels[key] = time
        is_walk && (walk_best[node] = min(walk_best[node], time))
        track_distance && (state_distance[key] = km)
        push!(queue, time, key)
        isnothing(stats) || (stats.state_enqueues += 1; stats.queue_peak = max(stats.queue_peak, length(queue)))
    end
    source = get(graph.node_id, origin, Int32(0))
    if source != 0
        enqueue!(ready, source, UInt32(0), false, 0.0)
        enqueue!(ready, source, UInt32(0), true, 0.0)
        transit_best[source] = ready
    else
        for hop in _walking_hops(topology, origin)
            hop.duration_ms <= min(topology.limit, cutoff - ready) || continue
            v = _walking_node(graph, hop.cell)
            enqueue!(ready + hop.duration_ms, v, UInt32(0), false, hop.distance_km)
            transit_best[v] = min(transit_best[v], ready + hop.duration_ms)
        end
    end
    while !isempty(queue)
        time, key = pop!(queue)
        isnothing(stats) || (stats.state_pops += 1)
        is_walk = _trip_state_walk(key)
        if get(labels, key, INF) != time
            isnothing(stats) || (stats.stale_pops += 1)
            continue
        end
        u = _trip_state_node(key)
        current_trip = _trip_state_trip(key)
        if is_walk
            for hop in _walking_hops(topology, graph.h3[u])
                hop.duration_ms <= min(topology.limit, cutoff - time) || continue
                v, candidate = _walking_node(graph, hop.cell), time + hop.duration_ms
                km = track_distance ? state_distance[key] + hop.distance_km : 0.0
                enqueue!(candidate, v, UInt32(0), false, km)
                transit_best[v] = min(transit_best[v], candidate)
            end
        else
            base = (time ÷ PERIOD) * PERIOD
            lower = time - base
            upper = cutoff - base
            other_ready = _trip_other_ready(time, current_trip, cutoff)
            for edge in graph.out_ptr[u]:(graph.out_ptr[u + 1] - Int32(1))
                generation += Int32(1)
                isnothing(stats) || (stats.edge_queries += 1)
                v = graph.edge_to[edge]
                dominance_limit = _trip_combined_dominance_limit(transit_best[v], walk_best[v])

                if current_trip != 0
                    first_group, stop = _trip_group_range(graph, edge, current_trip, stats)
                    if first_group != 0
                        connection = _trip_group_lower_bound(graph, first_group, stop, lower, stats)
                        if connection < stop
                            isnothing(stats) || (stats.event_rows_scanned += 1)
                            @inbounds d = graph.departure[connection]
                            if d <= upper
                                isnothing(stats) || (stats.event_groups += 1)
                                @inbounds a = graph.arrival[connection]
                                if a <= upper
                                    candidate = base + a
                                    km = track_distance ? (isnothing(graph.distance_km) ? NaN :
                                        state_distance[key] + graph.distance_km[connection]) : 0.0
                                    next_key = _trip_state_key(v, current_trip)
                                    if candidate < get(labels, next_key, INF) &&
                                            !(candidate >= MIN_TRIP_CONNECTION_MS &&
                                              transit_best[v] <= candidate - MIN_TRIP_CONNECTION_MS)
                                        transit_best[v] = min(transit_best[v], candidate)
                                        enqueue!(candidate, v, current_trip, false, km)
                                    end
                                    enqueue!(candidate, v, UInt32(0), true, km)
                                end
                            end
                        end
                    end
                end

                if current_trip == 0 || other_ready != INF
                    event_clock = current_trip == 0 ? lower : other_ready - base
                    slot = _trip_event_lower_bound(graph, edge, event_clock, stats)
                    stop = graph.trip_event_ptr[edge + Int32(1)]
                    while slot < stop
                        if _trip_suffix_reaches_limit(graph, slot, base, dominance_limit)
                            isnothing(stats) || (stats.event_dominance_breaks += 1)
                            break
                        end
                        connection = graph.trip_event_index[slot]
                        isnothing(stats) || (stats.event_rows_scanned += 1)
                        @inbounds d = graph.departure[connection]
                        d > upper && break
                        @inbounds trip = graph.trip_id[connection]
                        current_trip != 0 && trip == current_trip || begin
                            @inbounds seen_trip[trip] == generation || begin
                                @inbounds a = graph.arrival[connection]
                                @inbounds seen_trip[trip] = generation
                                isnothing(stats) || (stats.event_groups += 1)
                                if a <= upper
                                    candidate = base + a
                                    km = track_distance ? (isnothing(graph.distance_km) ? NaN :
                                        state_distance[key] + graph.distance_km[connection]) : 0.0
                                    next_key = _trip_state_key(v, trip)
                                    if candidate < get(labels, next_key, INF) &&
                                            !(candidate >= MIN_TRIP_CONNECTION_MS &&
                                              transit_best[v] <= candidate - MIN_TRIP_CONNECTION_MS)
                                        transit_best[v] = min(transit_best[v], candidate)
                                        enqueue!(candidate, v, trip, false, km)
                                    end
                                    enqueue!(candidate, v, UInt32(0), true, km)
                                end
                            end
                        end
                        slot += Int32(1)
                    end
                end
            end
        end
    end
    result = Dict{UInt64,Tuple{UInt32,Float64}}(origin => (ready, 0.0))
    for (key, time) in labels
        time <= cutoff || continue
        node = _trip_state_node(key)
        _trip_state_walk(key) && continue
        result[graph.h3[node]] = min(get(result, graph.h3[node], (INF, NaN)),
                                     (time, track_distance ? state_distance[key] : NaN))
    end
    for (key, time) in labels
        time <= cutoff || continue
        _trip_state_walk(key) || continue
        node = _trip_state_node(key)
        km = track_distance ? state_distance[key] : NaN
        for hop in _walking_hops(topology, graph.h3[node]; geographic=true,
                                 limit=min(topology.limit, cutoff - time))
            hop.duration_ms <= min(topology.limit, cutoff - time) || continue
            cell, candidate = hop.cell, time + hop.duration_ms
            total_km = track_distance ? km + hop.distance_km : NaN
            result[cell] = min(get(result, cell, (INF, NaN)), (candidate, total_km))
        end
    end
    h3 = sort!(collect(keys(result)))
    if track_distance
        return (h3=h3, arrival=UInt32[result[h][1] for h in h3],
                distance_km=Float64[result[h][2] for h in h3])
    end
    return (h3=h3, arrival=UInt32[result[h][1] for h in h3])
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
