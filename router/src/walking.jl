function _walking_limit(max_walk_s::Integer)
    0 <= max_walk_s <= MAX_BUDGET_MS ÷ 1000 ||
        throw(ArgumentError("max_walk_s must be an integer from 0 to 604800"))
    return UInt32(max_walk_s * 1000)
end

struct WalkingTopology
    index::WalkingIndex
    limit::UInt32
    neighbors::Dict{UInt64,Vector{WalkingNeighbor}}
    coverage::Dict{UInt64,Vector{WalkingNeighbor}}
end
WalkingTopology(index::WalkingIndex, limit::Integer) =
    WalkingTopology(index, UInt32(limit), Dict{UInt64,Vector{WalkingNeighbor}}(),
                    Dict{UInt64,Vector{WalkingNeighbor}}())

function _walking_hops(topology, origin; geographic=false, limit=topology.limit)
    iszero(limit) && return WalkingNeighbor[]
    cache = geographic ? topology.coverage : topology.neighbors
    hops = get(cache, origin, nothing)
    if isnothing(hops)
        hops = geographic ? walking_cells(topology.index, origin, limit) :
                            walking_neighbors(topology.index, origin, limit)
        # Cache full-radius geometry, never arrival labels or partial-radius results.
        if limit == topology.limit
            cache[origin] = hops
        end
    end
    return hops
end

"""Walking-aware CPU reference, returning sorted reachable H3 cells, arrivals and km."""
function route_walking(graph::Graph, origin::UInt64, departure_ms::Integer, budget_ms::Integer;
                       max_walk_s::Integer=3600, walking_index::Union{Nothing,WalkingIndex}=nothing)
    ready, cutoff = query_times(graph, origin, departure_ms, budget_ms)
    limit = min(_walking_limit(max_walk_s), budget_ms)
    index = isnothing(walking_index) ? WalkingIndex(graph) : walking_index
    topology = WalkingTopology(index, limit)
    return _walking_route_at(graph, topology, origin, ready, cutoff)
end

function _walking_route_at(graph, topology, origin, ready::UInt32, cutoff::UInt32)
    topology.index.resolution == graph.resolution && topology.index.cells == graph.h3 ||
        throw(ArgumentError("walking index does not match graph"))
    n = length(graph.h3)
    arrival, eligible = fill(INF, n), fill(INF, n)
    distance, eligible_distance = fill(NaN, n), fill(NaN, n)
    # State 0 boards transit; state 1 starts a walk. Only transit updates both.
    queue = BinaryMinHeap{Tuple{UInt32,Int32,Int}}()
    source = get(graph.node_id, origin, Int32(0))
    if source != 0
        arrival[source] = eligible[source] = ready
        distance[source] = eligible_distance[source] = 0.0
        push!(queue, (ready, source, 0), (ready, source, 1))
    else
        for hop in _walking_hops(topology, origin)
            hop.duration_ms <= cutoff - ready || continue
            v = graph.node_id[hop.cell]
            arrival[v] = ready + hop.duration_ms
            distance[v] = hop.distance_km
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
                km = isnothing(graph.distance_km) ? NaN : distance[u] + graph.distance_km[connection]
                isinf(km) && throw(ArgumentError("accumulated route distance is not finite"))
                eligible[v], eligible_distance[v] = candidate, km
                push!(queue, (candidate, v, 1))
                if candidate < arrival[v]
                    arrival[v], distance[v] = candidate, km
                    push!(queue, (candidate, v, 0))
                end
            end
        else
            for hop in _walking_hops(topology, graph.h3[u])
                hop.duration_ms <= cutoff - time || continue
                candidate, v = time + hop.duration_ms, graph.node_id[hop.cell]
                candidate < arrival[v] || continue
                km = eligible_distance[u] + hop.distance_km
                isinf(km) && throw(ArgumentError("accumulated route distance is not finite"))
                arrival[v], distance[v] = candidate, km
                push!(queue, (candidate, v, 0))
            end
        end
    end
    result = Dict{UInt64,Tuple{UInt32,Float64}}(origin => (ready, 0.0))
    for v in eachindex(arrival)
        arrival[v] == INF && continue
        result[graph.h3[v]] = (arrival[v], distance[v])
    end
    # Geographic-only cells are terminal destinations, never a second walking step.
    for u in 0:n
        if u == 0
            source == 0 || continue
            cell, time, km = origin, ready, 0.0
        else
            eligible[u] == INF && continue
            cell, time, km = graph.h3[u], eligible[u], eligible_distance[u]
        end
        for hop in _walking_hops(topology, cell; geographic=true, limit=min(topology.limit, cutoff - time))
            hop.duration_ms <= cutoff - time || continue
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
