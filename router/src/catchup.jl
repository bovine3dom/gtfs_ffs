"""Window routing with backward arrival repair and chronological, bounded-buffer aggregation.

Lookup and expansion counters cover arrival routing, not grouping or distance replay.
"""
function route_window_cached(graph::Graph, origin::UInt64, departure_ms::Integer,
                             budget_ms::Integer, window_ms::Integer;
                             step_ms::Integer=60_000, chunk_size::Integer=64)
    1 <= chunk_size <= 256 || throw(ArgumentError("chunk_size must be between 1 and 256"))
    plan = _window_plan(graph, origin, departure_ms, budget_ms, window_ms; step_ms)
    acc = _window_accumulator(graph, plan)
    groups = length(plan.groups)
    width = min(Int(chunk_size), groups)
    vertices = length(graph.h3)
    saved_arrivals = Matrix{UInt32}(undef, vertices, width)
    saved_distances = isnothing(graph.distance_km) ? nothing : Matrix{Float64}(undef, vertices, width)
    labels = fill(INF, vertices)
    connections = zeros(Int32, length(graph.edge_to))
    seen = Vector{UInt32}(undef, vertices)
    queue = BinaryMinHeap{Tuple{UInt32,Int32}}()
    full_searches = profile_lookups = routing_expansions = 0

    for first_group in 1:Int(chunk_size):groups
        last_group = min(first_group + Int(chunk_size) - 1, groups)
        fill!(labels, INF)
        full_searches += 1
        for index in last_group:-1:first_group
            first, count = plan.groups[index]
            ready = UInt32(Int64(plan.ready) + first * plan.step)
            cutoff = UInt32(Int64(plan.ready) + (first + count - 1) * plan.step + plan.budget)
            labels[plan.source] = ready
            # Each repair drains the heap, including stale entries, before the next group.
            push!(queue, (ready, plan.source))
            while !isempty(queue)
                time, u = pop!(queue)
                time == labels[u] || continue
                routing_expansions += 1
                for edge in graph.out_ptr[u]:(graph.out_ptr[u + 1] - Int32(1))
                    v = graph.edge_to[edge]
                    v == u && continue
                    connection = next_connection(graph.schedule_ptr, graph.departure,
                                                 graph.arrival, edge, time, cutoff)
                    connections[edge] = connection
                    profile_lookups += 1
                    connection == 0 && continue
                    candidate = (time ÷ PERIOD) * PERIOD + graph.arrival[connection]
                    if candidate < labels[v]
                        labels[v] = candidate
                        push!(queue, (candidate, v))
                    end
                end
            end

            # Earlier departures retain feasible upper bounds. Shrinking cutoffs only
            # hide old labels; any newly useful prefix is repaired within this cutoff.
            column = index - first_group + 1
            arrivals = view(saved_arrivals, :, column)
            for vertex in eachindex(labels)
                arrivals[vertex] = labels[vertex] <= cutoff ? labels[vertex] : INF
            end
            if !isnothing(saved_distances)
                _replay_distances!(view(saved_distances, :, column), seen, graph,
                                   plan.source, ready, cutoff, arrivals, connections)
            end
        end
        # Preserve the reference's floating-point mean update order across all chunks.
        for index in first_group:last_group
            column = index - first_group + 1
            distances = isnothing(saved_distances) ? nothing : view(saved_distances, :, column)
            _accumulate_window!(acc, plan, plan.groups[index],
                                view(saved_arrivals, :, column), distances)
        end
    end
    return _finish_window(acc, plan; searches=groups, backend="catchup", full_searches,
                          repair_searches=groups - full_searches, profile_lookups, routing_expansions)
end
