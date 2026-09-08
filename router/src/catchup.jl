"""Window routing with backward arrival repair and chronological, bounded-buffer aggregation.

Lookup and expansion counters cover arrival routing, not grouping or distance replay.
"""
function route_window_cached(graph::Graph, origin::UInt64, departure_ms::Integer,
                             budget_ms::Integer, window_ms::Integer;
                             step_ms::Integer=60_000, chunk_size::Integer=64,
                             workers::Integer=Threads.nthreads(:default),
                             distance_mode="itinerary", window_mode=:mean_intersection)
    track = _distance_mode(distance_mode) == :itinerary
    1 <= chunk_size <= 256 || throw(ArgumentError("chunk_size must be between 1 and 256"))
    workers > 0 || throw(ArgumentError("workers must be positive"))
    plan = _window_plan(graph, origin, departure_ms, budget_ms, window_ms; step_ms)
    acc = _window_accumulator(graph, plan, track; window_mode)
    groups = length(plan.groups)
    width = min(Int(chunk_size), groups)
    full_searches = cld(groups, Int(chunk_size))
    worker_count = Int(min(workers, Threads.nthreads(:default), full_searches))
    workspaces = [_catchup_workspace(graph, width, track) for _ in 1:worker_count]
    outcomes = Vector{Any}(undef, worker_count)
    profile_lookups = routing_expansions = 0
    for wave in 1:max(worker_count, 1):full_searches
        active = min(worker_count, full_searches - wave + 1)
        if active == 1
            first_group = (wave - 1) * Int(chunk_size) + 1
            outcomes[1] = _catchup_chunk!(workspaces[1], graph, plan, first_group, min(first_group + Int(chunk_size) - 1, groups))
        else
            # Ownership is by slot, not thread ID: spawned tasks may migrate.
            @sync for slot in 1:active
                let slot=slot, first_group=(wave + slot - 2) * Int(chunk_size) + 1
                    Threads.@spawn begin
                        outcomes[slot] = try
                            _catchup_chunk!(workspaces[slot], graph, plan, first_group, min(first_group + Int(chunk_size) - 1, groups))
                        catch error
                            error
                        end
                    end
                end
            end
        end
        # Join the whole wave before throwing or consuming any reusable buffer.
        for slot in 1:active
            outcomes[slot] isa Exception && throw(outcomes[slot])
        end
        for slot in 1:active
            profile_lookups += outcomes[slot][1]
            routing_expansions += outcomes[slot][2]
            first_group = (wave + slot - 2) * Int(chunk_size) + 1
            state = workspaces[slot]
            for index in first_group:min(first_group + Int(chunk_size) - 1, groups)
                column = index - first_group + 1
                distances = isnothing(state.saved_distances) ? nothing : view(state.saved_distances, :, column)
                _accumulate_window!(acc, plan, plan.groups[index], view(state.saved_arrivals, :, column), distances)
            end
        end
    end
    return _finish_window(acc, plan; searches=groups, origin, cells=graph.h3, backend="catchup", full_searches,
                          repair_searches=groups - full_searches, profile_lookups, routing_expansions,
                          workers=worker_count)
end

function _catchup_workspace(graph, width, track_distance=true)
    vertices = length(graph.h3)
    replay = track_distance && !isnothing(graph.distance_km)
    return (saved_arrivals=Matrix{UInt32}(undef, vertices, width),
            saved_distances=replay ? Matrix{Float64}(undef, vertices, width) : nothing,
            labels=Vector{UInt32}(undef, vertices), connections=replay ? zeros(Int32, length(graph.edge_to)) : nothing,
            seen=replay ? Vector{UInt32}(undef, vertices) : nothing, queue=BinaryMinHeap{Tuple{UInt32,Int32}}())
end

function _catchup_chunk!(state, graph, plan, first_group, last_group)
    (; saved_arrivals, saved_distances, labels, connections, seen, queue) = state
    fill!(labels, INF)
    profile_lookups = routing_expansions = 0
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
                isnothing(connections) || (connections[edge] = connection)
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
    return profile_lookups, routing_expansions
end
