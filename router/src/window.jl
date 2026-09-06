"""Budget-capped mean elapsed time and conditional route distance over regular samples."""
function route_window(graph::Graph, origin::UInt64, departure_ms::Integer,
                      budget_ms::Integer, window_ms::Integer;
                      step_ms::Integer=60_000, reuse::Bool=true)
    ready, _ = query_times(graph, origin, departure_ms, budget_ms)
    1 <= window_ms <= PERIOD || throw(ArgumentError("window must be between one millisecond and one day"))
    step_ms >= 1 || throw(ArgumentError("sample step must be at least one millisecond"))
    # A step beyond the window produces only the first sample; avoid narrowing huge integers.
    step = Int64(min(step_ms, window_ms))
    samples = cld(Int64(window_ms), step)
    samples <= 86_400 || throw(ArgumentError("window must contain at most 86400 samples"))
    start, budget = Int64(ready), Int64(budget_ms)
    cutoff = UInt32(start + (samples - 1) * step + budget)
    vertices = length(graph.h3)
    capped_sum = fill(UInt64(samples) * UInt64(budget), vertices)
    reachable_samples = zeros(UInt32, vertices)
    distance_km = fill(NaN, vertices)
    source = get(graph.node_id, origin, Int32(0))
    searches = 0

    if source != 0
        edges = graph.out_ptr[source]:(graph.out_ptr[source + 1] - Int32(1))
        signature = fill((INF, 0.0), length(edges))
        next_signature = similar(signature)
        distances = Vector{Float64}(undef, vertices)
        group_start = 0
        for sample in 0:samples
            if reuse && sample < samples
                time = UInt32(start + sample * step)
                base = div(time, PERIOD) * PERIOD
                for (slot, edge) in enumerate(edges)
                    next_signature[slot] = (INF, 0.0)
                    graph.edge_to[edge] == source && continue
                    index = next_connection(graph.schedule_ptr, graph.departure,
                                            graph.arrival, edge, time, cutoff)
                    index == 0 && continue
                    km = isnothing(graph.distance_km) ? 0.0 : graph.distance_km[index]
                    next_signature[slot] = (base + graph.arrival[index], km)
                end
            end
            if sample > 0 && (sample == samples || !reuse || !isequal(signature, next_signature))
                first_time = start + group_start * step
                count = sample - group_start
                # Only search as far as this unchanged group's last sample can reach.
                group_cutoff = UInt32(start + (sample - 1) * step + budget)
                arrivals = _route_at(graph, source, UInt32(first_time), group_cutoff, distances)
                searches += 1
                # The group's common cutoff includes destinations admitted by later budgets.
                for vertex in eachindex(arrivals)
                    arrival = arrivals[vertex]
                    (vertex == source || arrival == INF) && continue
                    first_reachable = max(0, cld(Int64(arrival) - budget - first_time, step))
                    reached = max(0, count - first_reachable)
                    reached == 0 && continue
                    n = UInt64(reached)
                    time = UInt64(first_time + first_reachable * step)
                    sum_ready = div(n * (2 * time + (n - 1) * UInt64(step)), 2)
                    savings = n * UInt64(budget) + sum_ready - n * UInt64(arrival)
                    capped_sum[vertex] -= savings
                    previous_count = reachable_samples[vertex]
                    reachable_samples[vertex] += UInt32(reached)
                    distance_km[vertex] = previous_count == 0 ? distances[vertex] :
                        distance_km[vertex] + (distances[vertex] - distance_km[vertex]) * (reached / reachable_samples[vertex])
                end
                group_start = sample
            end
            signature, next_signature = next_signature, signature
        end
        capped_sum[source] = 0
        reachable_samples[source] = UInt32(samples)
        distance_km[source] = 0.0
    end

    elapsed_ms = Float64.(capped_sum) ./ samples
    reachable_elapsed_ms = fill(NaN, vertices)
    for vertex in eachindex(reachable_samples)
        reached = reachable_samples[vertex]
        reached == 0 && continue
        conditional_sum = capped_sum[vertex] - UInt64(samples - reached) * UInt64(budget)
        reachable_elapsed_ms[vertex] = conditional_sum / reached
    end
    return (; elapsed_ms, reachable_elapsed_ms, distance_km, reachable_samples,
            sample_count=UInt32(samples), searches, reused_samples=Int(samples) - searches,
            elapsed_sum_ms=capped_sum)
end
