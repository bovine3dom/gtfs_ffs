function _window_plan(graph::Graph, origin::UInt64, departure_ms::Integer,
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
    source = get(graph.node_id, origin, Int32(0))
    groups = Tuple{Int,Int}[]
    if source != 0
        edges = graph.out_ptr[source]:(graph.out_ptr[source + 1] - Int32(1))
        signature = fill((INF, 0.0), length(edges))
        next_signature = similar(signature)
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
                push!(groups, (group_start, sample - group_start))
                group_start = sample
            end
            signature, next_signature = next_signature, signature
        end
    end
    return (; source, ready, budget=UInt32(budget), step, samples=Int(samples), cutoff, groups)
end

function _window_accumulator(graph, plan)
    return (elapsed_sum_ms=fill(UInt64(plan.samples) * UInt64(plan.budget), length(graph.h3)),
            reachable_samples=zeros(UInt32, length(graph.h3)), distance_km=fill(NaN, length(graph.h3)))
end

function _accumulate_window!(acc, plan, group, arrivals, distances)
    first, count = group
    first_time = Int64(plan.ready) + first * plan.step
    for vertex in eachindex(arrivals)
        arrival = arrivals[vertex]
        (vertex == plan.source || arrival == INF) && continue
        first_reachable = max(0, cld(Int64(arrival) - Int64(plan.budget) - first_time, plan.step))
        reached = max(0, count - first_reachable)
        reached == 0 && continue
        n = UInt64(reached)
        time = UInt64(first_time + first_reachable * plan.step)
        sum_ready = div(n * (2 * time + (n - 1) * UInt64(plan.step)), 2)
        acc.elapsed_sum_ms[vertex] -= n * UInt64(plan.budget) + sum_ready - n * UInt64(arrival)
        previous_count = acc.reachable_samples[vertex]
        acc.reachable_samples[vertex] += UInt32(reached)
        if !isnothing(distances)
            acc.distance_km[vertex] = previous_count == 0 ? distances[vertex] :
                acc.distance_km[vertex] + (distances[vertex] - acc.distance_km[vertex]) * (reached / acc.reachable_samples[vertex])
        end
    end
    return acc
end

function _finish_window(acc, plan; searches::Int, kwargs...)
    if plan.source != 0
        acc.elapsed_sum_ms[plan.source] = 0
        acc.reachable_samples[plan.source] = UInt32(plan.samples)
        acc.distance_km[plan.source] = 0.0
    end
    elapsed_ms = Float64.(acc.elapsed_sum_ms) ./ plan.samples
    reachable_elapsed_ms = fill(NaN, length(elapsed_ms))
    for vertex in eachindex(acc.reachable_samples)
        reached = acc.reachable_samples[vertex]
        reached == 0 && continue
        conditional_sum = acc.elapsed_sum_ms[vertex] - UInt64(plan.samples - reached) * UInt64(plan.budget)
        reachable_elapsed_ms[vertex] = conditional_sum / reached
    end
    return (; elapsed_ms, reachable_elapsed_ms, distance_km=acc.distance_km,
            reachable_samples=acc.reachable_samples, sample_count=UInt32(plan.samples),
            searches, reused_samples=plan.samples - searches, elapsed_sum_ms=acc.elapsed_sum_ms, kwargs...)
end

"""Reference window routing with origin-only grouping and chronological aggregation."""
function route_window(graph::Graph, origin::UInt64, departure_ms::Integer,
                      budget_ms::Integer, window_ms::Integer;
                      step_ms::Integer=60_000, reuse::Bool=true)
    plan = _window_plan(graph, origin, departure_ms, budget_ms, window_ms; step_ms, reuse)
    acc = _window_accumulator(graph, plan)
    distances = isnothing(graph.distance_km) ? nothing : Vector{Float64}(undef, length(graph.h3))
    for group in plan.groups
        first, count = group
        ready = UInt32(Int64(plan.ready) + first * plan.step)
        cutoff = UInt32(Int64(plan.ready) + (first + count - 1) * plan.step + plan.budget)
        arrivals = _route_at(graph, plan.source, ready, cutoff, distances)
        _accumulate_window!(acc, plan, group, arrivals, distances)
    end
    return _finish_window(acc, plan; searches=length(plan.groups))
end

"""Replay canonical Dijkstra discoveries using cached connections and final arrivals."""
function _replay_distances!(distances, seen, graph, source, ready, cutoff, labels, connections)
    fill!(distances, NaN)
    source == 0 && return 0
    distances[source] = 0.0
    isnothing(graph.distance_km) && return 0
    fill!(seen, INF)
    seen[source] = ready
    queue = BinaryMinHeap{Tuple{UInt32,Int32}}()
    push!(queue, (ready, source))
    visited = 0
    while !isempty(queue)
        time, u = pop!(queue)
        visited += 1
        base = (time ÷ PERIOD) * PERIOD
        for edge in graph.out_ptr[u]:(graph.out_ptr[u + 1] - Int32(1))
            v = graph.edge_to[edge]
            v == u && continue
            index = connections[edge]
            index == 0 && continue
            candidate = base + graph.arrival[index]
            candidate <= cutoff && candidate < seen[v] || continue
            candidate >= labels[v] || error("cached arrival labels are not optimal")
            # Track tentative improvements too, including the reference's overflow checks.
            km = distances[u] + graph.distance_km[index]
            isfinite(km) || throw(ArgumentError("accumulated route distance is not finite"))
            seen[v] = candidate
            if candidate == labels[v]
                distances[v] = km
                push!(queue, (candidate, v))
            end
        end
    end
    visited == count(time -> time <= cutoff, labels) || error("cached connections do not reproduce final arrival labels")
    return visited
end
