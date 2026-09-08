function _window_times(ready, budget, window_ms::Integer, step_ms::Integer)
    1 <= window_ms <= MAX_TIME_MS || throw(ArgumentError("window must fit the UInt32 time range"))
    step_ms >= 1 || throw(ArgumentError("sample step must be at least one millisecond"))
    step = Int64(min(step_ms, window_ms))
    samples = cld(Int64(window_ms), step)
    cutoff = Int64(ready) + (samples - 1) * step + budget
    cutoff < INF || throw(ArgumentError("last departure plus budget must be below UInt32 arrival INF"))
    # With millisecond steps and cutoff < INF, counts fit UInt32 and time sums fit UInt64.
    return step, samples, UInt32(cutoff)
end

function _window_plan(graph::Graph, origin::UInt64, departure_ms::Integer,
                      budget_ms::Integer, window_ms::Integer;
                      step_ms::Integer=60_000, reuse::Bool=true)
    ready, _ = query_times(graph, origin, departure_ms, budget_ms)
    step, samples, cutoff = _window_times(ready, budget_ms, window_ms, step_ms)
    start, budget = Int64(ready), Int64(budget_ms)
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

function _window_mode(mode)
    mode isa Union{Symbol,AbstractString} && mode in
        (:mean_intersection, :min_union, :max_intersection, :diff_union, :reachable_union,
         "mean_intersection", "min_union", "max_intersection", "diff_union", "reachable_union") ||
        throw(ArgumentError("window_mode must be mean_intersection, min_union, max_intersection, diff_union or reachable_union"))
    return Symbol(mode)
end

function _window_accumulator(graph, plan, track_distance=true; window_mode=:mean_intersection)
    mode = _window_mode(window_mode)
    return (elapsed_sum_ms=fill(UInt64(plan.samples) * UInt64(plan.budget), length(graph.h3)),
            elapsed_min_ms=mode in (:min_union, :diff_union) ? fill(INF, length(graph.h3)) : nothing,
            elapsed_max_ms=mode in (:max_intersection, :diff_union) ? zeros(UInt32, length(graph.h3)) : nothing,
            reachable_samples=zeros(UInt32, length(graph.h3)),
            distance_km=track_distance ? fill(NaN, length(graph.h3)) : nothing)
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
        if !isnothing(acc.elapsed_min_ms)
            # Shared absolute arrival: the group's latest departure has minimum elapsed time.
            elapsed = UInt32(Int64(arrival) - (first_time + (count - 1) * plan.step))
            if elapsed < acc.elapsed_min_ms[vertex]
                acc.elapsed_min_ms[vertex] = elapsed
                isnothing(distances) || (acc.distance_km[vertex] = distances[vertex])
            end
        elseif isnothing(acc.elapsed_max_ms) && !isnothing(distances)
            acc.distance_km[vertex] = previous_count == 0 ? distances[vertex] :
                acc.distance_km[vertex] + (distances[vertex] - acc.distance_km[vertex]) * (reached / acc.reachable_samples[vertex])
        end
        if !isnothing(acc.elapsed_max_ms)
            # Only the reachable suffix contributes: its earliest departure is worst.
            elapsed = UInt32(UInt64(arrival) - time)
            if previous_count == 0 || elapsed > acc.elapsed_max_ms[vertex]
                acc.elapsed_max_ms[vertex] = elapsed
                isnothing(acc.elapsed_min_ms) && !isnothing(distances) && (acc.distance_km[vertex] = distances[vertex])
            end
        end
    end
    return acc
end

function _window_elapsed(total, counts, minimum, maximum, samples, budget)
    elapsed = Float64.(total) ./ samples
    conditional = fill(NaN, length(elapsed))
    for i in eachindex(counts)
        reached = counts[i]
        reached == 0 && continue
        if !isnothing(minimum) && !isnothing(maximum)
            elapsed[i] = Float64(reached < samples ? budget : maximum[i]) - minimum[i]
        elseif !isnothing(minimum)
            elapsed[i] = minimum[i]
        elseif !isnothing(maximum)
            elapsed[i] = maximum[i]
        end
        conditional[i] = isnothing(minimum) && isnothing(maximum) ?
            (total[i] - UInt64(samples - reached) * UInt64(budget)) / reached : elapsed[i]
    end
    return elapsed, conditional
end

function _finish_window(acc, plan; searches::Int, origin=nothing, cells=nothing, kwargs...)
    if plan.source != 0
        acc.elapsed_sum_ms[plan.source] = 0
        isnothing(acc.elapsed_min_ms) || (acc.elapsed_min_ms[plan.source] = 0)
        isnothing(acc.elapsed_max_ms) || (acc.elapsed_max_ms[plan.source] = 0)
        acc.reachable_samples[plan.source] = UInt32(plan.samples)
        isnothing(acc.distance_km) || (acc.distance_km[plan.source] = 0.0)
    end
    elapsed_ms, reachable_elapsed_ms = _window_elapsed(acc.elapsed_sum_ms, acc.reachable_samples,
        acc.elapsed_min_ms, acc.elapsed_max_ms, plan.samples, plan.budget)
    distance_km = acc.distance_km
    if isnothing(distance_km)
        distance_km = fill(NaN, length(elapsed_ms))
        ids = findall(!iszero, acc.reachable_samples)
        distance_km[ids] = _od_distances(origin, cells[ids])
    end
    return (; elapsed_ms, reachable_elapsed_ms, distance_km,
            reachable_samples=acc.reachable_samples, sample_count=UInt32(plan.samples),
            searches, reused_samples=plan.samples - searches, elapsed_sum_ms=acc.elapsed_sum_ms, kwargs...)
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
            graph.arrival[index] > cutoff - base && continue
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
