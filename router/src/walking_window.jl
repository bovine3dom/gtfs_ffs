function _walking_window_plan(graph::Graph, origin::UInt64, departure_ms::Integer,
                              budget_ms::Integer, window_ms::Integer;
                              step_ms::Integer=60_000, max_walk_s::Integer=3600,
                              walking_index::Union{Nothing,WalkingIndex}=nothing,
                              distance_mode="itinerary")
    mode = _distance_mode(distance_mode)
    ready, _ = query_times(graph, origin, departure_ms, budget_ms)
    1 <= window_ms <= PERIOD || throw(ArgumentError("window must be between one millisecond and one day"))
    step_ms >= 1 || throw(ArgumentError("sample step must be at least one millisecond"))
    step = Int64(min(step_ms, window_ms))
    samples = Int(cld(Int64(window_ms), step))
    samples <= 86_400 || throw(ArgumentError("window must contain at most 86400 samples"))
    budget = UInt32(budget_ms)
    limit = min(_walking_limit(max_walk_s), budget)
    index = isnothing(walking_index) ? WalkingIndex(graph) : walking_index
    index.resolution == graph.resolution && index.cells == graph.h3 ||
        throw(ArgumentError("walking index does not match graph"))
    return (; ready, budget, step, samples, index, limit=UInt32(limit), track_distance=mode == :itinerary)
end

"""Independent walking searches with shared geometry and chronological window aggregation."""
function route_window_walking(graph::Graph, origin::UInt64, departure_ms::Integer,
                              budget_ms::Integer, window_ms::Integer;
                              step_ms::Integer=60_000, max_walk_s::Integer=3600,
                              walking_index::Union{Nothing,WalkingIndex}=nothing,
                              distance_mode="itinerary")
    plan = _walking_window_plan(graph, origin, departure_ms, budget_ms, window_ms;
                                step_ms, max_walk_s, walking_index, distance_mode)
    (; ready, budget, step, samples) = plan
    topology = WalkingTopology(plan.index, plan.limit)
    acc = plan.track_distance ? Dict{UInt64,Tuple{UInt64,UInt32,Float64}}() : Dict{UInt64,Tuple{UInt64,UInt32}}()
    for sample in 0:(samples - 1)
        time = UInt32(Int64(ready) + sample * step)
        point = _walking_route_at(graph, topology, origin, time, time + budget, plan.track_distance)
        _accumulate_walking!(acc, point, time, budget, samples)
    end
    return _finish_walking_window(acc, samples; budget, origin, searches=samples,
                                  reused_samples=0, backend="walking_reference", workers=1)
end

function _accumulate_walking!(acc, point, ready, budget, samples)
    penalty = UInt64(samples) * UInt64(budget)
    for i in eachindex(point.h3)
        cell = point.h3[i]
        total, reached, km = get(acc, cell, (penalty, UInt32(0), NaN))
        total -= UInt64(budget - (point.arrival[i] - ready))
        reached += UInt32(1)
        km = reached == 1 ? point.distance_km[i] :
            km + (point.distance_km[i] - km) * (1 / reached)
        acc[cell] = (total, reached, km)
    end
end

function _finish_walking_window(acc, samples; budget, origin=nothing, kwargs...)
    h3 = sort!(collect(keys(acc)))
    elapsed_sum_ms = [acc[cell][1] for cell in h3]
    reachable_samples = [acc[cell][2] for cell in h3]
    distance_km = valtype(acc) == Tuple{UInt64,UInt32} ? _od_distances(origin, h3) : [acc[cell][3] for cell in h3]
    elapsed_ms = Float64.(elapsed_sum_ms) ./ samples
    reachable_elapsed_ms = [(elapsed_sum_ms[i] - UInt64(samples - reachable_samples[i]) * UInt64(budget)) /
                            reachable_samples[i] for i in eachindex(h3)]
    return (; h3, elapsed_ms, reachable_elapsed_ms, distance_km, reachable_samples,
            sample_count=UInt32(samples), elapsed_sum_ms, kwargs...)
end

function _accumulate_walking!(acc::Dict{UInt64,Tuple{UInt64,UInt32}}, point, ready, budget, samples)
    penalty = UInt64(samples) * UInt64(budget)
    for i in eachindex(point.h3)
        cell = point.h3[i]
        total, reached = get(acc, cell, (penalty, UInt32(0)))
        acc[cell] = (total - UInt64(budget - (point.arrival[i] - ready)), reached + UInt32(1))
    end
end
