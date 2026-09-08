function _walking_window_plan(graph::Graph, origin::UInt64, departure_ms::Integer,
                              budget_ms::Integer, window_ms::Integer;
                               step_ms::Integer=60_000, max_walk_ms::Integer=3_600_000,
                              walking_index::Union{Nothing,WalkingIndex}=nothing,
                              distance_mode="itinerary")
    mode = _distance_mode(distance_mode)
    ready, _ = query_times(graph, origin, departure_ms, budget_ms)
    step, samples, _ = _window_times(ready, budget_ms, window_ms, step_ms)
    budget = UInt32(budget_ms)
    limit = min(_walking_limit(max_walk_ms), budget)
    index = isnothing(walking_index) ? WalkingIndex(graph) : walking_index
    index.resolution == graph.resolution && index.cells == graph.h3 ||
        throw(ArgumentError("walking index does not match graph"))
    return (; ready, budget, step, samples, index, limit=UInt32(limit), track_distance=mode == :itinerary)
end

function _walking_window_accumulator(track, window_mode)
    mode = _window_mode(window_mode)
    if mode in (:max_intersection, :diff_union)
        stats = track ? Dict{UInt64,Tuple{UInt64,UInt32,Float64,UInt32,UInt32}}() :
            Dict{UInt64,Tuple{UInt64,UInt32,Nothing,UInt32,UInt32}}()
        return (; stats, best=Val(mode == :diff_union))
    elseif mode == :min_union
        return track ? Dict{UInt64,Tuple{UInt64,UInt32,Float64,UInt32}}() :
            Dict{UInt64,Tuple{UInt64,UInt32,Nothing,UInt32}}()
    end
    return track ? Dict{UInt64,Tuple{UInt64,UInt32,Float64}}() : Dict{UInt64,Tuple{UInt64,UInt32}}()
end

function _accumulate_walking!(acc::NamedTuple{(:stats, :best)}, point, ready, budget, samples)
    stats, best = acc.stats, acc.best isa Val{true}
    D = fieldtype(valtype(stats), 3)
    penalty = UInt64(samples) * UInt64(budget)
    for i in eachindex(point.h3)
        cell = point.h3[i]
        total, reached, km, minimum, maximum = get(stats, cell, (penalty, UInt32(0), D == Nothing ? nothing : NaN, INF, UInt32(0)))
        elapsed = point.arrival[i] - ready
        if best ? elapsed < minimum : (reached == 0 || elapsed > maximum)
            D == Nothing || (km = point.distance_km[i])
        end
        stats[cell] = (total - UInt64(budget - elapsed), reached + UInt32(1), km,
                       min(minimum, elapsed), max(maximum, elapsed))
    end
end

function _finish_walking_window(acc::NamedTuple{(:stats, :best)}, samples; budget, origin=nothing, kwargs...)
    stats = acc.stats
    h3 = sort!(collect(keys(stats)))
    elapsed_sum_ms, reachable_samples = [stats[h][1] for h in h3], [stats[h][2] for h in h3]
    distance_km = fieldtype(valtype(stats), 3) == Nothing ? _od_distances(origin, h3) : [stats[h][3] for h in h3]
    minimum = acc.best isa Val{true} ? [stats[h][4] for h in h3] : nothing
    elapsed_ms, reachable_elapsed_ms = _window_elapsed(elapsed_sum_ms, reachable_samples,
        minimum, [stats[h][5] for h in h3], samples, budget)
    return (; h3, elapsed_ms, reachable_elapsed_ms, distance_km, reachable_samples,
            sample_count=UInt32(samples), elapsed_sum_ms, kwargs...)
end

function _accumulate_walking!(acc::Dict{UInt64,Tuple{UInt64,UInt32,D,UInt32}}, point, ready, budget, samples) where D
    penalty = UInt64(samples) * UInt64(budget)
    for i in eachindex(point.h3)
        cell = point.h3[i]
        total, reached, km, minimum = get(acc, cell, (penalty, UInt32(0), D == Nothing ? nothing : NaN, INF))
        elapsed = point.arrival[i] - ready
        total -= UInt64(budget - elapsed)
        if elapsed < minimum
            minimum = elapsed
            D == Nothing || (km = point.distance_km[i])
        end
        acc[cell] = (total, reached + UInt32(1), km, minimum)
    end
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
    minimum = fieldcount(valtype(acc)) == 4
    straight = valtype(acc) in (Tuple{UInt64,UInt32}, Tuple{UInt64,UInt32,Nothing,UInt32})
    distance_km = straight ? _od_distances(origin, h3) : [acc[cell][3] for cell in h3]
    elapsed_ms = minimum ? Float64[acc[cell][4] for cell in h3] : Float64.(elapsed_sum_ms) ./ samples
    reachable_elapsed_ms = minimum ? copy(elapsed_ms) : [(elapsed_sum_ms[i] - UInt64(samples - reachable_samples[i]) * UInt64(budget)) /
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
