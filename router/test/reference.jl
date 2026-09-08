# Load into the CPU module with Base.include(Reachability, path).
export route_window, route_window_walking

"""Reference window routing with origin-only grouping and chronological aggregation."""
function route_window(graph::Graph, origin::UInt64, departure_ms::Integer,
                      budget_ms::Integer, window_ms::Integer;
                      step_ms::Integer=60_000, reuse::Bool=true, window_mode=:mean_intersection,
                      distance_mode="itinerary")
    track = _distance_mode(distance_mode) == :itinerary
    plan = _window_plan(graph, origin, departure_ms, budget_ms, window_ms; step_ms, reuse)
    acc = _window_accumulator(graph, plan, track; window_mode)
    distances = !track || isnothing(graph.distance_km) ? nothing : Vector{Float64}(undef, length(graph.h3))
    for group in plan.groups
        first, count = group
        ready = UInt32(Int64(plan.ready) + first * plan.step)
        cutoff = UInt32(Int64(plan.ready) + (first + count - 1) * plan.step + plan.budget)
        arrivals = _route_at(graph, plan.source, ready, cutoff, distances)
        _accumulate_window!(acc, plan, group, arrivals, distances)
    end
    return _finish_window(acc, plan; searches=length(plan.groups), origin, cells=graph.h3)
end

"""Independent walking searches with shared geometry and chronological window aggregation."""
function route_window_walking(graph::Graph, origin::UInt64, departure_ms::Integer,
                              budget_ms::Integer, window_ms::Integer;
                               step_ms::Integer=60_000, max_walk_ms::Integer=3_600_000,
                              walking_index::Union{Nothing,WalkingIndex}=nothing,
                              distance_mode="itinerary", window_mode=:mean_intersection)
    plan = _walking_window_plan(graph, origin, departure_ms, budget_ms, window_ms;
                                 step_ms, max_walk_ms, walking_index, distance_mode)
    (; ready, budget, step, samples) = plan
    topology = WalkingTopology(plan.index, plan.limit)
    acc = _walking_window_accumulator(plan.track_distance, window_mode)
    for sample in 0:(samples - 1)
        time = UInt32(Int64(ready) + sample * step)
        point = _walking_route_at(graph, topology, origin, time, time + budget, plan.track_distance)
        _accumulate_walking!(acc, point, time, budget, samples)
    end
    return _finish_walking_window(acc, samples; budget, origin, searches=samples,
                                  reused_samples=0, backend="walking_reference", workers=1)
end
