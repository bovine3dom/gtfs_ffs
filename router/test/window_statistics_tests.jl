# Independent scalar point queries, retaining chronological samples rather than
# using any window accumulator or grouped-arrival formula as the oracle.
function statistics_points(graph, origin, start, budget, window, step; walking=false, kwargs...)
    points = Dict{UInt64,Vector{Tuple{UInt32,Float64}}}()
    for time in start:step:(start + window - 1)
        ready = time % P
        point = walking ? route_walking(graph, origin, ready, budget; kwargs...) : route_details(graph, origin, ready, budget)
        cells = walking ? point.h3 : graph.h3
        for i in eachindex(cells)
            point.arrival[i] == INF && continue
            push!(get!(points, cells[i], Tuple{UInt32,Float64}[]), (point.arrival[i] - ready, point.distance_km[i]))
        end
    end
    points[origin] = fill((UInt32(0), 0.0), cld(window, step))
    return points
end

function check_statistics(graph, result, points, mode, budget; origin=DEMO_ORIGIN, straight=false)
    cells = hasproperty(result, :h3) ? result.h3 : graph.h3
    ids = findall(!iszero, result.reachable_samples)
    @test cells[ids] == sort!(collect(keys(points)))
    expected = map(cells[ids]) do cell
        samples = points[cell]
        times, km = first.(samples), last.(samples)
        n, total = length(samples), sum(UInt64.(times))
        capped = total + UInt64(result.sample_count - n) * UInt64(budget)
        best, worst = argmin(times), argmax(times)
        elapsed = mode == :min_union ? Float64(times[best]) : mode == :max_intersection ? Float64(times[worst]) :
            mode == :diff_union ? Float64(n < result.sample_count ? budget : times[worst]) - times[best] : capped / result.sample_count
        conditional = mode in (:mean_intersection, :reachable_union) ? total / n : elapsed
        distance = straight ? only(Reachability._od_distances(origin, [cell])) :
            mode in (:min_union, :diff_union) ? km[best] : mode == :max_intersection ? km[worst] : sum(km) / n
        (; elapsed, conditional, distance, n, capped)
    end
    @test result.elapsed_ms[ids] == getproperty.(expected, :elapsed)
    @test result.reachable_elapsed_ms[ids] == getproperty.(expected, :conditional)
    @test isapprox(result.distance_km[ids], getproperty.(expected, :distance); nans=true)
    @test result.reachable_samples[ids] == getproperty.(expected, :n)
    @test result.elapsed_sum_ms[ids] == getproperty.(expected, :capped)
end

@testset "Five window statistics against point queries" begin
    rows = [(1, 2, 0, 40, 9.0), (1, 2, 60, 10, 2.0),
            (1, 3, 0, 10, 8.0), (1, 3, 60, 10, 1.0), (1, 4, 0, 0, 4.0), (1, 5, 60, 60, 5.0)]
    fixtures = [(pack_graph(window_table(rows)), 0, 60, 61, 30),
        (pack_graph(window_table(rows; distances=false)), 0, 60, 61, 30),
        (pack_graph(window_table([(1, 2, 100, 0, 2.0), (2, 3, 110, 30, 3.0)])), 0, 100, 91, 30),
        (pack_graph(window_table(rows)), P - 1, 100, 61, 30),
        (pack_graph(window_table(rows)), 0, 0, 2, 1),
        (pack_graph(window_table(rows)), 0, 60, 1, 30)]
    for (graph, start, budget, window, step) in fixtures
        points = statistics_points(graph, DEMO_ORIGIN, start, budget, window, step)
        for mode in (:mean_intersection, :min_union, :max_intersection, :diff_union, :reachable_union), straight in (false, true)
            options = (; step_ms=step, window_mode=mode, distance_mode=straight ? :straight_line : :itinerary)
            for reuse in (false, true)
                check_statistics(graph, route_window(graph, DEMO_ORIGIN, start, budget, window; options..., reuse), points, mode, budget; straight)
            end
            for workers in (1, 8), chunk_size in (1, 2, 64)
                check_statistics(graph, route_window_cached(graph, DEMO_ORIGIN, start, budget, window; options..., workers, chunk_size), points, mode, budget; straight)
            end
        end
    end
    a, b, c, seconds = WalkingTests.chain(9)
    hop = Int(WalkingTests.walk(a, b).ms)
    graph = pack_graph(WalkingTests.raw_table([a, b, c], [(1, 2, 0, hop, 9.0), (1, 2, 60, hop, 1.0), (2, 3, hop, 0, 3.0)]))
    bare = WalkingIndex(graph)
    prepared = prepare_walking(bare; max_walk_ms=1000seconds)
    offgraph = first(setdiff(WalkingTests.disk(a, 1), [a, b, c]))
    for (origin, limit, index) in ((a, 0, bare), (a, 1000seconds, prepared),
                                  (offgraph, 1000seconds, prepared), (offgraph, 1000seconds + 1, prepared))
        budget = 2hop + 60
        points = statistics_points(graph, origin, 0, budget, 61, 30; walking=true, max_walk_ms=limit, walking_index=bare)
        for mode in (:mean_intersection, :min_union, :max_intersection, :diff_union, :reachable_union), straight in (false, true)
            options = (; step_ms=30, window_mode=mode, distance_mode=straight ? :straight_line : :itinerary, max_walk_ms=limit, walking_index=index)
            check_statistics(graph, route_window_walking(graph, origin, 0, budget, 61; options...), points, mode, budget; origin, straight)
            for (workers, chunk_size) in ((1, 1), (8, 2), (8, 64))
                check_statistics(graph, route_window_walking_cached(graph, origin, 0, budget, 61; options..., workers, chunk_size), points, mode, budget; origin, straight)
            end
        end
    end
end

@testset "Extrema ties, unknown distance, and coverage units" begin
    graph = pack_graph(window_table([(1, 2, 0, 0, 4.0)]))
    plan = (source=Int32(0), ready=UInt32(0), budget=UInt32(100), samples=2, step=30)
    for mode in (:min_union, :max_intersection, :diff_union)
        acc = Reachability._window_accumulator(graph, plan; window_mode=mode)
        for first in 0:1
            @inferred Reachability._accumulate_window!(acc, plan, (first, 1), fill(UInt32(30first), 2), fill(first == 0 ? NaN : 2.0, 2))
        end
        result = Reachability._finish_window(acc, plan; searches=2)
        @test result.elapsed_ms == result.reachable_elapsed_ms == [0, 0]
        @test all(isnan, result.distance_km)
    end
    for mode in (:min_union, :max_intersection, :diff_union), indexed in (false, true), track in (false, true)
        acc = indexed ? Reachability.WalkingOutputAccumulator([DEMO_ORIGIN], 3, UInt32(100), track; window_mode=mode) :
            Reachability._walking_window_accumulator(track, mode)
        for ready in (0, 30, 60)
            point = merge(indexed ? (ids=Int32[1],) : (h3=[DEMO_ORIGIN],),
                (arrival=UInt32[ready], distance_km=[ready == 0 ? NaN : 2.0]))
            @inferred Reachability._accumulate_walking!(acc, point, UInt32(ready), UInt32(100), 3)
        end
        result = Reachability._finish_walking_window(acc, 3; budget=UInt32(100), origin=DEMO_ORIGIN)
        @test result.elapsed_ms == result.reachable_elapsed_ms == [0.0]
        @test result.elapsed_sum_ms == [0]
        @test track ? isnan(only(result.distance_km)) : only(result.distance_km) == 0
    end
    # Hours: partial [.25, missing] -> .75 gap; full [.25, .75] -> .5 gap.
    graph = pack_graph(window_table([(1, 2, 0, 900_000, 9.0),
        (1, 3, 0, 900_000, 8.0), (1, 3, 3_600_000, 2_700_000, 2.0)]))
    for mode in (:max_intersection, :diff_union, :reachable_union), distance in (:itinerary, :straight_line), encoding in ("string", "split")
        result = route_window_cached(graph, DEMO_ORIGIN, 0, 3_600_000, 7_200_000; step_ms=3_600_000, window_mode=mode, distance_mode=distance)
        table = Arrow.Table(Reachability.window_arrow(graph, result, DEMO_ORIGIN, encoding; window_mode=mode))
        cells = encoding == "string" ? parse.(UInt64, table.index; base=16) : UInt64.(table.index_lower) .| (UInt64.(table.index_upper) .<< 32)
        order = indexin(DEMO_CELLS[mode == :max_intersection ? [1, 3] : [1, 2, 3]], cells)
        table = (; (name => getproperty(table, name)[order] for name in propertynames(table))...)
        @test collect(table.value) == (mode == :max_intersection ? [0, .75] : mode == :diff_union ? [0, .75, .5] : [100, 50, 100])
        if mode == :reachable_union
            @test table.elapsed_h == [0, .625, .5]
            @test table.reachable_elapsed_h == [0, .25, .5]
            @test table.reachable_fraction == [1, .5, 1]
            @test distance == :itinerary ? table.distance_km == [0, 9, 5] : table.distance_km[1] == 0
        else
            @test table.elapsed_h == table.reachable_elapsed_h
            @test distance == :itinerary ? collect(table.distance_km) == (mode == :diff_union ? [0, 9, 8] : [0, 2]) : table.distance_km[1] == 0
        end
    end
    for mode in (:max_intersection, :diff_union, :reachable_union), window in (1, 2)
        result = route_window_cached(graph, DEMO_ORIGIN, 0, 0, window; step_ms=1, window_mode=mode)
        table = Arrow.Table(Reachability.window_arrow(graph, result, DEMO_ORIGIN, "string"; window_mode=mode))
        @test table.value == [mode == :reachable_union ? 100 : 0]
        @test table.reachable_samples == table.sample_count == [window]
    end
end
