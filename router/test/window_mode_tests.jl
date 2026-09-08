# Independent point queries: never derive a minimum from a window engine's means.
function minimum_points(graph, origin, start, budget, window, step; walking=false, kwargs...)
    expected = Dict{UInt64,Tuple{Float64,UInt32,Float64,UInt64}}()
    samples = cld(window, step)
    for time in start:step:(start + window - 1)
        ready = time % P
        point = walking ? route_walking(graph, origin, ready, budget; kwargs...) : route_details(graph, origin, ready, budget)
        cells = walking ? point.h3 : graph.h3
        for i in eachindex(cells)
            point.arrival[i] == INF && continue
            elapsed = Float64(point.arrival[i] - ready)
            old, count, km, total = get(expected, cells[i], (Inf, UInt32(0), NaN, UInt64(samples * budget)))
            expected[cells[i]] = (min(old, elapsed), count + UInt32(1),
                elapsed < old ? point.distance_km[i] : km, total - UInt64(budget - elapsed))
        end
    end
    return expected
end

function check_minimum(graph, actual, expected; straight=false, origin=DEMO_ORIGIN)
    cells = hasproperty(actual, :h3) ? actual.h3 : graph.h3
    ids = findall(!iszero, actual.reachable_samples)
    @test Set(cells[ids]) == Set(keys(expected))
    for i in ids
        elapsed, count, km, total = expected[cells[i]]
        @test actual.elapsed_ms[i] == actual.reachable_elapsed_ms[i] == elapsed
        @test actual.reachable_samples[i] == count
        @test actual.elapsed_sum_ms[i] == total
        @test isequal(actual.distance_km[i], straight ? only(Reachability._od_distances(origin, [cells[i]])) : km)
    end
end

@testset "Minimum-union window engines" begin
    rows = [(1, 2, 0, 40, 9.0), (1, 2, 60, 10, 2.0),
            (1, 3, 0, 10, 8.0), (1, 3, 60, 10, 1.0),
            (1, 4, 0, 0, 4.0), (1, 5, 60, 60, 5.0), (1, 6, 300, 0, 6.0)]
    fixtures = [(pack_graph(window_table(rows)), 0, 60, 61, 30),
        (pack_graph(window_table([(1, 2, 100, 0, 2.0), (2, 3, 110, 30, 3.0)])), 0, 100, 91, 30),
        (pack_graph(window_table([(1, 2, 10, 20, 1.0), (2, 3, 40, 5, 2.0)])), P - 10, 100, 31, 10),
        (pack_graph(window_table([(1, 2, 0, 0, 2.0)])), 0, 0, 2, 1),
        (pack_graph(window_table(rows; distances=false)), 0, 60, 61, 30),
        (pack_graph(window_table(rows)), P - 1, 7P, P, P ÷ 4),
        (pack_graph(window_table([])), 0, 0, 61, 30)]
    rng = MersenneTwister(409)
    for _ in 1:12
        random_rows = [(rand(rng, 1:6), rand(rng, 1:6), rand(rng, (0, 30, 60, P - 10)),
                        rand(rng, (0, 10, 50, P)), rand(rng) * 10) for _ in 1:20]
        push!(fixtures, (pack_graph(window_table(random_rows)), rand(rng, (0, P - 10)), rand(rng, (0, 60, P)), 91, 30))
    end
    for (graph, start, budget, window, step) in fixtures
        expected = minimum_points(graph, DEMO_ORIGIN, start, budget, window, step)
        for distance_mode in (:itinerary, :straight_line)
            straight = distance_mode == :straight_line
            options = (; step_ms=step, window_mode=:min_union, distance_mode)
            for reuse in (false, true)
                result = route_window(graph, DEMO_ORIGIN, start, budget, window; options..., reuse)
                check_minimum(graph, result, expected; straight)
            end
            for workers in (1, 4), chunk_size in (1, 2, 64)
                result = route_window_cached(graph, DEMO_ORIGIN, start, budget, window; options..., workers, chunk_size)
                check_minimum(graph, result, expected; straight)
            end
        end
    end
    graph = first(fixtures)[1]
    result = route_window(graph, DEMO_ORIGIN, 0, 60, 61; step_ms=30, window_mode="min_union")
    @test result.elapsed_ms[graph.node_id[DEMO_CELLS[2]]] == 10 # later absolute arrival wins
    @test result.distance_km[graph.node_id[DEMO_CELLS[3]]] == 8 # earliest equal-duration sample, not shortest km
    grouped = route_window(fixtures[2][1], DEMO_ORIGIN, 0, 100, 91; step_ms=30, window_mode=:min_union)
    @test grouped.searches == 1
    @test grouped.elapsed_ms[fixtures[2][1].node_id[DEMO_CELLS[3]]] == 50
    plan = Reachability._window_plan(graph, DEMO_ORIGIN, 0, 60, 61; step_ms=30)
    @test isnothing(Reachability._window_accumulator(graph, plan).elapsed_min_ms)
    acc = Reachability._window_accumulator(graph, plan, false; window_mode=:min_union)
    @test isnothing(acc.distance_km)
    state = Reachability._catchup_workspace(graph, 2, false)
    @test all(isnothing, (state.saved_distances, state.connections, state.seen))
    for engine in (route_window, route_window_cached, route_window_walking, route_window_walking_cached)
        @test_throws ArgumentError engine(graph, DEMO_ORIGIN, 0, 60, 61; window_mode=:unknown)
        @test_throws ArgumentError engine(graph, DEMO_ORIGIN, 0, 60, 61; window_mode=123)
    end
    overflow = pack_graph(window_table([(1, 2, 0, 1, 1e308), (2, 3, 1, 1, 1e308)]))
    for engine in (route_window, route_window_cached, route_window_walking, route_window_walking_cached)
        @test_throws r"accumulated route distance" engine(overflow, DEMO_ORIGIN, 0, 10, 1; window_mode=:min_union)
        @test engine(overflow, DEMO_ORIGIN, 0, 10, 1; window_mode=:min_union, distance_mode=:straight_line).sample_count == 1
    end
end

@testset "Minimum-union walking representations" begin
    a, b, c, seconds = WalkingTests.chain(9)
    cells = [a, b, c]
    hop = Int(WalkingTests.walk(a, b).ms)
    rows = [(1, 2, 0, hop, 9.0), (1, 2, 60, hop, 1.0), (2, 2, hop, 0, 2.0),
            (2, 3, hop, 0, 3.0), (1, 2, P - 1, 0, 5.0)]
    for distances in (true, false)
        graph = pack_graph(WalkingTests.raw_table(cells, rows; distances))
        bare = WalkingIndex(graph)
        prepared = prepare_walking(bare; max_walk_ms=1000seconds)
        offgraph = first(setdiff(WalkingTests.disk(a, 1), cells))
        for origin in (a, offgraph), limit in (0, 1000seconds, 1000seconds + 1),
                (start, budget) in ((0, hop + 60), (P - 1, 2hop))
            expected = minimum_points(graph, origin, start, budget, 61, 30;
                walking=true, max_walk_ms=limit, walking_index=bare)
            for distance_mode in (:itinerary, :straight_line)
                options = (; step_ms=30, max_walk_ms=limit, distance_mode, window_mode=:min_union)
                check_minimum(graph, route_window_walking(graph, origin, start, budget, 61;
                    options..., walking_index=bare), expected; straight=distance_mode == :straight_line, origin)
                for walking_index in (bare, prepared), workers in (1, 4), chunk_size in (1, 2, 64)
                    actual = route_window_walking_cached(graph, origin, start, budget, 61;
                        options..., walking_index, workers, chunk_size)
                    check_minimum(graph, actual, expected; straight=distance_mode == :straight_line, origin)
                end
            end
        end
    end
    # An unknown winning distance stays unknown even when a later tied sample has km.
    for indexed in (false, true), track in (false, true)
        acc = indexed ? Reachability.WalkingOutputAccumulator([a], 3, UInt32(100), track; window_mode=:min_union) :
            Reachability._walking_window_accumulator(track, :min_union)
        for (ready, elapsed, km) in ((0, 10, NaN), (30, 10, 1.0), (60, 20, 0.0))
            point = merge(indexed ? (ids=Int32[1],) : (h3=[a],),
                (arrival=UInt32[ready + elapsed], distance_km=[km]))
            Reachability._accumulate_walking!(acc, point, UInt32(ready), UInt32(100), 3)
        end
        result = Reachability._finish_walking_window(acc, 3; budget=UInt32(100), origin=a)
        @test result.elapsed_ms == result.reachable_elapsed_ms == [10.0]
        @test result.elapsed_sum_ms == [40]
        @test track ? isnan(only(result.distance_km)) : only(result.distance_km) == 0
    end
end

@testset "Runtime window mode HTTP and WebSocket" begin
    graph = pack_graph(window_table([(1, 2, 0, 40, 9.0), (1, 2, 60, 10, 2.0),
        (1, 3, 0, 0, 8.0), (1, 4, 60, 60, 1.0), (1, 5, 300, 0, 5.0)]))
    handler = make_handler(graph)
    base = "/reachable?index=$(string(DEMO_ORIGIN; base=16))&departure_h=0&budget_h=$(60/3_600_000)"
    window = "&window_h=$(61/3_600_000)&step_h=$(30/3_600_000)"
    for encoding in ("string", "split"), metric in ("time", "time_distance_quantile"),
            distance_mode in ("itinerary", "straight_line"), walk in (0, 1)
        path = "$base$window&encoding=$encoding&metric=$metric&distance_mode=$distance_mode&max_walk_h=$walk"
        default = handler(HTTP.Request("GET", path))
        @test default.body == handler(HTTP.Request("GET", "$path&window_mode=mean_intersection")).body
        response = handler(HTTP.Request("GET", "$path&window_mode=min_union"))
        @test response.status == 200
        @test HTTP.header(response, "X-Router-Window-Mode") == "min_union"
        table = Arrow.Table(response.body)
        @test length(table.value) == 4
        @test sort(collect(table.reachable_samples)) == UInt32[1, 1, 3, 3]
        @test all(==(3), table.sample_count)
        @test table.reachable_fraction == table.reachable_samples ./ 3
        @test table.elapsed_h == table.reachable_elapsed_h
        @test sort(collect(table.elapsed_h)) == sort([0.0, 0.0, 10/3_600_000, 60/3_600_000])
        @test eltype(table.elapsed_h) == Float64
        if metric == "time"
            @test table.value == table.elapsed_h
        else
            @test table.time_quantile == Reachability.normalized_ranks(table.elapsed_h)
            @test table.distance_quantile == Reachability.normalized_ranks(table.distance_km)
            @test table.value == table.time_quantile - table.distance_quantile
        end
    end
    for flag in ("unknown", "", "min_union&window_mode=min_union", "MIN_UNION", "min_union&budget_s=1",
                  "max_intersection&window_mode=diff_union", "reachable_union&window_mode=reachable_union")
        @test handler(HTTP.Request("GET", "$base$window&window_mode=$flag")).status == 400
    end
    for suffix in ("", "&window_h=1&step_h=0"), flag in ("unknown&window_mode=", "min_union&budget_s=1", "reachable_union&window_mode=reachable_union")
        @test handler(HTTP.Request("GET", "$base$suffix&window_mode=$flag")).status == 400
    end
    legacy = make_handler(pack_graph(window_table([(1, 2, 0, 0, 0.0)]; distances=false)))
    for mode in ("mean_intersection", "min_union", "max_intersection", "diff_union", "diff_intersection")
        path = "$base$window&window_mode=$mode&max_walk_h=0&metric=time_distance_quantile"
        @test legacy(HTTP.Request("GET", path)).status == 400
        @test legacy(HTTP.Request("GET", "$path&distance_mode=straight_line")).status == 200
    end
    for target in (handler, legacy), distance in ("itinerary", "straight_line")
        response = target(HTTP.Request("GET", "$base$window&window_mode=reachable_union&metric=time_distance_quantile&distance_mode=$distance"))
        @test response.status == 400
        @test occursin("reachable_union is incompatible with time_distance_quantile", String(response.body))
    end
    for mode in ("reachable_union", "diff_intersection", "unknown", "")
        path = "$base&window_h=1&step_h=0&window_mode=$mode&metric=time_distance_quantile"
        @test legacy(HTTP.Request("GET", path)).status == 400
        @test legacy(HTTP.Request("GET", "$path&distance_mode=straight_line")).status == 200
    end
    coarse_cells = H3.API.cellToParent.((DEMO_CELLS[1], DEMO_CELLS[6]), 4)
    coarse = pack_graph((from_h3=[coarse_cells[1]], to_h3=[coarse_cells[2]],
        departure_ms=UInt32[0], duration_ms=Int64[0], distance_km=[2.0]))
    coarse_handler = make_handler(coarse)
    dispatch = make_resolution_handler(Dict(graph.resolution => handler, coarse.resolution => coarse_handler))
    socket_test(dispatch) do url, http
        socket_open(url) do ws
            for (id, mode) in enumerate(("min_union", "mean_intersection", "secret", "min_union&window_mode=min_union", "min_union"))
                path = "$base$window&max_walk_h=0&window_mode=$mode"
                socket_query(ws, id, path)
                reply = socket_receive(ws)
                if id in (3, 4)
                    error = JSON.parse(reply)
                    @test error["id"] == id
                    @test error["type"] == "error"
                    @test !occursin("secret", error["message"])
                else
                    @test socket_id(reply) == id
                    @test reply[5:end] == HTTP.get(http * path).body == handler(HTTP.Request("GET", path)).body
                end
            end
            id = 5
            for origin in (DEMO_ORIGIN, coarse_cells[1], DEMO_CELLS[7]),
                    mode in ("min_union", "mean_intersection", "max_intersection", "diff_union", "diff_intersection", "reachable_union"), encoding in ("string", "split"),
                    metric in ("time", "time_distance_quantile"), distance in ("itinerary", "straight_line"), walk in (0, 1)
                id += 1
                index = "index_lower=$(origin % UInt32)&index_upper=$((origin >> 32) % UInt32)"
                path = "/reachable?$index&departure_h=0&budget_h=$(60/3_600_000)$window&max_walk_h=$walk&window_mode=$mode&encoding=$encoding&metric=$metric&distance_mode=$distance"
                socket_query(ws, id, path)
                reply = socket_receive(ws)
                if mode == "reachable_union" && metric == "time_distance_quantile"
                    @test JSON.parse(reply)["id"] == id
                    @test JSON.parse(reply)["type"] == "error"
                    @test (origin == coarse_cells[1] ? coarse_handler : handler)(HTTP.Request("GET", path)).status == 400
                    continue
                end
                @test socket_id(reply) == id
                expected = (origin == coarse_cells[1] ? coarse_handler : handler)(HTTP.Request("GET", path))
                @test reply[5:end] == HTTP.get(http * path).body == expected.body
                table = Arrow.Table(expected.body)
                if mode == "reachable_union"
                    @test table.value == table.reachable_fraction == table.reachable_samples ./ table.sample_count
                elseif metric == "time_distance_quantile"
                    @test table.time_quantile == Reachability.normalized_ranks(table.elapsed_h)
                    @test table.distance_quantile == Reachability.normalized_ranks(table.distance_km)
                    @test table.value == table.time_quantile - table.distance_quantile
                end
                mode in ("mean_intersection", "max_intersection", "diff_intersection") && @test all(table.reachable_samples .== table.sample_count)
            end
            # Rotate zero forms and resolutions rather than multiplying the output-option matrix.
            zeros = ("step_h=1", "window_h=0&step_h=1", "window_h=1&step_h=0",
                "window_h=0&step_h=0", "window_h=0.0&step_h=1", "window_h=1&step_h=0e0",
                "window_h=0e0&step_h=0.0", "window_h=1193&step_h=0")
            modes = ("mean_intersection", "min_union", "max_intersection", "diff_union", "diff_intersection", "reachable_union", "unknown", "")
            id = 0xf0000000
            for encoding in ("string", "split"), metric in ("time", "time_distance_quantile"),
                    distance in ("itinerary", "straight_line"), walk in (0, 1)
                for (i, mode) in enumerate(modes)
                    origin = isodd(i) ? DEMO_ORIGIN : coarse_cells[1]
                    target = isodd(i) ? handler : coarse_handler
                    index = encoding == "string" ? "index=$(string(origin; base=16))" :
                        "index_lower=$(origin % UInt32)&index_upper=$((origin >> 32) % UInt32)"
                    suffix = zeros[mod1(Int(id), length(zeros))]
                    mode == "diff_intersection" && (suffix = walk == 0 ? "window_h=1&step_h=0" : "window_h=0&step_h=1")
                    times = startswith(suffix, "window_h=1193") ? "departure_h=12&budget_h=3" : "departure_h=0&budget_h=$(60/3_600_000)"
                    path = "/reachable?$index&$times&encoding=$encoding&metric=$metric&distance_mode=$distance&max_walk_h=$walk"
                    expected = target(HTTP.Request("GET", path))
                    path *= "&$suffix&window_mode=$mode"
                    response = HTTP.get(http * path)
                    id += UInt32(1)
                    socket_query(ws, id, path)
                    reply = socket_receive(ws)
                    table = Arrow.Table(response.body)
                    @test expected.status == response.status == 200 && socket_id(reply) == id &&
                        reply[5:end] == response.body == expected.body &&
                        isempty(HTTP.header(response, "X-Router-Window-Strategy")) &&
                        !hasproperty(table, :sample_count) && !hasproperty(table, :reachable_fraction) &&
                        (metric != "time" || table.value == table.elapsed_h)
                end
            end
            for field in ("window_h", "step_h"), bad in ("-1", "-0", "NaN", "Inf", "bad", "1e-999", "1e-10", "1200")
                other = field == "window_h" ? "step_h" : "window_h"
                path = "$base&$field=$bad&$other=0&window_mode=unknown"
                id += UInt32(1)
                socket_query(ws, id, path)
                error = JSON.parse(socket_receive(ws))
                @test HTTP.get(http * path; status_exception=false).status == 400 &&
                    error["id"] == id && error["type"] == "error"
            end
        end
    end
end
