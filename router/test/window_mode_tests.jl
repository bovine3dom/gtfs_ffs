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
            router = WindowKernelRouter(graph, KA.CPU(); batch_size=2)
            result = route_window_kernel!(router, DEMO_ORIGIN, start, budget, window; options...)
            check_minimum(graph, result, expected; straight)
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
    for encoding in ("string", "split"), metric in ("time", "distance_time_quantile"),
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
            @test table.value == table.distance_quantile - table.time_quantile
        end
    end
    for suffix in ("", "&window_h=0"), mode in ("mean_intersection", "min_union")
        path = "$base$suffix&max_walk_h=0"
        @test handler(HTTP.Request("GET", path)).body == handler(HTTP.Request("GET", "$path&window_mode=$mode")).body
    end
    for flag in ("unknown", "", "min_union&window_mode=min_union", "MIN_UNION", "min_union&budget_s=1")
        @test handler(HTTP.Request("GET", "$base&window_mode=$flag")).status == 400
    end
    legacy = make_handler(pack_graph(window_table([(1, 2, 0, 0, 0.0)]; distances=false)))
    for mode in ("mean_intersection", "min_union")
        path = "$base$window&window_mode=$mode&max_walk_h=0&metric=distance_time_quantile"
        @test legacy(HTTP.Request("GET", path)).status == 400
        @test legacy(HTTP.Request("GET", "$path&distance_mode=straight_line")).status == 200
    end
    for backend in ("origin", "catchup", "ka_cpu")
        configured = withenv("ROUTER_BACKEND" => "reference", "ROUTER_WINDOW_BACKEND" => backend) do
            configured_handler(graph)
        end
        for walk in (0, 1), distance in ("itinerary", "straight_line")
            path = "$base$window&window_mode=min_union&max_walk_h=$walk&distance_mode=$distance"
            @test configured(HTTP.Request("GET", path)).body == handler(HTTP.Request("GET", path)).body
        end
    end
    coarse_cells = H3.API.cellToParent.((DEMO_CELLS[1], DEMO_CELLS[6]), 4)
    coarse = pack_graph((from_h3=[coarse_cells[1]], to_h3=[coarse_cells[2]],
        departure_ms=UInt32[0], duration_ms=Int64[0], distance_km=[2.0]))
    coarse_handler = make_handler(coarse)
    dispatch = make_resolution_handler(Dict(graph.resolution => handler, coarse.resolution => coarse_handler))
    socket_test(dispatch) do url, http
        WS.open(url) do ws
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
                    mode in ("min_union", "mean_intersection"), encoding in ("string", "split"),
                    metric in ("time", "distance_time_quantile"), distance in ("itinerary", "straight_line")
                id += 1
                index = "index_lower=$(origin % UInt32)&index_upper=$((origin >> 32) % UInt32)"
                path = "/reachable?$index&departure_h=0&budget_h=$(60/3_600_000)$window&max_walk_h=0&window_mode=$mode&encoding=$encoding&metric=$metric&distance_mode=$distance"
                socket_query(ws, id, path)
                reply = socket_receive(ws)
                @test socket_id(reply) == id
                expected = (origin == coarse_cells[1] ? coarse_handler : handler)(HTTP.Request("GET", path))
                @test reply[5:end] == HTTP.get(http * path).body == expected.body
            end
        end
    end
end
