@testset "UInt32 time representation" begin
    M = Int64(INF) - 1
    a, b = DEMO_CELLS[1:2]
    row(d, duration) = (from_h3=[a], to_h3=[b], departure_ms=UInt32[d], duration_ms=Int64[duration], distance_km=[1.0])
    for d in (0, P - 1)
        duration = M - P - d
        graph = pack_graph(row(d, duration))
        @test maximum(graph.arrival) == M
        @test_throws ArgumentError pack_graph(row(d, duration + 1))
        for ready in (0, d, d + 1, 48P, M - 1), cutoff in (ready, M)
            departure = big(ready ÷ P) * P + d
            departure < ready && (departure += P)
            expected = departure + duration <= cutoff ? UInt32(departure + duration) : INF
            @test Reachability.next_arrival(graph.schedule_ptr, graph.departure, graph.arrival,
                1, UInt32(ready), UInt32(cutoff)) == expected
        end
    end
    graph = pack_graph(row(0, 10P))
    @test route_cpu(graph, a, 0, M)[graph.node_id[b]] == 10P
    @test Reachability.query_times(graph, a, P - 1, M - P + 1) == (P - 1, M)
    @test_throws ArgumentError Reachability.query_times(graph, a, P - 1, M - P + 2)
    @test Reachability._walking_limit(M) == M
    @test_throws ArgumentError Reachability._walking_limit(INF)
    @test Reachability._window_times(0, 0, M, 1) == (1, M, M - 1)
    @test Reachability._window_times(0, 0, M, typemax(UInt128)) == (M, 1, 0)
    @test_throws ArgumentError Reachability._window_times(0, 2, M, 1)
    @test_throws ArgumentError Reachability._window_times(0, 0, typemax(UInt128), 1)
    late = pack_graph((from_h3=DEMO_CELLS[1:3], to_h3=DEMO_CELLS[2:4],
        departure_ms=UInt32[1, 0, (M - 1000) % P],
        duration_ms=Int64[48P - 1, (M - 1000) - 48P, P], distance_km=ones(3)))
    labels = route_cpu(late, a, 0, M)
    @test labels[late.node_id[DEMO_CELLS[3]]] == M - 1000
    @test labels[late.node_id[DEMO_CELLS[4]]] == INF
    for engine in (route_window, route_window_cached, route_window_walking, route_window_walking_cached)
        options = engine in (route_window_walking, route_window_walking_cached) ? (max_walk_ms=0,) : (;)
        result = engine(late, a, 0, M - 1, 2; step_ms=1, options...)
        cells = hasproperty(result, :h3) ? result.h3 : late.h3
        at = findfirst(==(DEMO_CELLS[3]), cells)
        @test result.reachable_samples[at] == 2
        @test result.elapsed_ms[at] == M - 1000.5
    end
    for mode in (:mean_intersection, :min_union, :max_intersection, :diff_union, :reachable_union), distance in (:itinerary, :straight_line)
        options = (; step_ms=P, window_mode=mode, distance_mode=distance)
        expected = route_window(graph, a, P - 1, P, M - 2P; options...)
        for chunk_size in (1, 64, 257, typemax(UInt128))
            actual = route_window_cached(graph, a, P - 1, P, M - 2P; options..., chunk_size)
            @test actual.elapsed_sum_ms == expected.elapsed_sum_ms
            @test actual.reachable_samples == expected.reachable_samples
            @test isequal(actual.distance_km, expected.distance_km)
        end
        expected_walk = route_window_walking(graph, a, P - 1, P, M - 2P; options..., max_walk_ms=0)
        actual_walk = route_window_walking_cached(graph, a, P - 1, P, M - 2P; options..., max_walk_ms=0)
        @test actual_walk.h3 == expected_walk.h3
        @test actual_walk.elapsed_ms == expected_walk.elapsed_ms
        @test isequal(actual_walk.distance_km, expected_walk.distance_km)
    end
    # Exercise billions of represented samples with one analytical group, not billions of searches.
    n = M ÷ 2
    plan = (source=Int32(0), ready=UInt32(0), budget=UInt32(n), step=1, samples=n)
    acc = Reachability._window_accumulator(graph, plan)
    Reachability._accumulate_window!(acc, plan, (0, n), fill(UInt32(n - 1), length(graph.h3)), nothing)
    @test all(==(n), acc.reachable_samples)
    @test all(==(big(n) * (n - 1) ÷ 2), acc.elapsed_sum_ms)
    handler = make_handler(graph)
    for walk in (0, 1)
        query = "/reachable?index=$(string(a; base=16))&departure_h=0&budget_h=$(M/3_600_000)&max_walk_h=$walk"
        @test handler(HTTP.Request("GET", query * "&window_h=$(2/3_600_000)&step_h=$(1/3_600_000)")).status == 400
    end
    for field in ("budget_h", "max_walk_h", "window_h", "step_h")
        params = Dict("departure_h" => "0", "budget_h" => "0", "window_h" => "48", "step_h" => "25", "max_walk_h" => "0")
        params[field] = "200"
        uri = HTTP.URI("/reachable?index=$(string(a; base=16))&" * join(("$k=$v" for (k, v) in params), '&'))
        @test Reachability.parse_query(uri, graph) isa Tuple
    end
end
