@testset "Floating-hour transport boundary" begin
    graph = pack_graph(fixture_table())
    parse_hours(query) = Reachability.parse_query(HTTP.URI("/reachable?index=$(string(DEMO_ORIGIN; base=16))&$query"), graph)
    query = parse_hours("departure_h=2.35e1&budget_h=.25&window_h=1&step_h=2.5e-1&max_walk_h=.25")
    @test query[2:3] == (84_600_000, 900_000)
    @test query[5:6] == (3_600_000, 900_000)
    @test query[8] == 900_000
    defaults = parse_hours("departure_h=0&budget_h=168")
    @test defaults[5:6] == (0, 60_000)
    @test defaults[8] == 3_600_000
    for field in ("departure_h", "budget_h", "window_h", "step_h", "max_walk_h")
        base = Dict("departure_h" => "0", "budget_h" => "1", "window_h" => "1")
        for bad in ("", "NaN", "Inf", "-Inf", "1e309", "-0.1", "-1e-999", "1x", "1%0A", "1%20", "08:00:00")
            params = merge(base, Dict(field => bad))
            @test_throws ArgumentError parse_hours(join(("$k=$v" for (k, v) in params), '&'))
        end
        params = merge(base, Dict(field => field in ("budget_h", "max_walk_h") ? "168.000001" : "24.000001"))
        @test_throws ArgumentError parse_hours(join(("$k=$v" for (k, v) in params), '&'))
        @test_throws ArgumentError parse_hours("departure_h=0&budget_h=1&window_h=1&$field=1&$field=2")
    end
    for legacy in ("departure=08:00:00", "budget_s=3600", "window_s=60", "step_s=1", "max_walk_s=0")
        @test_throws ArgumentError parse_hours("departure_h=0&budget_h=1&$legacy")
    end
    @test_throws ArgumentError parse_hours("departure_h=24&budget_h=0")
    @test_throws ArgumentError parse_hours("departure_h=23.99999999&budget_h=0")
    @test parse_hours("departure_h=$(23 + 3599.999 / 3600)&budget_h=0")[2] == P - 1
    for extra in ("window_h=1e-10", "window_h=1e-999", "window_h=1&step_h=1e-10", "step_h=1", "window_h=0&step_h=1", "window_h=1&step_h=0")
        @test_throws ArgumentError parse_hours("departure_h=0&budget_h=1&$extra")
    end
    tiny = parse_hours("departure_h=0&budget_h=1e-10&max_walk_h=1e-10")
    @test tiny[3] == tiny[8] == 0
    for ms in (0.5, 1.5, 2.5, 1, 3, 900_000)
        @test Reachability._hours_ms(ms / 3_600_000, "budget_h", 168) == round(Int, ms, RoundNearest)
    end
    @test parse_hours("departure_h=0&budget_h=1&window_h=$(1/3_600_000)&step_h=24")[5:6] == (1, P)
    @test parse_hours("departure_h=0&budget_h=1&max_walk_h=$(1/3_600_000)")[8] == 1
    @test parse_hours("departure_h=0&budget_h=1&window_h=.024&step_h=$(1/3_600_000)")[5:6] == (86_400, 1)
    @test parse_hours("departure_h=0&budget_h=1&window_h=24&step_h=$(1/3600)")[5:6] == (P, 1000)
    handler = make_handler(graph; window_route=error, walking_window_route=error)
    for window in (1, 86_401/3_600_000)
        query = "departure_h=0&budget_h=1&window_h=$window&step_h=$(1/3_600_000)"
        @test_throws r"at most 86400 samples" parse_hours(query)
        for walk in (0, 1)
            response = handler(HTTP.Request("GET", "/reachable?index=$(string(DEMO_ORIGIN; base=16))&$query&max_walk_h=$walk"))
            @test response.status == 400
        end
    end
end

@testset "All departures before ranks" begin
    # B is partial, C never reachable, D/E complete. B must not affect either rank.
    graph = pack_graph(distance_table([(1, 2, 60, 70, 100.0), (1, 3, 1000, 0, 1.0),
        (1, 4, 0, 80, 50.0), (1, 4, 60, 80, 50.0), (1, 5, 0, 90, 20.0), (1, 5, 60, 90, 20.0)]))
    raw = route_window_cached(graph, DEMO_ORIGIN, 0, 100_000, 61_000; step_ms=60_000)
    @test raw.reachable_samples[graph.node_id[DEMO_CELLS[2]]] == 1
    @test raw.reachable_samples[graph.node_id[DEMO_CELLS[3]]] == 0
    handler = make_handler(graph)
    for mode in ("itinerary", "straight_line"), encoding in ("string", "split"), metric in ("time", "distance_time_quantile")
        path = "/reachable?index=$(string(DEMO_ORIGIN; base=16))&departure_h=0&budget_h=$(100/3600)&max_walk_h=0&encoding=$encoding&metric=$metric&distance_mode=$mode"
        response = handler(HTTP.Request("GET", "$path&window_h=$(61/3600)&step_h=$(1/60)"))
        @test response.status == 200
        table = Arrow.Table(response.body)
        ids = encoding == "string" ? parse.(UInt64, table.index; base=16) : UInt64.(table.index_lower) .| (UInt64.(table.index_upper) .<< 32)
        @test ids == sort(DEMO_CELLS[[1, 4, 5]])
        @test all(==(1), table.reachable_fraction)
        @test table.reachable_samples == table.sample_count == fill(2, 3)
        @test table.elapsed_h == table.reachable_elapsed_h
        @test eltype(table.elapsed_h) == eltype(table.reachable_elapsed_h) == Float64
        @test all(!endswith(string(f), "_ms") for f in propertynames(table))
        if metric == "time"
            @test table.value == table.elapsed_h
        else
            @test table.time_quantile == Reachability.normalized_ranks(table.elapsed_h)
            @test table.distance_quantile == Reachability.normalized_ranks(table.distance_km)
            @test sort(collect(table.time_quantile)) == [0, 0.5, 1]
        end
        point = Arrow.Table(handler(HTTP.Request("GET", path)).body)
        single = Arrow.Table(handler(HTTP.Request("GET", "$path&window_h=$(1/3_600_000)")).body)
        @test all(isequal(getproperty(point, f), getproperty(single, f)) for f in propertynames(point))
    end
    overnight = handler(HTTP.Request("GET", "/reachable?index=$(string(DEMO_ORIGIN; base=16))&departure_h=23.5&budget_h=24&window_h=1&step_h=.25&max_walk_h=0"))
    @test overnight.status == 200
    @test all(==(4), Arrow.Table(overnight.body).sample_count)
end
