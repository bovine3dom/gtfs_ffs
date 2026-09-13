using Logging

@testset "Packed graph resolution derivation" begin
    rng = MersenneTwister(8136)
    parent_table(t, res) = merge(t, (from_h3=H3.API.cellToParent.(t.from_h3, res),
        to_h3=H3.API.cellToParent.(t.to_h3, res)))
    same_graph(a, b) = all(isequal(getfield(a, f), getfield(b, f)) for f in fieldnames(Graph))
    families = [H3.API.latLngToCell(H3.API.LatLng(deg2rad(51.5), deg2rad(lon)), 7)
                for lon in (-0.1, 0.1, 0.3)]
    cells = reduce(vcat, H3.API.cellToChildren.(families, 8))
    for distances in (false, true), sample in 1:8
        table = (from_h3=rand(rng, cells, 800), to_h3=rand(rng, cells, 800),
            departure_ms=rand(rng, UInt32[0, 10, 20, P - 10, P - 1], 800),
            duration_ms=rand(rng, Int64[0, 10, 20, P, 2P, 7P, Int64(INF) - 2P], 800))
        distances && (table = merge(table, (distance_km=rand(rng, [-0.0, 0.0, 1.0, 7.0], 800),)))
        fine = pack_graph(table)
        snapshot = deepcopy(fine)
        @test coarsen_graph(fine, 8) === fine
        @test_throws ArgumentError coarsen_graph(fine, 9)
        @test_throws ArgumentError coarsen_graph(fine, -1)
        for res in 5:7
            graph = coarsen_graph(fine, res)
            expected = pack_graph(parent_table(table, res))
            @test same_graph(graph, expected)
            @test length(graph.departure) <= length(fine.departure)
            @test any(graph.edge_from .== graph.edge_to)
            for origin in graph.h3, ready in [0, 10, P - 1, rand(rng, 0:(P - 1))]
                budget = rand(rng, 0:8P)
                @test isequal(route_details(graph, origin, ready, budget),
                    route_details(expected, origin, ready, budget))
            end
        end
        @test same_graph(coarsen_graph(coarsen_graph(fine, 7), 6), coarsen_graph(fine, 6))
        @test same_graph(fine, snapshot)
    end
    a, b = cells[1], cells[end]
    # The slow late trip survives only in the second daily copy.
    table = (from_h3=UInt64[a, a], to_h3=UInt64[b, b],
        departure_ms=UInt32[0, P - 1], duration_ms=Int64[1, 2P], distance_km=[1.0, 2.0])
    fine = pack_graph(table)
    @test fine.departure == UInt32[0, P, 2P - 1]
    @test same_graph(coarsen_graph(fine, 5), pack_graph(parent_table(table, 5)))
    # Equal arrivals retain the latest departure; identical times retain the shortest km.
    ties = (from_h3=cells[1:4], to_h3=fill(b, 4), departure_ms=UInt32[10, 20, 20, 20],
        duration_ms=Int64[20, 10, 10, 10], distance_km=[0.0, 9.0, 2.0, 4.0])
    tied = coarsen_graph(pack_graph(ties), 7)
    @test tied.departure == UInt32[20, P + 20]
    @test tied.distance_km == [2.0, 2.0]
    @test same_graph(tied, pack_graph(parent_table(ties, 7)))
    invalid = merge(table, (duration_ms=Int64[-1, -2],))
    isolated = @test_logs (:warn, r"Skipping 2 of 2") pack_graph(invalid; skip_invalid_durations=true)
    coarse = coarsen_graph(isolated, 6)
    @test coarse.h3 == sort!(unique(H3.API.cellToParent.(isolated.h3, 6)))
    @test coarse.out_ptr == ones(Int32, length(coarse.h3) + 1)
    @test isempty(coarse.edge_to)
    empty = Graph(UInt64[], Dict{UInt64,Int32}(), Int32[1], Int32[], Int32[],
        Int32[1], UInt32[], UInt32[], 8, nothing)
    @test coarsen_graph(empty, 5).resolution == 5
    @test isempty(coarsen_graph(empty, 5).h3)
    @test isnothing(coarsen_graph(empty, 0).distance_km)
    repaired = pack_graph(table; badajoz_shuttle=true)
    extra = Reachability._badajoz_shuttle(8)
    for res in 5:7
        graph = coarsen_graph(repaired, res)
        @test same_graph(graph, pack_graph(parent_table(map(vcat, table, extra), res)))
        from, to = H3.API.cellToParent.([first(extra.from_h3), first(extra.to_h3)], res)
        details = route_details(graph, from, 28_800_000, 900_000)
        @test details.arrival[graph.node_id[to]] == 29_700_000
        @test details.distance_km[graph.node_id[to]] == 13.88
    end
end

@testset "Automatic resolution loader" begin
    cells = [H3.API.latLngToCell(H3.API.LatLng(deg2rad(51.5), deg2rad(lon)), 8)
             for lon in (-0.1, 0.3)]
    table = (from_h3=UInt64[cells[1]], to_h3=UInt64[cells[2]], departure_ms=UInt32[0],
        duration_ms=Int64[60_000], distance_km=[3.0])
    query(res; network="") = "/reachable?index=$(H3.API.h3ToString(H3.API.cellToParent(cells[1], res)))&departure_h=0&budget_h=1&max_walk_h=0&encoding=string" * network
    mktempdir() do dir
        fine_path, override_path, other_path = [joinpath(dir, name) for name in
            ("rail_res8.arrow", "rail_res6.arrow", "other_res8.arrow")]
        Arrow.write(fine_path, map(vcat, table, merge(table, (duration_ms=Int64[-1],))))
        Arrow.write(other_path, merge(table, (duration_ms=Int64[180_000],)))
        explicit = merge(table, (from_h3=H3.API.cellToParent.(table.from_h3, 6),
            to_h3=H3.API.cellToParent.(table.to_h3, 6), duration_ms=Int64[120_000], distance_km=[9.0]))
        Arrow.write(override_path, explicit)
        before = read(fine_path)
        logger = Test.TestLogger()
        handler = with_logger(logger) do
            load_handlers([fine_path, override_path, other_path])
        end
        @test count(r -> r.message == "Supplied graph", logger.logs) == 3
        @test count(r -> r.message == "Derived graph", logger.logs) == 5
        @test count(r -> r.message == "Added Elvas-Badajoz fantasy rail shuttle", logger.logs) == 3
        @test count(r -> r.level == Logging.Warn && occursin("Skipping 1 of 2", r.message), logger.logs) == 1
        @test count(r -> occursin("Opening Arrow file", string(r.message)) && startswith(string(r.message), "Startup:"), logger.logs) == 3
        for res in 5:8
            response = handler(HTTP.Request("GET", query(res)))
            @test response.status == 200
            result = Arrow.Table(response.body)
            @test sort(collect(result.elapsed_h)) == [0, (res == 6 ? 2 : 1) / 60]
            @test sort(collect(result.distance_km)) == [0, res == 6 ? 9.0 : 3.0]
            other = handler(HTTP.Request("GET", query(res; network="&network=other")))
            @test other.status == 200
            @test sort(collect(Arrow.Table(other.body).elapsed_h)) == [0, 3 / 60]
        end
        for res in 5:7
            raw = map(vcat, table, Reachability._badajoz_shuttle(8))
            raw = merge(raw, (from_h3=H3.API.cellToParent.(raw.from_h3, res),
                to_h3=H3.API.cellToParent.(raw.to_h3, res)))
            expected = make_handler(res == 6 ? pack_graph(explicit; badajoz_shuttle=true) : pack_graph(raw))
            for walk in ("0", "0.001"), window in ("0", "0.03333333333333333"), metric in ("time", "time_distance_quantile")
                path = replace(query(res), "max_walk_h=0" => "max_walk_h=$walk") * "&window_h=$window&metric=$metric"
                response = handler(HTTP.Request("GET", path))
                @test response.status == 200
                @test response.body == expected(HTTP.Request("GET", path)).body
                ignored = handler(HTTP.Request("GET", path * "&coarseness=garbage"))
                @test ignored.status == 200
                @test ignored.body == response.body
                @test filter(p -> first(p) != "X-Router-Queue-Wait-Ms", ignored.headers) ==
                      filter(p -> first(p) != "X-Router-Queue-Wait-Ms", response.headers)
                @test parse(Float64, HTTP.header(ignored, "X-Router-Queue-Wait-Ms")) >= 0
            end
        end
        @test handler(HTTP.Request("GET", query(4))).status == 400
        @test handler(HTTP.Request("GET", query(6; network="&network=absent"))).status == 400
        @test handler(HTTP.Request("GET", query(5) * "&metric=accessible_population")).status == 400
        population_path = joinpath(dir, "population.arrow")
        sibling = first(filter(!=(cells[1]), H3.API.cellToChildren(H3.API.cellToParent(cells[1], 7), 8)))
        Arrow.write(population_path, (h3=UInt64[cells[1], sibling], population=[10.0, 20.0]))
        populated = with_logger(NullLogger()) do
            load_handlers([fine_path]; population_path)
        end
        for res in 5:8
            response = populated(HTTP.Request("GET", query(res) * "&metric=accessible_population"))
            @test response.status == 200
            @test collect(Arrow.Table(response.body).value) == [res == 8 ? 10.0 : 30.0]
        end
        reversed = with_logger(NullLogger()) do
            load_handlers([override_path, other_path, fine_path])
        end
        for res in 5:8
            @test reversed(HTTP.Request("GET", query(res))).body == handler(HTTP.Request("GET", query(res))).body
        end
        @test_throws r"duplicate graph" load_handlers([fine_path, fine_path])
        @test_throws r"duplicate graph" load_handlers([fine_path, override_path, override_path])
        mismatch = joinpath(dir, "wrong_res8.arrow")
        Arrow.write(mismatch, explicit)
        logger = Test.TestLogger()
        with_logger(logger) do
            @test_throws r"does not match graph resolution 6" load_handlers([fine_path, mismatch])
        end
        @test !any(r -> r.message == "Derived graph", logger.logs)
        @test read(fine_path) == before
        @test sort(readdir(dir)) == sort(basename.([fine_path, override_path, other_path, mismatch, population_path]))
    end
end
