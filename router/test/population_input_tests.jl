@testset "Population CLI" begin
    @test parse_cli(["alpha_res5.arrow", "beta_res6.arrow"]) ==
        (paths=["alpha_res5.arrow", "beta_res6.arrow"], population_path="")
    for paths in (["--demo"], ["alpha_res5.arrow", "beta_res6.arrow"]),
        option in (["--population", "data/kontur_h3.arrow"], ["--population=data/kontur_h3.arrow"]),
        position in 0:length(paths)
        args = [paths[1:position]; option; paths[position + 1:end]]
        @test parse_cli(args) == (; paths, population_path="data/kontur_h3.arrow")
        @test args == [paths[1:position]; option; paths[position + 1:end]]
    end
    for args in (["--population"], ["--population="], ["--population", ""],
                 ["--population", "--demo"], ["--population", "--population=x"],
                 ["--population=x", "--population", "y"],
                 ["--population", "x", "--population=y"],
                 ["--population=x", "--population=x"])
        @test_throws ArgumentError parse_cli(["missing_res5.arrow"; args])
    end
    @test_throws ErrorException load_handlers(String[]; population_path="missing.arrow")
    @test_throws ErrorException load_handlers(["--demo", "missing_res5.arrow"];
                                              population_path="missing.arrow")
    handler = load_handlers(["--demo"])
    query = "/reachable?index=$(string(DEMO_ORIGIN; base=16))&departure_h=0&budget_h=0&max_walk_h=0"
    @test handler(HTTP.Request("GET", query)).status == 200
    @test handler(HTTP.Request("GET", query * "&metric=time_distance_quantile&distance_mode=straight_line")).status == 200
    @test handler(HTTP.Request("GET", query * "&metric=accessible_population")).status == 400
end

@testset "Population radius validation is metric-specific" begin
    cell = first(H3.API.cellToChildren(DEMO_ORIGIN, 8))
    graph = pack_graph((from_h3=[cell], to_h3=[cell], departure_ms=UInt32[0], duration_ms=Int64[0]))
    handler = make_handler(graph; population=Reachability._population([cell], [1.0]))
    for metric in ("time", "time_distance_quantile", "accessible_population"), window in (0, 0.01)
        path = "/reachable?index=$(string(cell; base=16))&departure_h=0&budget_h=0&max_walk_h=0&distance_mode=straight_line&metric=$metric&window_h=$window"
        expected = handler(HTTP.Request("GET", path))
        @test expected.status == 200
        for radius in ("-1", "", "not-a-number", "2147483648", "999999999999999999999")
            response = handler(HTTP.Request("GET", path * "&origin_radius=$radius"))
            @test response.status == (metric == "accessible_population" ? 400 : 200)
            metric == "accessible_population" || @test response.body == expected.body
        end
        @test handler(HTTP.Request("GET", path * "&origin_radius=0&origin_radius=0")).status == 400
    end
end

@testset "Population Arrow input and rollup" begin
    cells = H3.API.cellToChildren(DEMO_ORIGIN, 8)[1:2]
    mktempdir() do dir
        path = joinpath(dir, "population.arrow")
        for file in (true, false)
            Arrow.write(path, (h3=H3.API.cellToChildren(DEMO_ORIGIN, 8)[1:3], population=[1.25, 0.0, 2.5], extra=[1, 2, 3]); file)
            population = load_population(path)
            for resolution in 0:8
                rollup = Reachability._population_rollup(population, resolution)
                @test sum(values(rollup)) == 3.75
                @test rollup[H3.API.cellToParent(cells[1], resolution)] == (resolution == 8 ? 1.25 : 3.75)
                @test rollup === Reachability._population_rollup(population, resolution)
            end
        end
        for table in ((h3=cells,), (population=[1, 2],),
                      (h3=string.(cells), population=[1, 2]),
                      (h3=[missing, cells[1]], population=[1, 2]),
                       (h3=cells, population=["1", "2"]),
                       (h3=[cells[1], cells[1]], population=[1, 2]))
            Arrow.write(path, table)
            @test_throws ArgumentError load_population(path)
        end
        Arrow.write(path, (h3=UInt64[], population=Float64[]))
        @test isempty(Reachability._population_rollup(load_population(path), 5))
    end
    population = Reachability._population(cells, fill(floatmax(Float64), 2))
    @test_throws ArgumentError Reachability._population_rollup(population, 5)
    @test !haskey(population.rollups, 5)
    for resolution in (-1, 9)
        @test_throws ArgumentError Reachability._population_rollup(population, resolution)
    end
end
