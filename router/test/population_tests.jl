@testset "Population input" begin
    cells = H3.API.cellToChildren(DEMO_ORIGIN, 8)[1:2]
    mktempdir() do dir
        path = joinpath(dir, "population.arrow")
        Arrow.write(path, (h3=cells, population=[1.25, 2.5]))
        population = load_population(path)
        @test Reachability._population_rollup(population, 5)[DEMO_ORIGIN] == 3.75
        @test sum(values(Reachability._population_rollup(population, 8))) == 3.75
        for weights in ([NaN, 1.0], [Inf, 1.0], [-1.0, 1.0], [missing, 1.0])
            Arrow.write(path, (h3=cells, population=weights))
            @test_throws ArgumentError load_population(path)
        end
        @test_throws ArgumentError Reachability._population(UInt64[DEMO_ORIGIN], [1])
        @test_throws ArgumentError Reachability._population(UInt64[0], [1])
        @test_throws ArgumentError Reachability._population(cells, [1])
        @test_throws ArgumentError Reachability._population([cells[1], cells[2], cells[1]], [1, 2, 3])
        @test_throws ArgumentError Reachability._population([cells[1], cells[1]], [1, 2])
        snapshot = copy(cells)
        checked = Reachability._population(reverse(cells), [1.25, 2.5]; progress=true)
        @test Reachability._population_rollup(checked, 5; progress=true)[DEMO_ORIGIN] == 3.75
        @test cells == snapshot
        @test checked.h3 == reverse(cells)
    end
    for radius in ("-1", "1.0", "", "2147483648", "999999999999999999999")
        @test_throws ArgumentError Reachability._origin_radius(radius)
    end
    @test Reachability._origin_radius("0") == 0
    cell = first(H3.API.cellToChildren(DEMO_ORIGIN, 8))
    graph = pack_graph((from_h3=[cell], to_h3=[cell], departure_ms=UInt32[0], duration_ms=Int64[0]))
    tiny = Reachability._population([cell], [nextfloat(0.0)])
    result = route_population(graph, tiny, cell, 0, 0; window_ms=2, step_ms=1,
                              max_walk_ms=0, window_mode=:reachable_union)
    @test only(result.value) == nextfloat(0.0)
    empty = Reachability._population([cell], [0.0])
    result = route_population(graph, empty, cell, 0, 0; max_walk_ms=0)
    @test only(result.value) == 0.0
    @test result.shared_expansions == result.query_expansions == result.workers == 0
end

@testset "File-backed population network selection" begin
    mktempdir() do dir
        fine = first(H3.API.cellToChildren(DEMO_ORIGIN, 8))
        parent = H3.API.cellToParent(fine, 7)
        path = joinpath(dir, "population.arrow")
        Arrow.write(path, (h3=[fine], population=[4.25]); file=false)
        paths = [joinpath(dir, "alpha_res8.arrow"), joinpath(dir, "beta_res8.arrow"),
                 joinpath(dir, "alpha_res7.arrow")]
        for (file, cell) in zip(paths, (fine, fine, parent))
            Arrow.write(file, (from_h3=[cell], to_h3=[cell],
                departure_ms=UInt32[0], duration_ms=Int64[0]))
        end
        handler = load_handlers(paths; population_path=path)
        for (network, cell) in (("", fine), ("&network=beta", fine), ("", parent))
            response = handler(HTTP.Request("GET", "/reachable?index=$(string(cell; base=16))&departure_h=0&budget_h=0&metric=accessible_population$network"))
            @test response.status == 200
            @test only(Arrow.Table(response.body).value) == 4.25
        end
        response = handler(HTTP.Request("GET", "/reachable?index=$(string(parent; base=16))&departure_h=0&budget_h=0&metric=accessible_population&network=beta"))
        @test response.status == 400
    end
end

@testset "Independent population origins and shared states" begin
    origins = sort!(filter(!iszero, H3.API.gridDisk(DEMO_ORIGIN, 2)))
    hub, target = DEMO_CELLS[5:6]
    table = (from_h3=[origins; hub], to_h3=[fill(hub, length(origins)); target],
        departure_ms=UInt32[fill(1000, length(origins)); 2000],
        duration_ms=Int64[fill(500, length(origins)); 0])
    graph = pack_graph(table)
    cells = unique([origins; hub; target])
    population = Reachability._population(
        UInt64[first(H3.API.cellToChildren(h, 8)) for h in cells], Float64.(1:length(cells)) .+ 0.25)
    weights = Reachability._population_rollup(population, 5)
    index = WalkingIndex(graph)
    modes = (:mean_intersection, :max_intersection, :diff_intersection,
             :min_union, :diff_union, :reachable_union)
    for walk in (0, 3_600_000), mode in modes
        budget = walk == 0 ? 2000 : 3_600_000
        result = route_population(graph, population, DEMO_ORIGIN, 0, budget;
            origin_radius=2, window_ms=3000, step_ms=1000, max_walk_ms=walk,
            window_mode=mode, walking_index=index)
        @test result.h3 == origins
        for (i, origin) in enumerate(origins)
            counts = Dict{UInt64,Int}()
            for time in (0, 1000, 2000)
                topology = Reachability.WalkingTopology(index, walk)
                reference = Reachability._walking_route_at(graph, topology, origin,
                    UInt32(time), UInt32(time + budget), false)
                for cell in reference.h3
                    counts[cell] = get(counts, cell, 0) + 1
                end
            end
            expected = sum((get(weights, cell, 0.0) * (mode == :reachable_union ? count / 3 :
                mode in (:min_union, :diff_union) ? 1 : count == 3)) for (cell, count) in counts)
            @test isapprox(result.value[i], expected)
        end
        @test result.shared_expansions < result.query_expansions
    end
    for mode in modes
        result = route_population(graph, population, DEMO_ORIGIN, 0, 0;
            origin_radius=5, max_walk_ms=0, window_mode=mode)
        @test length(result.h3) > 64
        @test result.value == [get(weights, h, 0.0) for h in result.h3]
    end
    handler = make_handler(graph; population)
    base = "/reachable?index=$(H3.API.h3ToString(DEMO_ORIGIN))&departure_h=0&budget_h=0&max_walk_h=0"
    response = handler(HTTP.Request("GET", base * "&origin_radius=0"))
    @test response.status == 200
    @test response.body == handler(HTTP.Request("GET", base)).body
    @test make_handler(graph)(HTTP.Request("GET", base * "&metric=accessible_population")).status == 400
    for encoding in ("split", "string"), distance in ("itinerary", "straight_line")
        response = handler(HTTP.Request("GET", base * "&metric=accessible_population&origin_radius=2&encoding=$encoding&distance_mode=$distance&window_mode=ignored"))
        @test response.status == 200
        result = Arrow.Table(response.body)
        @test Set(propertynames(result)) == Set(encoding == "split" ? (:index_lower, :index_upper, :value) : (:index, :value))
        @test result.value == [weights[h] for h in origins]
        @test HTTP.header(response, "X-Router-Distance") == "not-computed"
        @test HTTP.header(response, "X-Router-Origin-Count") == string(length(origins))
    end
end

@testset "Population walking, deadlines, and daily rollover" begin
    table = Reachability._warmup_table()
    cells = unique(table.from_h3)
    rng = MersenneTwister(273)
    population = Reachability._population(H3.API.gridDisk(first(cells), 5),
        rand(rng, length(H3.API.gridDisk(first(cells), 5))))
    weights = Reachability._population_rollup(population, 8)
    graph = pack_graph((from_h3=rand(rng, cells, 60), to_h3=rand(rng, cells, 60),
        departure_ms=rand(rng, UInt32[0, 1, 1000, P - 1000], 60),
        duration_ms=rand(rng, Int64[0, 1000, 60000], 60), distance_km=ones(60)))
    fill!(graph.distance_km, Inf) # Population routing must not read itinerary distances.
    index = prepare_walking(WalkingIndex(graph))
    modes = (:mean_intersection, :max_intersection, :diff_intersection,
             :min_union, :diff_union, :reachable_union)
    for ready in (0, P - 2000), walk in (0, 1_200_000, 3_700_000), budget in (1000, 1_800_000), mode in modes
        result = route_population(graph, population, first(cells), ready, budget;
            origin_radius=2, max_walk_ms=walk, walking_index=index,
            window_ms=3001, step_ms=1000, window_mode=mode)
        for (i, origin) in enumerate(result.h3)
            counts = Dict{UInt64,Int}()
            for offset in (0, 1000, 2000, 3000)
                time = UInt32(ready + offset)
                reference = Reachability._walking_route_at(graph,
                    Reachability.WalkingTopology(index, min(walk, budget)), origin,
                    time, time + UInt32(budget), false)
                for h in reference.h3
                    counts[h] = get(counts, h, 0) + 1
                end
            end
            expected = sum(get(weights, h, 0.0) * (mode == :reachable_union ? count / 4 :
                mode in (:min_union, :diff_union) ? 1 : count == 4) for (h, count) in counts)
            @test isapprox(result.value[i], expected)
        end
    end
    # Exact final-walk limits include the boundary, but never permit a second walk.
    origin = first(cells)
    hops = walking_cells(index, origin, 3_600_000)
    hop = first(filter(h -> h.cell != origin, hops))
    isolated = pack_graph((from_h3=[origin], to_h3=[origin],
        departure_ms=UInt32[P - 1], duration_ms=Int64[0]))
    pair = Reachability._population([origin, hop.cell], [2.0, 3.0])
    for delta in (-1, 0)
        result = route_population(isolated, pair, origin, 0, hop.duration_ms;
            max_walk_ms=Int(hop.duration_ms) + delta)
        @test only(result.value) == (delta == 0 ? 5.0 : 2.0)
    end
    @test_throws ArgumentError route_population(graph, population, origin, 0,
        Int(Reachability.MAX_TIME_MS); window_ms=2, step_ms=1)
    @test_throws ArgumentError route_population(graph, population, origin, 0, 0; max_walk_ms=-1)
    @test_throws ArgumentError route_population(graph, population, origin, 0, 0; window_ms=-1)
    @test_throws ArgumentError route_population(graph, population, origin, 0, 0; step_ms=-1)
    @test_throws ArgumentError route_population(graph, population, origin, 0, 0;
        walking_index=WalkingIndex(isolated))
    pentagons = zeros(UInt64, 12)
    @test iszero(H3.Lib.getPentagons(8, pentagons))
    empty_population = Reachability._population(UInt64[], Float64[])
    result = route_population(graph, empty_population, first(pentagons), 0, 0;
        origin_radius=1, max_walk_ms=0)
    @test length(result.h3) == 6
    @test all(iszero, result.value)
end

@testset "Population origin/sample blocks" begin
    cells = sort!(filter(!iszero, H3.API.gridDisk(DEMO_ORIGIN, 2)))[1:9]
    origin, hub, target = cells[1:3]
    # All samples board one connection. Only later samples can reach the target.
    graph = pack_graph((from_h3=[origin, hub, target, cells[4:end]...],
        to_h3=[hub, target, hub, cells[4:end]...],
        departure_ms=UInt32[95, 100, 100, fill(0, 6)...],
        duration_ms=Int64[0, 0, 0, fill(0, 6)...]))
    population = Reachability._population(
        UInt64[first(H3.API.cellToChildren(h, 8)) for h in cells], collect(1.25:1:9.25))
    weights = Reachability._population_rollup(population, 5)
    index = WalkingIndex(graph)
    modes = (:mean_intersection, :max_intersection, :diff_intersection,
             :min_union, :diff_union, :reachable_union)
    for radius in (0, 1, 2), mode in modes
        result = route_population(graph, population, origin, 0, 96;
            origin_radius=radius, window_ms=96, step_ms=1, max_walk_ms=0,
            window_mode=mode, walking_index=index)
        tile_size = min(8, length(result.h3))
        jobs = cld(length(result.h3), tile_size) * cld(96, fld(64, tile_size))
        @test result.workers == min(Threads.nthreads(:default), jobs)
        @test result.shared_expansions < result.query_expansions
        for (i, source) in enumerate(result.h3)
            counts = Dict{UInt64,Int}()
            for time in UInt32(0):UInt32(95)
                reference = Reachability._walking_route_at(graph,
                    Reachability.WalkingTopology(index, 0), source, time, time + UInt32(96), false)
                for h in reference.h3
                    counts[h] = get(counts, h, 0) + 1
                end
            end
            expected = sum(get(weights, h, 0.0) * (mode == :reachable_union ? count / 96 :
                mode in (:min_union, :diff_union) ? 1 : count == 96) for (h, count) in counts)
            @test isapprox(result.value[i], expected)
        end
    end
    topology = Reachability.WalkingTopology(index, 0)
    ready = UInt32.(0:63)
    reached, shared, queries = Reachability._population_sample(graph, topology,
        fill(origin, 64), ready, ready .+ UInt32(96), weights)
    @test reached[origin] == typemax(UInt64)
    @test reached[hub] == typemax(UInt64)
    @test reached[target] == typemax(UInt64) << 4
    @test shared == 66
    @test queries == 188
    # A short final block must not set unused bits.
    reached, _, _ = Reachability._population_sample(graph, topology,
        fill(origin, 3), UInt32[0, 1, 2], UInt32[96, 97, 98], weights)
    @test reached[hub] == UInt64(7)
    @test !haskey(reached, target)
    result = route_population(graph, population, origin, 0, 0;
        window_ms=512, step_ms=1, max_walk_ms=0, walking_index=index)
    @test result.workers == min(Threads.nthreads(:default), 8)
    @test only(result.value) == weights[origin]
    for (window, step) in ((0, 1), (1, 0))
        result = route_population(graph, population, origin, 0, 0;
            window_ms=window, step_ms=step, max_walk_ms=0, window_mode=:ignored)
        @test only(result.value) == weights[origin]
    end
end
