module PopulationRangeTests
using Test, Random
import H3
using ..Reachability
using ..PopulationPackedTests

allocated_range(args::Vararg{Any,N}) where N = @allocated Reachability._population_sample_range!(args...)

@testset "Range repair keeps later walking-eligible arrivals" begin
    R = Reachability
    (; origin) = PopulationPackedTests.fixture(7)
    distance(a, b) = ceil(Int, H3.Lib.greatCircleDistanceKm(
        Ref(H3.API.cellToLatLng(min(a, b))), Ref(H3.API.cellToLatLng(max(a, b)))) * R.WALK_MS_PER_KM)
    a, b, c = first((origin, b, c) for b in H3.API.gridDisk(origin, 1) for c in H3.API.gridDisk(b, 1)
        if length(Set((origin, b, c))) == 3 && distance(origin, c) > max(distance(origin, b), distance(b, c)))
    ab, bc = distance(a, b), distance(b, c)
    limit = max(ab, bc)
    graph = R.pack_graph((from_h3=[a,b,c], to_h3=[b,b,c],
        departure_ms=UInt32[div(R.PERIOD, 2), ab + 10, div(R.PERIOD, 2)], duration_ms=Int64[0,0,0]))
    index = R.prepare_walking(R.WalkingIndex(graph); max_walk_ms=limit)
    population = R._population(UInt64[first(H3.API.cellToChildren(h, 8)) for h in (a,b,c)], [0.0,2.0,3.0])
    for mode in PopulationPackedTests.MODES, exclude in (false,true)
        actual = R.route_population(graph, population, a, 0, ab + bc + 10; walking_index=index,
            origin_radius=1, window_ms=96, step_ms=1, max_walk_ms=limit,
            window_mode=mode, exclude_origin_population=exclude)
        expected = PopulationPackedTests.oracle(graph, population, index, actual.h3,
            0, ab + bc + 10, 1, 96, limit, mode; exclude_origin_population=exclude)
        @test all(isapprox.(actual.value, expected; rtol=1e-12, atol=1e-12))
    end
end

@testset "Range repair with moving cutoffs" begin
    R = Reachability
    rng = MersenneTwister(20260910)
    f = PopulationPackedTests.fixture(7)
    nodes = f.graph.h3
    for trial in 1:3
        graph = R.pack_graph((from_h3=[nodes; rand(rng, nodes, 80)],
            to_h3=[nodes; rand(rng, nodes, 80)],
            departure_ms=UInt32[zeros(Int, length(nodes)); rand(rng, 0:900_000:86_399_000, 80)],
            duration_ms=Int64[zeros(Int, length(nodes)); rand(rng, (0, 1000, 3_600_000), 80)]))
        index = R.prepare_walking(R.WalkingIndex(graph))
        for mode in PopulationPackedTests.MODES, exclude in (false, true), batch in (3, 16, 64),
                (departure, budget, samples) in ((0, 0, 5), (28_800_000, 10_800_000, 96), (86_399_000, 604_800_000, 5))
            options = (; walking_index=index, origin_radius=2, window_ms=samples * 900_000,
                step_ms=900_000, max_walk_ms=trial == 1 ? 0 : 3_600_000,
                window_mode=mode, exclude_origin_population=exclude, origin_batch_size=batch)
            actual = R.route_population(graph, f.population, f.origin, departure, budget; options...)
            expected = R._route_population_reference(graph, f.population, f.origin, departure, budget; options...)
            @test actual.h3 == expected.h3
            @test iszero.(actual.value) == iszero.(expected.value)
            @test all(isapprox.(actual.value, expected.value; rtol=1e-12, atol=1e-9))
        end
    end
end

@testset "Range workspace reuse and partial tiles" begin
    R = Reachability
    (; graph, population, origin, index, limit) = PopulationPackedTests.fixture(7)
    origins = sort!(filter(!iszero, H3.API.gridDisk(origin, 2)))
    prepared = R._prepare_population(population, index)
    sources = R._population_sources(index, prepared, R._population_rollup(population, 7), origins, UInt32(limit))
    w = R.PopulationWorkspace(length(graph.h3), length(sources.weights), 16)
    labels = w.arrivals
    for (ids, samples) in ((1:16, 96), (17:19, 1), (1:1, 96), (1:16, 5)), mode in PopulationPackedTests.MODES
        actual = @inferred R._population_tile!(w, graph, index.prepared.graph, prepared, sources,
            ids, UInt32(28_800_000), UInt32(10_800_000), Int64(900_000), samples, UInt32(limit), mode)
        expected = PopulationPackedTests.oracle(graph, population, index, origins[ids],
            28_800_000, 10_800_000, 900_000, samples, limit, mode)
        @test all(isapprox.(actual.value, expected; rtol=1e-12, atol=1e-9))
        @test w.arrivals === labels
        @test isempty(w.pending) && isempty(w.queue) && isempty(w.times) && isempty(w.coverage_ids)
        @test all(iszero, w.heads)
    end
    other = R.PopulationWorkspace(length(graph.h3), length(sources.weights), 16)
    @test other.arrivals !== w.arrivals
    ready = fill(UInt32(28_800_000), 16)
    cutoffs = ready .+ UInt32(10_800_000)
    fill!(other.arrivals, R.INF)
    counts = @inferred R._population_sample_range!(other, graph, index.prepared.graph, prepared,
        sources, 1:16, ready, cutoffs, UInt32(limit), other.arrivals)
    @test counts isa Tuple{Int,Int}
    bytes = allocated_range(other, graph, index.prepared.graph, prepared,
        sources, 1:16, ready, cutoffs, UInt32(limit), other.arrivals)
    @test bytes == 0
end
end
