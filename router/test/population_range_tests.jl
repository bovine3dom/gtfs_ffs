module PopulationRangeTests
using Test, Random
import H3
using ..Reachability
using ..PopulationPackedTests

allocated_range(args::Vararg{Any,N}) where N = @allocated Reachability._population_sample_range!(args...)

@testset "Range queue equal times, stale keys, and sample reset" begin
    R = Reachability
    origin = H3.API.latLngToCell(H3.API.LatLng(0.5, 0.1), 7)::UInt64
    a, b, c, d, e = sort!(H3.API.gridDisk(origin, 1))[1:5]
    # Node a pops before b adds another origin at the same time; e has stale later keys.
    graph = R.pack_graph((from_h3=[c, d, c, b, b, a, a, e], to_h3=[a, b, e, e, a, e, a, e],
        departure_ms=UInt32[20, 20, 0, 0, 20, 20, 20, 20], duration_ms=Int64[0, 0, 30, 25, 0, 0, 0, 0]))
    index = R.prepare_walking(R.WalkingIndex(graph); max_walk_ms=0)
    population = R._population(UInt64[first(H3.API.cellToChildren(h, 8)) for h in graph.h3], ones(5))
    prepared = R._prepare_population(population, index)
    weights = R._population_rollup(population, 7)
    origins = [c, d, b]
    sources = R._population_sources(index, prepared, weights, origins, UInt32(0))
    w = R.PopulationWorkspace(5, length(sources.weights), 8)
    fill!(w.arrivals, R.INF)
    topology = R.WalkingTopology(index, 0)
    for departure in reverse(0:4:100)
        ready = UInt32[departure + sample for sample in 0:3 for _ in origins]
        cutoffs = ready .+ UInt32(40)
        R._population_sample_range!(w, graph, index.prepared.graph, prepared, sources,
            1:3, ready, cutoffs, UInt32(0), w.arrivals)
        expected = first(R._population_sample(graph, topology, repeat(origins, 4), ready, cutoffs, weights))
        @test w.reached == [get(expected, cell, UInt64(0)) for cell in prepared.cells]
        for (lane, cell) in enumerate(origins)
            labels = [time <= first(cutoffs) ? time : R.INF for time in w.arrivals[lane, 1:2:end]]
            @test labels == R.route_cpu(graph, cell, departure, 40)
        end
        @test all(==(R.INF), w.arrivals[:, 2:2:end])
        @test all(==(R.INF), w.arrivals[4:end, :])
        @test all(iszero, w.range_pending)
        @test all(==(R.INF), w.range_queued)
        @test isempty(w.queue) && isempty(w.pending)
    end
    huge = R._population(population.h3, fill(floatmax(Float64), 5))
    cache = R.PopulationResultCache(graph, huge, index)
    @test_throws ArgumentError R._cached_route_population(cache, c, 0, 40;
        origin_radius=1, window_ms=96, step_ms=1, max_walk_ms=0, window_mode=:min_union)
    @test isempty(cache.totals) && isempty(cache.order)
end

@testset "Range repair near the reserved time boundary" begin
    R = Reachability
    (; graph, population, origin, index) = PopulationPackedTests.fixture(7)
    step = div(Int(R.MAX_TIME_MS) - 100, 95)
    for mode in PopulationPackedTests.MODES, exclude in (false, true)
        options = (; walking_index=index, max_walk_ms=0, origin_radius=1,
            window_ms=95step + 1, step_ms=step, window_mode=mode, exclude_origin_population=exclude)
        actual = R.route_population(graph, population, origin, 0, 100; options...)
        expected = PopulationPackedTests.oracle(graph, population, index, actual.h3,
            0, 100, step, 96, 0, mode; exclude_origin_population=exclude)
        @test iszero.(actual.value) == iszero.(expected)
        @test isapprox(actual.value, expected; rtol=1e-12, atol=1e-6)
    end
end

@testset "Adaptive tiles use classified transit origins" begin
    R = Reachability
    origin = H3.API.latLngToCell(H3.API.LatLng(0.5, 0.1), 7)::UInt64
    cells = sort!(H3.API.gridDisk(origin, 18))
    @test length(cells) == 1027
    threshold = 2 * 64 * Threads.nthreads(:default)
    population = R._population(UInt64[first(H3.API.cellToChildren(h, 8)) for h in cells],
        Float64.(mod.(eachindex(cells), 8)))
    for heavy in unique([min(threshold - 1, 1027), min(threshold, 1027), 1027])
        # The cycle is boardable at time zero only. Other origins have no initial access.
        nodes = cells[1:heavy]
        graph = R.pack_graph((from_h3=nodes, to_h3=circshift(nodes, 1),
            departure_ms=zeros(UInt32, heavy), duration_ms=zeros(Int64, heavy)))
        index = R.prepare_walking(R.WalkingIndex(graph); max_walk_ms=0)
        @test count(i -> graph.out_ptr[i] < graph.out_ptr[i + 1], eachindex(graph.h3)) == heavy
        for samples in (1, 2, 4, 96)
            options = (; walking_index=index, max_walk_ms=0, origin_radius=18,
                window_ms=samples == 1 ? 0 : samples, step_ms=1)
            actual = R.route_population(graph, population, origin, 0, 1; options...)
            narrow = R.route_population(graph, population, origin, 0, 1; options..., origin_batch_size=16)
            wide = R.route_population(graph, population, origin, 0, 1; options..., origin_batch_size=64)
            @test narrow.shared_expansions != wide.shared_expansions
            @test actual == (samples == 1 || (samples > 4 && heavy >= threshold) ? wide : narrow)
            @test actual.h3 == cells
            @test iszero.(narrow.value) == iszero.(wide.value)
            @test narrow.value == wide.value
            weights = Float64.(mod.(eachindex(cells), 8))
            expected = copy(weights)
            samples == 1 && (expected[1:heavy] .= sum(weights[1:heavy]))
            @test actual.value == expected
        end
    end
end

@testset "Static initial walks and compact transit tiles" begin
    R = Reachability
    f = PopulationPackedTests.fixture(7)
    cells = sort!(H3.API.gridDisk(f.origin, 2))
    # Keep graph nodes but remove all connections through the normal input filter.
    graph = @test_logs (:warn, r"Skipping") R.pack_graph((from_h3=cells[1:2:end],
        to_h3=cells[1:2:end], departure_ms=zeros(UInt32, 10), duration_ms=fill(Int64(-1), 10));
        skip_invalid_durations=true)
    index = R.prepare_walking(R.WalkingIndex(graph))
    for mode in PopulationPackedTests.MODES, exclude in (false, true), samples in (1,96), walk in (0,3_600_000)
        options = (; walking_index=index, origin_radius=3, window_ms=samples == 1 ? 0 : samples,
            step_ms=1, max_walk_ms=walk, window_mode=mode, exclude_origin_population=exclude)
        actual = R.route_population(graph, f.population, f.origin, 0, 3_600_000; options...)
        expected = R._route_population_reference(graph, f.population, f.origin, 0, 3_600_000; options...)
        @test actual.workers == actual.shared_expansions == actual.query_expansions == 0
        @test actual.h3 == expected.h3
        @test iszero.(actual.value) == iszero.(expected.value)
        @test all(isapprox.(actual.value, expected.value; rtol=1e-12, atol=1e-12))
    end
    # Nonconsecutive global IDs must survive compaction and own-credit exclusion.
    graph = R.pack_graph((from_h3=cells[1:3:end], to_h3=cells[1:3:end],
        departure_ms=zeros(UInt32, 7), duration_ms=zeros(Int64, 7)))
    index = R.prepare_walking(R.WalkingIndex(graph); max_walk_ms=0)
    for mode in PopulationPackedTests.MODES, exclude in (false,true), tile in (3,16,64)
        options = (; walking_index=index, origin_radius=2, window_ms=96, step_ms=1,
            max_walk_ms=0, window_mode=mode, exclude_origin_population=exclude, origin_batch_size=tile)
        actual = R.route_population(graph, f.population, f.origin, 0, 100; options...)
        expected = R._route_population_reference(graph, f.population, f.origin, 0, 100; options...)
        @test actual.h3 == expected.h3
        @test iszero.(actual.value) == iszero.(expected.value)
        @test all(isapprox.(actual.value, expected.value; rtol=1e-12, atol=1e-12))
    end
end

@testset "Range repair keeps later walking-eligible arrivals" begin
    R = Reachability
    (; origin) = PopulationPackedTests.fixture(7)
    distance(a, b) = ceil(Int, H3.Lib.greatCircleDistanceKm(
        Ref(H3.API.cellToLatLng(min(a, b))), Ref(H3.API.cellToLatLng(max(a, b)))) * R.WALK_MS_PER_KM)
    a, b, c = first((origin, b, c) for b in H3.API.gridDisk(origin, 1) for c in H3.API.gridDisk(b, 1)
        if length(Set((origin, b, c))) == 3 && distance(origin, c) > max(distance(origin, b), distance(b, c)))
    ab, bc = distance(a, b), distance(b, c)
    limit = max(ab, bc)
    graph = R.pack_graph((from_h3=[b,b,c], to_h3=[a,b,c],
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
        @test all(iszero, w.range_pending) && all(==(R.INF), w.range_queued)
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

@testset "Sparse label reset across range and packed tiles" begin
    R = Reachability
    f = PopulationPackedTests.fixture(8)
    origins = sort!(H3.API.gridDisk(f.origin, 2))
    prepared = R._prepare_population(f.population, f.index)
    weights = R._population_rollup(f.population, 8)
    sources = R._population_sources(f.index, prepared, weights, origins, UInt32(f.limit))
    n = length(f.graph.h3)
    w = R.PopulationWorkspace(n + 10_000, length(sources.weights), 16)
    for samples in (1, 2, 4, 96)
        fill!(w.arrivals, 0)
        fill!(w.settled, 0)
        empty!(w.settled_ids)
        actual = R._population_tile!(w, f.graph, f.index.prepared.graph, prepared, sources,
            1:16, UInt32(0), UInt32(10_800_000), Int64(60_000), samples, UInt32(f.limit), :mean_intersection)
        expected = PopulationPackedTests.oracle(f.graph, f.population, f.index, origins[1:16],
            0, 10_800_000, 60_000, samples, f.limit, :mean_intersection)
        @test actual.value == expected
        @test all(==(R.INF), w.arrivals[:, (2n + 1):end])
    end
    fill!(@view(w.arrivals[:, (2n + 1):end]), 0)
    for mode in PopulationPackedTests.MODES, exclude in (false, true),
            (ids, samples, walk) in ((1:16, 96, f.limit), (17:19, 1, 0), (1:1, 96, f.limit), (1:16, 5, 0))
        sources = R._population_sources(f.index, prepared, weights, origins, UInt32(walk))
        own = exclude ? Int32[get(f.index.prepared.output_id, h, 0) for h in origins] : nothing
        actual = R._population_tile!(w, f.graph, f.index.prepared.graph, prepared, sources,
            ids, UInt32(0), UInt32(10_800_000), Int64(60_000), samples, UInt32(walk), mode, own)
        expected = PopulationPackedTests.oracle(f.graph, f.population, f.index, origins[ids],
            0, 10_800_000, 60_000, samples, walk, mode; exclude_origin_population=exclude)
        @test all(isapprox.(actual.value, expected; rtol=1e-12, atol=1e-9))
        @test all(iszero, w.arrivals[:, (2n + 1):end])
        @test isempty(w.queue) && all(iszero, w.range_pending)
    end
end
end
