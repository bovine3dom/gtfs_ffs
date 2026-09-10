if abspath(PROGRAM_FILE) == @__FILE__
    using Test, Random, Arrow, H3
    import KernelAbstractions as KA
    include("../Reachability.jl")
    using .Reachability
    include("../../../router/fixture.jl")
    const P = Int(Reachability.PERIOD)
    const INF = Reachability.INF
end
using .Reachability: GPU_FLOAT32_RTOL, GPU_FLOAT32_ATOL

const POPULATION_GPU_MODES = (:mean_intersection, :max_intersection, :diff_intersection,
                              :min_union, :diff_union, :reachable_union)
const POPULATION_GPU_ERRORS = Dict{DataType,Tuple{Float64,Float64}}()

function population_gpu_parity(router, population, origin, ready, budget; kwargs...)
    options = (; kwargs...)
    cpu_options = Base.structdiff(options, (; origins_per_tile=nothing))
    expected = route_population(router.graph, population, origin, ready, budget;
        walking_index=router.walking_index, cpu_options...)
    # Driver command lists use finalizers. Reclaim them between hardware test queries.
    router.backend isa KA.CPU || GC.gc()
    actual = Reachability._route_population_gpu(router, origin, ready, budget; options...)
    @test actual.h3 == expected.h3
    @test all(isapprox.(actual.value, expected.value; rtol=GPU_FLOAT32_RTOL, atol=GPU_FLOAT32_ATOL))
    @test iszero.(actual.value) == iszero.(expected.value)
    @test eltype(actual.value) == Float64
    @test actual.weight_and_accumulator_precision == "Float32"
    @test actual.bytes_downloaded == 4length(actual.h3) + 4actual.rounds
    @test actual.batches <= actual.rounds || isempty(router.graph.h3)
    @test all(>=(0), (actual.planning_s, actual.upload_s, actual.device_s, actual.download_s))
    errors = abs.(actual.value .- expected.value)
    absolute, relative = get(POPULATION_GPU_ERRORS, typeof(router.backend), (0.0, 0.0))
    POPULATION_GPU_ERRORS[typeof(router.backend)] = (max(absolute, maximum(errors; init=0.0)),
        max(relative, maximum((iszero(v) ? 0.0 : e / abs(v) for (e, v) in zip(errors, expected.value)); init=0.0)))
    return actual
end

function population_gpu_fixture(cells, sources, targets, departures, durations; weights=ones(length(cells)))
    graph = pack_graph((from_h3=UInt64[cells[i] for i in sources], to_h3=UInt64[cells[i] for i in targets],
        departure_ms=UInt32.(departures), duration_ms=Int64.(durations), distance_km=ones(length(sources))))
    fill!(graph.distance_km, Inf) # Population must not inspect itinerary distances.
    population = Reachability._population(UInt64[first(H3.API.cellToChildren(h, 8)) for h in cells], Float64.(weights))
    return graph, population
end

population_gpu_backends = Any[KA.CPU()]
if "--backend=oneapi" in ARGS
    import oneAPI
    oneAPI.functional() || error("GPU tests requested but oneAPI is unavailable")
    oneAPI.versioninfo()
    oneAPI.allowscalar(false)
    push!(population_gpu_backends, oneAPI.oneAPIBackend())
end
if "--backend=cuda" in ARGS
    import CUDA
    CUDA.functional() || error("GPU tests requested but CUDA is unavailable")
    CUDA.versioninfo()
    CUDA.allowscalar(false)
    push!(population_gpu_backends, CUDA.CUDABackend())
end

@testset "Device population: $(typeof(backend))" for backend in population_gpu_backends
    centre = first(H3.API.cellToChildren(DEMO_ORIGIN, 8))
    cells = sort!(filter(!iszero, H3.API.gridDisk(centre, 2)))
    @testset "Sample masks, partial blocks, and modes" begin
        nodes = cells[1:3]
        graph, population = population_gpu_fixture(nodes, [1, 2, 3], [2, 3, 2],
            [95, 100, 100], [0, 0, 0]; weights=[0.0, 2.25, 7.125])
        router = PopulationKernelRouter(graph, population, backend)
        @test eltype(router.device.weights) == Float32
        @test all(a -> eltype(a) == UInt32, router.device.labels)
        for samples in (1, 31, 32, 33, 63, 64, 65, 96, 97), mode in POPULATION_GPU_MODES,
                (radius, tile) in ((0, 8), (1, 4), (1, 8))
            actual = population_gpu_parity(router, population, nodes[1], 0, 96;
                window_ms=samples, step_ms=1, max_walk_ms=0, window_mode=mode,
                origin_radius=radius, origins_per_tile=tile)
            @test actual.samples == samples
            if samples == 96 && radius == 0
                expected = mode == :reachable_union ? 2.25 + 7.125 * 92 / 96 :
                    mode in (:min_union, :diff_union) ? 9.375 : 2.25
                @test isapprox(only(actual.value), expected; rtol=GPU_FLOAT32_RTOL, atol=GPU_FLOAT32_ATOL)
            end
        end
        for (window, step) in ((0, 1), (1, 0))
            actual = population_gpu_parity(router, population, nodes[1], 0, 0;
                window_ms=window, step_ms=step, window_mode=:ignored, max_walk_ms=0)
            @test actual.value == [0.0]
        end
        graph, population = population_gpu_fixture(nodes, [fill(1, 65); 2], [fill(2, 65); 3],
            [collect(0:64); 64], zeros(Int, 66); weights=[0.0, 2.0, 7.0])
        router = PopulationKernelRouter(graph, population, backend)
        for mode in POPULATION_GPU_MODES
            actual = population_gpu_parity(router, population, nodes[1], 0, 0;
                window_ms=65, step_ms=1, max_walk_ms=0, window_mode=mode)
            expected = mode == :reachable_union ? 2 + 7 / 65 : mode in (:min_union, :diff_union) ? 9 : 2
            @test isapprox(only(actual.value), expected; rtol=GPU_FLOAT32_RTOL, atol=GPU_FLOAT32_ATOL)
        end
    end

    @testset "Off-graph access, self reset, and later eligible arrival" begin
        # Select two walks whose combined endpoints exceed the one-hop limit.
        bare_graph, _ = population_gpu_fixture([centre], [1], [1], [P - 1], [0])
        bare = WalkingIndex(bare_graph)
        hop = first(walking_cells(bare, centre, 1_000_000))
        limit = hop.duration_ms
        second = first(filter(h -> all(x -> x.cell != h.cell, walking_cells(bare, centre, limit)) &&
            h.cell != centre, walking_cells(bare, hop.cell, limit)))
        a, b, c = centre, hop.cell, second.cell
        budget = Int(hop.duration_ms) + 10 + Int(second.duration_ms)
        population = Reachability._population([a, b, c], [0.0, 3.0, 7.0])
        for self in (false, true)
            graph, _ = self ? population_gpu_fixture([b], [1], [1], [Int(hop.duration_ms) + 10], [0]) :
                population_gpu_fixture([a, b], [1], [2], [0], [Int(hop.duration_ms) + 10])
            router = PopulationKernelRouter(graph, population, backend)
            for delta in (-1, 0), mode in POPULATION_GPU_MODES
                actual = population_gpu_parity(router, population, a, 0, budget + delta;
                    max_walk_ms=limit, window_ms=1, step_ms=1, window_mode=mode)
                @test only(actual.value) == (delta == 0 ? 10.0 : 3.0)
            end
        end
        # Without transit, access cannot start a second walk.
        graph, _ = population_gpu_fixture([b], [1], [1], [P - 1], [0])
        router = PopulationKernelRouter(graph, population, backend)
        actual = population_gpu_parity(router, population, a, 0, budget; max_walk_ms=limit)
        @test only(actual.value) == 3.0
        for delta in (-1, 0)
            actual = population_gpu_parity(router, population, a, 0, Int(limit);
                max_walk_ms=Int(limit) + delta)
            @test only(actual.value) == (delta == 0 ? 3.0 : 0.0)
        end
        far = first(H3.API.cellToChildren(last(DEMO_CELLS), 8))
        direct = first(walking_cells(router.walking_index, far, 1_000_000))
        population = Reachability._population([far, direct.cell], [2.0, 7.0])
        router = PopulationKernelRouter(graph, population, backend; walking_index=router.walking_index)
        @test all(h -> !(h in router.walking_index.prepared.output_cells), (far, direct.cell))
        for delta in (-1, 0)
            actual = population_gpu_parity(router, population, far, 0, Int(direct.duration_ms) + delta;
                window_ms=65, step_ms=1, window_mode=:reachable_union)
            @test isapprox(only(actual.value), delta == 0 ? 9.0 : 2.0; rtol=GPU_FLOAT32_RTOL, atol=GPU_FLOAT32_ATOL)
        end
    end

    @testset "Zero cycles, midnight, overlap, and reset" begin
        rng = MersenneTwister(0x504f5055)
        graph, population = population_gpu_fixture(cells, [collect(1:19); rand(rng, 1:19, 50)],
            [collect(1:19); rand(rng, 1:19, 50)], [zeros(Int, 19); rand(rng, [0, 1, 1000, P - 1000], 50)],
            [zeros(Int, 19); rand(rng, [0, 1000, 60000], 50)]; weights=rand(rng, 19))
        router = PopulationKernelRouter(graph, population, backend)
        for ready in (0, P - 2000), walk in (0, 1_200_000, 3_600_000), mode in POPULATION_GPU_MODES
            population_gpu_parity(router, population, centre, ready, 1_800_000;
                origin_radius=1, max_walk_ms=walk, window_ms=3001, step_ms=1000, window_mode=mode)
        end
        chain, population = population_gpu_fixture(cells, 1:18, 2:19, zeros(Int, 18), zeros(Int, 18))
        router = PopulationKernelRouter(chain, population, backend)
        for origin in cells[[1, 19, 1]]
            actual = population_gpu_parity(router, population, origin, 0, 0; max_walk_ms=0)
            @test only(actual.value) == 20 - findfirst(==(origin), cells)
        end
    end

    @testset "Request-local IDs, resolution 7, and empty population" begin
        coarse = H3.API.cellToParent(centre, 7)
        graph, population = population_gpu_fixture([coarse], [1], [1], [P - 1], [0]; weights=[4.25])
        router = PopulationKernelRouter(graph, population, backend)
        for mode in POPULATION_GPU_MODES
            population_gpu_parity(router, population, coarse, 0, 3_600_000;
                origin_radius=2, window_ms=65, step_ms=1, window_mode=mode)
        end
        # At res6 the default one-hour walk reaches no adjacent centres here.
        coarse = H3.API.cellToParent(centre, 6)
        far = last(sort!(filter(!iszero, H3.API.gridDisk(coarse, 20))))
        graph, _ = population_gpu_fixture([coarse], [1], [1], [P - 1], [0])
        population = Reachability._population([first(H3.API.cellToChildren(far, 8))], [11.5])
        router = PopulationKernelRouter(graph, population, backend)
        snapshot = copy(router.walking_index.prepared.output_cells)
        for radius in (6, 10, 18)
            actual = population_gpu_parity(router, population, far, 0, 0; origin_radius=radius)
            @test length(actual.h3) == 1 + 3radius * (radius + 1)
            @test actual.value == [h == far ? 11.5 : 0.0 for h in actual.h3]
            @test actual.output_cells > length(snapshot)
        end
        @test router.walking_index.prepared.output_cells == snapshot
        zero_population = Reachability._population([first(H3.API.cellToChildren(far, 8))], [0.0])
        for population in (zero_population, Reachability._population(UInt64[], Float64[]))
            router = PopulationKernelRouter(graph, population, backend)
            actual = population_gpu_parity(router, population, far, 0, 0; origin_radius=1)
            @test actual.value == zeros(7)
        end
        router = PopulationKernelRouter(graph, nothing, backend)
        @test Reachability._route_population_gpu(router, far, 0, 0).value == [0.0]
        empty, _ = population_gpu_fixture(UInt64[], Int[], Int[], Int[], Int[])
        population = Reachability._population(UInt64[], Float64[])
        router = PopulationKernelRouter(empty, population, backend)
        actual = population_gpu_parity(router, population, DEMO_ORIGIN, 0, 0; origin_radius=1)
        @test actual.rounds == 0
        @test actual.value == zeros(7)
    end

    @testset "Explicit unsupported requests" begin
        graph, population = population_gpu_fixture([centre], [1], [1], [0], [0])
        @test_throws ArgumentError PopulationKernelRouter(graph, population, backend; walking_index=WalkingIndex(graph))
        router = PopulationKernelRouter(graph, population, backend)
        for options in ((; max_walk_ms=3_600_001), (; max_walk_ms=-1), (; window_ms=-1),
                (; step_ms=-1), (; origin_radius=-1), (; origins_per_tile=0), (; origins_per_tile=33),
                (; window_ms=2, step_ms=1, window_mode=:invalid))
            @test_throws ArgumentError Reachability._route_population_gpu(router, centre, 0, 0; options...)
        end
        @test_throws ArgumentError Reachability._route_population_gpu(router, centre, 0, Int(Reachability.MAX_TIME_MS);
            window_ms=2, step_ms=1)
        @test_throws ArgumentError Reachability._route_population_gpu(router, UInt64(0), 0, 0)
        for cell in (centre, last(cells))
            huge = Reachability._population([cell], [2Float64(floatmax(Float32))])
            @test_throws ArgumentError PopulationKernelRouter(graph, huge, backend; walking_index=router.walking_index)
        end
        overflow_graph, huge = population_gpu_fixture(cells[1:2], [1], [2], [0], [0];
            weights=fill(Float64(floatmax(Float32)), 2))
        overflow_router = PopulationKernelRouter(overflow_graph, huge, backend)
        @test_throws ArgumentError Reachability._route_population_gpu(overflow_router, cells[1], 0, 0; max_walk_ms=0)
        graph.arrival[1] = INF
        @test_throws ArgumentError PopulationKernelRouter(graph, population, backend; walking_index=router.walking_index)
    end

    @testset "Exact UInt32 coverage and projection" begin
        upload(a) = Reachability._population_upload(backend, a)
        labels = fill(INF, 96)
        labels[1:32] = UInt32.(0:31)
        labels[32] = 10
        A = upload(labels)
        coverage = KA.zeros(backend, UInt32, 3)
        cutoffs, nodes = upload(fill(UInt32(10), 32)), upload(Int32[1, 2, 3])
        ptr, to, ms = upload([1, 3, 3, 3]), upload(Int32[2, 3]), upload(UInt32[5, 11])
        Reachability.population_cover!(backend, 256)(coverage, A, A, cutoffs, nodes, ptr, to, ms, UInt32(10); ndrange=96)
        actual = zeros(UInt32, 3)
        KA.copyto!(backend, actual, coverage)
        KA.synchronize(backend)
        @test actual == UInt32[0x800007ff, 0x3f, 0]
        masks, second = upload(UInt32[0x15, 0x2a]), upload(UInt32[0x15, 0x3f, 1])
        persistent = KA.allocate(backend, UInt32, 3)
        for union_mode in (false, true)
            fill!(persistent, union_mode ? UInt32(0) : UInt32(3))
            Reachability.population_project!(backend, 256)(persistent, coverage, masks, 2, union_mode; ndrange=3)
            Reachability.population_project!(backend, 256)(persistent, second, masks, 2, union_mode; ndrange=3)
            KA.copyto!(backend, actual, persistent)
            KA.synchronize(backend)
            @test actual == (union_mode ? UInt32[3, 3, 1] : UInt32[1, 3, 0])
        end
    end

    @testset "Strided Float32 reduction" begin
        rng = MersenneTwister(71)
        weights, bits = rand(rng, 1027), rand(rng, UInt32, 1027)
        masks = UInt32[0x49249249, 0x92492492, 0x24924924]
        upload(a) = Reachability._population_upload(backend, a)
        dw, db, dm = upload(Float32.(weights)), upload(bits), upload(masks)
        partials, totals = KA.allocate(backend, Float32, 9), KA.zeros(backend, Float32, 3)
        for weighted in (false, true)
            fill!(totals, 0f0)
            Reachability.population_reduce!(backend, 256)(partials, db, dw, dm, 3, weighted, 11; ndrange=256 * 9)
            Reachability.population_finish!(backend, 32)(totals, partials, 1, 3; ndrange=3)
            actual = zeros(Float32, 3)
            KA.copyto!(backend, actual, totals)
            KA.synchronize(backend)
            expected = [sum(weights[i] * (weighted ? count_ones(bits[i] & masks[o]) / 11 :
                (bits[i] >> (o - 1)) & UInt32(1)) for i in eachindex(bits)) for o in 1:3]
            @test all(isapprox.(actual, expected; rtol=GPU_FLOAT32_RTOL, atol=GPU_FLOAT32_ATOL))
        end
    end

    @testset "Population-scale Float32 reduction, 96 samples" begin
        rng = MersenneTwister(96)
        n = backend isa KA.CPU ? 3_000_000 : 65_537
        weights = rand(rng, n) .* 40_673
        bits = rand(rng, UInt32, n)
        upload(a) = Reachability._population_upload(backend, a)
        dw, db, masks = upload(Float32.(weights)), upload(bits), upload(UInt32[typemax(UInt32)])
        partials, totals = KA.allocate(backend, Float32, 256), KA.zeros(backend, Float32, 1)
        reduce!() = Reachability.population_reduce!(backend, 256)(partials, db, dw, masks, 256, true, 96; ndrange=256 * 256)
        reduce!() # Compile before timing.
        KA.synchronize(backend)
        elapsed = @elapsed begin
            for _ in 1:3
                reduce!()
                Reachability.population_finish!(backend, 32)(totals, partials, 1, 256; ndrange=1)
            end
            KA.synchronize(backend)
        end
        actual = zeros(Float32, 1)
        KA.copyto!(backend, actual, totals)
        KA.synchronize(backend)
        expected = sum(weights[i] * (count_ones(bits[i]) / 32) for i in eachindex(bits))
        absolute = abs(Float64(actual[1]) - expected)
        @test isapprox(actual[1], expected; rtol=GPU_FLOAT32_RTOL, atol=GPU_FLOAT32_ATOL)
        @info "Population-scale Float32 reduction" backend=typeof(backend) rows=n samples=96 reference=expected max_abs_error=absolute max_rel_error=absolute/expected device_s=elapsed
    end
    absolute, relative = POPULATION_GPU_ERRORS[typeof(backend)]
    @info "Population Float32 fixture parity" backend=typeof(backend) max_abs_error=absolute max_rel_error=relative
end

@testset "Population CLI on CPU" begin
    mktempdir() do dir
        network, population = joinpath(dir, "network.arrow"), joinpath(dir, "population.arrow")
        cell = first(H3.API.cellToChildren(DEMO_ORIGIN, 6))
        nodes = [cell, first(filter(!=(cell), H3.API.gridDisk(cell, 1)))]
        Arrow.write(network, (from_h3=nodes, to_h3=nodes, departure_ms=UInt32[0, 0], duration_ms=Int64[0, 0]))
        Arrow.write(population, (h3=[first(H3.API.cellToChildren(cell, 8))], population=[3.25]))
        project, script = dirname(@__DIR__), joinpath(dirname(@__DIR__), "benchmark-population-gpu.jl")
        origin = string(cell; base=16)
        julia = `$(Base.julia_cmd()) --threads=2 --project=$project`
        output = read(`$julia $script $network $population --backend=cpu $origin 1 0 0 0 0`, String)
        @test occursin("backend=ka_cpu_population", output)
        @test occursin("origins=7 samples=1 walk_h=0", output)
        @test occursin("downloaded=32B", output)
        @test occursin("weight_and_accumulator_precision=Float32", output)
        @test occursin("max_abs_error=0 max_rel_error=0", output)
        @test occursin("median of 3", output)
        output = read(`$julia -L $script -e 'benchmark_population(ARGS, KA.CPU()); benchmark_population([ARGS; "2"], KA.CPU())' $network $population $origin 1 0 0 0`, String)
        @test occursin("walk_h=1", output)
        @test occursin("walk_h=2", output)
        @test occursin("[walk_h=1]", read(`$julia $script --help`, String))
        for args in (String[], [network], fill("unused", 9),
                [network, population, origin, "1", "0", "0", "0", "-1"],
                [network, population, origin, "1", "0", "0", "0", "NaN"],
                [network, population, origin, "1", "0", "0", "0", "1200"])
            @test_throws ProcessFailedException read(pipeline(`$julia $script $args --backend=cpu`, stderr=devnull), String)
        end
    end
    @test !isdefined(Main, :CUDA) || "--backend=cuda" in ARGS
    @test !isdefined(Main, :oneAPI) || "--backend=oneapi" in ARGS
end
