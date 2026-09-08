function window_gpu_table(nodes, sources, targets, departures, durations; km=nothing)
    table = (from_h3=UInt64[nodes[i] for i in sources],
             to_h3=UInt64[nodes[i] for i in targets],
             departure_ms=UInt32.(departures), duration_ms=Int64.(durations))
    return isnothing(km) ? table : merge(table, (distance_km=Float64.(km),))
end

function window_gpu_parity(router, origin, departure, budget, window; step_ms=1)
    graph = router.parent.graph
    expected = route_window(graph, origin, departure, budget, window; step_ms)
    actual = Reachability.route_window_kernel!(router, origin, departure, budget, window; step_ms)
    for key in (:elapsed_ms, :reachable_elapsed_ms, :distance_km, :reachable_samples,
                :sample_count, :searches, :reused_samples, :elapsed_sum_ms)
        @test isequal(getproperty(actual, key), getproperty(expected, key))
    end
    @test actual.backend == (router.parent.backend isa KA.CPU ? "ka_cpu_batched" : "gpu_batched")
    @test actual.full_searches == actual.searches
    @test actual.repair_searches == 0
    @test actual.batches == (isempty(graph.edge_to) ? 0 : cld(actual.searches, router.batch_size))
    @test actual.batches <= actual.rounds <= actual.batches * length(graph.h3)
    return actual
end

window_gpu_backends = Any[KA.CPU()]
if "--backend=oneapi" in ARGS
    push!(window_gpu_backends, oneAPI.oneAPIBackend())
end

@testset "Kernel host window modes" for backend in window_gpu_backends
    graph = pack_graph(window_table([(1, 2, 0, 10, 1.0), (1, 2, 1, 1, 5.0),
        (2, 3, 10, 1, 2.0), (1, 4, 0, 0, 4.0)]))
    router = WindowKernelRouter(graph, backend; batch_size=2)
    for distance_mode in (:itinerary, :straight_line),
            window_mode in (:mean_intersection, :min_union, :max_intersection, :diff_union, :reachable_union)
        options = (; step_ms=1, window_mode, distance_mode)
        expected = route_window(graph, DEMO_ORIGIN, 0, 10, 2; options...)
        actual = route_window_kernel!(router, DEMO_ORIGIN, 0, 10, 2; options...)
        for field in (:elapsed_ms, :reachable_elapsed_ms, :distance_km, :reachable_samples, :sample_count, :elapsed_sum_ms)
            @test isequal(getproperty(actual, field), getproperty(expected, field))
        end
    end
end

@testset "Batched windows: $(typeof(backend))" for backend in window_gpu_backends
    nodes = sort!(filter(h -> h != 0 && H3.API.isValidCell(h) != 0,
                        H3.API.gridDisk(parse(UInt64, "85075dd7fffffff"; base=16), 1)))
    period = Int(Reachability.PERIOD)
    @testset "Groups, padding, reset, and canonical distances" begin
        # Seven distinct first hops, overtaking, tied parents, and zero-time cycles.
        sources = [fill(1, 7); 1; 2; 2; 3; 4; 3; 5; 6]
        targets = [fill(2, 7); 2; 3; 4; 4; 3; 5; 6; 6]
        departures = [collect(0:6); 0; 6; 6; 6; 6; period - 1; 0; 0]
        durations = [zeros(Int, 7); 100; 0; 0; 0; 0; 2; 0; 0]
        for km in (nothing, [collect(0.1:0.1:0.7); 9; 0.3; 0.7; 0.2; 0.8; 1.1; 2.3; 0]),
            batch in (1, 3, 8), check in (1, 4)
            graph = pack_graph(window_gpu_table(nodes, sources, targets, departures, durations; km))
            parent = KernelRouter(graph, backend)
            router = Reachability.WindowKernelRouter(parent; batch_size=batch, check_every=check)
            @test router.parent === parent
            @test router.parent.edge_from === parent.edge_from
            first = window_gpu_parity(router, nodes[1], 0, 20, 7)
            @test first.searches == 7
            snapshot = deepcopy(first)
            for (origin, departure, budget, window) in (
                (nodes[2], 0, 10, 9), (nodes[1], 0, 0, 7),
                (nodes[3], period - 2, 10, 7),
                (nodes[1], period - 1, 604_800_000, 9),
                (nodes[7], 0, 10, 7), (nodes[1], 0, 20, 7))
                window_gpu_parity(router, origin, departure, budget, window)
            end
            @test isequal(first, snapshot)
        end
    end

    @testset "Full-depth convergence" begin
        graph = pack_graph(window_gpu_table(nodes, 1:6, 2:7, zeros(Int, 6), zeros(Int, 6);
                                            km=fill(0.1, 6)))
        for check in (1, 4, 32)
            router = Reachability.WindowKernelRouter(graph, backend; check_every=check)
            result = window_gpu_parity(router, nodes[1], 0, 0, 1)
            @test result.rounds == 7
        end
    end

    @testset "Empty, edgeless, and validation" begin
        empty_graph = pack_graph(window_gpu_table(nodes, Int[], Int[], Int[], Int[]))
        for km in (nothing, Float64[])
            isolated = Graph(UInt64[nodes[1]], Dict(nodes[1] => Int32(1)), Int32[1, 1],
                             Int32[], Int32[], Int32[1], UInt32[], UInt32[], 5, km)
            for graph in (empty_graph, isolated)
                router = Reachability.WindowKernelRouter(graph, backend)
                @test router.batch_size == 64
                @test router.check_every == 4
                for origin in nodes[1:2]
                    result = window_gpu_parity(router, origin, 123, 0, 7)
                    @test result.batches == result.rounds == 0
                end
                for (b, c) in ((0, 4), (257, 4), (64, 0), (64, 33))
                    @test_throws ArgumentError Reachability.WindowKernelRouter(router.parent; batch_size=b, check_every=c)
                end
                for (origin, dep, budget, window, step) in (
                    (UInt64(0), 0, 0, 1, 1), (nodes[1], -1, 0, 1, 1),
                    (nodes[1], period, 0, 1, 1), (nodes[1], 0, -1, 1, 1),
                    (nodes[1], 0, 0, 0, 1), (nodes[1], 0, 0, 1, 0))
                    @test_throws ArgumentError Reachability.route_window_kernel!(router, origin, dep, budget, window; step_ms=step)
                end
            end
        end
    end

    @testset "Distance overflow, including temporary labels" begin
        for last_duration in (0, 20)
            graph = pack_graph(window_gpu_table(nodes, [1, 2, 1, 3], [2, 4, 3, 4],
                [0, 0, 0, 1], [0, 10, 1, last_duration];
                km=[floatmax(Float64), floatmax(Float64), 1.0, 1.0]))
            router = Reachability.WindowKernelRouter(graph, backend; batch_size=3)
            @test_throws ArgumentError route_window(graph, nodes[1], 0, 20, 1)
            @test_throws ArgumentError Reachability.route_window_kernel!(router, nodes[1], 0, 20, 1)
        end
    end

    @testset "Seeded random window parity" begin
        rng = MersenneTwister(0x57494e44)
        for sample in 1:8
            sources = [collect(1:6); rand(rng, 1:5, 30)]
            targets = [collect(1:6); rand(rng, 1:5, 30)]
            departures = [zeros(Int, 6); rand(rng, 0:20, 30)]
            durations = [zeros(Int, 6); rand(rng, 0:10, 30)]
            km = isodd(sample) ? rand(rng, 36) : nothing
            graph = pack_graph(window_gpu_table(nodes, sources, targets, departures, durations; km))
            router = Reachability.WindowKernelRouter(graph, backend; batch_size=3)
            for origin in nodes[[1, 3, 6, 7, 1]], step in (1, 3)
                window_gpu_parity(router, origin, 0, 20, 23; step_ms=step)
            end
        end
    end
end
@testset "Batched default-width boundary" for backend in ("--backend=oneapi" in ARGS ? [KA.CPU(), oneAPI.oneAPIBackend()] : [KA.CPU()])
    rows = [(1, 2, i, 1, Float64(i + 1)) for i in 0:64]
    push!(rows, (2, 3, 80, 10, 10.0))
    graph = pack_graph(window_table(rows))
    expected = route_window(graph, DEMO_CELLS[1], 0, 100, 65; step_ms=1)
    actual = route_window_kernel!(WindowKernelRouter(graph, backend), DEMO_CELLS[1], 0, 100, 65; step_ms=1)
    @test actual.batches == 2
    @test actual.searches == 65
    @test all(isequal(getproperty(actual, name), getproperty(expected, name)) for name in propertynames(expected))
end
