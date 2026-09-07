# Included after window_tests.jl; use its independent public-query oracle and fixtures.
function check_catchup(graph, origin, departure, budget, window; step_ms=10)
    reference = route_window(graph, origin, departure, budget, window; step_ms)
    naive = naive_window(graph, origin, departure, budget, window; step_ms)
    for chunk_size in (1, 2, 3, 64), workers in (1, 2, 4)
        actual = Reachability.route_window_cached(graph, origin, departure, budget, window;
                                                 step_ms, chunk_size, workers)
        for field in propertynames(reference)
            @test isequal(getproperty(actual, field), getproperty(reference, field))
        end
        @test actual.elapsed_sum_ms == naive.elapsed_sum_ms
        @test actual.reachable_samples == naive.reachable_samples
        @test isapprox(actual.distance_km, naive.distance_km; nans=true)
        @test actual.backend == "catchup"
        @test actual.full_searches == cld(reference.searches, chunk_size)
        @test actual.repair_searches == reference.searches - actual.full_searches
        @test actual.profile_lookups >= 0
        @test actual.routing_expansions >= 0
        @test actual.workers == min(workers, Threads.nthreads(:default), actual.full_searches)
    end
    return reference
end

@testset "Parallel catch-up waves and errors" begin
    rows = [(1, 2, i, 1, Float64(i + 1)) for i in 0:128]
    push!(rows, (2, 3, 150, 10, 10.0))
    graph = pack_graph(window_table(rows))
    expected = route_window_cached(graph, DEMO_CELLS[1], 0, 200, 129; step_ms=1, chunk_size=16, workers=1)
    snapshot = deepcopy(graph)
    for workers in (2, 4, 256, 257, typemax(UInt64))
        actual = route_window_cached(graph, DEMO_CELLS[1], 0, 200, 129; step_ms=1, chunk_size=16, workers)
        @test actual.full_searches == 9
        @test actual.workers == min(workers, Threads.nthreads(:default), 9)
        @test all(isequal(getproperty(actual, key), getproperty(expected, key)) for key in propertynames(expected) if key != :workers)
    end
    @test all(isequal(getfield(graph, key), getfield(snapshot, key)) for key in fieldnames(Graph))
    for workers in (0, -1)
        @test_throws ArgumentError route_window_cached(graph, DEMO_CELLS[1], 0, 200, 129; step_ms=1, workers)
    end
    overflow = pack_graph(window_table([(1, 2, 0, 1, 1e308), (1, 2, 1, 1, 0.0),
                                        (1, 3, 0, 2, 0.0), (1, 3, 1, 1, 0.0),
                                        (2, 4, 2, 8, 1e308), (3, 4, 2, 1, 0.0)]))
    @test_throws ArgumentError route_window_cached(overflow, DEMO_CELLS[1], 0, 20, 2; step_ms=1, chunk_size=1, workers=4)
    recovered = route_window_cached(overflow, DEMO_CELLS[1], 1, 20, 1; step_ms=1, workers=4)
    @test isequal(recovered.distance_km, route_window(overflow, DEMO_CELLS[1], 1, 20, 1; step_ms=1).distance_km)
end

@testset "Downstream catch-up routing" begin
    origin = DEMO_CELLS[1]
    day = Int(Reachability.PERIOD)

    @testset "Arrival catch-up with changed prefix distance" begin
        graph = pack_graph(window_table([(1, 2, 0, 10, 1.0), (1, 2, 10, 10, 9.0),
                                        (1, 2, 20, 10, 3.0), (2, 3, 40, 10, 2.0),
                                        (3, 4, 60, 10, 4.0)]))
        check_catchup(graph, origin, 0, 100, 21)
        independent = Reachability.route_window_cached(graph, origin, 0, 100, 21;
                                                       step_ms=10, chunk_size=1)
        cached = Reachability.route_window_cached(graph, origin, 0, 100, 21;
                                                  step_ms=10, chunk_size=64)
        @test cached.searches == 3
        @test independent.profile_lookups == 9
        @test cached.profile_lookups == 7
        @test cached.routing_expansions < independent.routing_expansions
    end

    @testset "Equal arrival changes winning predecessor" begin
        graph = pack_graph(window_table([(1, 2, 0, 10, 8.0), (1, 2, 10, 20, 1.0),
                                        (1, 3, 20, 0, 2.0), (2, 4, 40, 10, 3.0),
                                        (3, 4, 40, 10, 4.0), (4, 5, 60, 10, 5.0)]))
        check_catchup(graph, origin, 0, 100, 11)
        target = graph.node_id[DEMO_CELLS[4]]
        @test route_details(graph, origin, 0, 100).distance_km[target] == 11.0
        @test route_details(graph, origin, 10, 100).distance_km[target] == 6.0

        # Public Graph construction can retain equal-arrival connections pruned by packing.
        base = pack_graph(window_table([(1, 2, 10, 20, 1.0)]))
        equal = Graph(base.h3, base.node_id, base.out_ptr, base.edge_from, base.edge_to,
                      Int32[1, 3], UInt32[10, 20], UInt32[30, 30], base.resolution, [1.0, 9.0])
        check_catchup(equal, origin, 10, 30, 2; step_ms=1)
    end

    @testset "Zero-time cycles and late discovery of lower IDs" begin
        # Select roles by dense ID, not DEMO_CELLS' origin-first ordering.
        cells = sort(DEMO_CELLS)
        roles = [findfirst(==(cell), DEMO_CELLS) for cell in cells]
        low, middle, high, seed, other, source = roles[1:6]
        graph = pack_graph(window_table([(source, seed, 0, 10, 1.0),
                                        (source, seed, 10, 0, 9.0),
                                        (source, other, 10, 0, 2.0),
                                        (seed, middle, 10, 0, 3.0),
                                        (middle, low, 10, 0, 4.0),
                                        (low, middle, 10, 0, 5.0),
                                        (other, low, 10, 0, 6.0),
                                        (middle, high, 10, 0, 7.0),
                                        (low, low, 10, 0, 8.0)]))
        check_catchup(graph, DEMO_CELLS[source], 0, 30, 12; step_ms=1)
    end

    @testset "Moving cutoffs and overnight connections" begin
        graph = pack_graph(window_table([(1, 2, 0, 10, 1.0), (1, 2, 10, 90, 2.0),
                                        (2, 3, 15, 5, 3.0), (2, 3, 100, 10, 4.0),
                                        (3, 4, 25, 5, 5.0)]))
        check_catchup(graph, origin, 0, 25, 21)
        midnight = pack_graph(window_table([(1, 2, day - 10, 5, 1.0),
                                           (1, 2, 0, 5, 2.0), (1, 2, 10, 5, 3.0),
                                           (2, 3, 20, 5, 4.0), (3, 4, 30, 5, 5.0)]))
        check_catchup(midnight, origin, day - 10, 100, 41)
        check_catchup(midnight, origin, day - 10, 2day, 41)
        check_catchup(midnight, origin, day - 10, 0, 23; step_ms=3)
    end

    @testset "Unknown distances, absent origins, and validation" begin
        graph = pack_graph(window_table([(1, 2, 0, 10, 1.0), (1, 2, 10, 10, 2.0),
                                        (2, 3, 30, 10, 3.0)]; distances=false))
        check_catchup(graph, origin, 0, 100, 23)
        check_catchup(graph, DEMO_CELLS[7], 0, 100, 23)
        check_catchup(pack_graph(window_table([])), origin, 0, 100, 23)
        for size in (0, -1, 257, typemax(UInt64))
            @test_throws ArgumentError Reachability.route_window_cached(graph, origin, 0, 100, 23; chunk_size=size)
        end
        for (departure, budget, window, step) in ((-1, 100, 23, 10), (day, 100, 23, 10),
                                                 (0, -1, 23, 10), (0, 100, 0, 10),
                                                 (0, 100, 23, 0))
            @test_throws ArgumentError Reachability.route_window_cached(graph, origin, departure,
                                                                        budget, window; step_ms=step)
        end
    end

    @testset "Overflow matches reference scope and temporary improvements" begin
        bounded = pack_graph(window_table([(1, 2, 0, 100, 1e308), (1, 2, 50, 51, 0.0),
                                          (2, 3, 200, 0, 1e308)]))
        check_catchup(bounded, origin, 0, 100, 101; step_ms=50)
        transient = pack_graph(window_table([(1, 2, 0, 1, 1e308), (1, 3, 0, 2, 0.0),
                                            (2, 4, 1, 9, 1e308), (3, 4, 2, 1, 0.0)]))
        @test_throws ArgumentError route_window(transient, origin, 0, 20, 1)
        @test_throws ArgumentError Reachability.route_window_cached(transient, origin, 0, 20, 1)
    end

    @testset "Random differential and chronological means" begin
        rng = MersenneTwister(82714)
        for trial in 1:20
            rows = [(rand(rng, 1:5), rand(rng, 1:5),
                     rand(rng, (0, 10, 20, 50, day - 10)),
                     rand(rng, (0, 5, 20, 100, day + 5)), rand(rng) * 100)
                    for _ in 1:30]
            graph = pack_graph(window_table(rows; distances=isodd(trial)))
            check_catchup(graph, origin, rand(rng, (0, 10, day - 20)),
                          rand(rng, (0, 20, 100, day, 7day)), 73; step_ms=10)
        end
    end
end
@testset "Catch-up default chunk boundary" begin
    rows = [(1, 2, i, 1, Float64(i + 1)) for i in 0:64]
    push!(rows, (2, 3, 80, 10, 10.0))
    graph = pack_graph(window_table(rows))
    expected = route_window(graph, DEMO_CELLS[1], 0, 100, 65; step_ms=1)
    actual = route_window_cached(graph, DEMO_CELLS[1], 0, 100, 65; step_ms=1)
    @test actual.full_searches == 2
    @test actual.searches == 65
    @test all(isequal(getproperty(actual, name), getproperty(expected, name)) for name in propertynames(expected))
end
