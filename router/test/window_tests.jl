function window_table(rows; distances=true)
    table = (from_h3=UInt64[DEMO_CELLS[r[1]] for r in rows],
             to_h3=UInt64[DEMO_CELLS[r[2]] for r in rows],
             departure_ms=UInt32[r[3] for r in rows],
             duration_ms=Int64[r[4] for r in rows])
    return distances ? merge(table, (distance_km=Float64[r[5] for r in rows],)) : table
end

# Independent reference: each actual departure gets its own public query and budget.
function naive_window(graph, origin, departure, budget, window; step_ms=60_000)
    elapsed_sum = zeros(UInt64, length(graph.h3))
    reachable_sum = zeros(UInt64, length(graph.h3))
    counts = zeros(UInt32, length(graph.h3))
    distances = zeros(Float64, length(graph.h3))
    samples = 0
    for time in departure:step_ms:(departure + window - 1)
        phase = time % Int(Reachability.PERIOD)
        result = route_details(graph, origin, phase, budget)
        samples += 1
        for vertex in eachindex(result.arrival)
            if result.arrival[vertex] == Reachability.INF
                elapsed_sum[vertex] += UInt64(budget)
            else
                elapsed = UInt64(result.arrival[vertex]) - UInt64(phase)
                elapsed_sum[vertex] += elapsed
                reachable_sum[vertex] += elapsed
                counts[vertex] += UInt32(1)
                distances[vertex] += result.distance_km[vertex]
            end
        end
    end
    return (elapsed_sum_ms=elapsed_sum, elapsed_ms=elapsed_sum ./ samples,
            reachable_elapsed_ms=[counts[v] == 0 ? NaN : reachable_sum[v] / counts[v] for v in eachindex(counts)],
            distance_km=[counts[v] == 0 ? NaN : distances[v] / counts[v] for v in eachindex(counts)],
            reachable_samples=counts, sample_count=UInt32(samples))
end

function check_window(graph, origin, departure, budget, window; step_ms=60_000)
    expected = naive_window(graph, origin, departure, budget, window; step_ms)
    results = map((true, false)) do reuse
        actual = route_window(graph, origin, departure, budget, window; step_ms, reuse)
        @test actual.elapsed_sum_ms == expected.elapsed_sum_ms
        @test actual.reachable_samples == expected.reachable_samples
        @test actual.sample_count === expected.sample_count
        for field in (:elapsed_ms, :reachable_elapsed_ms, :distance_km)
            @test getproperty(actual, field) isa Vector{Float64}
            @test isapprox(getproperty(actual, field), getproperty(expected, field); nans=true)
        end
        @test actual.elapsed_sum_ms isa Vector{UInt64}
        @test actual.reachable_samples isa Vector{UInt32}
        @test actual.searches isa Int
        @test 0 <= actual.searches <= actual.sample_count
        @test actual.reused_samples === Int(actual.sample_count) - actual.searches
        if !reuse
            @test actual.searches == (haskey(graph.node_id, origin) ? actual.sample_count : 0)
        end
        actual
    end
    return first(results)
end

@testset "Window routing" begin
    day = Int(Reachability.PERIOD)
    origin = DEMO_CELLS[1]

    @testset "Moving budget with constant seeds" begin
        graph = pack_graph(window_table([(1, 2, 100, 0, 2.0), (2, 3, 110, 30, 3.0)]))
        result = check_window(graph, origin, 0, 100, 91; step_ms=30)
        target = graph.node_id[DEMO_CELLS[3]]
        @test result.searches == 1
        @test result.reachable_samples[target] == 2
        @test result.elapsed_sum_ms[target] == 330
        @test result.elapsed_ms[target] == 82.5
        @test result.reachable_elapsed_ms[target] == 65.0
        @test result.distance_km[target] == 5.0
    end

    @testset "Exact departures, half-open windows, and zero budget" begin
        graph = pack_graph(window_table([(1, 2, 100, 0, 4.0), (1, 1, 101, 0, 9.0)]))
        result = check_window(graph, origin, 100, 0, 2; step_ms=1)
        source, target = graph.node_id[origin], graph.node_id[DEMO_CELLS[2]]
        @test result.searches == 2
        @test result.reachable_samples[target] == 1
        @test result.reachable_samples[source] == 2
        @test result.elapsed_sum_ms[source] == result.elapsed_ms[source] == 0
        @test result.reachable_elapsed_ms[source] == result.distance_km[source] == 0
        @test result.elapsed_ms[target] == 0
        @test result.distance_km[target] == 4
        @test check_window(graph, origin, 0, 100, 100; step_ms=30).sample_count == 4
        @test check_window(graph, origin, 0, 100, 90; step_ms=30).sample_count == 3
        @test check_window(graph, origin, 0, 100, 1; step_ms=100).sample_count == 1
    end

    @testset "Midnight copies and self edges" begin
        graph = pack_graph(window_table([(1, 2, 10, 20, 1.5), (2, 3, 40, 5, 2.5),
                                        (1, 1, day - 5, 0, 8.0), (1, 1, 0, 0, 9.0)]))
        result = check_window(graph, origin, day - 10, 100, 21; step_ms=10)
        @test result.searches == 1
        @test result.reachable_elapsed_ms[graph.node_id[DEMO_CELLS[3]]] == 45
        changed = check_window(graph, origin, day - 10, day + 100, 31; step_ms=10)
        @test changed.searches == 2
        overnight = pack_graph(window_table([(1, 2, day - 5, 20, 3.0), (2, 3, 20, 10, 4.0)]))
        check_window(overnight, origin, day - 10, 2day, 31; step_ms=10)
    end

    @testset "Overtaking and selected route distances" begin
        graph = pack_graph(window_table([(1, 2, 10, 100, 1.0), (1, 2, 20, 10, 8.0),
                                        (1, 2, 40, 5, 2.0), (2, 3, 50, 5, 3.0),
                                        (1, 3, 0, 60, 0.5)]))
        result = check_window(graph, origin, 0, 100, 41; step_ms=10)
        @test isapprox(result.distance_km[graph.node_id[DEMO_CELLS[2]]], 5.6)
        @test isapprox(result.distance_km[graph.node_id[DEMO_CELLS[3]]], 8.6)
        ties = pack_graph(window_table([(1, 2, 10, 20, 1.0), (1, 2, 20, 10, 9.0),
                                       (1, 2, 20, 10, 4.0)]))
        @test check_window(ties, origin, 0, 100, 21; step_ms=10).distance_km[ties.node_id[DEMO_CELLS[2]]] == 4

        # Retain equal-arrival entries explicitly: normal packing prunes this case.
        base = pack_graph(window_table([(1, 2, 10, 20, 1.0)]))
        equal_arrivals = Graph(base.h3, base.node_id, base.out_ptr, base.edge_from, base.edge_to,
                               Int32[1, 3], UInt32[10, 20], UInt32[30, 30], base.resolution, [1.0, 9.0])
        result = check_window(equal_arrivals, origin, 10, 30, 2; step_ms=1)
        @test result.searches == 2
        @test result.distance_km[base.node_id[DEMO_CELLS[2]]] == 5
    end

    @testset "Unknown distances, absent origins, and empty graphs" begin
        graph = pack_graph(fixture_table())
        result = check_window(graph, origin, 28_800_000, 3_600_000, 180_001)
        @test result.searches == 1
        @test result.distance_km[graph.node_id[origin]] == 0
        @test all(isnan(result.distance_km[v]) for v in eachindex(graph.h3) if graph.h3[v] != origin)
        absent = check_window(graph, DEMO_CELLS[7], 0, 100, 31; step_ms=10)
        @test absent.searches == 0
        @test all(==(100), absent.elapsed_ms)
        @test all(iszero, absent.reachable_samples)
        isolated = check_window(graph, DEMO_CELLS[6], 0, 0, 31; step_ms=10)
        @test isolated.searches == 1
        @test isolated.reachable_samples[graph.node_id[DEMO_CELLS[6]]] == 4
        empty = pack_graph(window_table([]))
        result = check_window(empty, origin, day - 1, 0, 31; step_ms=10)
        @test isempty(result.elapsed_ms)
        @test result.searches == 0
    end

    @testset "Strong reuse and wide integer sums" begin
        graph = pack_graph(window_table([(1, 2, 12 * 3_600_000, 1_000, 2.0)]))
        result = check_window(graph, origin, 0, day, 6 * 3_600_000)
        @test result.sample_count == 360
        @test result.searches == 1
        @test result.reused_samples == 359
        wide = route_window(graph, origin, day - 1, 7day, day; step_ms=1_000)
        @test wide.sample_count === UInt32(86_400)
        @test 1 <= wide.searches <= 2
        @test maximum(wide.elapsed_sum_ms) > typemax(UInt32)
        @test all(==(86_400), wide.reachable_samples)
        target = graph.node_id[DEMO_CELLS[2]]
        train = 12 * 3_600_000
        expected_sum = sum(UInt64((train - (t % day) + day) % day + 1_000)
                           for t in (day - 1):1_000:(2day - 2))
        @test wide.elapsed_sum_ms[target] == expected_sum
        huge = pack_graph(window_table([(1, 2, 1, 0, 1e308), (1, 2, 2, 0, 0.0)]))
        for reuse in (true, false)
            result = route_window(huge, origin, 0, 1, 3; step_ms=1, reuse)
            @test result.distance_km[huge.node_id[DEMO_CELLS[2]]] ≈ (2 / 3) * 1e308
            @test all(isfinite, result.distance_km)
        end
        # The unreachable overflow path must not be explored using the whole window's cutoff.
        bounded = pack_graph(window_table([(1, 2, 0, 100, 1e308), (1, 2, 50, 51, 0.0), (2, 3, 200, 0, 1e308)]))
        result = route_window(bounded, origin, 0, 100, 101; step_ms=50)
        @test result.reachable_samples[bounded.node_id[DEMO_CELLS[3]]] == 0
    end

    @testset "Validation" begin
        graph = pack_graph(window_table([(1, 2, 10, 1, 1.0)]))
        for window in (0, -1, INF, typemax(UInt64))
            @test_throws ArgumentError route_window(graph, origin, 0, 0, window)
        end
        for step in (0, -1, typemin(Int64))
            @test_throws ArgumentError route_window(graph, origin, 0, 0, 1; step_ms=step)
        end
        @test route_window(graph, origin, 0, 0, 86_401; step_ms=1).sample_count == 86_401
        @test route_window(graph, origin, 0, 0, day; step_ms=999).sample_count == cld(day, 999)
        for departure in (-1, day, typemax(UInt64))
            @test_throws ArgumentError route_window(graph, origin, departure, 0, 1)
        end
        for budget in (-1, INF, typemax(UInt64))
            @test_throws ArgumentError route_window(graph, origin, 0, budget, 1)
        end
        @test_throws ArgumentError route_window(graph, UInt64(0), 0, 0, 1)
        @test_throws ArgumentError route_window(graph, H3.API.cellToParent(origin, 4), 0, 0, 1)
        @test route_window(graph, origin, 0, 0, 1; step_ms=typemax(UInt64)).sample_count == 1
        @test route_window(graph, origin, big(0), big(0), big(1); step_ms=big(10)^100).sample_count == 1
    end

    @testset "Random public-query reference" begin
        rng = MersenneTwister(4821)
        for trial in 1:20
            rows = [(rand(rng, 1:5), rand(rng, 1:5), rand(rng, (0, 10, 20, 50, day - 10)),
                     rand(rng, (0, 5, 20, 100, day + 5)), rand(rng) * 10) for _ in 1:24]
            graph = pack_graph(window_table(rows; distances=isodd(trial)))
            start = rand(rng, (0, 10, day - 20))
            budget = rand(rng, (0, 20, 100, day, 7day))
            check_window(graph, origin, start, budget, isodd(trial) ? 41 : 191; step_ms=10)
        end
    end
end
