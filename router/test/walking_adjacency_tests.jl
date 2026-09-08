# Included inside WalkingTests to reuse the raw fixtures and independent walk metric.
@testset "Resident walking adjacency" begin
    for res in (7, 9), distances in (false, true)
        a, b, c, seconds = chain(res)
        ab = walk(a, b).ms
        rows = [(1, 2, 0, ab, 7.0), (2, 2, ab, 0, 3.0),
                (2, 3, ab + 10, 0, 4.0), (1, 3, 0, ab + 10, 9.0)]
        graph = pack_graph(raw_table([a, b, c], rows; distances))
        bare = WalkingIndex(graph)
        prepared = prepare_walking(bare)
        serial = prepare_walking(bare; workers=1)
        @test isnothing(bare.prepared)
        @test prepared.prepared.limit == 3_600_000
        for field in (:geographic, :graph), column in (:offsets, :targets, :durations, :distances)
            @test getproperty(getproperty(prepared.prepared, field), column) ==
                  getproperty(getproperty(serial.prepared, field), column)
        end
        shared = Reachability.WalkingGeometryCache()
        topology = Reachability.WalkingTopology(prepared, 3_600_000, shared)
        for h in graph.h3
            geo = Reachability._walking_hops(topology, h; geographic=true)
            net = Reachability._walking_hops(topology, h)
            @test geo isa Reachability.WalkingRange{UInt64}
            @test net isa Reachability.WalkingRange{Int32}
            @test collect(geo) == walking_cells(bare, h)
            @test [(cell=graph.h3[x.cell], duration_ms=x.duration_ms, distance_km=x.distance_km)
                   for x in net] == walking_neighbors(bare, h)
            @test all(x -> x.cell != h, geo)
        end
        snapshot = deepcopy(prepared)
        offgraph = first(setdiff(disk(a, 1), graph.h3))
        for origin in (a, b, offgraph), limit in (0, seconds ÷ 2, seconds, 3600, 3601),
            budget in (ab - 1, ab, 7_200_000)
            expected = route_walking(graph, origin, 0, budget; max_walk_ms=1000limit, walking_index=bare)
            @test isequal(expected, route_walking(graph, origin, 0, budget;
                                                 max_walk_ms=1000limit, walking_index=prepared))
            expected_window = route_window_walking(graph, origin, 0, budget, 3;
                step_ms=1, max_walk_ms=1000limit, walking_index=bare)
            for route in (route_window_walking, route_window_walking_cached)
                actual = route(graph, origin, 0, budget, 3;
                    step_ms=1, max_walk_ms=1000limit, walking_index=prepared)
                for field in (:h3, :elapsed_sum_ms, :elapsed_ms, :reachable_elapsed_ms,
                              :reachable_samples, :distance_km, :sample_count)
                    @test isequal(getproperty(actual, field), getproperty(expected_window, field))
                end
            end
        end
        # Covered routes never enter either local cache or the shared lock registry.
        for origin in graph.h3
            Reachability._walking_route_at(graph, topology, origin, UInt32(0), UInt32(7_200_000))
        end
        @test isempty(topology.neighbors) && isempty(topology.coverage) && isempty(shared.entries)
        tasks = [Threads.@spawn route_walking(graph, origin, 1, 7_200_000;
                  walking_index=prepared) for origin in (a, b, c, a)]
        for (task, origin) in zip(tasks, (a, b, c, a))
            @test isequal(fetch(task), route_walking(graph, origin, 1, 7_200_000; walking_index=bare))
        end
        for field in (:geographic, :graph), column in (:offsets, :targets, :durations, :distances)
            @test getproperty(getproperty(prepared.prepared, field), column) ==
                  getproperty(getproperty(snapshot.prepared, field), column)
        end
        fallback = Reachability.WalkingTopology(prepared, 3_601_000)
        Reachability._walking_hops(fallback, a)
        Reachability._walking_hops(fallback, offgraph; geographic=true, limit=1000)
        @test haskey(fallback.neighbors, a) && haskey(fallback.coverage, offgraph)
        other = pack_graph(raw_table([a], [(1, 1, 0, 0, 0.0)]))
        @test_throws ArgumentError route_walking(other, a, 0, 0; walking_index=prepared)
        @test_throws ArgumentError route_window_walking_cached(other, a, 0, 0, 1; walking_index=prepared)
        for bad in (-1, Reachability.INF, typemax(UInt64))
            @test_throws ArgumentError prepare_walking(bare; max_walk_ms=bad)
        end
        @test_throws ArgumentError prepare_walking(bare; workers=0)
        @test isempty(prepare_walking(bare; max_walk_ms=0).prepared.geographic.targets)
    end
end
