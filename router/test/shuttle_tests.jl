@testset "Elvas-Badajoz load-time rail repair" begin
    for (res, elvas, badajoz) in ((5, 0x85390287fffffff, 0x853902bbfffffff),
                                (6, 0x863902857ffffff, 0x863902ba7ffffff),
                                (7, 0x873902851ffffff, 0x873902ba1ffffff))
        shuttle = Reachability._badajoz_shuttle(res)
        @test length(shuttle.from_h3) == 2342
        @test shuttle.from_h3 == [fill(elvas, 1171); fill(badajoz, 1171)]
        @test shuttle.to_h3 == [fill(badajoz, 1171); fill(elvas, 1171)]
        @test shuttle.departure_ms[1:1171] == shuttle.departure_ms[1172:end] == 14_400_000:60_000:84_600_000
        @test all(==(900_000), shuttle.duration_ms)
        @test all(==(13.88), shuttle.distance_km)
        # An unrelated source edge must survive, and the caller's columns must not change.
        a, b = H3.API.latLngToCell.(H3.API.cellToLatLng.(DEMO_CELLS[1:2]), res)
        table = (from_h3=UInt64[a], to_h3=UInt64[b], departure_ms=UInt32[START],
                 duration_ms=Int64[10], distance_km=[2.0])
        original = deepcopy(table)
        plain = pack_graph(table)
        graph = pack_graph(table; badajoz_shuttle=true)
        @test table == original
        @test !(elvas in plain.h3) && !(badajoz in plain.h3)
        @test route_details(graph, a, START, 10).arrival[graph.node_id[b]] ==
              route_details(plain, a, START, 10).arrival[plain.node_id[b]]
        @test graph.resolution == res
        expected_window = route_window(graph, elvas, START, 900_000, 120_000; step_ms=60_000)
        for result in (route_window_cached(graph, elvas, START, 900_000, 120_000; step_ms=60_000),
                       route_window_kernel!(WindowKernelRouter(KernelRouter(graph, KA.CPU())),
                                            elvas, START, 900_000, 120_000; step_ms=60_000))
            for name in (:elapsed_ms, :reachable_elapsed_ms, :distance_km, :reachable_samples, :sample_count)
                @test isequal(getproperty(result, name), getproperty(expected_window, name))
            end
        end
        for (from, to) in ((elvas, badajoz), (badajoz, elvas))
            for (ready, arrival) in ((14_340_000, 15_300_000), (14_400_000, 15_300_000),
                                     (84_600_000, 85_500_000), (84_600_001, P + 15_300_000),
                                     (0, 15_300_000), (START, START + 900_000))
                budget = arrival - ready
                result = route_details(graph, from, ready, budget)
                @test result.arrival[graph.node_id[to]] == arrival
                @test result.distance_km[graph.node_id[to]] == 13.88
                @test route_cpu(graph, from, ready, budget - 1)[graph.node_id[to]] == INF
                @test route_kernel!(KernelRouter(graph, KA.CPU()), from, ready, budget) == result.arrival
            end
        end
        # Merge with genuine faster service; duplicates and slower services are pruned normally.
        faster = (from_h3=UInt64[elvas, elvas], to_h3=UInt64[badajoz, badajoz],
                  departure_ms=UInt32[START, START + 60_000],
                  duration_ms=Int64[1_800_000, 60_000], distance_km=[14.0, 12.0])
        merged = pack_graph(map(vcat, table, faster); badajoz_shuttle=true)
        result = route_details(merged, elvas, START, 900_000)
        @test result.arrival[merged.node_id[badajoz]] == START + 120_000
        @test result.distance_km[merged.node_id[badajoz]] == 12.0
        duplicate = pack_graph(map(vcat, shuttle, shuttle))
        @test duplicate.departure == pack_graph(shuttle).departure
        legacy = pack_graph(NamedTuple{(:from_h3, :to_h3, :departure_ms, :duration_ms)}(table); badajoz_shuttle=true)
        @test isnothing(legacy.distance_km)
        mktempdir() do dir
            path = joinpath(dir, "shuttle.arrow")
            Arrow.write(path, table)
            before = read(path)
            restored = pack_graph(path; skip_invalid_durations=true, badajoz_shuttle=true)
            @test restored.departure == graph.departure
            @test restored.distance_km == graph.distance_km
            @test pack_graph(path).h3 == plain.h3
            @test read(path) == before
        end
        # In-process handler only: no server or network connection.
        handler = make_handler(graph)
        for mode in ("itinerary", "straight_line"), window in ("", "&window_h=0.03333333333333333&step_h=0.016666666666666666")
            response = handler(HTTP.Request("GET", "/reachable?index=$(string(elvas; base=16))&departure_h=8&budget_h=0.25&max_walk_h=0&encoding=string&metric=distance_time_quantile&distance_mode=$mode$window"))
            @test response.status == 200
            output = Arrow.Table(response.body)
            at = findfirst(==(string(badajoz; base=16)), output.index)
            @test output.elapsed_h[at] == 0.25
            @test output.distance_km[at] ≈ (mode == "itinerary" ? 13.88 : only(Reachability._od_distances(elvas, [badajoz])))
            @test mode != "straight_line" || output.distance_km[at] != 13.88
        end
        if res == 7
            km(a, b) = only(Reachability._od_distances(a, [b]))
            origin = argmax(h -> km(badajoz, h), filter(!iszero, H3.API.gridDisk(elvas, 1)))
            target = argmax(h -> km(elvas, h), filter(!iszero, H3.API.gridDisk(badajoz, 1)))
            access, egress = ceil.(Int, 720_000 .* (km(origin, elvas), km(badajoz, target)))
            seconds = cld(max(access, egress), 1000)
            @test km(origin, badajoz) * 720_000 > seconds * 1000
            @test km(elvas, target) * 720_000 > seconds * 1000
            arrival = cld(START + access, 60_000) * 60_000 + 900_000 + egress
            budget = arrival - START
            walked = route_walking(graph, origin, START, budget; max_walk_ms=1000seconds)
            at = findfirst(==(target), walked.h3)
            @test walked.arrival[at] == arrival
            @test walked.distance_km[at] ≈ km(origin, elvas) + 13.88 + km(badajoz, target)
            @test !(target in route_walking(plain, origin, START, budget; max_walk_ms=1000seconds).h3)
        end
    end
    # Coalesced stations retain a scheduled transit self-edge, just like source self-edges.
    coarse = Reachability._badajoz_shuttle(0)
    @test first(coarse.from_h3) == first(coarse.to_h3)
    graph = pack_graph(coarse; badajoz_shuttle=true)
    @test length(graph.edge_to) == 1
    @test length(graph.departure) == 2342 # one direction, two days after duplicate pruning
    column = Reachability._PatchedColumn(UInt32[1, 2], UInt32[3])
    @test collect(column) == UInt32[1, 2, 3]
    @test_throws BoundsError column[0]
    @test_throws BoundsError column[4]
end
