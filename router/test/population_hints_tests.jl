@testset "Graph-specific population schedule bounds" begin
    a, b = DEMO_CELLS[1:2]
    make_graph(departures) = pack_graph((from_h3=fill(a,length(departures)),
        to_h3=fill(b,length(departures)),departure_ms=UInt32.(departures),
        duration_ms=fill(Int64(100),length(departures))))
    graph = make_graph([0,1,10_799_999,10_800_000,86_399_999])
    other = make_graph([200,300])
    cells = [first(H3.API.cellToChildren(h,8)) for h in (a,b)]
    population = Reachability._population(cells,[0.1,2e-30])
    hints = Reachability._population_schedule_hints(population,graph)
    @test hints === Reachability._population_schedule_hints(population,graph)
    @test hints !== Reachability._population_schedule_hints(population,other)
    @test length(population.schedule_hints) == 2
    cold = make_graph([400])
    tasks = [Threads.@spawn Reachability._population_schedule_hints(population,cold) for _ in 1:8]
    @test all(task -> fetch(task) === fetch(first(tasks)),tasks)
    rng = MersenneTwister(313)
    profiles = [Graph(graph.h3,graph.node_id,graph.out_ptr,graph.edge_from,graph.edge_to,
        Int32[1,length(departure)+1],departure,departure .+ UInt32(100),graph.resolution,graph.distance_km)
        for departure in (UInt32[],UInt32[P+100])]
    for g in (graph,other,profiles...)
        bounds = Reachability._population_schedule_hints(population,g)
        for ready in UInt32[0,1,10_799_999,10_800_000,86_399_999,P,7P,INF-1000], delta in UInt32[0,1,200,999]
            @test Reachability._population_next_arrival(bounds,g,1,ready,ready+delta) ==
                Reachability.next_arrival(g.schedule_ptr,g.departure,g.arrival,1,ready,ready+delta)
        end
        for _ in 1:1000
            ready = rand(rng,UInt32(0):INF-UInt32(1))
            cutoff = ready + rand(rng,UInt32(0):INF-ready-UInt32(1))
            @test Reachability._population_next_arrival(bounds,g,1,ready,cutoff) ==
                Reachability.next_arrival(g.schedule_ptr,g.departure,g.arrival,1,ready,cutoff)
        end
    end
    # The same geometry can serve graphs with different timetables.
    index = prepare_walking(WalkingIndex(graph))
    for g in (graph,other), mode in (:mean_intersection,:max_intersection,:diff_intersection,:min_union,:diff_union,:reachable_union),
            exclude in (false,true)
        options = (;walking_index=index,window_ms=96,step_ms=1,max_walk_ms=0,
            window_mode=mode,exclude_origin_population=exclude)
        actual = route_population(g,population,a,0,100;options...)
        expected = Reachability._route_population_reference(g,population,a,0,100;options...)
        @test actual.h3 == expected.h3
        @test iszero.(actual.value) == iszero.(expected.value)
        @test all(isapprox.(actual.value,expected.value;rtol=1e-12,atol=0))
    end
end
