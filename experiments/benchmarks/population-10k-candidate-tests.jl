include("benchmark-population-10k.jl")
include("population-10k-candidates.jl")
using Random, Test
rng = MersenneTwister(17)
nodes = sort!(H3.API.gridDisk(PARIS,1))
graph = B.pack_graph((from_h3=[nodes;rand(rng,nodes,60)],to_h3=[nodes;rand(rng,nodes,60)],
    departure_ms=UInt32[zeros(Int,7);rand(rng,0:900_000:86_399_000,60)],
    duration_ms=Int64[zeros(Int,7);rand(rng,(0,1000,3_600_000),60)]))
walking = B.prepare_walking(B.WalkingIndex(graph))
population = B._population([first(H3.API.cellToChildren(h,8)) for h in H3.API.gridDisk(PARIS,5)], fill(0.1,91))
prepared = B._prepare_population(population,walking)
base = (B,graph,population,walking)
const CANDIDATES = (candidate_module(:Expiry;expiry=true),candidate_module(:H8;bins=8),candidate_module(:H24;bins=24))
@testset "Experimental population coverage and hints" begin
    for args in CANDIDATES
        for mode in (:mean_intersection,:max_intersection,:diff_intersection,:min_union,:diff_union,:reachable_union), exclude in (false,true), tile in (3,16,64)
            expected = query10(base;radius=2,mode,exclude,tile)
            actual = query10(args;radius=2,mode,exclude,tile)
            @test actual.h3 == expected.h3
            @test iszero.(actual.value) == iszero.(expected.value)
            @test all(isapprox.(actual.value,expected.value;rtol=1e-12,atol=1e-12))
        end
    end
end

@testset "Expiry forward point oracle" begin
    M = CANDIDATES[1][1]
    a, b, c = nodes[1:3]
    modes = (:mean_intersection,:max_intersection,:diff_intersection,:min_union,:diff_union,:reachable_union)
    for (departures,durations) in (([0,20,80,95],zeros(Int64,4)),
            ([0,10,20,30,40,50,60,70,80,90,100],zeros(Int64,11)), ([89,95],Int64[10,5])),
            walk in (0,3_600_000), budget in (0,10,3_600_000),
            (start,step) in ((0,1),(Int(B.PERIOD)-96,1),(0,fld(Int(B.INF)-3_600_000,96)))
        g = M.pack_graph((from_h3=[fill(a,length(departures));fill(c,length(departures));a;b;c],
            to_h3=[fill(b,2length(departures));a;b;c],
            departure_ms=UInt32[departures;departures;0;0;0], duration_ms=Int64[durations;durations;0;0;0]))
        w = M.prepare_walking(M.WalkingIndex(g))
        p = M._population([first(H3.API.cellToChildren(h,8)) for h in (a,b,c)], [0.1,1.0,2e-30])
        weights = M._population_rollup(p,7)
        topology = M.WalkingTopology(w,min(walk,budget))
        origins = sort!(H3.API.gridDisk(a,1))
        counts = [Dict{UInt64,Int}() for _ in origins]
        for (i, origin) in enumerate(origins), sample in 0:95
            ready = UInt32(start + sample * step)
            result = M._walking_route_at(g,topology,origin,ready,ready+UInt32(budget),false)
            for cell in result.h3
                counts[i][cell] = get(counts[i],cell,0)+1
            end
        end
        for mode in modes, exclude in (false,true)
            result = M.route_population(g,p,a,start,budget; walking_index=w, origin_radius=1,
                window_ms=96step,step_ms=step,max_walk_ms=walk,window_mode=mode,exclude_origin_population=exclude)
            expected = [sum((get(weights,cell,0.0) * (mode == :reachable_union ? hits/96 :
                mode in (:min_union,:diff_union) ? 1 : hits == 96)
                for (cell,hits) in counts[i] if !exclude || cell != origins[i]); init=0.0) for i in eachindex(origins)]
            @test result.h3 == origins
            @test iszero.(result.value) == iszero.(expected)
            @test all(isapprox.(result.value,expected;rtol=1e-12,atol=0))
        end
    end
end

@testset "Empty and next-day-only schedule profiles" begin
    for args in CANDIDATES[2:3]
        M = args[1]
        original = M.POPULATION_HINTS[]
        try
            for departure in (UInt32[],UInt32[B.PERIOD+100])
                g = (; edge_to=Int32[1],schedule_ptr=Int32[1,length(departure)+1],
                    departure,arrival=departure .+ UInt32(100))
                M.POPULATION_HINTS[] = population_hints(g,M.POPULATION_BINS)
                for ready in UInt32[0,99,100,101,B.PERIOD-1,B.PERIOD,B.INF-1000], delta in UInt32[0,1,200,999]
                    cutoff = ready + delta
                    @test M._population_next_arrival(g.schedule_ptr,g.departure,g.arrival,1,ready,cutoff) ==
                        B.next_arrival(g.schedule_ptr,g.departure,g.arrival,1,ready,cutoff)
                end
            end
        finally
            M.POPULATION_HINTS[] = original
        end
    end
end

const CROSSINGS = candidate_module(:Crossings; expiry=true)[1]
@testset "Expiry full-width origin masks" begin
    M = CROSSINGS
    cells = sort!(H3.API.gridDisk(PARIS,5))
    g = M.pack_graph((from_h3=repeat(cells,2),to_h3=fill(last(cells),2length(cells)),
        departure_ms=UInt32[fill(89,length(cells));fill(95,length(cells))],
        duration_ms=fill(Int64(10),2length(cells))))
    w = M.prepare_walking(M.WalkingIndex(g);max_walk_ms=0)
    p = M._population([first(H3.API.cellToChildren(h,8)) for h in cells],
        [fill(2e-30,length(cells)-1);1.0])
    for mode in (:mean_intersection,:max_intersection,:diff_intersection,:min_union,:diff_union,:reachable_union), exclude in (false,true)
        options = (; walking_index=w,origin_radius=5,window_ms=96,step_ms=1,max_walk_ms=0,
            window_mode=mode,exclude_origin_population=exclude,origin_batch_size=64)
        actual = M.route_population(g,p,PARIS,0,10;options...)
        expected = M._route_population_reference(g,p,PARIS,0,10;options...)
        @test actual.h3 == expected.h3
        @test iszero.(actual.value) == iszero.(expected.value)
        @test all(isapprox.(actual.value,expected.value;rtol=1e-12,atol=0))
    end
end
@testset "Incremental population expiry and reactivation" begin
    M = CROSSINGS
    a, b = nodes[1:2]
    g = Base.invokelatest(M.pack_graph, (from_h3=fill(a,3), to_h3=fill(b,3),
        departure_ms=UInt32[0,20,80], duration_ms=Int64[0,0,0]))
    w = Base.invokelatest(M.prepare_walking, Base.invokelatest(M.WalkingIndex,g); max_walk_ms=0)
    p = Base.invokelatest(M._population, [first(H3.API.cellToChildren(b,8))], [0.1])
    for mode in (:mean_intersection,:max_intersection,:diff_intersection,:min_union,:diff_union,:reachable_union), exclude in (false,true)
        result = Base.invokelatest(M.route_population,g,p,a,0,10; walking_index=w,
            window_ms=96,step_ms=1,max_walk_ms=0,window_mode=mode,exclude_origin_population=exclude)
        expected = mode == :reachable_union ? 0.1 * 23/96 : mode in (:min_union,:diff_union) ? 0.1 : 0.0
        @test iszero(only(result.value)) == iszero(expected)
        @test isapprox(only(result.value), expected; rtol=1e-12,atol=1e-12)
    end
end
