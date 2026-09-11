include("benchmark-population-10k.jl")
include("population-approved3-candidates.jl")
using Test, Random
rng = MersenneTwister(311)
cells = sort!(H3.API.gridDisk(PARIS, 5))
graph = B.pack_graph((from_h3=[cells; rand(rng,cells,400)],
    to_h3=[reverse(cells); rand(rng,cells,400)],
    departure_ms=UInt32[zeros(Int,length(cells)); rand(rng,0:1000:86_399_000,400)],
    duration_ms=Int64[zeros(Int,length(cells)); rand(rng,(0,1000,3_600_000),400)]))
walking = B.prepare_walking(B.WalkingIndex(graph))
population = B._population([first(H3.API.cellToChildren(h,8)) for h in cells],
    [i % 3 == 0 ? 2e-30 : i % 2 == 0 ? 0.1 : 1.0 for i in eachindex(cells)])
prepared = B._prepare_population(population,walking)
base = (B,graph,population,walking)
candidates = (approved_module(:SIMD;labels=true), approved_module(:Vector;labels=true,vector=true),
    approved_module(:Radix;radix=true), candidate_module(:Hints;bins=8,source_root=SNAPSHOT))
@testset "Adaptive candidate whole-query parity" begin
    for mode in (:mean_intersection,:max_intersection,:diff_intersection,:min_union,:diff_union,:reachable_union),
            exclude in (false,true), samples in (1,2,4,5,96), tile in (3,16,64,nothing)
        expected = query10(base;radius=5,mode,exclude,samples,tile)
        for args in candidates
            actual = query10(args;radius=5,mode,exclude,samples,tile)
            @test actual.h3 == expected.h3
            @test iszero.(actual.value) == iszero.(expected.value)
            @test all(isapprox.(actual.value,expected.value;rtol=1e-12,atol=1e-6))
        end
    end
    longstep = fld(Int(B.INF)-3_600_000,96)
    for args in candidates, (start,window,step) in ((0,0,1),(0,96,0),(86_399_999,96,1),(0,96longstep,longstep))
        options = (; origin_radius=2,window_ms=window,step_ms=step,max_walk_ms=3_600_000)
        actual = Base.invokelatest(args[1].route_population,args[2],args[3],PARIS,start,3_600_000;
            walking_index=args[4],options...)
        expected = B.route_population(graph,population,PARIS,start,3_600_000;walking_index=walking,options...)
        @test actual.h3 == expected.h3
        @test isapprox(actual.value,expected.value)
    end
end

@testset "Eight-bin lookup boundaries" begin
    M = candidates[4][1]
    original = M.POPULATION_HINTS[]
    try
        for departure in (UInt32[],UInt32[B.PERIOD+100],UInt32[0,1,10_799_999,10_800_000,86_399_999,86_400_000,172_799_999])
            g = (;edge_to=Int32[1],schedule_ptr=Int32[1,length(departure)+1],departure,arrival=departure .+ UInt32(100))
            M.POPULATION_HINTS[] = population_hints(g,8)
            for ready in UInt32[0,1,10_799_999,10_800_000,86_399_999,B.PERIOD,B.INF-1000], delta in UInt32[0,1,200,999]
                @test M._population_next_arrival(g.schedule_ptr,g.departure,g.arrival,1,ready,ready+delta) ==
                    B.next_arrival(g.schedule_ptr,g.departure,g.arrival,1,ready,ready+delta)
            end
        end
    finally
        M.POPULATION_HINTS[] = original
    end
end
