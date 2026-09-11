include("benchmark-population-10k.jl")
include("population-approved3-candidates.jl")
using Test, Random
rng = MersenneTwister(411)
cells = sort!(H3.API.gridDisk(PARIS,5))
graph = B.pack_graph((from_h3=[cells;rand(rng,cells,400)],to_h3=[reverse(cells);rand(rng,cells,400)],
    departure_ms=UInt32[zeros(Int,length(cells));rand(rng,0:1000:86_399_000,400)],
    duration_ms=Int64[zeros(Int,length(cells));rand(rng,(0,1000,3_600_000),400)]))
walking = B.prepare_walking(B.WalkingIndex(graph))
population = B._population([first(H3.API.cellToChildren(h,8)) for h in cells],
    [i%3 == 0 ? 2e-30 : i%2 == 0 ? 0.1 : 1.0 for i in eachindex(cells)])
prepared = B._prepare_population(population,walking)
base = (B,graph,population,walking)
candidates = [approved_module(Symbol("Followup_$(gate)_$(update)_$(labels)");gate,labels,update)
    for gate in (:quarter,:half,:full) for (labels,update) in ((true,false),(false,true),(true,true))]
append!(candidates,[approved_module(Symbol("Cutoff_$kind");gate=:half,labels=true,sites=:cutoff,kind)
    for kind in (:native,:compiler)])
@testset "Follow-up all-mode query parity" begin
    for mode in (:mean_intersection,:max_intersection,:diff_intersection,:min_union,:diff_union,:reachable_union),
            exclude in (false,true), tile in (3,16,64)
        expected = query10(base;radius=5,mode,exclude,tile)
        for args in candidates
            actual = query10(args;radius=5,mode,exclude,tile)
            @test actual.h3 == expected.h3
            @test iszero.(actual.value) == iszero.(expected.value)
            @test all(isapprox.(actual.value,expected.value;rtol=1e-12,atol=1e-6))
        end
    end
    for samples in (1,2,4,5), args in candidates
        actual = query10(args;radius=5,samples,exclude=true)
        expected = query10(base;radius=5,samples,exclude=true)
        @test actual.h3 == expected.h3
        @test iszero.(actual.value) == iszero.(expected.value)
        @test all(isapprox.(actual.value,expected.value;rtol=1e-12,atol=1e-6))
    end
end
