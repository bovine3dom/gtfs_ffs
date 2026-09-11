# Run in the same resident process after the isolated candidate trials.
module ApprovedProduction
    include(joinpath(Main.ROOT,"router/src/Reachability.jl"))
end
const RETAINED = let M = ApprovedProduction.Reachability
    g,w,pp = borrow(M,graph),borrow(M,walking),borrow(M,prepared)
    p = M.Population(population.h3,population.weights,population.rollups,
        IdDict{M.WalkingIndex,M.PreparedPopulation}(w=>pp),IdDict{M.Graph,Matrix{Int32}}(),ReentrantLock())
    @assert g.departure === graph.departure && g.arrival === graph.arrival
    @assert w.prepared.graph.targets === walking.prepared.graph.targets
    @assert M._prepare_population(p,w).weights === prepared.weights
    p.schedule_hints[g] = HINT8[1].POPULATION_HINTS[]
    (M,g,p,w)
end
for file in ("Reachability.jl","population.jl","population_packed.jl","population_range.jl")
    logline("RETAINED_SOURCE file=$file sha256=$(bytes2hex(open(sha256,joinpath(ROOT,"router/src",file))))")
end
let rng = MersenneTwister(314), (M,g,p,_) = RETAINED
    hints = M._population_schedule_hints(p,g)
    for _ in 1:100_000
        edge = rand(rng,eachindex(g.edge_to))
        ready = rand(rng,UInt32(0):B.INF-UInt32(1))
        cutoff = ready + rand(rng,UInt32(0):B.INF-ready-UInt32(1))
        @assert M._population_next_arrival(hints,g,edge,ready,cutoff) ==
            B.next_arrival(g.schedule_ptr,g.departure,g.arrival,edge,ready,cutoff)
    end
    logline("RETAINED_REAL_HINT_PARITY n=100000 PASS")
end
for (label,origin,radius,pairs) in (("final-Paris-1027",PARIS,18,5),("final-Paris-9919",PARIS,57,3),
        ("final-rural-1027",RURAL,18,3),("final-rural-9919",RURAL,57,3))
    approved_pairs(label,((:baseline,BASE),(:retained,RETAINED));pairs,origin,radius)
end
for samples in (1,2,4)
    approved_pairs("final-Paris-1027-S$samples",((:baseline,BASE),(:retained,RETAINED));pairs=3,samples)
end
for mode in (:mean_intersection,:max_intersection,:diff_intersection,:min_union,:diff_union,:reachable_union), exclude in (false,true)
    expected = measured(() -> query10(BASE;mode,exclude),"full-parity baseline $mode exclude=$exclude").value
    actual = measured(() -> query10(RETAINED;mode,exclude),"full-parity retained $mode exclude=$exclude").value
    parity(actual,expected)
    logline("FULL_PARITY mode=$mode exclude=$exclude origins=1027 bitwise=$(actual.value == expected.value) PASS")
end
profile_case(() -> query10(BASE),"approved-adaptive-baseline";source_file="population_range.jl")
profile_case(() -> query10(RETAINED),"approved-adaptive-retained";source_file="population_range.jl")
logline("APPROVED_RETAINED_COMPLETE")
