include("population-approved3-candidates.jl")
using Random
const SIMD = approved_module(:ApprovedSIMD;labels=true)
const VECTOR = approved_module(:ApprovedVector;labels=true,vector=true)
const RADIX = approved_module(:ApprovedRadix;radix=true)
const HINT8 = candidate_module(:ApprovedHint8;bins=8,source_root=SNAPSHOT)
const APPROVED_ROWS = []
for file in ("population-approved3-candidates.jl","population-approved3-kernels.jl","population-approved3-trials.jl")
    logline("SOURCE file=$file sha256=$(bytes2hex(open(sha256,joinpath(@__DIR__,file))))")
end
function approved_pairs(label, variants; pairs, kwargs...)
    expected = nothing
    for (name,args) in variants
        trial = measured(() -> query10(args;kwargs...), "$label $name warm")
        isnothing(expected) ? (expected = trial.value) : parity(trial.value,expected)
    end
    for pair in 1:pairs
        for (name,args) in (isodd(pair) ? variants : reverse(variants))
            trial = measured(() -> query10(args;kwargs...), "$label $name pair=$pair")
            parity(trial.value,expected)
            serialize(joinpath(ARTIFACTS,"approved-$label-$name-$pair.jls"),trial.value)
            push!(APPROVED_ROWS,(;label,name,pair,wall=trial.wall,cpu=trial.own_cpu,
                external=trial.external_cores,bytes=trial.bytes,rss=trial.rss))
            serialize(joinpath(ARTIFACTS,"approved-rows.jls"),APPROVED_ROWS)
        end
    end
    rows = filter(r -> r.label == label,APPROVED_ROWS)
    for (name,_) in variants
        selected = filter(r -> r.name == name,rows)
        ratios = [only(r.wall for r in rows if r.name == first(variants)[1] && r.pair == s.pair)/s.wall for s in selected]
        cpuratios = [only(r.cpu for r in rows if r.name == first(variants)[1] && r.pair == s.pair)/s.cpu for s in selected]
        logline("PAIRED label=$label name=$name n=$pairs wall=$(median(s.wall for s in selected)) cpu=$(median(s.cpu for s in selected)) wall_ratio=$(median(ratios)) ratio_range=$(extrema(ratios)) cpu_ratio=$(median(cpuratios)) external=$(extrema(s.external for s in selected)) bytes=$(median(s.bytes for s in selected)) rss=$(maximum(s.rss for s in selected))")
    end
end

let rng = MersenneTwister(312), M = HINT8[1]
    for i in 1:100_000
        edge = rand(rng,eachindex(graph.edge_to))
        ready = rand(rng,UInt32(0):B.INF-UInt32(1))
        cutoff = ready + rand(rng,UInt32(0):B.INF-ready-UInt32(1))
        @assert M._population_next_arrival(graph.schedule_ptr,graph.departure,graph.arrival,edge,ready,cutoff) ==
            B.next_arrival(graph.schedule_ptr,graph.departure,graph.arrival,edge,ready,cutoff)
    end
    logline("REAL_HINT_PARITY n=100000 PASS")
end
approved_pairs("Paris-1027",((:baseline,BASE),(:simd,SIMD),(:vector,VECTOR),(:radix,RADIX),(:hint8,HINT8));pairs=5)
logline("APPROVED_FIRST_PHASE_COMPLETE")
for (label,origin,radius) in (("Paris-9919",PARIS,57),("rural-1027",RURAL,18),("rural-9919",RURAL,57))
    approved_pairs(label,((:baseline,BASE),(:hint8,HINT8));pairs=3,origin,radius)
end
approved_pairs("Paris-127-B168",((:baseline,BASE),(:simd,SIMD),(:vector,VECTOR),(:radix,RADIX),(:hint8,HINT8));pairs=3,radius=6,budget=168)
logline("APPROVED_REGRESSION_PHASE_COMPLETE")
