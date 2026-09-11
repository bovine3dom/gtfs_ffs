# Verify the production producer without another graph load.
let (M,g,p,_) = RETAINED
    original = pop!(p.schedule_hints,g)
    trial = measured(() -> M._population_schedule_hints(p,g),"retained-hint-build")
    @assert trial.value == original
    @assert M._population_schedule_hints(p,g) === trial.value
    HINT8[1].POPULATION_HINTS[] = trial.value
    logline("RETAINED_HINT_BUILD bytes=$(sizeof(trial.value)) prototype_equal=true")
end
approved_pairs("final-Paris-127-B168",((:baseline,BASE),(:retained,RETAINED));pairs=3,radius=6,budget=168)
let mode=:reachable_union, exclude=true
    a = measured(() -> query10(BASE;mode,exclude),"roundoff baseline-a").value
    b = measured(() -> query10(BASE;mode,exclude),"roundoff baseline-b").value
    c = measured(() -> query10(RETAINED;mode,exclude),"roundoff retained").value
    parity(a,b)
    parity(a,c)
    for (name,result) in ((:baseline_b,b),(:retained,c))
        logline("ROUNDOFF name=$name bitwise=$(result.value==a.value) max_abs=$(maximum(abs.(result.value.-a.value)))")
        serialize(joinpath(ARTIFACTS,"roundoff-$name.jls"),result)
    end
    serialize(joinpath(ARTIFACTS,"roundoff-baseline-a.jls"),a)
end
approved_pairs("final-Paris-9919-S1",((:baseline,BASE),(:retained,RETAINED));pairs=3,radius=57,samples=1)
open(joinpath(@__DIR__,"population-approved3-trials.csv"),"w") do io
    println(io,join(propertynames(first(APPROVED_ROWS)),','))
    for row in APPROVED_ROWS
        println(io,join(values(row),','))
    end
end
logline("APPROVED_VERIFICATION_COMPLETE")
