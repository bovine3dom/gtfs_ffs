module Fast
include(joinpath(Main.ROOT, "router/src/Reachability.jl"))
end
const FAST = shared_inputs(Fast.Reachability)
const CASES = Dict{Tuple,Any}()
for args in (BASE, FAST)
    clean_trial(() -> query10(args; radius=6), "compile $(args[1])")
end
function cohort_counts(origins)
    s = B._population_sources(walking, prepared, population.rollups[7], origins, UInt32(3_600_000))
    network = walking.prepared.graph
    boardable(node) = graph.out_ptr[node] < graph.out_ptr[node+1]
    heavy = count(eachindex(origins)) do i
        node = s.sources[i]
        iszero(node) ? any(hop -> boardable(first(hop)), s.access[i]) :
            boardable(node) || any(j -> network.durations[j] <= 3_600_000 && boardable(network.targets[j]),
                network.offsets[node]:network.offsets[node+1]-1)
    end
    return (; heavy, fast=length(origins)-heavy, on_graph=count(!iszero,s.sources))
end
for (region, origin, radius, samples, budget) in (
        (:dense, PARIS, 18, 96, 3), (:dense, PARIS, 57, 96, 3),
        (:rural, RURAL, 18, 96, 3), (:rural, RURAL, 57, 96, 3),
        (:rural, RURAL, 57, 1, 3), (:dense, PARIS, 6, 96, 168))
    key = (region, radius, samples, budget)
    options = (; origin, radius, samples, budget)
    bt = clean_trial(() -> query10(BASE; options...), "baseline $key")
    ct = clean_trial(() -> query10(FAST; options...), "fast $key")
    parity(bt.value, ct.value)
    CASES[key] = (bt, ct)
    clean = bt.clean && ct.clean
    logline("RESULT key=$key clean=$clean baseline=$(clean ? bt.time : NaN) fast=$(clean ? ct.time : NaN) speedup=$(clean ? bt.time/ct.time : NaN) origins=$(length(ct.value.h3)) baseline_rss=$(bt.rss) fast_rss=$(ct.rss) workers=$(ct.value.workers)")
    logline("COHORT key=$key counts=$(cohort_counts(ct.value.h3))")
end
logline("STEP1_COMPLETE")
