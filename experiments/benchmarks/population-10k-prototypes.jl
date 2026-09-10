include("population-10k-candidates.jl")
using Random
logline("POP_LABELS destinations=$(length(prepared.weights)) tile16_workers8_bytes=$(16*8*4*length(prepared.weights))")
const INCREMENTAL = candidate_module(:Incremental; incremental=true)
const HINT8 = candidate_module(:Hint8; bins=8)
const HINT24 = candidate_module(:Hint24; bins=24)
for args in (INCREMENTAL, HINT8, HINT24)
    warm = clean_trial(() -> query10(args; radius=6), "prototype compile $(args[1])")
    parity(warm.value, query10(FAST; radius=6))
end
# Test real profile bounds, day wrap, cutoff rejection, and near-INF addition.
rng = MersenneTwister(20260910)
function search_probes(departure, lo, hi, time)
    probes = 0
    while lo < hi
        probes += 1
        mid = lo + ((hi-lo) >> 1)
        if departure[mid] < time
            lo = mid + 1
        else
            hi = mid
        end
    end
    return probes
end
for args in (HINT8, HINT24)
    M = args[1]
    baseline_probes = hinted_probes = 0
    for _ in 1:100_000
        edge = rand(rng, eachindex(graph.edge_to))
        ready = rand(rng, UInt32(0):B.INF-UInt32(1))
        cutoff = rand(rng, ready:B.INF-UInt32(1))
        actual = M._population_next_arrival(graph.schedule_ptr, graph.departure, graph.arrival, edge, ready, cutoff)
        expected = B.next_arrival(graph.schedule_ptr, graph.departure, graph.arrival, edge, ready, cutoff)
        @assert actual == expected
        clock = ready % B.PERIOD
        bins = M.POPULATION_BINS
        bin = Int(clock ÷ (B.PERIOD ÷ UInt32(bins))) + 1
        hints = M.POPULATION_HINTS[]
        stop = graph.schedule_ptr[edge+1]
        hi = bin == bins ? stop : min(stop, hints[bin+1,edge]+1)
        baseline_probes += search_probes(graph.departure, graph.schedule_ptr[edge], stop, clock)
        hinted_probes += search_probes(graph.departure, hints[bin,edge], hi, clock)
    end
    logline("HINT_PARITY module=$M random_profiles=100000 baseline_probes=$baseline_probes hinted_probes=$hinted_probes PASS")
    original = M.POPULATION_HINTS[]
    checks = 0
    try
        for _ in 1:40
            departures = UInt32[0, 10, 10, B.PERIOD-1, rand(rng, 0:Int(B.PERIOD)-1, 40)...]
            durations = Int64[12Int(B.PERIOD), 10, 0, 1, rand(rng, 0:12Int(B.PERIOD), 40)...]
            g = B.pack_graph((from_h3=fill(PARIS,length(departures)), to_h3=fill(PARIS,length(departures)),
                departure_ms=departures, duration_ms=durations))
            bins = M.POPULATION_BINS
            hints = Matrix{Int32}(undef,bins,1)
            for bin in 1:bins
                hints[bin,1] = searchsortedfirst(g.departure, (bin-1)*(Int(B.PERIOD)÷bins))
            end
            M.POPULATION_HINTS[] = hints
            for ready in UInt32[0,9,10,11,B.PERIOD-1,B.PERIOD,B.INF-1000], delta in UInt32[0,1,100,999]
                cutoff = ready + delta
                @assert M._population_next_arrival(g.schedule_ptr,g.departure,g.arrival,1,ready,cutoff) ==
                    B.next_arrival(g.schedule_ptr,g.departure,g.arrival,1,ready,cutoff)
                checks += 1
            end
        end
    finally
        M.POPULATION_HINTS[] = original
    end
    logline("HINT_SYNTHETIC module=$M checks=$checks PASS")
end
const PROTOTYPES = Dict{Tuple,Any}()
for (radius, budget) in ((18,3), (6,168))
    expected = CASES[(:dense,radius,96,budget)][1].value
    repeats = CASES[(:dense,radius,96,budget)][1].time > 30 ||
        (isdefined(Main, :VALIDATION_ONLY) && VALIDATION_ONLY) ? 1 : 3
    for rep in 1:repeats
        variants = ((:fast,FAST), (:incremental,INCREMENTAL), (:hint8,HINT8), (:hint24,HINT24))
        for (name, args) in (isodd(rep) ? variants : reverse(variants))
            trial = clean_trial(() -> query10(args; radius, budget), "prototype $name k$radius b$budget rep$rep")
            parity(trial.value, expected)
            push!(get!(PROTOTYPES, (name,radius,budget), []), trial)
        end
    end
    for name in (:fast,:incremental,:hint8,:hint24)
        ts = PROTOTYPES[(name,radius,budget)]
        clean = all(t.clean for t in ts)
        logline("PROTOTYPE name=$name k=$radius budget=$budget clean=$clean seconds=$(clean ? median(t.time for t in ts) : NaN) bytes=$(median(t.bytes for t in ts)) rss=$(maximum(t.rss for t in ts))")
    end
end
for mode in (:mean_intersection,:max_intersection,:diff_intersection,:min_union,:diff_union,:reachable_union), exclude in (false,true)
    expected = query10(BASE; mode, exclude)
    parity(query10(FAST; mode, exclude), expected)
    logline("PARITY name=fast mode=$mode exclude=$exclude origins=1027 PASS")
end
logline("PROTOTYPES_COMPLETE")
