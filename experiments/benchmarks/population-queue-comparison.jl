# Include this file in the resident benchmark process.
module Production
include(joinpath(Main.ROOT, "router/src/Reachability.jl"))
end

function shared_inputs(M)
    g, w, pp = borrow(M, graph), borrow(M, walking), borrow(M, prepared)
    p = M.Population(population.h3, population.weights, population.rollups,
        IdDict{M.WalkingIndex,M.PreparedPopulation}(w => pp), ReentrantLock())
    @assert g.departure === graph.departure
    @assert w.prepared.graph.targets === walking.prepared.graph.targets
    @assert M._prepare_population(p, w).weights === prepared.weights
    return (M, g, p, w)
end

function clean_trial(f, label)
    for attempt in 1:10
        wait_for_idle()
        before = process_stats(SERVER_PID).cpu
        trial = measured(f, "$label attempt=$attempt"; limit=300, idle=false)
        cpu = process_stats(SERVER_PID).cpu - before
        cpu <= max(0.05, trial.time * 0.05) && return trial
        logline("DISCARD $label server_cpu_s=$cpu")
    end
    error("No uncontaminated trial for $label")
end

function parity(a, b)
    @assert a.h3 == b.h3
    @assert iszero.(a.value) == iszero.(b.value)
    @assert all(isapprox.(a.value, b.value; rtol=1e-12, atol=1e-6))
end

const FINAL_INPUTS = shared_inputs(Production.Reachability)
const FINAL_TRIALS = Dict{Tuple,Any}()
for (radius, budget) in ((6,3), (10,3), (18,3), (6,168))
    baseline_warm = clean_trial(() -> query7(B, graph, population, walking, radius, budget), "final baseline k$radius b$budget warm")
    expected = baseline_warm.value
    warm = clean_trial(() -> query7(FINAL_INPUTS..., radius, budget), "final candidate k$radius b$budget warm")
    parity(warm.value, expected)
    repeats = baseline_warm.time > 30 ? 1 : 3
    for rep in 1:repeats, candidate in (isodd(rep) ? (false, true) : (true, false))
        args = candidate ? FINAL_INPUTS : (B, graph, population, walking)
        name = candidate ? "candidate" : "baseline"
        trial = clean_trial(() -> query7(args..., radius, budget), "final $name k$radius b$budget rep$rep")
        parity(trial.value, expected)
        push!(get!(FINAL_TRIALS, (name,radius,budget), []), trial)
    end
    for name in ("baseline", "candidate")
        ts = FINAL_TRIALS[(name,radius,budget)]
        result = last(ts).value
        logline("FINAL $name k$radius b$budget n=$(length(ts)) seconds=$(median(t.time for t in ts)) bytes=$(median(t.bytes for t in ts)) allocations=$(median(Base.gc_alloc_count(t.gcstats) for t in ts)) shared=$(result.shared_expansions) queries=$(result.query_expansions) workers=$(result.workers)")
    end
end
for mode in (:mean_intersection, :max_intersection, :diff_intersection, :min_union, :diff_union, :reachable_union), exclude in (false, true)
    expected = query7(B, graph, population, walking; mode, exclude)
    actual = query7(FINAL_INPUTS...; mode, exclude)
    parity(actual, expected)
    logline("FULL_PARITY mode=$mode exclude=$exclude origins=$(length(actual.h3)) PASS")
end
profile_case(() -> query7(FINAL_INPUTS...), "range-res7"; source_file="population_range.jl")
function short_query(args, radius, samples)
    M, g, p, w = args
    M.route_population(g, p, ORIGIN7, 28_800_000, 10_800_000; walking_index=w,
        origin_radius=radius, window_ms=samples == 1 ? 0 : samples * 900_000, step_ms=900_000)
end
for samples in (1,4,5,16), radius in (6,18)
    expected = short_query((B, graph, population, walking), radius, samples)
    parity(short_query(FINAL_INPUTS, radius, samples), expected)
    times = [Float64[], Float64[]]
    for rep in 1:3, v in (isodd(rep) ? (1,2) : (2,1))
        args = v == 1 ? (B, graph, population, walking) : FINAL_INPUTS
        t = clean_trial(() -> short_query(args, radius, samples), "short variant=$v k$radius samples=$samples rep=$rep")
        parity(t.value, expected)
        push!(times[v], t.time)
    end
    logline("SHORT k$radius samples=$samples baseline_s=$(median(times[1])) candidate_s=$(median(times[2]))")
end
logline("FINAL_COMPLETE memory=$(memory_stats())")
for file in ("Reachability.jl", "population_packed.jl", "population_range.jl")
    path = joinpath(ROOT, "router/src", file)
    logline("FINAL_SOURCE file=$file bytes=$(filesize(path)) sha256=$(bytes2hex(open(sha256, path)))")
end
