module HierarchyBenchmark
using H3, SHA, Statistics
include("../../router/src/Reachability.jl")
include("Hierarchy.jl")
using .HierarchyResearch
const R = Reachability
const ROOT = normpath(joinpath(@__DIR__, "../.."))
const CENTRES = ("Vienna" => UInt64(0x881e15b467fffff), "Salzburg" => UInt64(0x881f89af4dfffff))
const W = 3_600_000
cpu() = ccall(:clock, Clong, ()) / 1e6
hostcpu() = sum(parse.(Int, split(first(eachline("/proc/stat")))[2:end])[[1, 2, 3, 6, 7, 8]]) / 100
rss() = 1024parse(Int, match(r"VmRSS:\s+(\d+)", read("/proc/self/status", String))[1])
available() = 1024parse(Int, match(r"MemAvailable:\s+(\d+)", read("/proc/meminfo", String))[1])
disk(centre, radius) = sort!(filter!(!iszero, H3.API.gridDisk(centre, radius)))
row(io, fields) = (println(io, join(fields, ',')); flush(io))
log(io, text) = (println(io, text); flush(io); println(text); flush(stdout))

function query(data, hierarchy, centre, radius, departure, hours, samples, mode, exclude)
    options = (; window_ms=samples == 1 ? 0 : samples * 900_000, step_ms=900_000,
        window_mode=mode, max_walk_ms=W, exclude_origin_population=exclude)
    return isnothing(hierarchy) ? R.route_population(data.graph, data.population, centre, departure, hours * W;
        options..., walking_index=data.walking, origin_radius=radius) :
        route_hierarchy(hierarchy, disk(centre, radius), departure, hours * W; options...)
end

function quality(a, b, resolution)
    @assert a.h3 == b.h3
    delta = b.value .- a.value
    @assert all(delta .>= -1e-6)
    positive = findall(>(0), a.value)
    relative = delta[positive] ./ a.value[positive]
    percentiles = isempty(relative) ? fill(NaN, 3) : quantile(relative, [0.5, 0.95, 1.0])
    groups = Dict{UInt64,Vector{Int}}()
    for (i, h) in enumerate(a.h3)
        push!(get!(Vector{Int}, groups, H3.API.cellToParent(h, resolution)), i)
    end
    x, y = Float64[], Float64[]
    varying_fine = varying_core = 0
    for ids in values(groups)
        length(ids) > 1 || continue
        append!(x, a.value[ids] .- mean(a.value[ids]))
        append!(y, b.value[ids] .- mean(b.value[ids]))
        varying_fine += maximum(a.value[ids]) - minimum(a.value[ids]) > 1e-6
        varying_core += maximum(b.value[ids]) - minimum(b.value[ids]) > 1e-6
    end
    return (mean(a.value), mean(b.value), mean(delta), mean(abs.(delta)),
        sum(a.value) > 0 ? sum(abs.(delta)) / sum(a.value) : NaN, percentiles...,
        count(iszero, a.value), maximum(abs.(delta[findall(iszero, a.value)]); init=0.0),
        minimum(delta), varying_fine, varying_core, length(x) > 1 ? cor(x, y) : NaN,
        length(delta) > 1 ? cor(R.normalized_ranks(a.value), R.normalized_ranks(b.value)) : NaN)
end

function measure(data, candidates, city, centre, case, trials, errors, metadata; rounds=3)
    radius, departure, hours, samples, mode, exclude = case
    expected = Dict{Int,Any}()
    times = Dict(res => Float64[] for res in (8, 7, 6))
    for pair in 0:rounds, res in circshift([8, 7, 6], pair)
        GC.gc()
        c, host = cpu(), hostcpu()
        t = @timed query(data, res == 8 ? nothing : candidates[res], centre, case...)
        elapsed, external = cpu() - c, hostcpu() - host
        if pair == 0
            expected[res] = t.value
        else
            @assert t.value.h3 == expected[res].h3
            @assert all(isapprox.(t.value.value, expected[res].value; atol=1e-6, rtol=1e-12))
            @assert t.compile_time == 0
            push!(times[res], t.time)
        end
        row(trials, (city, radius, length(t.value.h3), departure, hours, samples, mode, exclude, res, pair,
            t.time, elapsed, t.bytes, t.gctime, t.compile_time, rss(),
            max(0.0, external - elapsed) / t.time, t.value.shared_expansions, t.value.query_expansions))
        log(metadata, "TRIAL city=$city case=$case core=$res pair=$pair wall_s=$(t.time)")
    end
    for res in (7, 6)
        row(errors, (city, radius, length(expected[8].h3), departure, hours, samples, mode, exclude, res,
            quality(expected[8], expected[res], res)...))
    end
    medians = Dict(res => median(times[res]) for res in (8, 7, 6))
    log(metadata, "MEDIANS city=$city case=$case seconds=$medians")
    return medians
end

function coverage_check(data, candidates, origins, city, io)
    weights = R._population_rollup(data.population, 8)
    for departure in (28_800_000, 84_600_000), hours in (3, 6), res in sort!(collect(keys(candidates)))
        h = candidates[res]
        sources = R._population_sources(h, h.destinations, weights, origins, UInt32(W))
        w = R.PopulationWorkspace(length(h.graph.h3), length(sources.weights))
        false_positive = false_negative = 0
        fp_weight = fn_weight = 0.0
        for (i, o) in enumerate(origins)
            reference = R._walking_route_at(data.graph, R.WalkingTopology(data.walking, W), o,
                UInt32(departure), UInt32(departure + hours * W), false)
            fine = Set(cell for cell in reference.h3 if get(weights, cell, 0.0) > 0)
            R._population_sample_packed!(w, h.graph, h.prepared.graph, h.destinations,
                sources, i:i, UInt32[departure], UInt32[departure + hours * W], UInt32(W))
            actual = Set(h.destinations.cells[id] for id in w.reached_ids if w.reached[id] != 0)
            fp, fn = setdiff(actual, fine), setdiff(fine, actual)
            @assert isempty(fn)
            res == 8 && @assert isempty(fp)
            false_positive += length(fp); false_negative += length(fn)
            fp_weight += sum(cell -> weights[cell], fp; init=0.0)
            fn_weight += sum(cell -> weights[cell], fn; init=0.0)
        end
        row(io, (city, res, departure, hours, length(origins), false_positive, false_negative, fp_weight, fn_weight))
    end
end

function prepare(data, origins, res, city, io)
    GC.gc()
    t = @timed prepare_hierarchy(data.graph, data.population, origins; core_resolution=res, walking_index=data.walking)
    h = t.value
    hints = R._population_schedule_hints(h.population, h.graph)
    bytes = Base.summarysize((h.graph, h.prepared, h.destinations, h.direct, h.origins, hints))
    log(io, "PREP city=$city core=$res stats=$(h.stats) wall_s=$(t.time) allocated=$(t.bytes) compile_s=$(t.compile_time) index_bytes=$bytes rss=$(rss())")
    return h
end

function main(args)
    (length(args) == 1 || (length(args) == 2 && args[2] == "--check")) ||
        error("Use: benchmark.jl NEW_OUTPUT_DIRECTORY [--check]")
    output, check = first(args), length(args) == 2
    ispath(output) && error("output exists; choose a new directory")
    isdir(dirname(abspath(output))) || error("output parent must exist")
    mkdir(output)
    open(joinpath(output, "metadata.txt"), "w") do metadata
        log(metadata, "Julia=$VERSION threads=$(Threads.nthreads()) CPU=$(Sys.cpu_info()[1].model) check=$check")
        for file in readdir(joinpath(ROOT, "router/src"); join=true)
            log(metadata, "SOURCE $(basename(file)) $(bytes2hex(open(sha256, file)))")
        end
        input = joinpath(ROOT, "data/austria_adjacent_res8.arrow")
        log(metadata, "INPUT $input sha256=$(bytes2hex(open(sha256, input))) shuttle=false")
        packed = @timed R.pack_graph(input)
        graph = packed.value
        @assert (length(graph.h3), length(graph.edge_to), length(graph.departure)) == (20244, 61796, 4791297)
        walking = @timed R.prepare_walking(R.WalkingIndex(graph))
        population = @timed R.load_population(joinpath(ROOT, "data/kontur_h3.arrow"))
        aligned = @timed R._prepare_population(population.value, walking.value)
        R._population_schedule_hints(population.value, graph)
        data = (; graph, walking=walking.value, population=population.value)
        log(metadata, "BASE pack_s=$(packed.time) walking_s=$(walking.time) population_load_s=$(population.time) align_s=$(aligned.time) graph_bytes=$(Base.summarysize(graph)) walking_bytes=$(Base.summarysize(data.walking)) prepared_bytes=$(Base.summarysize(aligned.value)) population_rows=$(length(data.population.h3)) population_sum=$(sum(Float64, data.population.weights)) rss=$(rss())")
        open(joinpath(output, "trials.csv"), "w") do trials
            open(joinpath(output, "quality.csv"), "w") do errors
                open(joinpath(output, "coverage.csv"), "w") do coverage
                    println(trials, "city,radius,origins,departure_ms,budget_h,samples,mode,exclude,core,pair,wall_s,cpu_s,bytes,gc_s,compile_s,rss_bytes,external_cores,shared,independent")
                    println(errors, "city,radius,origins,departure_ms,budget_h,samples,mode,exclude,core,fine_mean,coarse_mean,bias,mae,wmae,relative_p50,relative_p95,relative_max,zero_baselines,zero_max_abs,min_signed_error,varying_fine_parents,varying_coarse_parents,within_parent_correlation,rank_correlation")
                    println(coverage, "city,core,departure_ms,budget_h,origins,false_positive,false_negative,fp_population,fn_population")
                    promising = false
                    for (city, centre) in CENTRES
                        cohort = disk(centre, 19)
                        probe = cohort[round.(Int, range(1, length(cohort); length=16))]
                        h8 = prepare(data, probe, 8, city, metadata)
                        coverage_check(data, Dict(8 => h8), probe, city, coverage)
                        h8 = nothing
                        candidates = Dict(res => prepare(data, cohort, res, city, metadata) for res in (7, 6))
                        coverage_check(data, candidates, probe, city, coverage)
                        for radius in (check ? (18,) : (6, 18))
                            medians = measure(data, candidates, city, centre,
                                (radius, 28_800_000, 3, 96, :mean_intersection, false), trials, errors, metadata)
                            city == "Vienna" && radius == 18 && (promising = medians[8] / min(medians[7], medians[6]) >= 2)
                        end
                        check && continue
                        for (radius, departure, hours, samples, mode, exclude) in
                                ((6, 28_800_000, 3, 1, :mean_intersection, false),
                                 (6, 28_800_000, 3, 4, :mean_intersection, false),
                                 (6, 72_000_000, 3, 96, :reachable_union, false),
                                 (6, 84_600_000, 6, 96, :mean_intersection, true),
                                 (18, 28_800_000, 3, 96, :reachable_union, false))
                            measure(data, candidates, city, centre, (radius, departure, hours, samples, mode, exclude), trials, errors, metadata)
                        end
                        moved = first(filter(!=(centre), disk(centre, 1)))
                        measure(data, candidates, city * "-moved", moved,
                            (18, 84_600_000, 3, 4, :reachable_union, false), trials, errors, metadata)
                    end
                    if !check && promising && available() > 6 * 1024^3
                        city, centre = first(CENTRES)
                        cohort = disk(centre, 57)
                        candidates = Dict(res => prepare(data, cohort, res, city * "-large", metadata) for res in (7, 6))
                        for mode in (:mean_intersection, :reachable_union)
                            measure(data, candidates, city, centre, (57, 28_800_000, 3, 96, mode, false), trials, errors, metadata)
                        end
                    else
                        log(metadata, "LARGE skipped: promising=$promising available_memory=$(available())")
                    end
                    log(metadata, "FINISHED maxrss=$(Sys.maxrss()) rss=$(rss())")
                end
            end
        end
    end
end
end
if abspath(PROGRAM_FILE) == @__FILE__
    HierarchyBenchmark.main(ARGS)
end
