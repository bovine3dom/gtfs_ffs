module BoardingBenchmark
include("benchmark.jl")
const B, R, HR = HierarchyBenchmark, HierarchyBenchmark.R, HierarchyBenchmark.HierarchyResearch

function query(data, h, g, origins, departure, budget, samples, mode, exclude, variant; batch=64)
    options = (; window_ms=samples == 1 ? 0 : samples * 900_000, step_ms=900_000,
        window_mode=mode, exclude_origin_population=exclude, origin_batch_size=batch)
    variant == :fine && return R._route_population_origins(data.graph, data.walking, data.population,
        R._prepare_population(data.population, data.walking), R._population_rollup(data.population, 8),
        origins, UInt32(departure), budget, 900_000, samples, UInt32(min(B.W, budget)), mode, batch, exclude)
    variant == :free && return HR.route_hierarchy(h, origins, departure, budget; options...)
    return HR.route_boarding(h, g, origins, departure, budget; options...,
        correction=variant != :zero, history=variant == :independent ? :independent : :descending)
end

function measure(data, h, g, origins, case, name, trials, errors, outliers, metadata; rounds=3,
        variants=[:fine, :free, :walking])
    results = Dict{Symbol,Any}()
    times = Dict(v => Float64[] for v in variants)
    for pair in 0:rounds, variant in circshift(variants, pair)
        GC.gc()
        c = B.cpu()
        t = @timed query(data, h, g, origins, case..., variant)
        cpu = B.cpu() - c
        pair == 0 ? (results[variant] = t.value) :
            (@assert t.value.value == results[variant].value; push!(times[variant], t.time))
        B.row(trials, (name, h.core_resolution, length(origins), case..., variant, pair,
            t.time, cpu, t.bytes, t.gctime, t.compile_time, B.rss(), t.value.query_expansions))
        B.log(metadata, "TRIAL $name res=$(h.core_resolution) n=$(length(origins)) case=$case variant=$variant pair=$pair wall=$(t.time) compile=$(t.compile_time)")
    end
    fine = results[:fine].value
    for variant in variants[2:end]
        actual = results[variant].value
        delta = actual .- fine
        positive = findall(>(0), fine)
        relative = abs.(delta[positive]) ./ fine[positive]
        quantiles = isempty(relative) ? [NaN, NaN] : B.quantile(relative, [0.5, 0.95])
        B.row(errors, (name, h.core_resolution, length(origins), case..., variant,
            B.mean(fine), B.mean(actual), B.mean(delta), sum(abs, delta) / sum(fine),
            quantiles..., minimum(delta), maximum(delta), count(iszero, fine),
            maximum(abs.(delta[findall(iszero, fine)]); init=0.0)))
        for i in sortperm(abs.(delta); rev=true)[1:min(10, length(delta))]
            B.row(outliers, (name, h.core_resolution, length(origins), case..., variant,
                string(origins[i]; base=16), fine[i], actual[i], delta[i]))
        end
    end
    if haskey(results, :zero)
        @assert all(isapprox.(results[:zero].value, results[:free].value; atol=1e-6, rtol=1e-12))
    end
    for batch in (1, 16)
        other = query(data, h, g, origins, case..., :walking; batch)
        @assert all(isapprox.(other.value, results[:walking].value; atol=1e-6, rtol=1e-12))
    end
    medians = Dict(v => B.median(times[v]) for v in variants)
    B.log(metadata, "MEDIANS $name res=$(h.core_resolution) n=$(length(origins)) case=$case $medians")
    return medians, results
end

function coverage(data, h, g, origins, io)
    weights = R._population_rollup(data.population, 8)
    for departure in (28_800_000, 84_600_000), budget in (3B.W, 6B.W), origin in origins
        fine = R._walking_route_at(data.graph, R.WalkingTopology(data.walking, B.W), origin,
            UInt32(departure), UInt32(departure + budget), false)
        expected = Set(c for c in fine.h3 if get(weights, c, 0.0) > 0)
        credit = R.PopulationWorkspace(length(g.h3), length(h.destinations.weights))
        w = HR.BoardingWorkspace(credit, 1)
        i = h.origins[origin]
        HR.boarding_sample!(w, g, h, h.core_nodes + i, UInt32(departure),
            UInt32(departure + budget), UInt32(B.W))
        actual = Set(h.destinations.cells[id] for id in credit.reached_ids)
        union!(actual, (h.destinations.cells[h.direct.targets[j]] for j in h.direct.offsets[i]:(h.direct.offsets[i + 1] - 1)))
        fp, fn = setdiff(actual, expected), setdiff(expected, actual)
        B.row(io, (h.core_resolution, string(origin; base=16), departure, budget,
            length(fp), length(fn), sum(c -> weights[c], fp; init=0.0), sum(c -> weights[c], fn; init=0.0)))
    end
end

function main(args)
    length(args) in (1, 2) || error("Use: benchmark-boarding.jl NEW_OUTPUT_DIRECTORY [full|large]")
    selection = length(args) == 2 ? args[2] : "screen"
    selection in ("screen", "full", "large") || error("unknown benchmark selection")
    output, full, large = first(args), selection == "full", selection == "large"
    ispath(output) && error("output exists")
    isdir(dirname(abspath(output))) || error("output parent must exist")
    mkdir(output)
    open(joinpath(output, "metadata.txt"), "w") do metadata
        B.log(metadata, "Julia=$VERSION threads=$(Threads.nthreads()) CPU=$(Sys.cpu_info()[1].model) selection=$selection history=descending batch=64")
        for file in [readdir(joinpath(B.ROOT, "router/src"); join=true); joinpath.(@__DIR__, ["Hierarchy.jl", "Boarding.jl", "benchmark-boarding.jl"])]
            B.log(metadata, "SOURCE $(basename(file)) $(bytes2hex(open(B.sha256, file)))")
        end
        input = joinpath(B.ROOT, "data/austria_adjacent_res8.arrow")
        B.log(metadata, "INPUT sha256=$(bytes2hex(open(B.sha256, input)))")
        graph = R.pack_graph(input)
        walking = R.prepare_walking(R.WalkingIndex(graph); max_walk_ms=B.W)
        population = R.load_population(joinpath(B.ROOT, "data/kontur_h3.arrow"))
        R._prepare_population(population, walking)
        data = (; graph, walking, population)
        open(joinpath(output, "trials.csv"), "w") do trials
            open(joinpath(output, "quality.csv"), "w") do errors
                open(joinpath(output, "outliers.csv"), "w") do outliers
                    open(joinpath(output, "coverage.csv"), "w") do cov
                        println(trials, "city,res,origins,departure,budget,samples,mode,exclude,variant,pair,wall_s,cpu_s,bytes,gc_s,compile_s,rss,expansions")
                        println(errors, "city,res,origins,departure,budget,samples,mode,exclude,variant,fine_mean,actual_mean,bias,wmae,abs_relative_p50,abs_relative_p95,min_signed,max_signed,zero_fine,zero_max_abs")
                        println(outliers, "city,res,origins,departure,budget,samples,mode,exclude,variant,origin,fine_population,actual_population,signed_error")
                        println(cov, "res,origin,departure,budget,fp_cells,fn_cells,fp_population,fn_population")
                        for (city, centre) in (large ? B.CENTRES[1:1] : B.CENTRES), res in (large ? (6,) : (7, 6))
                            origins = B.disk(centre, large ? 57 : full ? 19 : 6)
                            h = B.prepare(data, origins, res, city, metadata)
                            prep = @timed HR.prepare_boarding(h)
                            g = prep.value
                            finite = UInt32[d for m in g.gaps for d in m if 0 < d < R.INF]
                            B.log(metadata, "BOARDING res=$res wall=$(prep.time) bytes=$(Base.summarysize(g)) profiles=$(length(g.arrival)) edges=$(length(g.edge_to)) gaps_bytes=$(sum(sizeof, g.gaps)) gap_p50_p95=$(B.quantile(finite, [0.5,0.95])) workspace_tag_bytes=$(2length(g.h3)) workspace_used_bytes=$(8length(g.h3))")
                            B.log(metadata, "STORAGE graph_payload=$(sum(sizeof, (g.out_ptr,g.edge_from,g.edge_to,g.schedule_ptr,g.departure,g.arrival))) profile_tags=$(sizeof(g.arrival_child)) edge_tags=$(sizeof(g.boarding)) walk_tags=$(sizeof(g.walk_child)) combined_index=$(Base.summarysize((h.graph,h.prepared,h.destinations,h.direct,h.origins,g))) allocated_prep=$(prep.bytes) rss=$(B.rss())")
                            if large
                                measure(data, h, g, origins, (28_800_000, 3B.W, 96, :mean_intersection, false), city,
                                    trials, errors, outliers, metadata)
                                continue
                            end
                            probe = origins[round.(Int, range(1, length(origins); length=16))]
                            coverage(data, h, g, probe, cov)
                            small = B.disk(centre, 6)
                            measure(data, h, g, small, (28_800_000, 3B.W, 4, :mean_intersection, false), city,
                                trials, errors, outliers, metadata; variants=[:fine, :free, :zero, :walking, :independent])
                            measure(data, h, g, small, (28_800_000, 3B.W, 96, :mean_intersection, false), city,
                                trials, errors, outliers, metadata)
                            if full
                                measure(data, h, g, B.disk(centre, 18), (28_800_000, 3B.W, 96, :mean_intersection, false), city,
                                    trials, errors, outliers, metadata)
                                for case in ((28_800_000, 3B.W, 1, :mean_intersection, false),
                                        (84_600_000, 6B.W, 96, :mean_intersection, true),
                                        (72_000_000, 3B.W, 96, :reachable_union, false))
                                    measure(data, h, g, small, case, city, trials, errors, outliers, metadata)
                                end
                                moved = first(filter(!=(centre), B.disk(centre, 1)))
                                measure(data, h, g, B.disk(moved, 6), (84_600_000, 3B.W, 4, :reachable_union, false), city * "-moved",
                                    trials, errors, outliers, metadata)
                            end
                        end
                    end
                end
            end
        end
        B.log(metadata, "FINISHED maxrss=$(Sys.maxrss())")
    end
end
end
abspath(PROGRAM_FILE) == (@__FILE__) && BoardingBenchmark.main(ARGS)
