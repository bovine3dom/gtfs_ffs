module AccessBenchmark
include("benchmark.jl")
const B, R = HierarchyBenchmark, HierarchyBenchmark.R
function measure(data, index, city, centre, radius, samples, mode, trials, metadata; rounds=3)
    reference, times = nothing, (Float64[], Float64[])
    for pair in 0:rounds, access in (iseven(pair) ? (false, true) : (true, false))
        GC.gc()
        c, host = B.cpu(), B.hostcpu()
        t = @timed B.query(data, access ? index : nothing, centre, radius, 28_800_000, 3, samples, mode, false)
        elapsed, external = B.cpu() - c, B.hostcpu() - host
        isnothing(reference) && (reference = t.value)
        @assert t.value.h3 == reference.h3
        zero_mismatches = count(iszero.(t.value.value) .!= iszero.(reference.value))
        error = abs.(t.value.value .- reference.value)
        positive = findall(>(0), reference.value)
        relative = error[positive] ./ reference.value[positive]
        variant = access ? "fine_access" : "production"
        B.row(trials, (city, radius, length(t.value.h3), samples, mode, variant, pair, t.time, elapsed,
            t.bytes, t.gctime, t.compile_time, B.rss(), max(0.0, external - elapsed) / t.time,
            t.value.shared_expansions, t.value.query_expansions, zero_mismatches,
            maximum(error), isempty(relative) ? NaN : B.quantile(relative, 0.95),
            isempty(relative) ? NaN : maximum(relative), B.mean(t.value.value)))
        B.log(metadata, "TRIAL city=$city radius=$radius samples=$samples mode=$mode variant=$variant pair=$pair wall_s=$(t.time) max_error=$(maximum(error)) zero_mismatches=$zero_mismatches")
        @assert zero_mismatches == 0 && all(isapprox.(t.value.value, reference.value; rtol=1e-12, atol=1e-6))
        pair == 0 || (@assert t.compile_time == 0; push!(times[1 + access], t.time))
    end
    baseline, access = B.median.(times)
    saving = baseline - access
    B.log(metadata, "MEDIANS city=$city radius=$radius samples=$samples mode=$mode baseline_s=$baseline access_s=$access speedup=$(baseline / access) break_even=$(saving > 0 ? ceil(index.stats.preparation_s / saving) : Inf)")
    return baseline / access
end

function main(args)
    length(args) == 1 || error("Use: benchmark-access.jl NEW_OUTPUT_DIRECTORY")
    output = only(args)
    ispath(output) && error("output exists; choose a new directory")
    isdir(dirname(abspath(output))) || error("output parent must exist")
    mkdir(output)
    open(joinpath(output, "metadata.txt"), "w") do metadata
        open(joinpath(output, "trials.csv"), "w") do trials
            B.log(metadata, "Julia=$VERSION threads=$(Threads.nthreads()) CPU=$(Sys.cpu_info()[1].model) core_resolution=8 result_cache=false")
            for file in [readdir(joinpath(B.ROOT, "router/src"); join=true); joinpath(@__DIR__, "Hierarchy.jl"); joinpath(@__DIR__, "benchmark.jl"); @__FILE__]
                B.log(metadata, "SOURCE $(basename(file)) $(bytes2hex(open(B.sha256, file)))")
            end
            input = joinpath(B.ROOT, "data/austria_adjacent_res8.arrow")
            B.log(metadata, "INPUT $input sha256=$(bytes2hex(open(B.sha256, input))) shuttle=false")
            packed = @timed R.pack_graph(input); graph = packed.value
            @assert (length(graph.h3), length(graph.edge_to), length(graph.departure)) == (20244, 61796, 4791297)
            walking = @timed R.prepare_walking(R.WalkingIndex(graph); max_walk_ms=B.W)
            population = @timed R.load_population(joinpath(B.ROOT, "data/kontur_h3.arrow"))
            aligned = @timed R._prepare_population(population.value, walking.value)
            R._population_schedule_hints(population.value, graph)
            data = (; graph, walking=walking.value, population=population.value)
            B.log(metadata, "BASE pack_s=$(packed.time) walking_s=$(walking.time) population_load_s=$(population.time) align_s=$(aligned.time) graph_bytes=$(Base.summarysize(graph)) population_rows=$(length(data.population.h3)) population_sum=$(sum(Float64, data.population.weights)) rss=$(B.rss())")
            println(trials, "city,radius,origins,samples,mode,variant,pair,wall_s,cpu_s,bytes,gc_s,compile_s,rss_bytes,external_cores,shared,independent,zero_mismatches,max_abs_error,relative_p95,relative_max,mean_population")
            gain, estimate = 0.0, 0
            for (city, centre) in B.CENTRES
                index = B.prepare(data, B.disk(centre, 19), 8, city, metadata)
                hints = R._population_schedule_hints(index.population, index.graph)
                estimate = max(estimate, ceil(Int, Base.summarysize((index.graph, index.prepared, index.destinations, index.direct, index.origins, hints)) * 9919 / 1141))
                for (radius, samples, mode) in ((6, 96, :mean_intersection), (18, 96, :mean_intersection),
                        (18, 96, :reachable_union), (6, 1, :mean_intersection), (6, 4, :mean_intersection))
                    speed = measure(data, index, city, centre, radius, samples, mode, trials, metadata; rounds=mode == :reachable_union ? 1 : 3)
                    radius == 18 && mode == :mean_intersection && (gain = max(gain, speed))
                end
                index = nothing
            end
            GC.gc()
            B.log(metadata, "LARGE_GATE best_1k_speedup=$gain estimated_index_bytes=$estimate available_bytes=$(B.available())")
            if gain >= 1.2 && estimate < 8 * 1024^3 && B.available() > 2estimate + 2 * 1024^3
                city, centre = first(B.CENTRES)
                index = B.prepare(data, B.disk(centre, 57), 8, city * "-large", metadata)
                measure(data, index, city, centre, 57, 96, :mean_intersection, trials, metadata)
            end
            B.log(metadata, "FINISHED maxrss=$(Sys.maxrss()) rss=$(B.rss())")
        end
    end
end
end
abspath(PROGRAM_FILE) == (@__FILE__) && AccessBenchmark.main(ARGS)
