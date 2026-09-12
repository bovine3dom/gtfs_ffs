module FullNetworkBenchmark
include("benchmark.jl")
const B = HierarchyBenchmark
const R, HR = B.R, B.HierarchyResearch

# Keep all geometry and distance fields. Do not retain per-node neighbor vectors.
function prepare_walking(index, metadata)
    n = length(index.cells)
    counts, network_counts = zeros(Int, n), zeros(Int, n)
    node_id = Dict(h => Int32(i) for (i, h) in enumerate(index.cells))
    Threads.@threads for u in 1:n
        hops = R.walking_cells(index, index.cells[u], B.W)
        counts[u] = length(hops)
        network_counts[u] = count(h -> haskey(node_id, h.cell), hops)
    end
    required = 24sum(counts) + 16sum(network_counts) + 16(n + 1)
    B.log(metadata, "WALK_COUNTS geographic=$(sum(counts)) network=$(sum(network_counts)) packed_bytes=$required available=$(B.available())")
    required + 2^31 < B.available() || error("Full walking arrays exceed available memory, before output IDs and population. No subset was used.")
    geographic = R.PackedWalking(cumsum(vcat(1, counts)), Vector{UInt64}(undef, sum(counts)),
        Vector{UInt32}(undef, sum(counts)), Vector{Float64}(undef, sum(counts)))
    network = R.PackedWalking(cumsum(vcat(1, network_counts)), Vector{Int32}(undef, sum(network_counts)),
        Vector{UInt32}(undef, sum(network_counts)), Vector{Float64}(undef, sum(network_counts)))
    Threads.@threads for u in 1:n
        j, k = geographic.offsets[u], network.offsets[u]
        for hop in R.walking_cells(index, index.cells[u], B.W)
            geographic.targets[j], geographic.durations[j], geographic.distances[j] = hop.cell, hop.duration_ms, hop.distance_km
            j += 1
            v = get(node_id, hop.cell, Int32(0))
            if !iszero(v)
                network.targets[k], network.durations[k], network.distances[k] = v, hop.duration_ms, hop.distance_km
                k += 1
            end
        end
        @assert j == geographic.offsets[u + 1] && k == network.offsets[u + 1]
    end
    cells, ids = copy(index.cells), copy(node_id)
    targets = Vector{Int32}(undef, length(geographic.targets))
    for (j, cell) in enumerate(geographic.targets)
        targets[j] = get!(ids, cell) do
            push!(cells, cell)
            Int32(length(cells))
        end
    end
    output = R.PackedWalking(geographic.offsets, targets, geographic.durations, geographic.distances)
    return R.WalkingIndex(index.cells, index.centres, index.bins, index.resolution,
        R.WalkingAdjacency(UInt32(B.W), node_id, geographic, network, cells, ids, output))
end

function stage(f, metadata, name)
    GC.gc()
    B.log(metadata, "START $name rss=$(B.rss()) available=$(B.available())")
    t = @timed f()
    GC.gc()
    B.log(metadata, "END $name seconds=$(t.time) allocated=$(t.bytes) gc_s=$(t.gctime) compile_s=$(t.compile_time) rss=$(B.rss()) available=$(B.available()) maxrss=$(Sys.maxrss())")
    return t.value, t.time
end

function measure(data, h, g, centre, city, radius, samples, step, hours, mode, prep_s, trials, quality, values, metadata)
    origins = B.disk(centre, radius)
    results = Dict{Symbol,Any}()
    times = Dict(v => Float64[] for v in (:fine, :boarding))
    for pair in 0:3, variant in circshift([:fine, :boarding], pair)
        GC.gc()
        cpu, host = B.cpu(), B.hostcpu()
        options = (; window_ms=samples * step, step_ms=step, window_mode=mode, max_walk_ms=B.W, origin_batch_size=64)
        t = @timed if variant == :fine
            R.route_population(data.graph, data.population, centre, 28_800_000, hours * B.W;
                options..., walking_index=data.walking, origin_radius=radius)
        else
            HR.route_boarding(h, g, origins, 28_800_000, hours * B.W; options...)
        end
        cpu, host = B.cpu() - cpu, B.hostcpu() - host
        @assert t.value.h3 == origins
        if pair == 0
            results[variant] = t.value
        else
            @assert all(isapprox.(t.value.value, results[variant].value; atol=1e-6, rtol=1e-12))
            push!(times[variant], t.time)
        end
        B.row(trials, (city, radius, samples, step, hours, mode, variant, pair, t.time, cpu, t.bytes,
            t.gctime, t.compile_time, B.rss(), max(0.0, host-cpu)/t.time, t.value.shared_expansions, t.value.query_expansions))
        B.log(metadata, "TRIAL city=$city radius=$radius samples=$samples step=$step hours=$hours mode=$mode variant=$variant pair=$pair seconds=$(t.time)")
    end
    fine, candidate = results[:fine].value, results[:boarding].value
    delta = candidate .- fine
    relative = abs.(delta[fine .> 0]) ./ fine[fine .> 0]
    percentiles = isempty(relative) ? fill(NaN, 3) : B.quantile(relative, [0.5, 0.95, 1.0])
    B.row(quality, (city, radius, samples, step, hours, mode, sum(fine), B.mean(delta), sum(abs, delta)/sum(fine),
        percentiles..., minimum(delta), maximum(delta), B.median(times[:fine]), B.median(times[:boarding]),
        prep_s + B.median(times[:boarding])))
    for i in eachindex(origins)
        B.row(values, (city, radius, samples, step, hours, mode, string(origins[i]; base=16), fine[i], candidate[i], delta[i]))
    end
end

function main(args)
    length(args) == 1 || error("Use: julia --project=router -t 8 experiments/hierarchy/benchmark-full.jl NEW_DIRECTORY")
    output = only(args)
    ispath(output) && error("Output exists; previous results must remain unchanged")
    isdir(dirname(abspath(output))) || error("Output parent must exist")
    mkdir(output)
    open(joinpath(output, "metadata.txt"), "w") do metadata
        try
            B.log(metadata, "PROMOTION=false status=started pid=$(getpid()) Julia=$VERSION threads=$(Threads.nthreads()) HEAD=$(strip(read(`git -C $(B.ROOT) rev-parse HEAD`, String)))")
            B.log(metadata, "batch=64 history=descending hints=8 max_walk_ms=$(B.W) skip_invalid_durations=true badajoz_shuttle=true distance=retained result_cache=false")
            for file in [filter(isfile, readdir(joinpath(B.ROOT, "router/src"); join=true));
                    joinpath.(@__DIR__, ["Hierarchy.jl", "Boarding.jl", "benchmark.jl", "benchmark-full.jl"])]
                B.log(metadata, "SOURCE $file $(bytes2hex(open(B.sha256, file)))")
            end
            input = joinpath(B.ROOT, "data/everything_res8.arrow")
            B.log(metadata, "INPUT path=$input bytes=$(filesize(input)) sha256=$(bytes2hex(open(B.sha256, input)))")
            B.log(metadata, "POPULATION sha256=$(bytes2hex(open(B.sha256, joinpath(B.ROOT, "data/kontur_h3.arrow"))))")
            graph, _ = stage(metadata, "fine_load") do
                R.pack_graph(input; skip_invalid_durations=true, badajoz_shuttle=true, progress=true)
            end
            @assert graph.resolution == 8
            B.log(metadata, "FINE resolution=$(graph.resolution) nodes=$(length(graph.h3)) edges=$(length(graph.edge_to)) profiles=$(length(graph.arrival)) distance=$(isnothing(graph.distance_km) ? 0 : length(graph.distance_km))")
            walking, _ = stage(metadata, "fine_walking") do
                prepare_walking(R.WalkingIndex(graph), metadata)
            end
            population, _ = stage(metadata, "fine_population") do
                p = R.load_population(joinpath(B.ROOT, "data/kontur_h3.arrow"))
                R._prepare_population(p, walking)
                p
            end
            data = (; graph, walking, population)
            open(joinpath(output, "trials.csv"), "w") do trials
                open(joinpath(output, "quality.csv"), "w") do quality
                    open(joinpath(output, "values.csv"), "w") do values
                        println(trials, "city,radius,samples,step_ms,hours,mode,variant,pair,wall_s,cpu_s,bytes,gc_s,compile_s,rss,external_cores,shared,separate")
                        println(quality, "city,radius,samples,step_ms,hours,mode,fine_sum,bias,wmae,relative_p50,relative_p95,relative_max,min_signed,max_signed,fine_median_s,warm_median_s,prepared_set_cold_s")
                        println(values, "city,radius,samples,step_ms,hours,mode,origin_h3,fine,boarding,signed_error")
                        for (city, lat, lon) in (("Paris", 48.85, 2.35), ("London", 51.5, -0.12))
                            centre = B.H3.API.latLngToCell(B.H3.API.LatLng(deg2rad(lat), deg2rad(lon)), 8)::UInt64
                            origins = B.disk(centre, 19)
                            @assert length(origins) == 1141 && length(B.disk(centre, 18)) == 1027
                            B.log(metadata, "CENTRE city=$city lat=$lat lon=$lon h3=$(string(centre; base=16)) resolution=8 prepared=1141 query=1027")
                            h, hp = stage(metadata, "hierarchy_$city") do
                                HR.prepare_hierarchy(graph, population, origins; core_resolution=6, walking_index=walking)
                            end
                            B.log(metadata, "HIERARCHY city=$city stats=$(h.stats)")
                            g, gp = stage(metadata, "boarding_$city") do
                                HR.prepare_boarding(h)
                            end
                            for (radius, samples, step, hours) in ((6, 4, 60_000, 3), (18, 96, 900_000, 3),
                                    (18, 96, 60_000, 3), (6, 180, 60_000, 6), (6, 180, 60_000, 12)),
                                    mode in (:mean_intersection, :reachable_union)
                                measure(data, h, g, centre, city, radius, samples, step, hours, mode, hp+gp, trials, quality, values, metadata)
                            end
                            h = g = nothing
                            GC.gc()
                        end
                    end
                end
            end
            B.log(metadata, "PROMOTION=false status=completed_requires_quality_review maxrss=$(Sys.maxrss())")
        catch error
            B.log(metadata, "PROMOTION=false status=blocked error=$(sprint(showerror, error)) maxrss=$(Sys.maxrss())")
            rethrow()
        end
    end
end
end
abspath(PROGRAM_FILE) == (@__FILE__) && FullNetworkBenchmark.main(ARGS)
