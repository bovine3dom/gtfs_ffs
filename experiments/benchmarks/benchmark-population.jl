using Statistics
using H3
using Arrow
using SHA
const SOURCE_SHA = bytes2hex(sha256(read(joinpath(@__DIR__, "../../router/src/population.jl"))))
include("../../router/src/Reachability.jl")
using .Reachability
const R = Reachability
const MODES = (:mean_intersection, :min_union, :reachable_union)
const DEPARTURE = 28_800_000

function logline(text)
    println(text)
    flush(stdout)
end

function measure(f, label)
    logline("BEGIN $label")
    warm = @timed f()
    logline("WARM $label seconds=$(warm.time) bytes=$(warm.bytes) peak_rss=$(Sys.maxrss())")
    warm.time > 120 && error("Stop: warm call exceeded 120 seconds")
    times, bytes, gc = Float64[], Int[], Float64[]
    result = warm.value
    for run in 1:3
        GC.gc()
        trial = @timed f()
        result = trial.value
        push!(times, trial.time)
        push!(bytes, trial.bytes)
        push!(gc, trial.gctime)
        logline("RUN $label run=$run seconds=$(trial.time) bytes=$(trial.bytes)")
        trial.time > 120 && error("Stop: measured call exceeded 120 seconds")
    end
    logline("MEASURE $label median_s=$(median(times)) median_bytes=$(median(bytes)) gc_s=$gc peak_rss=$(Sys.maxrss())")
    return result
end

function reference(graph, index, weights, origins, window)
    totals = zeros(length(origins), length(MODES))
    counts = zeros(Int, length(origins), length(MODES))
    raw_counts, workers, expansions = Int[], Int[], Int[]
    for (i, origin) in enumerate(origins)
        raw = window == 0 ? route_walking(graph, origin, DEPARTURE, 10_800_000;
            walking_index=index, distance_mode=:straight_line) :
            route_window_walking_cached(graph, origin, DEPARTURE, 10_800_000, window;
                step_ms=900_000, walking_index=index, distance_mode=:straight_line)
        push!(raw_counts, length(raw.h3))
        push!(workers, window == 0 ? 1 : raw.workers)
        push!(expansions, window == 0 ? -1 : raw.routing_expansions)
        samples = window == 0 ? 1 : Int(raw.sample_count)
        for (j, cell) in enumerate(raw.h3)
            reached = window == 0 ? 1 : Int(raw.reachable_samples[j])
            for (k, mode) in enumerate(MODES)
                selected = mode == :mean_intersection ? reached == samples : reached > 0
                selected || continue
                counts[i, k] += 1
                totals[i, k] += get(weights, cell, 0.0) *
                    (mode == :reachable_union ? reached / samples : 1)
            end
        end
    end
    return (; totals, counts, raw_counts, workers, expansions)
end

function main(args)
    network = isempty(args) ? joinpath(@__DIR__, "../../data/rail_and_friends_res6.arrow") : args[1]
    path = length(args) < 2 ? joinpath(@__DIR__, "../../data/kontur_h3.arrow") : args[2]
    logline("ENV julia=$VERSION threads=$(Threads.nthreads()) network=$(abspath(network)) network_bytes=$(filesize(network)) population_bytes=$(filesize(path)) available_bytes=$(Sys.free_memory())")
    logline("SOURCE population_sha256=$SOURCE_SHA")
    source = Arrow.Table(network)
    logline("NETWORK source_rows=$(length(source.from_h3))")
    source = nothing
    packed = @timed pack_graph(network; skip_invalid_durations=true, badajoz_shuttle=true)
    graph = packed.value
    prepared = @timed prepare_walking(WalkingIndex(graph))
    index = prepared.value
    logline("STARTUP pack_s=$(packed.time) pack_bytes=$(packed.bytes) prepare_s=$(prepared.time) prepare_bytes=$(prepared.bytes) vertices=$(length(graph.h3)) edges=$(length(graph.edge_to)) profiles=$(length(graph.departure)) peak_rss=$(Sys.maxrss())")
    loaded = @timed load_population(path)
    population = loaded.value
    @assert length(population.h3) == 32_957_699
    @assert eltype(population.weights) == Float64
    @assert all(w -> w > 0 && isinteger(w), population.weights)
    @assert sum(population.weights) == 8_031_924_024
    unique_count = length(Set(population.h3))
    @assert unique_count == length(population.h3)
    logline("POPULATION load_s=$(loaded.time) load_bytes=$(loaded.bytes) rows=$(length(population.h3)) unique=$unique_count valid_res8=true positive_integers=true total=$(sum(population.weights))")
    for (res, expected) in ((6, 2_016_971), (7, 9_012_014))
        rolled = @timed R._population_rollup(population, res)
        @assert length(rolled.value) == expected
        @assert sum(values(rolled.value)) == 8_031_924_024
        logline("ROLLUP res=$res cells=$(length(rolled.value)) total=$(sum(values(rolled.value))) seconds=$(rolled.time) bytes=$(rolled.bytes) map_bytes=$(Base.summarysize(rolled.value)) peak_rss=$(Sys.maxrss())")
    end
    weights = R._population_rollup(population, graph.resolution)
    GC.gc()
    origin = H3.API.latLngToCell(H3.API.LatLng(deg2rad(48.8566), deg2rad(2.3522)), graph.resolution)::UInt64
    logline("QUERY origin=$(string(origin; base=16)) resolution=$(graph.resolution) departure_h=8 budget_h=3 max_walk_h=1 step_min=15 graph_bytes=$(Base.summarysize(graph)) index_bytes=$(Base.summarysize(index)) peak_rss=$(Sys.maxrss())")
    for window in (0, 3_600_000, 86_400_000), radius in (0, 1, 3)
        window == 86_400_000 && radius == 3 && continue
        origins = sort!(filter(!iszero, H3.API.gridDisk(origin, radius)))
        samples = max(1, window ÷ 900_000)
        label = "radius=$radius window_ms=$window origins=$(length(origins)) samples=$samples queries=$(length(origins) * samples)"
        logline("ORIGINS $label h3=$(string.(origins; base=16))")
        modes = window == 0 ? MODES[1:1] : MODES
        expected = measure(() -> reference(graph, index, weights, origins, window), "reference $label")
        logline("REFERENCE $label raw_cells=$(expected.raw_counts) workers_per_origin=$(expected.workers) expansions_per_origin=$(expected.expansions)")
        for (k, mode) in enumerate(modes)
            result = measure(() -> route_population(graph, population, origin, DEPARTURE, 10_800_000;
                origin_radius=radius, window_ms=window, step_ms=900_000,
                walking_index=index, window_mode=mode), "population $label mode=$mode")
            @assert result.h3 == origins
            @assert all(isapprox.(result.value, expected.totals[:, k]; rtol=1e-12, atol=1e-6))
            logline("RESULT $label mode=$mode workers=$(result.workers) matrix_shared_expansions=$(result.shared_expansions) matrix_query_expansions=$(result.query_expansions) cells_per_origin=$(expected.counts[:, k]) population_per_origin=$(result.value) reference_population=$(expected.totals[:, k]) parity=PASS")
            if k == 1 && radius > 0
                for (i, cell) in enumerate(origins)
                    single = route_population(graph, population, cell, DEPARTURE, 10_800_000;
                        window_ms=window, step_ms=900_000, walking_index=index, window_mode=mode)
                    @assert isapprox(only(single.value), expected.totals[i, k]; rtol=1e-12, atol=1e-6)
                    logline("SINGLE parent_radius=$radius window_ms=$window origins=1 samples=$samples queries=$samples origin=$(string(cell; base=16)) shared_expansions=$(single.shared_expansions) query_expansions=$(single.query_expansions) workers=$(single.workers)")
                end
            end
        end
    end
    logline("COMPLETE peak_rss=$(Sys.maxrss()); all budgets were 3 hours")
end

if abspath(PROGRAM_FILE) == @__FILE__
    main(ARGS)
end
