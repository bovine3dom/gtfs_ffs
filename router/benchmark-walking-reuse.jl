using Printf, Statistics, SHA, TOML, Profile
using Arrow, H3

const ROOT = dirname(@__DIR__)
const BASELINE_REV = get(ENV, "WALKING_BASELINE_REV", "8edcb64459f542cab4c9886c722d8660da6a3433")
const CURRENT_SOURCES = Dict(name => read(joinpath(@__DIR__, "src", name), String)
    for name in readdir(joinpath(@__DIR__, "src")) if endswith(name, ".jl"))
const BASELINE_SOURCES = Dict{String,String}()

# Resolve every include from one source snapshot, without a worktree or second pack.
function snapshot_source(name, sources; baseline=false)
    source = get!(sources, name) do
        read(`git -C $ROOT show $BASELINE_REV:router/src/$name`, String)
    end
    return replace(source, r"include\(\"([^\"]+)\"\)" => text -> begin
        child = match(r"\"([^\"]+)\"", text)[1]
        expanded = snapshot_source(child, sources; baseline)
        path = baseline ? "$BASELINE_REV/router/src/$child" : joinpath(@__DIR__, "src", child)
        "Base.include_string(@__MODULE__, $(repr(expanded)), $(repr(path)))"
    end)
end
Base.include_string(Main, snapshot_source("Reachability.jl", CURRENT_SOURCES), joinpath(@__DIR__, "src", "Reachability.jl"))
Base.include_string(Main, replace(snapshot_source("Reachability.jl", BASELINE_SOURCES; baseline=true),
    "module Reachability" => "module Baseline"; count=1), "$BASELINE_REV/router/src/Reachability.jl")
const R = Reachability
const READY = 28_800_000
const WINDOW_FIELDS = (:h3, :elapsed_sum_ms, :elapsed_ms, :reachable_elapsed_ms,
                       :reachable_samples, :distance_km, :sample_count)

function check(actual, expected, label)
    isnothing(expected) && return
    fields = hasproperty(actual, :sample_count) ? WINDOW_FIELDS : (:h3, :arrival, :distance_km)
    for field in fields
        isequal(getproperty(actual, field), getproperty(expected, field)) ||
            error("PARITY FAILED $label field=$field")
    end
end

function measure(f, label; expected=nothing, repetitions=3, warm=true, adaptive=false)
    warm && check(f(), expected, "$label warmup")
    GC.gc()
    runs = NamedTuple[]
    result = nothing
    for repetition in 1:repetitions
        run = @timed f()
        result = run.value
        check(result, expected, "$label repetition=$repetition")
        push!(runs, (seconds=run.time, bytes=run.bytes, gc_seconds=run.gctime))
        # Benchmark repetition policy only: never truncate a route or its output.
        adaptive && run.time >= 10 && break
    end
    seconds = median(r.seconds for r in runs)
    statistic = length(runs) == 1 ? "single_warmed_s" : "median_s"
    @printf("TIMING %s %s=%.9f runs_s=%s allocated_MiB=%.3f gc_s=%.6f peak_RSS_MiB=%.3f parity=%s\n",
        label, statistic, seconds, join(round.([r.seconds for r in runs]; digits=9), ','),
        median(r.bytes for r in runs)/2.0^20, median(r.gc_seconds for r in runs),
        Sys.maxrss()/2.0^20, isnothing(expected) ? "anchor" : "PASS")
    if hasproperty(result, :sample_count)
        metadata = ("$field=$(getproperty(result, field))" for field in
            (:sample_count, :searches, :reused_samples, :workers, :full_searches,
             :repair_searches, :profile_lookups, :routing_expansions) if hasproperty(result, field))
        println("COUNTERS $label cells=$(length(result.h3)) ", join(metadata, ' '))
    elseif hasproperty(result, :h3)
        println("COUNTERS $label cells=$(length(result.h3))")
    end
    flush(stdout)
    return result, seconds
end

module Probe
using DataStructures, H3
const R = Main.Reachability
for name in (:Graph, :WalkingIndex, :WalkingNeighbor, :WalkingRange, :INF, :PERIOD, :MAX_BUDGET_MS,
             :query_times, :next_connection, :walking_neighbors)
    @eval const $name = R.$name
end
Base.@kwdef mutable struct Metrics
    graph_ns::UInt64 = 0
    replay_ns::UInt64 = 0
    warming_ns::UInt64 = 0
    seed_ns::UInt64 = 0
    egress_ns::UInt64 = 0
    output_ns::UInt64 = 0
    neighbor_ns::UInt64 = 0
    cells_ns::UInt64 = 0
    disk_ns::UInt64 = 0
    polygon_ns::UInt64 = 0
    shared_access_ns::UInt64 = 0
    geo_requests::Int = 0
    geo_zero_requests::Int = 0
    geo_local_misses::Int = 0
    geo_calls::Int = 0
    geo_grows::Int = 0
    partial_calls::Int = 0
    neighbor_requests::Int = 0
    neighbor_local_misses::Int = 0
    neighbor_calls::Int = 0
    disk_successes::Int = 0
    polygon_fallbacks::Int = 0
    profile_lookups::Int = 0
    routing_expansions::Int = 0
    replay_visits::Int = 0
    warming_passes::Int = 0
    warming_requests::Int = 0
    warming_builds::Int = 0
    eligible::Set{UInt64} = Set{UInt64}()
end
const TOPOLOGIES = Any[]
const WORKSPACES = Ref{Any}(nothing)
const AGGREGATE_NS = Ref(UInt64(0))
const FINISH_NS = Ref(UInt64(0))
function record_topology(topology)
    push!(TOPOLOGIES, topology) # Production creates workspaces before spawning workers.
    return topology
end

function measured_hops(topology, origin, limit, geographic, previous_radius)
    s = topology.stats
    started = time_ns()
    if geographic
        s.geo_calls += 1
        s.geo_grows += previous_radius > 0
        s.partial_calls += limit < topology.limit
        hops = measured_cells(topology.index, origin, limit, s)
        s.cells_ns += time_ns() - started
    else
        s.neighbor_calls += 1
        hops = walking_neighbors(topology.index, origin, limit)
        s.neighbor_ns += time_ns() - started
    end
    return hops
end

function measured_cells(index, origin, limit, s)
    R._walking_validate(index, origin, limit)
    centre = H3.API.cellToLatLng(origin)::H3.API.LatLng
    started = time_ns()
    candidates = R._walking_disk(origin, centre, limit)
    s.disk_ns += time_ns() - started
    if isnothing(candidates)
        s.polygon_fallbacks += 1
        started = time_ns()
        result = R._walking_polygon_cells(index, origin, centre, limit)
        s.polygon_ns += time_ns() - started
        return result
    end
    s.disk_successes += 1
    result = WalkingNeighbor[]
    for cell in candidates
        (iszero(cell) || cell == origin) && continue
        hop = R._walking_neighbor(origin, cell, centre, H3.API.cellToLatLng(cell)::H3.API.LatLng, limit)
        isnothing(hop) || push!(result, hop)
    end
    return sort!(result; by=hop -> hop.cell)
end
end

function change(source, old, new)
    @assert length(findall(old, source)) == 1 old
    return replace(source, old => new; count=1)
end

function install_probe()
    point = CURRENT_SOURCES["walking.jl"]
    # Preserve the snapshot's cache types, optional constructor and locking rules.
    point = change(point, "    shared::Union{Nothing,WalkingGeometryCache}\n",
        "    shared::Union{Nothing,WalkingGeometryCache}\n    stats::Metrics\n")
    point = change(point, "    WalkingTopology(index, UInt32(limit), Dict{UInt64,WalkingGeometryEntry}(),",
        "    record_topology(WalkingTopology(index, UInt32(limit), Dict{UInt64,WalkingGeometryEntry}(),")
    point = change(point, "                    Dict{UInt64,WalkingGeometryEntry}(), shared)",
        "                    Dict{UInt64,WalkingGeometryEntry}(), shared, Metrics()))")
    point = change(point, "    iszero(limit) && return WalkingNeighbor[]",
        "    s = topology.stats\n    if geographic\n        s.geo_requests += 1\n        s.geo_zero_requests += iszero(limit)\n        push!(s.eligible, origin)\n    else\n        s.neighbor_requests += 1\n    end\n    iszero(limit) && return WalkingNeighbor[]")
    point = change(point, "        shared = topology.shared",
        "        if geographic\n            s.geo_local_misses += 1\n        else\n            s.neighbor_local_misses += 1\n        end\n        shared = topology.shared")
    point = change(point, "entry = (UInt32(limit), geographic ? walking_cells(topology.index, origin, limit) :\n                                                walking_neighbors(topology.index, origin, limit))",
        "entry = (UInt32(limit), measured_hops(topology, origin, limit, geographic, isnothing(entry) ? UInt32(0) : entry[1]))")
    point = change(point, "            entry_lock, published = lock(shared.lock) do",
        "            shared_start = time_ns()\n            entry_lock, published = lock(shared.lock) do")
    point = change(point, "hops = geographic ? walking_cells(topology.index, origin, limit) :\n                                        walking_neighbors(topology.index, origin, limit)",
        "hops = measured_hops(topology, origin, limit, geographic, published[][1])")
    point = change(point, "                published[]\n            end\n        end",
        "                published[]\n            end\n            s.shared_access_ns += time_ns() - shared_start\n        end")
    point = change(point, "    topology.index.resolution == graph.resolution",
        "    s = topology.stats; graph_start = time_ns()\n    topology.index.resolution == graph.resolution")
    point = change(point, "        if state == 0", "        s.routing_expansions += 1\n        if state == 0")
    point = change(point, "                connection = next_connection", "                s.profile_lookups += 1\n                connection = next_connection")
    point = change(point, "    return _walking_result(", "    s.graph_ns += time_ns() - graph_start\n    return _walking_result(")
    point = change(point, "    source = get(graph.node_id, origin, Int32(0))\n    result =",
        "    s = topology.stats; seed_start = time_ns()\n    source = get(graph.node_id, origin, Int32(0))\n    result =")
    point = change(point, "    # Geographic-only cells", "    s.seed_ns += time_ns() - seed_start\n    egress_start = time_ns()\n    # Geographic-only cells")
    point = change(point, "    cells = sort!(collect(keys(result)))", "    s.egress_ns += time_ns() - egress_start\n    output_start = time_ns()\n    cells = sort!(collect(keys(result)))")
    point = change(point, "    return (h3=cells,", "    output = (h3=cells,")
    point = change(point, "distance_km=Float64[result[h][2] for h in cells])\nend",
        "distance_km=Float64[result[h][2] for h in cells])\n    s.output_ns += time_ns() - output_start\n    return output\nend")
    Base.include_string(Probe, point, "probe_walking.jl")

    window = CURRENT_SOURCES["walking_window.jl"]
    window = change(window, "    penalty = UInt64(samples)", "    started = time_ns()\n    penalty = UInt64(samples)")
    window = change(window, "        acc[cell] = (total, reached, km)\n    end\nend",
        "        acc[cell] = (total, reached, km)\n    end\n    AGGREGATE_NS[] += time_ns() - started\nend")
    window = change(window, "    h3 = sort!(collect(keys(acc)))", "    started = time_ns()\n    h3 = sort!(collect(keys(acc)))")
    window = change(window, "    return (; h3,", "    output = (; h3,")
    window = change(window, "sample_count=UInt32(samples), elapsed_sum_ms, kwargs...)\nend",
        "sample_count=UInt32(samples), elapsed_sum_ms, kwargs...)\n    FINISH_NS[] += time_ns() - started\n    return output\nend")
    Base.include_string(Probe, window, "probe_walking_window.jl")

    cached = CURRENT_SOURCES["walking_catchup.jl"]
    cached = change(cached, "    outcomes = Vector{Any}", "    WORKSPACES[] = workspaces\n    outcomes = Vector{Any}")
    cached = change(cached, "    for sample in last:-1:first", "    s = topology.stats\n    for sample in last:-1:first\n        graph_start = time_ns()")
    cached = change(cached, "            routing_expansions += 1", "            routing_expansions += 1\n            s.routing_expansions += 1")
    cached = change(cached, "                    profile_lookups += 1", "                    profile_lookups += 1\n                    s.profile_lookups += 1")
    cached = change(cached, "        _walking_catchup_replay!(workspace, graph, origin, source, ready, cutoff)",
        "        s.graph_ns += time_ns() - graph_start\n        replay_start = time_ns()\n        s.replay_visits += _walking_catchup_replay!(workspace, graph, origin, source, ready, cutoff)\n        s.replay_ns += time_ns() - replay_start")
    cached = change(cached, "        if sample == last && workers > 1",
        "        if sample == last && workers > 1\n            warming_start = time_ns()\n            warming_requests, warming_builds = s.geo_requests, s.geo_calls\n            s.warming_passes += 1")
    cached = change(cached, "            end\n        end\n        workspace.points[sample - first + 1] = _walking_result(",
        "            end\n            s.warming_ns += time_ns() - warming_start\n            s.warming_requests += s.geo_requests - warming_requests\n            s.warming_builds += s.geo_calls - warming_builds\n        end\n        workspace.points[sample - first + 1] = _walking_result(")
    Base.include_string(Probe, cached, "probe_walking_catchup.jl")
end
install_probe()

function probe(f, label, expected)
    empty!(Probe.TOPOLOGIES)
    Probe.WORKSPACES[] = nothing
    Probe.AGGREGATE_NS[] = Probe.FINISH_NS[] = 0
    GC.gc()
    run = @timed f()
    check(run.value, expected, "probe $label")
    topologies = Probe.TOPOLOGIES
    stats = getproperty.(topologies, :stats)
    println("PROBE $label wall_s=$(run.time) workers=$(length(topologies)) parity=PASS")
    for field in fieldnames(Probe.Metrics)
        field == :eligible && continue
        value = sum(getfield(s, field) for s in stats)
        println("PHASE $label $field=$value")
    end
    println("PHASE $label aggregate_ns=$(Probe.AGGREGATE_NS[]) finish_ns=$(Probe.FINISH_NS[])")
    coverage = sum(length(t.coverage) for t in topologies)
    neighbors = sum(length(t.neighbors) for t in topologies)
    unique_coverage = length(union((Set(keys(t.coverage)) for t in topologies)...))
    unique_eligible = length(union((s.eligible for s in stats)...))
    # One traversal deduplicates shared registries, locks, and published vectors.
    cache_bytes = Base.summarysize([(t.coverage, t.neighbors, t.shared) for t in topologies])
    shared = first(topologies).shared
    shared_entries = isnothing(shared) ? 0 : length(shared.entries)
    @assert all(t -> t.shared === shared, topologies)
    shared_bytes = isnothing(shared) ? 0 : Base.summarysize(shared)
    workspaces = Probe.WORKSPACES[]
    # Include the one resident index: its immutable fields are inlined, so a
    # type exclusion would not reliably exclude their reachable arrays.
    workspace_bytes = isnothing(workspaces) ? 0 :
        Base.summarysize(workspaces; exclude=Probe.Metrics)
    @printf("MEMORY %s coverage_entries=%d unique_coverage=%d neighbor_entries=%d unique_eligible=%d shared_entries=%d unique_cache_MiB=%.3f shared_registry_MiB=%.3f unique_workspaces_MiB=%.3f peak_RSS_MiB=%.3f\n",
        label, coverage, unique_coverage, neighbors, unique_eligible, shared_entries,
        cache_bytes/2.0^20, shared_bytes/2.0^20, workspace_bytes/2.0^20, Sys.maxrss()/2.0^20)
    if hasproperty(run.value, :profile_lookups)
        @assert run.value.profile_lookups == sum(s.profile_lookups for s in stats)
        @assert run.value.routing_expansions == sum(s.routing_expansions for s in stats)
    end
    empty!(Probe.TOPOLOGIES)
    Probe.WORKSPACES[] = nothing
    flush(stdout)
end

function sample_profile(f, label, seconds, output)
    Profile.clear()
    repetitions = clamp(ceil(Int, 2 / seconds), 1, 100)
    Profile.@profile for _ in 1:repetitions
        f()
    end
    for format in (:flat, :tree)
        open(joinpath(output, "$label.$format.txt"), "w") do io
            Profile.print(IOContext(io, :displaysize => (10000, 220)); format, C=true,
                mincount=3, sortedby=:count, maxdepth=45, threads=1)
        end
    end
    println("PROFILE $label repetitions=$repetitions path=$output/$label.{flat,tree}.txt")
end

function benchmark(graph, index, old_graph, old_index, origin, budget, window, step, output)
    samples = cld(window, step)
    case = "budget$(div(budget, 3_600_000))h_samples$samples"
    baseline_f = () -> Baseline.route_window_walking(old_graph, origin, READY, budget, window; step_ms=step, walking_index=old_index)
    reference_f = () -> R.route_window_walking(graph, origin, READY, budget, window; step_ms=step, walking_index=index)
    short = budget == 10_800_000 && samples == 12
    expected, old_s = measure(baseline_f, "$case original"; repetitions=samples == 1440 ? 1 : short ? 5 : 3,
        warm=short, adaptive=!short)
    reference, reference_s = measure(reference_f, "$case reference"; expected, repetitions=short ? 5 : 3,
        warm=short, adaptive=!short)
    times = Dict("original" => old_s, "reference" => reference_s)
    for workers in (1, 2, 4)
        cached_f = () -> R.route_window_walking_cached(graph, origin, READY, budget, window; step_ms=step, walking_index=index, workers)
        _, seconds = measure(cached_f, "$case cached$workers"; expected,
            repetitions=short ? 5 : 3, warm=short, adaptive=!short)
        times["cached$workers"] = seconds
        println("SPEEDUP $case cached$workers original=$(old_s/seconds) reference=$(reference_s/seconds)")
        if short || seconds < 15
            # Compile probe paths before collecting counters, including task-spawn code.
            probe_f = () -> Probe.route_window_walking_cached(graph, origin, READY, budget, window; step_ms=step, walking_index=index, workers)
            short && probe_f()
            probe(probe_f, "$case cached$workers", expected)
            short && workers == 1 && sample_profile(cached_f, "$case-cached1", seconds, output)
        end
    end
    probe_f = () -> Probe.route_window_walking(graph, origin, READY, budget, window; step_ms=step, walking_index=index)
    short && probe_f()
    probe(probe_f, "$case reference", expected)
    short && sample_profile(reference_f, "$case-reference", reference_s, output)
    body, _ = measure(() -> R.window_arrow(graph, reference, origin, "split"; metric="distance_time_quantile"), "$case arrow_ranks")
    println("PAYLOAD $case bytes=$(length(body))")
    return times
end

function main(args)
    1 <= length(args) <= 2 || error("usage: julia --project=router --threads=4 router/benchmark-walking-reuse.jl input.arrow [output-directory]")
    input = abspath(first(args))
    output = length(args) == 2 ? abspath(args[2]) : mktempdir("/tmp/opencode"; prefix="walking-reuse-", cleanup=false)
    isdir(output) || error("create the output directory before running the benchmark")
    println("ENV julia=$VERSION threads=$(Threads.nthreads()) cpu=$(Sys.CPU_NAME) loadavg=$(Sys.loadavg()) baseline=$BASELINE_REV output=$output")
    println("QUERY Paris departure=08:00 default_max_walk_s=3600 chunk_size=64 GPU=false")
    metadata = Dict("baseline_revision" => BASELINE_REV, "julia" => string(VERSION),
        "threads" => Threads.nthreads(), "input" => input, "input_bytes" => filesize(input),
        "current_sha256" => Dict(name => bytes2hex(sha256(source)) for (name, source) in CURRENT_SOURCES),
        "baseline_sha256" => Dict(name => bytes2hex(sha256(source)) for (name, source) in BASELINE_SOURCES))
    open(joinpath(output, "sources.toml"), "w") do io
        TOML.print(io, metadata; sorted=true)
    end
    flush(stdout)
    table = Arrow.Table(input)
    println("INPUT rows=$(length(table.from_h3))")
    packed = @timed R.pack_graph(table; skip_invalid_durations=true)
    graph = packed.value
    table = nothing
    @assert fieldnames(R.Graph) == fieldnames(Baseline.Graph)
    old_graph = Baseline.Graph((getfield(graph, field) for field in fieldnames(R.Graph))...)
    @assert all(getfield(graph, field) === getfield(old_graph, field) for field in fieldnames(R.Graph))
    index, old_index = R.WalkingIndex(graph), Baseline.WalkingIndex(old_graph)
    origin = H3.API.latLngToCell(H3.API.LatLng(deg2rad(48.8566), deg2rad(2.3522)), graph.resolution)::UInt64
    GC.gc()
    println("GRAPH res=$(graph.resolution) vertices=$(length(graph.h3)) edges=$(length(graph.edge_to)) profiles=$(length(graph.departure)) pack_s=$(packed.time) graph_MiB=$(Base.summarysize(graph)/2.0^20) origin=$(string(origin; base=16)) shared_fields=$(fieldcount(R.Graph)) identity=PASS peak_RSS_MiB=$(Sys.maxrss()/2.0^20)")
    wait_pid = tryparse(Int, get(ENV, "WALKING_BENCH_WAIT_PID", ""))
    if !isnothing(wait_pid)
        println("WAIT pid=$wait_pid before timing")
        flush(stdout)
        while isdir("/proc/$wait_pid")
            sleep(1)
        end
    end
    while true
        tests = filter(line -> occursin("router/test/runtests.jl", line),
            split(read(ignorestatus(`ps -C julia -o pid=,args=`), String), '\n'))
        isempty(tests) && break
        println("WAIT full_suite=$(join(strip.(tests), '|'))")
        flush(stdout)
        sleep(5)
    end
    println("TIMING_START loadavg=$(Sys.loadavg())")
    Profile.init(n=10^7, delay=0.001)
    baseline, _ = measure(() -> Baseline.route_walking(old_graph, origin, READY, 10_800_000; walking_index=old_index), "point3h original"; repetitions=5)
    measure(() -> R.route_walking(graph, origin, READY, 10_800_000; walking_index=index), "point3h reference"; expected=baseline, repetitions=5)
    benchmark(graph, index, old_graph, old_index, origin, 10_800_000, 3_600_000, 300_000, output)
    benchmark(graph, index, old_graph, old_index, origin, 10_800_000, 86_400_000, 60_000, output)
    week = benchmark(graph, index, old_graph, old_index, origin, 604_800_000, 3_600_000, 300_000, output)
    estimate = max(week["cached1"], week["cached4"]) * 120
    if estimate < 60
        println("OPTIONAL week_budget_samples1440 estimate_s=$estimate baseline_comparison=false")
        expected, _ = measure(() -> R.route_window_walking_cached(graph, origin, READY, 604_800_000, 86_400_000;
            step_ms=60_000, walking_index=index, workers=1), "budget168h_samples1440 cached1"; repetitions=1, warm=false)
        measure(() -> R.route_window_walking_cached(graph, origin, READY, 604_800_000, 86_400_000;
            step_ms=60_000, walking_index=index, workers=4), "budget168h_samples1440 cached4"; expected, repetitions=1, warm=false)
    else
        println("SKIP optional_week_budget_samples1440 estimated_max_cached1_4_s=$estimate benchmark_gate_s=60 no_application_limit=true")
    end
    changed = [name for (name, source) in CURRENT_SOURCES if read(joinpath(@__DIR__, "src", name), String) != source]
    println("COMPLETE loaded_snapshot_sha256_recorded=true subsequently_changed_sources=$(join(changed, ',')) peak_RSS_MiB=$(Sys.maxrss()/2.0^20) loadavg=$(Sys.loadavg())")
end

if abspath(PROGRAM_FILE) == @__FILE__
    main(ARGS)
end
