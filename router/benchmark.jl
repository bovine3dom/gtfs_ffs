using Dates, Printf, Random, SHA, Statistics, TOML
using Arrow, H3, HTTP, oneAPI
import KernelAbstractions as KA
const SOURCE_FILES = ("src/Reachability.jl", "src/kernels.jl", "src/window.jl", "Manifest.toml", "benchmark.jl")
const SOURCE_HASHES = Dict(file => bytes2hex(open(sha256, joinpath(@__DIR__, file))) for file in SOURCE_FILES)
include("src/Reachability.jl")
using .Reachability

route(graph::Graph, origin, departure, budget) = route_cpu(graph, origin, departure, budget)
route(router::KernelRouter, origin, departure, budget) = route_kernel!(router, origin, departure, budget)
timed_route(router, case) = @timed route(router, case.origin, case.departure_ms, case.budget_ms)
timed_handler(handler, request) = @timed handler(request)

function workloads(graph)
    degree = diff(graph.out_ptr)
    transit_degree = zeros(Int, length(graph.h3))
    for (from, to) in zip(graph.edge_from, graph.edge_to)
        from == to || (transit_degree[from] += 1)
    end
    connected = findall(>(1), transit_degree)
    isempty(connected) && error("benchmark needs connected rail cells")
    coordinates = H3.API.cellToLatLng.(graph.h3)
    origins = NamedTuple{(:name, :kind, :node, :snap_km),Tuple{String,String,Int,Float64}}[]
    for (name, lat, lng) in (("Paris", 48.8566, 2.3522), ("London", 51.5074, -0.1278),
                            ("Berlin", 52.5200, 13.4050), ("Madrid", 40.4168, -3.7038),
                            ("Helsinki", 60.1699, 24.9384), ("Lisbon", 38.7223, -9.1393))
        latitude, longitude = deg2rad(lat), deg2rad(lng)
        scores = [sin(latitude) * sin(coordinates[i].lat) +
            cos(latitude) * cos(coordinates[i].lat) * cos(longitude - coordinates[i].lng) for i in connected]
        nearest = argmax(scores)
        node = connected[nearest]
        distance = 6371.0088 * acos(clamp(scores[nearest], -1, 1))
        distance <= 25 || error("no connected cell within 25 km of $name; this benchmark needs a representative European snapshot")
        any(origin -> origin.node == node, origins) && error("duplicate city origin")
        push!(origins, (name=name, kind="city", node=node, snap_km=distance))
    end
    for (name, count) in (("one_edge", 1), ("terminal", 0))
        node = findfirst(==(count), transit_degree)
        isnothing(node) || push!(origins, (name=name, kind="control", node=node, snap_km=0.0))
    end
    cases = []
    for origin in origins, (clock, departure) in (("08:00:00", 28_800_000), ("12:00:00", 43_200_000), ("23:50:00", 85_800_000)), hours in (3, 12, 24, 168)
        h = graph.h3[origin.node]
        push!(cases, (id=length(cases) + 1, name=origin.name, kind=origin.kind, origin=h,
            index=H3.API.h3ToString(h), departure=clock, departure_ms=departure,
            horizon_h=hours, budget_ms=hours * 3_600_000, outdegree=degree[origin.node],
            transit_outdegree=transit_degree[origin.node], snap_km=origin.snap_km,
            lat=rad2deg(coordinates[origin.node].lat), lng=rad2deg(coordinates[origin.node].lng)))
    end
    return cases
end

function write_csv(path, rows)
    open(path, "w") do io
        println(io, join(keys(first(rows)), ','))
        # Columns contain only generated identifiers and numbers, never arbitrary input text.
        for row in rows
            println(io, join(values(row), ','))
        end
    end
end

function main(args)
    2 <= length(args) <= 3 || error("usage: julia --project=router --threads=4 router/benchmark.jl <input.arrow> <output-directory> [samples=20]")
    input, output = args[1:2]
    repetitions = length(args) == 3 ? parse(Int, args[3]) : 20
    repetitions > 0 || error("samples must be positive")
    ispath(output) && (!isdir(output) || !isempty(readdir(output))) && error("output directory must be new or empty")
    oneAPI.functional() || error("oneAPI is unavailable; no silent CPU fallback")
    gpu_info = sprint(oneAPI.versioninfo)
    print(gpu_info)
    started = string(now(UTC))
    starting_load = Sys.loadavg()
    input_sha = bytes2hex(open(sha256, input))
    packed = @timed pack_graph(input; skip_invalid_durations=true)
    graph = packed.value
    isnothing(graph.distance_km) || error("this arrival-only CPU/GPU comparison requires a four-column graph; distance requests always use Dijkstra")
    cases = workloads(graph)
    allocation = Dict{String,Float64}()
    routers = Any[graph]
    names = ["dijkstra", "ka_cpu", "oneapi"]
    for (name, backend) in (("ka_cpu", KA.CPU()), ("oneapi", oneAPI.oneAPIBackend()))
        result = @timed KernelRouter(graph, backend)
        allocation[name] = result.time
        push!(routers, result.value)
    end
    handlers = map(routers) do router
        make_handler(graph; route=(h, t, b) -> route(router, h, t, b))
    end
    requests = [HTTP.Request("GET", "/reachable?index=$(c.index)&departure=$(c.departure)&budget_s=$(c.budget_ms ÷ 1000)&encoding=split") for c in cases]
    first_calls = []
    for backend in eachindex(routers)
        start = time_ns()
        timed_route(routers[backend], first(cases))
        route_s = (time_ns() - start) / 1e9
        start = time_ns()
        timed_handler(handlers[backend], first(requests))
        push!(first_calls, Dict("backend" => names[backend], "first_route_s" => route_s,
                               "first_handler_after_route_s" => (time_ns() - start) / 1e9))
    end

    println("Warming and checking $(length(cases)) queries on all three implementations...")
    expected = [route(graph, c.origin, c.departure_ms, c.budget_ms) for c in cases]
    expected_bodies = [handlers[1](request).body for request in requests]
    for backend in eachindex(routers), i in eachindex(cases)
        c = cases[i]
        @assert route(routers[backend], c.origin, c.departure_ms, c.budget_ms) == expected[i]
        response = handlers[backend](requests[i])
        @assert response.status == 200 && response.body == expected_bodies[i]
    end
    reached = [count(!=(Reachability.INF), labels) for labels in expected]
    rows = NamedTuple[]
    rng = MersenneTwister(20260906)
    GC.gc()
    # Single-query calls, not batching. GPU routing includes its own final synchronization
    # and host result copy. No shared workspace is used by concurrent requests.
    for repetition in 1:repetitions, i in randperm(rng, length(cases)), phase in shuffle(rng, ["route", "handler_split"]), backend in randperm(rng, length(routers))
        c = cases[i]
        result = phase == "route" ? timed_route(routers[backend], c) : timed_handler(handlers[backend], requests[i])
        if phase == "route"
            @assert result.value == expected[i]
        else
            @assert result.value.status == 200 && result.value.body == expected_bodies[i]
        end
        push!(rows, (case_id=c.id, origin=c.name, kind=c.kind, index=c.index,
            departure=c.departure, horizon_h=c.horizon_h, reached=reached[i],
            phase=phase, backend=names[backend], repetition=repetition,
            ms=result.time * 1000, bytes=result.bytes, gc_ms=result.gctime * 1000))
    end
    ending_load = Sys.loadavg()
    summary = NamedTuple[]
    per_case = NamedTuple[]
    function summarize(rows)
        times = [r.ms for r in rows]
        return (samples=length(rows), median_ms=median(times), p95_ms=quantile(times, 0.95),
                mean_ms=mean(times), min_ms=minimum(times), inverse_mean_qps=1000 / mean(times),
                median_bytes=median([r.bytes for r in rows]), total_gc_ms=sum(r.gc_ms for r in rows))
    end
    for kind in unique(c.kind for c in cases), phase in ("route", "handler_split"), hours in (3, 12, 24, 168), name in names
        subset = filter(r -> r.kind == kind && r.phase == phase && r.horizon_h == hours && r.backend == name, rows)
        stats = summarize(subset)
        push!(summary, merge((kind=kind, phase=phase, horizon_h=hours, backend=name), stats))
        @printf("%-7s %-13s %3dh %-9s median %9.3f ms  p95 %9.3f ms  %9.1f q/s\n",
                kind, phase, hours, name, stats.median_ms, stats.p95_ms, stats.inverse_mean_qps)
    end
    for c in cases, phase in ("route", "handler_split"), name in names
        subset = filter(r -> r.case_id == c.id && r.phase == phase && r.backend == name, rows)
        push!(per_case, merge((case_id=c.id, origin=c.name, index=c.index, departure=c.departure,
            horizon_h=c.horizon_h, reached=reached[c.id], phase=phase, backend=name), summarize(subset)))
    end
    @assert bytes2hex(open(sha256, input)) == input_sha "input changed during benchmark"
    @assert all(bytes2hex(open(sha256, joinpath(@__DIR__, file))) == SOURCE_HASHES[file] for file in SOURCE_FILES) "source changed during benchmark"
    table = Arrow.Table(input)
    manifest = TOML.parsefile(joinpath(@__DIR__, "Manifest.toml"))
    metadata = Dict(
        "started_utc" => started, "finished_utc" => string(now(UTC)), "input" => abspath(input),
        "input_sha256" => input_sha, "input_bytes" => filesize(input), "julia" => string(VERSION),
        "cpu" => Sys.cpu_info()[1].model, "cpu_logical_threads" => Sys.CPU_THREADS,
        "julia_threads" => Threads.nthreads(), "interactive_threads" => Threads.nthreads(:interactive),
        "starting_loadavg" => starting_load, "ending_loadavg" => ending_load,
        "driver_override" => get(ENV, "ZE_ENABLE_ALT_DRIVERS", ""),
        "gpu_info" => gpu_info, "selected_device" => string(oneAPI.device()),
        "package_versions" => Dict(name => manifest["deps"][name][1]["version"]
            for name in ("oneAPI", "KernelAbstractions", "Atomix", "Arrow", "H3", "HTTP")),
        "input_rows" => length(table.from_h3),
        "skipped_rows" => count(d -> !(0 <= d <= Reachability.MAX_BUDGET_MS), table.duration_ms),
        "nodes" => length(graph.h3), "edge_groups" => length(graph.edge_to),
        "self_edge_groups" => count(graph.edge_from .== graph.edge_to),
        "profile_entries" => length(graph.departure), "host_graph_bytes" => Base.summarysize(graph),
        "pack_first_call_s" => packed.time, "graph_allocation_first_call_s" => allocation,
        "first_invocations" => first_calls, "samples_per_case" => repetitions, "cases" => length(cases),
        "seed" => 20260906, "measurements" => length(rows), "all_results_match" => true,
        "source_sha256" => SOURCE_HASHES)
    mkpath(output)
    write_csv(joinpath(output, "samples.csv"), rows)
    write_csv(joinpath(output, "summary.csv"), summary)
    write_csv(joinpath(output, "per_case.csv"), per_case)
    write_csv(joinpath(output, "workloads.csv"), [merge(c, (reached=reached[c.id],)) for c in cases])
    open(joinpath(output, "metadata.toml"), "w") do io
        TOML.print(io, metadata; sorted=true)
    end
    println("All labels and Arrow responses match. Results: ", abspath(output))
end

if abspath(PROGRAM_FILE) == @__FILE__
    main(ARGS)
end
