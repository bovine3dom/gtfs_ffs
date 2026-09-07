using Profile

const BASE_REV = "66a7a2d2a0532fb9137f80a725743676520d0ada"
const ROOT = dirname(@__DIR__)
# Reuse the diagnostic instrumentation, but always feed it the immutable baseline.
module Historical end
module CurrentDiagnostic end
source = read(`git -C $ROOT show $BASE_REV:router/benchmark-walking-reuse.jl`, String)
files = split(read(`git -C $ROOT ls-tree --name-only $BASE_REV:router/src`, String))
source = replace(source,
    "const CURRENT_SOURCES = Dict(name => read(joinpath(@__DIR__, \"src\", name), String)\n    for name in readdir(joinpath(@__DIR__, \"src\")) if endswith(name, \".jl\"))" =>
    "const CURRENT_SOURCES = Dict(name => read(`git -C $ROOT show $BASE_REV:router/src/\$name`, String) for name in $(repr(files)) if endswith(name, \".jl\"))")
ENV["WALKING_BASELINE_REV"] = BASE_REV
Base.include_string(Historical, replace(source, "Base.include_string(Main," => "Base.include_string(@__MODULE__,", "const R = Main.Reachability" =>
    "const R = parentmodule(@__MODULE__).Reachability"), joinpath(@__DIR__, "benchmark-walking-reuse.jl"))
const Old = Historical.Reachability

function main(args)
    isempty(args) && error("usage: julia --project=router --threads=8 router/benchmark-walking-output.jl input.arrow [--wait]")
    println("ENV baseline=$BASE_REV threads=$(Threads.nthreads()) input=$(abspath(args[1])) bytes=$(filesize(args[1]))")
    println("QUERY representative Paris=871fb4662ffffff midnight; actual browser state unknown")
    flush(stdout)
    packed = @timed Old.pack_graph(args[1]; skip_invalid_durations=true)
    graph = packed.value
    prepared = @timed Old.prepare_walking(Old.WalkingIndex(graph))
    index = prepared.value
    GC.gc()
    println("STARTUP pack_s=$(packed.time) prepare_s=$(prepared.time) prepare_bytes=$(prepared.bytes) vertices=$(length(graph.h3)) edges=$(length(graph.edge_to)) profiles=$(length(graph.departure)) index_bytes=$(Base.summarysize(index)) peak_RSS=$(Sys.maxrss())")
    origin = parse(UInt64, "871fb4662ffffff"; base=16)
    kwargs = (step_ms=900_000, walking_index=index, workers=4)
    f = () -> Old.route_window_walking_cached(graph, origin, 0, 604_800_000, 86_400_000; kwargs...)
    expected, _ = Historical.measure(f, "baseline7d96")
    probe = () -> Historical.Probe.route_window_walking_cached(graph, origin, 0, 604_800_000, 86_400_000; kwargs...)
    probe()
    Historical.probe(probe, "baseline7d96", expected)
    Profile.init(n=10^7, delay=0.001)
    Profile.@profile f()
    open("/tmp/opencode/walking-output-baseline-profile.txt", "w") do io
        Profile.print(io; format=:flat, sortedby=:count, mincount=10, C=true)
    end
    for metric in ("time", "distance_time_quantile")
        body, _ = Historical.measure(() -> Old.window_arrow(graph, expected, origin, "split"; metric), "baseline serialization $metric")
        println("PAYLOAD metric=$metric bytes=$(length(body))")
    end
    println("BASELINE_PROFILE_COMPLETE"); flush(stdout)
    if "--wait" in args
        while !isfile("/tmp/opencode/walking-output-continue")
            sleep(2)
        end
    end
    Base.include(Main, joinpath(@__DIR__, "src", "Reachability.jl"))
    Base.invokelatest(compare, graph, index, origin)
end

function compare(old_graph, old_index, origin)
    R = Main.Reachability
    graph = R.Graph((getfield(old_graph, field) for field in fieldnames(R.Graph))...)
    @assert all(getfield(graph, f) === getfield(old_graph, f) for f in fieldnames(R.Graph))
    prepared = @timed R.prepare_walking(R.WalkingIndex(graph))
    index = prepared.value
    println("NEW_PREPARATION seconds=$(prepared.time) bytes=$(prepared.bytes) retained_bytes=$(Base.summarysize(index)) shared_graph_fields=PASS")
    for (budget, window, step) in ((604_800_000, 86_400_000, 900_000), (10_800_000, 3_600_000, 300_000))
        label = "budget$(budget)_samples$(cld(window, step))"
        baseline, _ = Historical.measure(() -> Old.route_window_walking_cached(old_graph, origin, 0, budget, window;
            step_ms=step, walking_index=old_index, workers=4), "$label old4")
        for workers in (4, 8)
            result, _ = Historical.measure(() -> R.route_window_walking_cached(graph, origin, 0, budget, window;
                step_ms=step, walking_index=index, workers), "$label new$workers"; expected=baseline)
            for metric in ("time", "distance_time_quantile")
                body, _ = Historical.measure(() -> R.window_arrow(graph, result, origin, "split"; metric), "$label new$workers serialization $metric")
                @assert body == Old.window_arrow(old_graph, baseline, origin, "split"; metric)
            end
        end
    end
    diagnose(graph, index, origin)
    println("COMPLETE peak_RSS=$(Sys.maxrss())")
end

function diagnose(graph, index, origin)
    source = read(joinpath(@__DIR__, "benchmark-walking-reuse.jl"), String)
    Base.include_string(CurrentDiagnostic, replace(source,
        "Base.include_string(Main," => "Base.include_string(@__MODULE__,",
        "const R = Main.Reachability" => "const R = parentmodule(@__MODULE__).Reachability"),
        joinpath(@__DIR__, "benchmark-walking-reuse.jl"))
    Base.invokelatest(diagnostic_cases, graph, index, origin)
end

function diagnostic_cases(graph, index, origin)
    D = CurrentDiagnostic.R
    graph = D.Graph((getfield(graph, f) for f in fieldnames(D.Graph))...)
    packed(p) = D.PackedWalking(p.offsets, p.targets, p.durations, p.distances)
    p = index.prepared
    index = D.WalkingIndex(index.cells, index.centres, index.bins, index.resolution,
        D.WalkingAdjacency(p.limit, p.node_id, packed(p.geographic), packed(p.graph),
                           p.output_cells, p.output_id, packed(p.output)))
    for (budget, window, step) in ((604_800_000, 86_400_000, 900_000), (10_800_000, 3_600_000, 300_000))
        label = "indexed_budget$(budget)_samples$(cld(window, step))"
        f = () -> CurrentDiagnostic.Probe.route_window_walking_cached(graph, origin, 0, budget, window;
            step_ms=step, walking_index=index, workers=4)
        expected = D.route_window_walking_cached(graph, origin, 0, budget, window;
            step_ms=step, walking_index=index, workers=4)
        f() # Compile diagnostic paths before timing; retain scratch for reset comparison.
        workspaces = CurrentDiagnostic.Probe.WORKSPACES[]
        out = first(workspaces).output
        dense, touched = reset_timings(out)
        println("RESET $label universe=$(length(out.arrival)) touched=$(length(out.touched)) repetitions=100 dense_s=$dense touched_s=$touched point_buffers_bytes=$(Base.summarysize([w.points for w in workspaces]))")
        CurrentDiagnostic.probe(f, label, expected)
    end
end

function reset_timings(out)
    dense = @elapsed for _ in 1:100
        reset_dense!(out.arrival)
    end
    touched = @elapsed for _ in 1:100
        reset_touched!(out)
    end
    return dense, touched
end

# Prevent LLVM from collapsing repeated identical fills into one write pass.
@noinline reset_dense!(arrival) = fill!(arrival, typemax(UInt32))
@noinline function reset_touched!(out)
    @inbounds for v in out.touched
        out.arrival[v] = typemax(UInt32)
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    main(ARGS)
end
