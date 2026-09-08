using Statistics, Printf, SHA
include("src/Reachability.jl")
using .Reachability

const WINDOW_FIELDS = (:h3, :elapsed_sum_ms, :elapsed_ms, :reachable_elapsed_ms,
                       :reachable_samples, :distance_km, :sample_count)
function measure(f, label; expected=nothing)
    result = f() # warm compilation and execution, never timed
    fields = hasproperty(result, :sample_count) ? WINDOW_FIELDS : (:h3, :arrival, :distance_km)
    check(x) = isnothing(expected) || all(isequal(getproperty(x, k), getproperty(expected, k)) for k in fields)
    @assert check(result) label
    GC.gc()
    runs = [@timed(f()) for _ in 1:3]
    @assert all(r -> check(r.value), runs) label
    @printf("%s median_s=%.9f allocated_MiB=%.3f runs_s=%s parity=%s\n", label,
        median(r.time for r in runs), median(r.bytes for r in runs) / 2.0^20,
        join([r.time for r in runs], ','), isnothing(expected) ? "anchor" : "PASS")
    flush(stdout)
    return result
end

function main()
    length(ARGS) == 1 || error("usage: julia --project=router --threads=4 router/benchmark-walking-adjacency.jl data/rail_and_friends_res7.arrow")
    path = only(ARGS)
    println("Julia=$(VERSION) threads=$(Threads.nthreads(:default)) CPU=$(Sys.CPU_NAME) input=$path sha256=$(bytes2hex(open(sha256, path)))")
    packed = @timed pack_graph(path; skip_invalid_durations=true)
    graph = packed.value
    @assert graph.resolution == 7
    println("pack_s=$(packed.time) nodes=$(length(graph.h3)) edges=$(length(graph.edge_to)) profiles=$(length(graph.departure))")
    bare = WalkingIndex(graph)
    # Compile preparation on an empty index before measuring the real build once.
    prepare_walking(WalkingIndex(pack_graph((from_h3=UInt64[], to_h3=UInt64[],
        departure_ms=UInt32[], duration_ms=Int64[]))))
    build = @timed prepare_walking(bare; workers=4)
    index = build.value
    p = index.prepared
    println("prepare_s=$(build.time) allocated_MiB=$(build.bytes / 2.0^20) retained_index_MiB=$(Base.summarysize(index) / 2.0^20) adjacency_MiB=$(Base.summarysize(p) / 2.0^20) bare_index_MiB=$(Base.summarysize(bare) / 2.0^20) geographic_edges=$(length(p.geographic.targets)) graph_edges=$(length(p.graph.targets))")
    flush(stdout)
    origin, ready, budget = UInt64(0x871fb4662ffffff), 28_800_000, 10_800_000
    @assert haskey(graph.node_id, origin)
    println("origin=$(string(origin; base=16)) ready_ms=$ready budget_ms=$budget workers=4 max_walk_h=1")
    expected = measure(() -> route_walking(graph, origin, ready, budget; walking_index=bare), "point unprepared")
    measure(() -> route_walking(graph, origin, ready, budget; walking_index=index), "point prepared"; expected)
    measure(() -> route_details(graph, origin, ready, budget), "point no-walk")
    for samples in (12, 1440)
        window, step = samples == 12 ? (3_600_000, 300_000) : (86_400_000, 60_000)
        expected = measure(() -> route_window_walking_cached(graph, origin, ready, budget, window;
            step_ms=step, workers=4, walking_index=bare), "window$samples unprepared")
        measure(() -> route_window_walking_cached(graph, origin, ready, budget, window;
            step_ms=step, workers=4, walking_index=index), "window$samples prepared"; expected)
        measure(() -> route_window_cached(graph, origin, ready, budget, window;
            step_ms=step, workers=4), "window$samples no-walk")
    end
    println("peak_RSS_MiB=$(Sys.maxrss() / 2.0^20)")
end
main()
