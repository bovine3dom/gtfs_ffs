using Statistics
include("../../router/src/Reachability.jl")
using .Reachability
const R = Reachability

function measure(f, label)
    f()
    times, bytes = Float64[], Int[]
    result = nothing
    for _ in 1:3
        GC.gc()
        run = @timed f()
        result = run.value
        push!(times, run.time)
        push!(bytes, run.bytes)
    end
    println("MEASURE $label seconds=$times median=$(median(times)) bytes=$bytes peak_RSS=$(Sys.maxrss())")
    flush(stdout)
    return result
end

function main(args)
    isempty(args) && error("usage: julia --project=router --threads=8 experiments/benchmarks/benchmark-distance-modes.jl input.arrow [origin_hex]")
    origin = parse(UInt64, length(args) > 1 ? args[2] : "871fb4662ffffff"; base=16)
    println("ENV threads=$(Threads.nthreads()) input=$(abspath(args[1])) bytes=$(filesize(args[1])) origin=$(string(origin; base=16)) departure_h=0.0 budget_h=168.0 window_h=24.0 step_h=0.25 max_walk_h=1.0")
    flush(stdout)
    packed = @timed pack_graph(args[1]; skip_invalid_durations=true)
    graph = packed.value
    prepared = @timed prepare_walking(WalkingIndex(graph))
    index = prepared.value
    println("STARTUP pack_s=$(packed.time) prepare_s=$(prepared.time) graph_bytes=$(Base.summarysize(graph)) index_bytes=$(Base.summarysize(index)) vertices=$(length(graph.h3)) edges=$(length(graph.edge_to)) profiles=$(length(graph.departure))")
    flush(stdout)
    fields = (:h3, :elapsed_sum_ms, :elapsed_ms, :reachable_elapsed_ms, :reachable_samples, :sample_count,
              :searches, :full_searches, :repair_searches, :profile_lookups, :routing_expansions)
    for workers in (4, 8)
        route(mode) = route_window_walking_cached(graph, origin, 0, 604_800_000, 86_400_000;
            step_ms=900_000, walking_index=index, workers, distance_mode=mode)
        itinerary = measure(() -> route(:itinerary), "itinerary workers=$workers")
        straight = measure(() -> route(:straight_line), "straight_line workers=$workers")
        @assert all(isequal(getproperty(itinerary, f), getproperty(straight, f)) for f in fields)
        expected = merge(itinerary, (distance_km=R._od_distances(origin, itinerary.h3),))
        @assert isequal(expected.distance_km, straight.distance_km)
        for metric in ("time", "time_distance_quantile")
            body = measure(() -> R.window_arrow(graph, straight, origin, "split"; metric), "serialize workers=$workers metric=$metric")
            @assert body == R.window_arrow(graph, expected, origin, "split"; metric)
            println("PAYLOAD workers=$workers metric=$metric cells=$(length(straight.h3)) bytes=$(length(body)) parity=PASS")
        end
        for mode in (:itinerary, :straight_line)
            plan = R._walking_window_plan(graph, origin, 0, 604_800_000, 86_400_000;
                step_ms=900_000, walking_index=index, distance_mode=mode)
            width = cld(plan.samples, workers)
            output = R._walking_output_plan(plan, origin)
            state = R._walking_catchup_workspace(graph, plan, width, nothing, output)
            R._walking_catchup_chunk!(state, graph, origin, plan, 1, width)
            scratch = (state.arrival, state.eligible, state.connections, state.seenA, state.seenE,
                       state.kmA, state.kmE, state.queue, state.output.arrival, state.output.distance, state.output.touched)
            println("BUFFERS mode=$mode workers=$workers width=$width point_bytes_per_worker=$(Base.summarysize(state.points)) scratch_bytes_per_worker=$(Base.summarysize(scratch)) km_absent=$(isnothing(state.kmA))")
            flush(stdout)
        end
    end
    println("COMPLETE peak_RSS=$(Sys.maxrss())")
end

if abspath(PROGRAM_FILE) == @__FILE__
    main(ARGS)
end
