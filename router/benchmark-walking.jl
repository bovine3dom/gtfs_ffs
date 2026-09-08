using Printf, Statistics, Random, DataStructures
import H3
include("src/Reachability.jl")
using .Reachability

const DEPARTURE = 28_800_000
const BUDGET = 10_800_000
const WALK = 3_600_000
const STEP = 300_000
const NEVER = typemax(Int64)
hex(h) = string(h; base=16)
centre(h) = H3.API.cellToLatLng(h)::H3.API.LatLng

# Independent haversine, not the production geometry/filter/index implementation.
function walk(a, b)
    x = sin((a.lat - b.lat) / 2)^2 + cos(a.lat) * cos(b.lat) * sin((a.lng - b.lng) / 2)^2
    km = 2 * 6371.007180918475 * asin(sqrt(clamp(x, 0, 1)))
    return (ms=ceil(Int64, km * 720_000), km=km)
end

function measure(f, label)
    result = f()
    GC.gc()
    sleep(0.1)
    times, bytes = Float64[], Int[]
    for _ in 1:3
        run = @timed f()
        result = run.value
        push!(times, run.time)
        push!(bytes, run.bytes)
    end
    @printf("TIMING %s median_s=%.6f runs_s=%s allocated_MiB=%.3f\n",
            label, median(times), join(round.(times; digits=6), ','), median(bytes) / 2.0^20)
    flush(stdout)
    return result, median(times)
end

function counts(graph, result, label)
    network = count(h -> haskey(graph.node_id, h), result.h3)
    @printf("REACH %s total=%d graph=%d geographic_only=%d known_km=%d missing_km=%d\n",
            label, length(result.h3), network, length(result.h3) - network,
            count(isfinite, result.distance_km), count(isnan, result.distance_km))
    if hasproperty(result, :sample_count)
        @printf("WINDOW %s samples=%d searches=%d reused=%d reached_all=%d partial=%d\n",
                label, result.sample_count, result.searches, result.reused_samples,
                count(==(result.sample_count), result.reachable_samples),
                count(<(result.sample_count), result.reachable_samples))
    end
    flush(stdout)
end

function samples(graph, result, origin, label)
    p = centre(origin)
    for network in (true, false)
        slots = findall(h -> haskey(graph.node_id, h) == network, result.h3)
        sort!(slots; by=i -> walk(p, centre(result.h3[i])).km, rev=true)
        for i in Iterators.take(slots, 2)
            q = centre(result.h3[i])
            @printf("CELL %s h3=%s graph=%s lat=%.6f lon=%.6f elapsed_s=%.3f km=%.6f direct_km=%.3f\n",
                    label, hex(result.h3[i]), network, rad2deg(q.lat), rad2deg(q.lng),
                    (Int64(result.arrival[i]) - DEPARTURE) / 1000,
                    result.distance_km[i], walk(p, q).km)
        end
    end
end

"""Off-graph oracle: brute graph walks, separate schedule lookup, no geographic expansion."""
function brute_graph(graph, origin)
    n = length(graph.h3)
    centres = centre.(graph.h3)
    labels = fill(NEVER, n, 2) # 1: may board; 2: last edge transit, may also walk.
    queue = BinaryMinHeap{Tuple{Int64,Int,Int}}()
    @assert !haskey(graph.node_id, origin)
    p = centre(origin)
    for v in 1:n
        duration = walk(p, centres[v]).ms
        duration <= WALK || continue
        labels[v, 1] = DEPARTURE + duration
        push!(queue, (labels[v, 1], v, 1))
    end
    while !isempty(queue)
        time, u, state = pop!(queue)
        time == labels[u, state] || continue
        for edge in graph.out_ptr[u]:(graph.out_ptr[u + 1] - 1)
            profile = graph.schedule_ptr[edge]:(graph.schedule_ptr[edge + 1] - 1)
            slot = searchsortedfirst(view(graph.departure, profile), mod(time, 86_400_000))
            slot > length(profile) && continue
            arrival = fld(time, 86_400_000) * 86_400_000 + Int64(graph.arrival[first(profile) + slot - 1])
            v = Int(graph.edge_to[edge])
            if arrival <= DEPARTURE + BUDGET && arrival < labels[v, 2]
                labels[v, 2] = arrival
                push!(queue, (arrival, v, 2))
            end
        end
        state == 2 || continue
        for v in 1:n
            v == u && continue
            duration = walk(centres[u], centres[v]).ms
            duration <= WALK || continue
            arrival = time + duration
            if arrival <= DEPARTURE + BUDGET && arrival < labels[v, 1]
                labels[v, 1] = arrival
                push!(queue, (arrival, v, 1))
            end
        end
    end
    return labels, centres
end

function validate_offgraph(graph, index, origin)
    candidates = filter(h -> !iszero(h) && !haskey(graph.node_id, h), H3.API.gridDisk(origin, 12))
    sort!(candidates; by=h -> (walk(centre(origin), centre(h)).km, h))
    off = first(candidates)
    disabled = route_walking(graph, off, DEPARTURE, BUDGET; max_walk_ms=0, walking_index=index)
    @assert disabled == (h3=[off], arrival=UInt32[DEPARTURE], distance_km=[0.0])
    @assert all(==(Reachability.INF), route_details(graph, off, DEPARTURE, BUDGET).arrival)
    result, _ = measure(() -> route_walking(graph, off, DEPARTURE, BUDGET; walking_index=index), "offgraph_walk3600")
    counts(graph, result, "offgraph_walk3600")
    direct = filter(h -> !iszero(h) && walk(centre(off), centre(h)).ms <= WALK, H3.API.gridDisk(off, 12))
    outer = setdiff(H3.API.gridDisk(off, 12), H3.API.gridDisk(off, 11))
    @assert all(h -> iszero(h) || walk(centre(off), centre(h)).ms > WALK, outer)
    beyond = count(h -> walk(centre(off), centre(h)).ms > WALK, result.h3)
    @assert beyond > 0 && length(result.h3) > length(direct)
    access = [graph.h3[v] for v in eachindex(graph.h3) if walk(centre(off), index.centres[v]).ms <= WALK]
    @assert access == getproperty.(walking_neighbors(index, off), :cell)
    @printf("OFFGRAPH origin=%s access_vertices=%d pure_walk_cells=%d beyond_pure_walk=%d\n",
            hex(off), length(access), length(direct), beyond)
    p = centre(off)
    nearest = access[argmin([walk(p, centre(h)).km for h in access])]
    hop = walk(p, centre(nearest))
    @printf("ACCESS origin_lat=%.6f origin_lon=%.6f candidate=%s walk_s=%.3f km=%.6f disabled_origin_only=PASS\n",
            rad2deg(p.lat), rad2deg(p.lng), hex(nearest), hop.ms / 1000, hop.km)
    samples(graph, result, off, "offgraph_walk3600")
    flush(stdout)

    oracle = @timed brute_graph(graph, off)
    labels, centres = oracle.value
    actual = Dict(h => Int64(t) for (h, t) in zip(result.h3, result.arrival))
    @assert all(v -> get(actual, graph.h3[v], NEVER) == min(labels[v, 1], labels[v, 2]), eachindex(graph.h3))
    rng = MersenneTwister(7307)
    reached_geo = filter(h -> !haskey(graph.node_id, h) && h != off, result.h3)
    chosen = shuffle(rng, reached_geo)[1:min(32, length(reached_geo))]
    # Include independent disk candidates, not just destinations selected by production output.
    append!(chosen, shuffle(rng, candidates)[1:min(32, length(candidates))])
    for h in unique(chosen)
        q = centre(h)
        direct_ms = walk(centre(off), q).ms
        expected = direct_ms <= WALK ? DEPARTURE + direct_ms : NEVER
        for v in eachindex(graph.h3)
            labels[v, 2] == NEVER && continue
            duration = walk(centres[v], q).ms
            duration <= WALK || continue
            arrival = labels[v, 2] + duration
            arrival <= DEPARTURE + BUDGET && (expected = min(expected, arrival))
        end
        @assert get(actual, h, NEVER) == expected "offgraph egress mismatch at $(hex(h))"
    end
    @printf("ORACLE graph_labels=%d geographic_destinations=%d unreached_checks=%d brute_s=%.6f PASS\n",
            length(graph.h3), length(unique(chosen)), count(h -> !haskey(actual, h), unique(chosen)), oracle.time)
    flush(stdout)
end

function benchmark(path)
    println("LOAD path=$path bytes=$(filesize(path))")
    flush(stdout)
    loaded = @timed pack_graph(path; skip_invalid_durations=true)
    graph = loaded.value
    origin = H3.API.latLngToCell(H3.API.LatLng(deg2rad(48.8566), deg2rad(2.3522)), graph.resolution)::UInt64
    built = @timed WalkingIndex(graph)
    index = built.value
    @printf("GRAPH res=%d vertices=%d edges=%d profiles=%d km_available=%s origin=%s on_graph=%s load_s=%.6f graph_MiB=%.3f index_s=%.6f index_bytes=%d bins=%d\n",
            graph.resolution, length(graph.h3), length(graph.edge_to), length(graph.departure),
            !isnothing(graph.distance_km), hex(origin), haskey(graph.node_id, origin), loaded.time,
            Base.summarysize(graph) / 2.0^20, built.time, Base.summarysize(index), length(index.bins))
    origin_degree = length(walking_neighbors(index, origin))
    degrees = Int[]
    counted = @elapsed for h in graph.h3
        push!(degrees, length(walking_neighbors(index, h)))
    end
    @printf("WALKS scanned=%d directed=%d mean_degree=%.6f median_degree=%.1f max_degree=%d isolated=%d origin_degree=%d count_s=%.6f\n",
            length(degrees), sum(degrees), mean(degrees), median(degrees), maximum(degrees), count(iszero, degrees), origin_degree, counted)
    baseline, _ = measure(() -> route_details(graph, origin, DEPARTURE, BUDGET), "point_transit")
    zero, _ = measure(() -> route_walking(graph, origin, DEPARTURE, BUDGET; max_walk_ms=0, walking_index=index), "point_walk0")
    reached = findall(!=(Reachability.INF), baseline.arrival)
    expected = Dict(graph.h3[v] => (baseline.arrival[v], baseline.distance_km[v]) for v in reached)
    expected[origin] = (UInt32(DEPARTURE), 0.0)
    @assert isequal(Dict(h => (t, km) for (h, t, km) in zip(zero.h3, zero.arrival, zero.distance_km)), expected)
    println("PARITY exact_h3_arrival_km_including_origin=PASS")
    counts(graph, zero, "point_walk0")
    point, _ = measure(() -> route_walking(graph, origin, DEPARTURE, BUDGET; walking_index=index), "point_walk3600")
    counts(graph, point, "point_walk3600")
    arrivals = Dict(zip(point.h3, point.arrival))
    @assert all(v -> get(arrivals, graph.h3[v], Reachability.INF) <= baseline.arrival[v], reached)
    samples(graph, point, origin, "point_walk3600")
    for minutes in (60, 180)
        window = minutes * 60_000
        transit, _ = measure(() -> route_window_cached(graph, origin, DEPARTURE, BUDGET, window; step_ms=STEP, workers=1), "window$(minutes)_catchup")
        disabled, _ = measure(() -> route_window_walking(graph, origin, DEPARTURE, BUDGET, window; step_ms=STEP, max_walk_ms=0, walking_index=index), "window$(minutes)_walk0")
        reached_window = findall(>(0), transit.reachable_samples)
        @assert disabled.h3 == sort!(unique([graph.h3[reached_window]; origin]))
        for (i, h) in enumerate(disabled.h3)
            h == origin && continue
            v = graph.node_id[h]
            @assert disabled.elapsed_sum_ms[i] == transit.elapsed_sum_ms[v]
            @assert disabled.reachable_samples[i] == transit.reachable_samples[v]
            @assert isapprox(disabled.distance_km[i], transit.distance_km[v]; nans=true)
        end
        println("PARITY window$(minutes)_catchup_vs_walk0=PASS catchup_searches=$(transit.searches) reused=$(transit.reused_samples)")
        counts(graph, disabled, "window$(minutes)_walk0")
        enabled, seconds = measure(() -> route_window_walking(graph, origin, DEPARTURE, BUDGET, window; step_ms=STEP, walking_index=index), "window$(minutes)_walk3600")
        counts(graph, enabled, "window$(minutes)_walk3600")
        seconds <= 1 || break # Only extend to three hours if the one-hour window is cheap.
    end
    graph.resolution == 7 && validate_offgraph(graph, index, origin)
    @printf("COMPLETE res=%d process_peak_RSS_MiB=%.3f\n", graph.resolution, Sys.maxrss() / 2.0^20)
    flush(stdout)
end

function main(args)
    paths = isempty(args) ? [joinpath(@__DIR__, "..", "data", name) for name in
        ("rail_and_friends_dist_res5.arrow", "rail_and_friends_res6.arrow", "rail_and_friends_res7.arrow")] : args
    println("ENV julia=$VERSION threads=$(Threads.nthreads()) cpu=$(Sys.CPU_NAME) repetitions=3 departure_h=8.0 budget_h=3.0 max_walk_h=1.0 step_h=$(1/12)")
    for path in paths
        benchmark(path)
        GC.gc() # Do not retain multiple packed graphs or indices.
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    main(ARGS)
end
