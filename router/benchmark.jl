using Profile, SHA
include("serve.jl")
import H3

function benchmark_path(origin, trip, case, offset=0)
    _, departure, window, step, budget, walk, mode, metric = case
    return "/reachable?index=$(string(origin; base=16))&departure_h=$(departure + offset)" *
        "&window_h=$window&step_h=$step&budget_h=$budget&max_walk_h=$walk" *
        "&encoding=split&distance_mode=straight_line&window_mode=$mode&metric=$metric" *
        "&network=everything&origin_radius=100&exclude_origin_population=true" *
        "&normalisation=pop&normalisation_param=200&trip_aware=$trip"
end

function benchmark_mixed(handler, cases, origin, output)
    handler.admission.max_workers_per_request = handler.admission.capacity[2]
    request(case, offset) = handler(HTTP.Request("GET", benchmark_path(origin, "true", case, offset),
        ["X-Router-Timing" => "true"]))
    # Train with new departures, not complete-response cache hits.
    for case in cases[3:4], i in 1:4
        request(case, i / 3600)
    end
    heavy = Threads.@spawn @timed request(cases[1], 1)
    deadline = time() + 120
    while !istaskdone(heavy) && scheduler_stats(handler.admission).workers[2] == 0
        time() < deadline || error("bulk request did not start")
        sleep(0.001)
    end
    tasks = [Threads.@spawn @timed request(cases[isodd(i) ? 3 : 4], (10 + i) / 3600) for i in 1:8]
    open(joinpath(output, "mixed.csv"), "w") do io
        println(io, "case,seconds,queue_ms,status,lane")
        for (i, task) in enumerate([tasks; heavy])
            name = i == 9 ? "population" : isodd(i) ? "short_window" : "walk"
            measured = fetch(task)
            response = measured.value
            response.status == 200 || error("mixed request failed: $(response.status)")
            println(io, join((name, measured.time, HTTP.header(response, "X-Router-Queue-Wait-Ms"),
                response.status, HTTP.header(response, "X-Router-Lane")), ','))
        end
    end
end

# Use the server arguments. Environment variables select the benchmark matrix.
function benchmark(args)
    options = parse_cli(args)
    output = get(ENV, "ROUTER_BENCH_OUTPUT", "/tmp/router-benchmark")
    mkpath(output)
    rounds = parse(Int, get(ENV, "ROUTER_BENCH_ROUNDS", "3"))
    rounds >= 1 || error("ROUTER_BENCH_ROUNDS must be positive")
    caps = parse.(Int, split(get(ENV, "ROUTER_BENCH_WORKERS", string(options.max_workers_per_request)), ','))
    animate = get(ENV, "ROUTER_BENCH_ANIMATE", "false") == "true"
    resolutions = parse.(Int, split(get(ENV, "ROUTER_BENCH_RESOLUTIONS", "5,6,7,8"), ','))
    selected = split(get(ENV, "ROUTER_BENCH_CASES", "population,long,short_window,walk"), ',')
    aware = split(get(ENV, "ROUTER_BENCH_TRIP_AWARE", "true,false"), ',')
    handler = load_handlers(options.paths; options.population_path, options.trip_shards_path,
        options.max_pending, options.workspace_bytes, options.short_workers,
        options.max_workers_per_request)
    warmup_server()
    centre = H3.API.cellToLatLng(parse(UInt64, get(ENV, "ROUTER_BENCH_ORIGIN", "851fb08bfffffff"); base=16))
    cases = [("population", 11, 5, 0.5, 2, 0, "min_union", "accessible_population"),
        ("long", 0, 24, 0.51, 160, 0, "min_union", "time_distance_quantile"),
        ("short_window", 0, 12, 0.51, 1, 0, "min_union", "time_distance_quantile"),
        ("walk", 0, 0, 0.51, 0.5, 0.5, "mean_intersection", "time_distance_quantile")]
    open(joinpath(output, "trials.csv"), "w") do io
        println(io, "case,resolution,trip_aware,round,seconds,allocated_bytes,gc_seconds,response_bytes,sha256,workers,queue_ms,timing,worker_limit,searches,reused_samples,lane")
        for (cap_index, cap) in enumerate(caps), resolution in resolutions, trip in aware, (name, departure, window, step, budget, walk, mode, metric) in cases
            name in selected || continue
            1 <= cap <= handler.admission.capacity[2] || error("worker limit exceeds bulk capacity")
            handler.admission.max_workers_per_request = cap
            origin = H3.API.latLngToCell(centre, resolution)
            for round in 0:rounds
                # A one-millisecond offset avoids complete-response and population-total hits.
                offset = (cap_index - 1) * (rounds + 1) / 3_600_000 + (animate ? round * step : round / 3_600_000)
                query = benchmark_path(origin, trip, (name, departure, window, step, budget, walk, mode, metric), offset)
                run() = handler(HTTP.Request("GET", query, ["X-Router-Timing" => "true"]))
                Profile.clear()
                measured = if round == rounds && get(ENV, "ROUTER_BENCH_PROFILE", "false") == "true"
                    @timed Profile.@profile run()
                else
                    @timed run()
                end
                response = measured.value
                response.status == 200 || error("$name res$resolution: HTTP $(response.status): $(String(response.body))")
                timing = replace(HTTP.header(response, "Server-Timing"), ',' => ';')
                println(io, join((name, resolution, trip, round, measured.time, measured.bytes,
                    measured.gctime, length(response.body), bytes2hex(sha256(response.body)),
                    HTTP.header(response, "X-Router-Workers"), HTTP.header(response, "X-Router-Queue-Wait-Ms"), timing, cap,
                    HTTP.header(response, "X-Router-Searches"), HTTP.header(response, "X-Router-Reused-Samples"),
                    HTTP.header(response, "X-Router-Lane")), ','))
                flush(io)
                println("$name res$resolution trip=$trip round=$round seconds=$(measured.time) $timing")
                flush(stdout)
                if round == rounds && get(ENV, "ROUTER_BENCH_PROFILE", "false") == "true"
                    open(joinpath(output, "$name-res$resolution-$trip-workers$cap.profile"), "w") do profile
                        Profile.print(profile; format=:flat, sortedby=:count, mincount=5)
                    end
                end
            end
        end
    end
    if get(ENV, "ROUTER_BENCH_MIXED", "false") == "true"
        benchmark_mixed(handler, cases, H3.API.latLngToCell(centre, first(resolutions)), output)
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    benchmark(ARGS)
end
