include("../../router/serve.jl")
using Arrow, H3, Statistics
const R = Reachability

function main(args)
    length(args) == 2 || error("Use: concurrency-benchmark.jl GRAPH.arrow POPULATION.arrow")
    graph = pack_graph(args[1]; skip_invalid_durations=true, badajoz_shuttle=true)
    population = load_population(args[2])
    admission = RequestScheduler()
    pool = PopulationWorkspacePool()
    handler = make_handler(graph; population, admission, workspace_pool=pool)
    index = getfield(handler.handler, :walking_index)
    paris = H3.API.latLngToCell(H3.API.LatLng(deg2rad(48.8566), deg2rad(2.3522)), graph.resolution)::UInt64
    rural = UInt64(0x881fa44181fffff)
    base(origin, departure, budget, walk) = "/reachable?index=$(string(origin; base=16))&departure_h=$departure&budget_h=$budget&max_walk_h=$walk"
    fast = base(rural, 5, 0.5, 0.1)
    point = base(paris, 8, 3, 1)
    cached = base(rural, 5, 0.5, 0.1) * "&metric=accessible_population&origin_radius=10&window_h=13&step_h=0.06"
    bulk(departure) = base(paris, departure, 1, 1) * "&metric=accessible_population&origin_radius=18&window_h=1.6&step_h=0.016666667"
    for path in (fast, point, cached, bulk(8))
        @assert handler(HTTP.Request("GET", path)).status == 200
    end
    println("READY nodes=$(length(graph.h3)) profiles=$(length(graph.departure)) threads=$(Threads.nthreads(:default)) resources=$(scheduler_stats(admission))")
    flush(stdout)
    server = HTTP.serve!(make_stream_handler(handler), "127.0.0.1", 0;
        stream=true, listenany=true, verbose=-1)
    url = "http://127.0.0.1:$(HTTP.port(server))"
    try
        # Warm the client and the transport before measurements.
        for path in (fast, point, cached)
            HTTP.get(url * path; retry=false)
        end
        open(joinpath(@__DIR__, "concurrency-trials.csv"), "w") do io
            println(io, "scenario,kind,trial,wall_ms,queue_ms,workers,bytes,status")
            function measure(scenario, kind, trial, path)
                started = time_ns()
                response = HTTP.get(url * path; retry=false, status_exception=false)
                ms = (time_ns() - started) / 1e6
                row = (scenario, kind, trial, ms, HTTP.header(response, "X-Router-Queue-Wait-Ms"),
                    HTTP.header(response, "X-Router-Workers"), length(response.body), response.status)
                println(io, join(row, ',')); flush(io)
                @assert response.status == 200
                ms
            end
            for (kind, path) in (("short_point", fast), ("paris_point_3h", point), ("cache_hit", cached))
                measure("warmup", kind, 0, path)
            end
            for scenario in ("idle", "mixed")
                tasks = scenario == "mixed" ? [@async measure(scenario, "bulk", i, bulk(8 + i / 3600)) for i in 1:2] : Task[]
                if !isempty(tasks)
                    @assert timedwait(() -> scheduler_stats(admission).workers[2] > 0, 30) == :ok
                end
                for (kind, path) in (("short_point", fast), ("paris_point_3h", point), ("cache_hit", cached))
                    times = [measure(scenario, kind, i, path) for i in 1:30]
                    println("$scenario $kind p50_ms=$(median(times)) p95_ms=$(quantile(times, 0.95)) max_ms=$(maximum(times))")
                    flush(stdout)
                end
                foreach(fetch, tasks)
            end
        end
    finally
        close(server)
    end
    @assert scheduler_stats(admission).workers == (0, 0)
    # Compare the fixed three-worker policy with six-worker sequential service.
    full = max(1, admission.capacity[2])
    half = cld(full, 2)
    query(workers, offset) = route_population(graph, population, paris, 28_800_000 + offset, 3_600_000;
        walking_index=index, max_walk_ms=3_600_000, origin_radius=18, window_ms=5_760_000,
        step_ms=60_000, workers, workspace_pool=pool)
    query(full, 0)
    for trial in 1:2
        GC.gc()
        sequential = @timed [query(full, i * 1000) for i in 1:2]
        GC.gc()
        concurrent = @timed fetch.([Threads.@spawn query(half, i * 1000) for i in 1:2])
        for (a, b) in zip(sequential.value, concurrent.value)
            @assert a.h3 == b.h3 && iszero.(a.value) == iszero.(b.value)
            @assert all(isapprox.(a.value, b.value; rtol=1e-12, atol=1e-6))
            @assert a.shared_expansions == b.shared_expansions
        end
        println("THROUGHPUT trial=$trial sequential_$(full)_s=$(sequential.time) concurrent_$(half)_s=$(concurrent.time) sequential_allocated=$(sequential.bytes) concurrent_allocated=$(concurrent.bytes)")
        flush(stdout)
    end
    println("FINAL ", population_workspace_stats(pool), " ", scheduler_stats(admission))
end
main(ARGS)
