using Printf, Statistics
include("src/Reachability.jl")
using .Reachability
import H3
if "--gpu" in ARGS
    import oneAPI
end

timed_call(f) = @timed f()

function main(args)
    gpu = "--gpu" in args
    args = filter(!=("--gpu"), args)
    1 <= length(args) <= 4 || error("usage: julia --project=router router/benchmark-window-engines.jl <input.arrow> [origin_hex] [window_s=86400] [repetitions=3] [--gpu]")
    graph = @time pack_graph(args[1]; skip_invalid_durations=true)
    origin = length(args) >= 2 ? parse(UInt64, args[2]; base=16) : graph.h3[argmax(diff(graph.out_ptr))]
    window = length(args) >= 3 ? parse(Int, args[3]) * 1000 : 86_400_000
    repetitions = length(args) >= 4 ? parse(Int, args[4]) : 3
    repetitions > 0 || error("repetitions must be positive")
    chunks = parse.(Int, split(get(ENV, "ROUTER_BENCH_CHUNKS", "32,64,128"), ','))
    workers = parse.(Int, split(get(ENV, "ROUTER_BENCH_WORKERS", "1"), ','))
    batches = parse.(Int, split(get(ENV, "ROUTER_BENCH_BATCHES", "32,64,128"), ','))
    check_every = parse(Int, get(ENV, "ROUTER_BENCH_CHECK_EVERY", "4"))
    gpu_routers = []
    if gpu
        oneAPI.functional() || error("oneAPI requested but unavailable")
        oneAPI.versioninfo()
        parent = KernelRouter(graph, oneAPI.oneAPIBackend())
        gpu_routers = [WindowKernelRouter(parent; batch_size=b, check_every) for b in batches]
    end
    @info "Window engine comparison" origin=H3.API.h3ToString(origin) window_s=window÷1000 resolution=graph.resolution nodes=length(graph.h3) distance_available=!isnothing(graph.distance_km)
    for budget in (10_800_000, 604_800_000)
        names = ["origin"; ["catchup_$(c)_workers$w" for c in chunks for w in workers]; ["gpu_$(r.batch_size)_check$(r.check_every)" for r in gpu_routers]]
        functions = Any[() -> route_window(graph, origin, 0, budget, window)]
        append!(functions, [() -> route_window_cached(graph, origin, 0, budget, window; chunk_size=c, workers=w) for c in chunks for w in workers])
        append!(functions, [() -> route_window_kernel!(r, origin, 0, budget, window) for r in gpu_routers])
        expected = first(functions)()
        stats = []
        for (name, f) in zip(names, functions)
            @info "Warm and validate" name budget_h=budget÷3_600_000
            result = f()
            @assert result.elapsed_sum_ms == expected.elapsed_sum_ms
            @assert result.reachable_samples == expected.reachable_samples
            @assert isequal(result.distance_km, expected.distance_km)
            push!(stats, result)
        end
        times = [Float64[] for _ in functions]
        for repetition in 1:repetitions
            order = isodd(repetition) ? eachindex(functions) : reverse(eachindex(functions))
            for i in order
                measurement = timed_call(functions[i])
                @assert measurement.value.elapsed_sum_ms == expected.elapsed_sum_ms
                @assert isequal(measurement.value.distance_km, expected.distance_km)
                push!(times[i], measurement.time)
                stats[i] = measurement.value
            end
        end
        for i in eachindex(names)
            result = stats[i]
            full = hasproperty(result, :full_searches) ? result.full_searches : result.searches
            lookups = hasproperty(result, :profile_lookups) ? result.profile_lookups : -1
            rounds = hasproperty(result, :rounds) ? result.rounds : 0
            used_workers = hasproperty(result, :workers) ? result.workers : 1
            @printf("%s budget=%dh groups=%d full=%d lookups=%d rounds=%d cpu_workers=%d median=%.6fs speedup=%.2fx\n",
                names[i], budget ÷ 3_600_000, result.searches, full, lookups, rounds, used_workers,
                median(times[i]), median(first(times)) / median(times[i]))
            if hasproperty(result, :device_s)
                @printf("  last-sample stages: planning=%.4fs device+checks=%.4fs downloads=%.4fs host-replay=%.4fs aggregation=%.4fs\n",
                    result.planning_s, result.device_s, result.download_s, result.host_replay_s, result.aggregation_s)
            end
        end
        flush(stdout)
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    main(ARGS)
end
