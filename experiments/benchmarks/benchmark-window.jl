using Printf, Statistics
include("../../router/src/Reachability.jl")
Base.include(Reachability, joinpath(@__DIR__, "../../router/test/reference.jl"))
using .Reachability
import H3

function main(args)
    1 <= length(args) <= 2 || error("usage: julia --project=router experiments/benchmarks/benchmark-window.jl <input.arrow> [origin_hex]")
    graph = @time pack_graph(first(args); skip_invalid_durations=true)
    degree = diff(graph.out_ptr)
    origins = length(args) == 2 ? [parse(UInt64, args[2]; base=16)] :
        unique(graph.h3[[argmax(degree), something(findfirst(==(1), degree), argmax(degree))]])
    @info "Window benchmark" resolution=graph.resolution nodes=length(graph.h3) distance_available=!isnothing(graph.distance_km)
    for origin in origins, budget in (10_800_000, 604_800_000)
        fast = route_window(graph, origin, 0, budget, 86_400_000)
        naive = route_window(graph, origin, 0, budget, 86_400_000; reuse=false)
        @assert fast.elapsed_sum_ms == naive.elapsed_sum_ms
        @assert fast.reachable_samples == naive.reachable_samples
        @assert isapprox(fast.distance_km, naive.distance_km; nans=true)
        reused_times, naive_times = Float64[], Float64[]
        for repetition in 1:3
            for reuse in (isodd(repetition) ? (true, false) : (false, true))
                measurement = @timed route_window(graph, origin, 0, budget, 86_400_000; reuse)
                @assert measurement.value.elapsed_sum_ms == naive.elapsed_sum_ms
                push!(reuse ? reused_times : naive_times, measurement.time)
            end
        end
        @printf("%s budget=%dh samples=%d searches=%d reused=%d reuse=%.4fs naive=%.4fs speedup=%.2fx reachable=%d\n",
            H3.API.h3ToString(origin), budget ÷ 3_600_000, fast.sample_count, fast.searches,
            fast.reused_samples, median(reused_times), median(naive_times),
            median(naive_times) / median(reused_times), count(>(0), fast.reachable_samples))
        flush(stdout)
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    main(ARGS)
end
