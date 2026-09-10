using Printf, Statistics
include("Reachability.jl")
using .Reachability
import KernelAbstractions as KA
import H3

const POPULATION_USAGE = """
Usage: julia --threads=8 --project=experiments/gpu experiments/gpu/benchmark-population-gpu.jl
       <network.arrow> <population.arrow> [--backend=cuda|oneapi|cpu]
       [origin_hex] [radius=6] [budget_h=3] [window_h=24] [step_h=0.25] [walk_h=1]
Use '-' for an empty population. The default backend is cpu.
For walking comparisons, use walk_h=2 at res6, or res7 with the default.
Set POPULATION_GPU_ORIGINS_PER_TILE to test a tile size from 1 to 32.
"""

function benchmark_population(args, backend)
    2 <= length(args) <= 8 || throw(ArgumentError(POPULATION_USAGE))
    hours(i, default, name) = Reachability._hours_ms(length(args) >= i ? args[i] : default,
        name, Reachability.MAX_TIME_MS / 3_600_000)
    budget, window, step = hours(5, "3", "budget_h"), hours(6, "24", "window_h"), hours(7, "0.25", "step_h")
    max_walk_ms = hours(8, "1", "max_walk_h")
    loaded = @timed pack_graph(args[1]; skip_invalid_durations=true, badajoz_shuttle=true)
    graph = loaded.value
    pop_loaded = @timed args[2] == "-" ? Reachability._population(UInt64[], Float64[]) : load_population(args[2])
    population = pop_loaded.value
    isempty(graph.h3) && length(args) < 3 && throw(ArgumentError("an empty graph requires origin_hex"))
    origin = length(args) >= 3 ? parse(UInt64, args[3]; base=16) : graph.h3[argmax(diff(graph.out_ptr))]
    radius = length(args) >= 4 ? Reachability._origin_radius(args[4]) : 6
    tile = parse(Int, get(ENV, "POPULATION_GPU_ORIGINS_PER_TILE", window > 0 && step > 0 ? "8" : "32"))
    prepared = @timed prepare_walking(WalkingIndex(graph); max_walk_ms)
    index = prepared.value
    uploaded = @timed PopulationKernelRouter(graph, population, backend; walking_index=index)
    router = uploaded.value
    options = (; origin_radius=radius, window_ms=window, step_ms=step, max_walk_ms, walking_index=index)
    cpu() = route_population(graph, population, origin, 28_800_000, budget; options...)
    gpu() = Reachability._route_population_gpu(router, origin, 28_800_000, budget;
        origin_radius=radius, window_ms=window, step_ms=step, max_walk_ms, origins_per_tile=tile)
    cpu_warm = @timed cpu()
    gpu_warm = @timed gpu()
    expected = cpu_warm.value
    function parity(actual)
        actual.h3 == expected.h3 && iszero.(actual.value) == iszero.(expected.value) &&
            all(isapprox.(actual.value, expected.value;
                rtol=Reachability.GPU_FLOAT32_RTOL, atol=Reachability.GPU_FLOAT32_ATOL)) ||
            error("GPU population differs from the production CPU result")
        errors = abs.(actual.value .- expected.value)
        return maximum(errors; init=0.0), maximum((iszero(v) ? 0.0 : e / abs(v)
            for (e, v) in zip(errors, expected.value)); init=0.0)
    end
    max_abs_error, max_rel_error = parity(gpu_warm.value)
    cpu_times, gpu_times = Float64[], Float64[]
    result = gpu_warm.value
    for repetition in 1:3
        for device in (isodd(repetition) ? (false, true) : (true, false))
            measured = @timed device ? gpu() : cpu()
            absolute, relative = parity(measured.value)
            max_abs_error, max_rel_error = max(max_abs_error, absolute), max(max_rel_error, relative)
            push!(device ? gpu_times : cpu_times, measured.time)
            device && (result = measured.value)
        end
    end
    @printf("backend=%s origin=%s resolution=%d origins=%d samples=%d walk_h=%g tile=%d threads=%d\n",
        result.backend, H3.API.h3ToString(origin), graph.resolution, length(result.h3), result.samples,
        max_walk_ms / 3_600_000, tile, Threads.nthreads())
    @printf("weight_and_accumulator_precision=%s max_abs_error=%.9g max_rel_error=%.9g rtol=%g atol=%g\n",
        result.weight_and_accumulator_precision, max_abs_error, max_rel_error,
        Reachability.GPU_FLOAT32_RTOL, Reachability.GPU_FLOAT32_ATOL)
    @printf("startup: graph=%.6fs population=%.6fs walking=%.6fs resident=%.6fs cpu_warm=%.6fs device_warm=%.6fs\n",
        loaded.time, pop_loaded.time, prepared.time, uploaded.time, cpu_warm.time, gpu_warm.time)
    @printf("median of 3: cpu=%.6fs device=%.6fs speedup=%.3fx\n", median(cpu_times), median(gpu_times), median(cpu_times) / median(gpu_times))
    @printf("last device query: planning=%.6fs upload=%.6fs device+checks=%.6fs download=%.6fs rounds=%d batches=%d downloaded=%dB outputs=%d\n",
        result.planning_s, result.upload_s, result.device_s, result.download_s, result.rounds,
        result.batches, result.bytes_downloaded, result.output_cells)
end

if abspath(PROGRAM_FILE) == @__FILE__
    if "--help" in ARGS || "-h" in ARGS
        println(POPULATION_USAGE)
    else
        flags = filter(a -> startswith(a, "--"), ARGS)
        length(flags) <= 1 && all(in(("--backend=cpu", "--backend=cuda", "--backend=oneapi")), flags) ||
            throw(ArgumentError(POPULATION_USAGE))
        backend = if "--backend=cuda" in flags
            import CUDA
            CUDA.functional() || error("CUDA was requested but is unavailable")
            CUDA.versioninfo()
            CUDA.allowscalar(false)
            CUDA.CUDABackend()
        elseif "--backend=oneapi" in flags
            import oneAPI
            oneAPI.functional() || error("oneAPI was requested but is unavailable")
            oneAPI.versioninfo()
            oneAPI.oneAPIBackend()
        else
            KA.CPU()
        end
        benchmark_population(filter(a -> !startswith(a, "--"), ARGS), backend)
    end
end
