# GPU Routing Experiments

This optional environment contains KernelAbstractions/Atomix arrival, window,
and population kernels. The production server uses CPU routing and its own
dependencies. The packed shared CPU engine is the population reference.

## Run

From the repository root:

```sh
julia --project=experiments/gpu -e 'using Pkg; Pkg.instantiate()'
julia --threads=4 --project=experiments/gpu experiments/gpu/test/runtests.jl
```

The tests use the KernelAbstractions CPU backend (`KA.CPU`) by default.
For Intel hardware, add `--backend=oneapi`. The environment uses oneAPI 2.7.2
and checks device availability. The P630 population command below includes
the required process-local driver setting.

## Interface

```julia
include("experiments/gpu/Reachability.jl")
using .Reachability
import KernelAbstractions as KA

graph = pack_graph("data/rail_res5.arrow")
router = KernelRouter(graph, KA.CPU())
labels = route_kernel!(router, graph.h3[1], 0, 3_600_000)
windows = WindowKernelRouter(router; batch_size=32, check_every=4)
result = route_window_kernel!(windows, graph.h3[1], 0, 3_600_000, 86_400_000)
```

The loader extends the shared CPU module with experimental kernels. Use it instead
of including the production module separately. Times are integer milliseconds.
Use each mutable workspace for one request at a time. These two interfaces
support transit only. For population with walking, use the interface below.

## Benchmarks

```sh
julia --threads=4 --project=experiments/gpu experiments/gpu/benchmark.jl data/rail_res5.arrow data/gpu-benchmark
julia --threads=8 --project=experiments/gpu experiments/gpu/benchmark-window-engines.jl data/rail_res5.arrow --gpu
```

The point comparison requires Intel hardware and four-column input. Its local
handler measures query parsing, experimental transit routing, and Arrow encoding.
The window comparison uses CPU routing by default. Add `--gpu` for device routing;
`--backend=cuda` requires CUDA installed separately on an NVIDIA machine.
Chunk, batch, and worker options apply only to benchmarks.

See the [point report](../benchmarks/benchmark-results.md) and
[window report](../benchmarks/window-optimization-results.md) for measurements.

## Population

The population experiment uses 32 query lanes. Each lane represents one origin
and one departure sample. Arrival labels, walking expansion, coverage masks, and
population reductions stay on the device. This population-only interface
calculates coverage directly.

```julia
population = load_population("data/kontur_h3.arrow")
index = prepare_walking(WalkingIndex(graph))
router = PopulationKernelRouter(graph, population, KA.CPU(); walking_index=index)
result = Reachability._route_population_gpu(router, graph.h3[1], 28_800_000, 10_800_000;
    origin_radius=6, window_ms=86_400_000, step_ms=900_000)
```

The result contains sorted `h3` origins, `Float64` `value` totals, and diagnostics.
Device weights and reductions use `Float32`; downloaded totals are converted
to `Float64` on the host. `weight_and_accumulator_precision` is `"Float32"`.
Zero values remain in the result. Use `nothing` for an empty population.
The query accepts the production time, radius, walking, and mode parameters.
`origins_per_tile` accepts integers from 1 to 32. Its default is 32 for a point
query and 8 for a window query.

Transit updates two arrival states, including on self-edges. Walking updates
only the transit-ready state. Rounds continue until the labels are stable. Coverage
uses integer atomic OR operations. Intersection modes retain cells reached in
every sample. `min_union` and `diff_union` retain cells reached in any sample.
`reachable_union` multiplies each weight by its reached-sample fraction.
Two `Float32` reduction stages compute approximate totals. Arrival times and
coverage masks use exact `UInt32` operations. The production CPU calculation uses `Float64`.

Only origin totals and one `UInt32` flag per round return to the host.
`bytes_downloaded` is `4 * length(h3) + 4 * rounds`, also on `KA.CPU`.
`planning_s` measures host geometry and ID preparation. `upload_s` measures
request allocation and uploads. `device_s` includes convergence checks and
synchronization. `download_s` measures the totals transfer. Constructor work
is separate.

### Compare

```sh
julia --threads=8 --project=experiments/gpu experiments/gpu/benchmark-population-gpu.jl data/rail_and_friends_res6.arrow data/kontur_h3.arrow --backend=cpu
julia --threads=8 --project=experiments/gpu experiments/gpu/benchmark-population-gpu.jl data/everything_res6.arrow data/kontur_h3.arrow --backend=cuda 861fb4667ffffff 6 3 24 0.25 2
julia --threads=8 --project=experiments/gpu experiments/gpu/benchmark-population-gpu.jl data/everything_res7.arrow data/kontur_h3.arrow --backend=cuda
```

`walk_h` follows `step_h` and defaults to one hour. Use two hours at resolution 6
to exercise walking between cell centres around Paris. The GTX 1080 Ti command
above uses this limit; the resolution-7 alternative uses the one-hour default.
Preparation and both engines use the printed `walk_h` limit.
`pack_graph(...; badajoz_shuttle=true)` includes the server's shuttle in both paths.
`POPULATION_GPU_ORIGINS_PER_TILE` applies only to the benchmark.

Test radii 6, 10, and 18 for 127, 331, and 1027 origins on an ordinary H3 disk. A 24-hour
window with a 0.25-hour step has 96 samples. The script reports startup work,
checks production `route_population` with `rtol=2e-6` and `atol=1e-3`, and compares
three warm queries. It requires exact agreement on zero totals and origin IDs.
It reports maximum absolute and relative errors across the comparisons.
Both paths use the same graph, population, and prepared walking index.
The CPU warm-up includes population preparation. Use `--help` for the argument
list. Use `-` as the population path for an empty population.

CUDA is optional. On the NVIDIA machine, install it only in this environment:

```sh
julia --project=experiments/gpu -e 'using Pkg; Pkg.add("CUDA")'
julia --project=experiments/gpu -e 'using CUDA; CUDA.set_runtime_version!(v"12.9")'
julia --threads=4 --project=experiments/gpu experiments/gpu/test/runtests.jl --backend=cuda
```

For GTX 1080 Ti and other Pascal hardware, use the CUDA 12.9 compiler and runtime.
Restart Julia after the runtime selection. See the
[CUDA installation documentation](https://cuda.juliagpu.org/stable/installation/overview/).
Backend selection loads the requested device package. Driver configuration
stays under user control.

Run only the population tests on the Intel P630 with this process-local setting:

```sh
ZE_ENABLE_ALT_DRIVERS=/usr/lib/libze_intel_gpu_legacy1.so.1 julia --threads=2 --project=experiments/gpu experiments/gpu/test/population_gpu_tests.jl --backend=oneapi
```

Hardware tests disable scalar indexing and collect garbage between queries,
outside query timings, to release driver command lists.

### Validation

Population kernels pass small-fixture tests on `KA.CPU` and Intel HD Graphics
P630, including tile sizes through eight origins. Representative routing and
reduction tests report a maximum relative error of approximately `4.84e-7`.
Tests use `rtol=2e-6` and `atol=1e-3`; error depends on weights and reduction order.
Full-network hardware measurements at the target origin counts and CUDA
compilation and performance validation are pending.

### Limits

- A prepared walking index is required. The constructor prepares one hour by
  default. Its limit must cover the requested walking limit, even when the journey
  budget is smaller. An unprepared index or an excessive limit raises `ArgumentError`.
- Off-graph origins are supported. The CPU computes exact graph access and
  direct geographic walks once per origin per request. Request-local IDs include
  each origin and all direct cells, even outside the prepared output set.
- Requests allocate coverage buffers and can upload an extended weight vector.
  Device memory pools can retain allocations for reuse. Each tile processes all
  its sample blocks. Arrival storage has a fixed 32-lane capacity per graph node;
  coverage scales with output cells, and request metadata and totals scale with origins.
- Weights above the finite `Float32` range raise `ArgumentError` before upload.
  A nonfinite accumulated total also raises `ArgumentError`.
- Intel driver command lists can grow until garbage collection, independently of
  pooled device memory. Long requests require further lifetime and launch-cost checks.
- Independent GPU lanes lose the CPU engine's shared event processing. Shared
  frontiers remain future work. Use each mutable router for one request at a time
  and keep its input data fixed after construction.
