# GPU Routing Experiments

Portable KernelAbstractions/Atomix arrival kernels and batched departure windows,
kept for further experiments. These are not imported by the production server.
The CPU Dijkstra/catch-up implementation remains the reference.

## Run

From the repository root:

```sh
julia --project=experiments/gpu -e 'using Pkg; Pkg.instantiate()'
julia --threads=4 --project=experiments/gpu experiments/gpu/test/runtests.jl
```

That runs kernel differential tests on KernelAbstractions CPU without a GPU.
For Intel hardware, add `--backend=oneapi`. The environment retains oneAPI 2.7.2;
device availability is checked explicitly, with no silent fallback. The original
P630 machine required `ZE_ENABLE_ALT_DRIVERS=/usr/lib/libze_intel_gpu_legacy1.so.1`.

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
Workspaces are mutable and must not be used concurrently. Walking stays on CPU.

## Benchmarks

```sh
julia --threads=4 --project=experiments/gpu experiments/gpu/benchmark.jl data/rail_res5.arrow data/gpu-benchmark
julia --threads=8 --project=experiments/gpu experiments/gpu/benchmark-window-engines.jl data/rail_res5.arrow --gpu
```

The point comparison requires Intel hardware and four-column input. Its local
benchmark handler measures query parsing, selected experimental routing and Arrow
encoding, not production walking. The window comparison can run CPU-only without
`--gpu`; `--backend=cuda` requires CUDA installed separately on an NVIDIA machine.
Chunk/batch/worker sweep options are benchmark-only.

[Original point measurements](../benchmarks/benchmark-results.md) and
[window measurements](../benchmarks/window-optimization-results.md) explain why
the production server uses CPU Dijkstra/catch-up. Their timings and hardware
claims are historical, not new measurements of this cleanup.
