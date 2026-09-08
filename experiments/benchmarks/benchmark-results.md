# CPU versus iGPU benchmark

Date: 2026-09-06. Historical benchmark, now at [`experiments/gpu/benchmark.jl`](../gpu/benchmark.jl).

## Conclusion

Use packed CPU Dijkstra (now the server's only point engine) for the current single-origin
res5 endpoint. It had the lowest per-case median in all 96 cases, for both routing
and handler-plus-Arrow execution, in both the one- and four-worker runs.

The GPU does accelerate the dense KernelAbstractions algorithm on longer searches,
but that algorithm does substantially more work than Dijkstra. This is not evidence
that a batched GPU router would lose: batching is not implemented or measured here.
No router implementation or dispatch default was changed for this benchmark.

## Machine and data

| Item | Value |
| --- | --- |
| CPU | Intel Xeon E3-1275 v6, 3.80 GHz, 4 physical cores / 8 logical threads |
| GPU | Intel HD Graphics P630, device `0x591d` |
| Active driver | Level Zero driver `1.3.30872`, API `1.5.0`, through the legacy1 override |
| Julia | 1.12.7 |
| Packages | oneAPI 2.7.2, KernelAbstractions 0.9.42, Atomix 1.1.3, Arrow 2.8.1 |
| Input | `data/rail_res5.arrow`, 91,151,146 bytes |
| Connections | 3,254,648 input; 56 out-of-range durations skipped, as in the server |
| Packed graph | 13,832 nodes; 57,523 directed edge groups, including 8,939 self-edges |
| Timetable profiles | 4,346,010 retained entries, including the second daily period |
| Host graph footprint | 36,931,888 bytes, approximately 35.2 MiB |

Input SHA-256:

```text
1ee84df96e3c44cd802c79a403ce89fafe3d1bd88382711f0ff20d9af61d0c6f
```

These are the existing repeating-day, transit-only, coarse-cell semantics, not a
calendar-correct GTFS model. Both implementations receive the identical packed graph.

## Method

- Six city origins: Paris, London, Berlin, Madrid, Helsinki and Lisbon. Each selects
  the nearest graph-cell centre with at least two outgoing non-self edge groups,
  within 25 km of the city coordinate. Snap distances and exact indices are recorded.
- Two additional controls have one and zero outgoing non-self edge groups.
- Departures: 08:00, 12:00 and 23:50. Budgets: 3, 12, 24 and 168 hours.
- Each of the 96 cases runs 20 times on each backend and in each measured phase.
  Two Julia configurations produce 23,040 timed calls, excluding warm-up.
- Backends are packed Dijkstra, `KA.CPU()`, and `oneAPIBackend()`. Dijkstra's search
  is single-threaded in both runs; KA CPU uses the configured Julia worker pool.
- Every case/backend/phase is warmed before measurement. Case, phase and backend
  order are shuffled with seed 20260906. Runs with different thread counts are sequential.
- The graph and kernel workspace remain resident. Route time includes label allocation,
  initialization, routing, GPU convergence synchronization and final host label transfer.
- Handler time additionally includes URL parsing, the uncontended request lock, and
  uncompressed split-index Arrow IPC-file serialization. It excludes sockets, network
  transfer, browser parsing and rendering. Request objects are constructed before timing.
- Full labels and Arrow response bytes are checked against Dijkstra outside the timer,
  including after every timed call. All matched. Input and source hashes are checked
  again after the run to detect changes during measurement.
- GC remains enabled. Allocations and GC time are recorded, but timing already includes
  GC occurring inside each call; GC time must not be added a second time.

## Warm routing

Medians in milliseconds, pooling the six cities and three departure times. Each cell
in the table represents 360 measured calls. Topological controls are excluded here.

| Budget | Dijkstra | KA CPU, 4 workers | P630 GPU |
| --- | ---: | ---: | ---: |
| 3 hours | **0.038** | 0.841 | 1.831 |
| 12 hours | **0.394** | 4.235 | 4.801 |
| 24 hours | **2.252** | 20.812 | 11.780 |
| 7 days | **3.244** | 71.110 | 31.959 |

For a matched-case comparison, calculate GPU median / Dijkstra median separately
for each city/departure, then take the median of those 18 ratios:

| Budget | GPU slowdown versus Dijkstra |
| --- | ---: |
| 3 hours | 46.6x |
| 12 hours | 12.5x |
| 24 hours | 6.6x |
| 7 days | 9.9x |

These ratios differ from dividing the pooled medians because query sizes vary greatly.
For example, Helsinki reaches only 133 cells at 08:00 with a 24-hour budget, whereas
Paris reaches 6,508. Per-case results are therefore important.

### Request handler and Arrow

Medians in milliseconds for the same four-worker process and city workload:

| Budget | Dijkstra + Arrow | KA CPU + Arrow | GPU + Arrow |
| --- | ---: | ---: | ---: |
| 3 hours | **0.167** | 0.968 | 1.978 |
| 12 hours | **0.622** | 5.107 | 4.981 |
| 24 hours | **2.497** | 26.232 | 12.331 |
| 7 days | **3.522** | 71.518 | 32.724 |

For the familiar Paris origin `851fb467fffffff` departing at 08:00:

| Budget | Reachable cells | Dijkstra + Arrow | GPU + Arrow |
| --- | ---: | ---: | ---: |
| 3 hours | 461 | 0.376 ms | 2.453 ms |
| 12 hours | 4,320 | 2.218 ms | 9.593 ms |
| 24 hours | 6,508 | 3.135 ms | 16.546 ms |
| 7 days | 8,294 | 3.563 ms | 26.133 ms |

### One-worker confirmation

Warm route medians, again pooling only the city workloads:

| Budget | Dijkstra | KA CPU, 1 worker | P630 GPU |
| --- | ---: | ---: | ---: |
| 3 hours | **0.036** | 1.468 | 1.834 |
| 12 hours | **0.461** | 6.584 | 4.458 |
| 24 hours | **2.098** | 46.800 | 11.831 |
| 7 days | **3.206** | 132.949 | 31.634 |

Increasing Julia workers improves KA CPU, but does not change the winner. The runs
used 4 default + 1 interactive Julia threads, and 1 default + 0 interactive threads,
respectively. Neither run executes independent queries concurrently.

## Caveats and interpretation

- This was a shared server, with background OpenCode activity and noticeable scheduling
  outliers. One-minute load averages were 1.60 to 3.89 during the four-worker run and
  2.06 to 2.41 during the one-worker run, including the benchmark's own load. No CPU
  quota was configured on the process's cgroup or its ancestors inspected afterward.
- P95 in `summary.csv` pools different workloads; it is not the tail for one fixed
  query. Per-case P95 has only 20 samples. Medians and matched-case rankings are the
  primary comparison, not production tail-latency guarantees.
- Some outliers were much larger than recorded GC time. The shared Julia heap and
  interleaved phases also prevent clean per-backend attribution of collection costs.
- `inverse_mean_qps` is just `1000 / mean_ms`, not a sustained server-capacity test.
  It excludes work between timers and is sensitive to outliers. No batched, concurrent,
  or sustained-throughput result is claimed.
- First-invocation diagnostics are saved separately, not included in the tables.
  They share a Julia process and compiler state and run in a fixed backend order,
  so they are not comparable cold starts. Packing first took about 7.0-7.8 s; the first
  GPU route invocation about 5.6-5.7 s. The first handler also pays shared Arrow/HTTP
  compilation costs that later backends do not pay again.

Dijkstra visits useful outgoing edges as labels are settled. The current GPU-shaped
algorithm scans all edge groups every relaxation round, repeats profile lookups for
unchanged sources, and synchronizes with the host every round. These are structural
reasons to expect a single-query disadvantage, not a measured attribution of kernel
versus synchronization time. Active-frontier filtering and less frequent convergence
checks are possible optimizations; their benefit still needs measurement.

The next meaningful GPU experiment is batched origins or departure times, especially
for the planned departure-window averaging. Benchmark that against packed CPU too,
rather than assuming a speedup over KA CPU is sufficient.

## Reproduction and artifacts

From the repository root, choose new or empty output directories:

```sh
env ZE_ENABLE_ALT_DRIVERS=/usr/lib/libze_intel_gpu_legacy1.so.1 julia --project=experiments/gpu --threads=4 experiments/gpu/benchmark.jl data/rail_res5.arrow data/benchmark-t4-repeat 20
env ZE_ENABLE_ALT_DRIVERS=/usr/lib/libze_intel_gpu_legacy1.so.1 julia --project=experiments/gpu --threads=1 experiments/gpu/benchmark.jl data/rail_res5.arrow data/benchmark-t1-repeat 20
```

The measured runs are saved locally in `data/benchmark-t4/` and `data/benchmark-t1/`:

- `samples.csv`: every timed call, allocation count, and GC time.
- `per_case.csv`: per-query/backend/phase summaries.
- `summary.csv`: city/control pooled summaries by horizon.
- `workloads.csv`: exact query origins, coordinates, degrees, snap distances and reach counts.
- `metadata.toml`: runtime/device information, configuration, hashes and first-call diagnostics.

Generated data artifacts are not committed. The two-sample `data/benchmark-pilot-t4/`
was exploratory and is not included in these tables.

To use the measured fastest implementation without changing source or installing a GPU runtime:

```sh
julia --threads=8 --project=router router/serve.jl data/rail_res5.arrow
```

The production server no longer includes a selectable kernel backend.

## Larger-network repeat: rail and friends

Repeated the unchanged benchmark on `data/rail_and_friends_res5.arrow` on the same
machine, 2026-09-06 10:55-10:59 UTC. This repeat uses four Julia workers only, with
the same 96 cases, 20 repetitions, phases and random seed: 11,520 timed calls.
The six city-origin H3 indices were unchanged. All labels and Arrow responses matched.
Dijkstra again beat both alternatives by per-case median in all 96 cases, in both phases.

### Network size

| Metric | Rail only | Rail and friends |
| --- | ---: | ---: |
| Input connections | 3,254,648 | 16,910,191 |
| Skipped durations | 56 | 3,314 |
| Nodes | 13,832 | 15,011 |
| Edge groups | 57,523 | 63,257 |
| Profile entries | 4,346,010 | 12,715,880 |
| Host graph footprint | 35.2 MiB | 154.7 MiB |

The larger export has 5.2x the raw connections, but only 8.5% more nodes and 10.0%
more edge groups. Of its skipped durations, 2,094 were negative and 1,220 exceeded
seven days. First packing took 25.2 s, including compilation; this is excluded below.

### Warm routing and handler times

City-workload medians in milliseconds, with 360 measurements per entry:

| Budget | Dijkstra route | KA CPU route | GPU route |
| --- | ---: | ---: | ---: |
| 3 hours | **0.043** | 0.873 | 1.841 |
| 12 hours | **0.914** | 5.969 | 5.421 |
| 24 hours | **2.229** | 26.469 | 13.628 |
| 7 days | **3.392** | 78.577 | 34.409 |

| Budget | Dijkstra + Arrow | KA CPU + Arrow | GPU + Arrow |
| --- | ---: | ---: | ---: |
| 3 hours | **0.175** | 0.978 | 1.992 |
| 12 hours | **0.898** | 6.888 | 5.598 |
| 24 hours | **2.857** | 32.012 | 13.768 |
| 7 days | **3.733** | 78.587 | 34.627 |

The phases are independently sampled, so small median reversals are timing variation,
not evidence that serialization saves time. Handler times still exclude network and browser work.
Seven-day route medians increased from 3.244 to 3.392 ms for Dijkstra and from 31.959
to 34.409 ms for the GPU. The recommendation is unchanged: use `reference` for the
current single-query endpoint. This repeat does not measure batched GPU throughput.

Raw results and runtime details are in `data/benchmark-rail-and-friends-t4/`, using
the same five artifact files described above. Starting/ending one-minute load averages
were 2.31/4.12, including the benchmark. Input SHA-256:

```text
7eb810fd034e52a5260e50ba04c06ef31fb9e2a1528642176dbf4bafce55c021
```

Reproduce with a new output directory:

```sh
env ZE_ENABLE_ALT_DRIVERS=/usr/lib/libze_intel_gpu_legacy1.so.1 julia --project=experiments/gpu --threads=4 experiments/gpu/benchmark.jl data/rail_and_friends_res5.arrow data/benchmark-rail-and-friends-t4-repeat 20
```
