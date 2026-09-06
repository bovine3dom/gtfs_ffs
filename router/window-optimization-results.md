# Window Optimization Results

CPU downstream catchup is now the default window engine. Batched Intel GPU routing
is optional, not the default: the measured GPU medians did not beat CPU catchup.
Point-query backend defaults and existing query/metric/output semantics are unchanged.

## Scope And Method

- Input: distance-enriched `data/rail_and_friends_dist_res5.arrow`, 34,898,127 rows,
  4,464 invalid-duration rows skipped, 15,011 graph vertices.
- Environment: the same Xeon E3 / Intel P630 and Julia 1.12.7 environment as the
  [prior benchmarks](benchmark-results.md).
- Origin: Paris `851fb467fffffff`; departures from 00:00 across a whole 24-hour
  window, one-minute steps: 1,440 samples, reduced to 1,257 first-hop groups.
- Each sample has either a three-hour or seven-day travel budget. Graph loading
  is excluded. Full-run results below are medians of three warmed repetitions.
- This is a performance-engineering harness, not a statistical capacity benchmark.
  It times routing, replay and aggregation, not HTTP/Arrow delivery. Execution order
  alternates between repetitions; results are checked against the origin reference.

The harness validates elapsed sums, reachable counts and exact distance means during
warmup, then elapsed sums and exact distance means during timed calls. Broader engine
and HTTP tests cover unchanged fields and metrics. The combined CPU/iGPU suite passed
9,815 checks, including the 65-group boundary at width 64. Live launcher tests for
`origin`, `catchup` and `oneapi` returned identical Arrow bytes for a distance-bearing
quantile query.

## CPU Downstream Cache

Source first-hop grouping remains in use. Chunks are visited chronologically, but
groups inside each chunk run backward. The last group starts a full search;
earlier departures retain feasible arrival upper bounds and only decrease labels.
Shrinking group cutoffs remain valid: old out-of-cutoff labels are hidden, while
newly useful prefixes are repaired within the current cutoff. If an onward arrival
does not change, propagation stops there. Cached profile indices are updated only
for outgoing edges of processed, changed tails.

Canonical CPU Float64 kilometre replay uses those cached indices, final labels and
tentative `seen` states to retain exact parent ties, including zero-duration cycles
and overflow behavior. It still traverses the selected itinerary structure per
group; this is not full kilometre-chain memoization. Arrival/distance snapshots are
bounded by chunk width times vertex count, and chronological aggregation preserves
bitwise-identical floating-point means.

| Engine / Chunk | 3h Median (s) | 7d Median (s) | Full Searches | Repairs | 3h Profile Lookups | 7d Profile Lookups |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| origin | 0.241404 | 4.822635 | 1,257 | 0 | not instrumented | not instrumented |
| catchup 32 | 0.139779 | 1.834510 | 40 | 1,217 | 367,075 | 1,815,578 |
| catchup 64 | 0.139755 | 1.738205 | 20 | 1,237 | 329,260 | 1,224,973 |
| catchup 128 | 0.153096 | 1.729963 | 10 | 1,247 | 309,681 | 930,256 |

Lookup counters cover arrival routing, not grouping or distance replay. Default
chunk 64 is a bounded compromise, not a universal optimum. Catchup is strongest in
these wide-window measurements; short or sparse sweeps may favor `origin`. There
is no automatic crossover policy; the override remains available.

## Batched iGPU

The portable KernelAbstractions/Atomix kernels use UInt32 arrival labels and flags,
with two label buffers and two active-flag buffers. Each edge/query has an Int32
connection-index cache. Uploaded graph arrays are shared with the parent router.
Convergence uses per-workgroup UInt32 change flags rather than one global atomic
hotspot. Power-of-two batch sizes use bit indexing; other sizes use ordinary
32-bit division. Queue-ordered rounds are checked on the host every N rounds.

All finite active tails are processed before convergence. Uninitialized connection
slots for unreachable tails or self-edges are safe to ignore; canonical replay
reads only visited useful edges. Arrival labels and connection indices are copied
to the host. There is no GPU Float64 requirement or independently atomic distance
update: CPU replay preserves canonical kilometre ties, and mean-time aggregation
also remains on the CPU, not on the device.

Groups reuse source first-hop grouping but otherwise run independently in a batch.
There is no GPU cross-query downstream arrival reuse. Remaining optimization
targets include bandwidth, work mapping and CPU replay; their relative importance
must be measured with stage timings rather than inferred from total runtime.

### Current GPU Measurements

These use strided host views of cached connections, avoiding the full connection-row
copy for every query. The CPU and GPU rows in this report were measured together:

| Batch | 3h Median (s) | 7d Median (s) | 3h Rounds | 7d Rounds |
| --- | ---: | ---: | ---: | ---: |
| 32 | 0.955506 | 7.042394 | 624 | 3,260 |
| 64 | 1.022461 | 7.350813 | 328 | 1,664 |
| 128 | 1.041185 | 7.488767 | 172 | 852 |

Last-sample stage diagnostics for batch 32, in seconds (not stage medians):

| Budget | Planning | Device + Checks | Downloads | Host Replay | Aggregation |
| --- | ---: | ---: | ---: | ---: | ---: |
| 3h | 0.0018 | 0.7277 | 0.0523 | 0.1451 | 0.0284 |
| 7d | 0.0019 | 4.0163 | 0.0523 | 2.8303 | 0.1865 |

Device execution plus convergence checks dominates the short-budget GPU result.
For long budgets, both that stage and exact CPU kilometre replay are substantial.
Downloads are comparatively small. The view change helped short queries, but did
not improve the long-budget median; sparse access and dense traversal have different
tradeoffs. The GPU remains an explicit experimental option, not a claimed speedup
over the optimized CPU engine.

### Intermediate GPU Measurements

These are **pre-view-optimization diagnostics**, not final timings for current code.
They already include active flags, workgroup change flags, 32-bit/power-of-two
indexing and checks every four rounds, but copied an entire connection row on the
host for every query. The latest code instead reads a strided view only at edges
visited by CPU distance replay. Whole-batch device-to-host connection downloads
remain; the removed work is the per-query host row copy.

| Batch | 3h Median (s) | 7d Median (s) | 3h Rounds | 7d Rounds |
| --- | ---: | ---: | ---: | ---: |
| 32 | 1.317495 | 6.795140 | 624 | 3,260 |
| 64 | 1.452601 | 6.859779 | 328 | 1,664 |
| 128 | 1.577082 | 7.012216 | 172 | 852 |

A separate six-hour-window, single-sample batch-64 pilot after flag/index changes but before
the view change produced the following stage diagnostics. It is neither the same
workload as the whole-day table nor a stable median:

| Budget | Total (s) | Planning | Device + Checks | Downloads | Host Replay | Aggregation |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 3h | 0.170684 | 0.0004 | 0.0732 | 0.0062 | 0.0880 | 0.0027 |
| 7d | 0.8996 | not recorded here | 0.4885 | 0.0058 | 0.3778 | 0.0270 |

Stage values are seconds, rounded, and need not sum to the total. Host replay
includes extracting per-query labels. No current-GPU speedup claim is made from
these intermediate or pilot numbers.

## Running And Inspecting

The launcher defaults to `ROUTER_WINDOW_BACKEND=catchup`, with
`ROUTER_WINDOW_CHUNK=64` (1..256). `origin` selects the old reference;
`oneapi` selects GPU batching and `ka_cpu` runs the same kernels for CPU verification.
The batched launcher defaults are `ROUTER_WINDOW_BATCH=32` (1..256) and
`ROUTER_WINDOW_CHECK_EVERY=4` (1..32). These are launcher defaults; the direct
`WindowKernelRouter` constructor currently defaults to batch 64.

`make_handler` accepts a separate `window_route` callback, defaulting to CPU catchup.
CPU origin/catchup retain `X-Router-Backend: reference`, while
`X-Router-Window-Strategy` reports `origin`, `catchup`, `gpu_batched` or
`ka_cpu_batched`. Optional headers expose full searches, repairs, profile lookups,
batches and rounds. `X-Router-Searches` still counts first-hop groups, including
repair groups. Distance-bearing point queries still use CPU Dijkstra.

From the repository root, CPU reference points plus default CPU catchup windows:

```sh
env ROUTER_BACKEND=reference julia --project=router router/serve.jl data/rail_and_friends_dist_res5.arrow
```

Explicit GPU windows, without switching point queries to the GPU:

```sh
env ZE_ENABLE_ALT_DRIVERS=/usr/lib/libze_intel_gpu_legacy1.so.1 ROUTER_BACKEND=reference ROUTER_WINDOW_BACKEND=oneapi julia --project=router router/serve.jl data/rail_and_friends_dist_res5.arrow
```

The `router/benchmark-window-engines.jl` harness accepts
`input.arrow [origin_hex] [window_s] [repetitions] [--gpu]`:

```sh
env ZE_ENABLE_ALT_DRIVERS=/usr/lib/libze_intel_gpu_legacy1.so.1 julia --project=router --threads=4 router/benchmark-window-engines.jl data/rail_and_friends_dist_res5.arrow 851fb467fffffff 86400 3 --gpu
```

Omit `--gpu` and the legacy-driver prefix for CPU-only runs. The comma-separated
`ROUTER_BENCH_CHUNKS` and `ROUTER_BENCH_BATCHES` both default to `32,64,128`;
`ROUTER_BENCH_CHECK_EVERY` defaults to 4. The script prints summaries directly to
stdout, including last-sample stages rather than stage medians. No persistent raw
sample archive accompanies the numbers transcribed into this report; redirect
stdout to a new result file when retaining another run.

The [historical window report](window-results.md) remains the record of
earlier first-hop-only measurements, not a benchmark of these new engines.
