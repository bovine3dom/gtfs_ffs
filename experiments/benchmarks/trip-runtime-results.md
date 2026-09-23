# Trip-aware runtime tests

## Result

The first CPU changes improve measured query time by 2.2–7.2 times.
The 10–100 times target is not reached. These results exclude HTTP, source loading,
walking preparation, and result-cache hits. No shard rebuild is needed for these changes.

The tests use the real `everything` component 1 at resolutions 5 and 6.
The resolution-5 graph has 18,985 nodes, 111,026 edges, and 275,764,926 profile entries.
The input shard is about 7.2 GiB. Tests use London, Paris, and Berlin origins at 08:00.
Walking permits one hour per walk. Population tests use seven origins and unit weights
at real transit cells. These weights are synthetic; the network and walking data are not.
Window tests use three departures over 15 minutes. Each table uses the minimum of
three warm runs after a separate compilation and result check.

| Workload | Resolution | Budget | Speedup range |
|---|---:|---:|---:|
| Point, no walking | 5 | 3 h | 2.6–4.3 times |
| Point, walking | 5 | 3 h | 2.4–3.8 times |
| Population | 5 | 3 h | 2.8–3.1 times |
| Population window | 5 | 3 h | 2.6–3.0 times |
| Point, no walking | 6 | 3 h | 3.5–7.2 times |
| Point, walking | 6 | 3 h | 3.0–5.2 times |
| Population | 6 | 3 h | 2.5–3.5 times |
| Population window | 6 | 3 h | 2.5–3.3 times |
| Population | 5 | 6 h | 3.1–4.7 times |
| Population window | 5 | 6 h | 2.7–4.4 times |

See `trip-runtime-res5.csv`, `trip-runtime-res6.csv`, and `trip-runtime-res5-6h.csv`.

## Changes in the router

1. Remove the full timetable scan used to size the trip scratch array.
   Use a sparse generation dictionary instead. Memory follows the visited trips,
   not the largest trip ID or the total timetable size.
2. Do not repeat transfer scans when an earlier arrival already permits those departures.
   Keep same-trip continuation lookups. For walking, compare the boarding-ready time,
   not just arrival time. For population queries, keep this limit separately for each lane.
3. Add `transfer_scans_skipped` to `TripRouteStats`.

The priority queue processes increasing times. For a fixed transfer delay, a later
transit arrival cannot improve transfer access. A later walking arrival can improve it,
because walking removes the delay. The implementation retains that case.

For Paris at resolution 5, event scans fall from 1,751,028 to 205,974.
Queue pops remain 26,015. The current optimization removes repeated work; it does not
remove the trip state space. Counter files record this distinction. Their first-city
times include instrumentation compilation and must not be used as timing results.

## Rejected and experimental options

- **Clear a trip Set on each edge:** rejected. Paris took about 3.55 seconds instead
  of 0.15 seconds in the initial instrumented run. Profiling showed repeated Set
  clearing as the main cost. The generation dictionary avoids that clear operation.
- **Index outgoing edges by node and trip:** promising. The separate
  `trip-continuation-probe.jl` process builds a packed index and changes only its local
  point router. It gives another 1.5–1.9 times improvement over the optimized point
  router on the three resolution-5 origins, with equal arrival labels. Index preparation
  took about two seconds. Allocated index storage was about 231 MB, including capacity.
  This prototype is not enabled in the server. Walking, population, file format,
  distance ties, and eviction still need tests before deployment.
- **Workspace reuse:** not implemented in this pass. It can reduce allocation and GC,
  but does not remove the remaining continuation lookups. Any pool must have a byte limit.
- **Parallel origins and departure blocks:** retain the existing worker limits. More
  workers can improve throughput but also multiply query memory. No new concurrency
  claim is made by these single-process timing tests.
- **More aggressive dominance or approximate state limits:** deferred. These can change
  reachable cells. Measure the error before accepting them.
- **CSA/RAPTOR:** deferred. The present evidence points first to unnecessary scans and
  failed continuation lookups, not a need to replace H3 routing.
- **iGPU:** not run in this pass. The retained kernel rejects multiple trip IDs and
  stores one label per node. That is insufficient for these transfer rules. A naive
  node-by-trip UInt32 label array for this real shard would require about 490 GiB
  for one query, before double buffering or population lanes. A sparse state or
  connection-frontier kernel remains possible, but requires a new representation.
  Do not extrapolate the old one-label GPU benchmarks to this task.

The strongest next step is the packed continuation index. Paris still makes about
2.2 million trip-group lookups, with only about 37,000 hits. The prototype addresses
that gap without a node-by-trip matrix.

## Memory and validation

Real-network parity processes ran with a 32 GiB virtual-memory limit and a 240-second
external timeout. Peak RSS was about 3.1 GB for the resolution-5 three-hour comparison
and 5.1 GB for the six-hour comparison. Each process held both router versions.
These measurements are not a guarantee for all networks, horizons, or concurrency levels.

The added population transfer limits require four bytes per node per active lane.
The existing algorithm uses at most 64 lanes per sample block. At 18,985 nodes this
adds about 4.6 MiB per full block, not a node-by-trip allocation. The sparse trip
scratch dictionary can still grow with all trips visited by a long query.

Arrival labels, walking results, distances, and population results match the previous
router on the measured workloads. Random tests add 396 passing checks across
resolutions 5, 6, and 8, walking on/off, multiple trips, midnight crossings, long
budgets, and departure windows. The full server suite was not run in this pass.

## Repeat the tests

Run from the repository root. Julia and the router dependencies must be installed.
The baseline patch reverses only this runtime experiment, not the resolution-based delay.
Use a temporary source copy; do not apply it to the working router.

```sh
mkdir -p /tmp/trip-baseline
cp -r router/src /tmp/trip-baseline/
patch -d /tmp/trip-baseline/src -p0 < experiments/benchmarks/trip-runtime-baseline.patch
export TRIP_BASELINE_SRC=/tmp/trip-baseline/src
julia --project=router experiments/benchmarks/trip-runtime-random-parity.jl
(ulimit -v 33554432; timeout 240 julia --project=router \
  experiments/benchmarks/trip-runtime-parity.jl \
  data/trip-shards/everything_res5/shard_1.bin /tmp/trip-results.csv)
```

Set `TRIP_BUDGET_H=6` for the longer-budget comparison. Change the shard path for
resolution 6. `trip-runtime.jl SHARD OUTPUT_PREFIX BUDGET_H` writes counters and CPU
profiles. `trip-continuation-probe.jl SHARD` runs the separate continuation experiment.
