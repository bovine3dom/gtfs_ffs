# Trip workspace and compact state IDs

## Result

This change mainly reduces allocation, not search work. The 10–100 times runtime
target is not reached. Compared with commit `a8ff9c3`, warm-query allocation falls
by about 86–99% across the resolution-5 and resolution-6 tests. Results match the
baseline in all measured cases.

Tests with unit population at transit cells show these additional speedups:

| Workload | Resolution | Budget | Speedup |
|---|---:|---:|---:|
| Point | 5 | 3 h | 1.02–1.08 times |
| Walking | 5 | 3 h | 1.09–1.12 times |
| Population | 5 | 3 h | 1.11–1.16 times |
| Population window | 5 | 3 h | 1.06–1.09 times |
| Point | 6 | 3 h | 1.01–1.03 times |
| Walking | 6 | 3 h | 1.11–1.27 times |
| Population | 6 | 3 h | 1.08–1.13 times |
| Population window | 6 | 3 h | About 1.00 times |
| Population | 5 | 6 h | 1.06–1.11 times |
| Population window | 5 | 6 h | 1.04–1.08 times |

The four-worker test with 61 origins gives about 1.13–1.17 times for population and
1.12–1.17 times for population windows. The network is real in all these tests.
Population weights in these tables are synthetic, as in the earlier benchmarks.

A separate test uses the real `data/kontur_h3.arrow` population file, 61 origins,
four workers, and a six-hour budget. Population speedup is only 1.02–1.05 times;
window runtime is effectively unchanged. Warm population allocation falls from
57–105 MB to 5–7 MB. Window allocation falls from 149–230 MB to 6–7 MB.
Do not use the synthetic-weight speedups as a forecast for the real population service.

All timings are minima of three warm runs after compilation and result checks.
Small timing differences can be noise. Some windows are slightly slower within that
range. HTTP, response-cache hits, and cold loading are excluded.

## Implementation

- A byte-limited cache reuses query scratch across graphs and requests.
- Each active worker owns one workspace. The cache lock protects checkout and return,
  not the routing loop. A `finally` block returns scratch after success or failure.
- States receive UInt32 IDs only when visited. Labels, distances, and settled masks
  use arrays. A sparse key-to-ID dictionary remains at state lookup and insertion.
- Point and walking queues store UInt32 state IDs. Queue pops no longer need a state
  dictionary lookup. Distances no longer use a separate dictionary.
- Population queues store UInt32 event IDs. Array links collect pending events for
  each state. Event slots are reused after pop, so storage follows the maximum number
  of pending events, not the total event history.
- Short pending lists avoid a timed-state dictionary. After 32 linear probes, a state
  gets a sparse fallback index. This prevents repeated long linear scans on dense
  queries. Index entries are removed as events leave the queue.
- Returned results own their data. They do not refer to pooled arrays.
- Point itinerary output selects the shortest retained distance when different trip
  states have the same arrival time. The old reduction depended on dictionary order.
  This explicit tie rule prevents results from changing with reused table capacity.

`TripRouteStats` now includes `pending_event_probes` and `pending_probe_peak`.
The peak counts linear lookup probes, not the full pending-list length. Sparse fallback
lookups and the one-time index build are not included in that counter.

The first compact-ID prototype retained a dictionary for all pending population events.
It cut allocation but gave little population speedup. Replacing that dictionary with
short array lists improved the synthetic population tests. The sparse fallback keeps
this representation safe from repeated long-list scans.

## Memory limits

`ROUTER_TRIP_WORKSPACE_CACHE_MIB` defaults to 256. It limits retained idle scratch.
Set it to `0` to disable retention. It is separate from the existing population
workspace budget and scheduler estimates. It is not a limit on live query memory
or process RSS. Oversized workspaces are not retained after a query.

The cache measures backing storage, including retained array capacity. Checkout removes
that workspace from idle accounting. Eviction drops references to idle workspaces;
Julia garbage collection controls when memory is reclaimed.

There is no cell-by-trip matrix, and no array sized by the largest source trip ID.
Memory grows with visited states, pending events, and node/lane arrays. The population
lane count remains at most 64 per block. A long query can still require a large active
working set. Concurrency can multiply that memory. This change does not impose a new
hard active-query memory limit.

Real-network comparison processes used a 32 GiB virtual-memory limit and a 240-second
timeout. They loaded the full prepared `everything` component 1, with continuation
and walking indexes. Peak RSS was about 2.2 GB at resolution 5, 3.5 GB at resolution 6,
4.5 GB for the six-hour test, and 6.4 GB for the real-population six-hour test.
Each process held both implementations. These are measured workloads, not a guarantee
for every origin, resolution, horizon, or concurrent request count.

Disabling retention keeps the compact-ID implementation active. The separate no-pool
run shows that compact IDs alone do not consistently improve point latency; a London
point query was about 5% slower. Most allocation savings require warm workspace reuse.

## Validation

- 33 workspace checks pass with four threads: sparse high-valued trip IDs, reset,
  nested and concurrent leases, retained-byte limits, exception cleanup, fallback
  indexing, and owned results.
- 396 randomized baseline comparisons pass with four threads: multiple resolutions,
  point/walking/population routing, midnight crossings, and departure windows.
- Real-network point, walking, population, and window results match the baseline in
  all recorded comparisons, including the real population file.
- The full router suite still stops at the 16 startup-log assertions in
  `resolution_tests.jl`. The preceding test sets pass. The suite is not fully green.

No shard or continuation-index rebuild is required. Restart the server to use the new
code and the configured scratch-cache limit.

## Repeat the measurements

Run from the repository root:

```sh
mkdir -p /tmp/trip-state-baseline
git archive a8ff9c3 router/src | tar -x -C /tmp/trip-state-baseline
export TRIP_BASELINE_SRC=/tmp/trip-state-baseline/router/src
julia --threads=4 --project=router router/test/trip_workspace_tests.jl
julia --threads=4 --project=router experiments/benchmarks/trip-runtime-random-parity.jl
(ulimit -v 33554432; timeout 240 julia --project=router \
  experiments/benchmarks/trip-runtime-parity.jl \
  data/trip-shards/everything_res5/shard_1.bin /tmp/trip-states.csv)
```

The shard must have a continuation sidecar. Use the resolution-6 shard for that test.
Set `TRIP_BUDGET_H=6` for a six-hour budget. Set `TRIP_ORIGIN_RADIUS=4` and start Julia
with `--threads=4` for 61 origins and multiple workers. Set
`TRIP_POPULATION=data/kontur_h3.arrow` for real population weights.

Set `TRIP_PROFILE=1` to save CPU profiles, or `TRIP_COUNTERS=1` to print counters after
the timed runs. Set `ROUTER_TRIP_WORKSPACE_CACHE_MIB=0` for the no-retention comparison.
CSV files named `trip-workspace-*.csv` contain the recorded measurements.
