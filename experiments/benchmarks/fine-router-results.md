# Fine Router Label Reset

## Change

Keep the routing loops unchanged. In `_population_tile!`, initialize all labels when no settled states exist. For later tiles, reset only the columns in `settled_ids`.

Reset old range labels before a packed tile too. A packed tile can replace `settled_ids`. Without this reset, a later range tile could read old labels.

The change adds no workspace fields, shared buffers, locks, resource limits, or cache entries. The default tile rule is unchanged. Workspace allocation sizes are unchanged. This change reduces repeated writes, not the size of the first allocation.

## Results

All times below are medians in seconds. CPU time is the total for the benchmark process, including its worker threads. Allocations are decimal GB per candidate request.

| Case | Baseline Wall | Candidate Wall | Baseline CPU | Candidate CPU | Allocated GB |
| --- | ---: | ---: | ---: | ---: | ---: |
| Exact short, default | 0.633 | 0.620 | 2.955 | 2.858 | 2.239 |
| Exact short, forced 64 | 0.792 | 0.782 | 2.934 | 2.803 | 3.129 |
| Paris | 22.355 | 21.036 | 143.165 | 139.957 | 5.077 |
| London | 49.887 | 53.309 | 303.984 | 301.145 | 5.109 |
| Long-distance | 99.401 | 101.754 | 565.148 | 562.089 | 4.254 |
| Transit-only | 9.384 | 9.271 | 47.592 | 46.887 | 2.250 |
| Rural | 3.213 | 3.144 | 14.631 | 13.803 | 2.337 |
| Paris, one sample | 2.977 | 2.931 | 15.219 | 15.225 | 1.863 |
| Paris, two samples | 6.076 | 6.092 | 32.523 | 32.446 | 1.514 |
| Paris, four samples | 7.554 | 7.187 | 38.114 | 38.087 | 1.631 |

The default short case used less CPU time in all eight measured pairs. Its median reduction was 0.097 CPU-seconds, or 3.3%. The rural reduction was 0.828 CPU-seconds, or 5.7%. These are small reductions in reset work, not a large routing speed gain.

London and long-distance wall medians increased while non-process host load was higher for the candidate. There is no demonstrated overall wall-time gain. Forced-64 and long-distance queries use one tile per worker. They save no reset work between tiles; their timing differences are controls, not evidence for this change. The one-, two-, and four-sample controls do not allocate range labels.

Measured RSS depends on prior allocations. For the default short case, it was 17.34-17.40 GB. For dense Paris, it was 22.77-22.80 GB. The complete process reached 31.62 GB during the sequence of rejected and retained experiments. This is not a candidate-only peak. No memory-capacity reduction is claimed.

The retained timed calls ran from 21:59:59 through 22:34:13 UTC on 2026-09-12. There are 94 measured calls and 20 warm calls in [fine-router-trials.csv](fine-router-trials.csv). All have identical population values and expansion counts. [fine-router-summary.csv](fine-router-summary.csv) contains the per-case summaries and host-load measurements. The largest host-load estimate in a measured candidate call was 3.37 busy logical CPUs outside the benchmark process.

Native profiles are in `/tmp/opencode/fine-router-final-*-profile.txt`. The final dense profile recorded 143.379 baseline CPU-seconds and 141.880 candidate CPU-seconds. It recorded the candidate's full initialization on the first tile and the small visited-column reset on later tiles. Profile samples are diagnostic; they are not converted into latency estimates.

The 10,000-origin case was not run. Dense-city CPU reductions were small, and host load did not permit a strong large-query latency conclusion.

## Remaining Costs

The dense native profile still shows walking relaxations, per-origin label checks,
timetable searches, heap operations, and population coverage work. The retained
change removes some repeated clearing. It does not remove these routing operations.

The default short query still allocates 2.239 GB. Dense queries allocate 3.687 GB
for label matrices alone. Reducing allocation or improving data locality remains
worth testing. The rejected variants show that extra checks in the relaxation
loop can cost more than the initialization work they avoid. No hardware cache
or memory-bandwidth counters were collected.

## Method

- Use Julia 1.12.7 with eight threads on an Intel Xeon E3-1275 v6 at 3.80 GHz. The host has four physical cores, eight logical CPUs, and 62.6 GiB of RAM.
- Use `--heap-size-hint=14G` for the benchmark process. This is a GC hint, not a memory limit.
- Load one fine-8 graph per process. Share the graph, walking index, population, and schedule hints between the baseline and candidate engines.
- Call `route_population` directly. Do not use the origin result cache or an HTTP server.
- Use `everything_res8.arrow`, invalid-duration filtering, and the standard Elvas-Badajoz repair. The graph has 900,197 nodes, 2,984,575 edges, and 316,136,664 two-day profile entries.
- Load 32,957,699 population cells. The resident walking index contains 105,273,069 geographic walks and 33,030,524 network walks.
- Alternate baseline and candidate order. Run GC before each timed call. Keep GC time inside each call in the measurement. Exclude round zero from summaries.
- Record UTC timestamps, wall time, process CPU time, allocated bytes, GC time, compilation time, RSS, and estimated non-process host CPU load.
- Check H3 IDs, zero values, population totals, and both expansion counts after each timed call. Use `rtol=1e-12` and `atol=1e-6` for totals.

The host was not idle. At 22:13:09 UTC, two OpenCode processes used about 1.8 logical CPUs. The user router process was idle. The CSV records host load for each call. Wall-time differences under different host loads are not evidence of a routing speed gain.

The first benchmark process loaded the graph at 21:22:08 UTC. It was stopped after the first dense regression. The second process started at 21:32:23 UTC and completed preparation at 21:36:13 UTC. All later experiments used that resident graph. No derived global graphs were loaded.

## Source Snapshot

The baseline files were copied before the first kernel edit to `/tmp/opencode/fine-router-baseline-7YNPdO`.

| File | Baseline SHA-256 |
| --- | --- |
| `population_packed.jl` | `197c1afc8175ce5618f64b109d000628bd8b2b08ebbcf71fe62e18e9da066f40` |
| `population_range.jl` | `f9c9df8c5e84d218bae927948f18f0d86cd273b5a811d77b9c66c9ea1fb6b5be` |

The retained packed source hash is `cd4cd56f59833bc0f2814485ee1b1bf4764ee6d789e34ada589aa5be4c79361b`. The range source matches the baseline. The packed snapshot also contains the old optional callback. The benchmark never supplies that callback. Both engines use the current fine-only module for shared types and preparation.

## Cases

| Case | Origins | Samples | Departure | Budget | Walk Limit | Window | Step |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Exact short case, `881fa44181fffff`, radius 10 | 331 | 217 | 5 h | 0.5 h | 0.1 h | 13 h | 0.06 h |
| Paris, radius 18 | 1,027 | 96 | 8 h | 3 h | 1 h | 96 min | 1 min |
| London, radius 18 | 1,027 | 96 | 8 h | 3 h | 1 h | 96 min | 1 min |
| Paris long-distance, radius 6 | 127 | 96 | 8 h | 168 h | 1 h | 96 min | 1 min |
| Paris transit-only, radius 18 | 1,027 | 96 | 8 h | 3 h | 0 | 96 min | 1 min |
| Rural, 48.3 N / 3.0 E, radius 18 | 1,027 | 96 | 8 h | 3 h | 1 h | 96 min | 1 min |
| Paris short-window controls | 1,027 | 1, 2, 4 | 8 h | 3 h | 1 h | 0, 2, 4 min | 1 min |

The exact short case has 293 transit-classified origins. Its default uses 16 origins per tile and eight workers. The label matrices total 921,801,728 bytes. A forced size of 64 uses five workers and 2,304,504,320 label bytes. Do not use the forced-64 result as the default baseline.

Both dense city cases have 1,027 transit-classified origins. They use 64 origins per tile, eight workers, and 3,687,206,912 label bytes. The long-distance case uses one tile per worker, so it does not benefit from a partial reset between tiles.

## Rejected Changes

| Experiment | Reason Not Retained |
| --- | --- |
| Initialize each column on its first enqueue; check validity inside the pruning loop | Dense Paris CPU time increased by about 4.6%. |
| Move column-validity checks outside the pruning loop | Dense Paris CPU time increased by about 6.9%. |
| Use per-lane validity bits and omit column initialization | Dense Paris CPU time increased by about 6.3%. |
| Initialize labels in the constructor and reset touched columns later | Serial initialization increased request wall time. |
| Allocate and initialize each workspace inside its worker task | Short-case CPU work increased. The forced-64 result did not show a clear gain. |

The native dense profiles showed added cost in the on-demand initialization and bounds-check paths. None of those relaxation-loop changes remain in production. No earlier queue, SIMD, wide-label, schedule-cache, or coverage-expiry experiments were repeated.

[fine-router-rejected.csv](fine-router-rejected.csv) contains the rejected measurements. These measurements are not included in the retained summary.

## Correctness

The reset uses these invariants:

1. A fresh workspace has no settled IDs. Its first tile initializes the full label matrix.
2. The range enqueue function records each state that receives a finite label.
3. At the next tile, only those columns can contain finite labels. All other columns remain `INF`.
4. Packed traversal does not write range labels. Clear the old range columns before packed traversal can replace the settled-ID list.

The regular range suite tests cold zero-filled storage, first packed and range tiles, partial tiles, mode changes, and origin exclusion. It also places zero values in 10,000 extra nodes outside the graph. Later tiles must leave those columns unchanged. Existing tests cover stale queue keys, equal times, independent A/E states, moving cutoffs, the reserved time boundary, and a zero-allocation range call.

- The focused one-thread run passed 3,720 assertions across 19 test sets.
- The full eight-thread run passed 107,565 assertions across 84 test sets. This includes all 152 new reset checks. The first full run reached the tool's five-minute timeout; the retry completed with a longer timeout.
- All 24 full-graph mode/exclusion comparisons passed. The maximum difference was `1.862645149230957e-9`, in weighted `reachable_union`. The other modes matched exactly. See [fine-router-parity.csv](fine-router-parity.csv).
- Test logs are `/tmp/opencode/fine-router-final-t1.log` and `/tmp/opencode/fine-router-final-full-t8-retry.log`.

## Reproduction

```sh
julia --project=router --threads=8 --heap-size-hint=14G \
  experiments/benchmarks/fine-router-benchmark.jl \
  /tmp/opencode/fine-router-baseline-7YNPdO \
  data/everything_res8.arrow /tmp/opencode/fine-router-recheck.csv
julia --project=router experiments/benchmarks/fine-router-summary.jl \
  /tmp/opencode/fine-router-recheck.csv /tmp/opencode/fine-router-recheck-summary.csv
TMPDIR=/tmp/opencode julia --project=router --threads=1 experiments/benchmarks/fine-router-tests.jl
TMPDIR=/tmp/opencode julia --project=router --threads=8 router/test/runtests.jl
```

The benchmark helper has no dependency on the retired hierarchy files. The default run uses four measured rounds per engine. The recorded short-case confirmation uses eight rounds per engine. The recorded Paris reset screen uses three rounds per engine.
