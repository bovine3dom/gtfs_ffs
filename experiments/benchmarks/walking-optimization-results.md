# Walking Optimization Results

Final shared-geometry implementation with staggered prewarming measured on 2026-09-07 using
`benchmark-walking-reuse.jl`. The complete run passed exact output comparisons
against the original implementation.

The 3-hour-budget, 1,440-departure window took **34.11 s original versus 1.32 s
cached with four workers**. The 7-day-budget, 12-departure case took **5.88 s
original versus 1.82 s cached-4**. Staggered prewarming roughly halved the latter
case versus the previous shared-cache implementation, while preserving exact
output. It showed no clear full-day/3h-budget benefit: that case was 7.7% slower
than the previous run, but its unchanged one-worker control was also 8.7% slower.
No further optimization changes were made as part of this final measurement.

## Reproduction

Run from the repository root. The optional output directory must already exist;
omitting it creates a fresh directory under `/tmp/opencode`.

```bash
julia --startup-file=no --compiled-modules=existing --threads=4 \
  --project=experiments/gpu experiments/benchmarks/benchmark-walking-reuse.jl \
  data/rail_and_friends_res7.arrow
```

The completed measurement used:

```bash
time julia --startup-file=no --compiled-modules=existing --threads=4 \
  --project=experiments/gpu experiments/benchmarks/benchmark-walking-reuse.jl \
  data/rail_and_friends_res7.arrow \
  /tmp/opencode/walking-reuse-prewarm-20260907
```

After graph loading the script automatically waits for a Julia process running
`router/test/runtests.jl`; `WALKING_BENCH_WAIT_PID` can additionally identify a
specific process to await. The one- and four-thread suites had both exited before
benchmark loading; no concurrent Julia suite was found at timing start or in the
spot checks. The existing router server was not stopped; its cumulative
CPU time stayed unchanged in those checks. This was not an otherwise isolated
or CPU-pinned machine. Timing-start load average was `[2.52, 2.11, 1.63]`.

The original implementation is loaded using `git show` from pinned commit
`8edcb64459f542cab4c9886c722d8660da6a3433`, which was unchanged `HEAD` during the
investigations. `WALKING_BASELINE_REV` can explicitly select another revision.
Every included baseline source comes from that revision, not the worktree.
Current sources are also loaded from an in-memory snapshot. `sources.toml`
records full SHA-256 hashes for both snapshots; no current source changed during
the completed run.

## Method

- Julia 1.12.7, four default threads, CPU target `skylake`, no GPU execution.
- Paris origin `871fb4662ffffff`, latitude 48.8566, longitude 2.3522, departure
  08:00. All cases use the default `max_walk_s=3600`; cached windows use default
  `chunk_size=64` and explicit workers 1, 2, or 4.
- Input: 1,258,977,802 bytes and 34,965,943 rows. Production validation skipped
  4,464 invalid-duration rows. Packed graph: 68,783 vertices, 228,136 edge groups,
  38,235,247 profile entries, and 596.77 MiB retained size.
- One Arrow load and one graph pack per run. Both Graph types actually have ten
  fields; the benchmark asserts identical field names and shares every field by
  identity, including all large vectors. Baseline and current walking indices are
  constructed separately. Packing took 28.62 s and is excluded from route times.
- Methods are warmed, but every measured request creates fresh topology caches
  and workspaces, as production does. There is no cross-request geometry reuse.
- Point and 3-hour-budget/12-sample results are medians of five unprofiled runs.
  Other reported medians use three runs. The original 1,440-sample case was run
  once after warming the same method on the smaller case and is explicitly not
  a median. The repetition policy reduces repeats for calls taking at least ten
  seconds; it never truncates a route, radius, budget, sample count, or result.
- Each measured current result is checked with `isequal` against the original:
  `h3`, `elapsed_sum_ms`, `elapsed_ms`, `reachable_elapsed_ms`,
  `reachable_samples`, `distance_km`, and `sample_count`. Point checks cover
  `h3`, `arrival`, and `distance_km`. This includes exact floating-point distance
  agreement and chronological aggregation, not tolerance-based comparison.
- Separate instrumented copies count actual builds, local misses, shared-cache
  access, graph lookups, replay visits, and phase times. The probe derives the
  cache types, optional three-argument topology constructor, and locking logic
  from the current source snapshot; build wrappers run inside the actual build
  branches, including the per-entry lock. Fetching an existing shared entry is
  not counted as a build. Probes pass exact parity and do not overwrite
  production methods. The copied chunk method retains the new optional `slot`
  and `workers` parameters. `warming_ns`, `warming_requests`, `warming_builds`,
  and `warming_passes` account for the entire prepass separately from arrival
  repair, distance replay, and result merging. Unprofiled timings and
  instrumented timings stay separate.

## Route Times

All entries are seconds. A point request still uses `route_walking`; there is no
separate cached point API being substituted into this comparison.

| Budget and departure sampling | Original | Current reference | Cached 1 | Cached 2 | Cached 4 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 3h, point | 0.13471 | 0.09890 | n/a | n/a | n/a |
| 3h, 1h window / 5min step / 12 samples | 0.82320 | 0.52332 | 0.23012 | 0.16349 | 0.12187 |
| 3h, 24h window / 1min step / 1,440 samples | 34.11059 single | 7.93791 | 3.05718 | 1.90791 | 1.31593 |
| 7d, 1h window / 5min step / 12 samples | 5.87844 | 4.32198 | 4.23644 | 2.55754 | 1.81531 |

Output sizes were 5,516 point cells, 9,788 cells for the short 3h-budget window,
16,143 for its full-day window, and 256,247 for the 7d-budget window.

Four workers were **6.75x faster than original and 4.29x faster than current
reference** for the short 3h-budget window; **25.92x and 6.03x** respectively for
its full-day window. On the 7d-budget window the gains were **3.24x and 2.38x**;
four workers were 2.33x faster than one, not a linear 4x speedup.

The short-window cached-4 range was 0.107-0.137 s; the 7d-budget cached-4 range
was 1.725-1.972 s. All three measured final worker counts passed exact parity.

### Scheduling Comparison

Against the immediately previous shared-cache run, prewarming reduced cached-4
time by **16.0% for 3h/12** and **51.1% for 7d/12**. The broad 7d benefit is
substantial, not just a counter reduction: cached-2 also fell from 4.21 to 2.56 s.

The 3h/1,440 result was **7.7% slower**, with no demonstrated prewarming gain.
Its one-worker path does not prewarm, yet also slowed **8.7%**; reference slowed
3.0%. These separate runs cannot establish that prewarming caused the regression.
The report retains the slower measurement rather than selecting the prior best.
No extra optimization or source changes were attempted after this comparison.

The old roughly 100-second full-day estimate was an extrapolation from twelve
samples, not a measurement. The fresh original full-day query took 34.11 s:
cross-sample cache accumulation and different times of day matter.

The optional 7d-budget/1,440-sample case was not run. The benchmark's conservative
extrapolation for workers 1 and 4 was 508.37 s, exceeding its 60-second experiment gate.
That estimate is not a measured latency and may overestimate amortized geometry
cost. This is solely a benchmark resource decision, not an application limit.

## Actual Reuse

These counters cover arrival routing only, excluding distance replay and
geographic output. Reference counters come from the exact-parity probe; cached
counters are also returned by the production engine and checked against the probe.

| Case | Engine | Full / repair searches | Profile lookups | Arrival-state expansions |
| --- | --- | ---: | ---: | ---: |
| 3h / 12 | Reference | 12 / 0 | 79,551 | 38,990 |
| 3h / 12 | Cached 1 | 1 / 11 | 27,082 | 12,275 |
| 3h / 12 | Cached 2 | 2 / 10 | 32,135 | 14,781 |
| 3h / 12 | Cached 4 | 4 / 8 | 42,378 | 19,982 |
| 3h / 1,440 | Reference | 1,440 / 0 | 7,027,282 | 3,462,557 |
| 3h / 1,440 | Cached 1, 2, and 4 | 23 / 1,417 | 740,324 | 273,417 |
| 7d / 12 | Reference | 12 / 0 | 1,278,876 | 815,892 |
| 7d / 12 | Cached 1 | 1 / 11 | 157,550 | 95,152 |
| 7d / 12 | Cached 2 | 2 / 10 | 261,319 | 161,763 |
| 7d / 12 | Cached 4 | 4 / 8 | 470,821 | 295,962 |

Cached-1 reduces lookups by **66.0%, 89.5%, and 87.7%** across those three cases.
For 1,440 samples all worker counts retain width-64 chunks, so arrival work is
identical. For twelve samples the chunk width shrinks as workers increase,
creating more independent full searches and less within-chunk repair reuse.

`searches` remains the sample count and `reused_samples=0`: every departure still
gets output. Neither field means that arrival routing performed no reuse.

Distance replay visits 38,990 states for the short 3h window, 3,462,557 for the
full-day 3h window, and 815,892 for the 7d window, independent of worker count.
Thus the arrival-lookup reduction must not be advertised as an equal reduction
in all work.

## Geometry Evidence

The pre-edit profile in `/tmp/opencode/walking-current-profile-20260907/` found
roughly **92-94%** of short-window time in geographic expansion, with about
**81-83%** in `polygonToCells`. The old full-radius-only cache made **11,023**
`walking_cells` calls for **2,280** unique eligible sources; **10,182** calls were
uncached partial-radius builds. Sorting and aggregation were each well below 1%.
The original walking window was a sequential CPU implementation despite four
available Julia threads.

For the same twelve samples, result generation still makes 19,155 geographic
requests, including 282 zero-radius requests. Prewarming adds 3,314 requests with
two workers or 6,486 with four, making their total API calls 22,469 and 25,641.
The final probe distinguishes those calls and local-cache misses from builds:

| Engine | Local geographic misses | Actual builds | Actual radius upgrades | Local coverage entries |
| --- | ---: | ---: | ---: | ---: |
| Original, pre-edit profile | 11,023 | 11,023 | n/a | 841 |
| Current reference | 8,638 | 8,638 | 6,387 | 2,251 |
| Cached 1 | 3,096 | 3,096 | 845 | 2,251 |
| Cached 2, shared + prewarm | 4,825 | 3,619 | 1,368 | 3,915 |
| Cached 4, shared + prewarm | 7,680 | 4,048 | 1,797 | 7,085 |

Only 2,251 unique sources require nonzero geographic coverage here; 29 other
eligible sources have zero remaining radius. Local entry counts include multiple
references to shared vectors, not that many independent builds. Cached-4 has
7,187 local graph-neighbor misses but only **2,280 actual neighbor builds**. The
shared registry has 4,531 keys: 2,251 geographic and 2,280 graph-neighbor sources.

Each worker first checks its local dictionary without locking. A local miss uses
a briefly locked registry keyed by `(geographic, h3)`, then a per-entry lock to
publish geometry only if the requested radius exceeds the published radius.
Published vectors stay immutable; smaller requests filter the larger result.
Local snapshots can retain older vectors after an upgrade. The registry lock is
not held during geometry computation or while waiting on an entry lock.

At the first processed sample of each parallel chunk, after distance replay,
each worker now prewarms its eligible geographic sources from a different
fractional offset and wraps around. Each still visits its entire eligible list;
this is not an output partition or cap. The subsequent result merge retains its
original order, as does chronological window aggregation. The goal is to let
workers build different shared entries instead of queueing behind the same one.

The largest-radius cache helps, but forward chronological enumeration often asks
for a slightly larger remaining radius again. Backward chunk traversal also
improves geometry reuse: cached-1 needs only 845 growth rebuilds versus 6,387 in
current reference. Its speedup is therefore not solely graph Dijkstra repair.

For 1,440 samples, reference makes 83,228 geographic builds for 3,325 unique
eligible sources. Final cached 1/2/4 makes **8,062 / 8,323 / 8,659** actual builds,
versus **8,062 / 13,518 / 22,349** before sharing. The shared registry has 6,618
keys: 3,293 nonzero-radius geographic sources and 3,325 graph-neighbor sources.
Partial-radius build counts depend on thread scheduling and the order of radius
publication; these are counts from one exact-parity probe, not fixed API counters.

For the 7d budget all 33,989 eligible sources use full radius. **Reference and
cached 1/2/4 now all build geographic coverage exactly 33,989 times**, with zero
upgrades. The same is true for graph-neighbor builds. Cached-4 still has 135,956
local misses for each kind, but only 33,989 actual builds. Its shared registry
has 67,978 keys. Before sharing, cached-2/4 computed 67,978 / 135,956 geographic
builds. Full-radius duplicate computation has been eliminated, not merely hidden
by a larger local dictionary.

Every geographic build observed by the current probes used the certified disk
fast path; **zero polygon fallbacks** occurred in all three workloads and all
worker configurations. Exact comparison against the old polygon implementation
passed for the full output fields. This demonstrates the fast path is actually
used for these workloads, not that all origins/radii will avoid fallback.

The fast path is not an output cap: `_walking_disk` certifies that the outer
ring's cell polygons miss the walking cap, using center distances and enclosing
cell radii. If its bounded attempts cannot certify coverage, polygon enumeration
remains the fallback. Both paths retain the exact distance cutoff. This benchmark
does not replace the geometry correctness tests or prove all H3 edge cases.

## Remaining Costs

Single-worker exact-parity instrumentation, in milliseconds. Geographic egress
includes cache handling, candidate enumeration, and result-dictionary merging;
the graph phase includes graph-neighbor work. These rows are exclusive phases.

| Phase | Reference 3h / 12 | Cached 1, 3h / 12 | Cached 1, 3h / 1,440 |
| --- | ---: | ---: | ---: |
| Arrival graph routing | 35.39 | 20.52 | 159.79 |
| Canonical distance replay | 0 | 12.22 | 1,008.35 |
| Geometry prewarming | 0 | 0 | 0 |
| Result-dictionary seeding | 1.29 | 1.31 | 153.29 |
| Geographic egress | 445.83 | 179.79 | 1,364.72 |
| Per-sample sorting and result vectors | 3.71 | 3.77 | 306.80 |
| Chronological aggregation | 3.22 | 2.01 | 200.57 |
| Final window sorting/output | 0.56 | 0.55 | 1.56 |
| Instrumented wall time | 490.08 | 220.39 | 3,196.66 |

For cached-1 with twelve samples, actual geometry computation is **170.96 ms,
77.6%** of wall time; disk discovery/certification alone is **136.53 ms, 61.9%**.
These are subcosts of egress, not extra rows to add. The production sampling
profiles also show substantial `cellToBoundary` work inside disk certification.
Replacing polygon filling did not make fresh geographic discovery free.

For 1,440 samples, geometry computation falls to **500.40 ms, 15.7%** after cache
amortization, while replay becomes **1,008.35 ms, 31.5%**. Replay is small on the
short-window and 7d-budget cases, but it is no longer negligible for this full-day
case. Serial aggregation is **6.3%** of that single-worker run, not the dominant
cost; it was only about 0.4% in the original short-window profile.

For the final four-worker full-day probe, aggregation itself took 205.99 ms of
1.318 s wall time, about **15.6%**. Replay summed to 1.285 worker-seconds, prewarming
to 0.351 worker-seconds, and subsequent geographic egress to 1.354 worker-seconds.
Actual geometry computation, counted wherever it occurred, was 0.652
worker-seconds and is already included in prewarming/egress. These parallel
totals must not be presented as exclusive percentages of wall time.

### Prewarming Attribution

`warming_ns` includes list discovery, rotated traversal, cache access, builds,
and waits during the prepass. It is a separate phase, not included in graph
repair, replay, or subsequent result merging. Geometry build timers count builds
in either prewarming or result generation and therefore overlap those phases.

One-worker runs do not prewarm. Per-case request/build/pass counts are in `run.log`;
the output-generation request counts remain 19,155, 1,699,164, and 407,868.

The broad 7d-budget four-worker comparison shows the intended scheduling effect:

| Instrumented phase | Shared, no prewarm | Final prewarm |
| --- | ---: | ---: |
| Arrival routing, summed workers | 0.664 s | 0.787 s |
| Distance replay, summed workers | 0.322 s | 0.351 s |
| Prewarming, summed workers | 0 | 3.857 s |
| Subsequent geographic egress, summed workers | 12.160 s | 0.624 s |
| Actual geometry builds, overlapping the above | 2.843 s | 3.511 s |
| Shared access including builds and waits, overlapping | 12.106 s | 4.444 s |
| Serial aggregation | 0.204 s | 0.240 s |
| Request wall time | 3.646 s | 1.798 s |

All 33,989 geographic builds moved into prewarming; none were omitted from the
measurement. The non-build portion of summed shared access fell from about
**9.09 to 0.73 worker-seconds** after subtracting actual geographic and neighbor
build time. That remainder includes registry/entry-lock work and waiting, not
just pure lock wait, and it is not additive request latency. The reduction is
consistent with breaking the same-entry convoy rather than reducing the number
of required geographic surfaces. Serial aggregation is now about **13.4%** of
the broad four-worker probe. Geometry preparation, graph/replay work, output
construction, and aggregation still impose a remaining backend cost; no further
optimization is attempted here.

The two labels are genuinely needed: earliest arrival permits boarding, whereas
walking eligibility means the origin or a transit arrival. A walking arrival may
beat a later eligible transit arrival without making another walking leg legal.
Collapsing them would lose valid itineraries or allow consecutive walking legs.
Fresh canonical distance replay preserves deterministic distance choices when
earlier departures change prefix distance even if downstream arrival labels do
not change. It uses cached connections instead of repeating schedule searches.

Instrumentation adds timers and counters. Its multiworker phase totals are sums
of overlapping worker elapsed times, not fractions of request wall time. Worker
phase fractions are quoted only for single-worker runs; aggregation is separately
timed on the serial consumer, including in multiworker runs.

## Memory And Serialization

MiB below distinguish total allocations per production request from retained
objects. Workspace size uses **one `Base.summarysize` traversal over the complete
workspace collection**, excluding mutable probe metrics. It includes the one
resident walking index (about 6.344 MiB), counted once, plus private state, point
buffers, local cache dictionaries, old local snapshots, and the shared registry.
The immutable index's fields are inlined; excluding its type alone does not
reliably exclude its reachable arrays in Julia 1.12, so it is explicitly included.
The old pre-shared report's workspace figures excluded that index and are not
directly comparable without accounting for it.

Geometry-cache size likewise traverses all local caches and shared references
together, rather than summing per-worker sizes. Shared-registry size is a subset
of geometry-cache size, which is a subset of workspace size; do not add these
columns. These are approximate retained sizes, not isolated RSS peaks.

| Case | Workers | Allocated/request | Unique workspaces, with index | Unique geometry caches | Shared registry subset |
| --- | ---: | ---: | ---: | ---: | ---: |
| 3h / 12 | 1 | 26.30 | 12.64 | 1.86 | 0 |
| 3h / 12 | 2 | 31.63 | 16.88 | 3.12 | 2.60 |
| 3h / 12 | 4 | 38.73 | 23.45 | 3.70 | 2.60 |
| 3h / 1,440 | 1 | 767.67 | 20.74 | 3.20 | 0 |
| 3h / 1,440 | 2 | 774.19 | 33.86 | 5.32 | 3.53 |
| 3h / 1,440 | 4 | 782.65 | 47.47 | 7.19 | 3.53 |
| 7d / 12 | 1 | 558.76 | 105.64 | 37.47 | 0 |
| 7d / 12 | 2 | 589.24 | 126.42 | 55.07 | 48.82 |
| 7d / 12 | 4 | 604.82 | 139.02 | 61.32 | 48.82 |

Original/current-reference full-day allocations were 3,466.78 / 3,147.81 MiB per
request. Original/current-reference 7d-window allocations were 592.04 / 578.92
MiB. Four workers still allocate slightly more than original in the 7d-budget
case, but much less than the pre-shared version: **604.82 versus 1,117.74 MiB**,
about a 46% reduction. Its unique geometry caches fell from **149.88 to 61.32
MiB**, about 59%, despite the new registry and per-entry locks. The 135,956 local
coverage entries are references, not 135,956 separately owned geometry vectors.

Process peak RSS was **3,850.29 MiB**, versus **3,488.31 MiB** after loading. This
is a cumulative high-water mark across loading, all engines, and probes, not a
per-engine RSS comparison. Complete CLI wall time was **4m18.938s**, including
loading, all timing repetitions, correctness checks, probes, and profiles.

Separately measured ranked Arrow serialization (`distance_time_quantile`, split
H3 encoding), excluded from the route table:

| Case | Median | Arrow bytes |
| --- | ---: | ---: |
| 3h / 12 | 3.76 ms | 706,810 |
| 3h / 1,440 | 6.72 ms | 1,164,386 |
| 7d / 12 | 114.31 ms | 18,451,874 |

Serialization is small relative to these backend route times, though the large
7d payload could have separate transfer and frontend costs.

## Artifacts And Limits

The authoritative final run is tagged `prewarm-final` at
`/tmp/opencode/walking-reuse-prewarm-20260907/`:

- `run.log`: all runs, exact-parity results, counts, phase times, memory, and CLI timing.
- `sources.toml`: baseline revision, source hashes, Julia version, thread count,
  input path, and byte size. The input itself was not SHA-256 hashed.
- `budget3h_samples12-reference.{flat,tree}.txt` and
  `budget3h_samples12-cached1.{flat,tree}.txt`: 1 ms production sampling profiles
  with C frames, restricted to the active single worker.

Earlier artifacts remain unchanged: `shared-no-prewarm` is
`/tmp/opencode/walking-reuse-shared-20260907/`, `pre-shared` is
`/tmp/opencode/walking-reuse-20260907-complete/`, and `original-profile` is
`/tmp/opencode/walking-current-profile-20260907/`. Earlier original full-day
times of 33.50351 and 33.64995 s are historical; the final table uses the freshly
measured 34.11059 s, not a mixture of runs.

The adapted prewarm probe was smoke-tested for exact output, four warm passes,
and output-request accounting, then completed the real-data matrix with workers
1/2/4. All current measured routes and instrumented copies passed exact checks.
No current-engine correctness bug was found. Separately, the final full suite passed
**49,749 checks with both one and four threads**, including live HTTP, geometry
coverage, replay, shared-cache publication and recovery tests.

This is one origin and one dataset, with a small number of repetitions, shared
machine scheduling, and only one original full-day timing. No browser, HTTP
transfer, cancellation, request queueing, frontend processing, or rendering
profile was taken. These backend improvements do not establish that the entire
user-visible long tail is fixed.
