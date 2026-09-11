# Indexed Walking Output

Verified on 2026-09-07, Julia 1.12.7, eight default-pool threads, CPU `skylake`.
Prepared geographic output reduction and chronological aggregation now use stable
integer IDs, not per-sample H3 dictionaries and sorting. The then-default four-worker
96-sample query improved **2.30x**, from **26.528 s to 11.519 s**, with exact outputs.

## Workload And Method

Input: `data/everything_res7.arrow`, 6,942,972,330 bytes, 192,828,612 rows.
Packing skips 62,709 invalid durations and retains 371,202 vertices, 1,428,854
directed transit edges and 178,493,346 profile entries, including transit kilometres.
This is not the smaller rail-only input used by the previous adjacency report.

Read-only inspection of `H3-MON/www/data/reachable.json` established these defaults:
seven-day budget, midnight departure, one-day window, 900-second sampling, 3600-second
walks and `distance_time_quantile`. The benchmark uses those parameters and representative
Paris origin `871fb4662ffffff`. The actual browser origin/control state is unknown.
The regression case uses the same origin at midnight, a three-hour budget and a one-hour
window at five-minute intervals (12 samples).

The baseline was profiled **before production edits**. Its sources and diagnostic
harness came from Git revision `66a7a2d2a0532fb9137f80a725743676520d0ada`.
The benchmark packed once, retained that graph while implementation/tests ran, then
constructed the new module's `Graph` around the **identical field objects**, asserting
identity for every field. Old/new prepared indexes were built separately because their
adjacency structs differ. No timetable vectors were copied or packed twice in this
comparison. Source changes are not hot-loaded into the user's server.

Each timing follows an untimed warmup and reports the median of three runs. Packing,
preparation, compilation, diagnostics and serialization are outside routing timings.
All new warmups/repetitions pass `isequal` on all seven window result fields against
the old prepared engine. Both metrics' complete Arrow byte bodies also match.
Thread suites finished before the matched timings. The machine had 64 GiB RAM and
about 44 GiB available initially; the live server initially used 12.2 GiB RSS. No
user process was signalled, stopped or queried, and no frontend file was changed.

## Baseline Profile

The first warmed baseline was 26.816 s (25.493, 26.816, 26.880), allocating
17,624 MiB cumulatively, with median GC time 1.322 s. The diagnostic copy returned
identical outputs in 25.517 s:

| Phase | Measured Seconds | Accounting |
| --- | ---: | --- |
| Arrival repair/search | 6.118 | Sum of worker elapsed intervals |
| Fresh canonical kilometre replay | 31.672 | Sum of worker elapsed intervals |
| Geometry prewarm | 0.355 | Sum of worker elapsed intervals |
| Graph/origin result seeding | 1.373 | Sum of worker elapsed intervals |
| Geographic dictionary reduction | 26.130 | Sum of worker elapsed intervals |
| Per-sample H3 sorting/materialization | 9.755 | Sum of worker elapsed intervals |
| Chronological dictionary aggregation | 6.077 | Serial elapsed interval |
| Final output | 0.107 | Serial elapsed interval |

Worker intervals overlap and include GC/waiting. They are **not** measured CPU time
and must not be added to serial times or interpreted as wall-time percentages.
There were zero geometry builds, polygon fallbacks, cache misses or shared-cache
entries. Prewarming requested 1,126,548 already prepared ranges and built nothing.
Replay visited 54,095,990 states. The statistical profile also records heap, hashing,
sorting and GC stacks; raw profile and phase logs are listed below.

## Matched Results

| Case | Workers Requested (Used) | Median Seconds | Allocated MiB | Median GC Seconds |
| --- | ---: | ---: | ---: | ---: |
| Old prepared, 7 days / 96 samples | 4 (4) | 26.527657 | 17624.044 | 1.208009 |
| Indexed, 7 days / 96 samples | 4 (4) | 11.518533 | 4584.664 | 0.513594 |
| Indexed, 7 days / 96 samples | 8 (8) | 8.232678 | 4859.763 | 0.590741 |
| Old prepared, 3 hours / 12 samples | 4 (4) | 0.016969 | 71.518 | 0 |
| Indexed, 3 hours / 12 samples | 4 (4) | 0.015955 | 139.238 | 0 |
| Indexed, 3 hours / 12 samples | 8 (6) | 0.014685 | 197.819 | 0 |

Raw routing seconds:

| Case | Three Runs |
| --- | --- |
| Old 96 / 4 workers | 26.027820837, 26.527657258, 26.777066319 |
| New 96 / 4 workers | 11.518532548, 12.652314724, 11.473304904 |
| New 96 / 8 workers | 8.117456495, 8.232677873, 8.419097377 |
| Old 12 / 4 workers | 0.016969014, 0.016586405, 0.017226579 |
| New 12 / 4 workers | 0.015531552, 0.015954784, 0.016126609 |
| New 12 / 8 workers | 0.014684864, 0.014098446, 0.014727707 |

The broad result contains 732,512 cells; the short result contains 1,484. Four-worker
old/new search counters are identical: 4 full searches, 92 repairs, 19,323,647
profile lookups and 8,249,013 expansions for the broad query. Eight workers perform
8 full searches and 88 repairs, with 22,955,906 lookups and 10,189,365 expansions.
This is the existing chunk partition, not a claim that eight workers do less search.
The default was four workers at measurement time; it now uses all default-pool threads.

Four-worker broad-query cumulative allocation decreased about 74%. Sparse-query
allocation increased: output scratch and aggregation arrays are sized to the resident
universe even when few destinations are touched. This is a known memory tradeoff,
not a sparse-memory improvement or a claim of consistently faster small queries.

## Indexed Diagnostic

A supplemental isolated process repacked the same input for the new phase/scratch
probe after the matched comparison completed. These are not additional old/new
timing repetitions. The closed harness later used its shared graph for this probe.

The exact-output four-worker probe took 11.177 s: summed worker elapsed intervals
were 5.589 s for arrival repair, 32.587 s for canonical replay, and 3.565 s for indexed
seeding, geographic reduction and sample copying combined. Serial aggregation took
0.195 s and finishing 0.058 s. Geometry/prewarming remained zero. The dominant
remaining cost is replay, not geographic enumeration or serial aggregation.
The indexed diagnostic intentionally does not populate the old geographic-request
counter/set; its zero `unique_eligible` is not a count of reachable graph E labels.

The resident output universe has 1,086,800 IDs. Actual four-worker retained point
columns measured 1,125,151,608 bytes for 96 samples and 243,208 bytes for 12 samples.
Complete workspace `summarysize`, including the shared index but not the separate
window accumulator, was 1,548.453 MiB for the broad query and 451.965 MiB for the
short query. A short query still allocates full-universe scratch arrays.

The first repeated-fill reset microprobe allowed compiler elimination and is not
used for conclusions. A corrected no-inline microprobe used the measured universe
and touched counts, but **synthetic sequential IDs**, not the actual discovery order.
Median times for 100 resets were 12.816 ms dense versus 29.005 ms touched for 732,512
IDs, and 12.658 ms dense versus 0.037 ms touched for 1,439 IDs. Touched resets preserve
the sparse advantage; their broad-case cost is small compared with replay. No
unvalidated adaptive threshold or additional reset strategy was added. The benchmark's
corrected no-inline helper will use actual touched IDs on subsequent full runs.

A separate prepared Paris fixture's warmed 18-neighbor integer reduction allocated
zero bytes after scratch capacity was established. `@code_warntype` showed concrete
`Int32` target IDs, `UInt32` arrivals and `Float64` distances. This is a fixture-level
loop check, not a claim that complete queries allocate nothing.

## Startup And Serialization

| Measurement | Old | Indexed |
| --- | ---: | ---: |
| Preparation seconds, single build including compilation | 7.930 | 8.460 |
| Preparation allocated bytes | 1,816,454,488 | 1,941,116,384 |
| Complete prepared index retained bytes (`summarysize`) | 282,137,600 | 351,014,088 |

Packing took 202.351 s, including compilation. The additional resident output map,
cell vector and integer target column retain 68,876,488 bytes. Geographic offsets,
durations and km are shared with the existing packed adjacency, not duplicated.
The baseline probe's complete four-workspace footprint, including one resident index,
was 1,686.202 MiB. Whole matched-process peak RSS was 20,839,006,208 bytes, including
the graph, both prepared indexes, compilation, all warmups, profiling and outputs.
Cumulative allocations are not simultaneous retained memory or incremental RSS.

Broad-query time serialization produced 41,022,418 bytes, with baseline median
20.258 ms. Distance/time quantile serialization produced 52,742,938 bytes, with
baseline median 413.355 ms and indexed four-worker median 388.454 ms. The serializer
was not changed; these timings do not establish a serialization optimization.
Network transfer, browser decoding/rendering and frontend responsiveness were not
measured. A 52.7 MB payload and an 11.5 s default backend query remain substantial.

## Semantics And Verification

Graph IDs are the prefix of a deterministic union with every prepared geographic
destination. Packed integer geographic targets preserve the original H3 traversal
order. Each worker owns arrival/km arrays and touched IDs, plus reusable sample
columns containing 32-bit IDs, 32-bit arrivals and 64-bit km (16 bytes per cell of
column payload, excluding array capacity/headers). Samples seed graph A labels,
then relax direct off-graph access and graph E labels in the original order, using
strict improvements and the same tentative infinity checks. Chronological array
aggregation uses the identical incremental floating-point formula. The final reachable
H3 union is sorted once; per-sample output has no H3 dictionary or sort.

Off-graph origins remain indexed: their direct walking surface is enumerated once at
the request radius and extends output IDs locally, even outside the prepared union.
Unprepared indexes and requests exceeding the prepared radius retain the original
dictionary catch-up fallback. No output/work/cache cap or clipping was introduced.
The independent dictionary `route_window_walking`, point API, public H3 geometry
APIs, canonical replay and HTTP `walking_catchup` strategy/search counters remain.

Full CPU suites: **58,057 checks passed with one thread and 58,057 with four**.
Selected walking catch-up/output suites: **10,159 checks passed with eight threads**.
The 5,424 new checks cover exact result fields at worker requests 1/4/8 and chunk
sizes 1/2/64, overlapping geography, ties, self-edges/zero cycles, midnight and shrinking
cutoffs, zero/equal/larger walking limits, remote off-graph direct destinations,
missing km/NaN, tentative transit overflow and recovery after worker errors.
Whole in-process HTTP bodies match the independent engine for both metrics, including
96-sample requests and off-graph origins; existing live concurrent HTTP tests also pass.

## Evidence

The frozen-baseline harness is closed. Its diagnostic compared dense and touched
resets for broad and sparse output storage. Use the retained
[walking harness](benchmark-walking.jl) for current routing comparisons.

Raw local artifacts:

- `/tmp/opencode/walking-output-benchmark.log`: matched old/new timings and parity.
- `/tmp/opencode/walking-output-baseline-profile.txt`: all-thread statistical profile.
- `/tmp/opencode/walking-output-tests-t1.log` and `walking-output-tests-t4.log`: full suites.
- `/tmp/opencode/walking-output-selected-t8.log`: eight-thread selected suites.
- `/tmp/opencode/walking-output-probe-smoke.log`: updated diagnostic harness fixture smoke test.
- `/tmp/opencode/walking-output-diagnostic.log`: supplemental real-graph new phases and retained memory.
- `/tmp/opencode/walking-output-reset.log`: corrected synthetic broad/sparse reset microprobe.
- `/tmp/opencode/walking-output-inference.log`: concrete types and warmed reduction allocation check.

No commits, GPU changes, frontend changes, request caps, cancellation or streaming
pipeline were introduced. Replay/heap work and large sparse-universe scratch remain
possible follow-up targets; no further speedup is claimed without measurement.
