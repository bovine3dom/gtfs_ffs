# Resident Walking Adjacency

Verified 2026-09-07 with Julia 1.12.7, CPU `skylake`, four default-pool threads.
The service now pays for exact geographic discovery once at handler construction,
instead of rediscovering graph-vertex neighborhoods in each request.

## Implementation

`WalkingIndex(graph)` remains an inexpensive, unprepared spatial snapshot.
`prepare_walking(index; max_walk_s=3600, workers=min(4, Threads.nthreads(:default)))`
returns a new index containing read-only packed adjacency. Each graph vertex runs
the existing exact `walking_cells` once. Workers own separate vertex lists; packing
is deterministic in vertex order. The geographic list retains canonical H3 order,
and its graph subset stores integer node IDs in that same order. Durations and
kilometres retain the exact existing values, including upward millisecond rounding.

`make_handler` eagerly prepares one default-radius index and shares it across point
and window requests. Borrowed ranges avoid per-query neighbor copies, geometry,
locks, and per-hop graph H3 lookups. Every routing/replay/egress loop checks both the
requested hop limit and remaining budget. Off-graph origins and larger requested
radii use the existing exact geometry and request-local caches. There is no persistent
fallback cache, clipping, new resource cap, or change to `max_walk_s=0..604800`.

Constant propagation of the geographic selector and integer iterator state keep
packed/vector iteration inferred. An early benchmark exposed boxed iteration; that
version was fixed and its timings are not used below. `@code_warntype` confirmed
concrete geographic hop fields after the fix.

## Reproduction

```sh
julia --project=router --threads=1 router/test/runtests.jl
julia --project=router --threads=4 router/test/runtests.jl
julia --project=router --threads=4 router/benchmark-walking-adjacency.jl data/rail_and_friends_res7.arrow
```

The new standalone benchmark leaves the existing source-instrumented harness intact
apart from importing its new range type; that harness's probe also passed a fixture
window parity smoke test. The benchmark packs the input once, builds the real adjacency
once after compiling preparation on an empty index, warms each query, then measures
three runs. Tests were finished before the reported benchmark was started.

Input SHA-256: `8ee8596c7dc825f0f03e0e7fc729489b4825704a0714028025e01eb47b3d40aa`.
The input contains 34,965,943 rows; loading skips 4,464 invalid durations, retaining
68,783 vertices, 228,136 directed transit edges and 38,235,247 packed profile entries.
Packing took 34.621 s, including compilation. The input has no transit kilometre column.

All query comparisons use the same packed graph, Paris origin `871fb4662ffffff`,
08:00 departure, three-hour journey budget and 3,600-second walking limit.
The short window spans one hour at five-minute intervals (12 samples); the long
window spans one day at one-minute intervals (1,440 samples). Both walking paths use
`route_window_walking_cached` with four workers and the default chunk size 64.
The unprepared comparison uses the existing request-local geometry path in the same
source version, not a separately compiled historical revision. No-walk comparisons
use `route_details` and `route_window_cached` with the same graph, budget and workers;
their outputs intentionally differ because walking is disabled.

## Startup And Memory

| Measurement | Value |
| --- | ---: |
| Preparation, single measured build | 1.524 s |
| Preparation allocated bytes | 305.919 MiB |
| Geographic directed neighbors | 1,194,218 |
| Graph directed neighbors | 436,486 |
| Bare spatial index, `summarysize` | 6.344 MiB |
| Packed adjacency including node map, `summarysize` | 40.083 MiB |
| Complete prepared index, `summarysize` | 46.426 MiB |
| Whole benchmark peak process RSS | 3,591.234 MiB |

RSS includes the graph, loading, compilation, warmups and all queries, not just adjacency.
Build-time private lists and growing packed arrays temporarily exceed retained size.
The fixed radius bounds which neighbors are retained, not their number: fine-resolution
graphs can have very large adjacency. Explicit preparation and server startup have no caps.

## Warmed Results

| Query | Unprepared | Prepared | No Walk | Walking Speedup |
| --- | ---: | ---: | ---: | ---: |
| Point | 91.764 ms | 1.834 ms | 0.663 ms | 50.02x |
| 12 samples | 104.655 ms | 13.456 ms | 9.089 ms | 7.78x |
| 1,440 samples | 1.376 s | 1.121 s | 0.289 s | 1.23x |

Raw elapsed seconds and median total allocations:

| Case | Three Runs (s) | Allocated MiB |
| --- | --- | ---: |
| Point unprepared | 0.091763936, 0.081348806, 0.097313786 | 9.846 |
| Point prepared | 0.002144838, 0.001834491, 0.001704063 | 2.572 |
| Point no-walk | 0.000703001, 0.000662712, 0.000423527 | 0.793 |
| 12 unprepared | 0.104654922, 0.095102926, 0.111958964 | 38.709 |
| 12 prepared | 0.013455675, 0.014491739, 0.013021943 | 27.452 |
| 12 no-walk | 0.007442047, 0.009102518, 0.009089087 | 13.283 |
| 1,440 unprepared | 1.379889264, 1.375637060, 1.355886696 | 782.704 |
| 1,440 prepared | 1.075442475, 1.121182187, 1.154925596 | 1130.915 |
| 1,440 no-walk | 0.316569182, 0.275433077, 0.289248032 | 214.891 |

The prepared full-day path allocates about 348 MiB more cumulatively than unprepared,
despite being faster. These are total allocations over a query, not simultaneously
retained memory. This remaining allocation cost has not been isolated by phase.
Adjacency removes discovery, not per-departure routing, distance replay, geographic
result construction or chronological aggregation. These results cover one origin and
dataset, three repetitions, and no HTTP serialization, browser or frontend transfer.
The optional seven-day-budget benchmark was not run.

## Verification

The complete CPU suite passed **52,633 checks with one thread and 52,633 with four**,
including 2,884 new adjacency checks. Coverage includes res7/res9 packed versus
unprepared point and reference/catch-up windows, zero/smaller/equal/larger hop limits,
remaining-budget boundaries, off-graph origins, missing kilometre values, transit
ties and self-edge eligibility, changing origins, graph mismatch rejection,
deterministic serial/parallel packing, and concurrent read-only index reuse.
Existing live concurrent walking HTTP/Arrow tests passed with eager handler preparation.
Covered graph-origin routes leave both topology caches and the shared lock registry
empty; larger-radius/off-graph tests confirm fallback cache population instead.

Every measured prepared result, including warmup and all repetitions, passed `isequal`
against the unprepared result for all point fields or all seven window output fields.
This includes exact selected-route kilometre values and `NaN`, not just arrival times.
No GPU or frontend tests were run for this CPU-only change.

Raw logs are in `/tmp/opencode/walking-adjacency-benchmark-matched.log` and
`/tmp/opencode/walking-adjacency-tests-t{1,4}.log` on the verification machine.
