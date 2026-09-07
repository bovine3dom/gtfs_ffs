# Straight-Line Distance Results

Measured 2026-09-07 with Julia 1.12.7, eight Julia threads, on the actual
`data/everything_res7.arrow` input (6,942,972,330 bytes). Itinerary remains the
default; straight-line is an explicit alternative, not an itinerary approximation
silently substituted by the frontend.

## Reproduce

```sh
julia --project=router --threads=8 router/benchmark-distance-modes.jl data/everything_res7.arrow
```

The optional second argument selects a hexadecimal origin. The measured origin is
representative Paris `871fb4662ffffff`, not a claim about the user's actual browser
selection. Departure is midnight, budget seven days (604800 s), departure window
one day, step 900 s (96 samples), walking limit 3600 s. Each case warms once and
reports three runs with GC before each measured run. Both modes share the exact
same packed graph and prepared walking index; no graph or distance table is cloned.
Times below include routing, chronological aggregation, final sorting and, for
straight-line, final OD-distance calculation. Serialization is measured separately.

No user server was queried, stopped or signalled. Before starting, the 62 GiB host
had 56 GiB available. The host was not isolated. An initial test attempt overlapped
graph loading and hit its runner timeout; successful full-suite reruns and diagnostic
smokes were performed after the benchmark. Startup numbers include compilation.

## Routing

| Workers | Mode | Raw seconds | Median seconds | Cumulative allocation MiB |
| --- | --- | --- | ---: | ---: |
| 4 | itinerary | 11.375897 / 11.077173 / 11.274861 | 11.274861 | 4584.667 |
| 4 | straight_line | 2.624430 / 2.504539 / 2.642910 | 2.624430 | 1085.371 |
| 8 | itinerary | 8.459158 / 8.323661 / 8.182434 | 8.323661 | 4859.769 |
| 8 | straight_line | 2.252171 / 2.212664 / 2.275094 | 2.252171 | 1271.515 |

Four-worker straight-line is **4.30x faster**, with 76.3% less cumulative allocation.
Eight-worker straight-line is **3.70x faster**, with 73.8% less cumulative allocation.
Going from four to eight workers improves straight-line latency only 1.17x; the
default remains four workers. These are warmed measurements for this origin and
workload, not a general latency guarantee or a cold HTTP benchmark.

Both modes return **732,512 cells**. At each matched worker count, H3 cells, integer
elapsed sums, coverage counts, sample count, unconditional/conditional time means,
search/full-search/repair counts, profile lookups and routing expansions match
exactly (`isequal`, with no numerical tolerance). Expected straight-line distances
are computed only on the final itinerary result; replacing that result's distance
column produces **byte-identical Arrow bodies** to straight-line routing for both
metrics. The default itinerary algorithm, chronological floating-point averaging,
selected ties and overflow behavior are retained.

## Payloads

| Metric | Payload bytes | Four-worker serialization median | Eight-worker serialization median |
| --- | ---: | ---: | ---: |
| time | 41,022,418 | 0.018030 s | 0.018064 s |
| distance_time_quantile | 52,742,938 | 0.386419 s | 0.385111 s |

Raw serialization seconds:

- Four-worker time: 0.018030 / 0.018826 / 0.016060.
- Four-worker quantile: 0.382890 / 0.387501 / 0.386419.
- Eight-worker time: 0.018064 / 0.017869 / 0.019125.
- Eight-worker quantile: 0.392012 / 0.379026 / 0.385111.

The schema and payload size are unchanged for this complete-distance input, but
the distance and distance-rank values intentionally change. Straight-line ranks
describe geographical displacement relative to travel-time ranks, not route length
relative to travel-time ranks. Network transfer and frontend decoding/rendering
are not included. The new mode does not shrink the 52.7 MB quantile response.

## Memory

Graph packing took 178.431 s and walking preparation 8.793 s. The graph contains
371,202 vertices, 1,428,854 directed edges and 178,493,346 retained daily-profile
entries. Packing skipped 62,709 invalid-duration input rows using the existing
loader option (25,563 negative and 37,146 above seven days).

Retained graph size by `Base.summarysize` is 3,530,512,136 bytes; the prepared
walking index is 351,014,088 bytes. The benchmark process's lifetime high-water RSS
was **16,713,760,768 bytes (15.566 GiB)**. This includes loading, both modes and
serialization; it is not an independently measured per-mode peak or incremental
request memory. Allocations above are cumulative allocated bytes, not retained RSS.

The script additionally runs one representative first chunk per mode/worker count
and measures its retained sample columns and routing/output scratch:

| Requested workers | Samples in chunk | Mode | Point buffers per worker MiB | Scratch per worker MiB |
| --- | ---: | --- | ---: | ---: |
| 4 | 24 | itinerary | 268.257 | 34.686 |
| 4 | 24 | straight_line | 134.129 | 12.447 |
| 8 | 12 | itinerary | 134.129 | 34.686 |
| 8 | 12 | straight_line | 67.065 | 12.447 |

Point columns drop from 16 to **8 bytes per reached cell per sample** (Int32 ID and
UInt32 arrival only). Table values include Julia container overhead. Scratch covers
arrival/eligibility labels, optional replay connections/seen/km labels, queue, and
output arrival/distance/touched arrays; it excludes the shared graph/index, point
buffers, aggregation and serialization. Other chunks can differ in reachability.
Straight-line has `nothing` for connections, replay seen labels, kmA/kmE, output km
and accumulator km. No per-sample floating-point distance averaging remains.

## Validation

- Full suite, one thread: **65,317 passed**.
- Full suite, four threads: **65,317 passed**.
- Selected catch-up/indexed/straight-line suites, eight threads: **17,419 passed**.
- New straight-line tests: **7,260 checks** per run. They compare independent
  itinerary point/window results with arrival-only routing for prepared/unprepared
  indexes, larger walking radii, off-graph/remote origins, zero budgets/walking,
  midnight and seven-day cutoffs, self-edges and no consecutive walks.
- HTTP checks cover both metrics and encodings, point/window and walking/transit,
  missing input kilometres, invalid/duplicate modes, default versus explicit
  itinerary byte parity, truthful/CORS-exposed headers, and bypass of itinerary-only
  callbacks. Expected straight-line HTTP bodies come from itinerary times plus H3 OD
  distances, not from the optimized arrival-only implementation.
- Overflow fixtures still throw in itinerary mode; straight-line point/window/HTTP
  requests succeed with the same valid arrival statistics as the no-km oracle.
- Workspace inspection proves km buffers and replay-only connection/seen state are
  absent. The adapted existing diagnostic probe passes in both modes: itinerary
  replay visits 768 on the fixture versus **zero** straight-line visits, with the
  same 111 profile lookups and 222 routing expansions. Its nonzero straight-line
  `replay_ns` is timer/branch overhead, not replay work.
- The existing `benchmark-walking-output.jl` diagnostic smoke passes for both its
  96-sample and 12-sample fixture cases after adapting the shared probe instrumentation.
- Launcher dispatch smoke passes with `ROUTER_BACKEND=cpu` and window backends
  `origin`, `catchup` and `ka_cpu`, for straight-line walking/transit points/windows.
  The launcher body is evaluated with only the final listener replaced by handler
  construction; no live user port is bound or queried.
- A real loopback HTTP listener on an ephemeral port passes eight straight-line
  walking/transit, point/window and time/quantile cases on a no-km fixture. Received
  Arrow bodies exactly match in-process responses and all OD distances are finite.

Raw logs are in `/tmp/opencode/straight-distance-benchmark.log`,
`straight-distance-tests-t1.log`, `straight-distance-tests-t4.log`,
`straight-distance-selected-t8.log`, `straight-distance-probe-smoke.log` and
`straight-distance-output-probe-smoke.log` and `straight-distance-launcher-smoke.log`.
The report retains the measurements
needed for comparison without depending on those temporary files.

## Scope

Straight-line computes great-circle distance between final H3 centres using the
same H3 authalic sphere as walking durations, once per final reachable cell. Walking
geometry still needs hop distances to calculate durations. The independent baseline
and dictionary fallback skip km propagation too, but their existing per-sample
geographic output work remains. Chronological merge, unrestricted walking radius,
output cardinality and preparation memory are unchanged; no parallel pipeline,
GPU optimization, automatic frontend selection or resource caps were added.

Transit-only HTTP straight-line points use `route_cpu`; windows use CPU catch-up
without distance replay. Transit window origin signatures retain existing grouping
and counters, including their distance-aware tie distinctions. Configured itinerary
callbacks remain the default path; straight-line CPU backend/strategy headers report
the actual engine. oneAPI hardware and browser rendering were not benchmarked.
