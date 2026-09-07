# Real-Data Walking Baseline

Measured 2026-09-07 on the working-tree walking implementation based on commit
`3330ddf2daac38e5477e71b9ba613dd4f7ec4cb9`. No router implementation or data was
changed for this benchmark. All assertions passed; all three graphs completed
within the 300-second process limit on both sequential runs.
These are historical measurements: the main tables precede the cumulative work
guard, and the res7 rerun measured that guarded baseline with unchanged reachability.
All provisional output, candidate, work and geometry-cache resource caps have since
been removed by user choice. These timings are not a benchmark of the uncapped version.

## Reproduction

From the repository root, with the existing router environment installed:

```sh
timeout 300s julia --project=router --threads=1 router/benchmark-walking.jl
```

This was the actual command for both complete runs. Tables use the final run,
not the fastest result across runs. Optional positional Arrow paths restrict the
inputs; with no arguments the script loads the three files listed below.

- Julia 1.12.7, Linux x86_64, Xeon E3-1275 v6 at 3.80 GHz, 4 cores / 8 logical CPUs, 62 GiB RAM. One Julia compute thread and one catch-up worker; no GPU.
- Default inputs are explicitly under `data/`. Newer root-level Arrow exports were neither loaded nor modified.
- Graphs load sequentially with `pack_graph(skip_invalid_durations=true)`. Each graph and its one static `WalkingIndex` leave scope before the next load; full GC runs between graphs.
- Origin is `(48.8566, 2.3522)` mapped to each graph's resolution. Queries depart at 08:00 with a three-hour budget. Walking uses the default 3,600-second per-hop limit, 5 km/h, centre-to-centre spherical distances, and upward millisecond rounding.
- Each timed query gets one untimed warm-up, full GC, a 0.1-second pause, then three timed calls. Reported query times are medians; stdout also contains all three observations and median allocated bytes. Load, index construction, degree scan and oracle times are single observations, not medians.
- Index construction is excluded from query timings. Queries reuse the static index, but point calls build their own per-query geometry caches. A walking window shares geometry within that call, not arrival labels or caches from an earlier timed call.
- The concurrent unit-test process had ended before the first benchmark launched. A separate existing router server remained running; this is a shared-machine baseline, not an isolated CPU microbenchmark. Small transit/disabled-query timings varied noticeably between runs.

## Inputs And Memory

| Resolution | Input under `data/` | File bytes | Vertices | Directed transit edges | Packed schedule entries | Load s |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 5 | `rail_and_friends_dist_res5.arrow` | 1,256,536,034 | 15,011 | 63,257 | 12,715,880 | 43.931 |
| 6 | `rail_and_friends_res6.arrow` | 1,257,390,570 | 35,760 | 131,966 | 22,439,385 | 33.710 |
| 7 | `rail_and_friends_res7.arrow` | 1,258,977,802 | 68,783 | 228,136 | 38,235,247 | 27.234 |

All three inputs actually contain `distance_km`. Each loader skipped **4,464**
invalid connections: 2,753 negative durations and 1,711 above seven days. Raw row
counts were 34,898,127 / 34,921,863 / 34,965,943 respectively. Packed schedule
entries include the router's retained daily profiles and next-day copies; they
are not raw input connection counts.

| Resolution | `summarysize(graph)` MiB | `summarysize(index)` bytes | Index bins | Index build ms | Process peak RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| 5 | 307.454 | 3,483,880 | 15,011 | 11.342 | 2,704.512 |
| 6 | 443.064 | 5,255,776 | 34,916 | 16.340 | 3,064.633 |
| 7 | 596.766 | 6,651,784 | 41,654 | 34.480 | 3,516.023 |

`Base.summarysize` measures retained Julia object graphs, not total process RAM.
RSS is `Sys.maxrss()`, the cumulative high-water mark of this one process,
including loading/JIT/temporary allocations, not the incremental cost of an
index. The index stores vertices, centres and bins, not a materialized global
walking adjacency list.

## Walking Topology

Every graph vertex was scanned with `walking_neighbors(index, cell)` at the
default one-hour limit. Counts are directed, exclude self, and were not sampled.

| Resolution | Directed walks | Mean degree | Median | Max | Zero-degree vertices | Paris degree | Full scan s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 5 | 0 | 0.000000 | 0 | 0 | 15,011 | 0 | 0.017691 |
| 6 | 3,106 | 0.086857 | 0 | 2 | 34,087 | 0 | 0.086557 |
| 7 | 436,486 | 6.345841 | 4 | 22 | 10,551 | 18 | 0.601298 |

The coarse Paris grids have no graph neighbours within the 5 km limit. Res6 has
some walking edges elsewhere, but neither coarse Paris query gained reachable
cells in these runs. This is resolution-sensitive centre geometry, not evidence
that walking is disabled.

## Query Medians

All times in milliseconds. `walk0` explicitly runs the walking API with walking
disabled, rather than substituting the faster transit API.

| Resolution | Paris origin | `route_details` | Point walk0 | Point walk3600 | Point walk3600 allocated MiB |
| --- | --- | ---: | ---: | ---: | ---: |
| 5 | `851fb467fffffff` | 0.694 | 0.649 | 14.776 | 0.841 |
| 6 | `861fb4667ffffff` | 0.507 | 0.843 | 32.704 | 2.329 |
| 7 | `871fb4662ffffff` | 0.609 | 4.878 | 133.526 | 10.012 |

Windows are half-open: 60 minutes means 08:00 through 08:55 at five-minute
steps, **12 samples**, each with its own three-hour budget. The script also ran
180-minute windows, **36 samples**, because each 60-minute walking median was
below its one-second extension threshold.

| Resolution | Window min | Transit catch-up ms | Walking API walk0 ms | Walking API walk3600 ms | Walk3600 allocated MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| 5 | 60 | 7.662 | 6.040 | 115.727 | 7.235 |
| 6 | 60 | 3.897 | 18.902 | 199.066 | 17.178 |
| 7 | 60 | 7.415 | 18.759 | 760.601 | 50.846 |
| 5 | 180 | 9.995 | 14.569 | 273.951 | 20.482 |
| 6 | 180 | 12.941 | 25.776 | 559.874 | 47.913 |
| 7 | 180 | 23.831 | 46.580 | 1,911.049 | 126.970 |

Walking windows perform **12 or 36 serial independent searches**, with
`reused_samples=0`. Their 60-minute medians are approximately 15x / 51x / 103x
the corresponding transit catch-up API medians; this is an end-to-end comparison
with different output coverage, not a same-work algorithm speedup. Catch-up also
reported 12/36 origin groups and zero origin-group reuse for these Paris windows;
that counter does not describe its internal cached downstream work. No catch-up
or walking optimization was attempted.

## Actual Reachability

Counts include the origin. Geographic-only means an output H3 cell absent from
`graph.node_id`, not an additional transit vertex. Every returned distance in
these inputs was known: **known-km count equals total output count; missing = 0**
for every point and window row below, in both modes.

| Resolution | Point walk0 total (all graph) | Point walk3600 total | Graph | Geographic-only |
| --- | ---: | ---: | ---: | ---: |
| 5 | 519 | 519 | 519 | 0 |
| 6 | 912 | 912 | 912 | 0 |
| 7 | 1,352 | 5,516 | 1,407 | 4,109 |

| Resolution | Window min | Walk0 union (all graph) | Walk3600 union | Graph | Geographic-only | Walk3600 reached every sample | Walk3600 partial |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 5 | 60 | 752 | 752 | 752 | 0 | 348 | 404 |
| 6 | 60 | 1,394 | 1,394 | 1,394 | 0 | 604 | 790 |
| 7 | 60 | 2,278 | 9,788 | 2,356 | 7,432 | 3,493 | 6,295 |
| 5 | 180 | 929 | 929 | 929 | 0 | 264 | 665 |
| 6 | 180 | 1,806 | 1,806 | 1,806 | 0 | 476 | 1,330 |
| 7 | 180 | 2,859 | 12,924 | 2,913 | 10,011 | 2,564 | 10,360 |

Selected actual point labels, not reconstructed itineraries. Elapsed seconds
are measured from 08:00; km is the API's chosen-route accumulated distance.

| Origin | Destination | On graph | Elapsed s | Km |
| --- | --- | --- | ---: | ---: |
| Paris res5 | `85184dc3fffffff` | yes | 10,440.000 | 513.681088 |
| Paris res6 | `86184dc0fffffff` | yes | 10,440.000 | 519.967393 |
| Paris res7 | `87184dd71ffffff` | yes | 10,798.000 | 500.385470 |
| Paris res7 | `871f9158cffffff` | no | 10,197.605 | 408.104281 |
| Paris res7 | `871f91581ffffff` | no | 9,837.419 | 407.604022 |

Each Paris origin itself returned exactly 08:00 and 0 km.

## Off-Graph Access

The deterministic nearest nonnetwork cell in a radius-12 Paris H3 disk was
`871fb4641ffffff`, centre **48.825316, 2.426403**. The same 08:00 / three-hour
query with walking enabled took **98.058 ms** median, allocating **8.254 MiB**.

- Walking disabled returned only that origin at 08:00 / 0 km; `route_details` had no reachable graph labels.
- Brute-scanning all vertices found **17** possible graph access cells, exactly matching indexed neighbour IDs. One nearest candidate was `871fb464cffffff`: **1,595.039 seconds / 2.215331 km** of access walking. This is an available access hop, not a claim that the sampled routes below chose it.
- An independently distance-filtered H3 disk contained **19** pure-walking cells including the origin. Its outer ring was checked to lie outside the walking radius.
- The actual walking-plus-transit query returned **3,584** cells: **1,004 graph + 2,580 geographic-only**, **3,584 known km / 0 missing**. **3,565** outputs lie beyond the pure-walking radius, demonstrating transit use rather than just a walking disk.

| Destination | On graph | Centre latitude, longitude | Elapsed s | Km |
| --- | --- | --- | ---: | ---: |
| `871f902e8ffffff` | yes | 45.698510, 4.900173 | 10,457.000 | 397.332480 |
| `8718630a2ffffff` | no | 48.082902, -1.746108 | 10,783.759 | 319.437174 |
| `87186319bffffff` | no | 48.120584, -1.755821 | 10,543.632 | 319.182262 |

## Validation And Limits

- **Disabled point parity passed on all three real graphs:** exact output cell set, exact arrival milliseconds and `isequal` km against `route_details`, including the origin. Enabling walking did not remove or delay any transit-reachable graph destination.
- **Disabled window parity passed for all six windows:** cell unions, exact elapsed sums and reachable-sample counts against `route_window_cached`; km means compared with `isapprox`. Enabled windows were benchmarked and counted, not independently re-aggregated from fresh point calls in this script.
- **Fine-graph independent oracle passed:** a separate two-state Dijkstra brute-scans all graph centres for access and graph walking, uses its own haversine implementation and schedule binary search, and never calls production walking geometry/routing helpers. Both transit-eligible and post-walk states are retained so consecutive walking hops are forbidden.
- For the off-graph query, all **68,783 graph labels**, including unreachable vertices, matched exactly. Seed `7307` selected 32 reached geographic destinations plus 32 independently enumerated nearby candidates; after deduplication, **62 destinations** were checked, including **2 unreachable** ones. Each expected arrival was recomputed by scanning all independent transit-eligible graph labels for final egress, plus direct walking. The graph oracle took **3.771 s**; its timing excludes the subsequent destination scans. Thus the geographic check is not just egress added to the production route's own labels.
- The oracle independently checks arrival optimality, not chosen-path km ties or raw Arrow schedule packing. It shares the already packed transit graph. All real outputs had distance data, so missing-distance propagation was not exercised here.
- Full itineraries are not available from this API. No boarding stops, services, transfer sequence or chosen access/egress chain is inferred from the returned labels. Stored transit km can be less than straight-line distance between H3 centres because the stored segments and quantized cell centres describe different geometry; these data were not physically audited.
- Walking is spherical cell-centre geometry, not a pedestrian street network: no roads, barriers or within-cell access costs are modeled. Final off-network cells are terminal destinations, not extra transfer vertices.
- The first complete run gave point walk3600 medians of 19.137 / 32.310 / 135.551 ms and 60-minute walking medians of 117.864 / 229.129 / 760.174 ms, with identical reach counts. Smaller baseline timings were less stable, e.g. res7 point walk0 was 1.254 ms first run versus 4.878 ms final run. Do not interpret tiny baseline differences as performance improvements.
- No HTTP/Arrow serialization, server latency, GPU execution, 24-hour windows, week-long budgets, or requests exceeding the then-active caps were benchmarked. The former 250,000-output-cell cap was never approached. Existing unit tests were not launched or modified by this benchmark work.

## Historical Guarded Rerun

After adding the 500,000,000-visit cumulative request guard:

```sh
timeout 300s julia --project=router --threads=1 router/benchmark-walking.jl data/rail_and_friends_res7.arrow
```

All parity/oracle assertions and reach counts above were unchanged. Medians were
**148.190 ms** for the Paris walking point, **779.927 ms** for the 12-sample window,
**1,894.490 ms** for the 36-sample window, and **85.283 ms** for off-graph access.
These queries did not exhaust the guard. The 68,783-label/62-destination independent
oracle passed again. This rerun overlapped a small live HTTP smoke test on a separate
synthetic graph, not production HTTP latency measurement.

Historical validation outside the benchmark: **16,035 tests passed with both one and four
Julia threads**, including candidate/output/work-limit rejection and cache reuse /
saturation checks. Four concurrent live HTTP requests also passed, returning 21 / 1 /
21 / 7 cells for default walking, disabled walking, a three-sample window, and a shorter
walking limit on the res9 smoke fixture. Walking-aware catch-up and GPU work remain
unimplemented; the current walking window backend is the serial baseline without
resource caps. The obsolete rejection/saturation tests above have been replaced.

After resource-cap removal, the full CPU suite passed **15,896 checks each with one
and four Julia threads**, including cache correctness, removed-keyword rejection,
invalid walking parameters and HTTP exception propagation. Real-data benchmarks
and live HTTP measurements were not rerun for this change.
