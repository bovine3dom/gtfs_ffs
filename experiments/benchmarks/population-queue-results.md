# CPU Population Queue and Range Repair

## Method

The baseline is clean HEAD `fad4b3b`. It uses the packed CPU engine with the
default 16-origin window tile. It is not the older H3-dictionary implementation.
The frozen source is in `/tmp/opencode/population-cpu-frozen`.

The benchmark uses `data/everything_res7.arrow` and `data/kontur_h3.arrow`.
Graph loading uses `skip_invalid_durations=true` and `badajoz_shuttle=true`.
One process loads one graph. Separate module constructors share the same graph,
walking, population-map, and prepared-weight arrays. Identity assertions check
this sharing. No GPU or shortcut module is loaded.

The host has an Intel Xeon E3-1275 v6 with four physical cores and eight logical
CPUs. Julia 1.12.7 uses eight routing threads and one interactive watchdog thread.
The live server stays running. Final trials reject measured live-server CPU use
above the greater of 0.05 seconds and 5% of trial time. Baseline and candidate
trials alternate after warmup. The three-hour cases use three retained trials per
variant. The final seven-day case uses one retained call per variant because the
baseline exceeds one minute. All origins, values, and zero masks are compared.
Value tolerance is `rtol=1e-12`, `atol=1e-6`.

The origin is the resolution-7 cell at the centre of `861fb4667ffffff`.
Departure is 08:00. The window is 24 hours, with a 15-minute step and 96 samples.
The primary budget is three hours. Radius 6 also has a 168-hour budget case.
The timed selector is `mean_intersection`, with origin population included.
Separate full-graph comparisons cover all six selectors and both exclusion values.

## Final Results

Use the `VERIFIED_RESULT` rows in the raw log. An intermediate timing pass used
an incorrect process-stat parser during live-server activity checks. Those times
are not used in this table. The final pass restores and uses the original parser.

| Origins | Budget | Baseline seconds | Candidate seconds | Speedup | Measurement |
| ---: | ---: | ---: | ---: | ---: | --- |
| 127 | 3 h | 2.958 | 1.948 | 1.52x | Median of 3 |
| 331 | 3 h | 8.614 | 6.290 | 1.37x | Median of 3 |
| 1,027 | 3 h | 22.863 | 17.437 | 1.31x | Median of 3 |
| 127 | 168 h | 82.029 | 39.232 | 2.09x | One warmed call each |

| Origins | Budget | Baseline allocated bytes | Candidate allocated bytes | Baseline allocations | Candidate allocations |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 127 | 3 h | 297,739,480 | 639,603,104 | 3,902 | 3,913 |
| 331 | 3 h | 305,320,704 | 644,537,032 | 7,416 | 7,296 |
| 1,027 | 3 h | 305,639,160 | 654,770,640 | 21,052 | 20,562 |
| 127 | 168 h | 1,905,243,048 | 1,402,580,128 | 4,773 | 4,572 |

The short-budget requests trade approximately 342-349 MB of additional allocation
for lower request time. The eight arrival matrices reserve 388,408,320 bytes.
Their capacity is reused across tiles in the same request. Grouped coverage
records reduce other buffer costs.

For the seven-day request, allocation falls by approximately 503 MB. The final
verified calls report zero major faults and zero live-server CPU seconds.

Short-window checks use the same three-hour budget and three trials per variant:

| Origins | Samples | Baseline seconds | Candidate seconds |
| ---: | ---: | ---: | ---: |
| 127 | 1 | 0.0508 | 0.0418 |
| 1,027 | 1 | 0.3506 | 0.3129 |
| 127 | 4 | 0.2013 | 0.2153 |
| 1,027 | 4 | 1.3126 | 1.1420 |
| 127 | 5 | 0.2223 | 0.1878 |
| 1,027 | 5 | 1.8321 | 1.2700 |
| 127 | 16 | 0.6450 | 0.4588 |
| 1,027 | 16 | 4.9979 | 3.6935 |

Short calls have visible run-to-run variation. The 127-origin four-sample case
does not show a gain in this pass; it uses the unchanged packed search. Its trial
ranges overlap: 0.1545-0.2031 seconds for the baseline and 0.1754-0.2289 seconds
for the candidate. No speedup is claimed for that case. The five- and sixteen-sample
checks support use of range repair when a tile spans multiple time blocks.

## Input Identity

| File | Bytes | Raw rows | SHA-256 |
| --- | ---: | ---: | --- |
| `everything_res6.arrow` | 7,394,123,842 | 205,357,244 | `ffb1f5ebc8a43dcf3bb387667aa9d925af4c8363ead983c85ce4ecd19a3b550e` |
| `everything_res7.arrow` | 7,397,939,938 | 205,463,043 | `614ac1a6a5b87d35c6e83c057a64161005fb339e80a369f4065709e3d33309d7` |
| `everything_res8.arrow` | 7,402,563,314 | 205,591,459 | `eb0b3d7e26bed7535522a4ce34aa1f4439053676a3d99356e8887c1b86b06ada` |
| `kontur_h3.arrow` | 234,395,250 | 32,957,699 | `c21eaf6c3eb65563e80f2055347ad13f979014a190f8472817c25a591f427eb3` |

Only resolution 7 is packed for routing. Resolution 6 and 8 checks read file
metadata and compute hashes. Resolution 7 skips 88,936 invalid rows and adds
2,342 shuttle connections. The packed graph has 379,305 nodes, 1,471,782 edges,
and 192,645,963 profile entries. The population total is 8,031,924,024.

Walking uses the production one-hour limit and a one-hour prepared index.
There are 3,737,032 network walk edges and 6,038,589 positive-population walk
entries. The baseline profile samples the network-walking loop. This is not the
resolution-6 one-hour case that traverses no walking edges.

## Scheduling Experiment

The queue-only candidate keeps the baseline routing kernel and aggregation order.
It has fixed worker tasks, private workspaces, disjoint output slots, and no tile
wave barrier. Worker errors stop further work claims. All tasks join before an
error returns. Its expansion counts and values match the baseline.

| Origins | Budget | Baseline seconds | Queue seconds | Trials |
| ---: | ---: | ---: | ---: | ---: |
| 331 | 3 h | 8.896 | 8.676 | 3 each |
| 1,027 | 3 h | 22.699 | 21.442 | 3 each |
| 127 | 168 h | 81.554 | 82.580 | 3 each |

The queue-only change gives small gains when tiles span multiple worker groups.
The 127-origin case has eight tiles and no second worker group. Some short-budget
127-origin scheduling trials overlap live-server activity and are not used here.
The seven-day queue-only difference is about 1.3%; it does not establish a gain.

## Range Experiment

The first prototype retains independent transit and walking-eligible labels for
each origin. It processes departure samples backward and repairs only strict
arrival improvements. It rebuilds coverage from all valid labels for each sample,
including unchanged labels. Coverage still uses 64-lane blocks.

The first prototype stores one egress record per origin and sample. Its warmed
medians are 7.099 seconds at 331 origins and 18.785 seconds at 1,027 origins for
the three-hour budget. At 127 origins and 168 hours, its warmed median is 51.408
seconds, but request allocation is 9.149 GB. The first long-budget call takes
257.534 seconds while system swap use rises. That call is not a warmed median.

The revised implementation groups equal-time egress records and reuses arrival
buffers per worker, rather than allocating them per tile. A tile with one time
block retains the packed origin/sample search. There is no new public option.

## Routing Counters

These counts include both graph states and source labels. They exclude coverage
scans, which are included in request time and CPU profiles. The queue-only counts
match the baseline. The final range counts match the first range prototype.

| Origins | Budget | Baseline shared expansions | Range shared expansions | Baseline query expansions | Range query expansions |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 127 | 3 h | 7,754,276 | 9,227,295 | 127,118,228 | 66,825,790 |
| 331 | 3 h | 21,429,560 | 30,566,152 | 288,650,184 | 153,098,692 |
| 1,027 | 3 h | 57,454,931 | 91,632,321 | 604,264,154 | 322,957,638 |
| 127 | 168 h | 164,541,443 | 105,778,211 | 7,003,217,627 | 887,013,346 |

Range routing shares origin lanes within one sample, rather than origin/sample
lanes within a block. Thus, its shared expansion count can rise while request
time falls. Its query expansion count measures label improvements, not coverage
records for unchanged labels.

## Tests

The final full production suites pass 99,898 assertions on both one and eight
threads. Both runs finish within 600 seconds. They include later walking-eligible
arrivals after an earlier walking arrival at the same node.
Startup warmup also covers multi-block population windows with walking. Separate
one- and eight-thread checks passed after extending the synthetic warmup.
Additional focused runs pass 2,907 assertions each on one and eight threads,
including those 12 assertions. Production source is unchanged between these runs.

Tests cover all six selectors, moving cutoffs, zero-duration cycles, UTC midnight,
off-graph access, direct walks, partial tiles, fractional weights, zero outputs,
origin exclusion, prepared-data identity, HTTP, WebSockets, and network dispatch.
Queue tests hold the first tile while another worker advances beyond the first
worker group. They check each output slot, workspace ownership, joined failures,
and a successful request after a failure. Range tests reuse private buffers across
tile sizes and selectors. Inference checks pass. The warmed range sample kernel
allocates zero bytes when called through a typed function boundary.

## Memory and Profiles

Shared object sizes are 3,530,544,548 bytes for the graph, 352,083,192 bytes for
walking, 95,207,796 bytes for prepared population, and 285,212,784 bytes for the
resolution-7 population map. These figures overlap where objects share arrays.
Do not add them to estimate process RSS.

Graph loading reaches a process high-water RSS of 23,384,850,432 bytes. System
swap use rises during loading. Request measurements exclude graph loading,
population aggregation, and walking preparation. The live server is not stopped,
restarted, or queried by the benchmark.

A separate observer samples process RSS approximately every 20 ms during the
verified pass. These figures are the largest observed RSS within each case's
timed calls. They include the shared graph, both module namespaces, and earlier
profile storage. They are not isolated workspace sizes or exact request peaks.

| Origins | Budget | Baseline sampled peak RSS, bytes | Candidate sampled peak RSS, bytes |
| ---: | ---: | ---: | ---: |
| 127 | 3 h | 8,957,779,968 | 9,346,211,840 |
| 331 | 3 h | 9,099,296,768 | 9,422,413,824 |
| 1,027 | 3 h | 8,957,718,528 | 9,314,750,464 |
| 127 | 168 h | 9,117,409,280 | 9,341,083,648 |

The observer log is `/tmp/opencode/population-rss.csv`. Process RSS does not
return to a fixed value between requests. Use allocation and buffer sizes to
compare storage requirements; use sampled RSS to describe this shared process.

The baseline profile has 2,950 routed stack samples. Schedule lookup accounts for
20.0% at the innermost population source line. Heap removal accounts for 19.46%,
and pending-event removal accounts for 17.83%. Full stack samples include routing,
source labels, walking, coverage, and reduction, not only population projection.

The range profile has 2,563 routed stack samples. Schedule lookup accounts for
30.28%, heap removal for 6.28%, pending-event removal for 6.91%, and deferred
walking coverage for 6.83%. These are proportions within each profile, not
absolute speedup estimates. The full profiles retain stacks outside the range
kernel, including tile reduction and request setup.

The production source change adds 6,736 bytes across `Reachability.jl`,
`population_packed.jl`, and `population_range.jl`. This is source size, including
comments and whitespace, not compiled machine-code size. There are no new
dependencies or public query options.

Raw logs and profiles are under `/tmp/opencode/population-queue.log` and
`/tmp/opencode/population-cpu-frozen/`.

## Reproduction

Run `benchmark-population-queue.jl` with `julia --threads=8,1 --project=router`.
Set `POP_SNAPSHOT` to a clean worktree at `fad4b3b`. Set `POP_ARTIFACTS` to a
temporary directory and `POP_SERVER_PID` to the live server PID to monitor.
The loader reports the path for its next command file. Include
`population-queue-comparison.jl` from that command file to compare the current
implementation with the frozen baseline without another graph load.
After the comparison, use a command file containing `exit()` to stop only the
benchmark process. Do not run graph-loading tests during timing trials.
