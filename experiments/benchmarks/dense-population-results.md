# Dense Population CPU Results

## Decision

Retain array-based pending masks and an exact target-label check in range repair.
Keep the binary heap, independent origin labels, and current tile selection.
Do not retain the maximum-label table. The changes do not meet the 5-10x goal.
They keep the packed, static-walking, fallback, and result-cache algorithms.

The runtime patch adds 24 lines and removes five lines. It adds no dependency.
Each range workspace has two new arrays. Other workspaces have empty arrays.

## Final Times

Times are medians in seconds. The ratio is the median of three matched
baseline/retained ratios, not the ratio of the two time medians. A ratio above
one is better. Brackets show the minimum and maximum paired ratios.

| City / Origins / Samples / Budget h | Baseline / Retained Wall s | Baseline / Retained CPU s | Paired Wall Ratio [Range] |
| --- | --- | --- | --- |
| Paris / 1,027 / 96 / 3 | 9.770 / 8.143 | 67.53 / 55.83 | 1.200 [1.192, 1.216] |
| Paris / 9,919 / 96 / 3 | 28.402 / 23.000 | 200.27 / 161.65 | 1.207 [1.185, 1.237] |
| Paris / 127 / 96 / 168 | 38.500 / 32.410 | 274.46 / 232.89 | 1.179 [1.164, 1.188] |
| Paris / 1,027 / 96 / 12 | 165.349 / 140.103 | 1159.38 / 997.24 | 1.180 [1.149, 1.192] |
| London / 1,027 / 96 / 3 | 19.361 / 15.018 | 133.27 / 102.34 | 1.287 [1.261, 1.303] |
| Rural / 1,027 / 96 / 3 | 0.2238 / 0.2281 | 1.007 / 1.057 | 0.958 [0.933, 1.050] |
| Chad / 9,919 / 96 / 3 | 0.8038 / 0.7876 | 0.802 / 0.786 | 1.021 [0.934, 1.060] |
| Paris / 1,027 / 1 / 3 | 0.2635 / 0.2630 | 1.518 / 1.521 | 1.002 [0.998, 1.011] |
| Paris / 1,027 / 2 / 3 | 0.6830 / 0.6745 | 4.670 / 4.610 | 1.013 [1.009, 1.018] |
| Paris / 1,027 / 4 / 3 | 1.1341 / 1.1319 | 7.801 / 7.819 | 0.997 [0.993, 1.030] |
| Rural / 1,027 / 1 / 3 | 0.0803 / 0.0772 | 0.088 / 0.086 | 1.030 [0.973, 1.090] |
| Rural / 1,027 / 2 / 3 | 0.0817 / 0.0810 | 0.097 / 0.096 | 0.948 [0.922, 1.168] |
| Rural / 1,027 / 4 / 3 | 0.0823 / 0.0855 | 0.103 / 0.107 | 0.963 [0.744, 1.236] |

The five main cases had gains in every pair. The 12-hour case took more than
two minutes per call. No 9,919-origin long-budget query was run.

Do not claim a rural gain. The 96-sample rural case lost 4.2% in paired wall
ratio and used more memory. Short controls had substantial variation. The
static-only Chad case had identical allocation and no routing expansions.
Point and short-window routing did not change. These controls do not support
a new performance policy.

## Method

The frozen baseline is commit `80069cb148caf71a63159c8da56176675e630e5c`.
The source was saved with `git archive` before the experiments.
Measurements on 2026-09-12 used Julia 1.12.7, eight routing threads, and a Xeon
E3-1275 v6 with four cores and eight hardware threads. User applications stayed active.
No user server or input file was changed. The host was not isolated.

One process loaded one resolution-7 graph. All variants shared its arrays,
the prepared one-hour walking index, population weights, and eight-bin schedule
hints. Module aliases let the frozen and current functions use the same types.
No graph conversion or second graph load was needed.

Each timed call ran `route_population` directly. It included source preparation,
static-origin classification, workspace allocation, routing, coverage, and
reduction. It excluded graph loading, index preparation, HTTP, and Arrow output.
No call used the population-result cache. Each call had fresh workspaces.
The shared prepared indexes were warm. Hardware caches were not cleared.

Queries started at 08:00. Samples were 15 minutes apart. The walking limit was
one hour. Timed queries used `mean_intersection` and included origin population.
Paris used `871fb4660ffffff`, rural France used `871f94d80ffffff`, and Chad used
`876bac79cffffff`. London used the resolution-7 cell at 51.5074, -0.1278.

Each final case had a warm call per variant and three alternating matched pairs.
Full GC ran before each call, outside the timer. GC during the call was included.
All measured pairs had zero compilation time. CPU time is process CPU time.
External CPU use is an estimate from Linux CPU counters, not a host-isolation test.
All pairs remain in [the CSV](dense-population-trials.csv); pair zero is warmup.
The CSV has 104 rows: 26 warm calls and 78 measured calls. External CPU use in
measured calls longer than one second ranged from 0.42 to 1.06 cores, with a
median of 0.79 cores. No pair was removed for external load.

## Memory And Startup

Allocation is not resident memory. These are median allocated decimal MB per
complete query, including fresh workspaces.

| Case | Baseline MB | Retained MB |
| --- | ---: | ---: |
| Paris 1,027 / 96 / 3 h | 1810.005 | 1870.890 |
| Paris 9,919 / 96 / 3 h | 1856.974 | 1916.799 |
| Paris 127 / 96 / 168 h | 1402.574 | 1389.167 |
| Paris 1,027 / 96 / 12 h | 2364.644 | 2251.777 |
| London 1,027 / 96 / 3 h | 1849.810 | 1886.681 |
| Rural 1,027 / 96 / 3 h | 619.398 | 691.620 |
| Static-only Chad 9,919 / 96 / 3 h | 42.090 | 42.090 |

The two arrays use 9,103,320 bytes per range workspace, or 72,826,560 bytes for
eight workers. A 64-origin label matrix still uses 194,204,160 bytes per worker.
Removing pending dictionaries offsets part or all of the added array allocation,
depending on the case. No resident graph index was added.

Allocation profiling of one warmed 64-origin, 96-sample tile recorded 871
allocations and 162,016 bytes. These were tile/block setup allocations, not
range-kernel allocations. This diagnostic reused a workspace; the complete-query
benchmarks did not. The focused range-kernel allocation test still passed at
zero bytes.

Maximum process RSS during loading was 22.245 GB. The largest post-call RSS in
the final matrix was 10.737 GB. This includes all resident variants and diagnostic
buffers. It is not an isolated production footprint or a sampled query peak.
GC during a measured call never exceeded 0.008 seconds.

Graph packing took 115.179 s. Walking preparation took 7.721 s, population loading
2.173 s, population preparation 2.496 s, and schedule-hint preparation 0.185 s.
These stages include compilation. The first baseline query took 12.332 s.
The first final retained-module query took 9.506 s, but common code was already
compiled. These are not comparable clean-process cold-start times. Final pairs
used warm code and indexes, with no result-cache lookup.

## Verification

The focused population suite passed 9,173 assertions at one thread and at eight
threads. The full eight-thread suite passed 106,356 assertions in 6 min 26 s.
The benchmark script also passed a small-graph test. New tests cover same-time
origin arrivals after a pop, stale keys, zero-time cycles, partial tiles, queue
reset, and the reserved time boundary.
They compare exact coverage masks with the raw engine and transit labels with
the point oracle. A range-cache test checks that overflow publishes no result.
Existing tests cover fractional weights, empty and zero populations, unprepared
and larger-radius fallback, all modes, exclusions, and joined worker failures.

All 104 final calls had exact H3 arrays, values, and expansion counts. All 16
additional full-network mode/exclusion comparisons had exact values and zero
masks; maximum absolute error was zero. They covered all six modes at 127
origins and mean/weighted modes at 1,027 origins. The required tolerance was
`rtol=1e-12, atol=1e-6`. Eight full-network sample blocks also had exact A/E labels
and reached masks: four at 64 origins and 3 h, and four at 16 origins and 168 h.

## Correctness

A is the transit-ready label. E is the walk-eligible label. Both remain separate
for each origin. A pending bit refers to its current label in these arrays.
Every strict label decrease has a heap key at the new time. A matching last key
can accept more bits without another push. Other keys can remain in the heap.

At a pop, only pending bits whose labels equal the key time are removed and
processed. Old keys cannot remove bits at another time. The last-key marker is
cleared before relaxation. Thus a zero-time edge can add a new origin after a
pop at the same time. The drained queue leaves no pending bits between samples.

The target check runs before schedule lookup. It skips an edge only if every
origin bit already has A and relevant E labels at or before the current time.
Travel durations are nonnegative, so no arrival can strictly improve these
labels. E is ignored only when the existing minimum-walk and request-deadline
check proves that no useful walk can follow. An unreached `INF` label prevents
this skip. No destination-specific bound or approximate cutoff is used.

## Experiments

The fresh baseline profile had 11,739 routing-worker samples. Schedule lookup
accounted for 2,992 samples, pending-dictionary pop for 1,048, and heap pop for
1,002. These are inclusive call counts within the routing worker, not hardware
instruction counters.

Two 64-origin Paris tiles, with 96 samples and a three-hour budget, had
47,305,806 enqueue attempts and 8,156,703 accepted updates. Of 7,021,096 heap
pops, 2,144,974 were stale. Accepted state expansions were unchanged by the
retained queue. The optimization removes hashing, not independent label work.

The broad target-bound probe kept `INF` for every unreached origin. It rebuilt
the maximum label after each sample. A profile minimum-duration bound could
skip only 218,812 of 20,626,319 schedule lookups in the short case. For two
16-origin tiles with a seven-day budget, it could skip 4,053,724 of 171,735,498.
After lookup, the broad bound rejected 40.8% of long-budget transit candidates.
The table still did not reduce CPU time.

| Screen | Array Queue CPU s | Queue And Masked Check CPU s | Queue And Maximum Table CPU s |
| --- | ---: | ---: | ---: |
| Paris 1,027 / 96 / 3 h, two-pair median | 58.67 | 55.34 | 59.60 |
| Paris 127 / 96 / 168 h, one pair | 240.25 | 230.70 | 254.42 |

The maximum table added another 24.28 MB across eight workers. It was slower
than the array queue in CPU time. No minimum-duration index or maximum-label
table was added to production. The masked check uses current labels and needs
no extra index.

For the same two dense tiles, the retained masked check skipped 9,102,294 of
20,626,319 schedule lookups, or 44.1%. It checked 78,598,423 origin lanes.
The skipped masks contained 66,354,256 origin bits. These counters include the
cost of finding a possible improvement; they are not an assumed speedup.

A bounded trace of the latest two samples in the first two dense tiles had
70,009 distinct `(time, state)` keys. Both tiles used 36,281 keys. No trace entry
was dropped at the 200,000-key limit. This shows duplicate work across tiles;
it does not prove an end-to-end gain from larger tiles. Larger cohorts were not
implemented. They still need a memory and worker-balance plan.

Prepared network walks use H3 order, not duration order. Only the prepared
population-output walks are duration-sorted. No early `break` was added to
network walking. SIMD, radix queues, trip patterns, and expiry were not reopened.

[Orionet](https://arxiv.org/html/2506.16488v3) and
[Early Pruning](https://arxiv.org/html/2603.12592v4) give research context only.
Orionet studies parallel point-to-point and batch queries on weighted graphs.
Early Pruning stops sorted transfer scans using a bound at a query destination.
Population queries need all reachable destinations, so these target bounds do
not apply directly. The retained check uses each edge's own A/E labels instead.
Neither paper's implementation was used. This report claims no published speedup.

## Inputs

Both input hashes were checked in this run.

| Input | Bytes | SHA-256 |
| --- | ---: | --- |
| `everything_res7.arrow` | 7,397,939,938 | `614ac1a6a5b87d35c6e83c057a64161005fb339e80a369f4065709e3d33309d7` |
| `kontur_h3.arrow` | 234,395,250 | `c21eaf6c3eb65563e80f2055347ad13f979014a190f8472817c25a591f427eb3` |

The loader used `skip_invalid_durations=true` and `badajoz_shuttle=true`.
The graph had 379,305 nodes, 1,471,782 edges, and 192,645,963 profile entries.
The population input had 32,957,699 rows. The prepared indexes had 3,737,032
network walks and 6,038,589 positive-population walks.

| Retained Source | SHA-256 |
| --- | --- |
| `population_packed.jl` | `8636defa35ca9b59696793721f7d555f77402d95e9e96f1c51e9dce286d88171` |
| `population_range.jl` | `f9c9df8c5e84d218bae927948f18f0d86cd273b5a811d77b9c66c9ea1fb6b5be` |

## Reproduction

Use [the benchmark](dense-population-benchmark.jl) with an extracted baseline
archive. Check the parent directory before you create the archive or output.
For the archive made in this run:

```sh
julia --threads=8 --project=router experiments/benchmarks/dense-population-benchmark.jl \
  /tmp/opencode/dense-population-80069cb /tmp/opencode/dense-population-repeat.csv
```

The script checks that shared source files match the baseline. It loads the
inputs once, runs the final workload matrix, and checks all six population modes
with and without origin exclusion. The script does not need the experiment
prototypes. The temporary source archive is not a repository input.

The local run log is `/tmp/opencode/dense-population-run.log`. Final test logs
are `/tmp/opencode/dense-population-focused-t1-final.log`,
`/tmp/opencode/dense-population-focused-t8-final.log`, and
`/tmp/opencode/dense-population-full-t8.log`. The prototype code was removed.
The benchmark process was stopped after verification; its graph was released.
