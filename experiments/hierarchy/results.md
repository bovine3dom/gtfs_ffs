# Austria Hierarchy Results

## Decision

Keep the experimental [API and harness](README.md). Do not change production
defaults. Fine access profiles plus a coarse core give a clear runtime reduction,
but errors depend strongly on location, window mode, and origin set. Res6 is faster
and less accurate than res7 in these cases. Neither level is a drop-in replacement.

The later [res8 access control](access-results.md) isolates preprocessing without
extra coarsening. Its matched 1,027-origin gains were only 1.007x and 1.026x, with
model-equivalent values. Prefer the current production window path when no extra
coarsening is wanted. The res7/res6 tables below retain their own matched baselines.

The reference is the current res8 H3 model, not ground truth. Res8 already contains
approximation. All errors below measure the additional change from that model.
No error tolerance was selected for production use. No res5 core was tested:
res7 and res6 already showed the relevant speed and error trade-off.

## Method

Measured on 2026-09-12 with Julia 1.12.7, eight threads, and an Intel Xeon E3-1275
v6 at 3.80 GHz. One benchmark process loaded the large graph. The user server was
not changed or stopped. The host was not isolated. Raw trials record other host
CPU activity, process CPU time, allocation, GC time, and compilation time.

Input was `data/austria_adjacent_res8.arrow`, without the fantasy shuttle. The
approved export excludes 3,660 ambiguous trip groups. It has 20,244 network cells,
61,796 directed edges, and 4,791,297 packed daily profile entries. Its SHA-256 is
`1f7868950158495ad72cfc47b2d1659ffc21329805de558ac312be2a2b0b1de7`.
The graph occupied 130,902,128 bytes by `Base.summarysize`.

`data/kontur_h3.arrow` had 32,957,699 res8 cells and total weight 8,031,924,024.
Weights stayed Float64 at runtime. One shared fine walking index used a one-hour
limit. Fine population destinations were not replaced with parent totals.

The baseline called the current production `route_population` directly. It used
the array queue, label pruner, source classifier, range reuse, and prepared
schedule hints. It did not use a result cache or a historical GPU snapshot.
Each variant used fresh query workspaces. Query source assembly and allocation
were timed. Common input preparation and candidate profile preparation were not
in query times. Each case had one warm round and three measured rounds, with
orders `8/7/6`, `6/8/7`, `7/6/8`, and `8/7/6`. Full GC preceded each call.

Vienna used `881e15b467fffff`; Salzburg used `881f89af4dfffff`. Each normal city
index prepared a radius-19 disk with 1,141 fine origins. The same index served
radius-6 and radius-18 disks, new times and budgets, and a centre moved one cell.
The large Vienna index prepared all 9,919 requested origins before timing.
These are regional origin indexes with a whole-input core, not universal indexes.

The final code puts coarse schedule hints in an index-owned cache. This lifetime
fix followed the timing run; the run explicitly removed discarded cores from
the shared cache. It does not change profiles, search, or population reduction.
The two 1,027-origin intersection cases and all point checks were then repeated
with the final code. Each run loaded one fine graph; the runs did not overlap.

## Main Times

Median seconds. Departure is 08:00, budget is 3 h, walking limit is 1 h, and the
window has 96 samples at 15-minute intervals. Mode is `mean_intersection`.

| City | Fine origins | Res8 | Res7 core | Res6 core | Res7 speedup | Res6 speedup |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Vienna | 127 | 1.764 | 0.525 | 0.186 | 3.36x | 9.48x |
| Vienna | 1,027 | 9.867 | 2.949 | 1.185 | 3.35x | 8.33x |
| Salzburg | 127 | 1.119 | 0.403 | 0.173 | 2.77x | 6.47x |
| Salzburg | 1,027 | 6.453 | 2.415 | 1.147 | 2.67x | 5.63x |
| Vienna | 9,919 | 55.797 | 19.265 | 9.237 | 2.90x | 6.04x |

The 9,919-origin medians are not interactive latency. Preprocessing does not make
the entire large case instantaneous. Smaller prepared subsets remain usable.

The final cache-ownership check gave these 1,027-origin medians. It used the same
warm round and three measured rounds. Population error statistics and index byte
counts were unchanged. All measured calls had zero compilation and GC time.

| City | Res8 s | Res7 s | Res6 s | Res7 speedup | Res6 speedup |
| --- | ---: | ---: | ---: | ---: | ---: |
| Vienna | 9.966 | 2.996 | 1.235 | 3.33x | 8.07x |
| Salzburg | 6.544 | 2.520 | 1.170 | 2.60x | 5.60x |

Final-check query allocations were about 150.8/76.2/42.3 MB for Vienna and
79.5/46.1/30.2 MB for Salzburg, in res8/res7/res6 order. Other host CPU use was
about 0.42 to 0.88 cores during the measured calls. This is not an isolated-host
performance bound. Final-check preparation took 2.99/2.86 s for Vienna and
1.27/1.10 s for Salzburg, in res7/res6 order. The full matrix below retains the
first run's matched results, including the 9,919-origin measurements.

## Main Errors

For origin `i`, let `F[i]` be the res8 population and `C[i]` be the candidate
population. Relative error is `(C[i] - F[i]) / F[i]`, only where `F[i] > 0`.
WMAE is `sum(abs(C - F)) / sum(F)`. It weights each relative error by its reference
population. It is not the unweighted mean of per-origin percentages.

All table entries below are percentages. These are the same cases as Main Times.
The p50, p95, and maximum describe individual origin errors, not timing variation.

| City | Origins | Core | WMAE | p50 | p95 | Maximum |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Vienna | 127 | 7 | 3.77 | 3.72 | 5.03 | 6.36 |
| Vienna | 127 | 6 | 9.98 | 9.75 | 11.82 | 12.49 |
| Vienna | 1,027 | 7 | 5.28 | 4.16 | 8.11 | 3,493.21 |
| Vienna | 1,027 | 6 | 14.64 | 11.61 | 24.75 | 9,806.97 |
| Salzburg | 127 | 7 | 9.07 | 7.59 | 18.66 | 27.54 |
| Salzburg | 127 | 6 | 31.59 | 31.19 | 49.94 | 65.45 |
| Salzburg | 1,027 | 7 | 24.25 | 12.41 | 281.96 | 701.10 |
| Salzburg | 1,027 | 6 | 85.51 | 44.88 | 1,339.46 | 4,968.98 |
| Vienna | 9,919 | 7 | 16.37 | 0.00 | 71.64 | 3,551.73 |
| Vienna | 9,919 | 6 | 57.58 | 0.00 | 1,055.81 | 18,153.44 |

A zero median does not mean that the large index has small errors. Its tail is
large. Do not use a city-wide mean or rank correlation as an error bound.

Mean population per origin and absolute bias give a second view of these errors:

| City | Origins | Fine mean | Res7 mean | Res7 bias/MAE | Res6 mean | Res6 bias/MAE |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Vienna | 1,027 | 2,149,219 | 2,262,708 | +113,489 | 2,463,957 | +314,738 |
| Salzburg | 1,027 | 171,198 | 212,712 | +41,514 | 317,596 | +146,397 |
| Vienna | 9,919 | 376,026 | 437,584 | +61,559 | 592,558 | +216,532 |

The measured errors were nonnegative, so signed bias and MAE were equal. There
were no zero reference totals in these main cases. The CSV keeps zero-reference
counts and their maximum absolute error separate from relative errors. Unit tests
cover zero populations and very small positive weights.

## Other Queries

These queries reuse the 1,141-origin city indexes. Times are median seconds.
`I` means `mean_intersection`; `R` means the sample-weighted `reachable_union`.
The 23:30, 6 h cases exclude the fine origin's own population.

| City | Origins | Start | Budget h | Samples | Mode | Res8 | Res7 | Res6 | Res7 WMAE % | Res6 WMAE % |
| --- | ---: | --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| Vienna | 127 | 08:00 | 3 | 1 | I | 0.106 | 0.0151 | 0.00570 | 13.42 | 21.25 |
| Vienna | 127 | 08:00 | 3 | 4 | I | 0.421 | 0.0444 | 0.0126 | 13.58 | 27.04 |
| Vienna | 127 | 20:00 | 3 | 96 | R | 1.700 | 0.486 | 0.179 | 9.54 | 20.77 |
| Vienna | 127 | 23:30 | 6 | 96 | I | 2.957 | 0.839 | 0.305 | 4.82 | 17.24 |
| Vienna | 1,027 | 08:00 | 3 | 96 | R | 9.608 | 2.809 | 1.076 | 14.40 | 29.88 |
| Vienna moved | 1,027 | 23:30 | 3 | 4 | R | 1.125 | 0.0971 | 0.0292 | 11.72 | 28.02 |
| Salzburg | 127 | 08:00 | 3 | 1 | I | 0.0682 | 0.0113 | 0.00617 | 43.11 | 70.36 |
| Salzburg | 127 | 08:00 | 3 | 4 | I | 0.177 | 0.0384 | 0.0158 | 46.97 | 91.20 |
| Salzburg | 127 | 20:00 | 3 | 96 | R | 1.166 | 0.411 | 0.181 | 41.68 | 72.29 |
| Salzburg | 127 | 23:30 | 6 | 96 | I | 3.139 | 0.979 | 0.357 | 13.21 | 36.51 |
| Salzburg | 1,027 | 08:00 | 3 | 96 | R | 6.318 | 2.384 | 1.092 | 36.99 | 100.78 |
| Salzburg moved | 1,027 | 23:30 | 3 | 4 | R | 0.253 | 0.0207 | 0.00985 | 14.98 | 61.08 |

The large Vienna index also served `reachable_union`: res8 took 56.689 s, res7
took 18.508 s, and res6 took 8.957 s. WMAE was 15.73% and 41.91%. Res7 per-origin
p50/p95/maximum errors were 15.45%/29.17%/180.83%; res6 errors were
41.84%/80.14%/1,759.41%.

The profile index is independent of departure, budget, and mode. These results
also show that an acceptable error in one mode cannot be assumed in another.

## Local Variation

The implementation does not assign one total to every fine origin in a parent.
For the 1,027-origin intersection cases, all 162 res7 parents with varying fine
values still had varying candidate values in each city. Within-parent Pearson
correlation was 0.953 for Vienna and 0.836 for Salzburg. Res6 values were 0.843
and 0.679. Correlation uses deviations from each parent's mean.

This is not a guarantee of local ranking. For Vienna's 127-origin point case,
res7 within-parent correlation fell to 0.093. Only 16 candidate parent groups
varied, compared with 22 reference groups. The initial fine access constraint is
preserved, but coarse transfers can still remove later route differences.

## Preparation

The core covers the complete fine input. Core counts do not depend on the origin
cohort. The following output counts are directed, deduplicated CSR entries.

| Item | Res7 core | Res6 core |
| --- | ---: | ---: |
| Core nodes | 8,204 | 2,146 |
| Transit edges, including self-edges | 31,832 | 10,494 |
| Packed core profile entries | 3,051,398 | 1,752,692 |
| Projected network walks | 105,814 | 12,041 |
| Fine population output walks | 877,977 | 352,862 |

Index MB below use decimal bytes. They include the combined graph, fine output
CSR, direct walks, origin map, and coarse schedule hints. They exclude the shared
fine graph, fine walking index, population input, and global fine weight map.
They are additional storage, not total process memory.

| Cohort | Origins | Core | Prepare s | Access phase s | Source profile entries | Index MB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Vienna | 1,141 | 7 | 2.490 | 1.562 | 14,688,982 | 179.69 |
| Vienna | 1,141 | 6 | 2.292 | 1.587 | 5,198,693 | 76.65 |
| Salzburg | 1,141 | 7 | 1.182 | 0.322 | 5,695,204 | 116.02 |
| Salzburg | 1,141 | 6 | 1.041 | 0.292 | 2,537,271 | 46.96 |
| Vienna large | 9,919 | 7 | 5.207 | 3.688 | 29,509,253 | 344.15 |
| Vienna large | 9,919 | 6 | 5.247 | 4.015 | 14,126,281 | 183.24 |

The large combined graphs have 18,123 and 12,065 nodes before the A/E split.
Their output suffix has 97,682 unique populated fine cells. The 9,919 virtual
sources have no incoming edges or outgoing walking edges. Other prepared origins
do not become transfer nodes during a query.

Common preparation took 4.366 s for graph packing, 1.401 s for fine walking,
2.413 s for population loading, and 6.104 s for fine population alignment and its
global weight map. These startup figures include compilation in those stages.
Fine walking occupied 81.27 MB and prepared population occupied 22.45 MB; some
arrays are shared, so do not add object sizes to estimate process RSS.
Peak process RSS for the full run was 6,886,789,120 bytes, or 6.41 GiB. This
includes shared inputs, candidate indexes, compilation, and query workspaces.

For a fixed cohort, estimated break-even query count is
`ceil(prepare_seconds / (fine_query_seconds - coarse_query_seconds))`.
Using the matched medians, both levels amortize preparation in one 1,027-origin
or 9,919-origin, 96-sample query. Vienna's 127-origin, 96-sample case needs about
three res7 queries or two res6 queries. Its 127-origin point case needs about 28
res7 queries or 23 res6 queries. These estimates exclude common startup, do not
include result-cache hits, and do not treat work for a new region as free.

## Verification

The final experimental suite passed 7,069 assertions at both one and eight
threads. It includes 200 seeded random graphs and the cache-ownership check.

The tests compare res8-core coverage with the current fine model and compare
coarse coverage with an independent label-correction implementation. Random
fixtures also check the core's A and E labels directly. The tests cover fine
first boarding, off-network origins, self-legs, duplicate egress, midnight,
seven-day budgets, time limits near UInt32 INF, all six modes, partial tiles,
zero weights, and exact fine-origin exclusion with a `2e-30` remaining weight.

On the full Austria graph, 16 fine origins per city were checked at 08:00 and
23:30 with 3 h and 6 h budgets. That is 128 point comparisons per core level,
including res8 as an adapter control. Res8 had no false positives or negatives.
Res7 and res6 had no observed false negatives among populated fine destinations.
They had positive false-positive counts and population weight, recorded in
`coverage.csv`. Those counts are origin-destination incidences, not unique cells
across all origins. These finite checks are not a proof of an upper bound for all
possible inputs.

The seven original population test files passed 11,238 assertions at eight
threads. No production file changed. A full router or browser suite was not run.

## Next Choice

The choices are current production res8, res8 access preprocessing without extra
coarsening, and explicitly approximate res7/res6 cores. The access-only control
did not establish a material many-origin window gain and needs extra index memory.
Res7 is less optimistic than res6, while res6 gives more speed and larger errors.
The next decision is whether to expose an approximate mode for selected regions
and error trade-offs. The API already permits all three core resolutions without
changing the fine-origin output contract. No new choice is enabled by default.

Raw evidence is in [results/metadata.txt](results/metadata.txt),
[trials.csv](results/trials.csv), [quality.csv](results/quality.csv), and
[coverage.csv](results/coverage.csv). Source hashes identify the unchanged
production engine used for the baseline and candidate kernels.
The post-fix evidence is in [final-check/metadata.txt](final-check/metadata.txt),
[trials.csv](final-check/trials.csv), [quality.csv](final-check/quality.csv), and
[coverage.csv](final-check/coverage.csv).

## Transfer Compilation (Closed)

The final experiment replaced core walking edges with walk-plus-next-transit profiles.
Initial access and final walking were unchanged. The option and its tests were
removed after regressions.

The run used one Austria load and eight threads, without a shuttle or result cache.
Each city prepared 1,141 origins. Times are medians of three warmed alternating
pairs for `mean_intersection` at 08:00. Each query used a 3 h budget, 1 h walking,
and 96 samples at 15-minute steps.

| City, 1,027 origins | Production s | Compiled s | CPU s, production/compiled | Query MB, production/compiled |
| --- | ---: | ---: | ---: | ---: |
| Vienna | 10.056 | 32.579 | 73.423 / 236.224 | 150.78 / 151.53 |
| Salzburg | 6.800 | 20.397 | 45.475 / 140.538 | 78.61 / 74.51 |

Core edges grew from 61,796 to 927,451. Profiles grew from 4,791,297 to 118,072,279.
All 638,524 runtime network walks were removed. The denser transit graph was slower,
despite fewer expansions. Vienna/Salzburg preparation took 11.022/7.765 s and
allocated 17.236/13.087 GB cumulatively. The indexes retained 1.826/1.266 GB of
additional data. Peak process RSS was 10.812 GB. The preparation check counted
144,453,990 raw core events.

The matched res7 check reduced WMAE only from 5.280% to 5.186%. Latency rose from
3.093 to 4.837 s. Index storage rose from 179.69 to 334.94 MB. No 9,919-origin or
res6 run followed.

Before removal, 14,593 assertions passed at both one and eight threads. Tests
covered transit resets, no double walking, empty local population, midnight, short
budgets, and all six modes. All res8 timed values matched exactly. The 128 full-graph
point checks had zero false positives or negatives. This checks the res8 model,
not ground truth.
