# Shared-host Paired Trials Appendix

The earlier observations remain in the original report as history. They used an
idle gate and do not support the decisions here. This appendix uses new trials.

For the current adaptive engine and retained graph-specific schedule bounds,
see the [three-experiment report](population-approved3-results.md). The measurements
below remain the record of the earlier engine and candidate decisions.

## Method

These are measured shared-host trials, not isolated or idle-host trials. The
baseline is frozen commit `7b7036f900e47cd4056f124c324abbeeaf6f748a`. One Julia
1.12.7 process shares the resolution-7 graph and prepared population vectors
between modules. It uses eight routing threads and one observer thread on the
four-core, eight-thread Xeon E3-1275 v6. User applications remain running. No live
queries, process suspension, external load generation, or input changes occur.

Each case has a full warmup for every variant. Odd pairs use the listed order;
even pairs reverse it. Full garbage collection occurs before each call, outside
the measured interval. The runner retains every measured pair. It records wall
time, process CPU seconds, allocation, sampled RSS, and external busy cores.
External busy cores equal aggregate busy CPU seconds minus benchmark-process
CPU seconds, divided by the observation interval. The counters have 10 ms
resolution. Short calls therefore have more counter uncertainty.

Ratios compare matched calls, not independent medians. A ratio above 1 favors
the candidate. SD is the sample standard deviation of the paired ratios. CPU
ratios measure process CPU work under the observed load. They do not isolate
algorithm cost: shared load can also change cache behavior and scheduling.
No result predicts an ideal-host speed.

All requests use 96 samples, a 15-minute step, and a one-hour walking limit.
The journey budget is three hours, except the stated 168-hour diagnostic.
The timed mode is `mean_intersection`, with own-origin population included.
Each result comparison requires identical H3 arrays and zero masks, with
`rtol=1e-12` and `atol=1e-6` for values. Separate checks retain all six output
selectors with both origin-exclusion settings.

## Static Walking

Wall and CPU columns are medians in seconds. External load covers both variants.

| Region | Origins | Pairs | Baseline wall | Fast wall | Baseline CPU | Fast CPU | Paired wall ratio: median [range], SD | CPU ratio | External cores |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | --- |
| Paris | 1,027 | 5 | 17.427 | 17.503 | 123.48 | 123.28 | 1.000 [0.985, 1.014], 0.011 | 1.000 | 0.63-0.93 |
| Paris | 9,919 | 3 | 43.567 | 43.211 | 310.59 | 311.56 | 1.004 [0.982, 1.060], 0.040 | 0.997 | 0.45-0.92 |
| Rural France | 1,027 | 5 | 0.330 | 0.274 | 1.35 | 1.14 | 1.226 [0.833, 1.454], 0.239 | 1.195 | 0.80-1.31 |
| Rural France | 9,919 | 3 | 3.039 | 2.916 | 17.84 | 16.95 | 1.036 [1.026, 1.051], 0.013 | 1.053 | 0.86-0.92 |
| Chad | 1,027 | 3 | 0.268 | 0.078 | 0.89 | 0.07 | 3.747 [2.577, 3.761], 0.680 | 12.714 | 0.80-1.82 |
| Chad | 9,919 | 3 | 1.771 | 0.805 | 6.88 | 0.80 | 2.267 [2.099, 2.387], 0.145 | 8.753 | 1.46-2.15 |

Paris shows no clear change. The short rural case has large pair variation and
one reversal. The larger rural case has a small consistent gain in three pairs.
Both Chad cases show a gain in every pair, above the observed pair variation.
This supports a measured shared-host win for static-only requests, not a
general speedup claim. For Chad with 9,919 origins, median request allocation
falls from 668.02 MB to 42.70 MB. The fast path uses no routing workers there.

## Expiry Candidate

`population-expiry.jl` is experimental. It uses the production A/E range repair
kernel, but credits P only after a valid pop. Per-origin P times decrease
monotonically. Seen masks and touched IDs avoid a full P-matrix reset per tile.
Direct walking coverage has time zero and remains valid for all samples.

Lazy bucket records share an origin mask for equal new P times. A record is
created only when expiry changes. Repair runs before expiry. Current P times
identify stale records. Per-origin active totals, union totals, and intersection
totals replace sample-mask history and repeated population scans. Intersection
membership can only decrease. Cell counts give exact empty totals; a nonpositive
sum with active cells triggers a recomputation without an epsilon threshold.

The first Paris case emits 88,485,339 records per request. Its largest tile emits
1,905,483 records. Each record has 24 payload bytes across three vectors; vector
capacity and headers add storage. The event vectors are reused between tiles.
Request allocation and sampled RSS include all other routing storage.

| Paris origins / budget | Pairs | Fast wall s | Expiry wall s | Fast CPU s | Expiry CPU s | Paired wall ratio: median [range], SD | CPU ratio | Fast / expiry allocation MB | External cores |
| --- | ---: | ---: | ---: | ---: | ---: | --- | ---: | --- | --- |
| 1,027 / 3 h | 3 | 16.729 | 22.254 | 123.35 | 155.66 | 0.778 [0.747, 0.778], 0.018 | 0.795 | 666.78 / 2,546.74 | 0.38-0.88 |
| 127 / 168 h | 3 | 39.762 | 37.224 | 279.08 | 266.21 | 1.068 [1.047, 1.070], 0.013 | 1.047 | 1,430.90 / 1,735.49 | 0.56-0.78 |

The long diagnostic emits only 200 records per request, with a maximum of 30 in
one tile. It shows a consistent wall and process-CPU gain in three pairs. This
is evidence for the expiry approach under this measured condition, not a failed
hypothesis. However, the three-hour case is slower in every pair and allocates
3.82 times as much memory. Keep the candidate experimental. A production policy
cannot be based on one long-budget case. Do not extrapolate the diagnostic to
10,000 origins or to an idle host.

## Schedule Hints

Each row compares the same paired no-hint call with one hint table. CPU time is
the whole request's process CPU time, not isolated lookup CPU time. No hardware
instruction or cache counters were measured. The earlier exact probe counts
remain lookup-sample results, not routed-request instruction counts.

| Paris origins / budget | Bins | Pairs | Fast / hinted wall s | Fast / hinted CPU s | Paired wall ratio: median [range], SD | CPU ratio | External cores |
| --- | ---: | ---: | --- | --- | --- | ---: | --- |
| 1,027 / 3 h | 8 | 5 | 17.273 / 15.870 | 123.45 / 113.50 | 1.070 [1.056, 1.094], 0.016 | 1.087 | 0.45-0.94 |
| 1,027 / 3 h | 24 | 5 | 17.273 / 15.929 | 123.45 / 112.15 | 1.084 [1.003, 1.097], 0.040 | 1.099 | 0.45-1.12 |
| 127 / 168 h | 8 | 3 | 39.767 / 38.226 | 277.74 / 274.36 | 1.056 [1.005, 1.064], 0.032 | 1.015 | 0.46-0.87 |
| 127 / 168 h | 24 | 3 | 39.767 / 37.925 | 277.74 / 274.36 | 1.055 [1.033, 1.064], 0.016 | 1.012 | 0.55-0.86 |

Eight bins give a consistent modest gain in the three-hour case, above its pair
variation. The long case has only a 1.5% median process-CPU ratio gain; one wall
pair gains less than 0.5%. For 24 bins, the long-case baseline has more external
load in every pair. Do not attribute the full wall difference to the lookup.

The tables require 47,097,024 bytes for 8 bins and 141,291,072 bytes for 24 bins,
outside request allocations. Their observed build times are 0.291 and 0.440 s.
Keep production without hints. The evidence supports a limited shared-host
gain, but not a uniform gain clearly above variation across these conditions.
It also does not establish a benefit from the extra 94.19 MB for 24 bins.

## Origin Tiles

The reference in each row is the explicit 16-origin fast path. Each group uses
five pairs for 1,027 origins or three pairs for 9,919 origins. These trials compare
32 and 64 origins with 16; they do not repeat the old 8-origin or worker-count
sweeps. All variants use the same eight-thread process.

| Region / origins | Tile | Reference / candidate wall s | Reference / candidate CPU s | Paired wall ratio: median [range], SD | CPU ratio | Allocation ratio | External cores |
| --- | ---: | --- | --- | --- | ---: | ---: | --- |
| Paris / 1,027 | 32 | 17.267 / 13.476 | 123.53 / 92.94 | 1.300 [1.279, 1.319], 0.019 | 1.327 | 1.57 | 0.52-1.10 |
| Paris / 1,027 | 64 | 17.267 / 10.423 | 123.53 / 73.18 | 1.665 [1.618, 1.763], 0.054 | 1.688 | 2.72 | 0.42-1.10 |
| Paris / 9,919 | 32 | 44.042 / 35.801 | 311.75 / 255.61 | 1.222 [1.208, 1.231], 0.011 | 1.217 | 1.53 | 0.65-0.83 |
| Paris / 9,919 | 64 | 44.042 / 30.190 | 311.75 / 216.67 | 1.460 [1.442, 1.481], 0.019 | 1.434 | 2.59 | 0.44-0.83 |
| Rural / 1,027 | 32 | 0.253 / 0.265 | 1.17 / 1.36 | 0.858 [0.636, 1.077], 0.162 | 0.850 | 1.63 | 0.68-1.04 |
| Rural / 1,027 | 64 | 0.253 / 0.323 | 1.17 / 1.79 | 0.758 [0.604, 0.915], 0.116 | 0.646 | 2.88 | 0.71-1.16 |
| Rural / 9,919 | 32 | 2.907 / 2.751 | 16.93 / 15.83 | 1.049 [1.040, 1.065], 0.013 | 1.065 | 1.59 | 0.89-1.00 |
| Rural / 9,919 | 64 | 2.907 / 2.814 | 16.93 / 15.91 | 1.029 [1.028, 1.034], 0.003 | 1.064 | 2.76 | 0.89-1.05 |

The Paris gains are consistent and clearly above pair variation. They are valid
shared-host gains. But the smaller rural case loses process CPU performance in
every pair at both larger tile sizes. Tile 64 also loses wall performance in
every pair there. Allocation increases for all larger tiles.

### Current Decision

Retain an adaptive default after static-origin classification:

```julia
default_tile = (samples == 1 || (samples > 4 && length(heavy) >= 128 * Threads.nthreads(:default))) ? 64 : 16
```

Here, `heavy` contains only origins with possible transit access. Windows with two
to four samples retain the existing 16-origin, one-block search. For longer windows,
the threshold retains at least two full 64-origin tiles per worker. Eight threads
require `2 * 64 * 8 = 1024` transit origins, not total origins. This is a
parallelism criterion, not a density label assigned after measurement.

| Case | Transit origins | Default on eight threads | Paired tile-64 wall ratio |
| --- | ---: | ---: | ---: |
| Paris / 1,027 | 1,027 | 64 | 1.665 |
| Paris / 9,919 | 9,412 | 64 | 1.460 |
| Rural / 1,027 | 813 | 16 | 0.758 |
| Rural / 9,919 | 7,987 | 64 | 1.029 |

The rule retains the Paris gains and the small larger-rural gain. It keeps tile
16 for the smaller rural case, where tile 64 was slower. The point default
remains 64. The internal `origin_batch_size` override remains available for
benchmarks. The 64-lane mask limits each block, not the request's origin count.

The adaptive code was added after the paired trials. The table records explicit
tile choices with equivalent configurations, not new automatic-selection timings.
Tile 64 needs more workspace RAM: observed request allocation was 2.6-2.9 times
the tile-16 allocation on eight threads. The criterion does not guarantee the
best performance on all machines. Expiry and eight-bin hints remain experimental.

The maximum sampled RSS across timed requests is 11,625,746,432 bytes. RSS
includes the shared graph, all hint tables, and retained allocator pages. It is
not the private workspace size. The expiry candidate's maximum sampled RSS is
10,667,204,608 bytes. Loading peaks at 23,317,147,648 sampled bytes and changes
system swap use; loading is not part of the paired request intervals.

## Verification

All 128 measured calls pass parity. The final 1,027-origin Paris checks pass all
six selectors with both exclusion settings against the frozen baseline. All 36
baseline, fast, and expiry selector-output arrays are retained, including zeros.

The final focused suite passes 2,440 assertions with one thread and with eight
threads. It includes forward point routing for every sample, zero cycles and
budgets, day boundaries, large absolute times near `INF`, same-sample expiry
postponement, expiry and reactivation,
tiny positive weights, per-origin exclusion, direct walking, and full 64-origin
masks with a partial final tile. Tests require exact zero masks and use no
absolute tolerance for the new forward-oracle and full-width cases.

No production source changed during the paired trials. The full production suites
were not repeated in that measurement phase; the original report retains their
earlier results. Final focused tests ran after all paired timing calls, not during
those calls. Adaptive-default validation is recorded separately below.

### Adaptive Default Validation

The final selection rule also retains the one-block path for two- to four-sample
windows. The full eight-thread suite passes 100,275 assertions. A focused one-thread
run passes 3,271 assertions, including all 76 adaptive-selection checks.

After the adaptive change, the full production suite passes 100,239 assertions
with one thread and 100,239 with eight threads. Each run has a 600-second limit;
the runs complete in 377.6 and 361.6 seconds, respectively. They run sequentially,
without a concurrent benchmark. These are test durations, not routing benchmarks.

The 40 new assertions use a resolution-7 disk with 1,027 origins and known
resolution-8 population weights. A zero-duration cycle gives different shared
expansion counts for explicit tiles 16 and 64. Default results match the selected
explicit tile, including worker and expansion counts. Tests check the transit
threshold and one origin below it with the same total origin count. They also
check all origins with transit access, point queries, 96-sample windows, partial
final tiles, exact zero masks, and independently calculated population values.

## Artifacts

The complete log and serialized result arrays are in
`/tmp/opencode/population-shared-20260910/`. `shared-rows.jls` retains every
measurement. Files named by case, variant, and pair retain output arrays.
`summary.log` includes CPU-ratio SD and ranges for every comparison. Rebuild it
with `population-shared-summary.jl <artifact-directory>`.
`truth-<mode>-<exclude>.jls` retains frozen-baseline selector outputs. Matching
`fast-...` and `expiry-...` files retain candidate outputs. The log records source
hashes, input hashes, pair order, CPU load, allocation, and sampled RSS.

Use the resident loader from the main report with an empty `POP_ARTIFACTS`
directory. Its first command must include `population-shared-trials.jl`.
Do not include the old validation-only phase. That phase retains historical
rejection labels and is not the shared-host measurement method.
