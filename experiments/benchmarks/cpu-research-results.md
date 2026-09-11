# CPU Research Decisions

## Scope

Retain packed population routing, backward range repair, static walking totals,
adaptive origin tiles, and eight-bin schedule bounds. Stop the closed CPU
prototype paths. These results do not measure the handler's population-result
cache. The current [population benchmark](benchmark-population.jl) and
[cache benchmark](benchmark-population-cache.jl) remain available.

Measurements used Julia 1.12.7 on a Xeon E3-1275 v6 with four cores, eight
hardware threads, and 62 GiB RAM. Later trials used eight routing threads and
one observer. User applications remained active. Shared-host results do not
predict isolated-host performance. No hardware instruction counters were collected.

## Inputs And Baselines

| Input | Rows | SHA-256 |
| --- | ---: | --- |
| `everything_res6.arrow` | 205,357,244 | `ffb1f5ebc8a43dcf3bb387667aa9d925af4c8363ead983c85ce4ecd19a3b550e` |
| `everything_res7.arrow` | 205,463,043 | `614ac1a6a5b87d35c6e83c057a64161005fb339e80a369f4065709e3d33309d7` |
| `everything_res8.arrow` | 205,591,459 | `eb0b3d7e26bed7535522a4ce34aa1f4439053676a3d99356e8887c1b86b06ada` |
| `kontur_h3.arrow` | 32,957,699 | `c21eaf6c3eb65563e80f2055347ad13f979014a190f8472817c25a591f427eb3` |

Resolution 6 had 117,777 nodes, 544,603 edges, and 100,385,985 profiles.
Resolution 7 had 379,305 nodes, 1,471,782 edges, and 192,645,963 profiles.
The resolution-7 loader excluded 88,936 invalid rows and added 2,342 shuttle
connections. Its one-hour index had 3,737,032 network walks and 6,038,589
positive-population walks. Population summed to 8,031,924,024 people.
Resolution 8 was checked for metadata and hash only in these CPU comparisons.

Each comparison shared one resident graph and prepared arrays between variants.
Request times exclude loading, preparation, HTTP, and Arrow encoding. Unless
specified, requests used 08:00, 96 samples at 15-minute intervals, a three-hour
budget, one-hour walking, `mean_intersection`, and included origin population.
Paris used `861fb4667ffffff` at resolution 6 and `871fb4660ffffff` at resolution 7.
Rural France used `871f94d80ffffff`; Chad used `876bac79cffffff`.

| Stage | Frozen baseline |
| --- | --- |
| Packed default-16 | `3980e5f00c5cdcc3a423b7009299fb90f377fd55` |
| Queue and range repair | `fad4b3b` |
| Static walking and explicit tile trials | `7b7036f900e47cd4056f124c324abbeeaf6f748a` |
| Eight-bin bounds | Adaptive engine at `ae70b1f633e68c9ce1df953b9363b78c8bb3159f` |
| SIMD follow-up | Retained eight-bin adaptive engine |

## Packed And Range Results

Typed reduction, compact IDs, reusable vectors, and deferred walking coverage
removed the main allocation costs. The warmed inner kernel allocated zero bytes;
complete requests still allocated workspaces and outputs. StaticArrays was not
needed. Projection fell below 1% of routed profile samples.

Resolution-6 final default-16 times are seconds. Every final call matched frozen
origin and value arrays exactly. Final values are three-call medians. A star
marks a single frozen call; other frozen values are earlier three-call medians.

| Walk h | Origins | Budget h | Samples | Frozen | Packed | Packed allocation MB |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 127 | 3 | 96 | 4.044133 | 0.359967 | 44.600 |
| 1 | 331 | 3 | 96 | 11.379481 | 0.908105 | 46.720 |
| 1 | 1,027 | 3 | 96 | 19.698348 | 1.873117 | 48.477 |
| 1 | 127 | 168 | 4 | 7.128525 | 0.417346 | 100.304 |
| 1 | 127 | 168 | 96 | 112.251257* | 8.738933 | 110.962 |
| 2 | 127 | 3 | 96 | 4.665312 | 0.644716 | 74.092 |
| 2 | 331 | 3 | 96 | 10.746704 | 1.636985 | 75.110 |
| 2 | 1,027 | 3 | 96 | 17.786890* | 3.038466 | 78.766 |
| 2 | 127 | 168 | 4 | 9.587326 | 1.100595 | 293.522 |
| 2 | 127 | 168 | 96 | 132.813579* | 23.661530 | 590.122 |

The one-hour, three-hour-budget profile traversed no walking edges. Actual
two-hour walking improved by 5.61-8.71x, not 10x. It used a two-hour prepared
index. Loading reached 18.88 GB RSS under memory pressure; request RSS was much
lower. Allocation totals are not resident memory.

Resolution-7 range repair reduced times from 2.958/8.614/22.863 s to
1.948/6.290/17.437 s at 127/331/1,027 origins (three-call medians).
The 127-origin, 168-hour case took 82.029 versus 39.232 s (one call each).
Short-budget allocation increased by 342-349 MB; long-budget allocation fell
by about 503 MB. Sampled request RSS was 8.96-9.42 GB; loading reached 23.38 GB.
These earlier trials used a live-server activity gate, not full host isolation.
Range repair keeps separate A/E labels and rebuilds coverage at each sample.
Tiles with one time block retain the packed search. Queue-only gains were small.

## Adaptive Tiles And Bounds

Later shared-host trials retained every pair and recorded external CPU load.
They alternated variant order after warmup, with full GC before each call.
Ratios below are medians of matched baseline/candidate ratios, not ratios of
independent medians. Initial idle-gate failures were not used for decisions.

Static-only Chad requests at 9,919 origins improved by a paired ratio of 2.267
(range 2.099-2.387). Allocation fell from 668.02 to 42.70 MB, with no routing
workers. Paris showed no clear static-path gain. Do not generalize the Chad result.

Explicit tile 64 versus tile 16 gave Paris ratios of 1.665 and 1.460 at 1,027
and 9,919 origins. Rural ratios were 0.758 and 1.029. The adaptive rule selects
64 for point queries. It selects 64 for windows with more than four samples
only when transit-capable origins number at least `128 * Threads.nthreads(:default)`.
Otherwise it selects 16. Static origins are removed first. This gives two full
64-origin tiles per worker. Automatic selection was added after the explicit
tile trials. Allocation was 2.6-2.9 times that of tile 16; this is not a universal optimum.

Final retained eight-bin comparisons used the frozen adaptive engine:

| Case / samples / budget h | Pairs | Baseline / retained wall s | CPU s | Paired wall ratio [range] |
| --- | ---: | --- | --- | --- |
| Paris 1,027 / 96 / 3 | 5 | 10.772 / 10.019 | 73.06 / 67.53 | 1.076 [1.016, 1.103] |
| Paris 9,919 / 96 / 3 | 3 | 30.795 / 28.841 | 216.25 / 200.36 | 1.088 [1.068, 1.105] |
| Rural 1,027 / 96 / 3 | 3 | 0.230 / 0.282 | 1.10 / 1.21 | 1.018 [0.775, 1.042] |
| Rural 9,919 / 96 / 3 | 3 | 2.854 / 2.791 | 15.85 / 15.54 | 1.023 [1.016, 1.038] |
| Paris 1,027 / 1 / 3 | 3 | 0.283 / 0.272 | 1.66 / 1.54 | 1.043 [1.015, 1.117] |
| Paris 1,027 / 2 / 3 | 3 | 0.795 / 0.722 | 5.06 / 4.74 | 1.105 [0.977, 1.192] |
| Paris 1,027 / 4 / 3 | 3 | 1.178 / 1.116 | 8.38 / 7.78 | 1.055 [1.053, 1.057] |
| Paris 127 / 96 / 168 | 3 | 39.267 / 38.443 | 278.73 / 274.58 | 1.024 [0.999, 1.027] |
| Paris 9,919 / 1 / 3 | 3 | 1.103 / 0.996 | 5.15 / 4.73 | 1.077 [1.017, 1.139] |

Do not claim a gain for the short rural case. Bounds use exact integer times
and are cached by graph identity within each population object. The table took
47,097,024 bytes and 0.180 s of timed build work. It is separate from walking
geometry and request-result caching. Main Paris allocation was about 1.817/1.878 GB;
maximum sampled request RSS was 12.04 GB. Loading reached 23.30 GB.

## Rejected Paths

- Runtime schedule cache: all three actual-walking cases were 0.4-5.0% slower; eight workers needed 28.73 MB more. This was a per-worker lookup cache, not the retained bounds or handler result cache.
- Incremental population matrices: the preliminary short case added about 550 MB. Rejected background-load timings did not support adoption.
- Expiry buckets: Paris 1,027 took 22.254 versus 16.729 s and allocated 3.82 times as much. The 127-origin, 168-hour diagnostic had a real paired gain of 1.068, but did not justify a general policy.
- Timestamp radix: the main adaptive case had a paired wall ratio of 0.956 and more allocation. Retain the binary heap.
- Dense SIMD: compiler and LLVM reads had ratios of 0.985 and 0.974 in the initial main case. Long-budget CPU gains did not justify the short-budget loss.
- SIMD follow-up: half-density reads gained only 0.8% paired wall time at 9,919 origins. Vector updates and full-only gates did not help the main case. Cutoff-only results had reversals. This rejects these implementations, not SIMD in general.

The SIMD density probe found only 2.079% of enqueue masks had at least 32 bits,
versus 69.106% of cutoff masks. Vector instructions and zero-allocation helper
calls were verified, but microbenchmarks did not predict useful request gains.
The rural follow-up had unequal external load; its apparent 10% gain is unreliable.

## Verification And Evidence

Historical range suites passed 99,898 assertions at one and eight threads.
Adaptive suites later passed 100,239 at both counts. Bounds tests passed 4,204
checks per suite and 100,000 random real-profile comparisons per lookup.
All 12 full-network mode/exclusion comparisons had exact H3 arrays and zero masks
with `rtol=1e-12, atol=1e-6`. Eleven had bitwise-equal values. Fraction-weighted
union with exclusion differed by at most `2.2351741790771484e-8`, also seen between
baseline runs. Do not promise bitwise totals across worker schedules.

Retained evidence: [116 trial rows](population-approved3-trials.csv),
[45 follow-up rows](population-simd-followup-trials.csv),
[mask density](population-simd-followup-density.csv), and the
[bounds run log](population-approved3.log) and [follow-up log](population-simd-followup.log).
The logs retain source hashes, startup, profiles, and validation context.
Local source archives and detailed arrays were recorded under
`/tmp/opencode/population-3980e5f`, `/tmp/opencode/population-cpu-frozen`,
`/tmp/opencode/population-shared-20260910`, and `/tmp/opencode/population-approved3`.
These temporary artifacts are not required repository inputs. No seven-day
performance claim is supported for 10,000 origins. ARM and CUDA were not tested
in these CPU trials. See the [active roadmap](roadmap.md) for further work.
