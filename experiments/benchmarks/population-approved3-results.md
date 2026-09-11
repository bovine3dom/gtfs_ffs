# Three CPU Experiments Under Adaptive Tiling

## Decision

Retain eight-bin schedule bounds for CPU accessible-population routing. Keep
the SIMD and timestamp-radix candidates in experiments only. No dependency,
GPU kernel, input dataset, public query flag, or production request limit changed.

The retained bounds use exact integer times. Population arithmetic remains
`Float64`. The binary heap, pending dictionary, adaptive tile rule, static-origin
path, backward A/E repair, and fresh sample coverage remain unchanged.

## Method and Inputs

These are shared-host measurements, not isolated-host measurements. The host was
a Xeon E3-1275 v6 with AVX2, four cores, eight hardware threads, and 62 GiB RAM.
Julia 1.12.7 used eight routing threads and one observer thread. User applications
remained running. No idle gate or rejected timing sample was used.

The initial worktree was clean at `ae70b1f633e68c9ce1df953b9363b78c8bb3159f`.
The current source bytes were archived before edits in
`/tmp/opencode/population-approved3/current.tar`. This baseline already had
adaptive 16/64-origin tiles. It was not the earlier default-16 baseline.

One process packed `everything_res7.arrow` once. Module constructors shared the
graph, walking, population, and prepared-weight arrays. Identity assertions
checked the shared arrays. The graph remained resident through final validation.
The benchmark process then exited. No user server was stopped.

| Input or prepared object | Count |
| --- | ---: |
| Resolution-7 input rows | 205,463,043 |
| Graph nodes | 379,305 |
| Transit edges | 1,471,782 |
| Packed profiles | 192,645,963 |
| Actual one-hour network walking edges | 3,737,032 |
| Positive-population walking entries | 6,038,589 |
| Population input rows | 32,957,699 |

The normal loader options included invalid-duration exclusion and the Badajoz
shuttle. The network SHA-256 was
`614ac1a6a5b87d35c6e83c057a64161005fb339e80a369f4065709e3d33309d7`.
Input hashes, source hashes, loading times, and memory observations are in the
[complete run log](population-approved3.log).

Each case had a warmup for each variant. Odd pairs used the listed order; even
pairs reversed it. Full GC ran before the measured interval. Measurements cover
the complete `route_population` call, including source preparation and workspace
allocation, but not HTTP transport or Arrow encoding. Tests did not run during
the paired measurements. Small candidate tests overlapped initial data loading;
load times are not isolated performance results.

The runner recorded wall seconds, process CPU seconds, external busy cores,
allocated bytes, and sampled process RSS. Linux CPU accounting excludes duplicate
guest counters. Process CPU uses the process's own user and system counters, not
child counters. External load is aggregate busy CPU minus benchmark CPU, divided
by wall time. Counter resolution limits short-call accuracy. All 116 paired
observations remain in the [raw CSV](population-approved3-trials.csv). Warmups,
profiles, validation calls, and loading calls remain in the complete log.

Unless specified otherwise, requests used 96 samples at 15-minute intervals,
a three-hour journey budget, a one-hour walking limit, `mean_intersection`, and
no own-origin exclusion. Ratios are medians of matched baseline/candidate pairs.
They are not ratios of the separate time medians. A ratio above 1 favors the
candidate.

## Isolated Candidates

Paris, 1,027 origins, five pairs per variant:

| Variant | Wall s | CPU s | Paired wall ratio [range] | Paired CPU ratio | Allocation MB |
| --- | ---: | ---: | --- | ---: | ---: |
| Current baseline | 10.536 | 73.29 | 1.000 | 1.000 | 1817.46 |
| Compiler SIMD, density hybrid | 10.537 | 72.72 | 0.985 [0.957, 1.019] | 1.009 | 1817.48 |
| Eight-lane LLVM SIMD, density hybrid | 11.024 | 73.61 | 0.974 [0.930, 0.992] | 0.996 | 1817.84 |
| Timestamp radix queue | 11.018 | 75.63 | 0.956 [0.909, 1.003] | 0.968 | 1835.67 |
| Eight-bin bounds prototype | 9.942 | 68.12 | 1.046 [1.020, 1.108] | 1.075 | 1817.08 |

External load across these pairs was 0.46-1.08 busy cores. SIMD did not give a
useful full-request gain. The radix queue was slower and allocated more memory.
Only bounds advanced to the retained implementation. There was no combination
of multiple winning candidates to test.

Additional prototype bounds checks, three pairs each:

| Case | Baseline / bounds wall s | Baseline / bounds CPU s | Paired wall ratio [range] |
| --- | --- | --- | --- |
| Paris, 9,919 | 33.975 / 30.372 | 218.88 / 203.36 | 1.092 [1.067, 1.295] |
| Rural, 1,027 | 0.293 / 0.276 | 1.18 / 1.13 | 1.044 [0.741, 1.150] |
| Rural, 9,919 | 2.919 / 2.888 | 15.77 / 15.21 | 1.011 [0.947, 1.017] |

The 127-origin, 168-hour diagnostic completed below 60 seconds per call.
It used three pairs per variant:

| Variant | Wall s | CPU s | Paired wall ratio [range] | Paired CPU ratio |
| --- | ---: | ---: | --- | ---: |
| Baseline | 46.315 | 280.70 | 1.000 | 1.000 |
| Compiler SIMD | 40.102 | 270.12 | 1.106 [0.855, 1.281] | 1.041 |
| Eight-lane LLVM SIMD | 41.071 | 267.31 | 1.200 [0.972, 1.251] | 1.056 |
| Timestamp radix | 46.774 | 285.68 | 0.943 [0.876, 1.225] | 0.983 |
| Eight-bin bounds | 44.560 | 277.44 | 1.039 [0.873, 1.232] | 1.012 |

Diagnostic external load ranged from 0.58 to 2.76 busy cores. SIMD reduced process
CPU here, but the wall ratios varied widely. This result does not justify a
production policy that loses on the main three-hour case.

## Retained Implementation

`Population.schedule_hints` owns an `IdDict{Graph,Matrix{Int32}}`. The population
lock protects preparation. Different timetables cannot share bounds through the
same walking index. No global cache or `Graph` layout change was added.

Server handler preparation builds the bounds once and reports the startup stage.
Direct routing prepares them on the first heavy request. Static-only requests
still return before routing workspace allocation. Workspaces share a typed
reference to the table. Both packed and range traversal use the bounds; other
metrics retain the original `next_arrival` implementation.

The lower bound is the first departure at the bin boundary. The upper bound also
includes the first departure beyond the next boundary. The last bin uses the
original profile end. Empty and next-day profiles remain valid. The cutoff check
occurs before addition. The upper-bound expression avoids `Int32` overflow at
the profile-end sentinel.

The table has 47,097,024 bytes. The production builder took 0.180 seconds of
timed work, 0.212 wall seconds, and 0.210 process CPU seconds. Its table was
element-for-element equal to the prototype table. The final cache reused it.

The retained code has no architecture-specific instructions or new package
requirements. It uses ordinary Julia integer operations on supported Julia CPU
targets. Only the experimental SIMD code uses generic LLVM vectors. ARM and
other non-x86 hardware were not tested.

### Final Paired Times

All final rows compare the actual retained source with the frozen adaptive source.

| Case | Samples | Budget h | Pairs | Baseline / retained wall s | Baseline / retained CPU s | Paired wall ratio [range] | CPU ratio |
| --- | ---: | ---: | ---: | --- | --- | --- | ---: |
| Paris, 1,027 | 96 | 3 | 5 | 10.772 / 10.019 | 73.06 / 67.53 | 1.076 [1.016, 1.103] | 1.081 |
| Paris, 9,919 | 96 | 3 | 3 | 30.795 / 28.841 | 216.25 / 200.36 | 1.088 [1.068, 1.105] | 1.080 |
| Rural, 1,027 | 96 | 3 | 3 | 0.230 / 0.282 | 1.10 / 1.21 | 1.018 [0.775, 1.042] | 1.009 |
| Rural, 9,919 | 96 | 3 | 3 | 2.854 / 2.791 | 15.85 / 15.54 | 1.023 [1.016, 1.038] | 1.024 |
| Paris, 1,027 | 1 | 3 | 3 | 0.283 / 0.272 | 1.66 / 1.54 | 1.043 [1.015, 1.117] | 1.078 |
| Paris, 1,027 | 2 | 3 | 3 | 0.795 / 0.722 | 5.06 / 4.74 | 1.105 [0.977, 1.192] | 1.067 |
| Paris, 1,027 | 4 | 3 | 3 | 1.178 / 1.116 | 8.38 / 7.78 | 1.055 [1.053, 1.057] | 1.076 |
| Paris, 127 | 96 | 168 | 3 | 39.267 / 38.443 | 278.73 / 274.58 | 1.024 [0.999, 1.027] | 1.015 |
| Paris, 9,919 | 1 | 3 | 3 | 1.103 / 0.996 | 5.15 / 4.73 | 1.077 [1.017, 1.139] | 1.089 |

Paris windows show a consistent useful gain at both main origin counts. The
larger rural case has a smaller consistent gain. Do not claim a gain for the
short rural case: its independent wall median increased, and one matched pair
regressed. These three short pairs do not establish a systematic regression or
a speedup. The long-budget diagnostic gain is small.

### Final Resources

MB and GB below are decimal units. RSS is the maximum sampled process RSS across
both variants, not incremental table memory.

| Case / samples / budget h | Baseline / retained allocation MB | Peak RSS GB | External busy cores |
| --- | --- | ---: | --- |
| Paris 1,027 / 96 / 3 | 1817.66 / 1817.17 | 12.04 | 0.43-1.21 |
| Paris 9,919 / 96 / 3 | 1880.04 / 1878.04 | 11.47 | 0.51-0.90 |
| Rural 1,027 / 96 / 3 | 619.59 / 619.68 | 9.36 | 0.63-1.00 |
| Rural 9,919 / 96 / 3 | 1836.45 / 1836.42 | 11.84 | 0.83-1.00 |
| Paris 1,027 / 1 / 3 | 288.51 / 288.51 | 8.39 | 0.69-1.09 |
| Paris 1,027 / 2 / 3 | 282.83 / 282.73 | 8.66 | 0.57-0.91 |
| Paris 1,027 / 4 / 3 | 293.78 / 293.69 | 8.34 | 0.68-0.78 |
| Paris 127 / 96 / 168 | 1430.60 / 1430.06 | 10.19 | 0.59-0.88 |
| Paris 9,919 / 1 / 3 | 313.65 / 313.07 | 8.34 | 0.57-0.65 |

The largest sampled loading RSS was 23,303,884,800 bytes, about 21.7 GiB.

## Kernel and Profile Evidence

The SIMD experiment changed only queued-label equality and coverage cutoff masks.
It did not vectorize dependent graph relaxations or population reductions. The
density hybrid used sparse set-bit iteration below one quarter of the allocated
width, or for widths below 16. Reads used the matrix row stride and valid mask.
The strict-improvement store loop was not changed because the read-only experiment
did not give a useful gain on the main case.

Native output for concrete `Matrix{UInt32}` arguments shows `tzcnt`/`blsr` in the
sparse helper. The eight-lane helper shows `vpcmpeqd ymm` and `vmovmskps`.
Unsigned cutoff comparison uses `vpminud`, not a signed comparison. The compiler
reduction also generates vector instructions, with more mask-construction work.
No hardware instruction or cache counters were collected.

The typed one-million-call microprobe gave these times in milliseconds. It is a
single diagnostic observation per cell, not a paired request result.
The corresponding artifact is `kernels-typed.log`.

| Width / active bits | Sparse | Compiler dense | Eight-lane dense |
| --- | ---: | ---: | ---: |
| 16 / 16 | 8.64 | 5.56 | 3.85 |
| 64 / 8 | 4.66 | 19.49 | 20.82 |
| 64 / 16 | 9.00 | 16.44 | 16.68 |
| 64 / 32 | 22.75 | 15.47 | 14.25 |
| 64 / 64 | 39.25 | 14.53 | 14.13 |

The warmed typed hot loop allocated zero bytes in all 50 checked combinations.
The outer timing call can box its return value. A separate stand-alone forwarding
helper probe was not allocation-free. These helpers remain experimental.

The radix queue has 33 reused buckets and a nonempty bitmap. Only timestamp bits
select buckets. Full keys remain payloads and pending-dictionary identities.
Explicit resets occur before each independent packed sample and each backward
range sample. Equal-time lower-state events do not violate its ordering.

Fresh adaptive-engine profiles attributed 29.23% of baseline routed samples to
schedule lookup, versus 24.25% after the change. Baseline heap pop accounted for
about 8.0%; final-walk coverage accounted for about 5.3%. These are sampled routed
stack shares, not whole-request hardware counters. They do not support a large
overall speedup claim.

## Correctness and Artifacts

- The full router suite passed with one thread and with eight threads, sequentially, within each 600-second limit.
- The new production schedule-bound test set passed 4,204 checks per suite. It includes concurrent preparation, distinct timetables with one walking index, empty profiles, next-day profiles, bin boundaries, and near-limit times.
- Experimental mask tests passed 307,200 checks across widths 1-64, partial tiles, high bits, and unsigned boundary values.
- Radix tests passed 160,051 checks, plus a warmed capacity-reuse allocation check.
- Whole-query candidate tests passed 2,912 checks and 84 profile-boundary checks with one and eight threads. They cover all six modes, exclusion, point and zero-step requests, midnight, long horizons, fractional weights, and tile sizes 3/16/64.
- Both the prototype lookup and retained lookup passed 100,000 random real-profile comparisons against the original lookup.
- All 12 full-network mode/exclusion comparisons had identical H3 arrays and zero masks and passed `rtol=1e-12`, `atol=1e-6`.
- Eleven of those 12 value comparisons were bitwise equal. The fraction-weighted union with exclusion was not. A follow-up found a maximum difference of `2.2351741790771484e-8` both between two baseline runs and between baseline and retained code. Do not claim bitwise-stable `Float64` totals across worker schedules.
- HTTP/WebSocket tests preserve zero-row filtering, metric-specific radius validation, all six window selectors, and point behavior when the window or step is zero.

The full log contains the measured source hashes. Final cleanup replaced the
integer division operator with its ASCII `div` alias; routing logic did not change.
The recorded source was reconstructed and checked against its SHA-256 hashes.
Native lookup output then matched after removal of module names and JIT symbol
numbers. The 4,204 schedule-bound checks passed again at one and eight threads.
See `ascii-equivalence-final.log` and `ascii-equivalence-t8.log` in the artifact
directory. The complete suites ran before this alias-only cleanup.
Detailed native output, profiles, test logs,
the frozen source archive, command files, and serialized results remain under
`/tmp/opencode/population-approved3/`. This directory also retains unsuccessful
early microprobe and allocation-harness logs.

Use [the resident loader](benchmark-population-10k.jl) with `POP_SNAPSHOT` set to
the frozen directory. Run [isolated trials](population-approved3-trials.jl),
[retained trials](population-approved3-final.jl), and
[producer verification](population-approved3-verify.jl) in that same process.
The checked-in scripts combine the sequential command phases from this run.
Do not start a second full graph load for a candidate.

## SIMD Gate Follow-Up

This follow-up used the **retained eight-bin adaptive engine** as its baseline.
Production source and dependencies did not change. A byte comparison against
`/tmp/opencode/population-simd-followup/current.tar` passed before shutdown.
One new graph load served every candidate. It had the same 379,305 nodes,
192,645,963 profiles, and 3,737,032 one-hour walking edges. Peak loading RSS was
23,362,052,096 bytes, about 21.8 GiB. The live servers remained running.

The [new kernels](population-simd-followup-kernels.jl) compare quarter-density,
half-density, and full-mask-only gates. Tests also include forced sparse,
compiler-vectorized, and eight-lane LLVM paths. The new update path compares
unsigned labels, intersects the result with the incoming mask, and blends the
new time into distinct allocated rows. A chunk with no improvement performs no
store. Inactive lanes retain their old values. Partial rows use scalar code.
Queue and pending-dictionary changes remain scalar and occur after the helper.

The microprobe covers widths 3/16/32/64 and active counts 0/1/4/8/16/32/64,
clipped to the width. Input masks vary inside the measured loop. Gate selection,
mask construction, comparison, and stores are included. Separate update cases
have either no improvements or improvements on every active lane. Each cell has
three trials of 100,000 calls. These are shared-host microbenchmarks, not isolated
hardware measurements.

Selected final microprobe medians, in ns per call:

| Width / active | Operation | Sparse | Quarter gate | Half gate | Full-only gate |
| --- | --- | ---: | ---: | ---: | ---: |
| 64 / 16 | Equality read | 10.95 | 15.49 | 11.54 | 11.52 |
| 64 / 32 | Equality read | 20.17 | 15.79 | 12.44 | 21.49 |
| 64 / 64 | Equality read | 38.71 | 16.23 | 12.38 | 20.16 |
| 64 / 32 | Improving update | 55.75 | 28.55 | 23.57 | 81.50 |
| 64 / 64 | Improving update | 100.83 | 31.44 | 25.51 | 22.60 |

The quarter gate was too aggressive for the 64-row, 16-active equality case.
The half gate corrects that choice. Microprobe variation and compiler layout
effects remain visible; these numbers do not define a universal crossover.
Both the [initial microprobe](population-simd-followup-micro.log) and the
[final microprobe and native output](population-simd-followup-kernels.log) are retained.

### Actual Mask Density

One instrumented 1,027-origin request recorded these masks at allocated width 64.
Instrumentation was not enabled during paired timings.

| Site | Masks | At least 16 bits | At least 32 bits | All 64 bits |
| --- | ---: | ---: | ---: | ---: |
| Enqueue input, after time/walk checks | 401,566,415 | 9.167% | 2.079% | 0.0054% |
| Post-validation pop, including zero masks | 61,435,410 | 9.459% | 2.257% | 0.0083% |
| Cutoff-scan input | 24,819,610 | 80.782% | 69.106% | 26.884% |

Pop counts include 18,618,854 zero masks. These are masks after queued-time
equality filtering, not the pending masks entering that comparison. Thus the pop
column is not an exact gate-hit counter for the equality helper. The enqueue
and cutoff columns measure their helper inputs. The sparse enqueue inputs limit
the opportunity for vector stores. Dense cutoff inputs motivated a separate
cutoff-only experiment. All counts are in the [density CSV](population-simd-followup-density.csv).

### Follow-Up Requests

All requests used eight routing threads, 96 samples, a three-hour budget, and
one-hour walking. Each row has three interleaved pairs. Wall and CPU values are
separate medians; ratios use matched pairs. Every pair passed output parity.

| Case / candidate | Baseline / candidate wall s | Baseline / candidate CPU s | Paired wall ratio [range] |
| --- | --- | --- | --- |
| Paris 1,027 / quarter reads | 9.770 / 9.664 | 67.70 / 66.38 | 1.012 [1.002, 1.020] |
| Paris 1,027 / half reads | 9.770 / 9.560 | 67.70 / 66.95 | 1.023 [1.008, 1.024] |
| Paris 1,027 / full-only reads | 9.770 / 9.942 | 67.70 / 67.24 | 0.984 [0.956, 1.012] |
| Paris 1,027 / half updates | 9.770 / 9.869 | 67.70 / 68.15 | 0.990 [0.985, 1.019] |
| Paris 1,027 / full-only updates | 9.770 / 10.001 | 67.70 / 68.88 | 0.978 [0.966, 0.981] |
| Paris 1,027 / cutoff-only LLVM, half | 9.833 / 9.671 | 67.60 / 66.78 | 1.032 [0.977, 1.051] |
| Paris 1,027 / cutoff-only compiler, half | 9.833 / 9.573 | 67.60 / 67.07 | 1.027 [0.994, 1.041] |
| Paris 127 / half reads | 1.877 / 1.789 | 12.56 / 12.12 | 1.030 [1.022, 1.049] |
| Paris 9,919 / half reads | 28.469 / 27.811 | 200.58 / 197.58 | 1.008 [1.006, 1.024] |
| Rural 1,027 / half reads | 0.314 / 0.276 | 1.18 / 1.15 | 1.101 [0.997, 1.423] |

All 45 paired observations remain in the [follow-up CSV](population-simd-followup-trials.csv).
It records wall time, process CPU, external load, allocated bytes, and sampled
RSS. The [complete log](population-simd-followup.log) also retains warmups and
the diagnostic. External load during the main screen ranged from 0.46 to 0.93
busy cores. The rural baseline calls saw 1.67-2.01 external cores, versus
1.46-1.60 for the candidate. Do not interpret the rural wall ratio as a reliable
10% speedup. The largest paired RSS was about 11.64 GB. Request allocation stayed
near 1.817 GB at 1,027 Paris origins. No idle gate or trial rejection was used.

### Follow-Up Decision

**Retain no additional production change.** Half-density reads show a modest
benefit, rather than proof that SIMD cannot help. However, the target 9,919-origin
case gained only 0.8% in median paired wall time and about 1.5% in process CPU.
That margin does not justify the extra LLVM path under the requested useful-win
criterion. Full-only gates and vector updates did not improve the main case.
Cutoff-only results had reversals and small CPU gains. The costly 168-hour
diagnostic was not repeated because no candidate qualified for promotion.

Exact helper tests passed 384,000 checks from 12,000 randomized matrices, with
unsigned boundary values, zero masks, inactive lanes, and partial rows. All 2,688
warmed typed microprobe allocation checks passed with zero bytes. Whole-query
tests passed 1,320 checks across all six modes, exclusion settings, partial tiles,
point requests, and short windows. Native output shows unsigned comparisons,
mask extraction, `vblendvps`, vector stores, and the no-improvement store branch.
The vector instructions select integer bit patterns; they do not approximate
times or population. No hardware counters were collected. No production
dependency was added. The full router suite was not repeated because production
bytes were unchanged. The benchmark process exited after validation.
