# Shared Timed Events Across Origin Tiles

## Decision

Reject both tested 128-origin implementations: UInt128 and the two-word follow-up
below. Keep the array queue and masked target check. Production files and previous
staged changes remain intact. UInt128 increased wall and CPU time in every pair.
Paris 1k exceeded the 20% wall-time loss limit. Do not test 256/512-origin masks
on this evidence. The 5-10x goal was not met.

## Architecture

The prototype used `PopulationWorkspace{M,H}` with `M=UInt64` or `UInt128`.
Each worker had one heap for up to 128 origins, instead of two independent
64-origin tiles. Heap keys and H3 IDs stayed `UInt64`. Pending, settled,
coverage, and reduction masks used `M`. Each origin kept separate `UInt32`
transit-ready (A) and walk-eligible (E) labels.

Only pending lanes whose label matched the popped `(time, state)` expanded.
The marker was cleared before relaxation, so zero-time connections could add
new lanes after a pop. A schedule lookup served all eligible lanes in that
event, including lanes on both sides of the old 64-origin boundary.
Event queues stayed private to workers. No global queue or lock was added.

The experimental default selected 128 only for more than four samples and at
least `128 * Threads.nthreads(:default)` classified transit-capable origins.
Full cohorts processed one sample per block. Partial cohorts used the remaining
mask capacity. Point queries, short windows, and small origin sets kept their
old defaults. The public batch override still accepted only `1..64`.

## Measurements

These are complete `route_population` calls with 96 samples, a three-hour
budget, and a one-hour walking limit. Times are medians in seconds. Ratios are
medians of three matched baseline/candidate ratios; values above one are better.
Brackets give the minimum and maximum paired wall ratios.

| Case | Baseline / 128 Wall s | Baseline / 128 CPU s | Wall Ratio [Range] | CPU Ratio |
| --- | --- | --- | --- | --- |
| Paris 1,027 | 7.861 / 9.593 | 55.526 / 65.705 | 0.816 [0.796, 0.832] | 0.847 |
| Paris 9,919 | 23.033 / 27.564 | 160.075 / 186.429 | 0.837 [0.830, 0.842] | 0.859 |
| London 1,027 | 14.653 / 16.373 | 102.039 / 116.891 | 0.898 [0.872, 0.907] | 0.873 |

Paris 1k and London each had 1,027 transit-capable origins: 17 baseline tiles
or nine candidate cohorts. Paris 10k had 9,412 transit-capable origins: 148
tiles or 74 cohorts. Both variants used eight workers in all cases.

The generic-64 check kept the old tile policy. Its paired wall ratio was 1.014
[0.997, 1.043]; its CPU ratio was 0.999. Values and expansion counts were exact.
Type parameterization alone showed no material cost or established speed gain.

Full-query event expansions fell by 30.4%, 24.9%, and 34.4%, respectively.
Independent origin expansion counts did not change. Separate instrumentation
compared two 64-origin tiles with one 128-origin cohort on the first 128
classified Paris origins, over all 96 samples:

| Counter | Two 64-Origin Tiles | One 128-Origin Cohort | Reduction |
| --- | ---: | ---: | ---: |
| Accepted event expansions | 4,876,122 | 3,548,475 | 27.2% |
| Heap pops, including stale keys | 7,571,323 | 5,703,044 | 24.7% |
| Schedule lookups after target pruning | 11,524,025 | 8,574,172 | 25.6% |

These are event execution counts, not unique-key counts. They confirm work
sharing, not cache reuse. The prior [key trace](dense-population-results.md#experiments)
motivated the test. Savings did not offset larger masks, labels, and coverage
work. The separate CPU cost of each component was not measured.

## Memory

Fresh fixed workspaces, excluding shared hints, used 230,200,624 bytes per
64-origin worker and 454,331,408 bytes per 128-origin worker. Eight workspaces
used 1,841,604,992 and 3,634,651,264 bytes: an increase of 1,793,046,272 bytes.
This includes arrays and empty containers, not only labels. Growing heap and
coverage buffers add storage.

The A/E label matrix grew from 194,204,160 to 388,408,320 bytes per worker.
Median complete-query allocation, in decimal MB, was:

| Case | Baseline MB | 128 MB |
| --- | ---: | ---: |
| Paris 1,027 | 1870.905 | 3678.624 |
| Paris 9,919 | 1916.853 | 3718.226 |
| London 1,027 | 1886.238 | 3712.799 |

Maximum process RSS was 22.872 GB during loading; maximum post-call RSS was
13.073 GB. These include all three code variants but only one graph. Post-call
RSS is not a sampled query peak or an isolated production footprint. Allocation
is not RSS. Measured-call GC took at most 0.077 s.

## Method And Checks

The run used Julia 1.12.7 and eight threads on a four-core Xeon E3-1275 v6
on 2026-09-12. One process
loaded `data/everything_res7.arrow` and `data/kontur_h3.arrow` once. Loader options
were `skip_invalid_durations=true` and `badajoz_shuttle=true`. Graph arrays,
prepared walking/population indexes, and schedule hints were common to all
variants. Input hashes were not recomputed; see the prior report.

Queries started at 08:00, with 15-minute sample steps and `mean_intersection`.
Origin population was included. Each case had one warm call per variant and
three alternating pairs. GC ran before each timer. Timings include source
preparation, static classification, workspace allocation, routing, coverage,
and reduction. They exclude loading, index preparation, HTTP, and Arrow output.
No timed query used a result cache. All measured calls had zero compilation time.
Estimated external CPU use across measured calls ranged from 0.50 to 1.18 cores.
No pair was removed. User applications remained active.

Packing, walking preparation, population loading, population preparation, and
schedule hints took 103.254, 8.526, 2.124, 3.786, and 0.292 s, including
compilation. The first 128-origin query took 10.996 s with common code warm.
This is not a clean-process startup measurement.

All 32 timed calls had exact H3 arrays, zero-value masks, and population values.
The focused suite passed 9,173 assertions, plus 1,759 prototype assertions,
at one and eight threads: 10,932 per run. Tests checked exact A/E labels and
masks against the frozen queue, point-oracle arrivals, all six modes, fractional
weights, exclusions, midnight, seven-day budgets, moving cutoffs, and the reserved
time boundary. Cohort sizes were 1, 3, 16, 64, 65, 127, and 128. Tests covered
high lanes added after a same-time pop, stale keys, queue reset, complete wide
queries, and partial cache misses. Both warmed UInt128 kernel allocation checks
returned zero bytes.
Floating-point comparisons used `rtol=1e-12, atol=1e-6`, with exact zero masks.

The failed screen stopped the conditional five-pair, 12-hour, seven-day
full-network, rural, and short-window performance runs. Full-network all-mode
checks and the full router suite were not repeated. No candidate was promoted.

## Source And Cleanup

The baseline was the current worktree, not `HEAD` (`80069cb148caf71a63159c8da56176675e630e5c`).
All `router/src` files were frozen and checked byte for byte before each run.
The snapshot, `/tmp/opencode/shared-events-baseline`, includes the staged queue
and target-check changes.

| Source | SHA-256 |
| --- | --- |
| Baseline `population_packed.jl` | `8636defa35ca9b59696793721f7d555f77402d95e9e96f1c51e9dce286d88171` |
| Baseline `population_range.jl` | `f9c9df8c5e84d218bae927948f18f0d86cd273b5a811d77b9c66c9ea1fb6b5be` |
| Generic-64 packed source | `11d409fc20695b071b688c4b6accdb98a6f04dff76be9787a1f9b83fc92321d4` |
| 128-origin packed source | `0763d2dd720c5b21334503240b50d26601937438247fced9abc6d2b2f5cf2ab6` |
| Both generic range sources | `db24851a35bf10090ff1e4f0a67f717cb553d165042fc9ea2334c0c0f7171ff9` |

The driver reused the guarded [dense helper](dense-population-benchmark.jl)
and loaded generated source into modules with common graph types. The driver
and prototype-only tests were removed. No benchmark is maintained for this
rejected candidate. [All trial rows](shared-events-trials.csv) remain; pair zero
is warmup. The process exited and released its graph. No commit, server change,
dataset change, dependency, or production API change was made.

## Two-Word Follow-Up

Reject this implementation too. Eight cohorts lost wall time in both Paris cases.
London improved, but its 1.5% CPU reduction did not justify nearly double the
fixed workspace storage. Four cohorts reduced CPU use but increased latency.
These results apply to the tested implementations, not to all shared-event designs.

Each worker used two standard `PopulationWorkspace{H}` objects. Their only shared
routing state was the heap and queued-time vector. Before expansion, a pop cleared
a matching marker. Each word removed only lanes whose labels matched the event
time. Edges were scanned once; eligible transit edges used one lookup for both
words. Updates used the original enqueue function, UInt64 masks, and separate
A/E matrices. Coverage and reduction code was extracted unchanged. Joint words
processed one sample at a time, including partial words. Cohorts of at most 64
origins used the original tile. No UInt128 arithmetic or inter-worker lock was used.

The same frozen source, host, inputs, loader options, and query parameters were
used. One process loaded the graph once. Each comparison had a warm pair and
three alternating measured pairs. [All 32 calls](shared-words-trials.csv) remain.
The baseline used eight workers. Ratios and medians follow the table above.

| Case / Candidate Workers | Baseline / Candidate Wall s | Baseline / Candidate CPU s | Wall Ratio [Range] | CPU Ratio |
| --- | --- | --- | --- | --- |
| Paris 1,027 / 8 | 8.057 / 8.276 | 55.454 / 55.866 | 0.973 [0.967, 0.988] | 0.992 |
| Paris 9,919 / 8 | 22.841 / 23.458 | 159.210 / 160.754 | 0.963 [0.955, 0.974] | 0.994 |
| London 1,027 / 8 | 14.953 / 14.517 | 101.185 / 99.713 | 1.039 [1.015, 1.056] | 1.015 |
| Paris 9,919 / 4 | 22.145 / 27.336 | 158.999 / 105.948 | 0.809 [0.801, 0.812] | 1.501 |

There were nine cohorts for each 1k case and 74 for Paris 10k. Classified-origin counts
were unchanged. Fixed storage was 457,366,624 bytes per worker. Eight used 3,658,932,992
bytes, versus baseline 1,841,604,992. Four used 1,829,466,496 bytes with the same 512
active origin slots as baseline. This does not establish a portable worker policy.
Median allocated MB, baseline/candidate, were 1870.962/3730.615 for Paris 1k,
1916.846/3752.865 for Paris 10k, and 1886.237/3750.093 for London. The four-cohort
Paris 10k comparison used 1916.849/1891.205 MB. Maximum process RSS was 22.934 GB;
maximum post-call RSS was 20.480 GB. These are not isolated query peaks.

Across 96 samples on the first 128 Paris origins, both engines had 4,876,122 word
expansions and 36,635,101 independent origin expansions. Joint processing combined
1,327,647 pairs of word expansions into single events, leaving 3,548,475 events. Lookups fell
from 11,524,025 to 8,574,172; heap pops fell from 7,571,323 to 5,703,044.
The latest two samples had 105,518 distinct `(sample, time, state)` keys in each
trace, with 47,725 keys present in both words. These measure actual event sharing.
They do not assign the remaining CPU cost to arithmetic, storage, or aggregation.

All calls had exact H3 arrays, zero-value masks, and values. The latest two samples and
the first sample also had exact full-network A/E matrices and reached masks for
both words. At one and eight threads, 9,173 existing plus 1,303 new assertions
passed: 10,476 per run. Tests covered all six modes, exclusions, cohorts through
129 origins, midnight, seven-day budgets, moving cutoffs, reserved times, late
same-time arrivals, stale keys, cache misses, and point/short-window controls.
Active warmed kernel allocation was zero. Measured compilation was zero; maximum
GC was 0.083 s. Estimated external load ranged from 0.51 to 1.01 cores. Graph
packing took 87.213 s. No 12-hour test or full router suite was run after rejection.

Generated source SHA-256: eight workers `8caeaa7ad30cd45c2e7ae7a9cd5cf8b9157366f0804a2e2db64a4614576e2e50`;
four workers `805afd4012cf2df635d5dd04fa648cb974cc3f295c221ac74ec6d6a2f2a79f9a`.
The temporary code was removed. Production source still matched the baseline byte
for byte. The original trial data remains unchanged.
