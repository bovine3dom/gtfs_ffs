# Res8 Access Control

## Decision

Keep `core_resolution=8` as a model-equivalent preprocessing option and control.
Do not recommend it as a faster replacement for the current 96-sample population
router on this evidence. Its matched 1,027-origin gains were 1.007x in Vienna and
1.026x in Salzburg, with substantial extra index storage. Production remains the
preferred measured window path when no additional coarsening is wanted.

The res8 H3 model already approximates actual journeys. This experiment checks
agreement with that model, not ground truth. It uses the retained hierarchy code
without merging fine core nodes. No runtime code, tests, server settings, or input
files changed. No additional routing algorithm was attempted.

## Method

Run on 2026-09-12 with Julia 1.12.7 and eight threads on the same Xeon E3-1275 v6.
One process loaded `data/austria_adjacent_res8.arrow` once, without the shuttle.
The graph had 20,244 nodes, 61,796 edges, and 4,791,297 profile entries. The input
hash matched the [earlier Austria run](results.md). Population had 32,957,699 rows,
total weight 8,031,924,024, and Float64 runtime weights. Fine walking used 1 h.
No CPU test suite ran during this benchmark. The user server was left unchanged.

For each city, one res8 hierarchy index prepared the same radius-19, 1,141-origin
cohort as the earlier res7/res6 run. That index served every smaller query below.
The core kept all fine nodes. Only the existing virtual-source preprocessing and
private population representation changed relative to production.

The baseline called current `route_population` directly. The candidate called
the retained `route_hierarchy` through the same query helper. Both used current
packed/range kernels, the array queue, pruner, and hints. Neither used a result
cache. Each call allocated fresh query workspaces and assembled its query sources.
All queries started at 08:00 with a 3 h budget and 1 h walking limit. Window steps
were 15 minutes. Origin exclusion was off.

Intersection and short controls had one warm pair, then three measured pairs.
Orders were production/access, access/production, production/access, and
access/production. `reachable_union` had one warm pair and one measured pair as
a parity control. Full GC preceded each call. All measured calls had zero reported
compilation and GC time. The host was not isolated.

Every call checked sorted origin IDs, exact zero-mask equality, and per-origin
values with `rtol=1e-12, atol=1e-6`. A failed warm check would stop before measured
pairs. Work counts were recorded but were not required to match.

## Query Times

Median seconds within this run only. Gain is production time divided by access
time. A value below 1 means that access preprocessing was slower.

| City | Origins | Samples | Mode | Production s | Fine access s | Gain |
| --- | ---: | ---: | --- | ---: | ---: | ---: |
| Vienna | 127 | 96 | Intersection | 1.8241 | 1.8163 | 1.004x |
| Vienna | 1,027 | 96 | Intersection | 10.2652 | 10.1961 | 1.007x |
| Salzburg | 127 | 96 | Intersection | 1.0877 | 1.1244 | 0.967x |
| Salzburg | 1,027 | 96 | Intersection | 6.4995 | 6.3343 | 1.026x |
| Vienna | 1,027 | 96 | Reachable union | 10.4283 | 10.1631 | 1.026x |
| Salzburg | 1,027 | 96 | Reachable union | 6.4674 | 6.4295 | 1.006x |
| Vienna | 127 | 1 | Point | 0.14164 | 0.13733 | 1.031x |
| Vienna | 127 | 4 | Intersection | 0.44768 | 0.44766 | 1.000x |
| Salzburg | 127 | 1 | Point | 0.06942 | 0.05574 | 1.245x |
| Salzburg | 127 | 4 | Intersection | 0.17069 | 0.17918 | 0.953x |

The reachable-union entries are single measured pairs, not three-pair evidence
of a gain. Salzburg's point case improved, but its process CPU median improved
only from 0.1143 to 0.1072 s. Its four-sample case was slower. This does not establish
a general window benefit. Other host CPU activity reached about 2.77 cores during
the point trials, compared with about 0.58 to 0.74 during the main 1,027-origin
intersection trials.

## Parity And Work

All 72 calls, including warm calls and baseline repeats, passed the value and
zero-mask checks. Intersection, point, and four-sample values matched exactly.
For reachable union, maximum absolute errors were `2.7939677238464355e-9` in Vienna
and `4.656612873077393e-10` in Salzburg. Relative p95 errors were `2.02e-16` and zero;
maximum relative errors were `6.63e-16` and `2.13e-16`. These are floating-point
reduction differences within tolerance, not observed additional coarsening error.

The following table covers the 1,027-origin intersection cases. CPU and allocation
are medians. Work counts were stable across repeats.

| City | Variant | CPU s | Query MB | Shared expansions | Independent expansions |
| --- | --- | ---: | ---: | ---: | ---: |
| Vienna | Production | 72.556 | 150.78 | 44,959,930 | 435,833,812 |
| Vienna | Fine access | 72.372 | 150.64 | 44,924,133 | 448,408,350 |
| Salzburg | Production | 44.550 | 79.49 | 48,786,928 | 191,556,889 |
| Salzburg | Fine access | 44.670 | 75.90 | 50,193,600 | 201,906,441 |

Virtual sources replace initial E walks and A boarding states. Fine own-cell
population is credited through the output suffix rather than graph-prefix A
weights. Thus exact coverage does not require identical expansion counts. The
control retained essentially all fine-core work and did not reduce independent
expansions in these cases.

## Preparation And Memory

Index bytes include the combined graph, fine output CSR, direct walks, origin map,
and candidate hints. They exclude shared fine inputs and the global population map.
MB and GB in this report use decimal bytes. Allocated GB are cumulative preparation
allocations, not resident memory. Vienna's first preparation included compilation.

| Cohort | Origins | Prepare s | Compile s | Access phase s | Source profiles | Index MB | Allocated GB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Vienna | 1,141 | 3.8955 | 1.5116 | 1.8655 | 46,573,563 | 492.28 | 5.500 |
| Salzburg | 1,141 | 1.4978 | 0.0000 | 0.2975 | 13,505,615 | 201.82 | 2.126 |

Each combined graph had 21,385 nodes, including the virtual origins. The unchanged
core had 4,791,297 profile entries, 638,524 network walking entries, and 1,936,815
population output entries. Fine populated destinations numbered 97,397 in Vienna
and 97,654 in Salzburg. Initial access-node counts were 72,568 and 40,500.

Common preparation took 4.526 s for graph packing, 1.335 s for walking, 2.390 s
for population loading, and 5.989 s for population alignment and its global map.
Common resident memory was about 3.42 GB after these stages. Peak process RSS was
5,538,885,632 bytes, or 5.16 GiB. The retained fine graph itself occupied 130.90 MB.

Using `ceil(prepare / (production - access))`, the 1,027-origin median estimates
are 57 queries for Vienna and 10 for Salzburg. The 127-origin Vienna window gives
496 queries. Salzburg's slower 127-origin window has no finite break-even.
The point estimates are 904 queries for Vienna and 110 for Salzburg. These tiny
window savings overlap run variation; they do not establish reliable amortization.
The CSV and metadata retain the unrounded values, including the near-zero saving
in Vienna's four-sample case. All estimates exclude common startup and result-cache
hits. Preparing a new region is not free.

## Large-Case Gate

The 9,919-origin res8 index was not prepared or timed. Neither city's main
1,027-origin window reached the 1.2x gate; the best gain was 1.0261x. The point-only
gain did not satisfy this many-origin window gate.

Scaling the largest 1,141-origin index by `9919 / 1141` gave a conservative linear
index estimate of 4,279,482,431 bytes. This is an estimate, not a measured large
index. Available host memory was 25.33 GB, so memory did not block the case. The
timing gate avoided another long run without a material smaller-window gain.

## Scope And Reproduction

The [res7/res6 results](results.md) remain separate matched comparisons. Do not
divide their candidate medians by this run's baseline medians. Qualitatively,
preprocessing without coarsening did not reproduce their multi-fold window gains.
This control does not assign a precise fraction of those gains to each component.

The API permits res8 preprocessing without extra coarsening, or res7/res6 with
the measured additional error. Production defaults are unchanged.

```sh
julia --threads=8 --project=router experiments/hierarchy/benchmark-access.jl /tmp/opencode/new-access-run
```

The output path must not exist. The 80-line script reuses the existing preparation
and query helpers. Raw evidence is in [metadata.txt](access-results/metadata.txt)
and [trials.csv](access-results/trials.csv). Runtime and production source hashes
are recorded there. No runtime bug was found, so no CPU test suite was rerun.
