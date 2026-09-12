# Child-Aware Boarding Experiment

This report records the scalar prototype. The retained engine now uses 64-origin
lanes and schedule hints. See [the batched follow-up](boarding-batched.md) for
current timings, storage, and verification. The measurements below remain as
the record of the scalar experiment, not the current engine's performance.

## Scalar Decision

Keep this candidate as an experimental option. Do not change production defaults.
The res6 candidate reduced error in the main windows. The large Vienna case was
2.10 times as fast as the fine router. Res7 was slower than the fine router in
both main city cases. Neither resolution is a general accuracy improvement.

All changes are in `experiments/hierarchy/`. Production code, input files, server
code, and the default `route_hierarchy` algorithm are unchanged. The scalar version
of `Boarding.jl` contained 266 lines for preparation, routing, and the adapter. It used the existing
production source classifier, worker scheduler, and population coverage helper.
It does not copy or rewrite production source code.

## Model

The reference is the current res8 router. It is not a model of exact locations.
The candidate keeps one earliest-arrival label for each parent A state and E state.
Each label has a `UInt8` fine-child tag. Children are actual represented res8 cells,
sorted by cell ID. Tags are local to a parent. Tag zero is for private sources.
Res7 has at most seven children; res6 has at most 49. Res8 is a test control.

For incoming child `i` and boarding child `j`, the added time is:

```text
gap(i, i) = 0
gap(i, j) = ceil(centre_distance_km(i, j) * 720000) milliseconds
```

Preparation obtains these durations from the existing fine walking network.
Each parent has a small `UInt32` pair matrix. A pair outside the prepared walking
limit has value `INF`. There is no fixed two-minute or five-minute charge, and
there is no rounding to whole minutes.

Core profiles are grouped by `(source parent, target parent, boarding child)`
before the earliest-arrival envelope is built. Each retained event records its
fine destination child. Both daily copies of the original schedules contribute.
The envelope chooses the earliest arrival, then the lowest destination-child tag.
It can discard a later arrival at another destination child. This is intentional.

An A label may have arrived by walking. Its child tag is that walk's fine endpoint.
The selected endpoint belongs to the minimum projected walk, with ties resolved
by fine destination ID. The kernel also records walking time already used.
A departure is eligible only when both conditions hold:

```text
walking_time_used + gap <= query_walking_limit
scheduled_departure >= A_time + gap
```

The arrival remains the scheduled arrival. A transit arrival updates A and E and
sets walking time used to zero. E keeps the true fine transit-arrival child.
A network walk updates A only. A boarding gap is not added inside a transit edge.
Thus `A -> B, B -> C` has no added time at B. The same rule applies to trip shortcuts.
Private source profiles already include the first walk and first boarding.
They receive no second charge.

### State And History

Each origin has its own scalar event order: `(time, parent-state ID)`.
Labels improve in lexicographic order `(time, walking time used, child tag)`.
Only a strict decrease enters the queue. Equal-clock child changes can enter it,
but cannot cause a cycle: all three fields have finite integer domains and the
label strictly decreases. An old queue key reads the current winning metadata.
There is no permanent settled-state rule and no fixed repair-count limit.

This is not location-aware arrival dominance. An earlier arrival at one child can
suppress a later arrival that is closer to a departure. A discarded label can
also have less walking time used. The single-winner rule permits missed routes.

`history=:independent` clears labels for each sample. `history=:descending`
processes all query samples from latest to earliest and retains labels between
samples. Each query starts with fresh state. Each origin has separate state.
Coverage is processed in blocks of at most 64 samples; block and origin tile sizes
do not change route event order.

Descending history is a separate approximate algorithm, not an acceleration that
is equivalent to independent queries. A retained journey can start after the new
ready time because waiting is permitted. A new parent winner can suppress another
journey, but cannot make a previously emitted journey infeasible in this model.
Every new departure uses the time, child, and walking resource from one label.
No departure uses an old child with a new label time. Output checks the current
sample's cutoff, including labels that came from a later sample.

The test counterexample arrives at child B at time 100, or child C at time 200.
A departure from C at time 201 is reachable through the later arrival. A query
ready at zero loses it under the single-winner rule. A query ready at ten finds it.
Descending history keeps that valid journey for the earlier sample. Independent
history does not. Do not interpret a descending window as the intersection or
union of independent candidate point queries.

### Remaining Optimism

Projected network walks still use the minimum from any source child of the parent.
The fine source of that minimum can differ from the E tag. Final egress still uses
the minimum from any child, including zero-cost credit for other represented
children. These are explicit, unchanged relaxations. The boarding correction does
not make these walks or output cells exact. Feasibility claims above apply to this
relaxed model, not to complete fine-cell journeys. There is no guaranteed error sign.

## Measurements

Runs used Julia 1.12.7, eight threads, and an Intel Xeon E3-1275 v6. The input was
`austria_adjacent_res8.arrow`, without the shuttle: 20,244 nodes, 61,796 edges,
and 4,791,297 profiles. Fine walking was prepared once at one hour. All variants
used the same population, query samples, and origin batch size of 64. The host was
not isolated. The small 127-origin cases therefore had only two routing tiles.

Each case has a warm round and three measured, interleaved rounds. Times below
are medians. Measured rounds had zero reported compile time. Preparation is not
included. There is no result cache. Source hashes and allocation, GC, CPU, and RSS
measurements are in the raw artifacts.

Main cases start at 08:00, use a three-hour budget, and sample 96 departures at
15-minute intervals. The mode is `mean_intersection`.

| City | Origins | Core | Fine (s) | Free transfers (s) | Walking gaps (s) | Fine / gaps | Free WMAE | Gaps WMAE |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Vienna | 1,027 | 7 | 10.009 | 3.068 | 14.302 | 0.70 | 5.28% | 9.87% |
| Vienna | 1,027 | 6 | 10.295 | 1.262 | 7.156 | 1.44 | 14.64% | 3.41% |
| Salzburg | 1,027 | 7 | 4.738 | 1.774 | 5.502 | 0.86 | 24.25% | 18.07% |
| Salzburg | 1,027 | 6 | 4.548 | 0.835 | 2.620 | 1.74 | 85.51% | 26.88% |
| Vienna | 9,919 | 6 | 57.941 | 9.582 | 27.600 | 2.10 | 57.58% | 13.43% |

WMAE is `sum(abs(candidate - fine)) / sum(fine)`. Relative percentiles use only
positive fine results. Zero-reference errors are reported separately.

The large res6 case has mean signed error -2,663 people, but this small mean hides
large errors. Its minimum and maximum signed errors are -1,900,483 and +1,915,817.
Its absolute relative p95 is 74.18%, versus 1,055.81% for free transfers. At 1,027
origins, res6 absolute relative p95 is 7.24% in Vienna and 389.98% in Salzburg.
The Salzburg mean signed error is positive, +34,422 people. Error sign is not fixed.

### Controls And Holdouts

The new zero-penalty kernel matches the old hierarchy in all four real four-sample
controls. This separates label and profile overhead from the boarding correction.
It is slower than the old packed kernel. The candidate does not share transit
expansions between origins; it shares population coverage work between samples.

Fresh and descending samples differ on real data. For res6, four-sample WMAE is
22.39% with descending history versus 23.75% with independent history in Vienna.
The corresponding Salzburg values are 25.15% and 26.94%.

Holdouts use 127 origins. Res6 results are mixed:

| Case | Vienna free / gaps WMAE | Salzburg free / gaps WMAE |
| --- | ---: | ---: |
| Point at 08:00, three hours | 21.25% / 17.57% | 70.36% / 32.35% |
| 23:30, six hours, 96 samples, origin excluded | 17.24% / 17.45% | 36.51% / 9.48% |
| 20:00, three hours, 96 samples, reachable union | 20.77% / 19.29% | 72.29% / 33.17% |
| Moved centre, 23:30, four samples, reachable union | 23.20% / 20.32% | 67.77% / 19.77% |

The six-hour candidate is slower than fine routing in both cities. Res7 worsens
Vienna error in every measured case. Keep the raw failures; do not select a
production threshold from the best main-window result.

Point checks compare fine destination sets for 16 origins per city. For 08:00 and
a three-hour budget, res6 has 18,403 false-positive and 209,917 false-negative
origin-destination pairs. Their population sums are 2,163,190 and 35,302,887.
Res7 has 5,765 false-positive and 208,595 false-negative pairs, with population sums
740,502 and 30,982,990. These sums count each tested origin separately. Both signs
also occur in the midnight and six-hour checks. Aggregate population error can
hide these destination-set differences.

### Preparation And Storage

The res7 pair matrices contain 269,824 payload bytes. Res6 uses 1,257,400 bytes.
For finite, nonzero ordered pairs, the median/p95 durations are 11.25/22.38 minutes
at res7 and 30.16/56.59 minutes at res6. These are pair-count percentiles, not
journey-weighted percentiles. Pairs beyond one hour are excluded from those values.

For the 1,141 prepared Vienna origins, profiles increase from 17,740,380 to
19,451,990 at res7, and from 6,951,385 to 9,326,148 at res6. Boarding preparation
adds 13.56 and 14.68 seconds, respectively, after base hierarchy preparation.
Salzburg adds 3.45 and 3.41 seconds. These are single preparation measurements.

For 9,919 Vienna origins, the candidate has 121,466 edges and 18,441,306 profiles.
The old hierarchy has 15,878,973 profiles. Candidate graph-array payload is
149,036,308 bytes. Profile destination tags add 18,441,306 bytes, boarding tags
121,466 bytes, and walking endpoint tags 12,041 bytes. Both graphs are retained by
the API. Their combined index with output, direct walks, and origin mapping uses
363,990,433 bytes by `summarysize`, excluding shared fine inputs and schedule hints.

Base preparation takes 5.65 seconds and boarding preparation takes 20.49 seconds.
Boarding preparation allocates 13.57 GB cumulatively. Peak process RSS is 7.80 GB.
Allocation volume is not retained index size.

For `n` private graph nodes, each scalar workspace adds `2n` child-tag bytes and
`8n` walking-resource bytes to `8n` time-label bytes. E resource entries stay zero;
the uniform array layout retains them for simplicity. At 9,919 origins this is
24,130 tag bytes and 96,520 resource bytes per active tile. Origins within a tile
use the same arrays sequentially. Production scheduler scratch arrays are still
allocated in addition to this state. Tags are child IDs, never quantized times.

## API And Verification

After the normal hierarchy setup:

```julia
candidate = prepare_boarding(index) # index core resolution must be 6, 7, or 8
result = route_boarding(index, candidate, origins, 28_800_000, 10_800_000;
    window_ms=86_400_000, step_ms=900_000, history=:descending)
```

Use `history=:independent` for fresh samples. Use `correction=false` for the
zero-penalty control. The walking limit must match preparation. Zero walking or
zero budget uses the existing fine fallback. All six population modes and exact
fine-origin exclusion are supported. Keep prepared arrays read-only.

```sh
julia --project=router -t 1 experiments/hierarchy/test-boarding.jl
julia --project=router -t 8 experiments/hierarchy/test-boarding.jl
julia --project=router -t 8 experiments/hierarchy/benchmark-boarding.jl NEW_DIRECTORY full
julia --project=router -t 8 experiments/hierarchy/benchmark-boarding.jl NEW_LARGE_DIRECTORY large
```

The benchmark requires a new output directory with an existing parent. With no
selection argument it runs the small screen. Run `large` only after the main case
shows a useful accuracy and speed tradeoff. The large run here used res6 only.

All 12,126 assertions pass with one thread and with eight threads. This includes
the existing 7,069 hierarchy assertions. Tests cover same-child transfers,
shortcuts, exact millisecond thresholds, walking
resource limits, two-day profiles, zero-penalty parity, res8 parity, and a history
counterexample. On 200 random graphs, an expanded-state oracle retains every
`(parent state, child, walking resource)` label and scans profiles linearly. Candidate
point coverage is a subset of that oracle's relaxed coverage. Tests also check
input immutability and both history modes across tile sizes. The real harness checks
candidate values at tile sizes 1, 16, and 64, including the large case.

Raw artifacts are in `boarding-results/` and `boarding-large/`. Each has trials,
signed quality metrics, and the ten largest absolute origin errors per case.
`boarding-results/coverage.csv` contains the point set checks. `boarding-screen/`
retains the slower first prototype measurements. The first prototype repeated
coverage for each sample. Batching coverage reduced cost without changing its
measured values. Source hashes distinguish the two runtime versions.
