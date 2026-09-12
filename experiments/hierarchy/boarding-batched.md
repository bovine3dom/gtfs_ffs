# Batched Boarding Follow-Up

## Decision

Retain the batched engine in `Boarding.jl`. It replaces the scalar runtime without
changing the selected single-winner model or the experimental API. Production
code and the default `route_hierarchy` path are unchanged. No input files, server
code, or production configuration changed.

The scalar timings were not a measure of `UInt8` tag overhead alone. The scalar
engine had no shared transit expansion and no schedule hints. This follow-up
restores both. It does not isolate their individual speed contributions.

## Matched Results

All cases use Austria res8 input without the shuttle, one-hour fine walking,
08:00 departure, a three-hour budget, and 96 samples at 15-minute intervals.
The mode is `mean_intersection`, with origin batch size 64 and descending history.
Each case has one warm round and three measured, interleaved rounds. Preparation
is separate. All measured rounds reported zero compile time. No result cache was
used. Julia 1.12.7 ran with eight threads on the Xeon E3-1275 v6.

| City | Origins | Core | Fine (s) | Free hierarchy (s) | Scalar corrected (s) | Batched corrected (s) | Scalar / batched | Fine / batched |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Vienna | 1,027 | 6 | 10.482 | 1.335 | 7.288 | 2.003 | 3.64 | 5.23 |
| Salzburg | 1,027 | 6 | 4.565 | 0.845 | 2.541 | 0.802 | 3.17 | 5.69 |
| Vienna | 1,027 | 7 | 10.305 | 3.130 | 15.164 | 4.720 | 3.21 | 2.18 |
| Salzburg | 1,027 | 7 | 4.457 | 1.675 | 5.308 | 1.627 | 3.26 | 2.74 |
| Vienna | 9,919 | 6 | 56.291 | 9.434 | 26.820 | 7.551 | 3.55 | 7.45 |

Every batched population value is exactly equal to the frozen scalar value in
these five cases. Maximum observed difference is zero. Res6 WMAE therefore remains
3.41% in Vienna and 26.88% in Salzburg at 1,027 origins, and 13.43% in the large
Vienna case. Res7 WMAE remains 9.87% and 18.07%. Batching does not improve accuracy.

The large corrected case is also 1.25 times as fast as the free hierarchy. This
does not mean correction is free. The corrected model reaches fewer states and
destinations in many cases. At 1,027 Vienna origins, correction still costs about
50% more time than the free res6 hierarchy.

The host was not isolated. The CSV files include estimated external CPU activity,
CPU time, allocation volume, GC time, RSS, and shared and per-lane expansions.
No other test or benchmark process was run by this task during these measurements.

## Engine

Each worker processes a tile of at most 64 origins. A and E arrival times, child
tags, and walking resources are matrices with one row per origin. One `UInt64`
mask identifies pending origins for a shared `(time, parent state)` queue event.
No `UInt128` masks or additional route labels are used.

On an A event, the kernel groups active origins by incoming child. At most 49
nonzero child groups are possible, plus the private-source tag zero. For each
boarding profile and child group, it calculates one gap and filters origins by
their remaining walking resource and current target labels. It then performs one
schedule lookup for that eligible group. Different walking resources do not need
separate lookups: they only affect eligibility for the same gap.

Preparation builds eight-bin schedule bounds with the existing production helper
and a temporary private population cache. The candidate owns the resulting hint
matrix. Regional candidates do not enter the shared input population's cache.
Lookup returns the profile index so that the scheduled arrival and child tag
always come from the same event. No schedule or profile time is changed.

E events share each projected walk across all active origins. After each sample,
all retained E labels within that sample's cutoff contribute to output. Coverage
masks pack origins and samples into at most 64 lanes. The unchanged production
tile reducer handles the six population modes and origin exclusion. Experimental
dispatch calls that reducer with `invoke`; it does not copy or rewrite its source.

### Event Order And History

Within each origin, queue order remains `(time, state ID)`. Profiles retain their
original traversal order. Other origins cannot change that origin's labels.
Each enqueue requires a strict decrease in `(time, walking resource, child tag)`.
An equal-clock metadata improvement can queue the state again. Clearing the queue
marker before relaxation prevents loss of that update. Stale keys with no matching
pending lanes do no work. This removes redundant scalar expansions.

The incoming child groups and walking resources are copied before relaxing any
outgoing edge. A zero-time self-edge can change a label during expansion. The
snapshot prevents a later profile from combining the old child with a newly reset
walking resource. A dedicated two-lane test checks this case.

Descending history still runs samples from latest to earliest, across all tile
blocks. Independent history clears labels for every sample. The production
packed and range entry points both dispatch to the same tagged label-correcting
kernel. Neither uses permanent settlement of parent states. Each new tile starts
with fresh labels and metadata. The selected history algorithm therefore does
not depend on tile size or the production packed/range branch.

The [scalar report](boarding-results.md) defines the approximation and its history
counterexample. Those limits still apply. Later-but-closer arrivals can be lost.
Descending history is not equivalent to independent queries. Projected walking
sources and final egress remain optimistic. There is no guaranteed error sign.

## Work And Storage

For 1,027 Vienna origins at res6, 30,381,882 per-lane expansions use 5,039,026 shared
queue expansions. The scalar reference performs 30,468,058 expansions, including
redundant equal-time queue entries. In the large case, the corresponding counts
are 126,833,193 per-lane, 24,037,093 shared, and 127,212,043 scalar expansions.

The runtime, preparation, and adapter together contain 321 lines, versus 266 for
the scalar version. Final review removed unused source metadata and a diagnostic
argument. The routing kernel is unchanged from the measured version. No permanent
scalar runtime copy was added to the repository.

At res6 with 1,141 prepared origins, a full tile uses 420,736 tag bytes and
1,682,944 resource bytes. With 9,919 prepared origins, these become 1,544,320 and
6,177,280 bytes. Time matrices reuse the production workspace when dimensions
match. Pending masks, queue markers, and group snapshots add separate small arrays.
State grows with tile width, not with all query origins at once. E resource entries
remain zero but occupy the same matrix layout as A entries.

The new hint matrices use 1,566,432 bytes for Vienna res6, 1,396,448 for Salzburg
res6, 2,702,976 for Vienna res7, and 2,238,112 for Salzburg res7. The large res6
candidate adds 3,886,912 hint bytes. Profiles and gap matrices are unchanged.

Large-query allocation volume increased from about 0.803 GB for scalar routing to
1.342 GB for batched routing. Per-tile metadata allocation is retained in this
bounded prototype; workspaces are not cached across queries. Median batched GC
time is about 0.020 seconds. Peak RSS for the large comparison process is 7.90 GB.
These allocation totals are not retained index sizes. Large boarding preparation
takes 20.30 seconds, in addition to 5.84 seconds for the base hierarchy.

## Verification

All 13,729 assertions pass with the frozen scalar reference on one and eight
threads. The self-contained eight-thread run passes all 12,929 assertions without
the temporary reference. These totals include the existing 7,069 hierarchy checks.

The tests use more than 64 prepared origins on each of 200 random graphs. They
compare tile sizes 1, 16, and 64, both history modes, and one-, four-, and 96-sample
queries. They cover all six modes, origin exclusion, zero-penalty parity, res8
parity, the expanded-state feasibility oracle, and same-clock resource changes.
Real-case tile checks also pass at 1, 16, and 64, including the large query.

The scalar reference is frozen at `/tmp/opencode/Boarding-scalar-266.jl`. Its SHA-256
is `01d002974d17fc88c63fe5780d01edd6e313f4af290ce5d8d2c92d7f0a72aa2c`.
A temporary module gives it distinct graph types while sharing unchanged input
arrays. It does not replace current methods or prepare a different profile index.

```sh
BOARDING_SCALAR_REFERENCE=/tmp/opencode/Boarding-scalar-reference.jl julia --project=router -t 1 experiments/hierarchy/test-boarding.jl
BOARDING_SCALAR_REFERENCE=/tmp/opencode/Boarding-scalar-reference.jl julia --project=router -t 8 experiments/hierarchy/test-boarding.jl
BOARDING_SCALAR_REFERENCE=/tmp/opencode/Boarding-scalar-reference.jl julia --project=router -t 8 experiments/hierarchy/benchmark-batched.jl NEW_DIRECTORY
BOARDING_SCALAR_REFERENCE=/tmp/opencode/Boarding-scalar-reference.jl julia --project=router -t 8 experiments/hierarchy/benchmark-batched.jl NEW_LARGE_DIRECTORY large
```

Without `BOARDING_SCALAR_REFERENCE`, tests and benchmarks run without the temporary
scalar comparison. Raw results are in `boarding-batched/` and
`boarding-batched-large/`. Earlier scalar and holdout artifacts remain unchanged.
The unchanged experimental API is `prepare_boarding(index)` followed by
`route_boarding(index, candidate, origins, departure, budget; ...)`.
