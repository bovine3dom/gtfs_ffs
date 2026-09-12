# Global Coarse Router

The engine is in `router/src/coarse.jl`. It does not load experiment code.
It prepares one global model for each source and target resolution pair.
It does not prepare a region, city, or origin set. Startup work is an accepted
cost and is separate from query latency.

The user selects `coarseness=N`. Zero keeps normal routing. The integration
selects `target = max(5, origin_resolution - N)` for positive offsets. The engine
supports all six pairs: 8 to 7, 8 to 6, 8 to 5, 7 to 6, 7 to 5, and 6 to 5.
The [HTTP contract](../../router/docs/api.md#coarseness) uses these global models.

**The full-network population speed gate passes for the global res8-to-res6
model.** All measured queries include fine-origin access. The global startup
cost is not charged to each query. The normal route remains `coarseness=0`.

## Global State

`prepare_coarse_router(fine, walking, target; population=nothing, progress=false)`
has no origin argument. The model retains its source graph and walking index.
Each timetable entry stores an Int32 reference into the fine timetable and a
UInt16 arrival-child tag. Departure, arrival, and transit distance arrays are
not copied. Boarding-child tags also use UInt16. Res8-to-res5 groups can contain
343 fine children. Active child groups do not use a 64-child bit mask.

Walking projections and positive population output rows are global. Output IDs
refer to the source-resolution walking output index. They do not change with
the origin set. Population rows are separate from core routing state IDs.
The model does not add coarse-parent population totals to the fine outputs.

Each query enumerates exact fine initial walks. It then takes a real fine
transit connection before it enters the coarse core. It does not seed an
arbitrary parent with a minimum initial walking time. There is no origin access
cache, regional profile cache, or result cache in the engine.

The model uses one winning arrival child per parent state and descending
window history. Boarding within the arrival child has no extra cost. A change
of child uses the prepared fine walking gap. The remaining walking budget
includes a preceding projected network walk. Final egress uses the minimum
fine egress duration from the parent, as in the historical boarding model.
This last projection can add optimism. A single winning child can also lose
fine journeys. Results can be higher or lower than the fine reference.

## Walking Memory

`router/src/walking_geometry.jl` now counts walking entries before it allocates
packed arrays. A second pass fills the arrays. It retains all geographic,
network, output, duration, and distance fields. It does not retain a temporary
neighbor vector for every graph node. It has no production resource cap and
does not remove zero-population geometry.

## Full-Network Run

The run uses the complete `everything_res8.arrow` input with distance values,
`skip_invalid_durations=true`, and `badajoz_shuttle=true`. The fine graph has
900,197 nodes, 2,984,575 edges, and 316,136,664 two-day profile entries.
The input SHA-256 is
`eb0b3d7e26bed7535522a4ce34aa1f4439053676a3d99356e8887c1b86b06ada`.
Source hashes, the dirty-worktree HEAD, CPU, thread count, and stage memory are
in [the run metadata](global-res8-20260912/metadata.txt).

The driver loads the full fine graph exactly once. It prepares all six global
models and derives ordinary res5, res6, and res7 graphs before query timing.
The two city cohorts, shifted centres, and large cohort use the same res8-to-res6
model object. Every timed call includes its own fine-origin access work.

| Source | Target | Core Nodes | Core Edges | Profile References | Preparation (s) |
|---:|---:|---:|---:|---:|---:|
| 8 | 6 | 117,777 | 1,556,215 | 251,740,470 | 17.848 |
| 8 | 7 | 379,305 | 2,156,496 | 274,199,678 | 19.413 |
| 8 | 5 | 31,917 | 1,216,181 | 241,406,317 | 17.234 |
| 7 | 6 | 117,777 | 926,763 | 160,598,351 | 10.972 |
| 7 | 5 | 31,917 | 638,772 | 146,677,723 | 8.237 |
| 6 | 5 | 31,917 | 309,101 | 84,323,515 | 5.008 |

All six model preparations took 78.711 seconds in total. Registry RSS, with
all four graph resolutions present, was 27,509,641,216 bytes (25.62 GiB).
No lazy model construction or memory fallback was needed.
Peak process RSS for the complete run was 39,587,491,840 bytes (36.87 GiB).
This peak includes query workspaces while the six-model registry is resident.

See [the query table](global-res8-20260912/summary.md) for the measured speed
ratios and errors. Each main case uses 1,027 fine origins, 96 samples, and a
three-hour travel budget. Walking is one hour at 5 km/h. Sampling intervals
are 15 minutes and one minute. Both variants use batches of 64 origins.
There is one first invocation and three measured interleaved pairs per case.

Paris uses res8 centre `881fb46625fffff` at latitude 48.85, longitude 2.35.
London uses res8 centre `88194ad14dfffff` at latitude 51.5, longitude -0.12.
The main disk radius is 18. Each moved case shifts its centre by one fine cell.
The large Paris disk has radius 57 and 9,919 origins.

The 1,027-origin cases have speed ratios from 4.10 to 5.05. The 9,919-origin
case takes 116.503 seconds for fine routing and 22.797 seconds for coarse
routing, a 5.11 speed ratio. The first London call takes 14.464 seconds and
uses the same model that served Paris. It has no regional setup stage.

The comparison with the historical regional model checks 8,216 origin-case
values. The maximum absolute population difference is 1.863e-8. The change
removes regional preprocessing without a material change in these results.
Dense-window WMAE remains about 19 to 22 percent. The large case has WMAE of
12.82 percent, but relative error can be large when the fine denominator is
small. The raw values and outliers retain those denominators.

The run completed 11 population cases and 88 query calls, including first
invocations. All measured pairs had zero compilation time. All six models
were prepared on the full network. Query timing and full-network quality
comparisons use the res8-to-res6 model, not the other five pairs.

## Time And Distance

`route_coarse_time` returns source-resolution H3 cells. Point results have
arrival and distance arrays. Window results use the existing six window
reducers and their standard elapsed-time, reachability, and distance fields.
Equal window extrema use the earlier departure's distance.

Itinerary distance follows the selected coarse-model journey. It includes the
initial fine walk, referenced fine transit segments, boarding gaps, projected
network walks, and final fine egress. It is not replaced with straight-line
distance. An unknown transit distance remains unknown. Straight-line mode uses
the existing origin-to-destination distance calculation.

The time diagnostics compare point and four-sample windows in both cities,
with itinerary and straight-line distance. They are single diagnostic calls,
not paired latency benchmarks. The full-network destination differences are
material; the metadata records missing and extra cells and elapsed-time error.
Do not interpret the approximate time output as fine-model equivalence.

| City | Samples | Common Cells | Missing Fine Cells | Extra Coarse Cells | Matched Elapsed MAE (min) |
|---|---:|---:|---:|---:|---:|
| Paris | 1 | 32,806 | 25,151 | 8,905 | 18.65 |
| Paris | 4 | 39,926 | 21,475 | 12,766 | 16.83 |
| London | 1 | 69,155 | 22,886 | 5,280 | 19.90 |
| London | 4 | 71,741 | 22,983 | 5,179 | 18.98 |

Both distance modes returned the same destination sets and elapsed values in
these diagnostics. There is no uniform time-query speed guarantee. For example,
the London four-sample straight-line call took 0.400 seconds for coarse routing
and 0.202 seconds for fine routing. Population is the measured speed gate.

## API Limits

The model requires a matching prepared walking index. It accepts positive
query walking limits up to the prepared limit, after the travel-budget clip.
Zero walking and zero budget use fine routing. Population queries require a
population input at model preparation. Time queries do not require one.
Origins are validated at the source resolution; population origins are sorted
and deduplicated. The engine does not change the HTTP cache or parser.

## Verification

The focused engine suite passed 2,275 assertions with one thread and with
eight threads. It covers all six resolution pairs, unseen origins, zero
limits, exclusion, all six population modes, exact boarding-gap thresholds,
midnight, distance ties, and 64 lanes with distinct tags above 255. Random
fixtures compare time-window coverage with population reductions. Tests also
cover positive population outside the global walking geometry. The walking
preparation comparison passed 15 assertions across all packed columns and IDs.

Run the focused engine tests before a full benchmark. Do not run large tests
against the existing server. The benchmark uses a separate process and does
not change input files. Use a new output directory to preserve prior results.

```sh
julia --project=router -t 8 router/test/coarse_router_tests.jl
julia --project=router -t 8 experiments/hierarchy/test-full-preparation.jl
julia --project=router -t 8 experiments/hierarchy/benchmark-global.jl experiments/hierarchy/NEW_GLOBAL_RUN
julia --project=router experiments/hierarchy/summarize-global.jl experiments/hierarchy/NEW_GLOBAL_RUN
```
