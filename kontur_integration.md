# Kontur Population Integration

Status: proposal only. Implementation is deferred until the dataset and metric
semantics are confirmed.

## Goal

Add accessible population as a routing metric, initially on the CPU and eventually
as an on-device reduction for many-origin accessibility or centrality calculations.

The user has a dataset mapping resolution-8 H3 cells to population. An Arrow file is
a suitable input format. Its location, schema and geographic coverage are not yet
confirmed.

## Counting Population

Attach population to unique destination cells, not to stops or individual walking
edges:

```text
accessible_population = sum(population[cell] for each qualifying destination cell)
```

A destination qualifies only after applying the journey budget and walking rules.
Membership in a stop's prepared walking neighbourhood is not sufficient: the final
walk must fit the remaining journey time and start from a walk-eligible arrival.

Collapse all routes and overlapping walking radii to one reachability result per
destination before summing population. Otherwise residents near several stops would
be counted repeatedly. Preserve the existing prohibition on consecutive walks.

## Resolution Options

### Aggregate to Graph Resolution

For each loaded graph at resolution 8 or coarser, aggregate source population by H3
parent and align the resulting weights with the existing prepared destination IDs.

Advantages:

- Smallest implementation; reuses the current geographic destination set.
- Population preparation happens once per loaded resolution.
- Query-time counting becomes a reduction over resident weights.

Limitations:

- Reaching a coarse cell credits its entire assigned population, including at zero
  walking time. The current router treats movement within a graph cell as free.
- Resolution-5 cells can substantially overstate fine-grained walking access.
- H3 parent aggregation conserves population by logical hierarchy; it is not an
  exact geometric polygon overlay, since boundaries are not perfectly nested.
- Resolution-8 totals do not determine population at finer resolutions. Do not
  silently distribute those totals uniformly among children.

### Keep Population Destinations at Resolution 8

Keep transit routing at the loaded graph's resolution, but count resolution-8
population cells reached by direct walking or final walking from the network.

This is the recommended direction if the metric is intended to represent walking
access to residents, rather than population assigned to the current map cells.
It is a recommendation, not an agreed decision.

Consequences:

- Requires a separate population-destination mapping and finer egress adjacency;
  it is not just a join onto the current coarse output cells.
- Reachability must be deduplicated at resolution 8 before counting.
- Transit-only arrival at a coarse cell must not automatically credit its entire
  population. The zero-walk and origin-cell conventions need to be specified.
- Geometry still uses current H3-centre estimates, not actual stop coordinates,
  roads or pedestrian barriers. Finer population cells do not remove that source
  approximation.

## Departure Windows

Let C_s be the distinct destination cells reachable at sampled departure s, N the
sample count, and p(c) the population of destination c.

| Statistic | Definition | Relationship to Current Modes |
| --- | --- | --- |
| Reliable accessible population | Sum p(c) where c is reachable in all N samples | Matches the destination intersection of `mean_intersection` |
| Potential accessible population | Sum p(c) where c is reachable in at least one sample | Matches the destination union of `min_union` |
| Mean accessible population per departure | Sum p(c) times reachable_samples(c), divided by N | A separate statistic, not the current `mean_intersection` mode |

Following `window_mode` is the proposed initial behavior. Confirm this before
implementation.

The union does not mean all counted residents are accessible from one common
departure. Likewise, the intersection is over sampled departures, not a proof of
continuous availability throughout the interval.

Use the query's journey budget as the initial accessibility threshold. Means or
coverage counts calculated at a larger budget cannot generally answer population
queries at a smaller threshold without additional information or recalculation.

## Input and Resident Data

Proposed Arrow columns:

- `h3`: non-null `UInt64`, valid resolution-8 H3 cells.
- `population`: non-null, finite, nonnegative numeric values. Preserve fractional
  estimates if present; do not round them without agreement.

Before loading, establish whether missing cells mean zero population or unknown
coverage. Also establish whether repeated H3 rows are errors or intentional additive
records; reject duplicates by default unless their meaning is known.

Prepare population weights and destination mappings once, rather than joining Arrow
tables during each request. Share immutable weights across workers. A Float64 weight
vector costs about 8 MB per million destinations, excluding mappings and adjacency.

Current prepared output IDs are stable within each loaded graph, but final result
rows are sorted and filtered. Do not zip resident weights against result row positions
without resolving their IDs. Support off-network origins and larger walking-radius
fallbacks as well as the prepared path.

## Response Shape

The primary proposal is one population total per origin, suitable for many-origin
maps without downloading full destination surfaces. Population on individual map
cells is an alternative or additional output. The response shape and frontend
presentation remain undecided. Units are people; existing time metrics are unchanged.

## CPU Implementation Sequence

1. Confirm dataset location, schema, coverage, missing-cell policy and resolution model.
2. Prepare validated population weights and the selected destination mapping at startup.
3. Add a CPU reduction after per-destination reachability has been deduplicated, using
   the agreed window predicate. Do not sum all prepared candidates indiscriminately.
4. Add the agreed response shape and document its units and approximation.
5. Verify tiny fixtures and measure preparation time, retained memory and query cost
   before integrating GPU reductions.

Tests should cover overlapping stop radii, multiple transit paths, origins and
zero-budget queries, off-network origins, walking-radius fallback, partial window
coverage, missing population records, duplicate input rows and resolution handling.
The sum should agree with an independently enumerated set of qualifying cells.

## GPU Direction

The current GPU window engine downloads label batches and aggregates them on the
CPU. Population weights alone do not make that an on-device calculation, and the
current GPU kernels do not implement walking.

The eventual pipeline would:

1. Keep the timetable, relevant walking adjacency and population weights resident.
2. Compute arrival and walking-eligible states, including population-cell egress.
3. Collapse reachability to one covered state per population destination.
4. Maintain union/intersection masks or sample counts for the selected window metric.
5. Reduce population weights on-device and download a scalar per origin when no
   destination map is requested.

Grouped departures must contribute their actual sample multiplicities. Running
masks or counts suffice for these population summaries; full departure-by-cell
histories are unnecessary. Integer population permits exact integer reductions;
fractional estimates require an explicit numerical comparison tolerance for parallel
floating-point sums.

Benchmark origins per second against CPU many-origin execution. More origins alone
will not fix a pipeline that still downloads every surface and performs serial host
work. Prefer arrival-only routing: itinerary kilometre replay is not needed for this
metric.

## Existing References

- `router/src/walking_geometry.jl`: resident walking adjacency and output IDs.
- `router/src/walking_output.jl`: indexed destination reduction and window counts.
- `router/src/walking.jl`: two-state CPU routing and dictionary geographic oracle.
- `router/src/Reachability.jl`: HTTP/Arrow metrics and window destination filtering.
- `experiments/gpu/window_gpu.jl`: experimental GPU download and host aggregation path.
- `geonames/readme.md`: references `public_kontur_population_20231101`, with `h3`
  and `population` columns.
- `plots/walker.jl`: existing population aggregation by H3 parent.
- `plots/plotter.jl`: historical population/accessibility estimates and map exports.
- `tidied_up/sql/03_edgelist_sane_insert.sql`: historical edge-local population
  weighting, not a distinct query-wide accessible-population total.

These references do not establish the location or schema of the user's current
population dataset.

## Open Questions

1. Where is the resolution-8 population dataset, and what are its exact columns and
   types? Do absent rows mean zero population or missing geographic coverage?
2. Should population remain at resolution 8 or be aggregated to graph resolution?
   If kept at resolution 8, what are the zero-walk and origin-cell conventions?
3. Should the response be one population total per origin, destination population
   values, or both?
4. Should window population follow the existing intersection/union mode, or should
   mean accessible population per departure be available as a separate statistic?
