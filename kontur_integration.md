# Kontur Population Integration

## Implemented Contract

The CPU router supports `metric=accessible_population` through HTTP and WebSockets.
Both transports use the same [query contract](router/docs/api.md), including network
selection, time limits, walking rules, encoding, and parameter validation.

Load population with `--population data/kontur_h3.arrow` or
`--population=data/kontur_h3.arrow`. Supply one path and use the option once.
The Arrow input requires unique, valid resolution-8 `h3::UInt64` cells and numeric
`population` values. Both columns require a value in every row. Values must be finite
and zero or more, including after conversion to `Float64`. Fractional values are retained.
The loader validates rows, then checks uniqueness in sorted cell order and rejects duplicates.

Population is summed by logical H3 parent at each loaded routing resolution from 0
through 8. An absent cell has zero population. Population queries at finer resolutions,
or without loaded population data, return HTTP 400. Other metrics remain available.
Each reached routing cell contributes its whole population, including the origin at
zero time by default. Set `exclude_origin_population=true` to exclude each result origin's
own routing-cell population in point queries and all window modes. Other origins can still count that cell.
The default is `false`; `1` and `0` are also valid. Other metrics ignore all values of this option.
Population queries reject empty or invalid values with HTTP 400. Duplicate parameters return HTTP 400.
Logical parent aggregation conserves population; it is not a geometric
polygon overlay. Coarse cells can overstate local walking access.

`origin_radius` selects all cells within an H3 grid distance of the query cell as
independent origins. It includes cells without population or transit. It is independent
of walking. The default, zero, selects only the query origin. Each response row contains
an origin H3 index in the requested encoding and `value::Float64` in people.
The router calculates all origin totals. The public response contains one cell per origin
with a positive final total after window aggregation. It omits zero totals, including the query origin.
An origin with zero local population is included if its accessible total is positive.
All-zero results contain an empty Arrow table with the same schema.
`X-Router-Origin-Count` reports origins examined, not rows returned.
Internal `route_population` results retain all generated origins, including zero totals.
Other metrics ignore `origin_radius` values. Duplicate parameters return HTTP 400.
Rows are sorted by H3 value. Population routing calculates neither itinerary nor
origin-destination distances. A valid `distance_mode` is accepted but has no effect.

Let `P(c)` be a cell's population, `k(c)` its reachable sample count, and `N` the total
sample count. Multiple paths and overlapping walks count each cell once per sample.
Each final walk must fit the remaining time budget and start from a walk-eligible state.

| Window modes | Population total |
| --- | --- |
| `mean_intersection`, `max_intersection`, `diff_intersection` | Sum `P(c)` where `k(c) = N` |
| `min_union`, `diff_union` | Sum `P(c)` where `k(c) > 0` |
| `reachable_union` | Sum `P(c) * k(c) / N`, the mean accessible population |

`window_h=0` or `step_h=0` selects one departure and ignores `window_mode`.
All three families give the same total for one sample. Union totals can include cells
reached at different departures. Intersection totals apply to sampled departures.

## Resident Data

`serve.jl` calls `load_population(...; progress=true)`. Handler construction prepares
each required resolution once. Startup reports loading, validation, uniqueness,
aggregation, and indexed population preparation. Prepared data stays in memory.

Population maps are H3-keyed `Dict{UInt64,Float64}` objects, shared across networks
at the same resolution. Each population object caches a `PreparedPopulation`
sidecar for each prepared walking index. The sidecar aligns `Float64` weights
with compact destination IDs. Its compressed sparse row (CSR) walking data
contains positive-population destinations, sorted by duration within each node.
Workers share the immutable walking adjacency and prepared population data.

The [benchmark report](experiments/benchmarks/population-results.md) records these
facts for `data/kontur_h3.arrow`:

| Property | Value |
| --- | ---: |
| File size | 234,395,250 bytes |
| Unique resolution-8 rows | 32,957,699 |
| Source values | `Float64`, all positive integers |
| Global population | 8,031,924,024 |
| Resolution-6 map | 2,016,971 cells |
| Resolution-7 map | 9,012,014 cells |

Both parent maps retain the global total. These integer values and their union and
intersection sums are exactly representable in `Float64`. Generic fractional weights
and sample-weighted sums can differ with reduction order. The benchmark compares
results with `rtol=1e-12` and `atol=1e-6`.

## Shared CPU Engine

The packed engine shares work across origins and departure samples. Each block
uses up to 64 query lanes, each one an `(origin, sample)` pair. The default tile
holds up to 64 origins for one sample, or 16 origins for multiple samples. Each
block holds up to `floor(64 / origin_count)` samples for that tile. One worker
owns a tile and processes all its time blocks. Each worker then takes the next
available tile, without a barrier between groups of tiles. The worker count is
the smaller of the default thread count and origin-tile count. Each worker has
private buffers. All workers stop before a request error is returned.

Masks share an expansion at any matching cell, time, and walking-eligibility state.
Each query lane keeps its own deadline. `PopulationWorkspace` uses compact IDs,
packed `UInt64` event keys, and settled and reached mask arrays. Final-walk
coverage is deferred until search ends, then scans the positive-population CSR
once per reached walk-eligible node per block. Each lane retains its own remaining
walking budget. Mask aggregation and population sums stay on the worker.
Output storage holds one scalar per origin.

For tiles with multiple time blocks, routing starts at the latest sample and
works backward. Each origin keeps separate transit and walking-eligible arrival
labels across all blocks. Routing processes only strict improvements to these
labels. Coverage includes unchanged labels that fit the current sample's cutoff.
Coverage is rebuilt for each sample and combined in 64-lane blocks. Arrival
buffers are reset between tiles. Other mask arrays reset only touched entries.
Tiles with one time block retain the packed origin/sample search.

Workers reuse vector capacity across blocks and tiles. The measured warmed inner
kernel allocates zero bytes. Complete requests still allocate workspaces and
outputs. Large buffers need capacity that can grow with the request. This design
uses standard vectors and requires no StaticArrays dependency.

Off-graph origins use the packed path. Graph access, direct population coverage,
and extra destination IDs are prepared once per request. An unprepared index or
an effective walking limit above the prepared limit uses the reference path with
the same semantics. The effective limit is the smaller of the walk limit and
journey budget. Server startup prepares one hour. A two-hour walk request with
at least a two-hour budget uses reference routing unless the index covers two hours.

Startup compiles synthetic routing queries, including all three population families,
Arrow responses, HTTP, and WebSockets. This uses synthetic graphs and population.

## Verification

Tests cover all six modes, independent per-origin walking results, HTTP/WebSocket
parity, deadlines, overlapping walks, partial batches, fractional population,
zero-population cells, and reference fallback.

The [CPU range report](experiments/benchmarks/population-queue-results.md) measures
the current engine against the frozen packed default on the standard resolution-7
graph. It uses the production one-hour walking limit and real walking edges.

The [earlier CPU report](experiments/benchmarks/population-optimization-results.md#final-default-16-results)
records the default-16 measurements for 127, 331, and 1,027 origins. Its timing
matrix uses `mean_intersection`, four- and 96-sample windows, and three-hour and
seven-day budgets. Union and weighted comparisons cover a selected subset.
Every final timed output matches the frozen origin and value arrays exactly.
The table marks single-call baselines separately from three-call medians.

That resolution-6 three-hour profile at the one-hour limit traverses zero walking edges.
Actual two-hour walking cases improve by 5.61-8.71x. Those measurements use a
two-hour prepared index. Schedule lookup and heap/pending-event work are the main
remaining costs; projection is below 1% of routed profile samples.

The optional [GPU population experiment](experiments/gpu/README.md#population)
performs walking, coverage, and `Float32` reduction on the device. Small-fixture
checks cover `KA.CPU` and Intel P630. Full-network hardware comparison and CUDA
validation are pending. The production server stays CPU-only with its existing dependencies.

## Next Steps

1. Profile transit-time lookup and queue processing across many origins and samples.
2. Preserve shared state expansions on the GPU. Measure wider masks before adoption.
3. Compare full-network GPU requests with the packed CPU engine at the target origin counts.
4. Add resumable planet-scale runs over populated origins, with bounded batches and saved results.
