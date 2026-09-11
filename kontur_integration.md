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

Each population object also caches eight-bin schedule bounds by graph identity.
These bounds narrow exact timetable searches. They do not depend on walking
geometry. The handler's population-result cache is separate: it reuses completed
origin totals. The rejected per-worker runtime schedule cache was a different
lookup experiment; its rejection does not apply to either retained cache.

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
uses up to 64 query lanes, each one an `(origin, sample)` pair. The point default
holds up to 64 origins. Windows with two to four samples retain 16 origins and the
one-block search. For longer windows, the default is 64 if the count of
origins with possible transit access is at least `128 * Threads.nthreads(:default)`.
Otherwise it is 16. Classification removes static-only origins before this choice.
The threshold gives at least two full 64-origin tiles per worker: 1,024 transit
origins on eight threads. It does not use a regional density label. Each
block holds up to `floor(64 / origin_count)` samples for that tile. One worker
owns a tile and processes all its time blocks. Each worker then takes the next
available tile, without a barrier between groups of tiles. The worker count is
the smaller of the default thread count and transit-origin tile count. Each worker has
private buffers. All workers stop before a request error is returned.

The 64-lane mask limits a block, not the number of origins in a request.
The internal `origin_batch_size` override remains available for benchmarks.
Tile 64 needs more workspace RAM. In the eight-thread paired trials, request
allocation was 2.6-2.9 times the tile-16 allocation. These trials used explicit
tile choices before automatic selection was added. They measure equivalent tile
configurations, not a separate automatic-selection run. The rule does not give
the best performance on every machine. No public flag was added.

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

Before routing-buffer allocation, the engine checks each origin and its legal
initial network walks for outgoing connections. A self-connection also counts:
it can permit another walk. Origins that cannot reach a connection use one static
population total for all samples and modes. The total counts each initial walking
destination once and applies origin exclusion before summation. A request with
only these origins uses no routing workers. Other origins fill compact tiles;
their results keep the original sorted H3 order.

Other off-graph origins use the packed path. Graph access, direct population coverage,
and extra destination IDs are prepared once per request. An unprepared index or
an effective walking limit above the prepared limit uses the reference path with
the same semantics. The effective limit is the smaller of the walk limit and
journey budget. Server startup prepares one hour. A two-hour walk request with
at least a two-hour budget uses reference routing unless the index covers two hours.

Startup compiles synthetic routing queries, including all three population families,
Arrow responses, HTTP, and WebSockets. This uses synthetic graphs and population.

## Verification

The [CPU research report](experiments/benchmarks/cpu-research-results.md) records
range-baseline parity for 9,919-origin Paris and rural requests, paired trials
with measured external CPU load, and the retained eight-bin schedule bounds.
Expiry, SIMD, and radix implementations were not adopted; those paths are closed.

Tests cover all six modes, independent per-origin walking results, HTTP/WebSocket
parity, deadlines, overlapping walks, partial batches, fractional population,
zero-population cells, and reference fallback.

Resolution-7 range and adaptive trials used actual one-hour walking edges.
Earlier resolution-6 three-hour profiles at that limit traversed no walking edges.
Actual two-hour walking cases improved by 5.61-8.71x with a two-hour prepared
index. These are shared-host measurements, not general speed guarantees.
Schedule lookup and heap work remained the main measured costs.

The optional [GPU population experiment](experiments/gpu/README.md#population)
performs walking, coverage, and `Float32` reduction on the device. Small-fixture
checks cover `KA.CPU` and Intel P630. Full-network hardware comparison and CUDA
validation are pending. The production server stays CPU-only with its existing dependencies.

## Next Steps

1. Profile transit-time lookup and queue processing across many origins and samples.
2. Preserve shared state expansions on the GPU. Measure wider masks before adoption.
3. Compare full-network GPU requests with the packed CPU engine at the target origin counts.
4. Add resumable planet-scale runs over populated origins, with bounded batches and saved results.
