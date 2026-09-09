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
zero time. Logical parent aggregation conserves population; it is not a geometric
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
each required resolution once. Startup reports file loading,
row validation, uniqueness checks, and aggregation progress. Preparation keeps the
maps in memory and leaves the source file unchanged.

Population maps are H3-keyed `Dict{UInt64,Float64}` objects, shared across networks at
the same resolution. Workers reuse the existing packed, immutable walking adjacency.
Population weights are looked up by H3 key; alignment with compact destination IDs
is a remaining optimization.

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

The engine shares work across origins and departure samples. A job uses up to 64
query lanes, each one an `(origin, sample)` pair. With one sample, a tile holds up to
64 origins. With multiple samples, it holds up to eight origins. Each time block holds
up to `floor(64 / tile_size)` samples. Flat worker waves use up to all default threads.

Masks share an expansion at any matching cell, time, and walking-eligibility state.
Each query lane keeps its own deadline. Union and intersection coverage is retained
only for unfinished tiles. Weighted means are accumulated as blocks finish. Output
storage is scalar per origin, rather than a full origin-by-sample-by-cell history.

Startup compiles synthetic routing queries, including all three population families,
Arrow responses, HTTP, and WebSockets. This uses synthetic graphs and population.

## Verification

Tests include an independent population oracle based on existing routing results.
The benchmark report compares all three families with independent walking routes.
The final daytime case uses `everything` at resolution 6, a Paris origin, departure
at 08:00, a three-hour journey budget, and eight threads.

For seven origins and 96 samples, the weighted population query took 149.02 ms,
compared with 316.67 ms for independent cached windows. The single-origin weighted
query took 251.89 ms versus 42.10 ms. The reference includes distance output and three
sums; the population path calculates one sum. These are medians of three measured calls
on a shared machine. The report records memory use, swap activity, and earlier rail results.

The full production suites passed with one and eight threads. They include independent
per-origin comparisons, HTTP/WebSocket parity, per-query deadlines, overlapping walks,
partial batches, fractional population, and zero-population cells.

## Next Steps

1. Profile and optimize single-origin queries. Compare them with the cached reference.
2. Use compact integer destination IDs and aligned population weights to reduce dictionary work.
3. Explore a population-only GPU path with resident data, walking-eligibility states,
   per-query deadlines, coverage reduction, and scalar output per origin.
4. Add resumable planet-scale runs over populated origins, with bounded batches and saved results.
