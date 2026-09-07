**Timetable Router**
Earliest-arrival reachability on a uniform-resolution H3 graph, with a
repeating fantasy daily timetable. Run the commands below from the repository root.

**Model**
- All stops within one cell are freely interchangeable, with zero transfer cost.
- Estimated walking provides access, transfers, direct walking and geographic egress.
  Walks are available at any time, but consecutive walks are forbidden.
- Departures repeat every 24 hours; waiting, including overnight waiting, counts.
- Real dates, service calendars and trip continuity are not represented.
- Transit vertices come from the input; walking also returns reachable geographic
  cells at the graph resolution. Cell-centre estimates are not street routing or a
  guarantee of reachability for every point in a cell.
- `--demo` uses a small synthetic fixture, not the real rail export.

**Input And Export**
The loader requires the first four columns below; the fifth is optional. Types are non-null:

| Column | Type | Valid Values |
| --- | --- | --- |
| `from_h3` | `UInt64` | Valid H3 cell, one common resolution throughout the graph |
| `to_h3` | `UInt64` | Valid H3 cell at the same resolution |
| `departure_ms` | `UInt32` | `0..86399999`, milliseconds since midnight |
| `duration_ms` | `Int64` | `0..604800000`, milliseconds, at most seven days |
| `distance_km` (optional) | `Float64` | Finite, nonnegative length of this connection |

Resolution is inferred from both endpoint columns; mixed resolutions are rejected.
An empty graph retains the res5 default. Existing four-column files still work for
times, but contain no information from which actual route kilometres can be recovered.

Malformed schemas, nulls, invalid cells and out-of-range departure clocks are rejected.
`pack_graph` also rejects out-of-range durations by default, reporting negative/over-limit
counts and the duration range. The server uses `skip_invalid_durations=true`: it skips
those connections with a startup warning, without changing the Arrow file or guessing
replacement times. All input cells remain in the node mapping, possibly isolated.
Zero-duration connections and durations exactly seven days remain valid.
Arrow IPC files and streams are supported, including
compression supported by the Arrow loader; uncompressed IPC is recommended for compatibility.
The launcher takes a filename containing either IPC encoding, not a stdin argument.

`export.sql` uses `transitous_everything_20260218_edgelist_fahrtle2`:
`e.h3` and `e.next_h3` are resolution-11 cells, `departure_time` is `DateTime`,
and `travel_time` is `Int64` **minutes**, multiplied by `60000` for milliseconds.
It retains rail types `2` and `100..117`, maps endpoints to `target_resolution` (default 5),
and adds `geoDistance(stop_lon, stop_lat, next_lon, next_lat) / 1000 AS distance_km`.
It deduplicates and sorts all five projected values, preserving parallel connections
with different distances. Keep your desired transport filter when adapting it.
Self-edges and negative durations
are not filtered by the export; the server reports and skips out-of-range durations.

Run this export yourself after ensuring `data/` exists and configuring ClickHouse access:

```sh
clickhouse-client --queries-file router/export.sql > data/rail_res5.arrow
```

The query sets `output_format_arrow_compression_method = 'none'` before `FORMAT Arrow`.
Clock extraction uses the effective ClickHouse timezone of `departure_time`; dates
are discarded, not converted into service calendars. Confirm that this clock matches
the intended timetable and API departure clock. For reproducibility, record the source
snapshot/table, export time, timezone, resolution and units alongside the export.
Replace the source table only with one having the same column semantics.

**Run**
Instantiate the pinned environment, then choose either the demo or real input:

```sh
julia --project=router -e 'using Pkg; Pkg.instantiate()'
julia --project=router router/serve.jl --demo
# Or, after exporting:
julia --project=router router/serve.jl data/rail_res5.arrow
```

With walking disabled (`max_walk_s=0`), `ROUTER_BACKEND=cpu` is the default and runs the kernels on `KA.CPU()`.
`ROUTER_BACKEND=reference` selects the CPU Dijkstra reference instead.
`ROUTER_BACKEND=oneapi` selects Intel GPU kernels for arrival-only point queries.
oneAPI is imported only when the point or window backend explicitly requests it;
CPU-only launch does not initialize it. The environment pins oneAPI.jl to `2.7.2`.
Walking requests always use the two-state CPU reference, regardless of configured
point/window backends. They never silently run a transit-only kernel.
On this machine, launch with the required legacy-driver prefix:

```sh
env ZE_ENABLE_ALT_DRIVERS=/usr/lib/libze_intel_gpu_legacy1.so.1 ROUTER_BACKEND=oneapi julia --project=router router/serve.jl data/rail_res5.arrow
```

`ROUTER_HOST` defaults to `127.0.0.1`; `ROUTER_PORT` defaults to `1988`.

**HTTP API**
Save a response from the running server:

```sh
curl --fail --show-error 'http://127.0.0.1:1988/reachable?index=85075dd7fffffff&departure=08:00:00&budget_s=3600&encoding=split' -o data/reachable.arrow
```

`GET /reachable` requires exactly one origin representation: `index`, a 15-digit
hexadecimal H3 string without `0x`, OR both `index_lower` and `index_upper`, unsigned
32-bit decimal words with `index = lower | (upper << 32)`. The cell must match the graph resolution.
`departure` is `HH:MM:SS`; `budget_s` is an integer in `0..604800` (seven days).
`max_walk_s` is an integer in `0..604800`, default **3600 seconds per walking hop**;
`0` disables walking and preserves the previous transit-only output and backend paths.
Malformed, duplicate or unknown parameters return HTTP 400.

Output `encoding` is independent of the input representation and defaults to `split`:

| Encoding | Index Columns |
| --- | --- |
| `split` | `index_lower UInt32`, `index_upper UInt32` |
| `string` | `index Utf8`, canonical lowercase H3 strings, no dictionary |

Both include `value Float64`, elapsed **minutes including waiting**, and
`elapsed_ms UInt32`, the exact elapsed milliseconds. `value` is required by H3-MON.
Rows are unique and sorted by H3, include arrivals exactly at the budget cutoff,
and always include the origin at elapsed zero. An off-graph origin can walk to transit
or directly to surrounding cells; with walking disabled it returns only itself.
Single-departure responses include `distance_km` when walking is enabled or transit
distances are available, following the selected earliest-arrival itinerary.

Responses use `Content-Type: application/vnd.apache.arrow.file` and
`Arrow.write(io, table; file=true, compress=nothing, dictencode=false)`:
true IPC files with `ARROW1` at the head and footer, no IPC compression or dictionaries.

**Estimated Walking**
Walk duration is the great-circle distance between H3 centres at **5 km/h**, rounded
up to integer milliseconds. Both the per-hop limit and remaining journey budget are
inclusive. Walking kilometres are added to the selected transit itinerary, not used
as a secondary shortest-path objective. No roads, terrain, water barriers or stop
coordinates are used. At res5 a default 5 km walk may reach no other cell centre;
nearby stops within one coarse cell still transfer freely.

Every origin can start a walk. Transit, including a scheduled within-cell self-edge,
restores walking eligibility. A walked arrival alone cannot start another walk.
Separate earliest-arrival and walk-eligible labels preserve later transit arrivals
that permit useful onward walks. Final geographic cells without transit are terminal
destinations, not stepping stones for chained walking.

```text
/reachable?index=871fb4662ffffff&departure=08:00:00&budget_s=10800&max_walk_s=3600
```

The service retains one spatial index. Request-local caches share geometry between
window samples; no walking arrival-label reuse or GPU routing is enabled yet.
The following walking-only resource guards return **HTTP 422**, never partial Arrow
or silently clipped reachability:

- At most **250,000 unique output cells**, including the origin and the entire window
  union. Embedders can change this with `make_handler(...; max_cells=...)`.
- At most **2,000,000 candidate slots per geographic enumeration**, checked before
  allocating H3 polygon output. This conservative bound may reject a query whose
  exact filtered output would fit.
- At most **500,000,000 work visits per request**, counting per-sample graph vertices,
  transit edges, spatial candidates, walking hops and window aggregation. This is a
  work bound, not a wall-clock timeout. Reduce the window, journey budget or hop limit,
  or increase `step_s` when it is exceeded.

Geometry caches retain at most 2,000,000 hops; once full, queries recompute uncached
geometry without dropping destinations. `X-Router-Max-Walk-S` exposes the requested
limit. `X-Router-Distance` is `connection-sum+estimated-walk-km` with transit distance
data, or `partial-estimated-walk-km` without it. Pure walks still have known km;
itineraries using transit without distance data have `NaN`, including later egress.

Direct Julia APIs are `route_walking` and `route_window_walking`, with `max_walk_s`,
optional resident `walking_index=WalkingIndex(graph)`, and `max_cells` keywords.
They return a sorted `h3` vector alongside aligned result columns. Existing
`route_cpu`, `route_details`, `route_window` and kernel APIs remain transit-only.
See [walking results](walking-results.md) for real res5/res6/res7 validation and timings.

**Departure Windows**
Add `window_s` to average departures in `[departure, departure + window_s)`.
The window may cross midnight and lasts at most 86400 seconds. `step_s` defaults to
60 and must be an integer from 1 to 86400; it requires a positive window. `window_s=0`
or omission retains single-departure behavior. A step longer than the window produces
one sample, and the end of the window is never sampled.

```sh
env ROUTER_BACKEND=reference julia --project=router router/serve.jl data/rail_and_friends_dist_res5.arrow
# Whole-day departures, every minute, each with its own three-hour travel budget:
curl --fail 'http://127.0.0.1:1988/reachable?index=851fb467fffffff&departure=00:00:00&window_s=86400&step_s=60&budget_s=10800&encoding=split' -o data/day-average.arrow
```

Window output retains `value` in minutes for H3-MON and adds these statistics:

| Column | Meaning |
| --- | --- |
| `elapsed_ms Float64` | Mean elapsed time, charging the full budget for unsuccessful departures |
| `reachable_elapsed_ms Float64` | Mean elapsed time over successful departures only |
| `distance_km Float64` | Mean selected-route length over those same successful departures |
| `reachable_fraction Float64` | Successful samples divided by all samples |
| `reachable_samples UInt32` | Number of departures reaching the cell within budget |
| `sample_count UInt32` | Total sampled departures |

The response contains cells reachable at least once plus the origin. The origin has
zero time/distance and full coverage. Unknown itinerary distance is `NaN`, not zero.
A window distance mean is `NaN` if any successful sample has unknown kilometres;
unknown samples are not excluded from that mean. With walking disabled,
`X-Router-Distance` reports `unavailable` or `connection-sum-km`.
The internal `route_window` result includes every graph node, assigning the full budget
to never-reachable nodes; the HTTP output omits those nodes as before.

Walking windows run independent two-state searches per departure, including geographic
egress, then average the collapsed per-cell results chronologically. They report
`X-Router-Backend: reference`, `X-Router-Window-Strategy: walking_reference`, one worker,
one search per sample and zero reused samples. This correctness baseline is slower
than transit-only catch-up; walking-aware catch-up remains pending.

With `max_walk_s=0`, window routing is selected independently of `ROUTER_BACKEND`:

| `ROUTER_WINDOW_BACKEND` | Implementation |
| --- | --- |
| `catchup` (default) | CPU downstream arrival repair with cached profile indices |
| `origin` | Original CPU reference with first-hop grouping only |
| `oneapi` | Batched Intel GPU arrival labels, CPU canonical kilometre replay |
| `ka_cpu` | Same batched kernels on CPU for verification |

`ROUTER_WINDOW_CHUNK` defaults to 64 (1..256) for CPU catchup.
`ROUTER_WINDOW_WORKERS` defaults to up to four available Julia default-pool threads
(1..256 requested, capped by the thread pool and number of chunks). Start Julia with
`--threads=4` to use four workers; without extra Julia threads execution stays serial.
Each slot owns a private reusable workspace. Waves run in parallel, then aggregate
in chronological order, preserving exact floating-point means. Set workers to 1
for an explicit serial comparison. `X-Router-Workers` reports the actual count.

```sh
env ROUTER_BACKEND=reference julia --project=router --threads=4 router/serve.jl data/rail_and_friends_dist_res5.arrow
```

The batched engines use `ROUTER_WINDOW_BATCH`, default 32 (1..256), and
`ROUTER_WINDOW_CHECK_EVERY`, default 4 (1..32), for host convergence checks.
To keep point queries on the CPU reference while explicitly enabling GPU windows:

```sh
env ZE_ENABLE_ALT_DRIVERS=/usr/lib/libze_intel_gpu_legacy1.so.1 ROUTER_BACKEND=reference ROUTER_WINDOW_BACKEND=oneapi julia --project=router router/serve.jl data/rail_and_friends_dist_res5.arrow
```

Distance-bearing point queries still use CPU Dijkstra regardless of backend selection.
`make_handler` accepts a separate `window_route` callback, defaulting to CPU catchup
with up to four available workers.
CPU `origin` and `catchup` windows retain `X-Router-Backend: reference`;
`X-Router-Window-Strategy` distinguishes `origin`, `catchup`, `gpu_batched`, and
`ka_cpu_batched`. `X-Router-Searches` and `X-Router-Reused-Samples` retain their
grouping meanings. When present, `X-Router-Full-Searches`, `X-Router-Repair-Searches`,
`X-Router-Profile-Lookups`, `X-Router-Batches`, and `X-Router-Rounds` expose engine work.
Transit-only query metrics, output fields and bitwise-identical means are preserved
when `max_walk_s=0`.

Reuse compares absolute first-hop arrival/distance labels, ignoring useless self-edges,
across adjacent samples. When they are unchanged, all non-origin routing results are
unchanged. One search covers that group, using the last sample's cutoff so newly
admitted destinations are not lost. Integer arithmetic sums the group's changing
elapsed times and capped penalties without expanding one result per sample. There is
no cache across requests. These transit-only engines retain source first-hop grouping;
it is deliberately not applied to walking requests.
Catchup additionally processes groups backward inside bounded chunks, repairing only
decreased arrival labels and caching profile indices for processed tails. It replays
canonical kilometres per group, then aggregates chronologically to preserve exact means;
it does not memoize complete kilometre chains. GPU groups are independent but batched,
with kilometre replay and mean aggregation still on the CPU.
The direct `route_window` API remains the origin reference; `route_window_cached`
is the optimized CPU API. `route_window(...; reuse=false)` disables source grouping
for an independent-search comparison.
See [`window-results.md`](window-results.md) for historical first-hop reuse measurements
and verification, including res6/res7 loading, and
[`window-optimization-results.md`](window-optimization-results.md) for the
new downstream-cache and batched-iGPU measurements. Catchup improves the measured wide
windows; short or sparse sweeps may favor `origin`, which remains an explicit override.

**Selected-Route Kilometres**
Distance is attached to each retained schedule connection, not to an H3 edge group.
It follows the actual connection selected during Dijkstra relaxation, including when
a later departure overtakes an earlier one. Equal-arrival connections prefer the later
departure during profile construction; identical departure/arrival pairs prefer the
shorter segment. Between different paths, the first earliest label wins deterministically.
This does not optimize distance among all earliest-arrival paths.

The export's kilometres sum geodesic distances between successive stops along the
selected coarse itinerary. They are not origin-to-destination straight-line distance
or railway-track length. Free movement between stops inside a single H3 cell remains
unmeasured, so coarse resolutions can understate physical journey length substantially.
Use finer input graphs or better upstream segment geometry when that distinction matters.

The original four rail/rail-and-friends files have only the original four columns;
`data/rail_and_friends_dist_res5.arrow` is the distance-enriched benchmark input.
Re-export with the updated `export.sql` to obtain meaningful kilometres for other inputs;
four-column files can already answer time-window queries. Keep `distance_km` in `DISTINCT`
and `ORDER BY`, and retain the transport filter and resolution appropriate to your file.

**Distance Minus Time Quantile**
Add `metric=distance_time_quantile` to a point or window request. The default remains
`metric=time`. This mode requires an input `distance_km` column; otherwise it returns
HTTP 400 rather than ranking unavailable distances.

```text
/reachable?index=851fb467fffffff&departure=00:00:00&window_s=86400&step_s=60&budget_s=10800&metric=distance_time_quantile
```

For windows, averaging happens first. The router ranks the returned mean `distance_km`
and budget-capped mean `elapsed_ms`, then returns:

```text
value = distance_quantile - time_quantile
```

This is not the average of per-departure rank differences, and does not rank the
conditional `reachable_elapsed_ms`. Both ranks use the same returned cells with finite
distance and time, including the origin, not just cells visible in the viewport.
Ties share a rank; the empirical-CDF ranks are rescaled to 0..1 as in the old plotting
helper. Constant columns and singleton results receive rank zero. No rounding is
applied to the input means before ranking.

The result retains the underlying time, distance and coverage columns and adds
`distance_quantile` and `time_quantile`. `value` is now dimensionless, in -1..1:
positive means a higher distance rank than time rank, negative the reverse.
`X-Router-Metric` identifies the selected mode. Update the H3-MON title accordingly;
its colour-quantile settings do not calculate this difference themselves.

**H3-MON Snapshots**
H3-MON now supports this API through `onclick`/`onmove` JSON metadata; its
`www/data/reachable.json` and `reachable.csv` provide a runnable res5 example.
For static snapshots, it reads `www/data/<name>.arrow` with an optional same-basename
`.json` sidecar. To publish a response into the existing
sibling checkout, run this yourself from this repository:

```sh
curl --fail --show-error 'http://127.0.0.1:1988/reachable?index=85075dd7fffffff&departure=08:00:00&budget_s=3600&encoding=split' -o ../H3-MON/www/data/reachable.arrow
```

With the frontend running on port 1983, open
`http://localhost:1983/?data=reachable.arrow&cartogram=none&quantileSource=map`.
An optional `reachable.json` can set the display title and disable spatial infill:

```json
{"t":"Travel time (minutes)","raw":false,"quantileSource":"map","cartogram":"none","infill":false,"defaultValue":null}
```

The installed `@loaders.gl/arrow` 4.3.3 / `apache-arrow` 19.0.1 frontend accepts IPC
files and streams but buffers the full `arrayBuffer()`, not progressive streaming.
Keep IPC uncompressed and strings plain, never dictionary-encoded, including extra
columns. See H3-MON `src/app.js:372` for parsing and `src/app.js:2184` for `value` use.
No frontend changes are required for this snapshot workflow.

**Tests**
The default suite uses CPU; `--backend=oneapi` adds GPU tests. `ROUTER_FRONTEND`
enables checking emitted bytes with the installed frontend's actual Node ArrowLoader.

```sh
julia --project=router --threads=4 router/test/runtests.jl
env ZE_ENABLE_ALT_DRIVERS=/usr/lib/libze_intel_gpu_legacy1.so.1 julia --project=router --threads=4 router/test/runtests.jl --backend=oneapi
env ROUTER_FRONTEND=/home/olie/projects/H3-MON julia --project=router --threads=4 router/test/runtests.jl
```

Validated on the P630 with Julia 1.12.7 using CPU/oneAPI differential tests,
the frontend reader, and a live HTTP launcher smoke test. The supplied res5 export
contains 3,254,648 connections: startup skips 27 negative-duration self-edges and
29 roughly 56-year durations, retaining 3,254,592 connections and 13,832 cells.
The large durations all have zero departure clocks and resemble epoch-to-2026
timestamp differences; the four-column export cannot establish their exact provenance.
Nine real-network queries across three origins and three horizons matched Dijkstra
on both KA CPU and oneAPI. These are correctness checks, not performance benchmarks.

**CPU/GPU Benchmarks**
See [`benchmark-results.md`](benchmark-results.md) for the 2026-09-06 measurements.
Packed CPU Dijkstra was faster than the current one-query GPU router in every tested
case by median, including Arrow output. For interactive use, select
`ROUTER_BACKEND=reference`; the existing `cpu` default is the slower KA CPU algorithm.
The point-query default has not been changed. These historical measurements do not
cover the new batched window engine; see the window optimization report linked above.
The same result held for the larger `rail_and_friends_res5.arrow` export (16.9 million
connections); its measurements are included in the report.

The benchmark warms all paths, checks identical labels and Arrow bytes, and saves raw
samples plus environment/data hashes. Run from the repository root with a new output directory:

```sh
env ZE_ENABLE_ALT_DRIVERS=/usr/lib/libze_intel_gpu_legacy1.so.1 julia --project=router --threads=4 router/benchmark.jl data/rail_res5.arrow data/router-benchmark 20
```
