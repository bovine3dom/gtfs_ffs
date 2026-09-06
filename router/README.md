**Timetable Router**
Earliest-arrival reachability on a uniform-resolution H3 graph, with a
repeating fantasy daily timetable. Run the commands below from the repository root.

**Model**
- All stops within one cell are freely interchangeable, with zero transfer cost.
- There is no intercell walking, access or egress, or generated shuttle service.
- Departures repeat every 24 hours; waiting, including overnight waiting, counts.
- Real dates, service calendars and trip continuity are not represented.
- Only cells present in the input form the graph. Results are an approximation,
  not a guarantee of reachability for every geographic cell or point in a cell.
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

`ROUTER_BACKEND=cpu` is the default and runs the kernels on `KA.CPU()`.
`ROUTER_BACKEND=reference` selects the CPU Dijkstra reference instead.
`ROUTER_BACKEND=oneapi` selects Intel GPU kernels; only that launcher branch imports
oneAPI, so CPU launch does not initialize it. The environment pins oneAPI.jl to `2.7.2`.
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
Malformed, duplicate or unknown parameters return HTTP 400.

Output `encoding` is independent of the input representation and defaults to `split`:

| Encoding | Index Columns |
| --- | --- |
| `split` | `index_lower UInt32`, `index_upper UInt32` |
| `string` | `index Utf8`, canonical lowercase H3 strings, no dictionary |

Both include `value Float64`, elapsed **minutes including waiting**, and
`elapsed_ms UInt32`, the exact elapsed milliseconds. `value` is required by H3-MON.
Rows are unique and sorted by H3, include arrivals exactly at the budget cutoff,
and always include the origin at elapsed zero. A valid off-graph origin returns only itself.
When `distance_km` is available in the graph, single-departure responses also include
that column for the selected earliest-arrival itinerary.

Responses use `Content-Type: application/vnd.apache.arrow.file` and
`Arrow.write(io, table; file=true, compress=nothing, dictencode=false)`:
true IPC files with `ARROW1` at the head and footer, no IPC compression or dictionaries.

**Departure Windows**
Add `window_s` to average departures in `[departure, departure + window_s)`.
The window may cross midnight and lasts at most 86400 seconds. `step_s` defaults to
60 and must be an integer from 1 to 86400; it requires a positive window. `window_s=0`
or omission retains single-departure behavior. A step longer than the window produces
one sample, and the end of the window is never sampled.

```sh
env ROUTER_BACKEND=reference julia --project=router router/serve.jl data/rail_and_friends_res5.arrow
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
zero time/distance and full coverage. Missing distance data is `NaN`, not zero (except
the zero-leg origin). `X-Router-Distance` reports `unavailable` or `connection-sum-km`.
The internal `route_window` result includes every graph node, assigning the full budget
to never-reachable nodes; the HTTP output omits those nodes as before.

Windows and distance-bearing point queries use CPU Dijkstra, regardless of the selected
arrival-only backend. `X-Router-Backend: reference` identifies these responses.
`X-Router-Searches` and `X-Router-Reused-Samples` expose within-call reuse for windows.
The existing KA CPU/oneAPI kernels remain available for arrival-only point queries.

Reuse compares absolute first-hop arrival/distance labels, ignoring useless self-edges,
across adjacent samples. When they are unchanged, all non-origin routing results are
unchanged. One search covers that group, using the last sample's cutoff so newly
admitted destinations are not lost. Integer arithmetic sums the group's changing
elapsed times and capped penalties without expanding one result per sample. There is
no cache across requests. `route_window(...; reuse=false)` is available for comparison.
See [`window-results.md`](window-results.md) for real-network reuse measurements and
verification, including res6/res7 input loading.

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

All four currently supplied rail/rail-and-friends files have only the original four
columns. Re-export with the updated `export.sql` to obtain meaningful kilometres;
existing files can already answer time-window queries. Keep `distance_km` in `DISTINCT`
and `ORDER BY`, and retain the transport filter and resolution appropriate to your file.

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
The default has not been changed. Batched GPU throughput has not yet been measured.
The same result held for the larger `rail_and_friends_res5.arrow` export (16.9 million
connections); its measurements are included in the report.

The benchmark warms all paths, checks identical labels and Arrow bytes, and saves raw
samples plus environment/data hashes. Run from the repository root with a new output directory:

```sh
env ZE_ENABLE_ALT_DRIVERS=/usr/lib/libze_intel_gpu_legacy1.so.1 julia --project=router --threads=4 router/benchmark.jl data/rail_res5.arrow data/router-benchmark 20
```
