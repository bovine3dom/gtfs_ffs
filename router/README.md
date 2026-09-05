**Res5 Rail Router**
Earliest-arrival reachability on a coarse H3 resolution-5 rail graph, with a
repeating fantasy daily timetable. Run the commands below from the repository root.

**Model**
- All stops within one cell are freely interchangeable, with zero transfer cost.
- There is no intercell walking, access or egress, or generated shuttle service.
- Departures repeat every 24 hours; waiting, including overnight waiting, counts.
- Real dates, service calendars, trip continuity and paths are not represented.
- Only cells present in the input form the graph. Results are an approximation,
  not a guarantee of reachability for every geographic cell or point in a cell.
- `--demo` uses a small synthetic fixture, not the real rail export.

**Input And Export**
The loader requires these four columns with exactly these non-null element types:

| Column | Type | Valid Values |
| --- | --- | --- |
| `from_h3` | `UInt64` | Valid H3 resolution-5 cell |
| `to_h3` | `UInt64` | Valid H3 resolution-5 cell |
| `departure_ms` | `UInt32` | `0..86399999`, milliseconds since midnight |
| `duration_ms` | `Int64` | `0..604800000`, milliseconds, at most seven days |

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
It retains rail types `2` and `100..117`, maps endpoints to resolution 5, deduplicates
the four projected values and sorts by all four. Self-edges and negative durations
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
32-bit decimal words with `index = lower | (upper << 32)`. The cell must be valid res5.
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

Responses use `Content-Type: application/vnd.apache.arrow.file` and
`Arrow.write(io, table; file=true, compress=nothing, dictencode=false)`:
true IPC files with `ARROW1` at the head and footer, no IPC compression or dictionaries.

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
