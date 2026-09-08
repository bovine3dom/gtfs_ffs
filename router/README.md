**Timetable Router**
Earliest-arrival reachability on a uniform-resolution H3 graph, with a
repeating fantasy daily timetable. Run the commands below from the repository root.

**Model**
- All stops within one cell are freely interchangeable, with zero transfer cost.
- Estimated walking provides access, transfers, direct walking and geographic egress.
  Walks are available at any time, but consecutive walks are forbidden.
- Departures repeat every 24 hours; waiting, including overnight waiting, counts.
- Real dates, service calendars and trip continuity are not represented.
- Transit vertices come from the input and the file-server rail repair below.
  Walking also returns geographic cells at the graph resolution. Cell-centre estimates
  are not street routing or a guarantee of reachability for every point in a cell.
- `--demo` uses a small synthetic fixture, not the real rail export.

**Input And Export**
This timetable-file format is separate from the floating-hour HTTP/WebSocket API.
The integer-millisecond input and low-level routing engine formats are unchanged;
no dataset rewrite or SQL export change is needed for the query-unit migration.
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

**Elvas-Badajoz Repair**
File-backed `serve.jl` loads always enable `pack_graph(path; badajoz_shuttle=true)`.
The default `pack_graph` and `--demo` remain unpatched. This restores the fantasy
rail service from `plots/longest_journey.jl:46-55`, not an actual published timetable
or a walking/road connection: 0.25 hours, 13.88 itinerary km, both directions,
every 1/60 hour from 4 through 23.5 hours inclusive (1,171 per direction, 2,342 rows).
The daily clock uses the same convention as the input, without timezone conversion.

`src/missing_data.jl` uses actual railway-station coordinates from OpenStreetMap
(retrieved 2026-09-07, [ODbL attribution](https://www.openstreetmap.org/copyright)):

| Station | Latitude, Longitude | OSM Source | Derived H3 res11 |
| --- | --- | --- | --- |
| Elvas | `38.8955418, -7.1422766` | [node 10784532230, version 4](https://www.openstreetmap.org/node/10784532230/history/4) | `8b3902851d9bfff` |
| Badajoz | `38.8907326, -6.9816158` | [node 2962633346, version 7](https://www.openstreetmap.org/node/2962633346/history/7) | `8b3902ba1802fff` |

Both nodes are tagged `railway=station`, `public_transport=station`, `train=yes`;
Elvas has station reference `57497` and Badajoz `37606` / UIC `7137606`.
These are not town centres. The original ClickHouse table
`transitous_everything_20260218_edgelist_fahrtle2` and connection helper
`plots/lib.jl:4-13` were located, but localhost native port 9000 and HTTP ports
8110/8123 were unavailable. Thus these are verified OSM station positions, **not
claimed to be the original first name-matched GTFS res11 cells**.

Endpoints follow the export convention: station position to res11, then parent
at the inferred graph resolution (finer-than-11 graphs use the position directly).
Elvas/Badajoz parents are `85390287fffffff` / `853902bbfffffff` at res5,
`863902857ffffff` / `863902ba7ffffff` at res6, and
`873902851ffffff` / `873902ba1ffffff` at res7. Coalesced endpoints retain a
scheduled transit self-edge, consistent with input self-edges and walking reset rules.

The repair runs once at load time, logs its row count, and uses read-only column
concatenation rather than copying the large input arrays. Existing connections,
including faster services, share normal FIFO profile pruning with the shuttle;
exact duplicates are pruned there too. Neither caller columns nor the Arrow file
are modified. Four-column input keeps `distance_km=nothing`: the repair does not
invent distances for unknown input services. Straight-line distance still measures
cell-centre separation, not 13.88 km. File graphs receive this rail repair regardless
of their original transport filter, since the packed schema retains no mode metadata.

Offline verification on 2026-09-07 read `data/rail_and_friends_res6.arrow` (34,921,863
rows): both station parents were already present; zero rows matched the full shuttle
signature. One patched pack took 81.6 seconds including compilation (6.53 GB cumulative
allocations, not peak memory), yielding 35,760 nodes, 131,968 edges and 22,444,069
two-day profile entries. The existing duration filter skipped 4,464 invalid rows.
Both directions at 08:00 and 23:30 arrived in exactly 900,000 ms with 13.88 itinerary
km and walking disabled. The input size/mtime stayed unchanged; no live server was
queried, signalled or restarted. The running server therefore needs a later normal
restart to use this repair.

Regression coverage is in `test/shuttle_tests.jl`: res5/6/7 endpoints, schedule
boundaries and cutoffs, reverse service, FIFO merging, legacy distance absence,
nonmutation, Arrow loading, CPU/reference/catch-up/batched windows, HTTP metrics,
and walking access-transit-egress. The full suite passed with both `-t 1` and `-t 4`:
`julia --project=router -t 1 router/test/runtests.jl` (repeat with `-t 4`).

**Run**
Instantiate the pinned environment, then choose either the demo or real input:

```sh
julia --project=router -e 'using Pkg; Pkg.instantiate()'
julia --project=router router/serve.jl --demo
# Or, after exporting:
julia --project=router router/serve.jl data/rail_res5.arrow
```

To keep several resolutions resident in one server:

```sh
julia --threads=8 --project=router router/serve.jl data/rail_res5.arrow data/rail_res6.arrow data/rail_res7.arrow
```

Files load and prepare sequentially before listening, including the Elvas-Badajoz
repair and one resident walking index per file. Each graph's resolution is inferred
from its H3 cells, not its filename. Only one graph per resolution is allowed;
duplicates fail startup with both filenames and the resolution. `--demo` remains
valid alone and cannot be mixed with files.

HTTP `/reachable` and queries on the same `/query` WebSocket select the graph by
the origin H3 cell's encoded resolution, with either hexadecimal or split-word
input. No dataset/resolution parameter, coordinate conversion, or resampling is
introduced. An unloaded resolution returns HTTP 400 (`no graph loaded for H3
resolution N`); WebSocket validation keeps its generic safe error and permits the
next valid query. Latest-pending scheduling remains per connection, even when
switching resolutions. Changing routing resolution changes the within-cell
transfer approximation, not just display detail.

All graphs, walking indexes and configured CPU/GPU workspaces stay resident;
budget RAM and device memory for their **sum**, plus startup/query scratch space.
Sequential preparation avoids concurrent large graph builds, not the summed
resident cost. Backend callbacks and device caches belong to each graph, while
one shared routing lock preserves serialized HTTP/WebSocket workspace use across
all resolutions. CPU window workers still use all Julia default-pool threads.
Changing files requires a restart; transport-type selection remains deferred.

With walking disabled (`max_walk_h=0`), `ROUTER_BACKEND=cpu` is the default and runs the kernels on `KA.CPU()`.
`ROUTER_BACKEND=reference` selects the CPU Dijkstra reference instead.
`ROUTER_BACKEND=oneapi` selects Intel GPU kernels for arrival-only point queries.
oneAPI is imported only when the point or window backend explicitly requests it;
CPU-only launch does not initialize it. The environment pins oneAPI.jl to `2.7.2`.
Walking requests always use two-state CPU routing, never transit-only GPU kernels.
Walking windows use parallel catch-up by default; `ROUTER_WINDOW_BACKEND=origin`
selects the independent walking reference for comparison.
On this machine, launch with the required legacy-driver prefix:

```sh
env ZE_ENABLE_ALT_DRIVERS=/usr/lib/libze_intel_gpu_legacy1.so.1 ROUTER_BACKEND=oneapi julia --project=router router/serve.jl data/rail_res5.arrow
```

`ROUTER_HOST` defaults to `127.0.0.1`; `ROUTER_PORT` defaults to `1988`.

<a id="query-websocket"></a>
**Query WebSocket**
`ws://127.0.0.1:1988/query` shares the HTTP server, resident index, routing callbacks,
workspace lock and Arrow encoder. Both transports use the same floating-hour query contract.
No frontend configuration or files are changed automatically. The client contract is
[H3-MON's query protocol](../../H3-MON/docs/query-websocket.md).

Browser origins are allowed by default, including `null` origins, with no configuration
required. Requests without Origin are also accepted. Optionally restrict browser access
by setting `ROUTER_WS_ORIGINS` to exact page origins (scheme, host and port):

```sh
ROUTER_WS_ORIGINS=http://localhost:8000,http://127.0.0.1:8000 julia --threads=4 --project=router router/serve.jl --demo
```

An unset or empty list allows all origins. Nonempty comma-separated entries are trimmed
and compared exactly, not by prefix; nonmatching origins receive HTTP 403. Explicit
allowlists cannot contain `null` or wildcards. Duplicate Origin headers are rejected.
There is **no built-in authentication**; Origin is not identity and can be forged by
non-browser clients. Keep the default loopback bind for local use. Remote deployments
must authenticate/authorize at a trusted reverse proxy, restrict direct backend access,
and terminate TLS with WebSocket upgrade forwarding for `wss://` on HTTPS pages.
Avoid payload/URL logging at the proxy if parameters contain sensitive information.

Send one JSON text message per query, with no subprotocol or acknowledgement:

```json
{"type":"query","id":42,"url":"/reachable?index=85075dd7fffffff&departure_h=8&budget_h=1&encoding=split"}
```

- IDs are increasing, connection-local JSON integers in `1..4294967295`; gaps and
  integer-valued numbers such as `42.0` are allowed, booleans are not. Reconnect before
  wrapping. A valid ID is consumed even if its type or URL validation fails.
- Success is one binary message: four big-endian ID bytes, then the **complete Arrow
  IPC file**, identical to the HTTP response body. For 42 the prefix is `00 00 00 2a`.
  Remove those four bytes before decoding Arrow; every result includes its schema.
- Errors are text `{"type":"error","id":42,"message":"query failed"}`. Validation
  errors use fixed safe messages, never reflected parameters or exception details.
- Only `/reachable` plus an optional query string is dispatched in-process. Absolute
  URLs, credentials/authority, fragments, other paths, raw whitespace/control characters
  and backslashes are rejected. Parameters and defaults are the HTTP API's, including
  both metrics, encodings, windows, walking and default `distance_mode=itinerary`.
- Unknown JSON fields are ignored. Wrong type/URL and parameter errors use the same
  serialized response lane and may be superseded while pending. Malformed JSON, binary
  requests, missing/invalid/reused/decreasing IDs close with policy code **1008**, not
  an invented ID-zero error. HTTP.jl retains its standard malformed-frame/UTF-8 policy.
- Each connection has one active job and one newest pending job. While 42 runs,
  arrivals 43, 44, 45 replace that slot: finish/send 42, then run 45. There are no
  ACK, done, cancel, progressive chunk or heartbeat messages at the application level.
  Close clears pending; already dispatched work finishes without cancellation and
  its result is discarded. A failed send stops further dispatch.

Routing across HTTP and all sockets remains serialized by the original handler lock.
The socket reader does not wait on that lock. Tasks yield before pending dispatch so
buffered arrivals can coalesce. Julia scheduling is cooperative: with only one runtime
thread, a non-yielding CPU calculation can delay reads until it finishes; there is no
preemption or latency guarantee, including when all available workers are busy.
No thread count is required, no worker process is created, and no query resource caps
are added. Large valid requests can still consume substantial CPU, memory and bandwidth.
The pending slot bounds jobs, not transport buffers, result sizes or connection counts.

For embedding, keep `make_handler(graph; ...)` for the shipped request-handler API;
wrap that **same instance** using `make_stream_handler(handler; origins=[...])` and
`HTTP.serve!(...; stream=true)`. Plain `/query` requests return HTTP 426.

**HTTP API**
The same server also supports the optional [query WebSocket API](#query-websocket).

Save a response from the running server:

```sh
curl --fail --show-error 'http://127.0.0.1:1988/reachable?index=85075dd7fffffff&departure_h=8&budget_h=1&encoding=split' -o data/reachable.arrow
```

`GET /reachable` requires exactly one origin representation: `index`, a 15-digit
hexadecimal H3 string without `0x`, OR both `index_lower` and `index_upper`, unsigned
32-bit decimal words with `index = lower | (upper << 32)`. The cell must match the graph resolution.
All query times are finite floating-point **hours**, accepting decimals and scientific notation:

| Parameter | Range | Default |
| --- | --- | --- |
| `departure_h` | `0 <= h < 24`, hours after midnight | Required |
| `budget_h` | `0..168` (seven days) | Required |
| `window_h` | `0..24`; zero disables sampling | `0` |
| `step_h` | `0 < h <= 24`; explicit use requires a positive window | `1/60` hour |
| `max_walk_h` | `0..168`, per walking hop; zero disables walking | `1` hour |

This is a breaking rename: `departure=HH:MM:SS`, `budget_s`, `window_s`, `step_s`
and `max_walk_s` are rejected, not interpreted as aliases. Values are parsed as
Float64, range-checked, then rounded once to integer milliseconds using Julia
`RoundNearest` (ties to even). Departures rounding to 24 hours are rejected.
Negative values, including signed negative zero, are rejected.
Positive windows and steps must round to at least 1 ms; smaller positive budgets
and walking limits may round to zero. Output time columns are Float64 hours at
this engine resolution (window means may contain fractional milliseconds).
After rounding, `ceil(window_ms / step_ms)` must not exceed the existing engine
limit of 86,400 samples; excessive requests return HTTP 400 before routing.
At a 1 ms step this allows at most 0.024 hours (86.4 seconds) of departures.
This adds no restriction to formerly valid seconds-based requests: the old minimum
step of one second already guaranteed at most 86,400 samples in a one-day window.
Zero walking uses transit-only backend paths. Distances remain kilometres.
Malformed, duplicate or unknown parameters return HTTP 400.

Output `encoding` is independent of the input representation and defaults to `split`:

| Encoding | Index Columns |
| --- | --- |
| `split` | `index_lower UInt32`, `index_upper UInt32` |
| `string` | `index Utf8`, canonical lowercase H3 strings, no dictionary |

Both include `value Float64`, elapsed **hours including waiting** for `metric=time`,
and `elapsed_h Float64`, the same elapsed hours. `value` is required by H3-MON.
No transport time columns retain `_ms` names.
Rows are unique and sorted by H3, include arrivals exactly at the budget cutoff,
and always include the origin at elapsed zero. An off-graph origin can walk to transit
or directly to surrounding cells; with walking disabled it returns only itself.
Single-departure responses include `distance_km` in straight-line mode, when walking
is enabled or transit distances are available. Its meaning follows the selected
distance mode below.

Responses use `Content-Type: application/vnd.apache.arrow.file` and
`Arrow.write(io, table; file=true, compress=nothing, dictencode=false)`:
true IPC files with `ARROW1` at the head and footer, no IPC compression or dictionaries.

**Distance Modes**
`distance_mode=itinerary` is the default. Omission preserves existing Arrow bodies,
chosen-itinerary distances, tie handling, overflow checks and distance headers.
Use `distance_mode=straight_line` explicitly for faster CPU arrival-only routing.
No frontend selection or default is changed.

```text
/reachable?index=871fb4662ffffff&departure_h=0&budget_h=168&window_h=24&step_h=0.25&max_walk_h=1&metric=distance_time_quantile&distance_mode=straight_line
```

In straight-line mode, `distance_km` is the origin-to-destination **H3-centre
great-circle distance**, using H3's WGS84 authalic sphere (radius 6371.007180918475 km).
It is not route geometry, accumulated transit/walking distance, or a road distance.
The origin is exactly zero. Distances are computed once per returned final cell,
after window aggregation, not averaged across departure samples. Travel times,
coverage counts and conditional/unconditional time means are unchanged.
`metric=time` still returns this distance column; `metric=distance_time_quantile`
ranks these OD distances and subtracts time ranks, so its values deliberately differ
from itinerary-distance ranks. Straight-line mode needs **no input `distance_km`
column**; the existing missing-column guard still applies to itinerary quantiles.

`X-Router-Distance-Mode` exposes the selected mode through CORS.
Straight-line responses have `X-Router-Distance: origin-destination-great-circle-km`;
itinerary distance headers retain their existing values.
Straight-line HTTP routing explicitly uses the CPU reference backend, including
`max_walk_h=0`; it does not call configured itinerary-only CPU/oneAPI callbacks.
`X-Router-Backend: reference` and the window strategy identify the actual engine.
The launcher honors walking `origin` versus catch-up selection and configured
workers/chunks; transit-only straight-line windows use CPU catch-up.

Julia APIs `route_walking`, `route_window_walking`, `route_window_walking_cached`
and transit-only `route_window_cached` accept `distance_mode="itinerary"` or
`distance_mode="straight_line"` (the equivalent Symbols are also accepted).
Invalid values/types throw `ArgumentError` before routing. `route_cpu` already
returns arrival-only labels and remains unchanged. Straight-line catch-up retains
two-state arrival repair but omits canonical km replay, connection caches needed
only by replay, km scratch, per-sample km columns and km averaging. Indexed point
columns are 8 bytes per cell (Int32 ID + UInt32 arrival), versus 16 for itinerary.
The independent point/window search and unprepared/larger-radius fallback also
skip km propagation. Walking-hop geometry for durations is unchanged.
See [straight-line results](straight-distance-results.md) and
[`benchmark-distance-modes.jl`](benchmark-distance-modes.jl) for matched measurements.

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
/reachable?index=871fb4662ffffff&departure_h=8&budget_h=3&max_walk_h=1
```

The service eagerly prepares one resident walking index per loaded graph at handler
construction, before accepting requests. Every graph vertex gets exact geographic
neighbors within 3,600,000 ms (5 km), with the graph subset derived from that same list.
Contiguous offsets, targets, durations and kilometres preserve canonical H3 order;
graph targets are precomputed integer node IDs. Up to four Julia threads build private
lists, then pack deterministically. This adds startup time and retained memory
proportional to the full adjacency, especially at fine resolutions, without resource caps.

Prepared catch-up windows also use stable integer IDs for the union of graph and
geographic destinations. Per-worker arrival/km arrays reset only touched output slots;
reusable sample buffers hold IDs instead of H3 keys. Geographic reduction and
chronological incremental means use arrays, and only the final reachable H3 union is
sorted. Off-graph origins enumerate direct geographic destinations once per request
and extend the output IDs locally, without losing the indexed graph egress path.
Larger hop limits and unprepared indexes retain the exact dictionary fallback.
The independent `route_window_walking` oracle and point API retain itinerary defaults.
Search counters and the `walking_catchup` HTTP strategy retain their existing meanings.

Prepared hits borrow read-only ranges without geometry calls or cache locks. Both the
requested hop limit and remaining budget filter these ranges. Requests above the
prepared radius and off-graph origins use the existing exact geometry and request-local
caches, never clipping the requested `max_walk_h=0..168`. Fallback data is shared
between window samples and workers, not retained across requests. Cached coverage is
reused when it covers the requested radius. Small expansions use a local H3 disk only
after certifying that its outer cell polygons cannot intersect the walking area;
uncertified or larger expansions fall back to complete polygon enumeration. The disk
attempts are a fast path, not an output bound or an average-edge-length approximation.
By user choice, walking has no resource caps on output cells, geographic candidates,
work or request-local geometry caches. Large valid requests may consume substantial
memory and CPU; results are not silently truncated. The existing seven-day journey
budget, one-day window, sample limits and `max_walk_h=0..168` validation remain.
`X-Router-Max-Walk-H` exposes the effective rounded limit in floating hours.
`X-Router-Distance` is `connection-sum+estimated-walk-km` with transit distance
data, or `partial-estimated-walk-km` without it. Pure walks still have known km;
itineraries using transit without distance data have `NaN`, including later egress.

Direct Julia APIs are `route_walking`, `route_window_walking` (independent reference),
and `route_window_walking_cached` (optimized windows), with `max_walk_ms`
and optional `walking_index` keywords. `WalkingIndex(graph)` remains cheap and unprepared;
explicitly use `index = prepare_walking(WalkingIndex(graph); max_walk_ms=3_600_000, workers=4)`
and pass `walking_index=index` to reuse prepared adjacency across point/window calls.
These low-level engine APIs use integer milliseconds for all time arguments,
including `max_walk_ms` (default `3_600_000`, formerly `max_walk_s=3600`).
Raw window diagnostics retain partial cells, capped `elapsed_ms` and conditional
`reachable_elapsed_ms`; strict coverage filtering and hour conversion occur at export.
Treat indices and their arrays as read-only and rebuild after vertex/resolution changes.
Preparation returns a new index without modifying the original. The HTTP service always
prepares the fixed default radius; larger requests never grow the resident adjacency.
They return a sorted `h3` vector alongside aligned result columns. Existing
`route_cpu`, `route_details`, `route_window` and kernel APIs remain transit-only.
See [walking results](walking-results.md) for real res5/res6/res7 validation and timings.
See [walking optimization results](walking-optimization-results.md) for before/after
profiles, exact original-output parity, worker scaling and remaining bottlenecks.
See [resident adjacency results](walking-adjacency-results.md) for preparation cost,
memory and matched prepared/unprepared/no-walk benchmarks.
See [indexed output results](walking-output-results.md) for the actual all-modes res7
96-sample, seven-day workload, phase profile and exact same-graph comparisons.

**Departure Windows**
Add `window_h` to average departures in `[departure_h, departure_h + window_h)`.
The window may cross midnight and lasts at most 24 hours. `step_h` defaults to
1/60 hour and explicitly specifying it requires a positive window. `window_h=0`
or omission retains single-departure behavior. A step longer than the window produces
one sample, and the end of the window is never sampled.
The existing engine limit of 86,400 samples per window still applies.

```sh
env ROUTER_BACKEND=reference julia --project=router router/serve.jl data/rail_and_friends_dist_res5.arrow
# Whole-day departures, every minute, each with its own three-hour travel budget:
curl --fail 'http://127.0.0.1:1988/reachable?index=851fb467fffffff&departure_h=0&window_h=24&step_h=0.016666666666666666&budget_h=3&encoding=split' -o data/day-average.arrow
```

Window output retains `value` in hours for `metric=time` and adds these statistics:

| Column | Meaning |
| --- | --- |
| `elapsed_h Float64` | Mean elapsed hours across all sampled departures |
| `reachable_elapsed_h Float64` | Successful-departure mean hours; equals `elapsed_h` for returned cells |
| `distance_km Float64` | Itinerary: mean selected-route length over successful departures. Straight-line: origin-to-destination H3-centre distance |
| `reachable_fraction Float64` | Successful samples divided by all samples; always `1.0` |
| `reachable_samples UInt32` | Number of departures reaching the cell within budget |
| `sample_count UInt32` | Total sampled departures |

The response contains only cells reachable from **every sampled departure** within
that departure's budget, plus the origin. Partial cells are excluded **before**
quantile ranks are computed, for both distance modes and geographic walking egress.
Counts are integer counts, not time units. The origin has
zero time/distance and full coverage. Unknown itinerary distance is `NaN`, not zero.
A window distance mean is `NaN` if any successful sample has unknown kilometres;
unknown samples are not excluded from that mean. With walking disabled,
In transit-only itinerary mode, `X-Router-Distance` reports `unavailable` or `connection-sum-km`.
The internal `route_window` result includes every graph node and charges the budget
for failed samples; HTTP and WebSocket output exclude any cell with a failed sample.

Walking windows process chronological chunks backward, repairing both arrival and
walking-eligible labels and caching selected transit connections. Fresh canonical km
replay preserves itinerary ties, missing distances and overflow checks even when
unchanged downstream arrivals have different prefix distances. Geographic egress is
still evaluated per departure, then collapsed results are aggregated chronologically.

They report `X-Router-Backend: reference`, `X-Router-Window-Strategy: walking_catchup`,
and actual workers/full searches/repairs/profile lookups. `X-Router-Searches` remains
the sample count and `X-Router-Reused-Samples` remains zero: reuse is downstream repair,
not transit-only origin grouping. `ROUTER_WINDOW_CHUNK` also controls walking catch-up.
Short windows use smaller chunks to expose parallelism.

Routing workspaces remain worker-private. Geometry is shared within a request using
per-cell build locks and lock-free private cache hits. Workers prepare different parts
of the geographic surface first to avoid all waiting for the same cell. All tasks
join before results are consumed or errors propagate; aggregation preserves exact
chronological means. The serial oracle remains available through
`ROUTER_WINDOW_BACKEND=origin`, reporting `walking_reference`.

With `max_walk_h=0`, window routing is selected independently of `ROUTER_BACKEND`:

| `ROUTER_WINDOW_BACKEND` | Implementation |
| --- | --- |
| `catchup` (default) | CPU downstream arrival repair with cached profile indices |
| `origin` | Original CPU reference with first-hop grouping only |
| `oneapi` | Batched Intel GPU arrival labels, CPU canonical kilometre replay |
| `ka_cpu` | Same batched kernels on CPU for verification |

`ROUTER_WINDOW_CHUNK` defaults to 64 (1..256) for CPU catchup.
CPU catch-up windows use all available Julia default-pool threads, capped by the
number of chunks. Julia's `--threads` controls window workers: start with
`--threads=8` to allow eight workers or `--threads=1` for serial execution.
Each slot owns a private reusable workspace. Waves run in parallel, then aggregate
in chronological order, preserving exact floating-point means. The Julia routing
APIs retain a positive `workers` keyword for explicit benchmark/test comparisons.
`X-Router-Workers` reports the actual count. Startup walking preparation is unchanged
and still defaults to at most four threads.

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
with all available default-pool threads. Its `walking_window_route(h, t, b, w, s, max_walk_ms,
walking_index)` callback defaults to walking catch-up and receives the resident index.
All callback time arguments, including the walking limit, are integer milliseconds.
CPU `origin` and `catchup` windows retain `X-Router-Backend: reference`;
`X-Router-Window-Strategy` distinguishes `origin`, `catchup`, `gpu_batched`, and
`ka_cpu_batched`. `X-Router-Searches` and `X-Router-Reused-Samples` retain their
grouping meanings. When present, `X-Router-Full-Searches`, `X-Router-Repair-Searches`,
`X-Router-Profile-Lookups`, `X-Router-Batches`, and `X-Router-Rounds` expose engine work.
Transit-only query metrics, output fields and bitwise-identical means are preserved
when `max_walk_h=0`.

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
Historical benchmark reports record the former seconds/minutes query contract and
union output. Their raw engine statistics remain meaningful; use this README's
hour-based URLs and `max_walk_ms` keywords for current requests and Julia calls.
`benchmark-window-engines.jl` now takes its optional window argument in hours
(default `24`), rather than seconds.

**Selected-Route Kilometres**
In the default itinerary mode, distance is attached to each retained schedule connection, not to an H3 edge group.
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
`metric=time`. With the default `distance_mode=itinerary`, quantiles require an input
`distance_km` column; otherwise HTTP 400 is returned rather than ranking unavailable
distances. `distance_mode=straight_line` requires no input kilometres.

```text
/reachable?index=851fb467fffffff&departure_h=0&window_h=24&step_h=0.016666666666666666&budget_h=3&metric=distance_time_quantile
```

For windows, time aggregation and strict all-departures filtering happen first.
The router ranks the selected `distance_km` and mean `elapsed_h`, then returns:

```text
value = distance_quantile - time_quantile
```

This is not the average of per-departure rank differences. For returned cells,
`reachable_elapsed_h` equals `elapsed_h`. Both ranks use the same returned cells with finite
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
curl --fail --show-error 'http://127.0.0.1:1988/reachable?index=85075dd7fffffff&departure_h=8&budget_h=1&encoding=split' -o ../H3-MON/www/data/reachable.arrow
```

With the frontend running on port 1983, open
`http://localhost:1983/?data=reachable.arrow&cartogram=none&quantileSource=map`.
An optional `reachable.json` can set the display title and disable spatial infill:

```json
{"t":"Travel time (hours)","raw":false,"quantileSource":"map","cartogram":"none","infill":false,"defaultValue":null}
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
For NVIDIA, the window benchmark accepts `--backend=cuda`; `--gpu` retains Intel
oneAPI. CUDA is an optional benchmark dependency: install it on the NVIDIA machine.
For a GTX 1080 Ti, select CUDA 12.9 (CUDA 13 cannot target Pascal):

```sh
julia --project=router -e 'using Pkg; Pkg.add("CUDA")'
julia --project=router -e 'using CUDA; CUDA.set_runtime_version!(v"12.9")'
env ROUTER_BENCH_WORKERS=1,8 julia --project=router --threads=8 router/benchmark-window-engines.jl data/rail_and_friends_res6.arrow --backend=cuda
```

This compares warmed CPU and GPU transit-only windows, excluding graph loading and
upload from query timing. Walking and server backend selection are unchanged.

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
