# Query Contract

## HTTP

Send queries to `GET /reachable`. The server returns an Arrow IPC **file** without compression or dictionary encoding.
The response has `Content-Type: application/vnd.apache.arrow.file` and `Cache-Control: no-store`.
CORS permits all origins.

`OPTIONS /reachable` returns HTTP 204. Other methods on `/reachable` return HTTP 405.
Other paths return HTTP 404. The server rejects invalid queries with HTTP 400 before routing.

Specify the origin in one format only:

- `index`: a 15-digit hexadecimal H3 cell. You can use uppercase or lowercase. Omit the `0x` prefix.
- `index_lower` and `index_upper`: two unsigned 32-bit integers in decimal format. The cell value is `lower | (upper << 32)`.

The H3 cell must be valid. Its resolution must be available in the selected network.
The server rejects unknown parameters, duplicate parameters, and invalid parameter formats.

| Parameter | Meaning | Default |
| --- | --- | --- |
| `network` | Filename prefix before the final `_resN.arrow` suffix | Network of the first command-line file |
| `departure_h` | Hours after midnight, `0 <= h < 24` | Required |
| `budget_h` | Journey time limit, zero or more hours | Required |
| `max_walk_h` | Time limit per walk, zero or more hours; zero disables walking | `1` |
| `window_h` | Departure-window length in hours; zero selects one departure | `0` |
| `step_h` | Sampling interval in hours, zero or more; zero selects one departure | `1/60` |
| `encoding` | `split` or `string`, independent of input representation | `split` |
| `distance_mode` | `itinerary` or `straight_line` | `itinerary` |
| `window_mode` | One of the six modes below | `mean_intersection` |
| `metric` | `time` or `time_distance_quantile` | `time` |

For example, add `&network=everything` to select files named `everything_resN.arrow`.
Names can include underscores, hyphens, spaces, and UTF-8 characters. Use percent encoding in query values where necessary.
An unknown network or an unavailable resolution returns HTTP 400.

If you omit `network`, the server uses the first command-line file's network.
The shell expands filename patterns before it supplies the file list.
`--demo` provides network `demo` at resolution 5. WebSocket query URLs use the same selection rules.

Specify times as finite `Float64` hours. You can use decimal or scientific notation.
The server rounds each value once to integer milliseconds. It rounds halfway values to the nearest even integer.
It rejects negative values, including negative zero.
It also rejects departure times that round to 24 hours.
Positive window lengths and sampling intervals must round to at least one millisecond.
Small positive budgets and walking limits can round to zero.

Window sampling applies only when `window_h` and `step_h` are both positive.
If either is zero, the server queries one departure without averaging.
It then ignores `window_mode`, including unknown or empty values.
Both time parameters must still have valid values.

## Time Limits

The router uses `INF = 4294967295` to mark an unreachable arrival.
Converted time inputs must fit in `0..4294967294` milliseconds, approximately 1193 hours.
Here, `ready` and `budget` are the departure time and journey budget in milliseconds.

For a single-departure query, this condition must be true:

```text
ready + budget < INF
```

For a window query, these conditions apply:

```text
samples = ceil(window_ms / step_ms)
ready + (samples - 1) * step_ms + budget < INF
```

The server checks these conditions before it allocates routing memory or starts routing.
A sampling interval greater than the window length gives one sample.
Memory use grows with the number of samples and destination cells.

## Results

Each result cell occurs once. Rows are sorted by H3 cell value.
The origin is included with zero elapsed time.
An origin outside the graph can use walking. With walking disabled, it returns only itself.

| Column | Type | Meaning |
| --- | --- | --- |
| `index_lower`, `index_upper` | `UInt32` | Split H3 output, when `encoding=split` |
| `index` | UTF-8 string | Canonical lowercase H3, when `encoding=string` |
| `value` | `Float64` | Selected elapsed hours for `metric=time`, except the fraction for `reachable_union`; otherwise a rank difference |
| `elapsed_h` | `Float64` | Selected elapsed-time statistic in hours; journey times include waiting |
| `distance_km` | `Float64` | Selected distance, when available |

Single-departure responses include distance if walking is enabled, the input contains connection distances, or straight-line mode is selected.
An unknown itinerary distance is `NaN`.
Straight-line mode calculates great-circle kilometres from the origin and destination cell centres, independently of input distances.
It calculates this distance once for each final result cell.

Each window sample uses departure time `departure + k*step`, starting with `k=0`.
The sampled departure must be earlier than `departure + window`.
A cell is reachable in a sample if a journey reaches it within that sample's budget.

| Mode | Include a cell when | `elapsed_h` | Itinerary `distance_km` |
| --- | --- | --- | --- |
| `mean_intersection` (default) | Every sample can reach it | Mean elapsed time | Mean distance |
| `min_union` | At least one sample can reach it | Minimum elapsed time | Best sample's distance |
| `max_intersection` | Every sample can reach it | Maximum elapsed time | Worst sample's distance |
| `diff_union` | At least one sample can reach it | Maximum minus minimum elapsed time; use the budget as the maximum if any sample cannot reach it | Best sample's distance |
| `diff_intersection` | Every sample can reach it | Maximum elapsed time minus minimum elapsed time | Best sample's distance |
| `reachable_union` | At least one sample can reach it | Mean time over all samples; use the budget for each unreachable sample | Mean distance over reachable samples |

The best sample has the minimum elapsed time. The worst sample has the maximum elapsed time.
These comparisons use journey duration, not absolute arrival time.
If equal times occur, the server selects the earliest sampled departure, even if its distance is `NaN`.
Time differences use hours.

For `reachable_union`, `value = reachable_samples / sample_count`.
This value is a **fraction from 0 to 1**. It equals `reachable_fraction`.

Window responses also contain these columns:

| Column | Type | Meaning |
| --- | --- | --- |
| `reachable_elapsed_h` | `Float64` | Same as `elapsed_h`, except in `reachable_union`, where it is the mean over reachable samples only |
| `reachable_fraction` | `Float64` | `reachable_samples / sample_count`, from 0 to 1 |
| `reachable_samples` | `UInt32` | Number of samples that can reach the cell |
| `sample_count` | `UInt32` | Total number of samples in the window |

Distance values use kilometres. Each destination has the same straight-line distance in all samples.

## Quantile Metric

For `time_distance_quantile`, the server first applies the window's cell selection rule.
It then removes rows where time or distance is not finite.
It ranks the selected time statistic and distance over the same remaining cells.
The rank of a value is the fraction of values that are less than or equal to it.
The server scales ranks so the minimum is zero and the maximum is one.
If all values are equal, their ranks are zero.

The response includes `time_quantile` and `distance_quantile`. It sets:

```text
value = time_quantile - distance_quantile
```

A positive value means that the time statistic ranks higher than distance.
For difference modes, the time statistic is the time range, not the journey duration.
Rank differences have no unit.

Itinerary quantiles require an input `distance_km` column.
Use `metric=time` with `reachable_union` when window sampling is active. The quantile metric returns HTTP 400 for this combination.
Other union modes can include partially reachable cells, subject to the finite-value check above.

## Response Headers

The `X-Router-*` headers report the metric, distance mode, window mode, walking limit, and engine statistics.
`X-Router-Backend` is `reference` for the CPU engine.
The window strategy is `catchup` or `walking_catchup`.
Headers report search, reuse, full-search, repair, lookup, and worker counts where applicable.

## WebSocket

Connect to `/query` on the same server.
The server accepts all browser `Origin` values, including `null`, and requests without an `Origin` header.
A request without a WebSocket upgrade returns HTTP 426.
Use the connection without an application subprotocol. Authentication requires a proxy.

```json
{"type":"query","id":42,"url":"/reachable?index=85075dd7fffffff&departure_h=8&budget_h=1&encoding=split"}
```

Send requests as JSON text. Each connection has its own ID sequence.
Use integer-valued IDs from 1 through 4294967295. Each ID must be greater than the previous ID.
You can skip IDs and use numbers such as `42.0`.
Reconnect before you exceed the ID range.

For success, the server sends one binary message.
The first four bytes contain the request ID as a **big-endian UInt32**.
The remaining bytes contain the complete Arrow file, identical to the HTTP response body.
Remove the four-byte prefix before you decode the Arrow data.

For a query error, the server sends JSON text:

```json
{"type":"error","id":42,"message":"query failed"}
```

The URL must be `/reachable`, with an optional query string.
It rejects absolute URLs, URL authorities, other paths, fragments, raw spaces, control characters, and backslashes.

Each valid ID is consumed, even if its request type, URL, or parameters are invalid.
The server ignores unknown JSON fields.
The server closes the connection with code 1008 for malformed JSON or binary requests.
It uses the same code for invalid, repeated, or decreasing IDs.

Each connection keeps one active query and the newest pending query.
For example, if queries 43, 44, and 45 arrive while query 42 runs, only query 45 remains pending.
Query 42 finishes and returns its result. Query 45 then runs.
A newer pending query can also replace a pending query that would return an error.

On disconnect, it discards pending work and the active query's eventual result. The active CPU calculation continues until it finishes.

HTTP and WebSocket routing jobs run one at a time.
CPU work can delay message processing, especially with one thread.
For remote access, use a trusted proxy that provides TLS, authentication, and access control.

## Use from Julia

Create a handler with `make_handler(graph; progress=false, request_lock=ReentrantLock())`.
Pass `make_stream_handler(handler)` to HTTP.jl with `stream=true`.

For named networks, create a dictionary such as `Dict(("rail", 5) => handler, ...)`.
Pass it to `make_network_handler(handlers; default_network="rail")`.
You must specify a default network that exists in the dictionary.
Use the same request lock for all graph handlers to run queries one at a time across networks and resolutions.
A direct `make_handler(graph)` accepts but ignores `network`. Only `make_network_handler` selects a network.
