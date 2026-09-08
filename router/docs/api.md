# Query Contract

## HTTP

`GET /reachable` returns an uncompressed, non-dictionary Arrow IPC **file** with
`Content-Type: application/vnd.apache.arrow.file`. CORS permits all origins;
responses use `Cache-Control: no-store`. `OPTIONS /reachable` returns 204, other
methods 405, and other paths 404. Invalid queries return 400 before routing.

Supply exactly one origin representation: a 15-digit hexadecimal H3 `index`
(case-insensitive, without `0x`), or decimal unsigned `index_lower` and
`index_upper` words. The cell is `lower | (upper << 32)` and must be valid at a
loaded resolution. Unknown, duplicate and malformed parameters are rejected.

| Parameter | Meaning | Default |
| --- | --- | --- |
| `departure_h` | Clock hours, `0 <= h < 24` | Required |
| `budget_h` | Nonnegative elapsed journey budget | Required |
| `max_walk_h` | Nonnegative limit per walking hop; zero disables walking | `1` |
| `window_h` | Departure-window width; zero requests a single departure | `0` |
| `step_h` | Nonnegative sampling interval; zero requests a single departure | `1/60` |
| `encoding` | `split` or `string`, independent of input representation | `split` |
| `distance_mode` | `itinerary` or `straight_line` | `itinerary` |
| `window_mode` | One of the six modes below | `mean_intersection` |
| `metric` | `time` or `time_distance_quantile` | `time` |

Times are finite Float64 hours, accepting decimal and scientific notation. They
round once to integer milliseconds, ties to even. Negative values (including
negative zero), non-finite values and clocks rounding to 24 hours are rejected.
Positive windows and steps must represent at least one millisecond. Tiny positive
budgets or walking limits may round to zero.

Window sampling and `window_mode` apply only when both `window_h` and `step_h` are
positive. If either is zero, the response is a single-departure isochrone without
averaging; `window_mode` is ignored, including unknown or empty values. Both time
fields still require valid values, and duplicate parameters are always rejected.

## Representation Bounds

Arrival labels reserve `INF = 4294967295` for unreachable. Other time inputs must
fit `0..4294967294` milliseconds, about 1193 hours. A point query requires
`ready + budget < INF`. A window has `samples = ceil(window_ms / step_ms)` and requires
`ready + (samples - 1) * step_ms + budget < INF`. These are checked before allocation
or routing. A step wider than the window produces one sample.

These are data-format limits, not resource quotas. A representable query may still
be expensive or exhaust available memory. Sampling is exact, not automatically reduced.

## Results

Rows are unique, sorted by H3 and include the origin at zero elapsed time. Off-graph
origins can walk; with walking disabled they return only themselves.

| Column | Type | Meaning |
| --- | --- | --- |
| `index_lower`, `index_upper` | `UInt32` | Split H3 output, when `encoding=split` |
| `index` | UTF-8 string | Canonical lowercase H3, when `encoding=string` |
| `value` | `Float64` | Elapsed hours for `metric=time`, except coverage fraction for `reachable_union`; otherwise rank difference |
| `elapsed_h` | `Float64` | Elapsed hours including waiting |
| `distance_km` | `Float64` | Selected distance, when available |

Point responses include distance when walking is enabled, the input has connection
distances, or straight-line mode is selected. Unknown itinerary distance is NaN;
it is not estimated from endpoint separation. Straight-line mode needs no input
distance column and computes H3-centre great-circle kilometres once per final cell.

Windows sample `departure + k*step`, strictly before `departure + window`:

| Mode | Include | `elapsed_h` | Itinerary `distance_km` |
| --- | --- | --- | --- |
| `mean_intersection` (default) | Every sample reaches | Mean elapsed | Mean |
| `min_union` | Any sample reaches | Minimum elapsed | Best sample |
| `max_intersection` | Every sample reaches | Maximum elapsed | Worst sample |
| `diff_union` | Any sample reaches | `(count < samples ? budget : maximum elapsed) - minimum elapsed` | Best sample |
| `diff_intersection` | Every sample reaches | Maximum elapsed minus minimum elapsed | Best sample |
| `reachable_union` | Any sample reaches | Capped mean over all samples | Conditional mean over reachable samples |

Extrema use elapsed time within each sample's budget, not absolute arrival time.
Ties select the earliest chronological departure, even if its distance is NaN.
`diff_union` deliberately uses **best-sample kilometres**: an unreachable, budget-capped
worst sample has no itinerary. Differences remain hours. For `reachable_union`,
`value = reachable_samples / sample_count` is a **fraction (0..1)**, equal to `reachable_fraction`.

Window rows also include `reachable_elapsed_h Float64`, `reachable_fraction Float64`,
`reachable_samples UInt32`, and `sample_count UInt32`. `reachable_elapsed_h` equals
the selected elapsed statistic, except in `reachable_union`, where it is the conditional
mean over reachable samples; its `elapsed_h` instead counts each missing sample at budget.
Counts describe the entire window. Distance values remain kilometres.
Straight-line kilometres are independent of departure samples. Point queries ignore
`window_mode` with either metric and omit all window-only columns.

For `time_distance_quantile`, rows with non-finite time or distance are removed,
then `time_quantile - distance_quantile` becomes `value`. Higher scores mean slower travel relative to itinerary or straight-line distance. Ranks use upper ECDF ties,
normalized so the minimum is zero and the maximum one; constant sets rank zero.
Extra columns are `distance_quantile` and `time_quantile`. Window coverage filtering
happens **before** ranking. Itinerary quantiles require an input `distance_km` column.
`reachable_union` with an active window (both width and step positive) rejects `time_distance_quantile` with HTTP 400;
other modes rank their selected elapsed statistic and associated distance. Union modes
retain partial coverage except for the non-finite time/distance filter above.
Rank differences are dimensionless, not hours; use an appropriate frontend title.
The former `distance_time_quantile` name is rejected with HTTP 400, including point queries; there is no alias.

The exposed `X-Router-*` headers describe metric, distance mode/meaning, window mode,
walking limit and engine diagnostics. `X-Router-Backend` is `reference` for
the CPU engine. Window strategies are `catchup` or `walking_catchup`, with search,
reuse, full/repair, lookup and worker counts where applicable.

## WebSocket

Connect to `/query` on the same server. All browser Origins, `null`, and requests
without Origin are accepted. A non-upgrade request returns 426. There is no
application subprotocol or authentication.

```json
{"type":"query","id":42,"url":"/reachable?index=85075dd7fffffff&departure_h=8&budget_h=1&encoding=split"}
```

- Send JSON text with connection-local, strictly increasing integer-valued IDs in `1..4294967295`. Gaps and `42.0` are accepted; booleans are not. Reconnect before wrapping.
- Success is one binary message: four **big-endian UInt32 ID bytes**, followed by the complete Arrow file, byte-identical to HTTP. Strip the prefix before decoding.
- Query errors are JSON text: `{"type":"error","id":42,"message":"query failed"}`. Validation uses fixed safe messages, not reflected parameters or exceptions.
- Only local `/reachable` with an optional query string is dispatched in-process. Absolute URLs, authorities, other paths, fragments, raw control/space characters and backslashes are rejected. No outbound URL is fetched.
- Valid IDs are consumed even for bad type/URL/parameters. Unknown JSON fields are ignored. Malformed JSON, binary requests and invalid/reused/decreasing IDs close with code 1008.
- Each connection keeps one active and one latest pending query. While 42 runs, 43/44/45 replace the pending slot; 42 finishes, then 45 runs. Pending errors can also be superseded.
- There are no ACK, done, cancel or progressive messages. Disconnect discards pending work and the eventual active result, without cancelling CPU work.

HTTP and socket jobs share a global serialization lock. Socket readers do not wait
on it, but Julia scheduling is cooperative: CPU work can delay reads, particularly
with one thread. The pending slot bounds jobs, not bytes or connection counts.
Use a trusted proxy for remote authentication, TLS and access control.

For embedding, build `make_handler(graph; progress=false, request_lock=ReentrantLock())`,
then pass `make_stream_handler(handler)` to HTTP.jl with `stream=true`.
Share the same request lock across handlers passed to `make_resolution_handler`
to serialize queries across graphs.
