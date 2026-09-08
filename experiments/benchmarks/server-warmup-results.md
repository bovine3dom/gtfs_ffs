# Synthetic Server Warmup

Measured 2026-09-08 with Julia 1.12.7, eight default-pool CPU threads,
curl 8.21.0 and Node 26.8.1's native WebSocket client.
These are synthetic first-response measurements, not real-dataset query timings.
Existing benchmark reports retain their original measured metric names and signs.
The final 190-query routing and transport warmup passed all four external-client
cases. Warm cases were measured sequentially in fresh processes, before the final
eight-thread test suite; no tests ran concurrently with timing measurements.

## Reproduce

Requires `curl` and Node >=22 with native `WebSocket` on `PATH`. From the repository
root, run each case in a fresh Julia process, sequentially and without concurrent tests:

```sh
for scenario in walking transit; do
  for transport in http ws; do
    for state in cold warm; do
      julia --threads=8 --project=router experiments/benchmarks/benchmark-server-warmup.jl "$transport" "$state" "$scenario" || exit
    done
  done
done
```

The harness constructs an eight-cell res8 graph with 129 one-minute departures per
edge (0 through 128 minutes, 1,032 rows), prepares its handler, optionally calls
`warmup_server()`, and only then opens an ephemeral loopback server. Each process
sends two identical requests with a half-hour budget, one-minute sampling,
straight-line distance, split H3 output and `time_distance_quantile`:

- `walking`: 16 departure samples and `max_walk_h=0.25`, using resident adjacency.
- `transit`: 129 departure samples and `max_walk_h=0`, exercising parallel chunks.

The harness validates HTTP status, Arrow magic at both ends, byte-identical repeated
responses, and WS big-endian request IDs. Server diagnostics assert the sample/search
count, metric and distance mode; transit must report three full searches and
`min(3, Threads.nthreads(:default))` workers. Measured requests use external clients;
the new startup warmup itself invokes Julia HTTP and WS clients before measurement.
It closes only its own test server and opens no public ports.
The cold/warm switch belongs to this benchmark, not the production server.

## Results

Warm rows below measure the final implementation. Cold rows reuse the previous
matching external-client baselines: the 129-departure fixture, query parameters
and client method are unchanged, and the cold path does not call `warmup_server()`.
Cold cases were not rerun in this final review.

One fresh process per row; seconds, not medians or latency guarantees:

| Scenario / Transport | State | Startup warmup | First request | Second request | First handler | Second handler |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Walking / HTTP | cold | 0 | 13.816702 | 0.000877 | 12.059494 | 0.000559 |
| Walking / HTTP | warm | 33.238997 | 0.279577 | 0.000845 | 0.003942 | 0.000520 |
| Walking / WS | cold | 0 | 15.169737 | 0.000992 | 12.827232 | 0.000536 |
| Walking / WS | warm | 33.717194 | 0.354338 | 0.000941 | 0.000696 | 0.000516 |
| Transit / HTTP | cold | 0 | 13.914283 | 0.000974 | 12.108819 | 0.000590 |
| Transit / HTTP | warm | 33.028519 | 0.296430 | 0.001083 | 0.000621 | 0.000689 |
| Transit / WS | cold | 0 | 13.368502 | 0.001081 | 11.093556 | 0.000626 |
| Transit / WS | warm | 33.311935 | 0.401977 | 0.001123 | 0.000668 | 0.000626 |

HTTP request timing is curl's `time_total`, covering connection through complete
body transfer; each request uses a fresh curl connection. Node/curl process startup
and response validation are excluded. WS timing starts inside Node immediately
before constructing the socket and ends on the first complete binary message;
the second query reuses that connection. WS close and validation are excluded.
No Julia client compilation or external process launch is inside either timer.
Node's own connection/request processing remains part of the WS measurement.

Handler timing covers server-side resolution dispatch, query validation, routing
and Arrow serialization, excluding transport. The final compilation stage took
31.09-31.72 seconds; complete warmup took 33.03-33.72 seconds, including synthetic
fixture/index preparation. No input files or real origins were queried.

All walking responses reported 16 searches, eight full searches and eight workers;
all transit responses reported 129 searches, three full searches and three workers.
Arrow payload sizes were respectively 3,594 and 2,210 bytes, excluding the WS ID.

Remaining first-response overhead is measurable: warmed HTTP took 280-296 ms and
WS took 354-402 ms, while first handler execution took 0.62-3.94 ms. Walking/transit
WS socket-open timings were 272.635/316.281 ms, with another 81.703/85.696 ms until
the first reply. This includes the external client's first connection processing
and remaining server/benchmark-wrapper dispatch overhead; it is not all routing
work or proof of zero remaining JIT. Second complete responses took 0.85-1.12 ms.

## Coverage And Limits

Startup executes 190 successful synthetic queries: 188 direct handler calls,
one real HTTP query and one real WS query. Direct workload coverage remains:
point and all six window modes; itinerary and straight-line distances; walking
disabled, resident adjacency, and larger-radius fallback; known and missing input
kilometres; on-graph and off-graph origins; both H3 input/output encodings; both
metrics wherever compatible. No-walk windows use 129 distinct departure groups to
exercise three parallel transit chunks; 16 samples alone do not cross the 64-group
transit chunk threshold. Walking windows retain 16 samples, exercising eight-worker
parallel waves and backward repair. Each progress increment is one completed query.
The fixture and requests are independent of resident graph sizes and resolutions:
`Graph` and `WalkingIndex` are concrete, nonparametric types, and resolution is an
integer field. Warmup runs once in the launcher, not once per graph or handler.

The transport stage opens a temporary `127.0.0.1:0` listener using the production
stream handler on the synthetic graph. HTTP uses `closeimmediately=true` and both
HTTP/WS explicitly use `proxy=nothing`. WS calls `HTTP.closewrite(ws.io)` before
sending ID 1 and compares the four-byte big-endian ID plus Arrow body against the
HTTP result. A `finally` block closes only that temporary listener before warmup
returns and public listening can begin. An intermediate stale pooled-connection
upgrade failure was resolved by `closeimmediately=true`; a request-side
`Connection: close` header alone was insufficient.

Warmup does not promise a zero-latency first response, precompile every error path,
or make large routing queries cheap.
Both timed scenarios use straight-line distance, so itinerary distance replay is
disabled. Startup still exercises itinerary replay, but these measurements do not
independently time its first network request or fallback/missing-distance requests.
Production warmup uses only a temporary loopback listener, imports no test/GPU
dependencies, retains no real query results, and never queries or mutates resident
graphs or datasets. It does not open the public listener until warmup completes.

## Verification

- Final eight-thread router suite, after all four warm benchmarks: **93,613 passed,
  no failures or errors**, in 4m54.2s. This includes the 190-query warmup,
  parallel-transit assertions, launcher/resolution dispatch and HTTP/WS sessions.
  Log: `/tmp/opencode/router-transport-warmup-fixed-final-t8.log`.
- All benchmark/test processes exited; no surviving listener/process leak was
  observed. Existing servers were left running and were not queried. This is not
  an in-process file-descriptor leak audit.
- Prior verification, not rerun here: optional KernelAbstractions CPU suite (4,067
  assertions), H3-MON (23 unit tests and headless desktop/mobile rendering), and
  active frontend metric/default/label checks. No GPU hardware was tested.
- Sign tests include hardcoded positive/negative results over HTTP and WS, both H3
  encodings, ties/constants, and rank population filtering before serialization.
