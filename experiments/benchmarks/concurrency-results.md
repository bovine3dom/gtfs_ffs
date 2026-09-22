# Resource Scheduler Results

## Method

Run from the repository root:

```sh
julia --threads=8 --heap-size-hint=32G --project=router \
  experiments/benchmarks/concurrency-benchmark.jl \
  data/everything_res8.arrow data/kontur_h3.arrow
```

The script loads its own graph and population data. It starts a temporary loopback
listener on an assigned port. It does not contact an existing server or change input files.
The heap hint controls garbage collection. It is not a RAM limit.

The run used Julia 1.12.7, eight logical CPUs, and 62 GiB of physical RAM.
Available physical RAM was 56 GiB before the run. Swap was not counted as capacity.
The graph had 900,197 nodes and 316,136,664 packed profiles.
The scheduler reserved two worker slots for short work and six for bulk work.
Each bulk query used three workers. The workspace and scratch budgets were each 8 GiB.

Each latency case has 30 measured HTTP requests. Client and server warmup requests
are excluded. The mixed case starts two population cache misses in Paris, each
with 1,027 origins, 96 samples, a one-hour budget, and a one-hour walk limit.
Their departure times differ. Both requests continued while the short cases ran.
The short cases run in the order shown, not as simultaneous short-client streams.

## HTTP Latency

| Case | Idle Median ms | Idle p95 ms | Mixed Median ms | Mixed p95 ms | Mixed Maximum ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| Short point, 0.5 h budget, 0.1 h walk | 13.804 | 27.842 | 26.619 | 50.238 | 59.805 |
| Paris point, 3 h budget, 1 h walk | 76.251 | 103.719 | 126.459 | 166.936 | 210.221 |
| Population cache hit, 331 origins | 1.690 | 10.445 | 4.390 | 21.870 | 26.549 |

All measured requests returned HTTP 200. Cache-hit responses had zero routing workers.
Short requests did not wait for the bulk requests to finish.
These results do not establish a 30 ms tail-latency guarantee. Shared CPU execution,
memory bandwidth, garbage collection, and the query itself still affect latency.
The Paris point query is in the short class, but it is not a 30 ms query even when idle.

See [concurrency-trials.csv](concurrency-trials.csv) for every HTTP measurement,
including admission wait, response size, worker count, and bulk completion time.

## Bulk Throughput

Each pair compares two sequential six-worker calls with two concurrent three-worker
calls. These direct population calls use the same pool and prepared graph, without
the result cache or HTTP. Each call has the Paris parameters described above.

| Pair | Sequential Six-Worker Total s | Concurrent Three-Worker Total s |
| --- | ---: | ---: |
| 1 | 12.346 | 11.253 |
| 2 | 12.833 | 11.805 |

H3 IDs, zero masks, and shared expansion counts matched in both pairs. Population
values matched with `rtol=1e-12` and `atol=1e-6`.
The concurrent policy was faster in these two pairs. This is not proof that three
workers per bulk query is optimal for other graphs or workloads.

The second sequential pair allocated 3.78 GB because the preceding concurrent
queries left three-worker layouts. The pool does not combine smaller worker vectors
to satisfy a larger request. This is a throughput-test allocation cost, not a routing
value difference. The second concurrent pair allocated 9.79 MB.

The pool retained 5,931,484,492 measured bytes at the end. Scheduler CPU, scratch,
output, and queue counters were all zero. Retained measurements include shared
schedule hints conservatively. They are not process RSS measurements.

## Final Review

Point requests now reserve one worker in either class. Small population misses
use tighter tile and buffer estimates. These changes leave the measured HTTP
cases' worker reservations unchanged. The full eight-thread regression suite passed.

WebSocket replies use two fragments of one binary message for the ID and Arrow
body. This preserves caller-owned buffers without another full body copy.
