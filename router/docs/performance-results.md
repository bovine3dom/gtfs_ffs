# Router measurements on the 60-CPU VM

## Conditions

- Baseline source: `8aa0c1b`, with the same request timers added.
- Julia: 1.13.0; 60 default-pool threads.
- Guest memory: about 94 GiB. Linux reports one NUMA node.
- Short workers: 12; maximum bulk workers per request: 48.
- Workspace budget: 32 GiB, including 25.6 GiB for bulk work.
- Inputs: the supplied population file, prepared trip shards, and both resolution-8 networks.
- Requests: the four supplied cases, with `trip_aware=true` and `network=everything`.

The tables use in-process handler time. They exclude network transfer and browser
work. Startup loads and first-use work are separate. Warm runs change departure
by one millisecond, so they do not use complete-response cache hits.
The machine was not isolated. Existing services remained running, and some test
compilation ran at the same time. Treat small timing differences with care.

## Population requests

These are medians of warm rounds 1 and 2. Allocation is total allocated bytes,
not peak RAM. GB means one billion bytes.

| Resolution | Before, s | After, s | Speed increase | Allocated GB, before | Allocated GB, after |
|---|---:|---:|---:|---:|---:|
| 5 | 7.71 | 2.43 | 3.2× | 23.35 | 1.07 |
| 6 | 26.21 | 4.48 | 5.8× | 193.88 | 4.37 |
| 7 | 49.70 | 3.52 | 14.1× | 375.95 | 12.09 |
| 8 | 101.16 | 23.00 | 4.4× | 558.78 | 15.76 |

All 48 Arrow response bodies in the final four-case comparison match the
baseline byte for byte. No approximation was added.

The main changes remove batch barriers, serial tile reductions, repeated large
scratch allocations, and writes to unused population state. Each worker keeps
its scratch while it processes tiles. The server also avoids population lookup
arrays that the trip engine does not use.

## Worker limits

An earlier sweep measured the population case with three limits. Values are
warm medians in seconds. Later fast-path and scratch-reuse changes are not
included in this sweep.

| Resolution | Limit 12 | Limit 24 | Limit 48 |
|---|---:|---:|---:|
| 5 | 4.07 | 2.95 | 2.46 |
| 6 | 8.34 | 5.70 | 4.95 |
| 7 | 6.08 | 4.19 | 3.55 |
| 8 | 32.65 | 21.55 | 22.49 |

Keep the supplied limit of 48 for now. Resolution 8 used only 21 workers at
limits 24 and 48 because of memory admission. The small difference between those
two timings does not justify a different global limit.
The workspace budget is not a process-memory limit. Graphs and caches need
additional RAM. Do not increase the budget only to make all workers active.

## Small requests during bulk work

The mixed test starts one population request, then eight fresh small requests
while bulk capacity is occupied. Four use the short-window case. Four use the
walking case. All responses returned HTTP 200.

| Small-request time | Before | After |
|---|---:|---:|
| Minimum | 9.338 s | 1.27 ms |
| Median | 9.339 s | 9.65 ms |
| Maximum | 9.343 s | 13.44 ms |

Before the change, the small requests waited in the bulk queue. After the change,
they used the short reserve. Queue waits were below 0.1 ms in the new run.
These eight samples are a smoke test, not a production p95 or p99 estimate.

One-worker windows initially exposed another cost: reuse of a large route's
scratch caused repeated clearing of large hash tables. A final change prevents
short searches from borrowing oversized scratch.
A separate test fills the pool with a long window, then measures five small
windows on one worker. These are median routing times, without Arrow encoding.

| Resolution | Unrestricted reuse, ms | Bounded reuse, ms |
|---|---:|---:|
| 5 | 41.71 | 9.06 |
| 6 | 46.59 | 18.52 |
| 7 | 106.52 | 43.25 |

All 15 response hashes in that comparison match. The main server and mixed-load
measurements above precede this final scratch-reuse change. The admission rule
remains conservative; it does not identify every cheap request.

## Animation and remaining costs

For the long window, advancing departure by 0.51 hours reuses 47 of 48 searches.
At resolution 5, warm frame time fell from 1.72 s to 0.72 s. All three measured
frame hashes match the uncached baseline. Later frames took about 1.08 s at
resolution 6 and 2.02 s at resolution 7.

The centre cell at resolution 8 has no transit node. Its transit-only window
returns only the origin. Do not use that case to assess long-route performance.
The new off-network shortcut reduces its warm handler time from about 10 ms to
less than 1 ms, without graph-sized output arrays.

A second resolution-8 test uses `881fb08803fffff`, an adjacent transit cell about
0.86 km from the original centre. Its cached animation frames take about 3.84 s.
The uncached first frame includes shard loading and takes 21.66 s. These are not
like-for-like warm timings. Each cached frame runs one new search and reuses 47.
The routing stage takes about 3.3 s, including the new search and window
aggregation. Encoding takes about 0.5 s. Thus, individual searches and large
responses still impose a latency floor.

New long-window requests at resolutions 5 through 7 show no large speed change.
They already run independent departures in parallel. The new sample cache saves
work only when departures overlap. Walking and population windows do not use it.

## Evidence and repeat runs

See [Performance](performance.md) for commands, timers, and Proxmox settings.
The raw measurements are in `experiments/benchmarks/`:

- `router-numa-baseline.csv`: baseline; coarse round 3 includes CPU profiling.
- `router-numa-final.csv`: main comparison; use warm rounds 1 and 2.
- `router-numa-workers.csv`: limits 48, 24, and 12.
- `router-numa-mixed.csv`: eight small requests and one bulk request per version.
- `router-numa-animation-baseline.csv` and `router-numa-animation.csv`: animation frames.
- `router-numa-short-pool.csv`: the final bounded-reuse test.

To repeat the last test, set `TRIP_BASELINE_SRC` to the baseline `router/src`
directory. Run this command from the repository root:

```sh
TRIP_BASELINE_SRC=/path/to/baseline/router/src \
  julia --threads=60 --project=router \
  experiments/benchmarks/router-numa-short-pool.jl /tmp/short-pool.csv
```

Validation included the full eight-thread test suite, one-thread focused tests,
396 randomized parity checks, workspace ownership tests, and the response-hash
comparisons above. The final scratch-reuse change has separate workspace and
real-data parity tests.
