# Packed continuation index

## Result

The optional index is now used by point, walking, population, and window routing.
It adds 1.1–2.0 times speedup over commit `9d875a2`. This is an incremental result,
not another 10 times improvement. The larger target is still not met across workloads.

Tests use the same real `everything` component 1, origins, budgets, and synthetic
population weights as [the first runtime tests](trip-runtime-results.md).
All measured arrival, distance, and population results match the baseline.
Times are the minimum of three warm runs. Cache hits and HTTP are not measured.

| Workload | Resolution | Budget | Additional speedup |
|---|---:|---:|---:|
| Point | 5 | 3 h | 1.49–1.95 times |
| Walking | 5 | 3 h | 1.37–1.78 times |
| Population | 5 | 3 h | 1.33–1.41 times |
| Population window | 5 | 3 h | 1.24–1.47 times |
| Point | 6 | 3 h | 1.23–1.35 times |
| Walking | 6 | 3 h | 1.15–1.41 times |
| Population | 6 | 3 h | 1.10–1.26 times |
| Population window | 6 | 3 h | 1.10–1.24 times |
| Population | 5 | 6 h | 1.17–1.35 times |
| Population window | 5 | 6 h | 1.16–1.30 times |

See `trip-continuation-res5.csv`, `trip-continuation-res6.csv`, and
`trip-continuation-res5-6h.csv`. For Paris at resolution 5, trip-group lookups fall
from 2,213,760 to 39,089. Event scans and queue pops do not change. The remaining
cost includes event processing, state dictionaries, and queue operations.

## Representation

Each node has a sorted list of packed `(trip ID, edge ID)` pairs. Each pair uses
eight bytes. Node offsets use Int64. Only existing trip/edge groups are stored;
there is no node-by-trip matrix. Sorting uses an in-place algorithm per node.

When a query must scan new transfers, it still visits all outgoing edges.
Otherwise it uses two binary searches to select only edges for the current trip.
Walking can improve transfer access after transit. Population lanes can have different
transfer access times. Both cases retain their existing separate transfer checks.

Preparation counts groups before allocating the pair array. The default limit is
512 MiB for the index arrays. Oversized indexes fail before that allocation. The
limit excludes Julia, graph metadata, and pages from the mapped transit file.
Preparation processes one shard at a time. It writes to a temporary file and then
renames the file. An unsuccessful build does not replace an existing sidecar.

The real sidecars occupy about 171 MiB at resolution 5 and 315 MiB at resolution 6.
They are memory-mapped at load and included in the shard cache size. The file header
checks shard size, modification time, node count, payload length, and node offsets.
The format is local Julia data, not an untrusted interchange format.

A server restart loads sidecars for previously cached shards. Shards without a sidecar
continue to use the existing router. No full shard rebuild is required. New trip shards
also require the separate continuation preparation command if indexes are wanted.

## Validation and limits

`router/test/continuation_tests.jl` passes 3,357 checks. These cover:

- Exact edge selection, absent trips, and multiple resolutions.
- Point and walking arrivals and distances.
- Population queries with multiple origins and departure samples.
- Midnight crossings and budgets up to two days.
- Memory-limit rejection without replacing an existing sidecar.
- Optional loading and rejection of stale or damaged files.

Real-network comparison processes used a 32 GiB virtual-memory limit and a 240-second
timeout. Peak RSS was about 2.1 GB for resolution 5, 3.3 GB for resolution 6, and
4.4 GB for the resolution-5 six-hour run. Each process held both router versions.
These results do not establish a memory bound for all query horizons or concurrency.
The full server suite was not run for this change.

## Commands

From the repository root:

```sh
julia --project=router router/prepare_continuation.jl data/trip-shards
julia --project=router router/test/continuation_tests.jl
```

To compare with the prior committed router without modifying the working files:

```sh
mkdir -p /tmp/continuation-baseline
git archive 9d875a2 router/src | tar -x -C /tmp/continuation-baseline
export TRIP_BASELINE_SRC=/tmp/continuation-baseline/router/src
(ulimit -v 33554432; timeout 240 julia --project=router \
  experiments/benchmarks/trip-runtime-parity.jl \
  data/trip-shards/everything_res5/shard_1.bin /tmp/continuation.csv)
```

The target shard must have a prepared sidecar. Set `TRIP_BUDGET_H=6` for the longer
comparison. The updated `trip-continuation-probe.jl` compares indexed and unindexed
point routing in one process without redefining router methods.
