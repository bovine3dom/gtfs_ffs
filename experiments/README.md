# Experiments

Research tools and recorded measurements, outside the production router.
Commands below run from the repository root; input and output arguments remain
relative to the working directory.

## GPU

[GPU experiments](gpu/README.md) have their own Julia environment, kernels,
differential tests and benchmark harnesses.

## Benchmarks

`benchmarks/` contains reusable CPU harnesses and measurement reports.
The [CPU research decisions](benchmarks/cpu-research-results.md) record accepted
and rejected population changes. The [dense-network results](benchmarks/dense-population-results.md)
cover range pending masks and exact label pruning. The [roadmap](benchmarks/roadmap.md) lists
conditional research ideas. Reports describe measured snapshots, not the current API.
The [shared-event results](benchmarks/shared-events-results.md) cover wider masks and paired 64-bit tiles.

```sh
julia --threads=8 --project=router experiments/benchmarks/benchmark-distance-modes.jl data/everything_res7.arrow
julia --threads=4 --project=router experiments/benchmarks/benchmark-window.jl data/rail_res5.arrow
```

The population harness uses the current router. The cache harness uses a synthetic
network and does not load a full dataset:

```sh
julia --threads=8 --project=router experiments/benchmarks/benchmark-population.jl data/everything_res7.arrow data/kontur_h3.arrow
julia --threads=8 --project=router experiments/benchmarks/benchmark-population-cache.jl
```

The trip-aware population benchmark uses prepared shards and checks radii 0, 1, 3,
6 and 10. It sends population requests through the router handler. Each request sets
`trip_aware=true`. It records per-request time, allocations, worker count and cache
counts in a CSV file. Use a graph with a matching prepared shard set:

```sh
julia --threads=60 --heap-size-hint=32G --project=router \
  experiments/benchmarks/trip-population-benchmark.jl \
  data/everything_res8.arrow data/kontur_h3.arrow data/trip-shards \
  /tmp/trip-population.csv
```

The shard directory must contain prepared `everything_res5` through `everything_res8`
subdirectories, each with a manifest and its shard files. The benchmark uses Paris,
a 3-hour budget, a 1.6-hour window and 1-minute steps. It uses a new departure
time for each request to avoid response-cache hits. The caches are not cleared between radii. This is an in-process
handler test. It does not measure network transport or cold startup.

Set `TRIP_POPULATION_RADII` and `TRIP_POPULATION_ROUNDS` to change the sweep and the
number of timed trials. `TRIP_POPULATION_WORKSPACE_GIB` sets the router workspace
limit in GiB. Its default is 8 GiB. Set `TRIP_POPULATION_SHORT_WORKERS` and
`TRIP_POPULATION_MAX_WORKERS_PER_REQUEST` to test scheduler limits. Memory admission
can still assign fewer workers.

Reference comparisons load `router/test/reference.jl` explicitly. Source-level
profiling instrumentation can depend on a specific source snapshot. Check it before
use after router changes. Closed prototype harnesses are not retained.

## Trip Experiments

[Trip shortcuts](trip-shortcuts/results.md) remain active for GPU comparison.
The [trip-pattern decision](trip-patterns/decision.md) records the stopped CPU
implementation path, input audit, correctness checks, and final measurements.

## Hierarchy

The [hierarchy decision](hierarchy/decision.md) records the stopped coarse/fine
experiment and its measured errors. The report retains selected evidence.
The prototype code and its test and benchmark scripts have been removed.

## Data

[`data/export.sql`](data/export.sql) is a dataset-specific ClickHouse export example.
Adapt the source table, transport filter and H3 resolution to your data. Source
`travel_time` is minutes; the export converts it to milliseconds. Departure clocks
use the source's effective ClickHouse timezone.

```sh
clickhouse-client --queries-file experiments/data/export.sql > data/rail_res5.arrow
```
