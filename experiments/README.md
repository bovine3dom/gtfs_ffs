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
and rejected population changes. The [roadmap](benchmarks/roadmap.md) lists
conditional research ideas. Reports describe measured snapshots, not the current API.

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

Reference comparisons load `router/test/reference.jl` explicitly. Source-level
profiling instrumentation can depend on a specific source snapshot. Check it before
use after router changes. Closed prototype harnesses are not retained.

## Trip Experiments

[Trip shortcuts](trip-shortcuts/results.md) remain active for GPU comparison.
The [trip-pattern decision](trip-patterns/decision.md) records the stopped CPU
implementation path, input audit, correctness checks, and final measurements.

## Data

[`data/export.sql`](data/export.sql) is a dataset-specific ClickHouse export example.
Adapt the source table, transport filter and H3 resolution to your data. Source
`travel_time` is minutes; the export converts it to milliseconds. Departure clocks
use the source's effective ClickHouse timezone.

```sh
clickhouse-client --queries-file experiments/data/export.sql > data/rail_res5.arrow
```
