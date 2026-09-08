# Experiments

Research tools and recorded measurements, outside the production router.
Commands below run from the repository root; input and output arguments remain
relative to the working directory.

## GPU

[GPU experiments](gpu/README.md) have their own Julia environment, kernels,
differential tests and benchmark harnesses.

## Benchmarks

`benchmarks/` retains the CPU harnesses, nine measurement reports and
[development history](benchmarks/history-todo.md). Reports describe their recorded
snapshots, not fresh measurements or the current public API.

```sh
julia --threads=8 --project=router experiments/benchmarks/benchmark-distance-modes.jl data/everything_res7.arrow
julia --threads=4 --project=router experiments/benchmarks/benchmark-window.jl data/rail_res5.arrow
```

The walking reuse and output comparisons load fixed Git snapshots and need this
repository's history plus the GPU environment, whose dependencies those snapshots
import. They do not rewrite the production source:

```sh
julia --threads=4 --project=experiments/gpu experiments/benchmarks/benchmark-walking-reuse.jl data/everything_res7.arrow
julia --threads=8 --project=experiments/gpu experiments/benchmarks/benchmark-walking-output.jl data/everything_res7.arrow
```

Reference comparisons load `router/test/reference.jl` explicitly. Source-level
profiling instrumentation is snapshot-specific and may need updating after router
changes. Measurements and their original artifact paths are preserved in the reports.

## Data

[`data/export.sql`](data/export.sql) is a dataset-specific ClickHouse export example.
Adapt the source table, transport filter and H3 resolution to your data. Source
`travel_time` is minutes; the export converts it to milliseconds. Departure clocks
use the source's effective ClickHouse timezone.

```sh
clickhouse-client --queries-file experiments/data/export.sql > data/rail_res5.arrow
```
