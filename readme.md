# GTFS FFS

Timetable experiments and a Julia reachability router for H3 maps.
Named after the Swiss railway company if anyone asks.

The router answers "where can I get to?" using a repeating daily timetable and
estimated walking. Results are Arrow tables for [H3-MON](https://github.com/bovine3dom/H3-MON),
including its population cartogram view.

## Run

From this directory, with Julia installed:

```sh
julia --project=router -e 'using Pkg; Pkg.instantiate()'
julia --threads=8 --project=router router/serve.jl data/everything_res8.arrow
```

Use filenames of the form `[name]_res[N].arrow`; the resolution must match the contents.
The first file sets the default network. The origin cell selects its matching resolution.
A resolution-8 file also supplies missing resolution-5, resolution-6, and resolution-7
graphs through in-memory derivation. Explicit files take precedence. No extra files are written.
See the [router load instructions](router/README.md#run) for startup and memory costs.

The website server shares CPU and memory budgets across all graphs.
With eight threads, it reserves two worker slots for short queries. Bulk queries
can use six slots, with up to three workers per query. Cache hits do not wait for bulk routing.
Use `--max-pending=128` to set the waiting queue size; `0` disables queueing.
The queue reserves space for short queries. Response writes have a separate byte budget.
The shared population workspace pool uses an 8 GiB budget by default.
Use `--workspace-memory-gib=8` to change this positive whole-GiB budget.
The pool can reduce the worker count to fit its estimate. These are not total
process memory limits. Graphs, temporary allocations, and outputs need extra RAM.
See [request and memory limits](router/README.md#request-and-memory-limits).
For a small synthetic example instead:

```sh
julia --threads=8 --project=router router/serve.jl --demo
```

The server listens on `127.0.0.1:1988`. It uses CPU Dijkstra and parallel CPU
catch-up, with no GPU setup or backend flags.

## Interface

Input columns:

```text
from_h3 UInt64, to_h3 UInt64, departure_ms UInt32, duration_ms Int64
distance_km Float64 (optional)
```

Query times are hours. Both HTTP and WebSocket queries return elapsed hours,
with split or string H3 indices. Itinerary and straight-line distances are
available, as are mean/intersection and minimum/union departure windows.

See the [router README](router/README.md) for the model, input and frontend setup,
and the [query contract](router/docs/api.md) for parameters and framing.

## Other Work

- [Experiments](experiments/README.md): GPU kernels, CPU benchmarks, measurement reports and a dataset-specific export example, separate from the server.
- [Original data notes](docs/data-notes.md): exploratory feed downloads and queries, not a reproducible current data pipeline.
- `plots/`, `src/` and `tidied_up/`: earlier analysis scripts.

Timetables and population data have their own source licences. The code licence
is in [LICENSE](LICENSE). This is an approximation for visualisation, not a journey planner.
