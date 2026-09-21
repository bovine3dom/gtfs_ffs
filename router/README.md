# Timetable Router

This Julia server calculates which H3 cells you can reach within a time budget.
It uses daily timetables and estimated walking times. It returns the results as Arrow data.
Use the results to make maps, not to plan turn-by-turn journeys.

## Run

Install Julia. Run these commands from this directory:

```sh
julia --project=. -e 'using Pkg; Pkg.instantiate()'
julia --threads=8 --project=. serve.jl path/to/timetable_res8.arrow
```

Supply Arrow files for one or more `(network, H3 resolution)` pairs.
You can use any directory. Each filename must have the form `[name]_res[N].arrow`.
Supply a network name. Use lowercase `.arrow` and a resolution from 0 to 15.
The resolution must agree with the file contents.

The server reads the final `_res[N].arrow` suffix.
For example, `rail_and_friends_res7.arrow` specifies network `rail_and_friends` and resolution 7.
Leading zeros are permitted: `res06` specifies resolution 6.

Different networks can use the same resolution.
Each network and resolution pair must be unique across the supplied files.
The server checks the names and pairs before it loads files one at a time.
Restart the server after you change an input file.

For each network with a resolution-8 file, the server automatically builds missing
resolution-5, resolution-6, and resolution-7 graphs in memory. No extra files or flags are needed.
An explicit file, such as `rail_res6.arrow`, takes precedence over automatic derivation.
The server does not derive resolution 8 from other resolutions. A network without
a resolution-8 file keeps only its supplied resolutions. This also applies to `--demo`.

Derivation maps endpoints to logical H3 parents and merges the packed two-day profiles.
It retains earliest arrivals and uses the same departure and distance tie rules as packing.
It does not expand the raw timetable rows. These are ordinary coarse routing graphs,
not resolution-8 routes: transfers within a parent cell take zero time.
The origin resolution selects the graph for every metric. There is no resolution-8 fallback.
Derived graphs inherit the resolution-8 shuttle at its logical parent cells; no second shuttle is added.
An explicit coarse file uses station coordinates at that resolution, which can give different cells.

Startup loads and validates all supplied graphs before derivation. Missing levels are built
from higher derived levels, without using explicit overrides as intermediate sources.
Walking indexes are prepared before requests are accepted. Graphs and indexes remain in memory.
Allow memory for these indexes and query workspaces. Input files are not changed.

```sh
julia --threads=8 --project=. serve.jl /data/rail_and_friends_res8.arrow /data/everything_res8.arrow
```

Add `network=everything` to a query to select that network.
If you omit `network`, the server uses the network of the first command-line file.
The shell expands filename patterns before it supplies this file list.
The server uses the origin cell's H3 resolution.
An unavailable resolution returns HTTP 400.
Startup logs show the default network, each supplied or derived source, and the resolutions in each network.

To run a small example with synthetic data, use this command:

```sh
julia --threads=8 --project=. serve.jl --demo
```

Use `--demo` instead of input files to serve network `demo` at resolution 5.
`ROUTER_HOST` defaults to `127.0.0.1`. `ROUTER_PORT` defaults to `1988`.
Startup reuses packed graphs and prepared walking and population data from
`ROUTER_STARTUP_CACHE_DIR`, which defaults to `~/.cache/gtfs-router`. The cache has
a 20 GiB limit and is safe to delete. It is rebuilt when an input file changes.
Walking preparation and window routing use the available threads in Julia's default thread pool.
The server keeps graphs and walking indexes in memory.

### Request And Memory Limits

HTTP and WebSocket queries share one resource scheduler across all graphs.
With `--threads=8`, short queries have two worker slots and bulk queries have six.
Each bulk query can use up to three workers. Thus, two bulk queries can run together.
Single-threaded point queries reserve one worker, even when classified as bulk.
Idle short slots are not assigned to bulk work. This choice protects short queries;
it does not promise maximum bulk throughput or a 30 ms response time.

For `T` default-pool threads, the short reserve is `ceil(T / 4)`.
The bulk limit is `T - reserve`, with at most `ceil(bulk / 2)` workers per query.
With one thread, both classes share one slot and take turns when both queues wait.
Routing yields between tiles or batches. A long search can still delay other work.
Worker slots are logical concurrency limits, not physical CPU affinity or preemption.

A time query uses the short class when it has no window, a budget of at most
three hours, and a walk limit of at most one hour. Population cache lookup uses
the short class. A miss uses that class only for at most 16 origins and four
samples, with the same budget and walk limits. Other queries use the bulk class.
These are cost estimates, not latency guarantees. A long single-origin window is bulk work.
An all-cache-hit query does not wait for bulk routing or a workspace lease.

Use `--max-pending=128` to set the total waiting queue size. The value must be a
nonnegative integer. A value of `0` disables queueing, not concurrent execution.
One eighth of the queue, rounded up, is reserved for short queries: 16 of 128 by default.
The remainder is for bulk queries. Active work and response writes do not use queue slots.
This default bounds request metadata; it is not a CPU concurrency setting.
Eligible requests use arrival order within each class. Smaller requests can pass
a request that cannot fit. After one second, that request prevents new work from
passing. A request that already owns memory can resume to release that memory.

A full class queue returns HTTP 503 with `Retry-After: 1`, or a WebSocket error
with the message `router busy; retry later`. The server does not retry the query.

All server graphs share one population workspace pool. Use
`--workspace-memory-gib=8` to set its budget in whole GiB. The value must be a
positive integer. Both options also accept a separate value, such as
`--max-pending 4 --workspace-memory-gib 2`. Supply each option only once.
The server checks these settings before it loads graph files.

The pool allocates buffers when a population cache miss needs them. It reuses
compatible buffers across multiple graphs and layouts. Each active query owns
its buffers until all its workers finish. Pool locks protect only allocation records.
The pool discards idle buffers when another query needs their memory.
It can reduce the worker count to fit the estimated budget. If one worker cannot
fit, the query returns HTTP 422. The pool can retain several GiB between queries.

The same setting limits scheduler scratch estimates. CPU and scratch admission
are checked together. Each class has a memory share in proportion to its worker slots.
All routing engines receive an explicit worker budget. A pool memory wait returns
CPU slots but keeps its scratch reservation. It does not block point routing.
The pool accounts for all active and idle workspaces, not only the last query.
Packed estimates include fixed arrays and a scheduler allowance for dynamic scratch.
Returned buffers are measured before the pool retains them. A failed query discards
only its own buffers, after all its workers finish.

Response buffers have a separate budget of 1 GiB, or the scratch budget if smaller.
This budget has the same short/bulk split. The server reserves estimated encoding
space before Arrow encoding, then keeps the actual body bytes until the write ends
or fails. A slow client holds output memory, not CPU slots. A full output share
returns 503 without waiting for clients to read. A response too large for its share returns 422.

These are not process RSS limits. Resident graphs, allocation overhead, temporary
dictionary growth, geometry fallback, and garbage awaiting collection need extra RAM.
Idle pooled buffers can coexist with non-population scratch. Allow for both budgets
when you plan memory. Idle connections and other processes also use RAM.
Admission starts after HTTP body parsing and query validation. Incoming message
sizes and raw connection counts need separate proxy limits and timeouts.
These limits cannot guarantee that the process will not run out of memory.
Use physical RAM for this calculation. Do not count swap as routing capacity.
On a 64 GB host, leave a reserve for the operating system, resident graphs, temporary
allocations, and other processes. Measure actual use before you increase a budget.

To enable population queries, add `--population data/kontur_h3.arrow` to the command:

```sh
julia --threads=8 --project=. serve.jl --population data/kontur_h3.arrow path/to/timetable_res8.arrow
```

You can also use `--population=data/kontur_h3.arrow` or combine the option with `--demo` instead of network files.
Supply the option only once, with one path. The server rejects a missing path or a repeated option before it loads data.
Without this option, population queries return HTTP 400.
The Arrow file must contain unique, valid `h3::UInt64` cells at resolution 8 and numeric `population` values.
Both columns require a value in every row. Duplicate cells are rejected.
Values must be finite and zero or more. Fractional values are retained in `Float64` weights.
The server sums population by logical H3 parent at each loaded routing resolution from 0 to 8.
Population queries at finer resolutions return HTTP 400. Other metrics remain available.
A cell absent from the population file has zero population.
Startup reports validation and aggregation progress. Networks at the same resolution share one population map.
Startup aligns population weights with each prepared walking index.
Population routing uses CPU threads and `Float64` totals. See [population integration](../kontur_integration.md) for details.

Before it accepts external requests, the server runs synthetic queries to compile the routing and response code.
This warmup runs 246 queries. It includes routing, all six window modes, both distance modes,
the three population mode families, Arrow, HTTP, and WebSockets.
It runs once for all resolutions, then closes its temporary loopback listener.
Startup logs show the warmup time.

## Input

Use files in Arrow IPC file or stream format. Each column below, if supplied, requires a value in every row.
The server ignores extra columns.

| Column | Type | Meaning |
| --- | --- | --- |
| `from_h3` | `UInt64` | Valid H3 source cell |
| `to_h3` | `UInt64` | Valid destination cell at the source resolution |
| `departure_ms` | `UInt32` | Milliseconds after midnight, `0..86399999` |
| `duration_ms` | `Int64` | Connection duration in milliseconds, zero or more |
| `distance_km` | `Float64`, optional | Connection distance in kilometres; finite and zero or more |

All source and destination cells in a file must have the same resolution.
An empty input has resolution 5. Thus, the server rejects an empty file named `name_res7.arrow`.
The packed timetable contains two daily copies. Each row must satisfy this condition:

```text
departure_ms + duration_ms + 86400000 < 4294967295
```

The server reports a warning and skips rows with invalid durations.
It rejects invalid column definitions, H3 cells, and departure times.

## Model

- Transfers between stops in one H3 cell take zero time.
- Timetables repeat every 24 hours. Journey times include waiting. The model does not use dates or service calendars. It does not track whether you stay on the same vehicle.
- Walking uses great-circle distance between H3 cell centres at 5 km/h. The server rounds walking times up to integer milliseconds. It does not use roads, terrain, or water barriers.
- Walking can connect an origin to transit, two transit cells, or transit to a destination. Direct walking is also permitted. A transit connection permits another walk. Two consecutive walks are not permitted.
- A journey or walk can equal its time limit. The default walking limit is one hour per walk. Set `max_walk_h=0` to disable all walking.
- Itinerary distance follows the selected earliest-arrival route. The selection is repeatable, but it does not minimize distance between equal-time routes. Straight-line distance is the distance between cell centres.

The server adds a **fictional Elvas-Badajoz shuttle** to every graph loaded from a file.
The shuttle takes 15 minutes and travels 13.88 itinerary kilometres in each direction.
It departs every minute from 04:00 through 23:30, including both endpoints.
Without an input `distance_km` column, transit kilometres are unknown.

Station coordinates: [OpenStreetMap](https://www.openstreetmap.org), copyright OpenStreetMap contributors, [ODbL](https://www.openstreetmap.org/copyright).

## Query

Query times use **hours**. Input timetable times use milliseconds.
Use an origin cell at a resolution available in the selected network.
These examples use resolution 5:

```sh
# Single departure. Return elapsed hours; metric=time is the default.
curl --fail --show-error 'http://127.0.0.1:1988/reachable?index=85075dd7fffffff&departure_h=8&budget_h=1' -o reachable.arrow

# Minimum elapsed time. Sample a two-hour window every 15 minutes.
curl --fail --show-error 'http://127.0.0.1:1988/reachable?index=85075dd7fffffff&departure_h=8&budget_h=3&window_h=2&step_h=0.25&max_walk_h=0.5&window_mode=min_union' -o window.arrow

# Time rank minus distance rank. Use straight-line distance.
curl --fail --show-error 'http://127.0.0.1:1988/reachable?index=85075dd7fffffff&departure_h=8&budget_h=3&distance_mode=straight_line&metric=time_distance_quantile' -o ranks.arrow
```

| `window_mode` | Include a cell when | Time statistic or displayed value |
| --- | --- | --- |
| `mean_intersection` (default) | Every sampled departure can reach it | Mean elapsed time |
| `min_union` | At least one sampled departure can reach it | Minimum elapsed time |
| `max_intersection` | Every sampled departure can reach it | Maximum elapsed time |
| `diff_union` | At least one sampled departure can reach it | Maximum minus minimum elapsed time; use the budget as the maximum if any sample cannot reach it |
| `diff_intersection` | Every sampled departure can reach it | Maximum minus minimum elapsed time |
| `reachable_union` | At least one sampled departure can reach it | Fraction of sampled departures that can reach it, from 0 to 1 |

`distance_mode=itinerary` is the default. Minimum and difference modes use the best sample's itinerary distance.
Maximum mode uses the worst sample's distance. Mean and coverage modes average distance over reachable samples.
If equal times occur, the server selects the earliest sampled departure.
Itinerary quantiles require an input `distance_km` column.
Use `metric=time` with `reachable_union` when window sampling is active.

Use `metric=accessible_population&origin_radius=2` for independent origins within two H3 grid steps.
The radius is independent of walking and includes cells with zero population or outside the transit graph.
The default radius of zero selects only the specified origin.
Results contain only origin H3 indices and `value::Float64` in people. The origin's population counts at zero time.
Add `exclude_origin_population=true` to exclude each result origin's own routing-cell population.
The default is `false`. Values `1` and `0` are also valid. This option applies to point queries and all window modes.
Other metrics ignore this option, including invalid values. Population queries reject empty or invalid values with HTTP 400.
The response contains one cell per origin with a positive final total. Zero totals are omitted, including the query origin.
An origin with zero local population is included if its accessible total is positive.
`X-Router-Origin-Count` reports origins examined. All-zero results contain an empty Arrow table.
Intersection modes count cells reached in every sample. `min_union` and `diff_union` count cells
reached in any sample. `reachable_union` returns mean accessible population in people.
Each reached cell contributes its whole population, once per origin and sample.
Population queries calculate origin totals and ignore `distance_mode`.
Other metrics ignore `origin_radius` values. Duplicate parameters return HTTP 400. See the query contract for details.

HTTP `/reachable` and WebSocket `/query` use the same [query contract](docs/api.md).
Time and quantile results contain `value`, `elapsed_h`, and split H3 indices.
Set `encoding=string` to get a hexadecimal `index` column instead.
Time values are hours. For `metric=time`, `reachable_union` values are fractions.
`time_distance_quantile` values are rank differences with no unit.

Set `window_h=0` or `step_h=0` to query one departure without averaging.
The default `window_h` is zero. For these queries, the server ignores `window_mode`, including unknown or empty values.

[H3-MON](https://github.com/bovine3dom/H3-MON) can display the responses.
Set an `onclick` hook's URL to a query above, with `index={index}`.
Set the hook's `resolution` to an available resolution.
For WebSockets, add `socket: "ws://127.0.0.1:1988/query"` to the hook.
The frontend controls cartogram weights and display settings.

The server runs one routing request at a time across all graphs.
Each WebSocket connection keeps one active query and the newest pending query.
Large windows and walking areas can increase processing time and memory use.
The server accepts all browser origins. It does not provide authentication.
For remote access, use a proxy that provides TLS and authentication.

## Tests

```sh
julia --threads=1 --project=. test/runtests.jl
julia --threads=8 --project=. test/runtests.jl
```

Optional [GPU experiments](../experiments/gpu/README.md) use a separate environment outside the production server.

The code uses the licence in [LICENSE](LICENSE). Timetable data uses its source licences.

## Population Result Cache

Each HTTP handler has one in-memory population cache.
The cache holds at most 100,000 `Float64` origin totals, including zero. This
capacity can hold several requests with tens of thousands of origins. When full,
the cache replaces the oldest inserted entry (FIFO). Hits do not change this
order. There is no capacity setting or TTL.

The key contains the actual origin, integer departure time, budget, effective
sample schedule, effective walk limit, population coverage mode, and origin
exclusion setting. Equivalent coverage modes use the same key. The centre and
radius select origins; they are not part of each origin key. A change to the
departure time does not reuse results from an earlier time window.

Each handler has a separate cache, protected by the request lock. The graph,
population, and walking geometry must not change during its lifetime. To replace
these inputs, create a new handler. Direct `route_population` calls are not cached.

Validation occurs before cache lookup. All missing origins use one routing call
with the existing tile selection and reference fallback. Only complete successful
results enter the cache. Routing excludes origin population without subtraction
from cached totals. HTTP output omits zeros. WebSocket uses the same handler cache.

Population responses include `X-Router-Cache-Hits` and `X-Router-Cache-Misses`.
These headers count origins, including zero totals. `X-Router-Origin-Count`
counts all selected origins. Worker and expansion headers count only new work.
An all-hit request does not prepare sources or route origins.

Run the synthetic spatial overlap benchmark from the repository root:

```sh
julia --project=router --threads=8 experiments/benchmarks/benchmark-population-cache.jl
```

The benchmark reports cold, repeat, and moved-centre requests after compilation
and input preparation. It uses synthetic data and does not measure HTTP output.
