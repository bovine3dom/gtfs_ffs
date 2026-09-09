# Timetable Router

This Julia server calculates which H3 cells you can reach within a time budget.
It uses daily timetables and estimated walking times. It returns the results as Arrow data.
Use the results to make maps, not to plan turn-by-turn journeys.

## Run

Install Julia. Run these commands from this directory:

```sh
julia --project=. -e 'using Pkg; Pkg.instantiate()'
julia --threads=8 --project=. serve.jl path/to/timetable_res*.arrow
```

Supply one Arrow file for each `(network, H3 resolution)` pair.
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

```sh
julia --threads=8 --project=. serve.jl /data/rail_and_friends_res*.arrow /data/everything_res*.arrow
```

Add `network=everything` to a query to select that network.
If you omit `network`, the server uses the network of the first command-line file.
The shell expands filename patterns before it supplies this file list.
The server uses the origin cell's H3 resolution.
An unavailable resolution returns HTTP 400.
Startup logs show the default network and the resolutions in each network.

To run a small example with synthetic data, use this command:

```sh
julia --threads=8 --project=. serve.jl --demo
```

Use `--demo` instead of input files to serve network `demo` at resolution 5.
`ROUTER_HOST` defaults to `127.0.0.1`. `ROUTER_PORT` defaults to `1988`.
Walking preparation and window routing use the available threads in Julia's default thread pool.
The server keeps graphs and walking indexes in memory.

Before it accepts external requests, the server runs synthetic queries to compile the routing and response code.
This warmup includes Arrow, HTTP, and WebSockets. It runs once for all resolutions, then closes its temporary loopback listener.
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

HTTP `/reachable` and WebSocket `/query` use the same [query contract](docs/api.md).
Arrow results contain `value`, `elapsed_h`, and split H3 indices.
Set `encoding=string` to get a hexadecimal `index` column instead.
Time values are hours. `reachable_union` values are fractions.
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

The code uses the licence in [LICENSE](LICENSE). Timetable data uses its source licences.
