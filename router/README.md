# Timetable Router

Where can you get to, and how long does it take? A Julia CPU router for turning
daily H3 timetables into Arrow reachability maps, with estimated walking and
departure windows. Built for visualisation, not turn-by-turn journey planning.

## Run

From this directory, with Julia installed:

```sh
julia --project=. -e 'using Pkg; Pkg.instantiate()'
julia --threads=8 --project=. serve.jl path/to/timetable_res*.arrow
```

Supply one Arrow file per H3 resolution. Filenames are arbitrary; the origin cell
selects the matching graph. Files load before the server listens, duplicate
resolutions are rejected, and changing files requires a restart.

For a small synthetic example instead:

```sh
julia --threads=8 --project=. serve.jl --demo
```

`--demo` cannot be combined with files. `ROUTER_HOST` and `ROUTER_PORT` default to
`127.0.0.1` and `1988`. Walking preparation and window routing use Julia's available
default-pool workers. Graphs and walking indexes stay in memory.

## Input

Arrow IPC files or streams with these non-null columns; extra columns are ignored:

| Column | Type | Meaning |
| --- | --- | --- |
| `from_h3` | `UInt64` | Valid H3 source cell |
| `to_h3` | `UInt64` | Valid destination at the same resolution |
| `departure_ms` | `UInt32` | Milliseconds after midnight, `0..86399999` |
| `duration_ms` | `Int64` | Nonnegative connection duration in milliseconds |
| `distance_km` | `Float64`, optional | Finite, nonnegative connection kilometres |

All endpoints in a file must share one resolution. Empty inputs default to res5.
Packing retains two daily profile copies, requiring
`departure_ms + duration_ms + 86400000 < 4294967295`.
The server warns and skips invalid duration rows; malformed schemas, cells and
departure clocks are rejected. Input files are never rewritten.

## Model

- Stops in one H3 cell are freely interchangeable, with zero transfer cost.
- Timetables repeat every 24 hours. Waiting counts; dates, calendars and trip continuity do not.
- Walking uses H3-centre great-circle distance at 5 km/h, rounded up to milliseconds. It ignores roads, terrain and water.
- Origins can walk to transit or surrounding cells. Transit enables another walk; consecutive walking hops are forbidden.
- Journey budgets and per-hop walking limits are inclusive. `max_walk_h=0` disables access, transfer and egress walks; the default is one hour per hop.
- Itinerary kilometres follow the deterministic earliest-arrival itinerary, not a distance-optimal tie-break. Straight-line kilometres measure cell-centre separation.

File-backed startup automatically adds a **fictional Elvas-Badajoz shuttle**:
15 minutes and 13.88 itinerary km in both directions, every minute from 04:00
through 23:30 inclusive. This is not a published service. `--demo` does not add it.
Without an input `distance_km` column, transit kilometres are unknown.

Station coordinates: [OpenStreetMap](https://www.openstreetmap.org), copyright OpenStreetMap contributors, [ODbL](https://www.openstreetmap.org/copyright).

## Query

Query times are **hours**, unlike the input timetable's milliseconds. Use an origin
at a loaded resolution; these examples use res5:

```sh
# Single departure, elapsed hours (metric=time is the default)
curl --fail --show-error 'http://127.0.0.1:1988/reachable?index=85075dd7fffffff&departure_h=8&budget_h=1' -o reachable.arrow

# Best elapsed time across a two-hour window, sampled every 15 minutes
curl --fail --show-error 'http://127.0.0.1:1988/reachable?index=85075dd7fffffff&departure_h=8&budget_h=3&window_h=2&step_h=0.25&max_walk_h=0.5&window_mode=min_union' -o window.arrow

# Distance rank minus time rank, using straight-line kilometres
curl --fail --show-error 'http://127.0.0.1:1988/reachable?index=85075dd7fffffff&departure_h=8&budget_h=3&distance_mode=straight_line&metric=distance_time_quantile' -o ranks.arrow
```

| `window_mode` | Included cells | Time statistic / displayed value |
| --- | --- | --- |
| `mean_intersection` (default) | Every sample reaches | Mean elapsed |
| `min_union` | Any sample reaches | Minimum elapsed; best-sample itinerary km |
| `max_intersection` | Every sample reaches | Maximum elapsed; worst-sample itinerary km |
| `diff_union` | Any sample reaches | Worst minus best elapsed; missing worst capped at budget; best-sample itinerary km |
| `reachable_union` | Any sample reaches | Reachable percentage (0..100); capped mean elapsed as context |

Extrema ties select the earliest departure. Coverage mode retains conditional mean
itinerary km and rejects quantiles for active windows. `distance_mode=itinerary`
is the default; itinerary quantiles require an input `distance_km` column.

HTTP `/reachable` and WebSocket `/query` share the [query contract](docs/api.md).
Arrow results contain `value`, `elapsed_h` and split H3 indices; use
`encoding=string` for hexadecimal `index` instead. Time values are hours;
`reachable_union` values are percentages, and `distance_time_quantile` values are
dimensionless rank differences. `window_h=0` (default) or `step_h=0` selects a single departure without averaging; `window_mode` is ignored, even unknown or empty values.

[H3-MON](https://github.com/bovine3dom/H3-MON) can display these responses directly.
Set an `onclick` hook's URL to a query above with `index={index}`, and its
`resolution` to a loaded resolution. Add `socket: "ws://127.0.0.1:1988/query"`
for WebSockets. Cartogram weights and presentation belong to the frontend.

Requests run one at a time across all graphs. Each socket retains one active and
one latest pending query. Large windows and walking radii can be expensive.
The server accepts all browser origins and has no authentication; use a TLS and
authentication proxy for remote access.

## Tests

```sh
julia --threads=1 --project=. test/runtests.jl
julia --threads=8 --project=. test/runtests.jl
```

Code licence: [LICENSE](LICENSE). Timetable data has its own source licences.
