# Austria Trip Shortcuts

## Status

The user approved exclusion of all 3,660 ambiguous trip groups from both graphs.
Both graphs were generated. Production code and `data/at_test.arrow` were not changed.

The experiment keeps all valid adjacent connections. For a trip with `n` legs,
it divides the legs into nonoverlapping segments of `ceil(sqrt(n))` legs. It adds
one endpoint shortcut per segment with more than one leg, including a final
partial segment. It does not add all-pairs or star connections.

## Input

Inspection date: 2026-09-10. Scope: Austria feeds, including stops outside Austria.

- File: `data/at_test.arrow`
- Size: 954,533,394 bytes
- SHA-256: `d0136a30a7c058585e0f5cbfb6038551f4471f06970dd0520c308f2ecdbe1f43`
- Stop rows: 6,111,604
- Trip groups, keyed by `(source, trip_id)`: 274,765
- Exact duplicate rows across all original columns: 1,632,701
- Missing stop sequences: 0
- Ambiguous trip groups after exact deduplication: 3,660
- Raw rows in ambiguous trips: 655,056
- Conflicting sequence groups: 49,138
- Distinct rows within those conflicting groups: 117,784

| Feed | Raw Rows | Trips | Ambiguous Trips |
| --- | ---: | ---: | ---: |
| at_Linz-AG-2026.gtfs | 127,477 | 6,721 | 0 |
| at_PTA-Carinthia-Flex-2026.gtfs | 1,902,726 | 20,323 | 3,642 |
| at_PTA-Eastern-Region-Flex-2026.gtfs | 1,770,766 | 97,731 | 0 |
| at_PTA-Salzburg-Flex-2026.gtfs | 350,194 | 15,173 | 0 |
| at_PTA-Styria-Flex-2026.gtfs | 694,640 | 60,269 | 0 |
| at_PTA-Tyrol-2026.gtfs | 423,743 | 20,180 | 0 |
| at_PTA-Upper-Austria-Flex-2026.gtfs | 301,681 | 12,340 | 18 |
| at_PTA-Vorarlberg-Flex-2026.gtfs | 276,129 | 14,803 | 0 |
| at_Railway-Current-Reference-Data-2026.gtfs | 264,248 | 27,225 | 0 |

## Schema

| Columns | Actual Type |
| --- | --- |
| source, trip_id, stop_id, stop_name | String |
| stop_sequence | Union{Missing, UInt32} |
| route_type | UInt16 |
| h3 | UInt64 |
| arrival_epoch_ms, departure_epoch_ms | Int64 |
| departure_clock_ms | UInt32 |
| stop_lat, stop_lon | Float64 |
| pickup_type, drop_off_type | Union{Missing, UInt8} |

Only the restriction columns contain missing values: 798,446 for `pickup_type`
and 800,998 for `drop_off_type`.

All H3 values are valid resolution-11 cells. All exceed 52 bits; this is not an
error for an H3 index. Keep these values as `UInt64`, not `Float64`. No H3 value
is zero. All coordinates are finite and within latitude and longitude limits.
Latitude ranges from 41.911106 to 53.55309. Longitude ranges from 4.3357277 to
30.488739.

Both epoch columns range from 1,767,225,600,000 to 1,767,372,240,000 ms.
There are no zero or negative epochs, negative dwells, or invalid daily clocks.
There are 5,923,595 zero-dwell raw rows.

After exact deduplication, the 271,105 unambiguous trips contain 4,340,616 stop
rows and 4,069,511 adjacent legs. None has a single stop. These legs have no
negative durations. There are 2,060 departure day transitions and 2,060 clock
rollovers. There are no clock-to-epoch offset changes in these trips.
Leg timing counts exclude ambiguous trips because their adjacency is undefined.
The input file is unchanged. Both exports exclude the same complete ambiguous trips.

## Reproduce

For the generated files, run the tests and CPU comparison:

```sh
julia --project=experiments/gpu experiments/trip-shortcuts/test.jl
julia --threads=8 --project=experiments/gpu experiments/trip-shortcuts/routing_tests.jl
julia --project=experiments/gpu experiments/trip-shortcuts/verify_exports.jl
julia --threads=8 --project=experiments/gpu experiments/trip-shortcuts/benchmark.jl --backend=cpu
```

To generate files with a fresh prefix:

```sh
julia --threads=8 --project=experiments/gpu experiments/trip-shortcuts/prepare.jl data/at_test.arrow 8 data/austria_repeat
```

The benchmark and export audit use the default `data/austria_*` paths.
The separate `inspect.jl` command reports the raw input conflicts and exits with
an error for this dataset. `prepare.jl` applies the approved exclusions.

The preprocessor verifies the approved input hash and exclusion count. It refuses
to overwrite any output. An optional input path, target resolution (default 8),
and output prefix are positional arguments. Use a fresh prefix to repeat export.

## Generated Graphs

All output files use uncompressed Arrow file format. Graph columns are
`from_h3::UInt64`, `to_h3::UInt64`, `departure_ms::UInt32`, `duration_ms::Int64`,
and `distance_km::Float64`. H3 parents come from original resolution-11 cells.
The shortcut file starts with the complete baseline columns, in the same order.

| Table | Rows | Endpoint Pairs | Self-Edge Rows | Column Bytes |
| --- | ---: | ---: | ---: | ---: |
| Adjacent | 4,069,511 | 61,796 | 1,601,370 | 146,502,396 |
| Shortcuts only | 877,556 | 60,710 | 27,392 | 31,592,016 |
| Combined | 4,947,067 | 112,536 | 1,628,762 | 178,094,412 |

There are 789,674 distinct added connection rows not present in the baseline.
No valid-trip legs or candidate shortcuts were rejected. Preparation took
90.394 seconds, including inspection. Total generation took 97.915 seconds.
See `preparation.log` for schema samples and counters.

| File Under `data/` | Bytes | SHA-256 |
| --- | ---: | --- |
| austria_adjacent_res8.arrow | 146,503,498 | `1f7868950158495ad72cfc47b2d1659ffc21329805de558ac312be2a2b0b1de7` |
| austria_shortcuts_res8.arrow | 178,095,514 | `8617f50ed842d573797599ca17c8684528045b22b891c9a58473f6e2124d5749` |
| austria_excluded_trips.arrow | 317,962 | `43ec74b72f3218110f09e0bb9d83a30ae93bc9dc4f34ee0b67932c2b02246ef0` |

The exclusion file records `source`, `trip_id`, `reason`, `raw_rows`, and
`unique_rows` for every excluded trip. Exact deduplication compares every original
column. It does not remove distinct stop occurrences or collapse repeated cells.

Adjacent duration is next arrival epoch minus current departure epoch. Shortcut
duration is endpoint arrival epoch minus start departure epoch. This includes
dwell and midnight. Both use exact integer milliseconds and the same daily clock.
Invalid legs are not bridged. A dwell inversion or clock-offset change blocks a
shortcut across that boundary. The profile bound is UInt32 INF, not 24 hours.
Zero epochs are treated as missing for this inspected 2026 snapshot.

Distance uses `H3.Lib.greatCircleDistanceKm` on stop coordinates in radians.
Shortcut distance is the sum of constituent adjacent distances. This spherical
model is common to both graphs; it does not reproduce an older WGS84 SQL export.
Pickup and drop-off restrictions remain ignored in both graphs. Arrival and
population parity are required. Chosen-itinerary distance ties can differ.

## CPU Results

Julia 1.12.7, eight default threads. Origin `881e15b467fffff` is the graph cell
nearest Vienna Hbf, 0.460 km from the specified coordinates. It also equals the
parent of the resolution-11 coordinate cell. Departure is 08:00; budget is three
hours; walking limit is one hour. Windows use 15-minute steps and
`mean_intersection`. Timings include Arrow encoding of origin totals.

| Graph | Nodes | Packed Edges | Profile Entries | Packed Array Bytes |
| --- | ---: | ---: | ---: | ---: |
| Adjacent | 20,244 | 61,796 | 4,791,297 | 77,645,240 |
| Shortcuts | 20,244 | 112,536 | 5,873,381 | 95,567,464 |

Both graphs share one prepared walking index because their sorted cells match.
It contains 638,524 network walks, 2,207,121 output walks, and 124,917 output cells.
Walking preparation took 1.322 seconds. No shuttle was added.

Population input is `data/kontur_h3.arrow`, with 32,957,699 rows and total population
8,031,924,024. Its SHA-256 is
`c21eaf6c3eb65563e80f2055347ad13f979014a190f8472817c25a591f427eb3`.
The raw load took 2.603 seconds. The first CPU warm-up also includes weight preparation.

| Origins | Samples | Adjacent (s) | Shortcuts (s) | Warm Repetitions A/S |
| ---: | ---: | ---: | ---: | --- |
| 1 | 1 | 0.022321 | 0.025184 | 1 / 3 |
| 127 | 1 | 0.135768 | 0.133875 | 3 / 3 |
| 127 | 4 | 0.475428 | 0.526236 | 3 / 3 |
| 127 | 96 | 8.812628 | 9.411237 | 3 / 3 |
| 331 | 96 | 23.975891 | 25.999008 | 1 / 1 |
| 1,027 | 96 | 68.030095 | 70.703775 | 1 / 1 |

Values are medians after one warm-up. A warm-up longer than ten seconds selects
one measured repetition; otherwise the script selects three. The one-origin
request uses one worker, the 127-origin point request uses two, and window
requests use eight. Every returned origin total matched exactly between graphs.
For 127 origins and 96 samples, allocation was 367,217,592 versus 375,669,768 bytes;
shared expansions were 8,355,286 versus 8,354,918. Sparse shortcuts do not improve
these larger CPU requests.

Transit-only point medians were 0.002769 versus 0.003622 seconds, with 5,350 reached
nodes. Full transit labels matched at eight graph origins, three departure
boundaries, and both three-hour and seven-day budgets. Walking arrival outputs
matched at 07:59:59.999, 08:00:00.000, and 08:00:00.001. At exactly 08:00, the
walking query covered 31,249 cells; one-origin accessible population was 5,785,472.

## Tests

All 14 inspection tests, 1,443 routing/export tests, and 19 full-export audit tests
passed. The routing suite passed with both `KA.CPU` and Intel P630. Tests cover segment sizes, final partial
segments, dwell, midnight, constant clock offsets, zero-duration cycles, repeated
cells, invalid gaps, millisecond boundaries, both walking labels, all six
population modes, and both CPU origin-population exclusion settings. GPU tests
use default inclusion. Every fixture shortcut has a
feasible baseline route at its departure. GPU population uses the existing
Float32 tolerance (`rtol=2e-6`, `atol=1e-3`).

## GPU Results

Intel HD Graphics P630, oneAPI.jl 2.7.2, process-local legacy driver. These are
complete request times, including planning, uploads, synchronization, convergence
checks, download, and Arrow encoding. Resident construction is separate. Full
garbage collection runs before each device query, outside the measured time.
The prototype kernel and its launch policy were not changed.

| Origins | Samples | Adjacent (s) | Shortcuts (s) | Rounds A/S | Batches |
| ---: | ---: | ---: | ---: | --- | ---: |
| 1 | 1 | 0.102587 | 0.055394 | 51 / 23 | 1 |
| 127 | 1 | 1.078034 | 0.803068 | 204 / 109 | 4 |
| 127 | 4 | 5.105526 | Not completed | 1,036 / Not measured | 16 |

The complete point comparisons used one measured adjacent request and the median
of three shortcut requests. Population errors were zero against the CPU for all
completed queries. One-origin device work with checks took 0.098139 versus
0.052490 seconds; downloaded data was 208 versus 96 bytes. For 127 origins and
one sample, device work with checks took 1.067065 versus 0.795024 seconds;
downloaded data was 1,324 versus 944 bytes. See `gpu-point-results.log` and
`gpu-many-results.log` for startup times and request phases.

Shortcuts improve these GPU point requests by 1.85x and 1.34x, but the optimized
CPU remains faster. The GPU four-sample baseline warm-up took 205.058 seconds,
although its next request took 5.106 seconds. The 300-second process timeout
occurred while the shortcut query waited for device synchronization. The process
exited. This does not establish a four-sample shortcut time or a specific driver
root cause. CPU measurements from this GPU session also contain timing noise;
the CPU table above comes from the earlier CPU-only run.

No 96-sample GPU request, larger GPU origin disk, CUDA hardware benchmark, or
seven-day population benchmark was run. Seven-day transit label parity was checked.
The CPU CLI/log path was checked again with one origin and with 127 origins/four
samples; see `cpu-check-results.log`. Its totals still matched exactly.

### Memory

Resident device array storage is 72,940,512 bytes for adjacent and 82,003,104 bytes
for shortcuts. Each includes 10,364,928 bytes for four UInt32 label arrays with
32 lanes per node (`4 * 32 * N * 4`). These are logical allocations, not measured
peak VRAM or driver memory. The first resident constructor took 7.312 seconds,
including population preparation; the second took 0.341 seconds.

Per-request arrays add `8*C + 128 + 4*min(256,ceil(C/256))*T + 4*O` bytes for
coverage, persistent masks, lane masks, partial reductions, and totals. Here `C`
is the request output cell count, `T` is origins per tile, and `O` is origin count.
Add source IDs (`4*O`), two pointer arrays (`16*(O+1)`), access IDs and times
(`8*access_rows`), and direct output IDs (`4*direct_rows`). If output cells extend
beyond the prepared set, add an uploaded weight vector (`4*C`). Driver command
lists, memory pools, and host population dictionaries are additional storage.
Downloaded totals and round flags use `4*O + 4*rounds` bytes.

### Device Commands

Use a fresh log path. Each command uses a separate process. CUDA must already be
available in the selected environment on the NVIDIA machine; no CUDA dependency
was installed on this host.

```sh
ZE_ENABLE_ALT_DRIVERS=/usr/lib/libze_intel_gpu_legacy1.so.1 timeout 300 julia --threads=8 --project=experiments/gpu experiments/trip-shortcuts/benchmark.jl --backend=oneapi --case=0:1 --log=experiments/trip-shortcuts/new-gpu-point.log
julia --threads=8 --project=experiments/gpu experiments/trip-shortcuts/benchmark.jl --backend=cuda --case=6:4 --log=experiments/trip-shortcuts/new-cuda.log
```
