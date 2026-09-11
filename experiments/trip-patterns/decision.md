# Austria Trip-Pattern Decision

Stop this implementation path. Raw position scans, indexed position scans,
FIFO pattern sweeps, and time-ordered sweeps did not justify production use.
This is not a general claim about trip-based routing algorithms. The experiment
used independent daily legs, not a selected-trip scan with vehicle continuity.

## Input Audit

Measured on 2026-09-11 with Julia 1.12.7. Group rows by `(source, trip_id)` and
order by `stop_sequence`. Remove exact duplicates across all original columns.
Exclude the same complete ambiguous groups as the approved adjacent export.
The source and export were unchanged. No shuttle was added.

| Item | Count |
| --- | ---: |
| Original stop rows | 6,111,604 |
| Source-qualified trip groups | 274,765 |
| Exact duplicate rows removed | 1,632,701 |
| Ambiguous groups excluded | 3,660 |
| Accepted trips | 271,105 |
| Accepted stop occurrences | 4,340,616 |
| Accepted adjacent legs | 4,069,511 |
| Ordered H3 patterns | 29,849 |
| Pattern leg positions | 512,335 |
| Patterns with repeated cells | 24,504 |
| Network cells / directed edges | 20,244 / 61,796 |
| Production daily profile entries | 4,791,297 |

| Input | SHA-256 |
| --- | --- |
| `data/at_test.arrow` | `d0136a30a7c058585e0f5cbfb6038551f4471f06970dd0520c308f2ecdbe1f43` |
| `data/austria_adjacent_res8.arrow` | `1f7868950158495ad72cfc47b2d1659ffc21329805de558ac312be2a2b0b1de7` |

The source had 954,533,394 bytes. Every accepted leg matched the export in order,
endpoints, daily departure, and duration. Legs used `departure_clock_ms` and the
next arrival epoch minus the current departure epoch. They recur daily without
service calendars, pickup rules, or drop-off rules. Intermediate boarding and
repeated cells remain valid. See the active [trip-shortcut audit](../trip-shortcuts/results.md).

There were 608 strict departure/arrival inversions at 96 of 512,335 positions,
from 162,143,229 unordered pairs including ties. Consecutive daily departure
orders had 137,819 strict inversions at 1,263 of 482,486 transitions. Ties did not
count. Midnight can rotate clock order without physical overtaking. Leg-start
clocks had 1,767 rollovers; the full audit, including terminal departures, had
2,060. These tests do not prove common service days or a safe single-trip rule.

## Correctness And Method

The final suite passed **47,591 assertions at each of one and eight threads**:
6,101 adversarial, 9,987 ordered-key, 303 inversion, and 31,200 checks across
300 seeded random fixtures. All variants matched independent A/E labels.
A is transit-ready; E is walk-eligible. Transit improves both; walking reads E
and improves only A. Coverage masks, zero masks, and fractional weights were
checked. Cases included repeated cells, E-only self-loops, stale tasks, updates
behind the active cursor, equal-time cycles, midnight, multi-day durations,
inclusive cutoffs, off-network walks, and the reserved UInt32 INF boundary.

All 16 final Austria cases had exact A/E and coverage-mask parity. Population
totals and zero masks matched production exactly; maximum absolute error was
zero. The permitted weight tolerance was `rtol=1e-12, atol=1e-8`. Every measured
repeat matched its warm result. The population input had 32,957,699 rows.

Each case used one origin and one sample. The point scanners were serial in an
eight-thread process. Methods shared the graph, walking, population, and prepared
schedule bounds. Production was called directly without a result-cache lookup.
After compilation, three rounds used production/FIFO/ordered orders `123`, `231`,
and `312`, with full GC before each call. Query workspaces were fresh; indexes
were resident. There was no hardware-cache flush or host isolation.

## Final Point Times

Times are median milliseconds. Vienna used `881e15b467fffff`; Salzburg used
`881f89af4dfffff`. These were the nearest network cells to 48.1855, 16.3768 and
47.813, 13.046, at distances of 0.460 and 0.420 km.

| City | Time | Budget h | Walk h | Production | FIFO | Ordered |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Vienna | 08:00 | 1 | 0 | 1.520 | 10.567 | 10.432 |
| Vienna | 08:00 | 3 | 0 | 3.214 | 24.370 | 28.653 |
| Vienna | 23:30 | 1 | 0 | 0.828 | 6.524 | 7.678 |
| Vienna | 23:30 | 3 | 0 | 0.893 | 9.005 | 14.942 |
| Salzburg | 08:00 | 1 | 0 | 0.542 | 2.904 | 2.818 |
| Salzburg | 08:00 | 3 | 0 | 3.769 | 30.171 | 34.909 |
| Salzburg | 23:30 | 1 | 0 | 0.717 | 1.491 | 2.862 |
| Salzburg | 23:30 | 3 | 0 | 0.470 | 2.355 | 5.149 |
| Vienna | 08:00 | 1 | 1 | 6.374 | 13.513 | 19.558 |
| Vienna | 08:00 | 3 | 1 | 31.299 | 32.465 | 52.530 |
| Vienna | 23:30 | 1 | 1 | 3.761 | 7.157 | 14.175 |
| Vienna | 23:30 | 3 | 1 | 7.816 | 13.680 | 20.874 |
| Salzburg | 08:00 | 1 | 1 | 2.120 | 2.560 | 3.720 |
| Salzburg | 08:00 | 3 | 1 | 18.442 | 24.387 | 40.401 |
| Salzburg | 23:30 | 1 | 1 | 1.395 | 2.572 | 2.716 |
| Salzburg | 23:30 | 3 | 1 | 3.162 | 4.642 | 7.467 |

Ordered lost all 16 point and label-only comparisons with production. FIFO also
lost all final point comparisons. Two small ordered gains over FIFO without
walking do not establish a robust advantage. Vienna's production morning 3 h
walking median was 25.099 ms in an earlier run and 31.299 ms here. Shared-host
variation matters. Even the slowest final matched production sample, 32.703 ms,
was below the fastest ordered sample, 50.264 ms.

## Work And Memory

For Vienna at 08:00, 3 h budget, 1 h walking, ordered reduced profile lookups
from 333,438 to 303,991 but increased queue pops from 41,435 to 91,308, including
52,172 stale pops. Position visits remained high: 544,962 versus 530,374.
Point allocation was 5,305,600 bytes for production, 3,999,320 for FIFO, and
4,695,216 for ordered. Lower allocation did not give a time gain.

Two-day position profiles retained 7,534,789 of 8,139,022 entries. They used
70,696,836 additional array bytes and 116,700,928 bytes including the raw index.
The production graph used 130,902,128 bytes. Shared object sizes overlap; do not
add them to estimate RSS. The experiment also retained the production graph.
Final audit time was 108.201 s, graph packing 3.911 s, raw packing 0.126 s, and
profile packing 0.253 s. Profile packing allocated 450,732,936 bytes. Maximum
process RSS was 3,704,164,352 bytes. Packing time alone is not startup time.

Duplicate spatial positions and repeated label correction remained costly.
The closed scripts and logs are removed; this report retains the decision and
final evidence. No multi-origin, time-window, or full router suite was run for
this path. Do not infer production suitability or a general trip-based limit.
