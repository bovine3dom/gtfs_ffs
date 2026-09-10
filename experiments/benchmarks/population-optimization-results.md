# Population Optimization Benchmarks

## Final Default-16 Results

These are the final cache-free CPU results. Public calls omit
`origin_batch_size`; the production window default is 16. Point queries
retain the default of 64. The schedule-cache keyword, helper, and workspace
field are absent. This final phase did not change production, README,
frontend, or GPU files.

Every timed call matched every frozen origin and population value exactly.
All ten final rows use three new measured calls after one warm call. The
mode is the public default, `mean_intersection`. Times exclude graph load,
walking preparation, population rollup, and population CSR preparation.
The same graph and population payloads were used throughout.

| Walk, hours | Origins | Budget, hours | Samples | Frozen baseline, seconds | Final default, seconds | Speed Ratio | Final allocation, MB |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 127 | 3 | 96 | 4.044133 | 0.359967 | 11.23x | 44.600 |
| 1 | 331 | 3 | 96 | 11.379481 | 0.908105 | 12.53x | 46.720 |
| 1 | 1,027 | 3 | 96 | 19.698348 | 1.873117 | 10.52x | 48.477 |
| 1 | 127 | 168 | 4 | 7.128525 | 0.417346 | 17.08x | 100.304 |
| 1 | 127 | 168 | 96 | 112.251257* | 8.738933 | 12.84x | 110.962 |
| 2 | 127 | 3 | 96 | 4.665312 | 0.644716 | 7.24x | 74.092 |
| 2 | 331 | 3 | 96 | 10.746704 | 1.636985 | 6.56x | 75.110 |
| 2 | 1,027 | 3 | 96 | 17.786890* | 3.038466 | 5.85x | 78.766 |
| 2 | 127 | 168 | 4 | 9.587326 | 1.100595 | 8.71x | 293.522 |
| 2 | 127 | 168 | 96 | 132.813579* | 23.661530 | 5.61x | 590.122 |

`*` identifies one measured frozen call, not a three-call median. All other
frozen times are the earlier three-call medians. MB means 1,000,000 bytes.
The baseline commit is `3980e5f00c5cdcc3a423b7009299fb90f377fd55`.

The one-hour cases meet a 10x target in these measurements. The actual
two-hour-walking cases improve by 5.61-8.71x. No actual-walking case in this
final matrix reaches 10x, and no 100x improvement was measured. The earlier
interleaved two-hour control supports the result: batch 16 was 7.30x faster
than frozen and 1.54x faster than batch 8 in that control.

### Long Walking Case

The previously unrun case now has a measured frozen baseline: 127 origins,
two-hour walking, a seven-day budget, and 96 samples. It ran once, after
the nine smaller final results had been saved. Its dedicated ceiling was
600 seconds. The call finished in 132.813579 seconds; no timeout occurred.
It did not use the normal 120-second current-call guard.

| Long-case property | Frozen, one call | Final, median of three |
| --- | ---: | ---: |
| Wall time, seconds | 132.813579 | 23.661530 |
| Allocated bytes | 38,946,172,744 | 590,121,880 |
| Allocation count | 1,099,996,147 | 4,479 |
| GC time, seconds | 4.399253 | 0.005232 |
| Shared expansions | 71,834,699 | 59,097,663 |
| Query-bit expansions | 2,040,526,272 | 2,040,477,504 |
| Sum of per-origin output populations | 75,548,503,335 | 75,548,503,335 |

The final warm call took 23.544596 seconds. Its three measured calls took
23.905520, 23.661530, and 22.665339 seconds. Allocation fell by about 98.5%,
but elapsed time improved by 5.61x because network search remains costly.
The different expansion counts do not indicate different output: all
per-origin values match exactly. The frozen 96-sample values also match
the frozen four-sample values for this seven-day case.

### Final Counts

The following counts are final public-call totals. They are not per-origin
counts. The mode and parameters match the timing table above.

| Walk / radius / budget / samples | Shared expansions | Query-bit expansions | Allocations per call |
| --- | ---: | ---: | ---: |
| 1 / 6 / 3 / 96 | 1,592,027 | 22,606,668 | 3,085 |
| 1 / 10 / 3 / 96 | 4,000,019 | 39,608,588 | 6,460 |
| 1 / 18 / 3 / 96 | 7,161,649 | 54,486,420 | 18,587 |
| 1 / 6 / 168 / 4 | 1,327,656 | 45,603,160 | 2,049 |
| 1 / 6 / 168 / 96 | 31,462,962 | 1,094,475,840 | 3,771 |
| 2 / 6 / 3 / 96 | 2,573,037 | 32,274,763 | 3,628 |
| 2 / 10 / 3 / 96 | 6,478,230 | 57,290,789 | 6,996 |
| 2 / 18 / 3 / 96 | 11,695,630 | 81,263,747 | 19,856 |
| 2 / 6 / 168 / 4 | 2,513,190 | 85,019,896 | 2,481 |
| 2 / 6 / 168 / 96 | 59,097,663 | 2,040,477,504 | 4,479 |

All nine smaller final rows reported zero GC time. The final long row
reported a median of 0.005232 seconds. Previous union and weighted checks
are recorded in the experiment history. The final ten-row matrix checks
the default intersection mode against full frozen arrays, not just sums.

### Memory And Allocation

The final module reused graph vectors, population columns, the res6 rollup,
both walking-index payloads, and prepared population CSR arrays. Assertions
checked these identities. The prepared sidecars still add 14,682,560 bytes
beyond the indexes. There was no new graph pack, source-population
validation, or resolution-7 rollup.

| Final memory probe, 127 origins | Three-hour budget, S96 | Seven-day budget, S96 |
| --- | ---: | ---: |
| Walking limit | 2 hours | 2 hours |
| Starting process RSS, bytes | 4,781,318,144 | 5,038,698,496 |
| Sampled process RSS peak, bytes | 4,781,318,144 | 5,071,065,088 |
| Observed increase, bytes | 0 | 32,366,592 |

RSS was sampled every 50 milliseconds in separate calls. These are process
measurements, not allocation totals or exact continuous peaks. Existing
Julia heap storage can satisfy a query without a higher RSS. A timed long
call reported 5,104,988,160 bytes of RSS after completion, about 4.75 GiB.
Overall process high-water RSS remains 18,880,339,968 bytes, or 17.58 GiB,
from the original network startup. Cumulative allocation is not live memory.

The final real-data kernel check uses 16 origins and four departures in one
64-lane batch. After warmup, two calls took 0.018717 and 0.018271 seconds.
Both allocated **zero bytes and zero objects**. The warmed workspace was
8,056,832 bytes by `Base.summarysize`.

StaticArrays is not required to obtain zero allocation in this kernel.
Concrete workspace fields and typed task results avoid boxed values.
Packed integer keys and bit masks are scalar data. Small immutable tuples
are stored in reusable vectors. These buffers have runtime-dependent
sizes, and reuse avoids allocation after capacity is sufficient. Public
calls still allocate workspaces and outputs. No new dependency was added.

### Final CPU Profiles

The final short profile contains five complete default calls. The final
long profile contains one complete default call. Neither contributes to
the timing medians. Both include flat/tree output, C frames, source-line
attribution, and portable raw data.

The short profile has 3,516 routed stacks, including 120 coordinator waits.
The long profile has 28,126 routed stacks, including 460 waits. The shares
below exclude those waits. They are not exact wall-time partitions.

| Work | Three-hour / S96 share | Seven-day / S96 share |
| --- | ---: | ---: |
| Schedule lookup, line 147 | 34.9% | 15.0% |
| Heap pop, line 116 | 16.1% | 21.6% |
| Pending-table pop, line 120 | 11.2% | 14.2% |
| Enqueue function, inclusive | 23.9% | 26.6% |
| Deadline-mask function, inclusive | 5.1% | 2.4% |
| Radius sorting, line 169 | 1.9% | 2.0% |

Inclusive rows overlap other work. Projection remains below 1% in both
profiles. Network lookup and heap/pending work remain the main targets.
The rejected schedule-cache experiment did not provide a useful gain.

### Final Provenance

The cache-free worktree was loaded into `FinalCPU.Reachability`, alias `F`.
The original frozen `B` module and earlier comparison modules were not
redefined. Source hashes were checked at load and after the final profiles;
the loaded source remained unchanged.

| Final source file | SHA-256 |
| --- | --- |
| `router/src/Reachability.jl` | `67b0c167c34a6ec61fd5c570191eb7f93c2eaf02c0b5d3f2f0627f2130c96015` |
| `router/src/population.jl` | `afb31233b7b016aaa80170a2371bb2fe47e878d8aedd349dfa8a96ce6a61ebbe` |
| `router/src/population_packed.jl` | `5f922a0864b56a764aacff66fcf9b3ef988e86f154b31d2da6965487335e964b` |

The data is the same 205,357,244-row res6 network and Kontur population
used below, with the shuttle enabled. All cases use cell
`861fb4667ffffff`, an 08:00 start, and 15-minute sample steps. The live
server used zero CPU during measured calls. Other OpenCode CPU work
continued, so these remain shared-machine results. No full CPU or GPU
test suite was run during this benchmark.

Artifacts are under `/tmp/opencode/population-3980e5f`:

- `final-default16-all.jls`: all ten final measurements and output arrays.
- `final-default16-L<walk>-k<radius>-b<budget>-s<samples>.jls`: individual results.
- `final-frozen-L2-k6-b168-s96.jls`: the new long frozen output arrays.
- `final-long-frozen-metadata.jls`: its single-call timing and counters.
- `final-memory.jls`: final memory probes.
- `final-worktree-sources.jls`: the exact final source text.
- `final-default16-L2-k6-b3-s96.*`: final short CPU profile artifacts.
- `final-default16-L2-k6-b168-s96.*`: final long CPU profile artifacts.
- `baseline.log`: all individual calls, guards, hashes, and process counters.

### Process Closed

Owned benchmark PID `925226` exited cleanly at **2026-09-09 23:14:46 UTC**
after all ten results, memory records, source snapshots, and profiles were
saved. A process check confirmed it is gone. Its last RSS was approximately
4.58 GiB. The live server, PID `915022`, remains running and was not queried,
signaled, or stopped. The owned resident data no longer consumes memory
while the main agent runs the final test suites.

## Historical Experiments

The remaining sections preserve earlier measurements and decisions. Their
former resident deadlines and command paths are not active. The final
default-16 table above supersedes earlier default and pending-result text.

## Schedule Cache Decision

**Recommendation: remove the opt-in schedule cache. Keep the window layout
default at 16 and the point-query layout at 64.** The cache did not meet
the required consistent 10% improvement. It increased time in all three
actual-walking cases and added about 28.73 MB for eight workers. No core
code was changed by this benchmark.

Fresh worktree source was loaded into `ScheduleCandidate.Reachability`,
with alias `SC`. The earlier `B` and `N` modules remain unchanged. The graph,
population columns, resolution-6 rollup, both walking indexes, and prepared
population CSR arrays are shared. Only module-specific wrappers and the
new schedule-cache storage were created. The source population was not
revalidated, and the graph was not packed again.

### Paired Cache Test

Both variants explicitly use `origin_batch_size=16`. Each variant has one
warm call, followed by three paired rounds. Round order alternates
off/on, on/off, off/on. Full GC and the live-server idle check precede each
call. Every origin and output value matched the frozen reference exactly.

| Walk, hours | Origins | Budget, hours | Samples | Cache off median, seconds | Cache on median, seconds | Effect of cache |
| ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 2 | 127 | 3 | 96 | 0.598538 | 0.628685 | 5.0% slower |
| 2 | 331 | 3 | 96 | 1.524529 | 1.530047 | 0.4% slower |
| 2 | 127 | 168 | 4 | 1.073366 | 1.118280 | 4.2% slower |
| 1 | 127 | 168 | 96 | 9.049084 | 9.014275 | 0.4% faster |

The geometric mean of off/on ratios is 0.97778. Thus, the cache is about
2.3% slower overall. Small differences are not evidence of a useful gain.
The three-hour, 127-origin samples in particular show timing variation:
off is 0.635429 / 0.598538 / 0.589231 seconds, and on is
0.628685 / 0.661858 / 0.567135 seconds. All individual times are in the log.

### Cache Memory

The graph has 117,777 nodes and 544,603 edges. The cache payload is
`4E + 12N = 3,591,736` bytes per worker. `Base.summarysize` reports
3,591,832 bytes per cache. An empty two-hour workspace grows from
6,506,264 to 10,098,096 bytes. Eight caches therefore add 28,734,656 bytes.

| Case: walk / radius / budget / samples | Off allocation, bytes | On allocation, bytes |
| --- | ---: | ---: |
| 2 / 6 / 3 / 96 | 74,094,088 | 102,830,248 |
| 2 / 10 / 3 / 96 | 75,113,464 | 103,849,624 |
| 2 / 6 / 168 / 4 | 293,523,568 | 322,259,728 |
| 1 / 6 / 168 / 96 | 110,964,216 | 139,700,376 |

The API allocation increase is 28,736,160 bytes and 56 objects per call.
Final observed process RSS was 4,808,306,688 bytes, about 4.48 GiB. The
process high-water RSS remains 17.58 GiB from the original graph pack.

### Cache Counters

A separate probe wraps the cache and counts calls without changing the
production files or the normal cache method. It uses the same tile layout
and workspace lifetime as the public route. All four counted matrices
passed exact frozen-output parity. Counter runs are not timing trials.

| Case: walk / radius / budget / samples | Node calls | Interval hits | Node hit rate | Outgoing-edge hit rate |
| --- | ---: | ---: | ---: | ---: |
| 2 / 6 / 3 / 96 | 1,808,715 | 388,993 | 21.5% | 16.9% |
| 2 / 10 / 3 / 96 | 4,677,201 | 898,828 | 19.2% | 14.8% |
| 2 / 6 / 168 / 4 | 1,307,123 | 95,551 | 7.3% | 5.3% |
| 1 / 6 / 168 / 96 | 29,788,671 | 10,885,668 | 36.5% | 30.2% |

| Case | Cold refreshes | Forward refreshes | Day changes | Backward refreshes | Edges that entered binary search |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2 / 6 / 3 / 96 | 61,100 | 1,230,342 | 44,292 | 83,988 | 4,230,585 |
| 2 / 10 / 3 / 96 | 60,873 | 3,218,145 | 172,660 | 326,695 | 12,101,220 |
| 2 / 6 / 168 / 4 | 669,184 | 502,637 | 39,751 | 0 | 3,562,882 |
| 1 / 6 / 168 / 96 | 667,552 | 12,455,770 | 2,029,440 | 3,750,241 | 55,416,060 |

The interval cache covers only a small part of outgoing-edge work in the
actual-walking cases. Most calls still refresh outgoing choices. The cost
of refreshes, interval bounds, extra storage, and cached-edge access offsets
the saved searches. The measured result does not support keeping this
additional code for a possible 2-3% gain.

### Default 16 Status

The production window default is now 16. Single public calls with both
`origin_batch_size` and `schedule_cache` omitted passed exact value and
expansion-count checks for all four cases above. They match explicit
batch 16 with the cache off. The paired medians above still use explicit
batch 16; they are not relabeled no-override medians.

Per the benchmark guard requested by the main agent, the full no-override
default-16 matrix is deferred until the main agent decides to remove the
cache. This includes the remaining original one-hour cases and the final
1,027-origin refresh. Earlier batch-16 measurements below remain historical.
The stored two-hour, 1,027-origin frozen value reference and its single
17.786890-second call remain available. That time is not a three-call median.
The two-hour, seven-day, 96-sample frozen baseline remains unrun.

### Cache Handoff

The loaded source was unchanged through the paired tests and counters.
The live server used no CPU during measured calls and was not queried or
signaled. Other OpenCode CPU work continued; timing variation remains a
caveat. Cache off and on used the same process and alternating trial order.

| Loaded file | SHA-256 |
| --- | --- |
| `router/src/population.jl` | `afb31233b7b016aaa80170a2371bb2fe47e878d8aedd349dfa8a96ce6a61ebbe` |
| `router/src/population_packed.jl` | `c44053098c3d170974877de346f573c0aa1d94469f6575829288473c7c8d7a80` |

Source text and paired data are stored as `schedule-cache-worktree-sources.jls`
and `schedule-cache-pairs.jls` in `/tmp/opencode/population-3980e5f`.
`SCHEDULE_PAIRS` holds the in-process measurements. `SC`, `SG`, `SP`,
`SI1`, and `SI2` identify this candidate and its shared data wrappers.
The cache counters are in `baseline.log` under `CACHE_COUNTERS`.

PID `925226` is idle and retained until **2026-09-09 23:34:49 UTC**.
The next input is `/tmp/opencode/population-3980e5f/command-15.jl`.
`extend_resident(3600)` extends the owned timeout. The earlier frozen and
packed modules, both original indexes, and all result arrays are retained.

## Prior Packed CPU Results

The optimized CPU source was loaded from the worktree into
`Current.Reachability`. The frozen module was not replaced. Graph vectors,
both walking-index payloads, population columns, and the resolution-6 rollup
are shared. The new `Population` object has its own empty prepared-index
cache. It did not validate the 32,957,699 population rows again.

All eight matched batch-8 cases passed exact comparison of every
origin and population value. These are prepared-index timings. Population
sidecars were prepared before query timing. No production, frontend, or GPU
files were changed by this benchmark.

### Original Batch 8

The window default was 8 when this table was measured. It is now 16.
This table is historical and does not describe the new public default.

Each current time is a median of three measured calls after one warm call.
No current timing-table call exceeded 30 seconds. The baseline times below
come from the earlier frozen runs on the same resident data. They are not
interleaved measurements. The seven-day, 96-sample baseline used one
measured call, as recorded in the earlier section. MB is 1,000,000 bytes.

| Walk, hours | Origins | Budget, hours | Samples | Frozen, seconds | Current batch 8, seconds | Speed Ratio | Current allocation, MB |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 127 | 3 | 96 | 4.044133 | 0.510873 | 7.92x | 45.572 |
| 1 | 331 | 3 | 96 | 11.379481 | 1.310942 | 8.68x | 46.838 |
| 1 | 1,027 | 3 | 96 | 19.698348 | 2.291478 | 8.60x | 48.642 |
| 1 | 127 | 168 | 4 | 7.128525 | 0.639495 | 11.15x | 100.291 |
| 1 | 127 | 168 | 96 | 112.251257 | 10.250244 | 10.95x | 117.844 |
| 2 | 127 | 3 | 96 | 4.665312 | 0.983081 | 4.75x | 75.184 |
| 2 | 331 | 3 | 96 | 10.746704 | 2.252912 | 4.77x | 75.863 |
| 2 | 127 | 168 | 4 | 9.587326 | 1.769430 | 5.42x | 297.966 |

All eight current rows reported zero GC time. For the two-hour, 127-origin,
three-hour case, allocation fell from 1,963,624,512 to 75,184,272 bytes.
Allocation count fell from 37,705,373 to 3,728. This is a 96.2% byte reduction.
API calls still allocate workspaces. A zero-allocation kernel does not mean
that the public route call allocates zero bytes.

### Layout Sweep

The following cases use two-hour walking. All times are current medians of
three measured calls after one warm call. Every layout passed exact
per-origin comparison with frozen output. The extra 331-origin, seven-day,
four-sample shape used one frozen call as its value reference. That call
took 29.322591 seconds; it is not a baseline timing median.

| Origins | Budget, hours | Samples | Batch 8, seconds | Batch 16, seconds | Batch 32, seconds | Batch 64, seconds |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 127 | 3 | 96 | 0.983081 | 0.615903 | 0.616586 | 1.010618 |
| 331 | 3 | 96 | 2.252912 | 1.588992 | 1.516884 | 1.531868 |
| 127 | 168 | 4 | 1.769430 | 1.119693 | 1.240588 | 1.881398 |
| 331 | 168 | 4 | 6.310148 | 3.872309 | 4.066870 | 4.055996 |

Batch 16 is 1.42-1.63 times as fast as batch 8 across these four shapes.
Its geometric-mean ratio is 1.55. Batch 32 has a geometric-mean ratio of
1.51, but is slower than 16 in both seven-day cases. Batch 64 has a ratio
of 1.20 and regresses both 127-origin shapes. At 127 origins, batches
8/16/32/64 use 8/8/4/2 workers. At 331 origins, they use 8/8/8/6 workers.

Batch 16 is the recommended window-query default for the tested workload.
It improves every measured shape. Do not select batch 64 only because it
reduces shared expansions: it can reduce useful parallel work too. No
production default was changed by this benchmark. Point queries and other
resolutions were not included in this layout recommendation.

The larger checks used only batches 8 and 16:

| Walk, hours | Origins | Budget, hours | Samples | Batch 8, seconds | Batch 16, seconds | Ratio, 16 versus 8 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1,027 | 3 | 96 | 2.291478 | 1.862848 | 1.23x |
| 2 | 1,027 | 3 | 96 | 4.033729 | 3.359017 | 1.20x |

The two-hour, 1,027-origin check obtained all frozen values in one
17.786890-second reference call. This was a value check, not a new frozen
median. Its current values matched exactly in both layouts.

### Interleaved Control

The control repeated the two-hour, 127-origin, three-hour, 96-sample case
with `mean_intersection`. It used the same process, vectors, and prepared
indexes. After a frozen warm call, it used three rounds with orders
frozen/8/16, 16/8/frozen, and frozen/8/16. Each call had an idle check and
full GC before timing. All values matched exactly.

| Engine / layout | Individual seconds | Median seconds | Ratio versus frozen |
| --- | --- | ---: | ---: |
| Frozen HEAD | 4.585903 / 4.834841 / 4.718945 | 4.718945 | 1.00x |
| Current, batch 8 | 0.999035 / 0.996205 / 1.021627 | 0.999035 | 4.72x |
| Current, batch 16 | 0.604891 / 0.646665 / 0.661410 | 0.646665 | 7.30x |

This control confirms a 1.54x batch-16 improvement over batch 8. It does
not establish the requested 10-100x gain with actual two-hour walking.
Some one-hour-walking cases exceed 10x, but the actual-walking control is
7.30x. A 100x improvement was not measured.

### Output Checks

The timing matrix uses `mean_intersection`. Separate full-array checks use
`min_union` and `reachable_union` at 127 origins with two-hour walking,
for three hours/96 samples and seven days/four samples. Frozen calls supply
the reference arrays. Union and intersection require exact equality.
Weighted results use `rtol=1e-12` and `atol=1e-6`.

Batches 8, 16, 32, and 64 passed both families and both budgets. The largest
observed weighted error was 2.98024e-7 people. All seven-day weighted checks
were exact. The public call with `origin_batch_size`
omitted also matched the frozen actual-walking output exactly.

### Memory And Types

The two sidecars were built once before queries. Repeated preparation
returned the same cached objects. The cache has two entries and shares the
original resolution-6 population map; no resolution-7 map was built.

| Property | One-hour index | Two-hour index |
| --- | ---: | ---: |
| Population preparation, seconds | 0.014352 | 0.032744 |
| Preparation allocation, bytes | 17,507,232 | 74,813,824 |
| Prepared population object, bytes | 4,520,364 | 14,036,772 |
| Added bytes beyond the index payload | 3,067,116 | 11,615,444 |
| Aligned destination count | 121,964 | 229,925 |
| Positive-population walking CSR entries | 14,871 | 816,844 |
| Empty workspace, bytes | 4,778,888 | 6,506,264 |
| Eight empty workspaces, estimated bytes | 38,231,104 | 52,050,112 |

The combined added sidecar size is 14,682,560 bytes. A real two-hour
workspace, after a complete eight-origin/96-sample tile, measured
8,369,448 bytes with `Base.summarysize`; eight similar workspaces would
use about 66,955,584 bytes. This is a workload-specific estimate. The
seven-day calls allocate larger pending/event storage.

The real tile passed exact parity with the first eight frozen origin
values. Two repeated, warmed 64-lane calls to `_population_sample_packed!`
took 0.028762 and 0.026531 seconds. Both allocated **zero bytes and zero
objects**. This is real-data kernel evidence, not only a synthetic test.
It does not cover every possible workspace growth pattern.

A separate RSS probe repeated the 331-origin, seven-day, four-sample,
two-hour-walking call with batch 8. It passed exact parity. Sampling every
50 milliseconds observed a peak of 4,823,244,800 bytes, about 4.49 GiB.
The starting RSS was 4,651,732,992 bytes; the observed increase was
171,511,808 bytes, about 163.6 MiB. This is a sampled process peak, not an
exact per-query allocation size. This probe is not a timing-table trial.

### Remaining CPU Cost

The final profiles use batch 16 and two-hour walking. The three-hour
profile contains five complete 127-origin/96-sample calls. The seven-day
profile contains three complete 127-origin/four-sample calls. Neither
profile contributes to the timing medians. Both have flat and tree output
with C frames and portable raw data.

The three-hour profile has 3,589 routed stacks, including 95 coordinator
waits. The seven-day profile has 4,292 routed stacks, including 263 waits.
The following shares exclude those waits, leaving 3,494 and 4,029 stacks.
They are sampled CPU attribution, not exact wall-time partitions.

| Work | Three-hour share | Seven-day share |
| --- | ---: | ---: |
| Schedule lookup, line 146 | 33.9% | 13.8% |
| Heap pop, line 116 | 16.2% | 23.4% |
| Pending-table pop, line 120 | 11.7% | 14.5% |
| Enqueue function, inclusive | 24.5% | 25.4% |
| Deadline-mask function, inclusive | 5.1% | 2.8% |
| Radius sorting, line 168 | 1.8% | 2.4% |
| Population credit function, inclusive | 0.8% | 1.5% |

The inclusive rows overlap other work; do not add every row. Projection is
now below 1% of the three-hour non-wait routed samples. The earlier boxed
projection loop is no longer a large cost. No boxing or generic-dispatch
function appears above the five-sample reporting threshold in these
profiles. The real warmed kernel also allocates zero bytes.

Further work should target schedule/profile lookup and heap/pending-event
work. Geometry credit and radius sorting are no longer the main three-hour
cost. Test any node-level reuse or event-pruning change against the frozen
arrays. The current evidence does not justify another large geometry
rewrite solely for speed.

### Source And Handoff

The loaded worktree source remained unchanged through the completed matrix
and final profiles. Its source text is retained in
`/tmp/opencode/population-3980e5f/current-worktree-sources.jls`.

| Loaded current file | SHA-256 |
| --- | --- |
| `router/src/Reachability.jl` | `67b0c167c34a6ec61fd5c570191eb7f93c2eaf02c0b5d3f2f0627f2130c96015` |
| `router/src/population.jl` | `854c6eb42cca8a2ef1eb9bb2415716393db06541f07343e686fd8194964d075d` |
| `router/src/population_packed.jl` | `3a197de2d923b13af7cd3d5707721ced1cec26972d22d1adc6aebc384a6fd257` |

`OPT_RUNS[(walk_hours, radius, budget_hours, samples, batch, mode)]` contains
the current measurements. `RESULTS` and `WALK_RESULTS` remain the frozen
one-hour and two-hour references. `EXTRA_REFERENCES` contains the additional
shapes and population families. Current profile files have prefixes
`current-L2-k6-b3-s96-batch16` and `current-L2-k6-b168-s4-batch16` in the
snapshot directory. The log remains `baseline.log` and includes all phases.

The live server used zero CPU during the completed measured calls. Other
OpenCode CPU work continued, including during the interleaved control.
These are shared-machine results, not isolated hardware limits. Process
high-water RSS remains 17.58 GiB from the original graph startup; it is
not a new current-query peak.

This phase retained PID `925226` until 22:44:02 local time. Cache Handoff
above gives the later deadline and next command path.
`extend_resident(3600)` resets the loop deadline and the owned self-timeout.
The live server was not queried, stopped, or signaled.

Current aliases are `N = Current.Reachability`, `CG`, `CP`, `CI1`, `CI2`,
`PP1`, and `PP2`. Frozen aliases `B`, `graph`, `population`, `I1`, and `I2`
remain available. Rebuild candidate wrappers from the original frozen
objects if module types change. The underlying vectors can still be shared.

For a follow-up input blob, create `core-node-probe.jl` in the snapshot
directory first. Then submit the next numbered command file with:

```julia
Base.invokelatest() do
    extend_resident(3600)
    Base.include(Main, joinpath(ARTIFACTS, "core-node-probe.jl"))
end
```

The blob can create a fresh candidate module with
`Base.include(candidate, joinpath(ROOT, "router/src/Reachability.jl"))`.
Apply any temporary method patch only inside that candidate's `Reachability`
module. Do not redefine `B` or `N`. Use `borrow(R, graph)` and
`borrow(R, I2)` for a candidate routing module `R`. Construct its population
object with the existing `h3`, `weights`, and `rollups`, a new
`IdDict{R.WalkingIndex,R.PreparedPopulation}()`, and the existing lock.
Prepare its population sidecar before timing. This does not reload the
network or revalidate the source population rows.

`compare_population_case(f, expected, label; exact=true)` validates every
origin and returns timing data. Use `WALK_RESULTS[(6,3,96)]` for the
two-hour, 127-origin, three-hour case. A blob can print one summary with
`logline`; output goes to `baseline.log`. Use `Base.invokelatest` when
calling functions or methods that the blob has just defined.

## Two-Hour Walking

This follow-up tests actual walking. It uses the same resident graph and
population map as the first phase. The walking limit is two hours, or ten
kilometres between cell centres. The frozen module is still
`Main.Frozen.Reachability`, from commit
`3980e5f00c5cdcc3a423b7009299fb90f377fd55`. Its population source hash was
checked again before the calls. Production edits by other agents do not
change this baseline. This benchmark did not edit production or GPU files.

All other query parameters are unchanged: Paris cell `861fb4667ffffff`,
08:00 start, 15-minute sample steps, and `mean_intersection` mode.

| Origins | Radius | Budget, hours | Samples | Median, seconds | Allocated bytes | Allocations | Median GC, seconds |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 127 | 6 | 3 | 96 | 4.665312 | 1,963,624,512 | 37,705,373 | 0.101226 |
| 331 | 10 | 3 | 96 | 10.746704 | 4,370,966,800 | 81,403,931 | 0.225308 |
| 127 | 6 | 168 | 4 | 9.587326 | 2,841,469,640 | 89,081,603 | 0.208569 |

Each completed row has one warm call and three measured calls. Warm times
were 4.439608, 10.187590, and 10.388427 seconds. Measured times were
4.558376 / 4.693013 / 4.665312; 9.896395 / 10.746704 / 10.802019; and
10.916343 / 9.587326 / 9.409774 seconds. The 331-origin case met the optional
30-second estimate guard. No measured call exceeded 30 seconds.

The two-hour-walking, 168-hour-budget, 127-origin, 96-sample case was **not
run**. Its estimate was `9.587326 * 24 = 230.096` seconds. The call guard
was 120 seconds, with a 90-second estimate threshold. No large case was
repeated, and the 1,027-origin matrix was not repeated in this phase.
The earlier 112.251-second result uses one-hour walking. Do not use it as
the baseline time for this two-hour-walking case.

| Case: radius / budget / samples | Shared expansions | Query-bit expansions | Sum of output populations |
| --- | ---: | ---: | ---: |
| 6 / 3 / 96 | 5,200,369 | 47,371,471 | 1,346,381,983 |
| 10 / 3 / 96 | 11,639,308 | 83,946,386 | 1,667,654,321 |
| 6 / 168 / 4 | 4,553,126 | 85,021,928 | 75,548,503,335 |

These output sums differ from the one-hour-walking phase. They add the
separate origin results and are not unique population totals. Exact arrays
are in `WALK_RESULTS[(radius, budget_hours, samples)]` and in files named
`baseline-L2-k<R>-b<B>-s<S>.jls` under the snapshot directory.

### Walking Index

`I1` retains the original one-hour index. `I2` is the new two-hour index.
Preparation used the frozen `prepare_walking(I1; max_walk_ms=7_200_000)`.
It took 2.117061 seconds and allocated 347,298,256 bytes. It did not reload
or pack the graph. Assertions checked shared cell, centre, and spatial-bin
arrays. Only the resolution-6 population map remains present.

| Index property | Result |
| --- | ---: |
| Graph vertices | 117,777 |
| Geographic adjacency entries | 847,513 |
| Geographic degree: minimum / median / mean / maximum | 6 / 6 / 7.196 / 15 |
| Geographic degree: 95th percentile / zero-degree vertices | 12 / 0 |
| Graph walking adjacency entries | 593,024 |
| Graph walking degree: median / mean / maximum | 5 / 5.035 / 14 |
| Graph vertices with no graph walking neighbor | 4,562 |
| Output cells / nonnetwork output cells | 229,925 / 112,148 |
| Radius-6 origins on the graph | 127 of 127 |
| Geographic walking degree at every radius-6 origin | 6 |
| `I1` / `I2` object bytes | 30,681,284 / 73,168,704 |
| Combined index bytes, with shared arrays counted once | 85,067,564 |

### Walking Counters

Separate counted copies used the same graph, walking vectors, and population
map. Both full-matrix counter calls passed exact output-array parity with
the frozen baseline. Counts below are totals across 192 jobs for the first
column and 16 jobs for the second column.

| Counter, 127 origins and two-hour walking | 3-hour budget, 96 samples | 168-hour budget, 4 samples |
| --- | ---: | ---: |
| Walking-state expansions | 2,459,435 | 2,193,879 |
| Full / clipped walking ranges | 700,283 / 1,759,152 | 2,193,879 / 0 |
| Geographic adjacency entries inspected | 16,016,506 | 15,885,685 |
| Geographic hops within the clipped limit | 6,446,181 | 15,885,685 |
| Geographic credits with no new bits | 4,466,463 (69.3%) | 12,148,908 (76.5%) |
| New query bits added by geographic credits | 9,645,721 | 62,778,731 |
| Graph walking adjacency entries inspected | 15,404,044 | 12,589,022 |
| All population credits / unchanged credits | 11,646,550 / 8,098,294 | 20,438,811 / 16,549,281 |
| Heap pops / stale pops | 13,266,468 / 8,066,099 (60.8%) | 16,257,411 / 11,704,285 (72.0%) |
| Deadline calls / calls before the first cutoff | 37,983,443 / 21,877,384 | 53,317,695 / 53,317,695 |

The first counter column proves that walking changes reachability. It also
shows overlap between geographic credits. The first phase's zero-hop
finding must not be used to describe this workload.

### Walking Profile

The CPU profile took 5.280927 seconds, separate from the timing trials.
It contains 3,602 routed stacks, including 619 coordinator-wait stacks.
The following shares use the remaining 2,983 stacks. GC stops within routing
remain included. These shares are not a wall-time partition.

| Work | Non-wait routed sample share |
| --- | ---: |
| Enqueue, pending/settled operations, and heap pop | 43.4% |
| Mask projection after task fetch | 22.3% |
| Schedule lookup | 13.7% |
| Walking range lookup and entry access at lines 118/122 | 7.6% |
| Population credit function, inclusive | 5.7% |
| Deadline-mask function, inclusive | 1.8% |

The inclusive function rows can overlap other rows. Type-unstable reduction
and queue work remain major targets with actual walking enabled. Geographic
overlap is large by count, but population credit is not the largest sampled
CPU cost. No speed increase is claimed from these counters alone.

The allocation profile used a 0.001 sample rate and took 6.446307 seconds.
It sampled 2,501 `WalkingRange{UInt64}`, 2,442 `WalkingRange{Int32}`,
2,374 `PackedWalking{UInt64}`, and 2,410 `PackedWalking{Int32}` allocations.
This estimates about 9.7 million walking-wrapper allocations per full call.
It also sampled 18,115 boxed `UInt64` values and 5,093 `Tuple{Int64,Int64}`
objects. Only three backing buffers were sampled; their byte estimate is
not reliable. Full-call allocation is given in the timing table.

### Geometry Groups

A smaller diagnostic used the first eight sorted radius-6 origins and eight
departures from 08:00 through 09:45. This is one exact 64-lane baseline tile,
not a whole-matrix count. Both group probes passed exact parity for coverage,
shared expansions, and query-bit expansions.

| Tile geometry property | Count |
| --- | ---: |
| Walking expansion events / distinct source cells | 18,730 / 4,792 |
| Distinct `(source, clipped radius)` groups | 12,798 |
| Distinct `(source, mask, clipped radius)` groups | 18,730 |
| Distinct radius values | 744 |
| Distinct `(source, admitted neighbor set)` groups | 5,992 |
| Full / clipped range events | 5,071 / 13,659 |
| Events with no neighbor inside the clipped radius | 10,968 (58.6%) |
| Accepted geographic hops / unchanged credits | 45,180 / 31,341 |
| Distinct geographic target cells | 2,114 |

For a fixed source, radius increases produce nested neighbor sets. Thus,
the admitted neighbor count identifies its set. The 12,798 raw radius
groups collapse to 5,992 neighbor sets. This supports reuse of geometry
and exact rejection of empty clipped ranges. It does not permit arbitrary
mask merging: each lane still needs its own deadline check. The raw radius
group frequencies are in `baseline-L2-geometry-radius-groups.jls`.

### Follow-Up State

The two-hour phase and all diagnostics are complete. No query remains
active. Every warm and measured matrix call in this phase reported zero
live-server CPU use and zero major faults. Other OpenCode CPU use remains
a caveat. The log records those CPU-time changes, including the two-second
idle check, so they are not exact in-query CPU measurements. Do not compare
one-hour and two-hour wall times as if machine load were identical.

The process uses about 4.06 GiB RSS after collection. Peak RSS remains
17.58 GiB from the original startup. There was no second graph load and no
additional population rollup. Artifacts use the prefix
`/tmp/opencode/population-3980e5f/baseline-L2-k6-b3-s96`, with CPU and
allocation flat/tree reports, source attribution, and portable CPU data.

This phase originally extended PID `925226` to 21:34:15 local time. The
Cache Handoff above gives the later deadline and next command path.
The old timeout parent was replaced after PID and command validation. The
self-timeout and command-loop deadline affect only this benchmark.
The user server was not stopped, signaled, or queried.

`extend_resident(3600)` extends both the loop deadline and self-timeout.
`I1` and `I2` hold the one-hour and two-hour indexes. `walking` still aliases
the one-hour index. `walk_query(M, g, p, index, radius, budget, samples)`
uses the two-hour limit and `mean_intersection`. Call it with a current
module and compare its arrays with `WALK_RESULTS`. The graph vectors and
population map can be shared. If current wrapper fields change, rebuild
those wrappers or the index as required; do not repack the network.

## Scope

The sections below describe the first, one-hour-walking phase. Its starting
worktree was clean. This benchmark kept production files read-only.
The baseline commit is `3980e5f00c5cdcc3a423b7009299fb90f377fd55`.
The source snapshot is `/tmp/opencode/population-3980e5f/router/src`.
The benchmark does not change, stop, or send requests to the live server.

The input is `data/everything_res6.arrow`, with 205,357,244 source rows.
The population input is `data/kontur_h3.arrow`. The process builds only the
resolution-6 population map. It loads the graph once, adds the shuttle, and
prepares walking before query measurement.

These cases use Paris cell `861fb4667ffffff`, an 08:00 start, 15-minute sample
steps, and a one-hour maximum walking leg. The primary mode is
`mean_intersection`. Radii 6, 10, and 18 contain 127, 331, and 1,027 origins.
The three-hour budget cases use 96 samples. The 168-hour budget starts with
127 origins and four samples. The 96-sample case runs only if its estimate
fits the benchmark guard. This guard does not limit production queries.

Each case has one warm call. If calls take less than 30 seconds, the result
uses the median of three measured calls. Otherwise, it uses one measured
call. Full garbage collection runs before timing. CPU profiling is a separate
call after measurement. The benchmark writes flat and tree profiles, raw
profile data, and source-line attribution to the snapshot directory.

Before the run, `pidstat` measured zero CPU use for live server PID `915022`
over three two-second intervals. The script checks this PID before each call
and records its CPU-time change during each call. The machine has eight
logical CPUs and approximately 29 GiB of available memory. Existing swap use
was 6.5 GiB. Results must account for other work on this machine.

## Baseline Results

All five baseline cases and the diagnostic profiles are complete.
Production optimization can start. No production files were changed.
The following values are baseline measurements, not an optimized comparison.
GB means 1,000,000,000 bytes. GiB means 1,073,741,824 bytes.

| Origins | Radius | Budget, hours | Samples | Median, seconds | Allocation, GB | Allocations per call | Median GC, seconds |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 127 | 6 | 3 | 96 | 4.044133 | 1.509697 | 34,576,743 | 0.109916 |
| 331 | 10 | 3 | 96 | 11.379481 | 3.271972 | 74,690,735 | 0.294162 |
| 1,027 | 18 | 3 | 96 | 19.698348 | 5.659379 | 129,399,879 | 0.511312 |
| 127 | 6 | 168 | 4 | 7.128525 | 2.218813 | 62,785,481 | 0.166317 |
| 127 | 6 | 168 | 96 | 112.251257 | 28.290328 | 784,041,905 | 3.784655 |

The first four rows have one warm call and three measured calls. The measured
times are 4.068280 / 4.044133 / 3.916794; 11.379481 / 12.355409 / 8.962989;
15.986388 / 22.125722 / 19.698348; and 7.128525 / 6.987557 / 7.608716 seconds.
Warm calls took 6.145696, 12.487132, 15.781205, and 8.127547 seconds.
The final row has one 107.785569-second warm call and one measured call.
Its reported time is not a median of three. No requested case remains unrun.

| Radius / budget / samples | Jobs | Shared expansions | Query-bit expansions | Sum of output populations |
| --- | ---: | ---: | ---: | ---: |
| 6 / 3 / 96 | 192 | 4,852,216 | 45,213,336 | 1,035,259,880 |
| 10 / 3 / 96 | 504 | 10,590,534 | 79,217,368 | 1,199,328,062 |
| 18 / 3 / 96 | 1,548 | 17,411,838 | 108,990,312 | 1,201,329,991 |
| 6 / 168 / 4 | 16 | 4,495,171 | 84,836,508 | 67,569,838,317 |
| 6 / 168 / 96 | 192 | 69,308,079 | 2,036,076,192 | 67,569,838,317 |

All rows used eight workers. Expansion counts apply to the whole matrix.
The output sum adds separate origin results. It is not a unique population
total. Exact per-origin arrays are stored in the baseline `.jls` files.
Assertions checked origin count, sort order, and finite values. This phase
did not repeat the independent single-origin reference checks from the
earlier population benchmark. Later optimized calls must match these frozen
arrays with `rtol=1e-12` and `atol=1e-6`.

## Profile Findings

The 127-origin, three-hour, 96-sample CPU profile took 5.225925 seconds.
It is separate from the timing table. It used a two-millisecond sample delay.
The flat and tree reports include C frames. Source-line attribution found
1,901 stacks with a population source frame. Of these, 321 were in the
coordinator's task wait. The table below uses the remaining 1,580 stacks.
It includes GC stops within routing. It is not a wall-time partition.

| Work | Source in frozen `router/src/population.jl` | Stacks | Share |
| --- | --- | ---: | ---: |
| Mask projection after task fetch | 217-225 | 423 | 26.8% |
| Enqueue, including pending checks and writes | 87-92 | 301 | 19.1% |
| Schedule lookup | 131 | 214 | 13.5% |
| Heap pop | 106 | 166 | 10.5% |
| Pending pop and settled read/write | 108-111 | 156 | 9.9% |
| Prepared walking range lookup | 118 and 122 | 156 | 9.9% |
| Population credit | 94-98 | 61 | 3.9% |
| Graph node lookup | 128 | 43 | 2.7% |

The main findings are:

1. The reduction loop has a type-inference problem. `fetch(task)` returns
   untyped data at line 203. The projection loop then uses dynamic calls for
   indexing, bit operations, comparisons, and field access. The tree report
   shows `ijl_apply_generic`, `ijl_get_nth_field_checked`, and
   `ijl_box_uint64` below lines 217-225. This is a priority before a new
   routing algorithm. The full call makes 34.6 million allocations.
2. Queue and state operations are the next large cost. Enqueue, heap pop,
   pending pop, and settled access account for about 39.4% of the listed
   non-wait stacks. The current path hashes `(time, cell, walk)` events and
   `(cell, walk)` settled states. Test compact node IDs, fewer pending-table
   lookups, and reusable typed state storage.
3. Schedule lookup accounts for 13.5%. The matrix has 192 jobs and 4.85
   million shared expansions. Sharing reduces the query-bit expansion count
   by 89.3%, but it still repeats network work between tiles and time blocks.
   A large speed increase will require less repeated network work, not only
   faster population addition.
4. The sampled `deadline_mask` function appears in 22 routed stacks, or
   1.4% of the non-wait denominator. Population credit accounts for 3.9%.
   These counts do not support either operation as the main three-hour
   bottleneck. Exact counters below measure redundant updates and the share
   of deadline calls that can retain all lanes.
5. GC time is about 2.4-3.4% of measured wall time. Allocation still matters:
   small-object creation and dynamic dispatch consume time outside GC.
   Prepared walking lookups also show allocation frames. The allocation
   profile confirms allocation of both walking ranges and packed wrappers.

Do not interpret these sample shares as exact speed limits. The coordinator
runs projection in sequence. Faster projection can also let workers start
the next wave sooner. CPU contention also changed thread availability.

The 168-hour, four-sample profile strengthens the projection finding. It
contains 2,771 routed stacks, with 281 in the coordinator wait. Projection
accounts for 1,299 of the remaining 2,490 stacks, or 52.2%. Final population
aggregation at lines 145-146 accounts for another 51 stacks, or 2.0%.
Heap pop accounts for 8.5%, and schedule lookup for 4.3%. This profile took
9.389947 seconds and is not a timing-table trial.

## Exact Counters

Counters were added only to a separate source copy under
`/tmp/opencode/population-3980e5f/counted`. Both counted cases used the same
graph and walking vectors and the same population map. Their full output
arrays matched the frozen baseline exactly. Counter calls include extra
work and compilation. Do not compare their times with production times.

| Counter, 127 origins | 3-hour budget, 96 samples | 168-hour budget, 4 samples |
| --- | ---: | ---: |
| Enqueue attempts | 24,665,282 | 24,406,171 |
| Enqueue attempts with no remaining bits | 10,211,264 | 11,585,670 |
| New pending events / heap pops | 9,160,916 | 10,176,087 |
| Existing pending events updated | 5,293,102 | 2,644,414 |
| Pending updates with no new bits | 1,173,442 | 438,268 |
| Stale heap pops | 4,308,700 (47.0%) | 5,680,916 (55.8%) |
| Network-state expansions | 2,426,108 | 2,248,145 |
| Walking-state expansions | 2,426,108 | 2,247,026 |
| Geographic walking hops visited | 0 | 225,125 |
| Graph walking hops visited | 0 | 142,117 |
| Population credit attempts | 4,852,216 | 4,720,296 |
| Credits with no new bits | 2,426,108 | 2,395,572 |
| Credits with no population weight | 0 | 18,240 |
| Deadline-mask calls | 24,665,282 | 24,631,296 |
| Deadline calls before the first cutoff | 14,591,750 (59.2%) | 24,631,296 (100%) |
| Schedule lookups | 21,709,854 | 12,131,519 |
| Valid schedule arrivals | 12,320,449 | 12,131,519 |
| Largest heap in one job | 9,140 | 113,542 |
| Largest settled table in one job | 13,036 | 167,001 |

The three-hour case enables walking but traverses no walking hops. The
one-hour limit is five kilometres between resolution-6 cell centres.
Nevertheless, the current code creates a walking state for each transit
arrival. Half of all expansions and half of all credit attempts then add
no walking reach. Test an exact empty-adjacency check before these states
enter the queue. Preserve the off-graph fallback and all nonempty ranges.

The seven-day case does traverse walking hops. Its counters rule out a
general no-walking assumption. All its deadline calls retain all lanes,
but deadline sampling is small compared with projection and queue work.

## Allocation Profile

The three-hour, 127-origin allocation profile used `sample_rate=0.002`.
It took 7.627961 seconds and is not a baseline timing trial. Flat and tree
allocation reports have suffixes `.alloc-flat.txt` and `.alloc-tree.txt`.

| Allocated type | Sampled allocation count | Sampled bytes |
| --- | ---: | ---: |
| `WalkingRange{UInt64}` | 4,831 | 309,184 |
| `WalkingRange{Int32}` | 4,774 | 305,536 |
| `PackedWalking{UInt64}` | 4,702 | 225,696 |
| `PackedWalking{Int32}` | 4,897 | 235,056 |
| `UInt64` boxes | 32,597 | 260,776 |
| `Tuple{Int64,Int64}` | 9,225 | 295,200 |

The four walking wrapper types account for 19,204 sampled allocations.
Their sampling estimate is about 9.6 million allocations per call. They
occur even when the walking ranges are empty. The full profile also shows
boxed values and iterator objects in the untyped reduction loop. Sampled
byte totals are not exact full-call totals. In particular, only 15 backing
buffers were sampled, so buffer-size estimates have high uncertainty.

## Optimization Targets

First, make task results and the reduction loop type-stable. Verify the
allocation decrease and profile again. Remove walking wrapper allocation
and omit only provably empty walking states. Next, reduce heap and dictionary
work, then test reuse across origin tiles and departure samples. Check all
three population families before release. Keep exact deadline and walking
semantics. Do not add a production query cap.

The requested 10x target would require times below 0.404, 1.138, and 1.970
seconds for the three 96-sample rows. The 100x target would require times
below 0.0404, 0.1138, and 0.1970 seconds. These are target values, not
predictions. Local credit or deadline changes alone do not support them.
The 168-hour, 96-sample target is 11.225 seconds for 10x, or 1.123 seconds
for 100x. Projection, empty walking states, and repeated network searches
are the main candidates. No optimized speed increase is claimed here.
Use interleaved frozen/current measurements on the same resident vectors
before claiming a speed increase.

## Startup And Memory

| Stage or object | Result |
| --- | ---: |
| Graph load and pack | 344.154 s; 31,013,585,504 allocated bytes |
| Sort connections | 107.642 s |
| Pack daily profiles | 196.725 s |
| Walking preparation | 1.470 s; 152,142,680 allocated bytes |
| Population load and validation | 2.347 s; 2,139,169,976 allocated bytes |
| Resolution-6 rollup | 1.745 s; 156,465,728 allocated bytes |
| Graph object | 1,782,917,248 bytes |
| Walking index | 30,681,284 bytes |
| Population rollup | 71,303,280 bytes; 2,016,971 cells |
| Population rows and conserved total | 32,957,699 rows; 8,031,924,024 people |
| Packed graph | 117,777 vertices; 544,603 edges; 100,385,985 profiles |
| Peak RSS through these query cases | 18,880,339,968 bytes; 17.58 GiB |

The loader excluded 88,932 invalid-duration rows and added 2,342 shuttle
rows. Only the resolution-6 population map was built. Query RSS was about
3.7-4.1 GiB. Cumulative allocation is not retained memory.

Memory pressure strongly affected startup. Pack used 109.4 CPU seconds but
344.2 wall seconds and incurred 4,808 major faults. System swap use rose
from 6.25 GiB to about 18.15 GiB. The live server's RSS fell as Linux moved
pages to swap. No command changed or stopped that server. The first measured
127-origin query had one major fault; the next two had none. The measured
331-origin cases and all four-sample, 168-hour cases had no major faults.
The 1,027-origin calls had 5, 2, and 0 major faults.
The 168-hour, 96-sample warm and measured calls had 1 and 3 major faults.

The live server used zero CPU seconds during every completed call above.
However, a later six-second `pidstat` sample found OpenCode PIDs `435609`
and `443235` at 103% and 216% CPU. The benchmark used 326% at that time.
The broad timing ranges are a warning: these are shared-machine baselines.
The script's `contaminated=false` field checks only the live server PID.
It does not establish that the whole machine was idle.

## Reproduction And Follow-Up

The run command was:

```bash
nohup timeout --signal=TERM --kill-after=5s 3000s julia --threads=8 --project=router --heap-size-hint=16G experiments/benchmarks/benchmark-population-optimization.jl > /tmp/opencode/population-3980e5f/baseline.log 2>&1 &
```

The snapshot was exported with `git archive` from the baseline commit before
module load. Each source file matched the working file at startup. The log
contains all source hashes, parameters, individual calls, allocation and GC
counts, CPU-time changes, major faults, RSS, and system swap use.

| File | SHA-256 |
| --- | --- |
| `router/src/population.jl` | `6275df6d8b47ac2577854513881a7bf78882e78da89cf25b35faec1b064042b4` |
| `router/src/Reachability.jl` | `fee9170a09690c84b14798cb870796a025bf7d19ed1433ce3434872066bcda5c` |
| `data/everything_res6.arrow` | `ffb1f5ebc8a43dcf3bb387667aa9d925af4c8363ead983c85ce4ecd19a3b550e` |
| `data/kontur_h3.arrow` | `c21eaf6c3eb65563e80f2055347ad13f979014a190f8472817c25a591f427eb3` |

Profile files use the prefix
`/tmp/opencode/population-3980e5f/baseline-k6-b3-s96`.
The suffixes are `.flat.txt`, `.tree.txt`, `.attribution.txt`, and
`.profile.jls`. The raw file contains portable profile data and line metadata.

Benchmark PID `925226` holds the graph, prepared walking indexes, population,
and frozen module `B`. The original 30-minute idle limit and 50-minute outer
timeout were replaced during the two-hour phase. See Follow-Up State above
for the current deadline and command path. `command-1.jl` completed a 168-hour
four-sample CPU profile, and a sampled allocation profile. It used a separate
instrumented source copy under `counted/`. It shares the original graph,
walking vectors, and population map. It checks exact output parity. Its
timings must not be used as baseline timings.

For the optimized phase, load current source into a new module. Use
`borrow(Current.Reachability, graph)`, and do the same for `walking` and
`population`. This builds module-specific wrappers, not another graph pack.
The `run_case` function compares current results with `RESULTS` when its tag
is not `baseline`. Use `Base.invokelatest` when a command defines new methods.

The first run used a cooperative timer around startup. It did not interrupt
the CPU-bound pack at 300 seconds. The harness now moves the measured call
to a worker so the timer can run, and allows 900 seconds for startup. The
query guard remains 180 seconds. The first seven-day estimate used the
12-fold job-count increase. The harness now uses the more conservative
24-fold sample-count increase. These harness corrections do not change the
production snapshot or the completed measurements above.
The corrected timer passed a separate short test with a CPU-bound loop.
It stopped only that test process and printed `TIMEOUT guard-smoke`.
A normal timed-call test also passed. The harness reports one trial if a
later measured call crosses the 30-second threshold.
