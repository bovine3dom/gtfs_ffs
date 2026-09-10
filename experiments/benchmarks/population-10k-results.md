# CPU Population Requests Near 10,000 Origins

## Status

The production change calculates static walking totals for origins that cannot
reach an outgoing connection on their initial walk. It puts only the remaining
origins in routing tiles. Windows with two to four samples retain 16 origins and
the one-block search. For longer windows, the default is 64 when at least
`128 * Threads.nthreads(:default)` origins remain; otherwise it is 16. The
point default remains 64. There are no new public options, dependencies, graph
fields, GPU changes, or input-data changes.

The original idle gate did not permit timing trials. The original observations
remain below as history, with their original rejection labels. They are not the
basis for performance decisions. The separate shared-host appendix uses new
interleaved trials. It records external load instead of requiring an idle host.
See the [Shared-host paired trials appendix](population-shared-results.md).

The new paired results support adaptive tiles. On eight threads, the threshold
is 1,024 transit origins, which gives at least two full 64-origin tiles per worker.
Paris has 1,027 and 9,412 transit origins; tile 64 gives paired wall ratios of
1.665 and 1.460. Rural France has 813 and 7,987 transit origins. The rule keeps
tile 16 for the smaller case to avoid its observed regression and selects tile
64 for the larger case, with a small 1.029 ratio gain. This criterion uses
available parallel work, not a density label.

The adaptive code was added after the explicit tile-override trials. These are
measurements of equivalent configurations, not separate automatic-selection
timings. Tile 64 needs more workspace RAM. Observed request allocation was
2.6-2.9 times the tile-16 allocation on eight threads. The rule is not an optimum
for every machine. The internal `origin_batch_size` override remains supported.
The 64-lane mask is a block limit, not an origin limit. Expiry and eight-bin
schedule hints remain experimental.

The final eight-thread suite passes 100,275 assertions, with 3,271 focused
assertions on one thread. Earlier full suites passed 100,239 assertions on both
thread counts before the additional short-window checks. The appendix separates
these tests from the original verification and timing results below.

## Baseline and Inputs

The worktree was clean before edits. The frozen baseline is the current range
engine at `7b7036f900e47cd4056f124c324abbeeaf6f748a`, not an older packed or
H3-dictionary implementation. The archive is
`/tmp/opencode/population-10k-baseline.tar`, with SHA-256
`40d5b8baf3ba0441c12dc76aa4ddbf6ba55ca5d7b73669dc3953518444ccd6f4`.
The extracted tree is `/tmp/opencode/population-10k-frozen`.

One Julia 1.12.7 process loads one standard `everything_res7` graph. Baseline and
candidate modules share the graph, walking, population-map, and prepared-weight
vectors. Identity assertions check this sharing. Resolution 6 and 8 files are
read for row counts and hashes only. No second full graph is packed.

| Input | Bytes | Rows | SHA-256 |
| --- | ---: | ---: | --- |
| `everything_res6.arrow` | 7,394,123,842 | 205,357,244 | `ffb1f5ebc8a43dcf3bb387667aa9d925af4c8363ead983c85ce4ecd19a3b550e` |
| `everything_res7.arrow` | 7,397,939,938 | 205,463,043 | `614ac1a6a5b87d35c6e83c057a64161005fb339e80a369f4065709e3d33309d7` |
| `everything_res8.arrow` | 7,402,563,314 | 205,591,459 | `eb0b3d7e26bed7535522a4ce34aa1f4439053676a3d99356e8887c1b86b06ada` |
| `kontur_h3.arrow` | 234,395,250 | 32,957,699 | `c21eaf6c3eb65563e80f2055347ad13f979014a190f8472817c25a591f427eb3` |

Loading uses `skip_invalid_durations=true` and `badajoz_shuttle=true`. It skips
88,936 rows and adds 2,342 shuttle connections. The graph has 379,305 nodes,
1,471,782 edges, and 192,645,963 profile entries. The one-hour walking index has
3,737,032 network walks and 6,038,589 positive-population walks. This workload
uses actual walking edges, not the resolution-6 one-hour case with no such edges.
The loaded population sums to 8,031,924,024 people.

The host is an Intel Xeon E3-1275 v6 with four physical cores and eight logical
CPUs. The benchmark uses eight routing threads and one interactive observer
thread. The live server is not queried, stopped, or changed. CPU checks read
aggregate `/proc/stat` counters and the benchmark's own process counters.
They count user, nice, system, IRQ, soft-IRQ, and steal time; they do not add guest
time twice. The original gate required at most 0.15 aggregate busy cores and
rejected external CPU time above `max(0.5 seconds, 0.15 * elapsed seconds)`.
The shared-host runner does not use this gate or discard trials for external load.

## Requests

All requests depart at 08:00 and use a three-hour journey budget and a one-hour
walking limit, except the stated seven-day diagnostic. Window requests have a
24-hour window, a 15-minute step, and 96 samples. The timed selector is
`mean_intersection`, with origin population included. Point requests use one
sample. The grid API returns 1,027 origins for radius 18 and 9,919 for radius 57.

| Region | Reproducible source | Resolution-7 origin | Origins | Walk-only origins |
| --- | --- | --- | ---: | ---: |
| Paris | Centre of `861fb4667ffffff`, then resolution 7 | `871fb4660ffffff` | 1,027 | 0 |
| Paris | Same source | `871fb4660ffffff` | 9,919 | 507 |
| Rural France | Latitude 46.6, longitude 2.5 degrees | `871f94d80ffffff` | 1,027 | 214 |
| Rural France | Same source | `871f94d80ffffff` | 9,919 | 1,932 |
| Off-network Chad | Latitude 10.0, longitude 20.0 degrees | `876bac79cffffff` | 1,027 | 1,027 |
| Off-network Chad | Same source | `876bac79cffffff` | 9,919 | 9,919 |

Paris is on the graph. Both other source cells are off the graph. Chad has no
graph nodes in either origin disk. Its 9,919-origin result has 5,292 positive
totals and 4,627 zero totals. The sum of its per-origin totals is 15,058,136 people
for both point and window queries. These are not zero-population test disks.

### Rejected Timing Observations

Each row is one call after compilation warmup. The last row has a 168-hour budget;
all other rows have a three-hour budget. Every row passes origin, value, and zero-mask
parity. **Do not use these observations to calculate speedups.**

| Region | Origins | Samples | Baseline observed s | Production observed s | Baseline sampled RSS, bytes | Production sampled RSS, bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Paris | 1,027 | 96 | 17.905 | 17.807 | 8,209,944,576 | 8,251,437,056 |
| Paris | 9,919 | 96 | 45.376 | 43.192 | 8,476,585,984 | 8,407,248,896 |
| Rural France | 1,027 | 96 | 0.335 | 0.270 | 8,412,528,640 | 8,313,249,792 |
| Rural France | 9,919 | 96 | 2.982 | 2.855 | 8,248,786,944 | 8,247,791,616 |
| Rural France | 9,919 | 1 | 0.705 | 0.749 | 7,859,372,032 | 7,626,641,408 |
| Chad | 9,919 | 96 | 1.880 | 0.844 | 9,698,508,800 | 9,310,076,928 |
| Chad | 9,919 | 1 | 0.821 | 0.852 | 9,079,906,304 | 9,088,819,200 |
| Paris, 168 h | 127 | 96 | 38.729 | 38.470 | 8,486,588,416 | 9,075,744,768 |

The larger Paris disk includes origins with much less transit work. A linear
extrapolation from its central 1,027 origins does not describe this disk.
The 1,027- and 9,919-origin seven-day requests were not run. The 127-origin
diagnostic already required about 39 observed seconds per call. No practical
seven-day time is claimed for 10,000 origins.

## Original Candidate Decisions

### Step 1: Static Walking

Retained. Classification uses outgoing-connection topology at the source and
all legal initial network-walk targets. It treats a self-connection as possible
transit because that connection can permit another walk. It does not infer
isolation from an off-graph source or a source with no outgoing edges alone.

On-graph static totals use the prepared positive-population CSR plus the source
weight. Off-graph totals use the existing direct destination IDs, which already
include the source. Exclusion occurs before summation. Only transit-capable
origins fill routing tiles; result slots remain in the original sorted order.
Unprepared indexes and larger effective walking limits keep the reference path.

The Chad requests use zero routing workers and allocate no arrival workspaces.
The 9,919-origin window allocates 42,703,424 bytes, versus 668,091,328 bytes for
the frozen baseline. These are request allocations, not process RSS.

### Step 2: Incremental Population Labels

The experimental candidate retains the original independent A/E routing labels.
Strict improvements update per-origin earliest population-arrival labels. Each
sample scans the retained population labels against its final cutoff, after
route repair. Thus unchanged arrivals can expire and improved arrivals can become
valid again. A missed sample permanently removes a cell from the intersection.
Direct walking coverage is combined through the existing destination masks.

This preliminary prototype does not use expiry buckets. Its full population-arrival matrices
require 569,243,136 bytes for eight workers with 16 origins and 1,111,803 prepared
destinations, before extra request destinations. It remains experimental; there
is no production switch or additional production allocation.

| Case | Existing coverage, observed s | Incremental coverage, observed s | Existing allocated bytes | Incremental allocated bytes |
| --- | ---: | ---: | ---: | ---: |
| Paris, 1,027, 3 h | 18.181 | 20.800 | 668,015,008 | 1,217,718,760 |
| Paris, 127, 168 h | 40.167 | 38.004 | 1,431,249,152 | 1,664,566,344 |

Both comparisons pass parity. Both timing pairs are rejected for background CPU
work. The short case adds about 550 MB of allocation and gives no evidence for
adoption. These observations do not test the expiry-bucket hypothesis. The new
expiry candidate is separate and remains outside production.

### Step 3: Tile and Worker Counts

The tile sweep tests 8, 16, 32, and 64 origins after static-origin removal. It uses
both 1,027- and 9,919-origin Paris and rural windows, plus the 127-origin seven-day
diagnostic. All completed comparisons preserve baseline values. The following
observed seconds are rejected for CPU contamination:

| Case | Tile 8 | Tile 16 | Tile 32 | Tile 64 |
| --- | ---: | ---: | ---: | ---: |
| Paris, 1,027, 3 h | 24.803 | 18.265 | 13.952 | 11.203 |
| Paris, 9,919, 3 h | 55.699 | 43.527 | 35.135 | 30.543 |
| Rural, 1,027, 3 h | 0.255 | 0.282 | 0.376 | 0.352 |
| Rural, 9,919, 3 h | 3.086 | 2.902 | 2.765 | 2.880 |
| Paris, 127, 168 h | 58.917 | 38.391 | 37.339 | 50.938 |

The long diagnostic uses eight, eight, four, and two workers respectively.
Larger tiles reduce the number of independent tasks. Eight A/E arrival matrices
require 194,204,160, 388,408,320, 776,816,640, or 1,553,633,280 bytes respectively.
The largest sampled request RSS in this sweep is 12,019,793,920 bytes.
At that stage, the default remained 16 because there was no accepted timing basis
for a change. The current decision uses the separate paired results above.

The 1,027-origin Paris case also passes with one and four worker tasks in the
same eight-thread process. Observed times are 71.556 seconds with one worker,
23.462 with four, and 18.265 with eight. These times are also rejected. Request
allocation is 137,712,600, 347,367,656, and 667,828,384 bytes respectively.
These are task-count comparisons on four physical cores, not a 20-core test.

### Step 4: Static Schedule Bounds

The experimental hint tables contain per-edge lower bounds for 8 or 24 time bins.
The binary-search upper bound includes the first departure beyond the next bin.
The last bin uses the original profile end. The cutoff test subtracts the day
base before addition, as the production helper does. `Graph` and production
`next_arrival` remain unchanged.

| Bins | Table bytes | Observed build s | Probes in 100,000 sampled lookups | Baseline probes for the same sample |
| ---: | ---: | ---: | ---: | ---: |
| 8 | 47,097,024 | 0.226 | 247,730 | 492,945 |
| 24 | 141,291,072 | 0.349 | 179,376 | 493,688 |

Build times also overlap background work. Probe counts are exact counts for
random real-profile lookups, not counts from a routed request. Each candidate
passes 100,000 random profile comparisons and 1,120 synthetic comparisons for
ties, overtaking, day wrap, cutoff rejection, and near-INF arithmetic.
The hint tables remain experimental. These original observations establish no
end-to-end gain. The appendix records later paired evidence for a limited gain.

The 1,027-origin Paris case passes with both tables. Its observed times are
18.181 seconds without hints, 17.165 with 8 bins, and 16.840 with 24 bins.
The 127-origin seven-day case also passes: observed times are 40.167 seconds
without hints, 38.919 with 8 bins, and 39.013 with 24 bins. All these times are rejected.

## Original Verification and Memory

The production suites pass 100,199 assertions with one thread and with eight
threads, under separate 600-second limits. They cover HTTP, WebSockets, all six
selectors, both exclusion settings, real walking, reference fallback, mutable
cache identity, and joined worker failures. New cases cover static-only requests,
nonconsecutive origin IDs, and a graph source with no outgoing edges that can
walk to a self-connection. The warmed typed range kernel still allocates zero bytes.

Experimental fixture tests pass 460 assertions on one and eight threads across
all six selectors, both exclusion settings, and partial tiles. They include
empty and next-day-only profiles, plus explicit population-label expiry and
reactivation during a 96-sample window. The real Paris and off-network 1,027-origin
cases also pass all six selectors with both exclusion settings. Comparisons
require identical origin arrays and zero masks, with `rtol=1e-12`, `atol=1e-6`
for real population weights.

A timer samples process RSS every 50 ms. Graph loading reaches a sampled
23,443,030,016 bytes. System swap use increases during loading. Request RSS
includes one shared graph, all module namespaces, hint tables when present,
and retained allocator pages. It is not an isolated workspace size. No test
loads a second full graph. GPU tests are not run because GPU code, dependencies,
and graph layout do not change.

## Reproduction and Historical Logs

Use a clean baseline tree at the commit above. Extract its router files into
`POP_SNAPSHOT`. Create an empty temporary directory for `POP_ARTIFACTS`. Do not
reuse a directory that contains old command files. Run from the repository root:

```sh
POP_SNAPSHOT=/tmp/opencode/population-10k-frozen \
POP_ARTIFACTS=/tmp/opencode/population-10k-rerun \
julia --threads=8,1 --project=router experiments/benchmarks/benchmark-population-10k.jl
```

The loader reports the next `command-N.jl` path. For the current shared-host
method, the first command contains:

```julia
include(joinpath(ROOT, "experiments/benchmarks/population-shared-trials.jl"))
exit()
```

The original phases below retain their historical timing labels. For new trials,
include `population-shared-trials.jl` instead. It alternates variant order after
warmup and records every pair under measured shared load. Do not run other test
or benchmark processes during these trials. Leave user applications unchanged.

For correctness only after an idle-gate failure in the first phase, include
`population-10k-validation.jl`. It resumes that phase without another graph load,
rejects all timings, and reduces prototype repetitions to one. Do not use its
output for a performance decision. A final command containing `exit()` stops
only the benchmark process.

The initial failed idle gate is in `/tmp/opencode/population-10k.log`. The resident
load and validation log is `/tmp/opencode/population-10k-run.log`. The frozen
directory retains the staged commands. Source hashes, input hashes, actual
origin counts, external CPU time, allocation, and sampled RSS are in the log.
The test logs are `/tmp/opencode/population-10k-tests-t1.log`,
`/tmp/opencode/population-10k-tests-t8.log`,
`/tmp/opencode/population-10k-candidate-tests-t1.log`, and
`/tmp/opencode/population-10k-candidate-tests-t8.log`.

Staged commands 1 and 2 failed the idle gates. The first prototype phase stopped
at a loop syntax error after its profile checks. The syntax was corrected;
command 6 completed both routing comparisons and all-selector parity without
another graph load. Earlier error output was buffered and appears after the exit
marker in the log. The current harness reports command errors through its logger.

The final idle probe still measured 1.12-2.73 busy cores after routing and tests
stopped. The benchmark then exited normally. The production source at that stage was
`population_packed.jl`, 17,057 bytes, SHA-256
`722c01939c27cba8709397b302da5eea18124c9cce1537afa8b56c27f9dde134`.
Its bytes match the production module used for the comparisons. The log also
records final hashes for the reproduction scripts.
