# Many-Origin GPU Work

Status: the packed CPU population engine is implemented and measured. The GPU
population prototype is implemented. Full-network hardware comparison is pending.

## Target Workload

The near-term CPU target is approximately 10,000 origins per request. Use the
current CPU range engine as the performance baseline. GPU work is separate.

Return one accessible-population value per qualifying origin. Keep timetable,
walking, and population data resident. Perform population reduction on the device
and download origin totals rather than departure-by-destination surfaces.

## Measure the Target Range

- [x] Benchmark CPU disks with 127, 331, and 1,027 origins.
- [x] Measure departure windows with four and 96 samples, including three-hour and seven-day budgets.
- [x] Check all three aggregation families on a benchmark subset. The final timing matrix uses `mean_intersection`.
- [x] Record CPU request time, allocation, sampled process memory, and shared state expansions.
- [x] Measure single departures at 1,027 and 9,919 origins. See the [adaptive CPU experiment report](experiments/benchmarks/population-approved3-results.md).
- [ ] Record full-network GPU request time, origins per second, peak memory, and transfers.

## Tune CPU Batches

- [x] Compare 64-query blocks containing 8 origins x 8 samples, 16 x 4, 32 x 2, and 64 x 1.
- [x] Measure reuse across origins and departures. Select 16 or 64 origins for windows after static-origin removal; retain the 64-origin point default.
- [x] Use compact IDs, packed `UInt64` event keys, and reusable settled and reached arrays.
- [x] Prepare aligned weights and duration-sorted walking CSR entries for positive population.
- [x] Defer final-walk coverage to one scan per reached walk-eligible node in each block.
- [x] Reset touched entries and use typed aggregation within worker-owned origin tiles.
- [x] Use a queue of whole tiles with private worker buffers and joined error handling.
- [x] Retain separate arrival labels for each origin across time blocks. Rebuild coverage for every sample.
- [x] Measure standard resolution-7 routing with 96 samples and the production one-hour walking limit.
- [x] Calculate static walking totals before routing-buffer allocation. Put only origins with possible transit access in tiles.
- [x] Check 9,919-origin Paris and rural requests against the frozen range engine. See the [10,000-origin report](experiments/benchmarks/population-10k-results.md).
- [x] Test incremental population labels, static schedule bounds, and tile sizes. Keep unproven candidates outside production.
- [x] Complete interleaved trials under measured shared load. Record wall time, process CPU time, external CPU load, and pair variation. See the [shared-host appendix](experiments/benchmarks/population-shared-results.md).
- [x] Add an experimental expiry-bucket candidate. Update population labels on valid pops, then apply sample expiry events.
- [x] Test dense-label SIMD, a timestamp-radix queue, and eight-bin bounds against the current adaptive engine. Retain only graph-specific eight-bin bounds. See the [three-experiment report](experiments/benchmarks/population-approved3-results.md).

Each tile uses blocks of up to `floor(64 / origin_count)` samples. One worker
processes all time blocks for its tile. The worker count is the smaller of the
default thread count and origin-tile count. The private `origin_batch_size`
keyword is for benchmarks. The public API permits all valid origin disks.

Windows with two to four samples retain 16 origins, avoiding an arrival matrix
for a query that fits one 64-query block. For longer windows, the default is 64 when the transit-origin count is at least
`128 * Threads.nthreads(:default)`; otherwise it is 16. This gives at least two
full 64-origin tiles per worker. Eight threads require 1,024 transit origins,
not 1,024 total origins. The 64-lane mask limits each block, not the request.
Tile 64 needs more workspace RAM. Request allocation was 2.6-2.9 times the
tile-16 allocation in those eight-thread trials. The rule is not an optimum
for all machines. Expiry buckets, dense-label SIMD, and the timestamp-radix queue
remain experimental. Eight-bin schedule bounds are retained. Each population
object caches them by graph identity, independently of walking geometry.

Workers take the next available tile without a barrier between groups of tiles.
For tiles with multiple time blocks, routing works backward through departure
samples and repairs only strict arrival improvements. One-block tiles retain
the packed origin/sample search. See the [CPU range report](experiments/benchmarks/population-queue-results.md).

Origins without possible initial transit access use one static walking total.
Other off-graph origins use the packed path. Reference routing handles unprepared
indexes and larger effective walking limits. Server startup prepares one hour;
full two-hour optimized walking requires a two-hour prepared index.

## GPU Algorithm

The CPU engine uses a chronological priority heap and origin/sample bitmasks.
Identical timed states share an expansion while retaining query membership.
Walking eligibility and deadlines remain specific to each query.

The prototype relaxes two arrival labels until convergence. Its 32 independent
query lanes use exact `UInt32` times and coverage masks, with `Float32` weights
and reductions. These lanes lose the CPU engine's shared event processing.

- [x] Implement two-state transit and walking propagation with per-query deadlines.
- [x] Deduplicate destination coverage per origin and departure sample on the device.
- [x] Reduce intersection, union, and fraction-weighted totals on the device.
- [x] Reuse fixed-lane arrival buffers and active-tile coverage storage.
- [x] Download scalar origin totals and round flags: `4 * origins + 4 * rounds` bytes.
- [x] Compare small fixtures on `KA.CPU` and Intel P630, including request transfers and synchronization.
- [ ] Preserve shared frontier work with compact state IDs on the GPU.
- [ ] Test wider masks if measurements support them.
- [ ] Compare full networks at 100 to 1,000 origins against the shared CPU engine, including all request costs.

CUDA validation is pending. GPU dependencies remain in the optional experiment;
the server uses CPU routing. The GPU README specifies preparation and memory limits.

## Correctness

Tests cover all six modes, CPU origin comparisons, overlapping walking areas,
different arrival times and deadlines, zero-duration cycles, daily rollover,
partial batches, off-network origins, and fractional population weights.

For population totals, intersection modes count cells reached in every sample.
Union modes count cells reached in any sample. `reachable_union` sums population
weighted by reachable fraction. Each population cell contributes once per origin
and sample. The public response omits origins with a zero total.

## Trip Shortcuts

- [x] Build adjacent and sparse square-root-segment graphs from the same Austria trip set.
- [x] Verify arrival and population parity, retaining intermediate stops and walking access.
- [x] Measure CPU windows and P630 single-departure queries. CPU windows were slower; completed GPU queries improved by 1.34-1.85x.
- [ ] Complete GPU window comparisons on CUDA. The P630 four-sample comparison timed out before both results were available.

See [the Austria experiment](experiments/trip-shortcuts/results.md) for the approved
trip exclusions, generated files, packed sizes, and measurement scope.

## Planet-Scale Follow-Up

- [ ] Add resumable execution over populated origins.
- [ ] Save results in partitions with dataset versions and query settings.
- [ ] Reuse prepared data across partitions without retaining full route surfaces.

Implementation references: [packed CPU engine](router/src/population_packed.jl),
[GPU experiments](experiments/gpu/README.md), and
[population integration](kontur_integration.md).

## Measured Status

The [final default-16 report](experiments/benchmarks/population-optimization-results.md#final-default-16-results)
records the timings. With 127 origins, three hours, and 96 samples, CPU time is
0.360 versus 4.044 seconds at a one-hour walking limit, and 0.645 versus 4.665
seconds with actual two-hour walking. The three-hour profile at the one-hour
walking limit traverses zero walking edges. Actual two-hour cases improve by
5.61-8.71x across the final matrix.

The warmed inner kernel allocates zero bytes with reusable dynamic vectors.
StaticArrays would not address the measured schedule-lookup and heap/pending-event
costs. Projection is below 1% of routed profile samples. The schedule-cache
candidate increased measured walking times and was removed. Next CPU work targets
transit-time lookup and queue processing.

The [current adaptive-engine measurements](experiments/benchmarks/population-approved3-results.md)
replace those earlier timings as the CPU comparison target. They use actual
one-hour resolution-7 walking with 3,737,032 network walking edges. Retained
eight-bin bounds give paired speedups of 1.076x and 1.088x for Paris windows with
1,027 and 9,919 origins. SIMD and timestamp-radix trials did not give a useful
gain on the main three-hour case. All results were measured on a shared host.
