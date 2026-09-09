# Many-Origin GPU Work

Status: planned. The population engine currently runs on the CPU.

## Target Workload

Optimize for approximately 100 to 1,000 origins per request. Single-origin speed is
a lower priority. Use the shared CPU population engine as the performance baseline.

Return one accessible-population value per qualifying origin. Keep timetable,
walking, and population data resident. Perform population reduction on the device
and download origin totals rather than departure-by-destination surfaces.

## Measure the Target Range

- [ ] Benchmark disks with approximately 127, 331, and 1,027 origins.
- [ ] Compare single departures and representative departure windows, including 96 samples.
- [ ] Include three-hour and seven-day budgets and all three population aggregation families.
- [ ] Record complete request time, origins per second, peak memory, transfers, and shared state expansions.

Existing real-data tests reach 37 origins for points and short windows, and seven
origins for 96-sample windows. They do not establish performance for the target range.

## Tune CPU Batches

- [ ] Compare 64-query blocks containing 8 origins x 8 samples, 16 x 4, 32 x 2, and 64 x 1.
- [ ] Measure the tradeoff between cross-origin and cross-departure reuse.
- [ ] Replace hot-path H3 dictionary keys with compact integer IDs.
- [ ] Align population weights with destination IDs during preparation.

Keep the external query semantics unchanged. Batch dimensions control execution,
not the number of origins a request may contain.

## GPU Algorithm

The CPU engine uses a chronological priority heap and origin/sample bitmasks.
Identical timed states share an expansion while retaining query membership.
Walking eligibility and deadlines remain specific to each query.

The heap makes the first settled arrival the earliest for each query and state.
A GPU cannot assume that property when processing events in a different order.
Choose and test one of these approaches before replacing the CPU heap:

- Time-ordered frontiers, with complete processing of zero-duration transitions at each time.
- Arrival-label relaxation until convergence, with grouping of equivalent timed states where possible.

Preserve shared work rather than merely running more independent GPU searches.
The existing experimental transit kernels omit walking and still perform host
aggregation. They are building blocks, not a population implementation.

- [ ] Implement two-state transit and walking propagation with per-query deadlines.
- [ ] Deduplicate destination coverage per origin and departure sample on the device.
- [ ] Reduce intersection, union, and fraction-weighted population totals on the device.
- [ ] Keep working memory proportional to active batches rather than the whole job.
- [ ] Compare against the shared CPU engine, including all synchronization and transfer costs.

## Correctness

Compare against independent CPU origin calculations. Cover overlapping walking areas,
different arrival times and deadlines, zero-duration cycles, daily rollover, partial
batches, off-network origins, and fractional population weights.

For population totals, intersection modes count cells reached in every sample.
Union modes count cells reached in any sample. `reachable_union` sums population
weighted by reachable fraction. Each population cell contributes once per origin
and sample. The response omits origins with a zero total.

## Planet-Scale Follow-Up

- [ ] Add resumable execution over populated origins.
- [ ] Save results in partitions with dataset versions and query settings.
- [ ] Reuse prepared data across partitions without retaining full route surfaces.

Implementation references: [CPU population engine](router/src/population.jl),
[GPU experiments](experiments/gpu/README.md), and
[population measurements](experiments/benchmarks/population-results.md).
