# Many-Origin GPU Work

The packed CPU population engine and optional GPU prototype are implemented.
Full-network CUDA comparison remains pending. The production server uses CPU
routing. Keep GPU dependencies in the optional experiment.

## CPU Baseline

Use the adaptive CPU range engine with graph-specific eight-bin schedule bounds.
The [CPU research report](experiments/benchmarks/cpu-research-results.md) records
input identity, paired timings, allocation, memory, correctness, and rejected paths.
Its direct routing measurements exclude the handler's population-result cache.

The CPU engine shares timed events with origin/sample masks and repairs range
labels backward through samples. The 64-lane limit applies to a block, not a
request. The report defines tile selection and its memory cost.

CPU trials covered 127 to 9,919 origins, point requests, four- and 96-sample
windows, and a 127-origin seven-day diagnostic. Do not infer seven-day performance
at 10,000 origins. Use actual resolution-7 walking for hardware comparisons.
Preserve all six output selectors, origin exclusion, and per-sample cell deduplication.

## GPU Tasks

The prototype has two-state propagation, exact `UInt32` times, coverage masks,
and device population reduction. It uses 32 independent query lanes and `Float32`
weights. It does not share the CPU engine's timed expansions. Small-fixture
checks passed on `KA.CPU` and Intel P630. See the
[GPU README](experiments/gpu/README.md) for preparation and memory limits.

- [ ] Validate on remote CUDA hardware.
- [ ] Compare full-network requests at 100 to 1,000 origins against the CPU engine.
- [ ] Record full request time, origins per second, peak memory, transfers, and synchronization.
- [ ] Preserve shared frontier work with compact state IDs where measurements support it.
- [ ] Test wider masks only if measurements support them.

Keep timetable, walking, and population data resident. Reduce population on the
device and download scalar origin totals, not departure-by-destination surfaces.
The current prototype downloads `4 * origins + 4 * rounds` bytes for totals and flags.

## Trip Shortcuts

Adjacent and sparse segment graphs use the same approved Austria trip set.
Arrival and population checks preserve intermediate stops and walking access.
CPU windows were slower; completed P630 point queries improved by 1.34-1.85x.

- [ ] Complete GPU window comparisons on CUDA. The P630 four-sample run timed out before both results were available.

See the [trip-shortcut results](experiments/trip-shortcuts/results.md) for input
exclusions, packed sizes, and scope. The separate CPU pattern-scan path is stopped;
its [decision report](experiments/trip-patterns/decision.md) does not reject
trip-based algorithms in general or change the active shortcut work.

## Planet-Scale Tasks

- [ ] Add resumable runs over populated origins.
- [ ] Save partitions with dataset versions and query settings.
- [ ] Reuse prepared data without retaining full route surfaces.

References: [CPU engine](router/src/population_packed.jl),
[population contract](kontur_integration.md), and
[conditional CPU roadmap](experiments/benchmarks/roadmap.md).
