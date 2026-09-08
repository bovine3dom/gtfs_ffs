# CPU Startup Measurements

Measured 2026-09-08 against clean starting revision
`873abc2843f76062d630cd6e7166675116ee8eca`, archived before editing. GPU algorithms
and GPU/distance benchmark scripts were not changed. No existing server was stopped.

## Workload And Method

- Intel Xeon E3-1275 v6, 4 physical cores / 8 logical CPUs, 62 GiB RAM; Julia 1.12.7.
- `data/rail_and_friends_res6.arrow`: 1,257,390,570 bytes, 34,921,863 rows,
  chunked Arrow columns with `distance_km`.
- Both versions used `skip_invalid_durations=true, badajoz_shuttle=true`:
  4,464 invalid durations excluded, 2,342 shuttle connections added.
- Identical output: 35,760 nodes and 22,444,069 retained two-day profile entries.
- Fresh Julia processes, tiny fixture warmup, Arrow opened before the timed region,
  `GC.gc()` before `@timed @profile pack_graph(table; ...)`, 10 ms sampling interval.
  Times include real-column compilation, validation, sorting and packing, not Julia
  process launch, Arrow opening, walking preparation or device upload.
- These are individual warm-filesystem runs on a shared machine, not cold-start
  medians. An initial baseline attempt exceeded the 120-second tool timeout and is
  excluded; completed measurements used a longer timeout. Later optimized runs
  ranged from 14.5 to 19.6 seconds, so do not infer thread scaling from packing times.

## Packing Results

| Default-Pool Threads | Baseline Seconds | Final Seconds | Baseline Allocated GB | Final Allocated GB | Baseline Peak RSS GB | Final Peak RSS GB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 4 | 98.530 | 14.463 | 6.523 | 5.641 | 3.416 | 4.103 |
| 8 | 86.545 | 14.856 | 6.523 | 5.664 | 3.426 | 4.048 |

GB above is decimal. Final runs enabled progress: plain stage logs at four threads,
an actual terminal meter at eight. The latter visibly advanced from partial
completion to 100%, rather than reporting only a completed file.

The four-thread baseline profile attributed 4,759 of 9,232 main-task samples to
endpoint sorting and 3,602 to per-edge profile sorting. Chunk-index searches appeared
in 2,550 samples. The changes directly address these costs:

1. Flatten only the randomly accessed sort columns: from/to H3 and optional distance.
   Already-flat vectors are borrowed read-only. Durations/departures remain borrowed,
   and the existing typed shuttle wrapper remains intact.
2. Replace endpoint `unique(vcat(...))` with a union without concatenating both full
   endpoint columns first.
3. Sort each edge's daily profile once, then scan its next-day and current-day copies
   in reverse. Since departures are strictly within one day, this is the same stable
   order and suffix minimum as sorting both copies, including distance ties.

Sorting/profile construction remains serial. No new parallel sorting dependency,
full-profile duplicate output buffers, persistent cache or environment knob is needed.
Flattening requires up to 24 extra bytes per input row with distances (16 without);
this is a deliberate bounded memory tradeoff, not a claim of lower peak memory.
The much larger `everything` datasets were not benchmarked here.

The final eight-thread stage times were: validation/filtering 1.748 s,
materialization 0.796 s, H3 indexing/validation 0.550 s, edge sorting 5.193 s,
grouping/profile packing 5.108 s. Stage totals exclude compilation between stages.

## Walking And Validation

Walking preparation now defaults to `Threads.nthreads(:default)`, bounded by the
number of vertices. An explicit `workers=4` remains available. Logging reports the
actual preparation task count, not a hard-coded cap. Window routing was already
using the full default pool and is unchanged.

On this res6 graph, warmed one-hour preparation took 0.328 s on the baseline's four
workers, 0.284 s on the final four-worker path and 0.285 s with eight workers.
There is no reliable hyperthreading speedup on this small walking workload; removing
the cap is not a promise that eight logical CPUs outperform four physical cores.
Final adjacency packing and output-ID mapping took approximately 1.4 ms and 0.4 ms.

All graph fields were compared across baseline/final four/eight-thread runs:
H3 order, node dictionary, edge endpoints, adjacency/schedule pointers, departure,
arrival, resolution and bitwise distance values. All matched. Geographic, graph and
indexed-output walking arrays, output cell order and ID dictionaries also matched.

Full CPU test suites passed with `--threads=1`, `4` and `8`, including real child-server
HTTP/WebSocket tests for res5/res6/res7. Added tests compare against the original
two-day profile sort with chunked Arrow input, signed-zero distance ties, overtaking,
duplicates, invalid durations and shuttle on/off; verify quiet defaults, worker logs,
error-stage behavior and redirected stage logs. Terminal tests also check exact
threaded meter counts. Stage boundaries flush stderr so file-backed logs are visible
before the server starts accepting requests. GPU execution was not tested.

To inspect current startup stages without starting a listener:

```sh
julia --project=router --threads=8 -e 'include("router/src/Reachability.jl"); using .Reachability; g = pack_graph(ARGS[1]; skip_invalid_durations=true, badajoz_shuttle=true, progress=true); prepare_walking(WalkingIndex(g); progress=true)' data/rail_and_friends_res6.arrow
```
