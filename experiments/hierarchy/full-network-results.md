# Historical Regional Index

This report describes the old regional constructor. See
[global-router-results.md](global-router-results.md) for the global engine.
The global engine prepares each model once at startup and accepts new origins
without a regional core rebuild. Startup preprocessing is an accepted cost.
Thus, the old regional preparation cost below is not a reason to block the
global engine. The saved measurements remain unchanged.

## Input And Scope

The input is `data/everything_res8.arrow`, not the Austria or res7 input.
Its size is 7,402,563,314 bytes. Its SHA-256 is
`eb0b3d7e26bed7535522a4ce34aa1f4439053676a3d99356e8887c1b86b06ada`.

The source file has 205,591,459 connections. The production loader skips 88,936
invalid durations and adds 2,342 shuttle connections. The experiment uses
`skip_invalid_durations=true` and `badajoz_shuttle=true`, as required.
It does not remove connections for resource control. All distance values remain
in the packed graph.

| Packed Fine Graph | Count |
|---|---:|
| H3 resolution | 8 |
| Nodes | 900,197 |
| Edges | 2,984,575 |
| Retained two-day profiles | 316,136,664 |
| Distance values | 316,136,664 |
| Geographic walking entries | 105,273,069 |
| Network walking entries | 33,030,524 |

Both query variants use this same graph, fine walking geometry, and population
input. Walking remains one hour at 5 km/h. Population destinations remain at res8.
The candidate uses a res6 core, eight schedule-hint bins, batches of 64 origins,
and descending history. Each query allocates its own workspace. There is no
result cache.

| City | Latitude | Longitude | Res8 Centre |
|---|---:|---:|---|
| Paris | 48.85 | 2.35 | `881fb46625fffff` |
| London | 51.5 | -0.12 | `88194ad14dfffff` |

Each city has 1,141 prepared origins at radius 19. The main queries have 1,027
origins at radius 18. Small and long-distance queries have 127 origins at radius
6. These are fine-grid disks, not res7 disks. The main cases use 96 samples at
15-minute and one-minute intervals. Six-hour and twelve-hour budgets use 180
samples at one-minute intervals. The start time is 08:00 in the input clock.

## Preparation

`Hierarchy.jl` now normalizes fine events directly and packs one coarse parent
at a time. It does not retain a daily-profile copy for every fine edge.
`Boarding.jl` also packs one source or parent at a time. Its query algorithm is
unchanged.

`benchmark-full.jl` prepares walking geometry in two passes. The first pass
counts entries. The second pass fills arrays with exact sizes. It does not
retain all neighbor lists or remove zero-population geographic destinations.
The graph, output, geographic, and distance fields remain available. A test
compares every packed column with production walking preparation.

The walking arrays need 3,069,445,208 bytes before output dictionaries and
population preparation. The driver checks this size against available memory
with a 2 GiB reserve. This is a benchmark preflight check, not a production cap
or a guarantee against memory use by other processes.

| Stage | Completed Run (s) |
|---|---:|
| Full fine load | 93.852 |
| Full fine walking preparation | 148.703 |
| Fine population preparation | 12.930 |
| Paris hierarchy preparation | 49.982 |
| Paris boarding preparation | 66.863 |
| London hierarchy preparation | 47.055 |
| London boarding preparation | 73.001 |

The driver loads the full fine graph once per run. It releases the Paris
candidate before it prepares London. It currently rebuilds the core for each
city. It does not measure a shared-core regional preparation path.
Peak process RSS for the completed run was 30,854,586,368 bytes (28.74 GiB).

The candidate setup costs are 116.844 seconds for Paris and 120.056 seconds for
London. These costs assume that the fine graph, walking geometry, and population
are resident. Warm query timings do not include these costs. The reported cold
estimate adds these measured setup stages to the median warm query. It is not
a single cold-request measurement or a measurement of source-only preparation.

## Results And Decision

See [the generated table](full-res8-20260912-retry/summary.md) for all cases.
The [raw run](full-res8-20260912-retry/) contains preparation timings, source
hashes, interleaved trials, per-origin values, and signed errors. `outliers.csv`
contains the ten largest absolute errors in each case, with origin IDs and fine
population values. WMAE is `sum(abs(candidate - fine)) / sum(fine)`.

For 1,027 origins, the warm speed ratios are about 6.5 to 7.4. However, the
one-minute cases have WMAE of about 19 to 22 percent. The 24-hour intersection
cases have much smaller WMAE: 2.70 percent in Paris and 3.91 percent in London.
These intersection results alone do not describe the dense-window error.

A cold regional request costs about two minutes with this old constructor.
The replacement global constructor removes that query-time core rebuild.
The population errors remain relevant approximation measurements. The user
selects the approximation with `coarseness=N`; zero keeps normal routing.

This run does not validate the time metric, HTTP integration, moved centres,
unprepared origins, 10,000-origin queries, or a res7 control. The small-graph
tests cover all six population modes, but the full-network run measures only
`mean_intersection` and `reachable_union`. It has no full-network per-destination
coverage audit. Do not use this report as approval for those unmeasured features.

## Run History

The first run is preserved in `full-res8-20260912/`. The tool stopped its process
after 600 seconds, during boarding preparation. It was not an out-of-memory
failure. Fine loading, full walking preparation, population preparation, and
the Paris hierarchy had completed. That run has no query measurements.

The retry used a separate background process, PID 1249951, with its own log.
It did not query, stop, or modify the existing server. It did not write to the
input files. Older experiment results remain unchanged. The source HEAD was
`952698e51902c10c3c6b6304d862bec530d96312`. The run metadata records the actual
source-file hashes because the worktree was dirty.

## Verification

The hierarchy and boarding test suites passed 12,929 assertions after the
profile preparation changes. The streaming walking comparison passed 15
assertions. The full benchmark checks result repeatability for each measured
pair and checks the exact origin set for every query.
All 20 full-network cases completed, with 160 query runs including warm-up
pairs. All measured pairs had zero compilation time.

Run from the repository root. Use a new result directory. For a long run, use
a separate process without a tool timeout. Do not reuse the server process.

```sh
julia --project=router -t 8 experiments/hierarchy/test-boarding.jl
julia --project=router -t 8 experiments/hierarchy/test-full-preparation.jl
julia --project=router -t 8 experiments/hierarchy/benchmark-full.jl experiments/hierarchy/NEW_RUN
julia --project=router experiments/hierarchy/summarize-full.jl experiments/hierarchy/NEW_RUN
```
