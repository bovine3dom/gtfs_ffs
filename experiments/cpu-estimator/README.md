# Rough URL CPU Estimate

Import the small, standalone function. It needs no model file, dependencies, or network requests.

```js
import { estimateCpuMs } from './estimate.mjs';
const cpuMs = estimateCpuMs('/reachable?index=881fb46625fffff&departure_h=5&budget_h=100');
```

The input is assumed to be a valid router URL. Full, relative, and query-only URLs work. The function returns a positive CPU-ms estimate. It does not validate requests or reject parameters outside the measurements. There is no detailed estimate object or confidence interval.

Budget defaults to 100 hours when omitted. Walking defaults to one hour, the window to zero, and the step to one minute. The default network is `everything`. Unknown networks use its factor. H3 resolution comes from `index` or the upper UInt32 word. It defaults to 8 if neither is supplied. Population disk size affects only the population metric.

The formula uses network, resolution, population disk size, budget, effective walking allowance, sample count, and `trip_aware`. Trip-aware requests use a rough 5.7 CPU multiplier. Non-population trip-aware windows use linear sample scaling because each sample runs a full search. This trip adjustment is a heuristic, not part of the fit. The budget term grows logarithmically before its fitted exponent is applied. It does not stop at six hours. Other resolutions and large parameter values extrapolate.

A fitted zero exponent removes that term from the generated function. The results report shows the fitted walking exponent.

This is an order-of-magnitude estimate. Errors of hundreds of times are possible. Do not use it as a hard admission or billing limit. See [results.md](results.md) for measured errors and sparse long-budget checks.

## Measurements

The original 384 observations remain in `calibration.csv`, `everything.jsonl`, and `rail_and_friends.jsonl`. They cover both networks and resolutions 5 through 8. The original lower-resolution graphs derive from each resolution-8 source.

The new `*-long.jsonl` files contain sparse 100-hour and 168-hour measurements at resolutions 6 and 8. The 100-hour measurements help fit the formula. The 168-hour Hamburg cases are held out. The previously inspected London cases are now development data, not a new independent test.

Each isolated calibration process runs one query at a time. Measurements include parsing, routing, and uncompressed Arrow serialization. Linux process CPU time includes Julia worker activity and GC. Graphs, compilation, and workspace pools are warm. Population result caching is disabled. HTTP admission, cache bookkeeping, queue time, proxy work, and network transfer are excluded.

## Reproduce

Run the long calibration commands in sequence from the repository root. Each process checks for at least 30 GiB available before loading its source once. It prepares one walking index at a time. The time limit is checked between queries; it does not interrupt active work.

```sh
julia --threads=8 --heap-size-hint=32G --project=router experiments/cpu-estimator/calibrate.jl rail_and_friends 1800 --long
julia --threads=8 --heap-size-hint=32G --project=router experiments/cpu-estimator/calibrate.jl everything 1800 --long
julia --threads=1 --project=router experiments/cpu-estimator/fit.jl
node --test experiments/cpu-estimator/estimate.test.mjs
```

`fit.jl` fits eight coefficients and writes the small `estimate.mjs`, `validation.csv`, and `results.md`. Edit the formula in `fit.jl`, not the generated file. The fit reads the retained JSONL records. The older metadata and logs are offline audit data; the function does not load them.

The small-graph `adapter.test.jl` and `adapter-timings.csv` check the measurement adapter against the in-process handler. No live endpoint is used. No production file, input data, or server configuration is changed.
