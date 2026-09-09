# Population Benchmarks

## Final All-Transport Run

The existing benchmark script completed all eight query shapes against `data/everything_res6.arrow` and the real population file. All point comparisons, all three window-family comparisons, and all untimed single-origin comparisons passed. The script required no changes for the final implementation. No production files or live server processes were changed, and no live queries were sent.

This run used Julia 1.12.7, eight default threads, Paris cell `861fb4667ffffff`, departure at 08:00, a three-hour budget for each sample, and a one-hour maximum walking leg. Radii 0, 1, and 3 contain 1, 7, and 37 origins. The four-sample window covers one hour; the 96-sample window covers 24 hours. Both use 15-minute steps. The 96-sample cases used only radii 0 and 1.

```bash
timeout --signal=TERM --kill-after=10s 600s julia --project=router --threads=8 experiments/benchmarks/benchmark-population.jl /home/olie/projects/gtfs_ffs/data/everything_res6.arrow /home/olie/projects/gtfs_ffs/data/kontur_h3.arrow
```

The log is `/tmp/opencode/population-everything.log`. The population source hash recorded before module load was unchanged at the end of the run:

```text
6275df6d8b47ac2577854513881a7bf78882e78da89cf25b35faec1b064042b4
```

This source includes duplicate validation, zero-weight exclusion, request-shared walking geometry, event-mask deadlines, and weight grouping before fraction multiplication. The earlier rail results below used different source and a different graph. They are not a same-dataset before/after comparison.

## All-Transport Startup

Available memory was approximately 29 GiB before the run. No other large load or benchmark ran in parallel. The script loaded and packed the graph once, enabled the shuttle, prepared walking, then loaded population and built resolution-6 and resolution-7 maps. It did not build a resolution-8 population dictionary or call server warmup.

| Input or stage | Measured result |
| --- | ---: |
| Network file bytes | 7,394,123,842 |
| Source connections | 205,357,244 |
| Invalid durations excluded | 88,932: 3,201 negative and 85,731 overflowing |
| Shuttle connections added | 2,342 |
| Packed vertices / edges / profiles | 117,777 / 544,603 / 100,385,985 |
| Graph load and pack | 87.706 s; 31,000,989,512 allocated bytes |
| Walking preparation | 0.982 s; 113,806,144 allocated bytes |
| Graph / walking index size | 1,782,917,248 / 30,681,284 bytes |
| Peak RSS after pack and preparation | 18,909,904,896 bytes (17.61 GiB) |
| Population file bytes | 234,395,250 |
| Population load and validation | 2.070 s; 2,138,878,856 allocated bytes |
| Population rows / unique valid resolution-8 cells | 32,957,699 / 32,957,699 |
| Population values / total | Positive integer-valued `Float64` / 8,031,924,024 |
| Resolution 6 map | 2,016,971 cells; total 8,031,924,024 |
| Resolution 6 aggregation | 1.482 s; 155,851,184 allocated bytes; 71,303,280 map bytes |
| Resolution 7 map | 9,012,014 cells; total 8,031,924,024 |
| Resolution 7 aggregation | 2.190 s; 567,464,320 allocated bytes; 285,212,784 map bytes |
| Final process peak RSS | 19,559,870,464 bytes (18.22 GiB) |

All known source and map counts and both conserved totals passed assertions. The extra source uniqueness check is outside the load timing. Allocated bytes are cumulative, not retained memory. Object sizes use `Base.summarysize`. The input warning means these results exclude connections outside the supported duration range; they do not cover every raw row.

System swap use rose from approximately 1.0 to 2.6 GiB during the run. Available memory was approximately 30 GiB after the benchmark exited. The shared-machine memory activity is a timing caveat, even though the measured query calls reported zero Julia garbage collection time.

## All-Transport Timings

Each median uses three calls after one warm call. Full garbage collection ran before each measured call, outside timing. The reference uses public `route_walking` for points and `route_window_walking_cached` for windows, with `distance_mode=:straight_line`. Reference origins run in sequence. Its timing includes distance output and all three population sums; the population path calculates one requested sum. Neither path includes HTTP or Arrow serialization.

Window population rows below use fraction-weighted `reachable_union`. Ratio is reference time divided by population time; below 1 means population routing was slower.

| Window | Radius | Origins | Samples | Population ms | Reference ms | Ratio | Population workers | Reference workers per origin |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Point | 0 | 1 | 1 | 10.047 | 8.778 | 0.87 | 1 | 1 |
| Point | 1 | 7 | 1 | 13.843 | 50.693 | 3.66 | 1 | 1 |
| Point | 3 | 37 | 1 | 34.153 | 278.648 | 8.16 | 1 | 1 |
| 1 hour | 0 | 1 | 4 | 23.633 | 9.895 | 0.42 | 1 | 4 |
| 1 hour | 1 | 7 | 4 | 25.592 | 66.413 | 2.60 | 1 | 4 |
| 1 hour | 3 | 37 | 4 | 39.303 | 376.292 | 9.57 | 5 | 4 |
| 24 hours | 0 | 1 | 96 | 251.894 | 42.098 | 0.17 | 2 | 8 |
| 24 hours | 1 | 7 | 96 | 149.016 | 316.671 | 2.13 | 8 | 8 |

All three family timings and allocation per call follow. MB means 1,000,000 bytes. Intersection uses `mean_intersection`, union uses `min_union`, and weighted uses `reachable_union`.

| Window | Radius | Intersection ms / MB | Union ms / MB | Weighted ms / MB | Reference MB |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 hour | 0 | 23.589 / 4.625 | 25.234 / 4.904 | 23.633 / 4.595 | 10.374 |
| 1 hour | 1 | 30.421 / 6.823 | 32.394 / 6.671 | 25.592 / 4.851 | 72.231 |
| 1 hour | 3 | 63.939 / 35.005 | 70.768 / 34.142 | 39.303 / 24.754 | 379.188 |
| 24 hours | 0 | 289.547 / 33.810 | 253.885 / 34.324 | 251.894 / 34.412 | 30.669 |
| 24 hours | 1 | 178.560 / 85.022 | 188.862 / 82.607 | 149.016 / 66.711 | 213.966 |

Point population allocation was 2.986, 3.576, and 12.601 MB for radii 0, 1, and 3. Reference allocation was 3.039, 21.192, and 106.589 MB. The first population point call took 928 ms, including compilation, and is excluded from the median. No warm or measured query call exceeded one second. Neither stop limit was reached.

## All-Transport Counters

These are actual public counters for the full origin/sample matrix, not per-origin averages. Shared expansions count settled shared states. Query expansions count expanded origin/sample bits. All three window families returned the same counters.

| Radius | Samples | Matrix queries | Jobs | Workers | Shared expansions | Query expansions |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 1 | 1 | 1 | 1 | 5,618 | 5,618 |
| 1 | 1 | 7 | 1 | 1 | 6,032 | 39,070 |
| 3 | 1 | 37 | 1 | 1 | 7,428 | 198,172 |
| 0 | 4 | 4 | 1 | 1 | 14,996 | 22,494 |
| 1 | 4 | 28 | 1 | 1 | 16,388 | 153,814 |
| 3 | 4 | 148 | 5 | 5 | 85,828 | 791,836 |
| 0 | 96 | 96 | 2 | 2 | 209,414 | 416,682 |
| 1 | 96 | 672 | 11 | 8 | 264,366 | 2,897,652 |

The 96-sample radius-0 case has two time blocks. Radius 1 has eleven time blocks and uses eight workers. Job counts follow the loaded source; worker counts come from the public result. The log retains actual single-origin counts for each radius, not counts divided by the number of origins. For example, the central origin has 5,618 point expansions, 14,996 shared / 22,494 query expansions for four samples, and 209,414 shared / 416,682 query expansions for 96 samples. The cached reference reports 22,494 and 223,222 arrival-routing expansions for those windows. Counter scopes and costs differ; fewer shared expansions do not imply a proportional speed improvement.

## All-Transport Parity

Intersection checks use raw `reachable_samples == sample_count`; union checks use positive counts. Fraction-weighted checks sum each resolution-6 map weight multiplied by `reachable_samples / sample_count`. Missing weights contribute zero. The checks do not use elapsed-time caps, minimum-time fields, or `_population_sample` as an oracle. All comparisons passed with `rtol=1e-12` and `atol=1e-6`. Small floating-point reduction differences remain in 96-sample weighted totals.

For the central Paris origin:

| Selection | Selected cells | Population |
| --- | ---: | ---: |
| Point at 08:00 | 2,809 | 60,021,751 |
| Four-sample intersection | 1,900 | 50,299,306 |
| Four-sample union | 3,837 | 70,820,598 |
| Four-sample fraction-weighted union | 3,837 | 60,272,096 |
| 96-sample intersection | 130 | 11,301,693 |
| 96-sample union | 7,687 | 85,806,651 |
| 96-sample fraction-weighted union | 7,687 | 42,025,190.770833 |

Radius-1 96-sample results, in sorted H3 order:

| H3 origin | Intersection cells / population | Union cells / population | Fraction-weighted population |
| --- | ---: | ---: | ---: |
| `861fb4297ffffff` | 128 / 11,294,739 | 7,588 / 85,238,319 | 41,457,891.375000 |
| `861fb4647ffffff` | 130 / 11,301,693 | 7,661 / 85,744,030 | 41,717,432.802083 |
| `861fb4667ffffff` | 130 / 11,301,693 | 7,687 / 85,806,651 | 42,025,190.770833 |
| `861fb466fffffff` | 130 / 11,301,693 | 7,605 / 85,319,316 | 41,594,740.260417 |
| `861fb4677ffffff` | 130 / 11,301,693 | 7,724 / 85,802,991 | 41,896,187.125000 |
| `861fb474fffffff` | 129 / 11,258,811 | 7,618 / 85,290,941 | 41,557,593.125000 |
| `861fb475fffffff` | 130 / 11,301,693 | 7,678 / 85,533,035 | 41,796,309.947917 |

Radius-3 point routes reached 2,472 to 2,824 cells per origin. Four-sample intersections had 1,714 to 1,900 cells, and unions had 3,526 to 3,837 cells. All 37 per-origin totals and expansion counts are in the log.

## All-Transport Limits

Population routing was slower for every radius-0 query shape. Its 96-sample weighted case took 5.98 times the cached reference time and allocated 34.412 MB instead of 30.669 MB. Population used two workers; the reference used eight. No stage profile was collected, so these timings do not identify a specific dictionary, heap, or geometry operation as the bottleneck.

Sharing across origins reduced measured time for radii 1 and 3. The 96-sample radius-1 weighted case took 149.016 ms versus 316.671 ms, with 66.711 MB versus 213.966 MB allocated. Its shared expansion count was 90.9% below its query-bit count. This validates the tested workload, not every origin, resolution, walking limit, or travel budget. Radius 3 with 96 samples and seven-day budgets were not run. The reference includes straight-line distance output and three sums, so this is not an isolated routing-kernel comparison. Only three timed calls were measured on a shared machine.

# Earlier Rail Run

The sections below retain the earlier rail-only run and its source hash. All timings, counts, and conclusions in these sections refer to that earlier run, not the final all-transport run above.

The earlier run used this command from the repository root. Running it now uses the current source, not the earlier source:

```bash
timeout --signal=TERM --kill-after=10s 600s julia --project=router --threads=8 experiments/benchmarks/benchmark-population.jl
```

The run used Julia 1.12.7 and eight default threads. The machine had 62 GiB RAM and approximately 29 GiB available before the run. The script loaded the graph once, enabled the Elvas-Badajoz shuttle, prepared walking, and loaded and aggregated population before query timing. Each case had one warm call and three measured calls. Tables give warm medians. Full garbage collection ran before each measured call, outside the timing. All measured calls reported zero garbage collection time.

The script records the population source hash before it includes the routing module. This process loaded source with SHA-256:

```text
e70282af5249edf157fdcb26868c9d210d70348e4c53c97ae2af24b4d07180ff
```

The complete log is `/tmp/opencode/population-daytime.log`. It contains all individual timings, per-origin H3 cells, population totals, cell counts, and expansion counts. The script stops after a completed measured or warm call exceeds 120 seconds. The external timeout limits the whole process to ten minutes, not each call.

## Input Validation

| Input or stage | Measured result |
| --- | ---: |
| Network | `data/rail_and_friends_res6.arrow` |
| Network file bytes / source connections | 1,313,601,330 / 36,482,861 |
| Invalid durations excluded / shuttle connections added | 1,842 / 2,342 |
| Packed vertices / edges / profiles | 36,931 / 139,384 / 23,613,870 |
| Graph load and pack | 17.681 s; 6,054,815,992 allocated bytes |
| Walking preparation | 0.357 s; 35,713,776 allocated bytes |
| Graph / walking index size | 464,590,568 / 8,929,688 bytes |
| Population file | `data/kontur_h3.arrow` |
| Population file bytes | 234,395,250 |
| Population load and validation | 1.150 s; 1,599,220,072 allocated bytes |
| Source rows / unique cells | 32,957,699 / 32,957,699 |
| Source values | `Float64`; all positive integers |
| Source H3 cells | All valid, resolution 8 |
| Source population total | 8,031,924,024 |
| Resolution 6 aggregation | 2,016,971 cells; total 8,031,924,024 |
| Resolution 6 cost | 1.541 s; 149,071,312 allocated bytes; 71,303,280 map bytes |
| Resolution 7 aggregation | 9,012,014 cells; total 8,031,924,024 |
| Resolution 7 cost | 2.320 s; 567,480,112 allocated bytes; 285,212,784 map bytes |
| Process peak RSS | 4,419,784,704 bytes (4.12 GiB) |

Assertions checked the known counts, uniqueness, value constraints, and conserved totals. The separate uniqueness check is outside load timing. Object sizes use `Base.summarysize`. Allocated bytes are cumulative allocation, not retained memory. Peak RSS includes startup and both population maps. Concurrent loader work can change future startup timings; this table describes the source loaded by this process.

## Query Method

Paris coordinates are 48.8566, 2.3522. The script converts degrees to radians for `H3.API.latLngToCell`, as the existing benchmark fixtures do. The resolution-6 origin is `861fb4667ffffff`. Routes start from this cell, without an extra coordinate-to-cell walking leg.

Departure is **08:00**. Every sample has a **three-hour travel budget** and a maximum walking leg of one hour. The one-hour window has four departures, from 08:00 through 08:45. The 24-hour window has 96 departures, from 08:00 through 07:45 the next day. Both use 15-minute steps. This is not a seven-day travel budget.

The point reference uses public `route_walking(...; distance_mode=:straight_line)`. The window reference uses `route_window_walking_cached` with the same distance mode. Origins run in sequence; sample routing within each window can use multiple workers. Reference timing includes routing, straight-line distance output, and all three population sums. Population timing includes one requested sum. Neither timing includes HTTP or Arrow output.

The three population families use the raw reference result, not `_population_sample`:

| Family | Representative mode | Reference rule |
| --- | --- | --- |
| Intersection | `mean_intersection` | Include cells with `reachable_samples == sample_count` |
| Union | `min_union` | Include cells with `reachable_samples > 0` |
| Fraction-weighted union | `reachable_union` | Multiply each weight by `reachable_samples / sample_count` |

Each rule uses the resolution-6 population map. Missing map entries contribute zero. Selection does not use capped or minimum elapsed-time fields. Counts include selected cells even if their population weight is zero or absent. The point has one sample, so the families have the same total; it is timed once. Each window is timed for all three representatives. Other mode names in the same families were not rerun.

## Warm Timings

Times are milliseconds. For window rows, population timing uses fraction-weighted `reachable_union`. Ratio is reference time divided by population time: below 1 means population routing was slower.

| Window | Radius | Origins | Samples | Population ms | Reference ms | Ratio | Population workers | Reference workers per origin |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Point | 0 | 1 | 1 | 3.775 | 3.251 | 0.86 | 1 | 1 |
| Point | 1 | 7 | 1 | 4.128 | 14.496 | 3.51 | 1 | 1 |
| Point | 3 | 37 | 1 | 17.561 | 64.150 | 3.65 | 1 | 1 |
| 1 hour | 0 | 1 | 4 | 7.319 | 4.749 | 0.65 | 1 | 4 |
| 1 hour | 1 | 7 | 4 | 10.805 | 29.565 | 2.74 | 1 | 4 |
| 1 hour | 3 | 37 | 4 | 11.376 | 113.537 | 9.98 | 5 | 4 |
| 24 hours | 0 | 1 | 96 | 60.041 | 13.963 | 0.23 | 2 | 8 |
| 24 hours | 1 | 7 | 96 | 38.863 | 75.626 | 1.95 | 8 | 8 |

All window families and cumulative allocation per call are below. MB means 1,000,000 bytes.

| Window | Radius | Intersection ms / MB | Union ms / MB | Weighted ms / MB | Reference MB |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 hour | 0 | 6.486 / 1.903 | 9.552 / 1.903 | 7.319 / 1.834 | 3.410 |
| 1 hour | 1 | 11.688 / 2.795 | 8.617 / 2.634 | 10.805 / 2.031 | 23.753 |
| 1 hour | 3 | 27.832 / 13.653 | 20.025 / 13.036 | 11.376 / 10.059 | 123.021 |
| 24 hours | 0 | 69.283 / 11.472 | 58.709 / 11.589 | 60.041 / 11.645 | 10.071 |
| 24 hours | 1 | 54.397 / 29.895 | 53.156 / 28.772 | 38.863 / 24.608 | 70.060 |

Point allocation was 0.914, 1.243, and 4.679 MB for radii 0, 1, and 3. The corresponding reference allocation was 1.053, 7.154, and 35.250 MB. The first population point call took 868 ms, including compilation; it is not in the warm median.

## Matrix Expansions

`matrix_shared_expansions` is the public `shared_expansions` counter for the complete origin/sample matrix. `matrix_query_expansions` is the public `query_expansions` counter: the number of origin/sample bits expanded across settled shared states. Neither is a per-origin average. All three window families had the same counters.

| Radius | Samples | Matrix queries | Jobs | Workers | Shared expansions | Query expansions |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 1 | 1 | 1 | 1 | 2,138 | 2,138 |
| 1 | 1 | 7 | 1 | 1 | 2,640 | 14,290 |
| 3 | 1 | 37 | 1 | 1 | 4,348 | 67,458 |
| 0 | 4 | 4 | 1 | 1 | 5,554 | 8,478 |
| 1 | 4 | 28 | 1 | 1 | 6,664 | 57,844 |
| 3 | 4 | 148 | 5 | 5 | 35,576 | 286,738 |
| 0 | 96 | 96 | 2 | 2 | 74,702 | 147,224 |
| 1 | 96 | 672 | 11 | 8 | 102,108 | 1,019,548 |

Job counts follow the loaded source: point tiles hold up to 64 origins. Window tiles hold up to eight origins, with up to 64 origin/sample bits per block. Radius 0 has 64 and 32 samples in two time blocks. Radius 1 has nine samples per full block and eleven time blocks. Returned worker counts confirm two and eight workers for these 96-sample cases.

The script also makes untimed single-origin population calls. Actual radius-1 per-origin counters are below, in sorted H3 order. For point calls, shared and query counts are equal. For windows, each entry is shared/query. These calls share samples but do not share origins.

| H3 origin | Point | Four samples | 96 samples | Cached reference, 96 samples |
| --- | ---: | ---: | ---: | ---: |
| `861fb4297ffffff` | 1,934 | 5,204 / 7,994 | 73,544 / 144,322 | 77,730 |
| `861fb4647ffffff` | 2,018 | 5,106 / 8,152 | 73,886 / 146,454 | 78,556 |
| `861fb4667ffffff` | 2,138 | 5,554 / 8,478 | 74,702 / 147,224 | 79,166 |
| `861fb466fffffff` | 2,018 | 5,122 / 8,108 | 72,862 / 144,212 | 77,264 |
| `861fb4677ffffff` | 2,026 | 5,294 / 8,452 | 74,156 / 146,806 | 78,538 |
| `861fb474fffffff` | 2,130 | 5,436 / 8,220 | 73,720 / 144,748 | 78,214 |
| `861fb475fffffff` | 2,026 | 5,292 / 8,440 | 73,766 / 145,782 | 78,184 |

The cached reference counter covers arrival routing, not geography or output. The point reference does not expose this counter; the log marks it as `-1`. Radius-3 per-origin population and cached-reference counters are in the complete log. Do not compare counters as if each expansion had the same cost.

## Population Checks

All eight query shapes passed comparison with `rtol=1e-12` and `atol=1e-6`. Each of the five window shapes passed all three families. All untimed single-origin checks also passed. Fraction-weighted totals can differ in the last few floating-point digits because reduction order differs.

For the central Paris origin:

| Selection | Selected cells | Population |
| --- | ---: | ---: |
| Point at 08:00 | 1,069 | 40,588,567 |
| Four-sample intersection | 712 | 33,559,810 |
| Four-sample union | 1,420 | 48,840,982 |
| Four-sample fraction-weighted union | 1,420 | 41,376,823.75 |
| 96-sample intersection | 63 | 8,223,726 |
| 96-sample union | 2,001 | 54,450,239 |
| 96-sample fraction-weighted union | 2,001 | 29,283,134.395833 |

Radius-1 results for 96 samples, in the same H3 order as the expansion table:

| Position | Intersection cells / population | Union cells / population | Fraction-weighted population |
| --- | ---: | ---: | ---: |
| 1 | 63 / 8,223,726 | 1,986 / 54,256,356 | 28,713,409.322917 |
| 2 | 67 / 8,307,287 | 1,999 / 54,437,310 | 28,915,600.239583 |
| 3 | 63 / 8,223,726 | 2,001 / 54,450,239 | 29,283,134.395833 |
| 4 | 12 / 4,069,430 | 1,994 / 54,336,823 | 28,558,835.114583 |
| 5 | 70 / 8,365,210 | 2,006 / 54,441,804 | 29,114,569.166667 |
| 6 | 63 / 8,223,726 | 1,991 / 54,267,446 | 28,692,595.052083 |
| 7 | 61 / 8,260,335 | 1,995 / 54,306,343 | 29,020,851.875000 |

Radius-3 point routes reached 777 to 1,069 cells, with population from 26,977,795 to 40,588,567. Four-sample intersections had 555 to 712 cells; unions had 1,114 to 1,420 cells. The complete log retains all 37 totals rather than only these ranges.

## Findings And Limits

The new path was slower than the independent reference for every radius-0 query shape. The 96-sample weighted case took 4.30 times the reference time and allocated more memory. It used only two workers, while the cached reference used eight. The population mask path still uses dictionaries, a heap, and per-lane deadline masks. No stage profile was collected, so this run does not assign the time difference to a specific operation.

Sharing across origins improved the measured totals for radii 1 and 3. The 96-sample radius-1 weighted case took 38.86 ms with eight workers, compared with 75.63 ms for seven independent cached windows. Its 102,108 shared expansions were 90.0% below its 1,019,548 expanded query bits. The independent cached reference reported 547,652 arrival-routing expansions in total; it also reuses work across samples.

These results establish performance only for this rail network, origin area, three-hour budget, and eight-thread process. They do not establish performance for the larger `everything` network or a seven-day budget. Radius 3 with 96 samples was not run. The machine was shared, and each median uses only three calls. The comparison includes reference distance output and three sums, versus one population sum; it is not an isolated test of the routing kernels.

## Earlier Midnight Run

Before origin/sample sharing, the four-thread midnight baseline measured radius-0 point routing at 0.390 ms versus 0.427 ms for the reference, and radius 1 at 0.558 ms versus 3.792 ms. It reached only 116 to 125 cells per origin. The four-sample mean-intersection timings were 0.803 versus 0.888 ms for radius 0, and 1.944 versus 3.999 ms for radius 1. Both population aggregations conserved the same known total.

Those runs used source SHA-256 `93912dc6f1feb9d411f67607b00d227a8a1dfacade2be981f2f369d75b98f46c`. Their logs remain at `/tmp/opencode/population-baseline.log` and `/tmp/opencode/population-baseline-final.log`. No old-engine snapshot was rerun at 08:00. Do not use the midnight results to calculate a daytime old-versus-new speed improvement.
