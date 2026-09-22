# Rough CPU Estimator

The standalone function is 874 bytes and 13 lines, including its comment. There is no runtime model file.

## Fit

Eight coefficients fit log CPU-ms by constrained least squares. Work exponents are nonnegative. The budget term is a power of 1 + log(1 + hours), so it grows beyond six hours without a hard cutoff. Coefficients are rounded to three significant digits.

The fitted walking exponent is 0.0049. A zero exponent removes that term from the generated function.

The original 384 records are development data, including the previously inspected London records. New 100-hour records also supply fitting data. New 168-hour Hamburg records are held out. Their CPU values are not used to fit the formula.

| Set | Rows | Median Factor Error | P90 Factor Error | Maximum Factor Error |
|---|---:|---:|---:|---:|
| Development | 397 | 2.26 | 8.83 | 103.41 |
| 168-hour holdout | 8 | 3.0 | 9.83 | 16.06 |

Factor error is the larger of predicted/actual and actual/predicted CPU time. These errors are not confidence limits.

## Long Checks

| Network | Resolution | Budget Hours | Population Radius | Samples | Actual CPU-ms | Estimate CPU-ms | Set |
|---|---:|---:|---:|---:|---:|---:|---|
| everything | 6 | 100.0 | not population | 1 | 137.1 | 58.8 | long_train |
| everything | 6 | 100.0 | 2 | 4 | 405.2 | 357.4 | long_train |
| everything | 6 | 100.0 | 6 | 1 | 343.0 | 398.8 | long_train |
| everything | 6 | 168.0 | not population | 4 | 945.3 | 132.0 | long_test |
| everything | 6 | 168.0 | 2 | 96 | 2051.4 | 1847.3 | long_test |
| everything | 8 | 100.0 | not population | 1 | 1515.1 | 148.6 | long_train |
| everything | 8 | 100.0 | 2 | 4 | 14592.9 | 903.5 | long_train |
| everything | 8 | 100.0 | 6 | 1 | 1650.2 | 1008.2 | long_train |
| everything | 8 | 168.0 | not population | 4 | 5358.7 | 333.7 | long_test |
| everything | 8 | 168.0 | 2 | 96 | 17676.6 | 4670.1 | long_test |
| everything | 8 | 100.0 | 18 | 1 | 144846.1 | 2333.0 | long_train |
| rail_and_friends | 6 | 100.0 | not population | 1 | 46.4 | 28.0 | long_train |
| rail_and_friends | 6 | 100.0 | 2 | 4 | 119.8 | 170.5 | long_train |
| rail_and_friends | 6 | 100.0 | 6 | 1 | 62.1 | 190.2 | long_train |
| rail_and_friends | 6 | 168.0 | not population | 4 | 100.7 | 63.0 | long_test |
| rail_and_friends | 6 | 168.0 | 2 | 96 | 398.5 | 881.1 | long_test |
| rail_and_friends | 8 | 100.0 | not population | 1 | 69.9 | 70.9 | long_train |
| rail_and_friends | 8 | 100.0 | 2 | 4 | 520.4 | 431.0 | long_train |
| rail_and_friends | 8 | 100.0 | 6 | 1 | 77.8 | 480.9 | long_train |
| rail_and_friends | 8 | 168.0 | not population | 4 | 269.9 | 159.2 | long_test |
| rail_and_friends | 8 | 168.0 | 2 | 96 | 575.2 | 2227.6 | long_test |

## Limits

- This is an order-of-magnitude estimate, not an admission or billing limit.
- The target is parsing, routing, and Arrow serialization process CPU time. It is not HTTP elapsed time.
- Measurements use Julia 1.12.7 on the local Xeon E3-1275 v6, eight default threads, and at most three route workers. Each process runs one query at a time.
- Graphs, compilation, and workspace pools are warm. Population result caching is disabled. Retained timings have zero compilation time.
- Location, departure time, output encoding, distance mode, exclusion, and window aggregation mode are ignored.
- The sample design pairs some parameters. Independent parameter effects are not established.
- Unknown networks use the everything factor. Other resolutions, walking above one hour, and larger parameters extrapolate without rejection. They were not validated.
- All 384 original observations remain in calibration.csv and the original JSONL files. New observations are in the *-long.jsonl files.
- No live endpoint, production file, server configuration, or input data was changed.

