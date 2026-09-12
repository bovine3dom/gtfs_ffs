# Full Res8 Benchmark

Promotion remains blocked. Warm query speed does not include index preparation.

Each row uses three measured pairs after one warm pair. Measured pairs have no compilation time. Both variants use 64 origins per batch and the same full fine graph, walking geometry, and population input. There is no result cache.

The cold estimate adds hierarchy preparation, boarding preparation, and the median warm query. The fine graph, walking geometry, and population are already resident. This sum is not a single cold-request measurement. It includes a core rebuild for each city.

WMAE is sum(abs(candidate - fine)) / sum(fine). Relative error percentiles use only origins with positive fine population. Negative bias means underestimation.

| City | Origins | Samples | Step (min) | Budget (h) | Mode | Fine (s) | Warm (s) | Speed ratio | Cold estimate (s) | WMAE (%) | Relative p95 (%) |
|---|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|
| Paris | 127 | 4 | 1 | 3 | mean_intersection | 0.555 | 0.128 | 4.33 | 116.973 | 15.99 | 33.91 |
| Paris | 127 | 4 | 1 | 3 | reachable_union | 0.572 | 0.173 | 3.31 | 117.017 | 14.19 | 33.18 |
| Paris | 1027 | 96 | 15 | 3 | mean_intersection | 33.953 | 5.195 | 6.54 | 122.040 | 2.70 | 5.34 |
| Paris | 1027 | 96 | 15 | 3 | reachable_union | 34.233 | 5.157 | 6.64 | 122.002 | 17.87 | 25.93 |
| Paris | 1027 | 96 | 1 | 3 | mean_intersection | 20.789 | 3.203 | 6.49 | 120.047 | 19.21 | 28.17 |
| Paris | 1027 | 96 | 1 | 3 | reachable_union | 20.602 | 3.047 | 6.76 | 119.892 | 20.97 | 30.26 |
| Paris | 127 | 180 | 1 | 6 | mean_intersection | 66.517 | 12.955 | 5.13 | 129.800 | 25.58 | 33.94 |
| Paris | 127 | 180 | 1 | 6 | reachable_union | 68.015 | 12.669 | 5.37 | 129.514 | 17.19 | 26.29 |
| Paris | 127 | 180 | 1 | 12 | mean_intersection | 129.829 | 28.616 | 4.54 | 145.460 | 5.82 | 7.09 |
| Paris | 127 | 180 | 1 | 12 | reachable_union | 130.769 | 27.618 | 4.73 | 144.462 | 5.17 | 6.33 |
| London | 127 | 4 | 1 | 3 | mean_intersection | 0.825 | 0.157 | 5.25 | 120.213 | 12.79 | 20.80 |
| London | 127 | 4 | 1 | 3 | reachable_union | 0.835 | 0.168 | 4.98 | 120.223 | 12.13 | 20.39 |
| London | 1027 | 96 | 15 | 3 | mean_intersection | 64.602 | 9.649 | 6.70 | 129.705 | 3.91 | 9.52 |
| London | 1027 | 96 | 15 | 3 | reachable_union | 63.653 | 9.335 | 6.82 | 129.391 | 20.22 | 27.04 |
| London | 1027 | 96 | 1 | 3 | mean_intersection | 43.004 | 5.955 | 7.22 | 126.011 | 21.99 | 30.50 |
| London | 1027 | 96 | 1 | 3 | reachable_union | 42.993 | 5.837 | 7.37 | 125.893 | 21.55 | 29.81 |
| London | 127 | 180 | 1 | 6 | mean_intersection | 48.972 | 10.440 | 4.69 | 130.496 | 15.52 | 41.38 |
| London | 127 | 180 | 1 | 6 | reachable_union | 47.598 | 9.610 | 4.95 | 129.666 | 15.52 | 44.19 |
| London | 127 | 180 | 1 | 12 | mean_intersection | 121.236 | 25.721 | 4.71 | 145.777 | 16.14 | 52.77 |
| London | 127 | 180 | 1 | 12 | reachable_union | 115.930 | 26.668 | 4.35 | 146.724 | 10.26 | 37.36 |

See `values.csv` for each origin, its fine population, and its signed error. See `outliers.csv` for the ten largest absolute errors in each case. See `metadata.txt` for source hashes and stage memory.

This run does not validate the time metric, HTTP integration, an unprepared origin, or a moved centre. It does not test 10,000 origins or a res7 core. It does not set a production default.
