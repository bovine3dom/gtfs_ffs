# Global Coarse Router

The full-network population speed gate passes for the res8-to-res6 model. Index preparation is global startup work. It is not a regional query cost. Every query includes fine-origin access work. No origin access cache or result cache is used.

All six models were resident during these queries. Each case has one first invocation and three measured interleaved pairs. Measured pairs have zero compilation time. The first invocation can include compilation.

| City | Origins | Step (min) | Mode | Fine median (s) | Coarse median (s) | Speed ratio | First coarse call (s) | WMAE (%) | Relative p95 (%) | Negative Origins (%) | Positive Origins (%) |
|---|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Paris | 1027 | 15 | mean_intersection | 34.609 | 7.788 | 4.44 | 10.406 | 2.70 | 5.34 | 74.29 | 25.71 |
| Paris | 1027 | 15 | reachable_union | 35.482 | 8.058 | 4.40 | 7.770 | 17.87 | 25.93 | 100.00 | 0.00 |
| Paris | 1027 | 1 | mean_intersection | 20.756 | 5.059 | 4.10 | 5.330 | 19.21 | 28.17 | 100.00 | 0.00 |
| Paris | 1027 | 1 | reachable_union | 21.397 | 5.083 | 4.21 | 4.989 | 20.97 | 30.26 | 100.00 | 0.00 |
| Paris_moved | 1027 | 1 | mean_intersection | 21.066 | 5.114 | 4.12 | 4.868 | 19.39 | 28.25 | 100.00 | 0.00 |
| Paris_large | 9919 | 1 | mean_intersection | 116.503 | 22.797 | 5.11 | 22.035 | 12.82 | 56.42 | 87.91 | 9.88 |
| London | 1027 | 15 | mean_intersection | 66.739 | 14.160 | 4.71 | 14.464 | 3.91 | 9.52 | 13.73 | 86.27 |
| London | 1027 | 15 | reachable_union | 67.493 | 14.447 | 4.67 | 14.387 | 20.22 | 27.04 | 100.00 | 0.00 |
| London | 1027 | 1 | mean_intersection | 44.701 | 8.995 | 4.97 | 9.297 | 21.99 | 30.50 | 100.00 | 0.00 |
| London | 1027 | 1 | reachable_union | 45.215 | 8.949 | 5.05 | 9.112 | 21.55 | 29.81 | 100.00 | 0.00 |
| London_moved | 1027 | 1 | mean_intersection | 45.300 | 8.990 | 5.04 | 9.084 | 21.82 | 30.38 | 100.00 | 0.00 |

All cases use 96 samples, a three-hour travel budget, and one-hour walking. `Paris_large` has 9,919 origins. Each moved case shifts the centre by one fine-grid cell. Both cities and all moved origins use the same model object.

The comparison with the historical regional model checked 8216 origin-case values. Maximum absolute population difference: 1.862645149230957e-8. The global model removes regional preparation without changing these measured population results.

See `metadata.txt` for all six model sizes, startup times, source hashes, memory, and time-output comparisons. Time measurements are single diagnostic calls, not paired latency benchmarks. Time output is approximate and can lose or add destinations. See `outliers.csv` for per-case population outliers.
