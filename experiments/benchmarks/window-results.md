# Departure-window implementation and measurements

Implemented sampled departure-window averages with request-local reuse, plus
selected-itinerary kilometre tracking. The new features use packed CPU Dijkstra;
this is not a batched-GPU benchmark.

## Contract

- Samples cover `[departure, departure + window_s)`, every `step_s` seconds.
- Unsuccessful departures contribute `budget_s` to mean elapsed time.
- `distance_km` and `reachable_elapsed_ms` average successful departures only.
- Coverage and sample counts are returned. HTTP omits cells never reached, as before.
- The origin has zero journey time/distance and full coverage.
- Missing input distance remains unavailable, not an inferred H3-centre distance.

Reuse is conservative: identical absolute first-hop arrival/distance states prove
that downstream results are unchanged. Changing an unused first-hop alternative may
still cause a new search. Each unchanged group uses its last sample's cutoff, so
later samples can admit destinations missed by an earlier budget without searching
the entire departure window's horizon unnecessarily. Elapsed-time sums and coverage
are exact integer calculations; distance uses a count-weighted floating-point mean.

## Real res5 measurement

Input: `data/rail_and_friends_res5.arrow`, 16,910,191 rows, with 3,314 out-of-range
durations skipped. The graph has 15,011 cells. These files currently have no
`distance_km` column, so real-network timings below cover time/coverage with unavailable
distance. Both modes were warmed; times are medians of three alternating measurements.
The baseline performs a separate budget-limited search for each departure.

| Origin | Budget | Samples | Searches with reuse | Reused samples | Reuse time | Independent time |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Paris `851fb467fffffff` | 3 h | 1,440 | 1,257 | 183 | 0.2174 s | 0.2275 s |
| Paris `851fb467fffffff` | 7 days | 1,440 | 1,257 | 183 | 4.4915 s | 5.0225 s |
| Sparse `8508134bfffffff` | 3 h | 1,440 | 3 | 1,437 | about 0.0001 s | 0.0369 s |
| Sparse `8508134bfffffff` | 7 days | 1,440 | 3 | 1,437 | 0.0105 s | 4.8679 s |

All elapsed sums and coverage counts matched the independent searches. Reuse saves
little at a busy origin whose useful departures change nearly every minute, but is
substantial for infrequent services. It is not a universal speedup claim.

Packing Arrow columns through a type-specialized function barrier avoided a regression
from dynamic column access: this run packed the large graph in 11.8 s including
compilation, rather than the initial implementation's 162 s. Warm query times exclude packing.

```sh
julia --project=router --threads=4 experiments/benchmarks/benchmark-window.jl data/rail_and_friends_res5.arrow
```

## Verification

- 2,641 checks passed with CPU, oneAPI, HTTP/Arrow and the existing frontend reader.
- Independent per-departure tests cover capped means, moving cutoffs, overnight wraps,
  sample boundaries, zero-time cycles, overtaking, varying connection lengths and
  deterministic ties. Large finite kilometre inputs also average without overflowing.
- Real res6 input loaded 35,760 cells; real res7 input loaded 68,783 cells. Two-departure
  windows and HTTP output matched separate point queries on both graphs.
- Tests use known segment lengths to verify route kilometres. Real kilometre values
  still require a five-column export, and the updated SQL has not been run locally.

## Next data step

Use `experiments/data/export.sql`, setting its target resolution and your desired mode filter.
Its additional non-null `Float64` column is:

```sql
geoDistance(e.stop_lon, e.stop_lat, e.next_lon, e.next_lat) / 1000 AS distance_km
```

Keep distance in deduplication. This reproduces the old stop-to-stop geodesic segment
measure, summed along the selected graph itinerary. It does not include unmodelled
movement within a coarse cell and is not railway-track geometry. After exporting,
the same server automatically includes point-route kilometres and conditional mean
kilometres in window responses, without further code changes.
