# Backend TODO

Frontend tasks are tracked in [H3-MON/todo.md](../../H3-MON/todo.md).

## Current Implementation Contract

Implemented: departure-window averaging and selected-itinerary kilometres.
Code, contract and measurements: [README](README.md), [window results](window-results.md).

- Sample a half-open departure window, every minute by default; expose the interval.
- Unreachable departures contribute the full travel budget to mean elapsed time.
  Return coverage/counts and average route kilometres over successful departures only.
- Reuse absolute arrival/distance results within a request when origin connection
  choices give the same first-hop labels. Use a common search cutoff and apply the
  moving per-departure budget during aggregation; never reuse a truncated search.
- Use CPU downstream catchup by default for windows; batched GPU arrivals with CPU
  kilometre replay are opt-in. Existing point-query backend selection remains.
- Attach optional `distance_km Float64` to each retained timetable connection. Sum
  these along the selected earliest-arrival route, without claiming minimum distance
  among equal-time routes. The export uses stop-to-stop geodesic km, not track geometry.
- Existing four-column files remain usable for time results; missing distance is
  unavailable, not a cell-centre approximation. Re-export to obtain real kilometres.
- Restore H3 validity checks and infer a uniform graph resolution instead of the
  temporary validation bypass. One input file per server remains the model.

Implementation checklist:

- [x] Pack per-connection distances and infer graph resolution.
- [x] Track selected-route distance in CPU Dijkstra.
- [x] Implement exact within-call reuse and capped-mean aggregation.
- [x] Add HTTP/Arrow window parameters, diagnostics, tests and documentation.
- [x] Validate reuse against independent per-departure searches and benchmark it.
- [x] Reproduce the old distance/time quantile plot using the returned `distance_km` values.

## Departure-Time Ranges

- [x] Accept a range of departure times and average the routing results.

- Define sampling frequency, interval endpoints and overnight wrapping. Start with
  an explicit sampled baseline before adding more sophisticated profile algorithms.
- For travel-time output, average elapsed journey durations, not absolute arrival
  timestamps. Apply the travel budget separately to each sampled departure.
- Decide how to handle destinations unreachable at some departures. Include reachable
  fraction or sample counts so a conditional mean cannot hide poor availability.
- Return the aggregate results as Arrow, with the averaging semantics documented.
- Benchmark batched departures on a resident graph and on-device sums/counts against
  the packed CPU baseline. Investigate reuse between nearby departures if it helps.

## Downstream Catch-Up Reuse

Implemented; measurements and caveats are in [window optimization results](window-optimization-results.md).

- [x] Shared grouping/aggregation helpers and bounded CPU catch-up implementation.
- [x] Batched KA kernels with activity flags and configurable convergence checks.
- [x] CPU differential tests for both implementations, including kilometre ties and overflow.
- [x] Full iGPU differential tests, real-network benchmarks and endpoint integration.

- Preserve the existing origin-group implementation as the reference.
- Process bounded chunks of groups backwards: earlier starts only improve arrival
  labels, so repair propagation can stop at unchanged downstream arrivals.
- Cache selected connections for settled tails. Reconstruct canonical kilometres
  from cached connections and final labels, preserving Dijkstra's tie behavior.
- Aggregate chunks in chronological order to preserve the existing distance means.
- Add a portable batched KA backend with active flags and chunked convergence checks.
  Keep distance replay on CPU; do not introduce racy or lower-precision GPU km labels.
- Validate every result against the reference and benchmark before selecting defaults.

- [x] Reuse downstream routing work when different departures catch the same onward connection.

- Keep the current origin-only grouping as a baseline; it does not reuse downstream chains.
- Measure catch-up frequency and compare incremental search repair with reusable
  departure-time profiles or suffix results.
- Keep prefix distance separate from suffix distance: equal onward arrivals do not
  imply equal total kilometres for routes reaching the connection differently.
- Invalidate paths through missed connections and respect each departure's moving
  budget. Do not simply retain old arrival labels when advancing the departure time.
- Differential-test arrivals, chosen-route kilometres and capped averages against
  independent searches, including midnight, exact departures and equal-time alternatives.
- Benchmark busy origins where first-hop changes currently prevent much reuse.

## Stop-to-Stop Distances

- [x] Calculate stop-to-stop distances to reproduce the earlier distance-quantile
  versus time-quantile plots.

The export, router and quantile-difference calculation are implemented and tested.
Use `metric=distance_time_quantile`. Real-data ranks were checked against the old
formula on a 1,067-cell full-day query; visual comparison with the old plot remains.

- [x] Add a selectable distance-quantile minus time-quantile output metric.
- Compute both ranks over the same finite-valued cell population and retain the
  underlying distance/time columns. Match the old rescaled empirical-CDF convention,
  with explicit handling of ties and constant columns.
- For windows, rank the averaged distance and budget-capped time, then subtract.
  Do not average per-departure rank differences.

- Locate the original plotting calculation and match its distance definition:
  distinguish direct origin-to-stop distance from accumulated route distance.
- Retain original stop coordinates or precomputed distances before H3 aggregation;
  do not silently substitute distances between coarse H3 cell centres.
- If distance follows a routed path, retain the necessary edge metadata and define
  how equal-time alternatives are selected.
- Return distance alongside travel time with explicit units, and reproduce a known
  plot before optimizing. Keep sampling and unreachable-destination handling aligned.

## Resolution and Transport Selection

- [ ] Let requests select H3 resolution and transport types.

- Start with named Arrow snapshots for supported resolution/mode combinations, such
  as rail-only or combined rail/bus graphs, rather than rebuilding graphs per request.
- Select snapshots through an allowlisted dataset identifier, not an arbitrary file
  path. Record each dataset's resolution, transport types and source snapshot.
- Validate origin resolution against the selected graph. Document that changing
  routing resolution changes the within-cell transfer approximation, not just display.
- Load and cache packed graphs/device data with a memory limit; avoid repeated packing
  and uploads when switching between already-loaded datasets.
- Connect dataset selection to H3-MON's metadata-defined controls. Revisit runtime
  filtering or resolution conversion only if separate snapshots become limiting.

## benchmarking

- see if igpu is actually faster than cpu


## stretch goals but out of scope

- it'd be cool to run this on every h3 in europe across all minutes of the day, and then rank h3 by travel time maybe weighted by population. should find the most central h3s in europe. but it'd be cool to plot that on a map anyway. ditto for the average difference in time/distance quantiles
