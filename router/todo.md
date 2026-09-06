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
- Use packed CPU Dijkstra for these queries. Existing GPU arrival-only routing remains.
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
- [ ] Supply an enriched real export with `distance_km` and reproduce the old distance/time plot.

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

## Stop-to-Stop Distances

- [ ] Calculate stop-to-stop distances to reproduce the earlier distance-quantile
  versus time-quantile plots.

The export and router support are implemented and fixture-tested. Real-data plot
verification awaits the additional distance column; existing files cannot supply it.

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
