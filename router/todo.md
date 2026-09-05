# Backend TODO

Frontend tasks are tracked in [H3-MON/todo.md](../../H3-MON/todo.md).

## Departure-Time Ranges

- [ ] Accept a range of departure times and average the routing results.

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
