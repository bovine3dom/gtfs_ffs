# Backend TODO

Frontend tasks are tracked in [H3-MON/todo.md](../../H3-MON/todo.md).

## Estimated Walking: Findings and Next Deliverables

Resident packed adjacency is verified: explicit `prepare_walking(index)`, eager handler
preparation, canonical geographic order and precomputed graph IDs. Both one- and
four-thread full suites passed **52,633 checks**, including 2,884 new adjacency checks.
Real res7 preparation took 1.524 s and retained 40.083 MiB of adjacency. Warmed
median-of-three prepared/unprepared timings were 1.834/91.764 ms for points,
13.456/104.655 ms for 12 samples, and 1.121/1.376 s for 1,440 samples, with exact
output parity. Full-day prepared allocations are higher; see
[resident adjacency results](walking-adjacency-results.md) for scope and raw samples.

Status: geometry, CPU point/window routing and HTTP walking are implemented. Real
res5/res6/res7 validation and timings are in [walking results](walking-results.md).
Walking windows now default to parallel two-state catch-up with shared geometry and
fresh canonical km replay. The independent CPU window oracle remains available.
Multi-origin work and GPU changes remain deferred.

Optimization findings and measurements: [walking optimization results](walking-optimization-results.md).
The old res7 12-sample profile attributed 92% of routing time to geographic enumeration
and only 5% to graph search. Clipped radii were not cached: 11,023 builds for 2,280
unique eligible cells. Partial-coverage reuse, certified small disks and backward
repair remove that repeated work. Shared geometry avoids per-worker duplication;
staggered preparation avoids workers queuing behind the same cell. Full-day minute
sampling with a three-hour budget improved from 34.11s to 1.32s on four workers,
with exact original-output parity. No resource caps were restored.

Current verification: **49,749 checks passed with both one and four Julia threads**,
including geometry/polygon parity, two-state replay and overflow, shared-cache
publication/recovery, and live concurrent HTTP parity. Real res7 comparisons cover
12/1,440 samples with a three-hour budget and 12 samples with a seven-day budget.
The actual `serve.jl` launcher also passed an HTTP smoke test with four workers.

Verification after resource-cap removal: **15,896 checks passed with both one and four
Julia threads**, including
independent raw-schedule walking oracles, geographic coverage, window means, HTTP
validation, removed-keyword rejection, cache correctness and unchanged transit-only
behavior. Historical live concurrent walking HTTP/Arrow checks also passed.
Real res7 validation independently matched all 68,783 graph labels and
62 geographic destinations; see the linked benchmark report for scope and caveats.

### Agreed Behavior

- Restore estimated walking with a configurable maximum duration per hop, default
  **60 minutes at 5 km/h** (5 km per hop). Zero disables walking.
- Expose the limit per request as `max_walk_s`, default `3600`.
- Every origin, including an off-graph origin, may start with a walk. Walking is
  available at any departure instant; it is not represented as scheduled services.
- Return all geographic H3 destinations reachable by a final walk, including cells
  with no transit stop and direct walk-only destinations. Include every graph vertex
  as a possible walking target, not just source-side transit cells.
- Initially use the loaded graph's H3 resolution for returned geographic cells.
- Keep the original no-consecutive-walk rule: start/train -> walk is allowed,
  walk -> train is allowed, and walk -> walk is forbidden.
- Walking consumes the overall journey budget and contributes estimated kilometres
  to the selected itinerary and conditional window-distance mean.

### Findings

- Before this work, the router followed scheduled connections only; an off-graph
  origin returned only itself. The original transit-only APIs remain available.
- The original res10 sketch used four H3 grid steps, distance
  `steps * 2 * sqrt(source_cell_area_km2 / 3)`, and 5 km/h. This was an area-based
  approximation, not pedestrian routing. Walking targets were source-side transit
  cells, and no general surrounding-cell egress surface was generated.
- Current Arrow inputs retain H3 endpoints and optional transit-segment kilometres,
  not original stop coordinates. Those kilometres cannot determine arbitrary
  transfer distances. Coordinates exist upstream but need an additional export.
- Graph-only estimate: symmetric great-circle distance between H3 cell
  centres, converted to duration at 5 km/h. This restores estimated walking, but
  does not reproduce the old grid-distance numbers exactly.
- Build neighbours with a 3D spatial hash of unit-sphere cell centres. Search the
  cell's bin and its 26 neighbours, then apply the distance/time cutoff. This handles
  the antimeridian and poles without enumerating large H3 disks or all vertex pairs.
  `H3.API.cellToLatLng` supplies latitude/longitude in radians.
- That spatial index covers existing graph vertices only. Geographic destinations
  additionally require H3-cell enumeration and exact distance filtering; do not
  mistake graph-neighbour lookup for complete egress coverage.
- Round walking duration upward to integer milliseconds; admit a hop only when its
  duration is within the configured limit. Validate limits before allocating topology.
- Coarse cells remain problematic: nearby stops across a boundary may have centres
  more than 5 km apart, while distant stops inside one cell remain freely connected.
  Do not silently subtract a cell radius or force adjacent cells to connect.
- Restore two labels/states per vertex: earliest arrival of any kind, and earliest
  arrival eligible to start a walk. An earlier walked arrival must not erase a later
  train arrival that can enable another walk.
- Transit self-edges may reset walking eligibility. Existing physical-cell self-edge
  skipping in catch-up, replay and GPU kernels is valid only for transit-only routing.
- Existing first-hop grouping is not safe unchanged: walking arrivals move with the
  departure time even when origin train choices do not. Downstream catch-up can still
  help after those walks catch the same service, but requires walking-aware states.

### Small Deliverables

- [x] **1. Walking topology only.** Add time-limited, estimated walking adjacency,
  defaulting to one hour, including lookup from arbitrary origins. Test against brute
  force, covering exact limits, zero, self-exclusion, poles and longitude wrapping.
  Do not change routing behavior in this deliverable.
- [x] **2. Walking-aware CPU point routing.** Add the two-state reference and selected
  distance accounting, direct walk-only coverage and geographic egress. Test train/walk
  combinations, rejected chained walks, later walk-eligible arrivals, off-graph origins
  and destinations, self-edge resets and overall cutoff equality.
- [x] **3. Window correctness baseline.** Run independent walking-aware point searches
  per sampled departure; preserve capped times, coverage, kilometres and rank-of-means
  output. Do not apply the transit-only grouping/cache until validated for walks.
- [x] **4. Walking-aware reuse.** Adapt state, connection caches and canonical replay;
  prove equivalence to deliverable 3 before restoring parallel catch-up performance.
- [x] **5. Service configuration.** Expose and document the maximum hop duration, with
  `max_walk_s=3600` by default and `0` disabling walking. Reuse the spatial index and
  cached/precomputed connectivity across limits where valid. By user choice, impose
  no resource caps on walking output, candidates, work or request-local geometry caches.
  Do not silently use a transit-only backend when walking is requested.
- [x] **6. Real-data validation.** Recorded walking-edge counts, memory and timings at
  res5/res6/res7, exact disabled-mode parity, and independent res7 arrival/egress checks.
  Sample time/km labels are recorded, not full itineraries: this API has no path output.
  Keep benchmarks and unsupported backend combinations explicit.

Input limits remain: 0..604800 seconds per hop, seven-day journey budget, one-day
window and existing sample limits. Provisional resource caps and their HTTP 422
handling have been removed; large valid requests may use substantial memory and CPU.
Further walking work should profile replay, output construction and aggregation;
serial aggregation was about 13-16% of the optimized four-worker probes, not most
of the tail. Point-query geography remains serial. Frontend/transfer time was not
profiled and must not be inferred from these backend measurements.
With centre-based estimates,
the default 5 km hop may reach no neighbouring res5 centre; verify useful walking
coverage on finer graphs rather than silently changing the distance model.

## Current Implementation Contract

Implemented: departure-window averaging, selected-itinerary kilometres and estimated
walking access/transfers/geographic egress. The reuse/backend details below describe
transit-only requests (`max_walk_s=0`); walking uses the baseline documented above.
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

- [x] Benchmark independent CPU chunks on 1/2/4 workers; keep aggregation chronological
  and workspaces bounded by worker count. GPU implementation stays unchanged.
  Four workers improved the measured catch-up window time by 1.84x / 2.61x for
  three-hour / seven-day travel budgets. See the optimization report for commands.

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

## Further CPU Optimizations

Compute work only; interactive request caching, coalescing and cancellation are not
priorities here. Profile the parallel path before choosing the next change.

- [ ] Reuse the kilometre-replay heap instead of constructing one for every group.
  Reuse workspace-owned storage only after the preceding operation has drained it;
  preserve canonical discovery order and safe exception handling.
- [ ] Parallelize aggregation across disjoint destination ranges.
  Process groups chronologically within each destination, preserving exact means;
  do not combine independently computed partial averages.
- [ ] Replace compute/aggregate wave barriers with a bounded pipeline.
  Overlap future chunk computation with ordered consumption, retain ownership until
  snapshots are consumed, and join workers safely on failure.
- [ ] Reduce full-graph clearing and snapshots for sparse queries.
  Investigate touched-vertex tracking or sparse/delta snapshots, with a dense fallback
  for broad coverage. Preserve cutoff filtering, capped penalties and route kilometres.

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


## stretch goals but out of scope

- it'd be cool to run this on every h3 in europe across all minutes of the day, and then rank h3 by travel time maybe weighted by population. should find the most central h3s in europe. but it'd be cool to plot that on a map anyway. ditto for the average difference in time/distance quantiles

## soon

- for distance - time quantile, also try straight line distance between origin and destination. should solve bordeaux looking weirdly fast because you have to go via Paris (lol)
