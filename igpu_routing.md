# iGPU Routing Research and Build Plan

| Item | Value |
|---|---|
| Status | Res5 transit-only MVP implemented; representative-data benchmarking pending |
| Research date | 2026-08-08 |
| Router sketch | `plots/longest_journey.jl` |
| Primary target | Low-power integrated GPUs, with Intel HD Graphics 630 as the minimum reference point |
| Implementation constraints | Julia, KernelAbstractions.jl, and backend-portable kernels |

## MVP implementation update (2026-09-05)

The first implementation is in [`router/`](router/README.md). The sections below
retain the original res10 walking/path research plan; the implemented MVP is narrower:

- Routing directly on resolution-5 H3 cells, not fine routing aggregated for display.
- Stops within one cell are freely interchangeable. Intercell walking, access/egress,
  service calendars, trip continuity, synthetic shuttles, and path reconstruction are absent.
- Daily-repeating timetable profiles fix overtaking, duplicate departures, and overnight wrapping.
- One `UInt32` label per node suffices without walking. Packed CPU Dijkstra is the oracle.
- The same dense KernelAbstractions/Atomix transit kernels run on CPU and iGPU.
  Separate previous/next labels avoid concurrent ordinary reads and atomic writes.
  No activity flags or frontier optimization are implemented yet.
- `GET /reachable` accepts a hex `index` or unsigned `index_lower`/`index_upper` words,
  explicit `departure=HH:MM:SS`, and `budget_s`. Outputs are unique reachable graph cells
  plus the origin, inclusive of the cutoff; an off-graph origin returns only itself.
- Arrow output is an uncompressed IPC file, with plain strings or split `UInt32` words,
  `value` (elapsed minutes, required by H3-MON), and exact `elapsed_ms`.
  H3-MON's installed reader accepts files and streams. Its `onclick`/`onmove`
  metadata hooks now fetch complete Arrow responses from the routing endpoint.

The input contract and repeatable export are in [`router/export.sql`](router/export.sql):
non-null `from_h3 UInt64`, `to_h3 UInt64`, `departure_ms UInt32`, `duration_ms Int64`.
Endpoints must be res5, clocks within a day, and durations between zero and seven days.
The source `travel_time` is in minutes and is multiplied by 60,000 during export.

The actual target is an Intel HD Graphics P630 on Linux. Julia 1.12.7, oneAPI.jl 2.7.2,
KernelAbstractions 0.9.42, and Atomix 1.1.3 are used by the new environment.
Device detection, CPU/iGPU routing, contended minimum/OR/CAS, HTTP concurrency, and
H3-MON Arrow-reader tests pass (1,313 checks combined). The oneAPI server launcher
also passes a live HTTP smoke test; no server is left running after validation.
The installed Atomix oneAPI extension does not implement plain atomic loads/stores;
the kernels use its supported read-modify-write operations instead.

The working legacy-driver override remains launch configuration, not routing code:

```sh
env ZE_ENABLE_ALT_DRIVERS=/usr/lib/libze_intel_gpu_legacy1.so.1 ROUTER_BACKEND=oneapi julia --project=router router/serve.jl --demo
```

Real-network latency, graph-sized memory headroom, and sustained-load behavior still
require the representative edgelist. Passing synthetic tests is not a GPU speedup claim.

## Scope and agreed goals

This document investigates how to accelerate the one-to-all earliest-arrival router sketched in `plots/longest_journey.jl` using an integrated GPU.

The desired workloads are equally important:

- Low latency for one origin and departure time.
- High throughput for many origins and/or departure times.

The desired output split is:

- The GPU computes earliest-arrival labels.
- The CPU reconstructs only selected paths.

Algorithmic changes are allowed. The kernel implementation should remain portable across KernelAbstractions backends rather than depending on Intel-only kernel code.

## Executive conclusions

1. CSR is a useful first step, but a conventional sparse matrix is not the right representation. The router needs a nested compressed representation: vertices point to outgoing edge groups, and edge groups point to packed timetable profiles.
2. The current router should not be ported directly. DataFrames, dictionaries, `DateTime`, strings, locks, channels, and H3 calls must all be removed from the routing loop first.
3. The most relevant published GPU algorithm uses topology-driven iterative relaxation, integer atomic minimum, active flags, and a CSR-inspired timetable hierarchy. It reports large speedups on a GTX 1080 Ti, but those numbers cannot be projected onto an HD 630.
4. Batching origins is the strongest likely use of the iGPU. Transportation-network research specifically finds that small, low-degree graphs often do not expose enough single-source work to occupy a GPU.
5. A packed CPU router is likely to be dramatically faster than the current sketch and may remain faster than an HD 630 for a single query. The GPU must be compared against the packed CPU implementation, not against the current DataFrame-based code.
6. HD 630 software support is a project risk. Intel now supplies Gen9 compute support through a frozen legacy runtime, while current oneAPI.jl artifacts use a newer Intel runtime. Device detection and 32-bit atomic operations must be proven on the target machine before GPU implementation is considered viable.
7. KernelAbstractions should be used for global work-item kernels and Atomix integer atomics. Portable subgroup or warp-level optimization should not be part of the first implementation.

## What the current router computes

The graph is constructed from train edges grouped by H3 cells at resolution 10:

- Only rail route types are selected at `plots/longest_journey.jl:37-43`.
- Departure dates are discarded and converted to `Time` at `plots/longest_journey.jl:44`.
- The resulting schedule repeats every day.
- A synthetic Elvas to Badajoz service is inserted at `plots/longest_journey.jl:46-55`.
- Edges are grouped by `(h3, next_h3)` at `plots/longest_journey.jl:57`.
- Walking is allowed within an H3 disk of radius 4.
- Consecutive walking legs are forbidden.
- The cutoff is inclusive.
- The principal label is earliest arrival per `(H3 cell, previous leg was walk)`.

Recorded scale in the source comments and experiments:

| Quantity | Approximate value | Source |
|---|---:|---|
| Unique H3 cells | 50,000 to 55,000 | `plots/longest_journey.jl:36` |
| Directed `(h3, next_h3)` groups | 163,192 | `plots/longest_journey.jl:83-99` |
| Portugal and Finland candidate origins | About 700 | `plots/longest_journey.jl:302-306` |
| Query horizon used for longest routes | Seven days | `plots/longest_journey.jl:267-272` |

The total number of timetable rows in the train subset is not recorded. That count is needed to estimate device memory and the value of schedule compression.

## Current performance problems

The current implementation is dominated by dynamic, allocation-heavy operations that are unsuitable for a GPU and expensive on a CPU.

| Problem | Location | Consequence |
|---|---|---|
| DataFrame group lookup in the inner loop | `plots/longest_journey.jl:121` | Hashing and dynamic objects for every expanded edge |
| DataFrame row materialization | `plots/longest_journey.jl:126` | Allocation and type-heavy column access |
| Tuple-keyed dictionaries for labels | `plots/longest_journey.jl:101-107` | Random memory access and no device representation |
| One global spinlock | `plots/longest_journey.jl:137-145`, `168-181` | Serializes competing relaxations |
| New channel for every wave | `plots/longest_journey.jl:110` | Repeated allocation and synchronization |
| Queue copy and clear every wave | `plots/longest_journey.jl:111-113` | Repeated memory traffic |
| Temporary dictionary every wave | `plots/longest_journey.jl:189-198` | More hashing and allocation |
| H3 calls during every eligible expansion | `plots/longest_journey.jl:152-160` | Repeated C-library calls for static data |
| Binary search in `friendly_keys` for each walk candidate | `plots/longest_journey.jl:157` | Repeated membership work |
| `DateTime`, `String`, and `Float64` in route state | `plots/longest_journey.jl:25-31` | Large labels and device-incompatible metadata |
| Complete searches retained in `bests` | `plots/longest_journey.jl:265-272` | High host memory use |

A packed CPU implementation should eliminate nearly all of this overhead before GPU work begins.

## Correctness issues to settle first

Performance comparisons are only useful if the reference semantics are explicit. The current sketch has several ambiguous or incorrect cases.

### Overtaking connections

For each `(u, v)` group, the router chooses the first departure at or after the current time:

```julia
idx = searchsortedfirst(t.departure_time, current_clock)
row = t[wrapped ? 1 : idx, :]
```

This is only correct when arrivals are nondecreasing with departure time. If a later fast train overtakes an earlier slow train, the earlier row is not the best edge traversal.

The packed representation should remove dominated connections or otherwise calculate the minimum arrival over all feasible departures.

### Daily wrapping

When no departure remains today, the current code considers the first departure tomorrow. It does not compare tomorrow's early services against a late but slow service still available today. A two-day periodic edge profile handles this correctly.

### Equal arrivals and distance

Equal arrival times never replace an existing label. The selected predecessor and accumulated distance therefore depend on scheduling order. The source itself notes an unresolved distance update rule at `plots/longest_journey.jl:334`.

The recommended first objective is:

```text
minimize arrival time
break exact ties by stable edge ID during CPU reconstruction
```

If minimum or maximum physical distance among equally fast paths is required, that is a separate secondary optimization. It should initially be calculated on the CPU over the subgraph of edges consistent with optimal arrival labels. It should not force 64-bit compound atomics into the first GPU kernel.

### Parent consistency

A child stores only the parent H3 cell and parent walk flag. That parent's label may later be replaced, so reconstructed paths need not correspond to the state that originally generated the child. GPU labels plus backwards CPU reconstruction avoid this problem.

### Calendar semantics

The current timetable repeats every day and discards service calendars, weekdays, dates, and time zones. This is acceptable for reproducing the current fantasy timetable, but it must not be confused with a real GTFS timetable.

The packed format should support both modes:

| Mode | Stored time |
|---|---|
| Repeating fantasy timetable | Time within a period plus period length |
| Real finite timetable | Absolute integer offset from a chosen epoch |

## Recommended routing state

The current `(vertex, was_walk)` state can be represented more compactly using two labels per vertex.

Define:

```text
arrival_any[v]
    Earliest arrival at v regardless of the previous action.

arrival_walkable[v]
    Earliest arrival at v where the previous action was not a walk.
    A walk may begin from this label.
```

Initialization at source `s` and time `t0`:

```text
arrival_any[s] = t0
arrival_walkable[s] = t0
all other labels = INF
```

Transit relaxation from `u` to `v`:

```text
candidate = next_arrival(edge, arrival_any[u])
arrival_any[v] = min(arrival_any[v], candidate)
arrival_walkable[v] = min(arrival_walkable[v], candidate)
```

Walking relaxation from `u` to `v`:

```text
candidate = arrival_walkable[u] + walk_duration
arrival_any[v] = min(arrival_any[v], candidate)
```

This is equivalent to the two explicit last-action states under the current rules:

- An earlier arrival can always wait for any train available to a later arrival.
- A train can follow either a train or a walk.
- A walk can only follow a start or train state.
- A train produces a walkable state.
- A walk does not produce a walkable state.

It also represents dominance naturally. `arrival_any[v]` is always less than or equal to `arrival_walkable[v]`.

Two activity sets are useful:

| Activity set | Work enabled by an improvement |
|---|---|
| `active_any` | Timetable edges leaving the vertex |
| `active_walkable` | Walking edges leaving the vertex |

If a transit arrival improves `arrival_walkable` but not `arrival_any`, it can still enable a useful walk. If a walk improves `arrival_any`, it can enable a useful train but not another walk.

## Packed graph representation

The device graph should use dense integer IDs and structure-of-arrays storage.

### Node mapping

Create a stable mapping:

```text
H3 UInt64 <-> dense Int32 node ID
```

H3 IDs, stop names, dictionaries, and display metadata remain on the host. Device kernels use only dense IDs.

### Timetable topology

Use outgoing CSR for directed edge groups:

| Array | Element type | Meaning |
|---|---|---|
| `out_ptr` | `Int32` | Length `V + 1`; outgoing edge range for each vertex |
| `edge_to` | `Int32` | Length `E`; destination vertex for each edge group |
| `edge_from` | `Int32` | Optional length `E`; source for edge-centric kernels |
| `schedule_ptr` | `Int32` | Length `E + 1`; profile range for each edge |
| `departure` | `UInt32` | Length `K`; retained departure offsets |
| `arrival` | `UInt32` | Length `K`; retained arrival offsets |

`edge_from` duplicates information implicit in `out_ptr`, but it allows one work item per edge without searching for its source. At the documented graph scale it costs roughly 0.65 MB.

This representation has two compressed levels:

```text
vertex --out_ptr--> edge group --schedule_ptr--> timetable profile
```

It should be implemented as ordinary vectors, not as `SparseMatrixCSC` and not through sparse matrix multiplication.

### Walking topology

Precompute H3 walking connectivity once on the CPU:

| Array | Element type | Meaning |
|---|---|---|
| `walk_ptr` | `Int32` | Length `V + 1`; walking edge range |
| `walk_to` | `Int32` | Length `W`; destination node |
| `walk_duration` | `UInt32` | Length `W`; integer duration |

The current H3 radius is 4. An H3 disk of radius 4 has at most 61 cells including the source, so the unfiltered upper bound is roughly 60 candidate walking edges per vertex. Only graph vertices that exist in the dense mapping should be retained.

Walking distance can remain a host-side `Float32` or `Float64` array used during path reconstruction. It is not needed to calculate earliest arrival.

### Host-only reverse topology

CPU path reconstruction needs incoming edges:

| Array | Meaning |
|---|---|
| `in_ptr` | Incoming timetable edge ranges by destination |
| `in_edge` | Timetable edge IDs |
| `walk_in_ptr` | Incoming walking ranges |
| `walk_in_edge` | Walking edge IDs |

The reverse topology does not need to be uploaded unless a future GPU algorithm requires it.

### Time representation

Use integer time relative to a query epoch.

The current seven-day horizon is 604,800,000 milliseconds, which fits in `UInt32`. Milliseconds preserve the existing walking calculation. Reserve `typemax(UInt32)` as infinity and guard additions against cutoff and overflow.

Thirty-two-bit integer labels are desirable because:

- They halve label bandwidth relative to 64-bit values.
- They avoid uncertain or slow 64-bit atomic support on old integrated GPUs.
- They work on CUDA, ROCm, oneAPI, and Metal more consistently than floating-point atomics.
- Distances and route metadata are not part of the atomic label.

If future horizons exceed the safe millisecond range, prefer a coarser unit such as seconds before adopting 64-bit labels.

## Timetable profile preprocessing

### Dominated connection removal

For a directed edge, a connection is dominated when a later or equal departure arrives no later. Waiting for the later connection is always at least as good under the current model because there are no trip-continuity or transfer-buffer constraints.

For a finite sorted list of `(departure, arrival)` pairs:

1. Sort by departure ascending.
2. Scan from the last departure to the first.
3. Track the minimum arrival seen so far.
4. Retain a connection only when its arrival is strictly less than that minimum.
5. Reverse the retained list.

The retained departures and arrivals are both strictly increasing after duplicate handling. `next_arrival(edge, ready_time)` is then one binary search for the first retained departure greater than or equal to `ready_time`.

This preprocessing fixes the overtaking bug and reduces schedule storage when later services dominate earlier ones.

### Repeating daily schedules

For a period `P`, build a two-period profile:

```text
for each daily connection (d, a):
    include (d, a)
    include (d + P, a + P)
```

Apply dominated-connection removal to this two-period list. For an absolute ready time `t`:

```text
base = floor(t / P) * P
clock = t - base
relative_arrival = profile_lookup(clock)
absolute_arrival = base + relative_arrival
```

The second period is sufficient because the same service one day earlier always dominates its copy on any later day.

Profile construction must preserve arrivals after midnight. A connection leaving before midnight and arriving after midnight needs an arrival offset greater than `P`, not a modulo-day arrival.

### Arithmetic progression compression

The directly relevant GPU paper groups equal-duration connections and compresses regular departure sequences into exact arithmetic progressions `(start, end, interval)`. It then partitions them into departure-time clusters, typically one hour each.

This was the paper's best representation, but it should be a later optimization here because:

- Binary search over a simple flat profile is easier to validate.
- AP construction and lookup add branches and irregular loops.
- The retained profile size is currently unknown.
- The synthetic Elvas to Badajoz minute-frequency service would compress very well, but it may not be representative.
- Irregular real schedules often become singleton AP entries.

Collect these statistics before implementing AP compression:

| Statistic | Decision informed |
|---|---|
| Raw connections per edge | Binary search cost and memory |
| Retained profile entries per edge | Benefit of dominance filtering |
| Distinct durations per edge | Benefit of connection-type grouping |
| AP tuples per edge or cluster | Benefit of AP compression |
| Empty and populated hourly clusters | Benefit of time clustering |

## CPU reference implementation

A packed CPU router is required both for performance comparison and as a correctness oracle.

The recommended reference is time-dependent Dijkstra over the packed graph:

- The preprocessed edge arrival function is FIFO.
- Waiting is allowed.
- Walking durations are nonnegative.
- The two-label state representation is finite and explicit.
- The search can stop when the minimum queued label exceeds the cutoff.

The reference should use the same `out_ptr`, `edge_to`, `schedule_ptr`, timetable profile, and walking arrays as the GPU. It should not use DataFrames or H3 during a query.

Connection Scan is also worth benchmarking as a CPU contender because it performs a contiguous pass over departure-sorted connections. It is not the first reference because integrating the special walking state and repeating timetable is more involved than packed time-dependent Dijkstra.

KernelAbstractions' CPU backend is useful for validating the kernel implementation, but it should not be assumed to be the fastest CPU router. GPU-shaped bulk-synchronous code can have significant overhead on a CPU.

## GPU algorithm candidates

### Candidate A: dense topology-driven relaxation

This is the first recommended GPU implementation and the closest match to the published public-transport GPU algorithm.

For timetable work, launch over `(query, edge)`:

```text
u = edge_from[edge]
if active_any[query, u]:
    ready = arrival_any[query, u]
    candidate = next_arrival(edge, ready)
    atomic_min(arrival_any[query, v], candidate)
    atomic_min(arrival_walkable[query, v], candidate)
```

For walking work, launch over `(query, walk_edge)` or `(query, vertex)`:

```text
if active_walkable[query, u]:
    candidate = arrival_walkable[query, u] + walk_duration
    atomic_min(arrival_any[query, v], candidate)
```

Use separate timetable and walking kernels initially. They have different lookup behavior, and separating them reduces branch divergence.

Advantages:

- Simple flat launches.
- Enough nominal work for a single query.
- No device-side dynamic allocation.
- Matches the strongest directly relevant published design.
- The query dimension naturally supports batching.

Disadvantages:

- Every round inspects all edge groups, even when few sources are active.
- Most work items may exit immediately.
- Timetable binary searches have varying lengths.
- Kernel launch and convergence overhead may dominate on the HD 630.

### Candidate B: dense vertex relaxation over CSR

Launch one work item per `(query, vertex)` and let active work items loop over their outgoing CSR ranges.

Advantages:

- Reads each source activity flag once.
- Scans `V` sources instead of `E` edge groups per round.
- Reuses the same source label across outgoing edges.
- Uses the CSR structure directly.

Disadvantages:

- One active low-degree vertex exposes very little parallel work.
- Degree variation creates work imbalance.
- The first rounds of a single query may leave almost the whole GPU idle.

This is likely attractive for batched origins and should be compared with Candidate A after the first dense kernel is working.

### Candidate C: compact origin-vertex frontier

Maintain a queue of active `(query, vertex, label_kind)` entries and launch only over that queue. This follows the many-source transportation-network research and general GPU frontier processing.

Required machinery includes:

- Current and next frontier buffers.
- An atomic next-frontier count.
- A queued epoch or compare-and-swap marker to suppress duplicates.
- A fixed capacity or overflow strategy.
- Final-label reads when processing the next round so multiple improvements collapse safely.

Advantages:

- Avoids scanning inactive topology.
- Work is proportional to the discovered frontier.
- Batching provides enough active vertices to occupy a small iGPU.

Disadvantages:

- Queue construction adds global atomics.
- Early single-query frontiers can be too small for a GPU.
- Deduplication and overflow handling complicate correctness.
- Irregular memory access remains.

This should only be implemented if dense-kernel profiling shows inactive scans dominate.

### Candidate D: destination gather without atomics

One work item per destination can scan all incoming edges and write one output label into a separate buffer. This avoids write conflicts and atomics.

It is not recommended as the primary algorithm because it scans all incoming topology every round, requires double-buffered labels, and usually performs much more work. It is only a fallback if a target backend cannot execute correct 32-bit Atomix operations.

## Activity and convergence representation

The public-transport GPU paper uses separate `active` and `next_active` arrays. This prevents one work item from clearing a source before another outgoing work item reads it.

Two implementation choices should be benchmarked.

### Double-buffered flags

```text
clear next_active
read active during relaxation
set next_active after a successful atomic improvement
swap active and next_active
```

This is simple but requires clearing buffers each round.

### Epoch arrays

Store the round number in an integer activity array:

```text
active_epoch[v] == current_round
successful update writes current_round + 1
```

Epochs avoid clearing and swapping full activity buffers. They consume four bytes per state and require care around wraparound and atomic visibility.

KernelAbstractions has no portable grid-wide barrier inside a kernel. Convergence should therefore use ordered kernel launches, not a persistent kernel with an assumed global synchronization primitive.

The relevant GPU paper reduced host synchronization by checking its changed flag only every roughly square-root-of-diameter rounds. A portable variant can enqueue a small fixed number of ordered rounds before synchronizing and reading a device change counter. Test chunk sizes such as 1, 2, 4, and 8. Extra empty rounds may be cheaper than one host-device synchronization per round.

## Atomics and portability

Multiple source edges can improve the same destination, so scatter-based kernels need an atomic minimum.

Use Atomix through KernelAbstractions with `UInt32` labels. The desired operation is conceptually:

```text
old = atomic_min(label, candidate)
if candidate < old:
    activate destination for the next round
```

The implementation must distinguish an actual winning update from a failed candidate before incrementing frontier counts or setting detailed activity state.

Do not place distance, parent, action, or a Julia object into the atomic label. Compound state would require 64-bit packing or a lock protocol and would reduce portability.

Backend support must be tested. KernelAbstractions documents Atomix integration, but integer atomic behavior and supported memory orderings ultimately depend on each backend and driver.

## Batching layout

For a batch of `B` queries, store the query index as the fastest-moving dimension:

```text
flat_index = query + B * vertex
```

This makes adjacent work items operating on the same vertex or edge across queries read contiguous labels.

Approximate query-state storage with `V = 55,000`:

| Storage | Bytes per query | Bytes for B=64 |
|---|---:|---:|
| Two `UInt32` arrival labels | 440,000 | 28.2 MB |
| Two `UInt32` activity epochs | 440,000 | 28.2 MB |
| Labels plus epochs | 880,000 | 56.3 MB |

Using byte flags instead of epochs reduces workspace size but adds clearing work. Batch size should be tuned against memory capacity, cache behavior, and query throughput. Likely initial values are 8, 32, 64, and 128 rather than placing all 700 origins in one workspace.

The graph is shared by every query and should remain resident for the complete batch run.

## CPU path reconstruction

The GPU returns only `arrival_any` and `arrival_walkable`.

To reconstruct a selected target:

1. Start from the chosen target's `arrival_any` label.
2. If `arrival_any[v] < arrival_walkable[v]`, find a walking predecessor `u` satisfying `arrival_walkable[u] + duration == arrival_any[v]`.
3. If `arrival_any[v] == arrival_walkable[v]`, prefer the walkable state for deterministic reconstruction.
4. For a walkable state at `v`, find an incoming timetable edge from `u` whose profile lookup at `arrival_any[u]` equals `arrival_walkable[v]`.
5. Break multiple valid choices by stable edge ID.
6. Continue until the source and initial time are reached.

Positive travel durations make time decrease during backwards traversal, preventing cycles in a reconstructed earliest path.

After selecting edges, attach host metadata:

- H3 IDs.
- Stop names or action labels.
- Transit geodesic distances.
- Walking distances.
- Departure and arrival display times.

This approach avoids a second full routing run and avoids inconsistent device parent records.

## Relevant published work

### GPU earliest arrival in public transport

Haryan, Ramakrishna, Nasre, and Reddy propose a topology-driven GPU algorithm specifically for one-to-all earliest arrival in timetable networks.

Their incremental variants map work to:

| Variant | GPU work unit |
|---|---|
| Connection | One connection per thread |
| Connection type | One `(u, v, duration)` group per thread |
| Connection type plus AP | One compressed duration group per thread |
| Cluster-AP | One group with departure-time cluster pruning per thread |
| Edge | One edge per thread |
| Warp | One CUDA warp per edge |

Their packed representation is inspired by CSR:

- A connection-type array stores endpoints and duration.
- A cluster-offset array identifies timetable clusters.
- An AP array stores exact arithmetic-progression departure encodings.

Their correctness machinery includes:

- Integer earliest-arrival arrays.
- Separate current and next active arrays.
- Atomic minimum for competing destination writes.
- Iteration until labels stop changing.

Their best algorithm is Cluster-AP with synthetic sub-trip shortcuts. Reported data includes:

| Dataset | Vertices | Edges | Connections | Connection types |
|---|---:|---:|---:|---:|
| London | 20.8k | 25.5k | 14.06m | 140.7k |
| Switzerland | 29.9k | 74.1k | 9.26m | 102.6k |
| Sweden | 45.7k | 101.9k | 6.56m | 158.3k |

On an NVIDIA GTX 1080 Ti, reported query times for Cluster-AP plus shortcuts ranged from about 0.09 ms to 3.20 ms across their datasets. Reported speedups ranged from 2.29x to 59.09x over CPU Connection Scan and 1.63x to 12.48x over a prior GPU algorithm. Cluster-AP processed about 3.35 percent of raw connections on average.

Important limitations for this project:

- The GPU was far more powerful than an HD 630.
- CUDA warp-size assumptions do not transfer to Intel or AMD.
- The datasets did not include the current H3 walking state.
- Preprocessing time and memory were not the focus of the reported query timings.
- Their graph contains explicit GTFS trips, enabling sub-trip shortcuts.
- The current query discards trip identity, so equivalent shortcuts cannot be built without changing ingestion.

### Many-source transportation routing

Heywood et al. study static shortest paths on low-density, high-diameter transportation graphs. Their key observation is directly relevant: smaller transportation networks do not expose enough single-source work to occupy a modern GPU.

They introduce an Origin-Vertex Frontier that combines active work from multiple sources. Their implementation reports up to 7.8x over a multicore CPU implementation.

The exact algorithm is for static road weights rather than timetables, so its relaxation function cannot be copied directly. Its batching strategy is nevertheless a strong fit for the 700-origin loop in `plots/longest_journey.jl`.

### General GPU shortest paths and frontiers

Davidson et al. compare workfront, near-far, and bucketed GPU SSSP methods. Their central result is that saving relaxation work must be balanced against the overhead of organizing frontiers and buckets.

Gunrock similarly organizes graph processing around advance, filter, and compute operations over vertex or edge frontiers. These results support an incremental strategy:

```text
start with a dense, simple, correct kernel
measure inactive scanning
add compact frontiers only if saved work exceeds queue overhead
```

### CSA and RAPTOR

Connection Scan Algorithm is an important CPU baseline because it uses contiguous scans over departure-sorted connections and supports earliest-arrival and profile queries.

RAPTOR is route-based rather than graph-edge-based. It scans each public-transport route at most once per transfer round and parallelizes naturally over routes. It becomes attractive if this project moves to a real timetable with trip continuity, transfer counts, and proper stop-to-route structure.

RAPTOR is not the recommended first GPU rewrite because the current input has already been collapsed to H3 edge schedules and the selected query does not retain the route/trip structures RAPTOR needs.

### Selective-check GPU algorithm

Maurya and Anand published a second GPU earliest-arrival approach based on edge coloring, arrival-sorted incoming edges, and checks for time-respecting paths. Its abstract reports average speedups of 6.45x over serial Connection Scan and 2.77x over the earlier edge-version GPU algorithm.

The algorithm is reported to perform best when all temporal paths are time-respecting. It is not the first recommendation here because its favorable structural assumption has not been measured on this graph, while the Haryan topology-driven representation maps directly onto the current edge-group data.

## Intel HD 630 hardware reality

Intel HD Graphics 630 is a Gen9/Kaby Lake GT2 integrated GPU with approximately 24 execution units. An execution unit runs SIMD instructions across hardware threads; it is not equivalent to 24 CPU cores or thousands of independently scheduled CPU-style threads.

Consequences for this router:

- There is substantially less compute capacity than the discrete GPUs used in the cited papers.
- GPU and CPU share system memory bandwidth.
- CPU and GPU may compete for package power and thermal budget.
- Branch divergence and random accesses remain expensive.
- Kernel launch and synchronization overhead matter more for small frontiers.
- Batches are likely to use the hardware more effectively than one source.

The graph should remain in device-accessible memory across all queries. Per-round host copies of labels or frontiers should be avoided.

## Julia and backend findings

### Repository environment

The repository's `plots/Manifest.toml` records Julia 1.11.5. This satisfies current oneAPI.jl's Julia 1.10 or newer requirement.

No direct dependency currently exists for:

- KernelAbstractions.jl.
- Atomix.jl.
- oneAPI.jl.
- CUDA.jl.
- AMDGPU.jl.
- Metal.jl.
- OpenCL.jl.

### KernelAbstractions

KernelAbstractions' documented primary GPU backends are:

- NVIDIA through CUDA.jl.
- AMD through AMDGPU.jl.
- Intel through oneAPI.jl.
- Apple through Metal.jl.

It provides a CPU backend for development and testing. Backends provide their own device array types, while the kernel body can remain shared.

Kernel launches are ordered and asynchronous. Correct timing requires backend synchronization. KernelAbstractions provides backend allocation, copying, indexing, workgroup-local memory, synchronization within a workgroup, and Atomix integration.

Portable subgroup or warp-level operations are not sufficiently settled for this design. The first implementation should use only global work-item indexing, ordinary loops, and integer atomics.

### oneAPI.jl and Gen9

Current oneAPI.jl documentation states:

- Linux is the supported platform.
- Windows support is experimental through WSL2.
- Intel Gen9 graphics or newer are supported.
- KernelAbstractions kernels run through `oneAPIBackend`.

Intel's current compute-runtime documentation adds an important qualification:

- Starting with runtime release 24.35, regular packages support Gen12 and later.
- Gen8, Gen9, and Gen11 support moved to packages with a `legacy1` suffix.
- Kaby Lake has OpenCL 3.0 and Level Zero 1.5 in the legacy package.
- Kaby Lake is not listed as supporting WSL in that legacy table.
- The legacy branch receives critical fixes but no new features.

At the research date, upstream oneAPI.jl's project version was 2.8.0 and pinned `NEO_jll` 26.18.38308. The Julia Yggdrasil build recipe for that artifact uses the default current Intel runtime build and does not visibly enable the separate legacy target.

Therefore the documentation-level statement "Gen9 or newer" is not enough to prove that a fresh oneAPI.jl installation will detect an HD 630 in 2026.

The likely Linux fallback is:

1. Install Intel's mutually compatible legacy1 Level Zero, OpenCL, IGC, and memory-management packages.
2. Use oneAPI.jl's documented local system-library configuration.
3. Restart Julia and verify the actual driver and device in `oneAPI.versioninfo()`.
4. Run kernel and atomic smoke tests before routing work.

This path must be tested on the target computer. It should not be treated as guaranteed from documentation alone.

### OpenCL fallback

OpenCL.jl added native Julia kernels and a KernelAbstractions backend in version 0.10. Intel is the best-supported GPU vendor for its SPIR-V path.

It is a possible alternative because Intel's Gen9 legacy runtime supports OpenCL 3.0. However:

- OpenCL.jl documents that it is undergoing major changes.
- Native kernels require the driver to accept SPIR-V.
- KernelAbstractions and Atomix integration for OpenCL has had active extension work.
- Correct 32-bit atomic minimum and compare-and-swap must be demonstrated on the exact driver.

Use OpenCL only as an experimentally validated fallback, not the default plan.

## Target-machine compatibility gate

Run this gate before substantial GPU router implementation.

| Test | Required result |
|---|---|
| Backend version information | HD 630 is listed as an available device |
| Vector-add KernelAbstractions kernel | Compiles, runs, and returns exact results |
| `UInt32` atomic minimum under contention | Correct minimum on repeated runs |
| Compare-and-swap under contention | Correct winner and old value |
| Repeated ordered kernel launches | No lost updates or driver failures |
| Graph-sized allocation | Fits with adequate headroom |
| Empty/small kernel timing | Launch overhead is recorded |
| Sustained kernel loop | No thermal, reset, or watchdog failures |

If Level Zero cannot pass this gate, test OpenCL. If neither passes, retain the packed CPU implementation or target a newer Intel Gen12 or later iGPU.

## Testing plan

There are currently no router tests. Correctness tests must precede performance kernels.

### Hand-built fixtures

| Fixture | Expected behavior |
|---|---|
| One direct train | Basic earliest arrival |
| Disconnected destination | Infinity label |
| Departure exactly at ready time | Connection is allowed |
| Arrival exactly at cutoff | Connection is allowed |
| Arrival after cutoff | Connection is rejected |
| No departure left today | Wrap to the next period |
| Late slow train versus tomorrow's early fast train | Choose minimum arrival |
| Earlier slow departure overtaken by later fast departure | Choose later departure |
| Train then walk | Allowed |
| Walk then train | Allowed |
| Walk then walk | Rejected |
| Earlier walked arrival and later train arrival | Later train may still enable a walk |
| Equal-arrival alternatives | Stable deterministic reconstruction |
| Positive-time cycle | Converges without corrupting labels |
| Seven-day repeated service | Correct day offsets |
| Synthetic minute-frequency edge | Correct profile and wrap behavior |

### Differential tests

For every implementation and backend:

- Compare every `arrival_any` label with packed CPU Dijkstra.
- Compare every `arrival_walkable` label with packed CPU Dijkstra.
- Test random origins and departure times.
- Test multiple cutoffs.
- Test single-query and batched layouts.
- Validate reconstructed paths by replaying every edge and checking the final label.
- Repeat GPU runs to detect nondeterministic label or atomic failures.

Equal predecessors may differ, but arrival labels must match exactly. Stable reconstruction rules should make selected CPU paths deterministic.

### Invariants

Assert these invariants in debug and test builds:

```text
arrival_any[v] <= arrival_walkable[v]
all finite labels <= cutoff
all timetable profile departures are increasing
all timetable profile arrivals are increasing
all CSR offsets are monotonic and end at the array length
all dense destinations are in 1:V
every reconstructed step strictly advances time
```

## Data audit before implementation

Export or query a reproducible representative graph and record:

| Metric | Why it matters |
|---|---|
| Vertex count | Label and activity memory |
| Directed edge-group count | Dense-kernel work per round |
| Raw timetable connection count | Device graph memory |
| Retained profile count | Dominance-filter effectiveness |
| Walking edge count | Walking-kernel work and memory |
| Outdegree histogram | Vertex-kernel load balance |
| Timetable length histogram | Binary-search divergence |
| Distinct durations per edge | Connection-type value |
| AP compression ratio | Whether Cluster-AP is worthwhile |
| Frontier size per round | Dense versus compact frontier decision |
| Number of convergence rounds | Kernel-launch sensitivity |
| Successful updates per round | Atomic contention and wasted work |
| Maximum simultaneous destination contention | Atomic hot spots |

The large `data/edgelist.arrow` file is about 2.9 GB and belongs to an older stop-based router. The longest-journey experiment instead queries a dated ClickHouse table. A compact, reproducible Arrow fixture for the selected train subset should be created for development and target-machine benchmarking.

## Benchmark plan

### Baselines

Benchmark all of these separately:

| Baseline | Purpose |
|---|---|
| Current `find_earliest_arrivals` | Measures total improvement from the sketch |
| Packed CPU Dijkstra | Real CPU competitor and correctness oracle |
| Optional CPU Connection Scan | Cache-friendly transit baseline |
| KernelAbstractions CPU backend | Kernel correctness and portability |
| GPU dense edge kernel | Primary GPU candidate |
| GPU dense vertex kernel | CSR work-mapping comparison |
| GPU compact frontier | Only after dense profiling justifies it |

### Workloads

Use representative origins rather than only the fastest or furthest examples.

| Dimension | Values |
|---|---|
| Batch size | 1, 8, 32, 64, 128, then chunked 700 |
| Horizon | 3 hours, 12 hours, 24 hours, 7 days |
| Departure time | Morning peak, midday, evening, overnight |
| Origin type | Major hub, regional station, sparse endpoint, geographically isolated cell |
| Walking | Disabled and enabled |

### Metrics

Record:

- Median and p95 single-query latency.
- Total queries per second for each batch size.
- End-to-end batch wall time.
- Preprocessing time.
- Graph upload time.
- Kernel compilation time.
- Warm steady-state query time.
- Number of rounds.
- Work items launched.
- Timetable lookups performed.
- Successful atomic updates.
- Peak workspace memory.
- CPU utilization while the GPU is active.
- Target power and thermal behavior when available.

### Timing protocol

GPU launches are asynchronous. Every measured region must synchronize the backend before stopping the timer.

Keep these measurements separate:

```text
first call including compilation
graph packing and upload
warm kernel-only routing
label download
CPU path reconstruction
complete end-to-end request
```

Warm up each kernel and batch shape before collecting samples. Keep the graph resident between queries.

### Success criteria

The GPU is successful for a workload only when it beats packed CPU Dijkstra under the same semantics.

Recommended dispatch behavior after benchmarking:

| Result | Runtime policy |
|---|---|
| GPU wins for B=1 | Use GPU for interactive and batch work |
| CPU wins for B=1, GPU wins for batches | CPU single queries, GPU queued batches |
| CPU wins for all tested batches | Keep packed CPU router and stop GPU optimization |
| Backend is unstable | Do not expose GPU routing on that target |

## Implementation phases

### Phase 0: semantics and reproducibility

Deliverables:

- A written correctness contract for repeating schedules, cutoff, waiting, walking, and ties.
- A small committed router fixture containing adversarial cases.
- A representative compact train-network snapshot or repeatable export command.
- Recorded graph statistics from the data-audit table.

Exit criteria:

- Expected labels are known for every hand-built fixture.
- The source dataset can be reproduced without relying on an interactive plot session.

### Phase 1: packed host graph

Deliverables:

- Dense H3 node mapping.
- Outgoing timetable CSR.
- Nondominated timetable profiles.
- Precomputed walking CSR.
- Host reverse CSR and route metadata.
- Serialization or Arrow storage for the packed input where useful.

Exit criteria:

- No DataFrame, H3, dictionary, `DateTime`, or string access occurs inside routing.
- Packed profile lookup passes overtaking and periodic-wrap tests.
- Memory footprint is recorded.

### Phase 2: packed CPU reference

Deliverables:

- Time-dependent Dijkstra using `arrival_any` and `arrival_walkable`.
- Deterministic CPU reconstruction from final labels.
- Differential tests against the hand-built fixtures.
- Benchmarks against the current router.

Exit criteria:

- All fixtures pass.
- Reconstructed paths replay to the exact final arrival.
- This implementation becomes the performance baseline.

### Phase 3: portable dense kernel

Deliverables:

- KernelAbstractions and Atomix dependencies.
- Backend-neutral graph upload/allocation helpers.
- Dense timetable relaxation kernel.
- Dense walking relaxation kernel.
- Activity and convergence handling.
- Batch layout with query-fast indexing.
- CPU-backend differential tests.

Exit criteria:

- Every label exactly matches packed CPU Dijkstra on the CPU backend.
- No backend-specific subgroup or warp code exists in the kernel.
- No route metadata is stored in device labels.

### Phase 4: target compatibility and first GPU benchmark

Deliverables:

- Completed HD 630 compatibility gate.
- Recorded driver, runtime, Julia, oneAPI/OpenCL, and KernelAbstractions versions.
- Dense-kernel correctness results on the target.
- Batch-size and workgroup-size benchmark matrix.

Exit criteria:

- Integer atomics are repeatedly correct.
- The router completes representative seven-day queries without driver failure.
- The comparison against packed CPU is available.

### Phase 5: evidence-driven optimization

Apply optimizations in this order, stopping when they do not improve end-to-end time:

1. Tune batch size and workgroup size.
2. Tune convergence-check chunk size.
3. Compare edge-centric and vertex-centric dense kernels.
4. Reduce activity-buffer clearing through epochs if clearing is measurable.
5. Add a compact origin-vertex frontier if inactive topology scans dominate.
6. Add departure-time clusters if binary searches dominate.
7. Add AP compression if it substantially reduces graph bytes or lookup work.
8. Restore trip identity and investigate sub-trip shortcuts only if temporal diameter dominates.
9. Consider RAPTOR only if the project adopts real route/trip semantics.

Exit criteria:

- Each retained optimization has an isolated benchmark showing an end-to-end improvement.
- Cross-backend correctness remains unchanged.

### Phase 6: plot and service integration

Deliverables:

- Router API used by plotting code without exposing DataFrames to the inner algorithm.
- Automatic CPU/GPU dispatch based on backend availability and workload size.
- Selected-route CPU reconstruction.
- Existing Arrow and GeoJSON output adapted to the new result format.
- User-facing fallback when a GPU backend is unavailable.

Exit criteria:

- The longest-journey origin loop uses batched routing where beneficial.
- Interactive single-origin routing selects the measured fastest backend.
- Plot output is reproduced from tested router results.

## Open risks and decisions

| Risk or decision | Current recommendation |
|---|---|
| Exact target OS | Prefer Linux; HD 630 WSL support is not documented by Intel's legacy table |
| Bundled oneAPI support for Gen9 | Do not assume it; run the compatibility gate |
| Legacy runtime compatibility with current oneAPI.jl | Test system-library configuration on target |
| OpenCL atomic support | Treat as experimental until contention tests pass |
| Single-query GPU benefit | Unknown; expect packed CPU to be competitive |
| Raw timetable size | Measure before choosing compression |
| Equal-arrival distance objective | Use deterministic path first; add secondary CPU optimization only if required |
| Fantasy versus real timetable | Implement current repeating semantics first and keep the packed format extensible |
| Walking approximation | Preserve current radius and speed initially, but precompute it |
| Trip shortcuts | Not possible from the selected columns without retaining trip structure |
| Device memory | Tune batch chunks after measuring `K`, `W`, and target allocation limits |
| Subgroup optimization | Avoid in the portable first implementation |
| iGPU memory contention | Keep CPU work out of the routing hot loop while GPU kernels run |

## Immediate next steps

1. Generate the res5 rail snapshot using `router/export.sql`, recording the source table and clock timezone.
2. Audit raw/retained connection counts, node and edge counts, self-edges, invalid durations, and memory footprint.
3. Compare representative queries across packed Dijkstra, KA CPU, and iGPU, separating compilation,
   preprocessing/upload, warm routing, label download, and complete HTTP/Arrow response time.
4. Use H3-MON's res5 `reachable.json` example to check click/pan request and colour-scale
   ergonomics; departure and budget currently remain fixed in the metadata URLs.
5. Run graph-sized allocation and sustained-load tests before relying on GPU service stability.
6. Revisit fine-resolution routing and two-state walking only if the coarse transfer approximation
   proves insufficient. Optimize batches/frontiers only after profiling the real graph.

## Sources

1. Chirayu Anant Haryan, G. Ramakrishna, Rupesh Nasre, and Allam Dinesh Reddy, [GPU Algorithm for Earliest Arrival Time Problem in Public Transport Networks](https://arxiv.org/abs/1912.00966), 2019/2020.
2. Peter Heywood et al., [A data-parallel many-source shortest-path algorithm to accelerate macroscopic transport network assignment](https://eprints.whiterose.ac.uk/id/eprint/149946/), Transportation Research Part C, 2019.
3. Andrew Davidson, Sean Baxter, Michael Garland, and John D. Owens, [Work-Efficient Parallel GPU Methods for Single-Source Shortest Paths](https://mgarland.org/papers/2014/sssp/), IPDPS 2014.
4. Yangzihao Wang et al., [Gunrock: A High-Performance Graph Processing Library on the GPU](https://arxiv.org/abs/1501.05387), PPoPP 2016.
5. Julian Dibbelt, Thomas Pajor, Ben Strasser, and Dorothea Wagner, [Connection Scan Algorithm](https://arxiv.org/abs/1703.05997), 2017.
6. Daniel Delling, Thomas Pajor, and Renato Werneck, [Round-Based Public Transit Routing](https://www.microsoft.com/en-us/research/publication/round-based-public-transit-routing/), ALENEX 2012.
7. Sunil Kumar Maurya and Anshu S. Anand, [A Novel GPU-Based Approach to Exploit Time-Respectingness in Public Transport Networks for Efficient Computation of Earliest Arrival Time](https://doi.org/10.1109/ACCESS.2022.3192443), IEEE Access 2022.
8. [KernelAbstractions.jl documentation](https://juliagpu.github.io/KernelAbstractions.jl/stable/).
9. [KernelAbstractions Atomix example](https://juliagpu.github.io/KernelAbstractions.jl/stable/examples/atomix/).
10. [KernelAbstractions supported backends](https://github.com/JuliaGPU/KernelAbstractions.jl).
11. [oneAPI.jl documentation](https://juliagpu.github.io/oneAPI.jl/stable/).
12. [oneAPI.jl installation and system-library configuration](https://juliagpu.github.io/oneAPI.jl/stable/installation/).
13. Intel, [Compute Runtime legacy platform support](https://github.com/intel/compute-runtime/blob/master/documentation/LEGACY_PLATFORMS.md).
14. Intel, [Graphics Compute Runtime](https://github.com/intel/compute-runtime).
15. JuliaGPU, [OpenCL.jl 0.10: Now with native Julia kernels](https://juliagpu.org/post/2025-01-13-opencl_0.10/).
16. [OpenCL.jl documentation](https://juliagpu.github.io/OpenCL.jl/stable/).

All package and runtime observations are time-sensitive and reflect upstream documentation and source inspected on 2026-08-08.
