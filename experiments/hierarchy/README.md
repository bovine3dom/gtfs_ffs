# Fine-Access Hierarchy

This is an experimental CPU population router. It does not change production
defaults, the server, input files, or the public router schema.
See [results.md](results.md) for measurements and the trade-off.
The [res8 access control](access-results.md) measures preprocessing without extra
coarsening. It did not show a material gain for the main many-origin windows.
The [boarding experiment](boarding-results.md) adds estimated walking time between
fine arrival and boarding cells. It has a separate API and permits underestimation.
The free-transfer model below remains the default for `route_hierarchy`.
The historical [batched boarding follow-up](boarding-batched.md) shares work
across up to 64 origins and retains the scalar model.
See [global-router-results.md](global-router-results.md) for the production
global engine and full-network query gate. It prepares no regional cores.
The [regional full-network run](full-network-results.md) is historical evidence.

## Reference Model

The reference is the current res8 H3 model, not ground truth. That model already
approximates transit and walking. A res7 or res6 core adds another approximation.
Do not interpret agreement with res8 as agreement with actual journeys.
`core_resolution=8` retains the fine core and uses the same access preprocessing.
It is a model-equivalent option, not an additional coarse level.

Each prepared fine origin has a private virtual source. Preparation enumerates
its fine initial walks, including a zero-time start at its own network node.
For a fine leg with departure `d`, arrival `a`, and access walk `w`, the source
profile has ready threshold `(d - w) mod PERIOD` and duration `a - d + w`.
The core becomes reachable only after that first transit leg. Two origins in
one parent can thus catch different first services and return different values.

The core merges fine nodes by H3 parent. All fine edges, including self-edges,
contribute to the core transit profiles. Both retained daily copies are normalized
before the earliest-arrival envelope is built. A transit arrival updates both
transit-ready state A and walk-eligible state E. A walk updates A only.

Core walking edges use the minimum duration of the original fine edges. They do
not use distances between coarse cell centres. Core E output rows contain the
original fine population destinations, with minimum fine egress duration per
destination. Each represented fine child also has a zero-duration output entry.
No parent population total replaces those destinations.

The added optimism comes from free transfers between represented children of a
parent, minimum projected walks, and access to egress from any represented child.
Initial walking and first boarding do not use those free transfers. The tests
check for lost reference destinations. These checks are not a proof for all data.

All private graph prefix weights are zero. True population weights are Float64
values in a unique fine-cell suffix. Direct initial walks use the same suffix,
so repeated egress cannot count a cell twice. Exclusion removes only the selected
fine origin, before sums. The private graph has mixed H3 metadata; at res8 it can
repeat H3 metadata for a core node and a virtual source. Do not pass it to public
graph or walking APIs. Only the production population kernels use its integer IDs.

## API

Run from the repository root with the `router` Julia project:

```julia
include("router/src/Reachability.jl")
include("experiments/hierarchy/Hierarchy.jl")
using .Reachability, .HierarchyResearch
using H3

fine = pack_graph("data/austria_adjacent_res8.arrow") # No shuttle.
population = load_population("data/kontur_h3.arrow")
walking = prepare_walking(WalkingIndex(fine); max_walk_ms=3_600_000)
centre = UInt64(0x881e15b467fffff)
origins = H3.API.gridDisk(centre, 19)
index = prepare_hierarchy(fine, population, origins;
    core_resolution=8, max_walk_ms=3_600_000, walking_index=walking)

result = route_hierarchy(index, H3.API.gridDisk(centre, 18), 28_800_000, 10_800_000;
    window_ms=86_400_000, step_ms=900_000, window_mode=:mean_intersection)
```

This example keeps the res8 model. It is not a speed recommendation: the measured
window gains were only 1.007x and 1.026x, with extra index storage. Use the current
production router for the measured window workload when no extra coarsening is
wanted. Set `core_resolution=7` or `6` only to select the documented approximate
trade-off. Neither choice is a production default.

`result.h3` contains sorted unique fine origins. `result.value` contains Float64
population values, including zeros. Work counts, worker count, backend, and core
resolution are also returned. `index.stats` contains preparation times and counts.

Preparation is specific to an origin set and one positive walking limit below
one day. It does not depend on departure, travel budget, window, mode, or exclusion.
Any subset of those origins can use the same index. A moved map centre is valid
if its complete requested disk is within the prepared set. This is a regional
profile index, not a universal index or a cache of query results.

An unprepared origin causes an error. A different positive walking limit causes
an error. Zero walking or zero budget uses the current fine router and returns
`backend=:fine_fallback`. Short positive budgets still use the hierarchy; direct
walks are clipped to the budget. Preparation rejects extreme transit durations
unless there is enough UInt32 headroom for every shifted two-day profile. Use
the fine router for those inputs. Query time validation reserves UInt32 INF.

All six population modes and `exclude_origin_population` are supported. The
intersection modes count destinations reached in every sample. The union modes
count destinations reached in any sample. `reachable_union` weights population
by the fraction of samples that reach each destination. These are population
coverage reductions, not elapsed-time statistics.

Treat the graph, population, walking geometry, and hierarchy as read-only after
preparation. Normal population preparation caches remain in use. The experiment
owns its coarse schedule-hint cache, so discarded regional cores are not retained
by the input population object. Fine geometry, weights, and hints remain shared.
A source adapter for the experimental index calls the unchanged
production scheduler, source classifier, packed/range kernels, array queue,
label pruner, and schedule hints. Each query has fresh workspaces. There is no
per-origin result cache, copied router, HTTP endpoint, or UI integration.

## Verification

```sh
julia --threads=1 --project=router experiments/hierarchy/test.jl
julia --threads=8 --project=router experiments/hierarchy/test.jl
julia --threads=8 --project=router experiments/hierarchy/benchmark.jl /tmp/opencode/new-hierarchy-run
julia --threads=8 --project=router experiments/hierarchy/benchmark-access.jl /tmp/opencode/new-access-run
```

For `benchmark.jl`, add `--check` after the output path to run only the two
1,027-origin intersection cases and the full-graph point checks.
`benchmark-access.jl` compares production with res8 preprocessing. It uses three
measured pairs for intersection and short controls, and one for reachable union.
Its separate large-case gate requires a 1.2x main-window gain at 1,027 origins.

Both harnesses require a new output directory with an existing parent. They refuse
to overwrite an existing path. Each loads the approved Austria graph once, prepares
one-hour fine walking once, and shares the Float64 population input. Both record
source hashes, preparation statistics, raw trials, and errors without result caches.
The res7/res6 harness also records point destination checks.

In the res7/res6 harness, each normal city cohort has 1,141 origins. The same index
serves disks with 127 and 1,027 origins, moved centres, and new query times. Each case
has one warm round and three measured interleaved rounds. Query source assembly
and allocation are timed. Index preparation is separate. The 9,919-origin Vienna
case runs only if a 1,027-origin candidate is at least twice as fast and at least
6 GiB of host memory is available. There is no query timeout in the harness.

Both harnesses are Linux-specific because they read process memory and CPU counters
from `/proc`. Run only one large benchmark process. Do not stop the user server.
Host activity is recorded, but the host is not isolated.
