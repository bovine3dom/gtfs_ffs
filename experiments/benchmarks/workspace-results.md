# Population Workspace Pool

## Results

Compatible requests reuse ordinary arrays in one pool. They do not reuse population results.
Warm requests allocated more than 99.7% fewer bytes than requests with private workspaces.
All warm timed requests reused eight workers.

The table shows medians. Round zero is excluded. GB and MB are decimal units.
CPU time includes all threads in the benchmark process.

| Case | Pairs | Private Wall s | Pooled Wall s | Private CPU s | Pooled CPU s | Private Allocation GB | Pooled Allocation MB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Short, 331 origins, 217 samples | 3 | 1.143 | 0.572 | 3.321 | 2.316 | 2.239 | 1.664 |
| Paris, 1,027 origins, 96 samples, 1 h budget | 2 | 7.771 | 6.953 | 36.232 | 33.463 | 5.028 | 3.885 |
| Paris, 1,027 origins, 96 samples, 3 h budget | 1 | 32.212 | 31.166 | 143.213 | 141.956 | 5.076 | 12.316 |

The initial pooled short request allocated 2.285 GB and took 2.211 s.
Its compilation time was 0.931 s. The first warm short request included another 0.055 s of compilation.
The initial pooled Paris request allocated 5.078 GB and took 26.088 s, with no measured compilation.
The initial requests had no reused workers. Their measurements include buffer construction and routing.

Warm pooled requests had no measured GC time.
Private warm requests had 0.002-0.009 s of GC time.
The main allocation reduction does not remove traversal work.

Background load was substantial: the timed calls measured 2.19-4.25 busy logical CPUs outside the benchmark process.
Wall times are observations under that load, not an isolated speed guarantee.
The three-hour warm case has only one pair.

## Memory

| Layout | Fixed Array Estimate, Bytes | Final Retained Measure, Bytes |
| --- | ---: | ---: |
| Short | 2,236,480,128 | 2,332,650,856 |
| Paris | 5,001,885,312 | 5,136,494,992 |

The retained measure uses `Base.summarysize` on the worker vector after the request.
It includes reserved queue, dictionary, and vector capacity. It also includes referenced schedule hints once.
Those hints are shared with the resident population index.
Julia 1.12.7 reported 8,000,040 bytes for an empty `Vector{Int}` after `sizehint!(v, 1_000_000)`.
Its `sizeof(v)` was zero. The installed `base/summarysize.jl` confirms that pointer-free memory does not require an element scan.

The largest sampled request RSS was 27.53 GB. This is not a measured peak.
Private requests ran while the pool retained its previous buffers. RSS thus includes both variants and allocation history.
The preparation-complete RSS was 21.16 GB, before an explicit GC.
These values are not server-only memory requirements.

The default pool budget is 8 GiB. Before allocation, the pool reduces the worker count if one worker fits but all requested workers do not.
The fixed estimate includes settled masks, destination masks, coverage masks, heads, range pending masks, queued times, and arrival labels.
It excludes dynamic containers, input graphs, source preparation, and returned data.
After a successful request, a retained measure above the budget causes the pool to discard its buffers.
Dynamic memory can exceed the budget during a request.

Before H3 disk allocation, a separate check estimates 16 bytes per disk-capacity cell for origins and output values.
This is a conservative estimate near pentagons. It is not added to the workspace estimate.
Cache keys and other request data are not included. Neither check is an aggregate memory or RSS limit.
The reference fallback discards retained workspaces, but its memory use is not budget-limited.
The pool does not control memory for loaded graphs, Arrow output, or slow clients.

## Correctness

All eight timed private/pooled pairs had identical H3 IDs, zero masks, values, and expansion counts.
The separate checks covered six modes with and without origin exclusion at both locations.
Each check used seven origins on the same fine graph.
All H3 IDs, zero masks, and expansion counts matched.
The maximum population difference was `3.725290298461914e-9`, below the `1e-6` absolute tolerance.
The two nonzero differences occurred in Paris `reachable_union` results.

The focused pool tests passed with one thread (393 checks) and eight threads (935 checks).
The existing queue tests passed with both thread counts (13 checks each).
Tests cover layout replacement, destination growth, worker reduction, cache hits, reference fallback, independent results, and finite-total errors.
A channel barrier holds surviving workers after a worker fails. The next pooled request cannot start until those workers finish.
The next request uses fresh workspaces after the failure.
Full integration tests are separate from this benchmark.

## Method

Run:

```sh
julia --project=router --threads=8 --heap-size-hint=32G experiments/benchmarks/workspace-benchmark.jl
```

- Use Julia 1.12.7 on an Intel Xeon E3-1275 v6 at 3.80 GHz, with eight logical CPUs and 62.6 GiB RAM.
- Load one `everything_res8.arrow` graph. Apply invalid-duration filtering and the standard Elvas-Badajoz repair.
- Prepare 900,197 nodes, 2,984,575 edges, and 316,136,664 two-day profile entries.
- Load 32,957,699 population cells, 105,273,069 geographic walks, and 33,030,524 network walks.
- Use one resident graph, index, population, and schedule-hint matrix for both variants.
- Call `route_population` directly. Do not use HTTP or the result cache.
- Run GC before each timed call. Keep GC time within each call in the measurement.
- Record the initial pair, then alternate variant order for three warm pairs.
- Advance departure by one minute per pair. For warm Paris pairs, alternate one-hour and three-hour budgets.
- Keep the short query's half-hour budget, six-minute walk limit, thirteen-hour window, and 216-second sample step.
- Keep the Paris query's one-hour walk limit, 96-minute window, and one-minute sample step.
- Use origin `881fa44181fffff`, radius 10, for the short case. Use central Paris, radius 18, for the dense case.

The heap-size hint is a GC setting, not a limit. No server was stopped for this run.
The measured packed-source SHA-256 was `ae4eb7ff1836e9d9bd65fa65ac5ec80808b2e5e913ef509c1d04244ee6148a5d`.
A later edit simplified public forwarding. It moved default walking-index construction under the pool lock.
These timed calls supply a walking index. The workspace and routing code did not change.

See [workspace-trials.csv](workspace-trials.csv), [workspace-parity.csv](workspace-parity.csv), and [workspace-benchmark.log](workspace-benchmark.log).
