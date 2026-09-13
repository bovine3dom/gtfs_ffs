# Hierarchy Decision

## Decision

The explicit coarse/fine router experiment is closed. Its runtime code,
prototypes, tests, and benchmark scripts have been removed. Use normal routing
at the origin resolution. Automatic graph derivation from resolution 8 to
7, 6, and 5 remains available. It is separate from this experiment.

The legacy `coarseness` query name remains accepted, but its value is not read.
All values have no effect. Duplicate query keys still return HTTP 400.
The server no longer prepares separate coarse/fine models or their caches.

## Invalid Dominance

The original model kept one arrival child per parent state. It also merged
profiles with different destination children. An earlier arrival at one child
could remove a later arrival at another child with a useful onward connection.
Positive-time transit within a parent could disappear. These rules lost valid
fine journeys. Agreement between coarse population and time output did not
prove correctness.

The repair kept independent arrival states and profiles for each fine child.
Transit arrived at its actual fine endpoint. Dominance applied only within
the same fine state. Synthetic continuity checks then passed.

Final walking still used the earliest parent arrival and the shortest access
from any child. This added destinations and reduced times. The repair restored
fine transit continuity, but did not make final walking exact.

## Full Resolution-8 Run

The repaired model was measured on 2026-09-12 with 900,197 fine nodes,
2,984,575 edges, and 316,136,664 two-day profile entries. The run used eight
Julia threads, retained distance data, and used no result cache.
The machine had other workloads and memory pressure. Wall times are diagnostic
measurements, not isolated performance guarantees.

The reported query used origin `881fa44181fffff`, radius 10, and 331 origins.
Departure was 5 hours, window length was 13 hours, and step was 0.06 hours.
It had 217 samples, a 30-minute budget, a six-minute walking limit, and
`mean_intersection`. The final walking parent resolution was 7.

| Measure | Fine | Repaired |
| --- | ---: | ---: |
| Population score sum | 56,011,366 | 93,462,445 |
| Median measured wall time, seconds | 1.441 | 1.834 |
| Median process CPU time, seconds | 4.593 | 5.734 |
| Query allocation, GB | 3.129 | 3.964 |

The residual population error was +37,451,079, or +66.86% of the fine sum.
Of 331 origins, 76 were equal and 255 were above fine. None were below fine.
The median relative error was 68.20%; p95 was 436.19%; maximum was 1,429.56%.
The repaired median wall time was 27.3% slower than fine routing.
These sums add independent origin scores, not one combined area's population.

The 18 nearby budget and walking holdouts had error ratios from 11.20% to
232.05%. Five other window-mode cases had ratios from 20.59% to 36.76%.
All 16 point and window coverage comparisons retained the fine destinations.
However, one raw window comparison added 130 destinations. Common destination
times could decrease by 24 minutes within a 30-minute budget.

## Paris Comparison

The dense Paris case used 1,027 origins, 96 one-minute samples, a three-hour
budget, a one-hour walking limit, and final walking parent resolution 6.

| Measure | Fine | Repaired |
| --- | ---: | ---: |
| Population score sum | 18,479,622,441 | 21,431,542,541 |
| Median measured wall time, seconds | 21.346 | 18.559 |
| Median process CPU time, seconds | 143.744 | 124.467 |

The fine/repaired time ratio was 1.150. This modest 15% speed ratio improvement
came with +15.97% population error. All 1,027 origins had positive errors.
The gain does not justify the approximation or the extra implementation.
Historical four-to-fivefold speed ratios used the invalid parent-state model.
They do not apply to the repaired model.

## Retained Evidence

These original CSV files are historical measurements, not current API results:

- [Repaired cases](child-state-20260912/cases.csv) record query settings.
- [Repaired quality](child-state-20260912/quality.csv) records per-case errors.
- [Repaired trials](child-state-20260912/trials.csv) retains warmups and measured calls.
- [Repaired coverage](child-state-20260912/coverage.csv) records fine comparisons.
- [Original quality](global-res8-20260912/quality.csv) records the invalid model.
- [Original trials](global-res8-20260912/trials.csv) retains its timing evidence.
- [Original outliers](global-res8-20260912/outliers.csv) records selected errors.
- [London time](global-res8-20260912/time_London.csv) and
  [Paris time](global-res8-20260912/time_Paris.csv) record original time comparisons.

Bulk per-origin and destination rows, process logs, and superseded reports were
removed. No reproduction commands remain because the executable code was removed.
CPU benchmark tools, CPU decision reports, baseline snapshots, GPU experiments,
and active trip-shortcut work remain separate and unchanged by this cleanup.
