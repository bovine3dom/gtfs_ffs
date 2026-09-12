# Research Roadmap

Use the [CPU decisions](cpu-research-results.md) and
[dense-network results](dense-population-results.md) as the population baseline.
Use the [GPU plan](../../gpu_todo.md) for active hardware work.

## CPU Measurement

- Before another [shared-event design](shared-events-results.md), measure how to reduce per-origin label and coverage work. Queue sharing alone did not justify its latency and memory costs.
- Measure kilometre replay and its heap before workspace changes.
- Measure sparse-query clearing and output storage before adding sparse buffers.
- Consider destination-parallel aggregation only if its measured cost warrants it. Preserve chronological reduction order.
- Consider a bounded compute/aggregate pipeline only after profiling. Preserve workspace ownership and join workers on failure.

These are conditional research ideas, not approved production changes. Check the
current implementation before starting. Use small independent correctness
fixtures, then paired resident-graph trials. Record CPU load, allocation, and RSS.
Do not restart the closed pattern, SIMD, radix, or expiry implementations without
new evidence. Dataset selection and public query rules are defined by the
[router contract](../../router/docs/api.md), not by old benchmark plans.
