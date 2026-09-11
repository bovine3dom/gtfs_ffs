# Research Roadmap

Use the [CPU decisions](cpu-research-results.md) as the population baseline.
Use the [GPU plan](../../gpu_todo.md) for active hardware work.

## CPU Measurement

- Profile timetable lookup and queue work before another population change.
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
