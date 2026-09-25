# Router performance

See [Measured results](performance-results.md) for the 60-CPU VM comparison.

## Request measurements

Set `ROUTER_TIMING=true` before you start the server. You can also send the HTTP
request header `X-Router-Timing: true` for one request.

The response contains these headers:

- `Server-Timing`: shard access, population cache lookup, routing, encoding,
  queue wait, and total handler time. All durations are in milliseconds.
- `X-Router-Lane`: `short` or `bulk` for a computed response.
- `X-Router-Response-Cache`: `hit` or `miss` for the complete response.

The timers do not include the time to send the response to the client. The
population lookup timer can include the first shard load. Workspace queue wait
can occur inside routing. Thus, do not add the stage durations to get total time.
A complete-response cache hit has fresh timings, not the timings of the old
computation. Timing is off by default.

## CPU and memory changes

Trip-aware population requests assign independent origin tiles to workers.
Each worker keeps its scratch memory until all its tiles are complete. It also
combines its tile results. Workers do not wait for the slowest tile in each batch.
The small-origin path can still run different departure blocks in parallel.

Population state arrays store the lanes for one node next to each other. A worker
initializes a node only when a search reaches it. This removes repeated writes
to unused state arrays. Scratch ownership follows the task, not its thread ID.
This reduces allocation and memory traffic. It does not pin Julia tasks to sockets.
Trip population routing does not build the extra population lookup arrays that
only the legacy packed engine uses. Short trip searches do not reuse very large
scratch buffers. This prevents repeated clearing of large hash tables for a
small route.

The server compiles the trip-aware paths with synthetic data before it accepts
requests. The first real request can still load shards and their indexes.

## Small requests

The server keeps the configured short-worker reserve. A window with no origin
node and no walking returns only the origin. It uses short admission and does
not allocate graph-sized result arrays. Single-departure requests
can use this reserve for both time and distance metrics. An unmeasured trip-aware
request uses this rule only when its budget is at most half an hour.

Each graph handler also keeps up to 1,024 request-cost records. A record includes
the origin, trip-aware mode, budget, window, walking limit, and metric. It does
not include departure time. Routing duration multiplied by the admitted worker
count gives a conservative estimate of serial work.

A request with a budget of at most three hours can use one short worker when its
estimated work is at most 200 ms. An expensive observation removes short-request
status immediately. Later cheap observations reduce the estimate gradually.
Cached samples do not train this estimate. This is a heuristic, not a latency
guarantee. A new departure can cost more than previous departures.

A small window can take longer on one worker than on many workers. It can then
run while an expensive request holds the bulk workers. Use mixed-load results,
not only single-request results, to assess this change.

## Animation cache

The server shares a 256 MiB cache of trip-aware, transit-only departure samples.
The cache stores exact results. A key includes the graph namespace, component,
origin, departure, budget, and distance mode. It contains no graph reference.
Eviction of a sample does not keep a mapped graph in memory.

If animation advances departure by one sample step, adjacent windows can reuse
all but one sample. Different budgets or departures do not share samples.
Walking windows and population windows do not use this new cache.
`X-Router-Searches` and `X-Router-Reused-Samples` show the work that remains.

## Repeatable benchmark

`router/benchmark.jl` accepts the same arguments as `router/serve.jl`.
It measures the four supplied request types at the centre of the supplied H3
cell. Round zero includes first-use work. Later rounds change departure by one
millisecond to prevent complete-response cache hits.

```sh
env ROUTER_BENCH_OUTPUT=/tmp/router-bench \
    ROUTER_BENCH_RESOLUTIONS=5,6,7,8 \
    ROUTER_BENCH_TRIP_AWARE=true,false \
    ROUTER_BENCH_ROUNDS=3 \
    julia --threads=60 --project=router router/benchmark.jl \
      --population data/kontur_h3.arrow --trip-shards data/trip-shards \
      --short-workers=12 --max-workers-per-request=48 \
      --workspace-memory-gib=32 \
      data/everything_res8.arrow data/rail_and_friends_res8.arrow
```

Additional controls:

- `ROUTER_BENCH_CASES=population,long,short_window,walk`: select cases.
- `ROUTER_BENCH_ORIGIN=851fb08bfffffff`: set the centre used at each resolution.
  Check that a transit-only test origin has outgoing connections. A fine cell
  at the centre of a coarse cell can have no transit service.
- `ROUTER_BENCH_WORKERS=12,24,48`: compare bulk-worker limits without loading the
  graphs again. Each limit uses different departures.
- `ROUTER_BENCH_PROFILE=true`: collect a CPU profile on the last round. That
  round includes profiler overhead.
- `ROUTER_BENCH_ANIMATE=true`: advance departure by the sample step instead of
  one millisecond.
- `ROUTER_BENCH_MIXED=true`: run eight small requests while a population request
  holds bulk capacity. This test uses the first selected resolution.

The output contains timings, allocation counts, GC time, response hashes,
worker counts, and queue time. Mixed-load results are in `mixed.csv`.
Run versions separately. Do not run two heavy benchmarks at the same time.
A hash comparison is valid only for the same query and departure.

## Proxmox settings for a later restart

The current guest exposes 60 virtual CPUs and one NUMA node. Its CPU type is
`QEMU Virtual CPU version 2.5+`. The guest cannot control the hidden host NUMA
placement.

First run `lscpu -e=CPU,CORE,SOCKET,NODE` and `numactl --hardware` on the Proxmox
host. Two sockets do not always mean two NUMA nodes.

If the host has two NUMA nodes, use these initial VM settings:

1. CPU type: `host`. Check live-migration requirements before this change.
2. Sockets: `2`. Cores per socket: `30`. This gives 60 virtual CPUs, not 60
   physical cores.
3. NUMA: enabled. Set the socket count to the host NUMA-node count if it is not
   two. Adjust cores per socket to keep 60 virtual CPUs.
4. Use fixed guest memory for repeatable benchmarks. Do not let ballooning
   remove routing memory during a run.
5. If CPU affinity is required, select host logical CPUs from both nodes.
   Keep SMT sibling pairs together. Leave capacity for the host and other VMs.
   A single affinity list does not bind each guest NUMA node to one host node.

For a two-node host, the CPU and topology command is:

```sh
# Run on the Proxmox host while the VM is stopped.
qm set VMID --cpu host --sockets 2 --cores 30 --numa 1
```

Proxmox also supports explicit `numa0` and `numa1` settings with guest CPU IDs,
`hostnodes`, memory size, and memory policy. Do not copy host CPU or node IDs from
this guest. Check the host topology and memory capacity before you set them.

After a full VM restart, verify the topology with `lscpu` and
`numactl --hardware` inside the guest. Repeat the worker sweep. Compare normal
placement with `numactl --interleave=all` for the shared graphs. Interleaving is
an experiment, not a recommended default: it also changes scratch placement.

Source: [Proxmox VM documentation](https://pve.proxmox.com/pve-docs/qm.1.html),
CPU type, CPU affinity, NUMA, and memory sections.
