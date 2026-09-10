using KernelAbstractions: @localmem, @synchronize

const GPU_FLOAT32_RTOL = 2e-6
const GPU_FLOAT32_ATOL = 1e-3

function _population_upload(backend, source)
    target = KA.allocate(backend, eltype(source), length(source))
    GC.@preserve source begin
        isempty(source) || KA.copyto!(backend, target, source)
        KA.synchronize(backend)
    end
    return target
end

"""Resident population data and arrival buffers. Do not use concurrent calls."""
struct PopulationKernelRouter{B,D}
    graph::Graph
    walking_index::WalkingIndex
    weights::Dict{UInt64,Float64}
    base_weights::Vector{Float32}
    backend::B
    device::D
end

function PopulationKernelRouter(graph::Graph, population::Union{Population,Nothing}, backend;
                                walking_index=prepare_walking(WalkingIndex(graph)))
    p = walking_index.prepared
    isnothing(p) && throw(ArgumentError("GPU population requires a prepared walking index"))
    walking_index.resolution == graph.resolution && walking_index.cells == graph.h3 ||
        throw(ArgumentError("walking index does not match graph"))
    length(graph.h3) <= typemax(Int32) ÷ 32 || throw(ArgumentError("32-lane labels exceed Int32"))
    all(i -> graph.departure[i] <= graph.arrival[i] < INF, eachindex(graph.arrival)) ||
        throw(ArgumentError("GPU population requires finite nonnegative transit durations"))
    for e in eachindex(graph.edge_to)
        ids = graph.schedule_ptr[e]:(graph.schedule_ptr[e + 1] - 1)
        issorted(view(graph.departure, ids)) && issorted(view(graph.arrival, ids)) ||
            throw(ArgumentError("GPU population requires packed FIFO profiles"))
    end
    weights = isnothing(population) ? Dict{UInt64,Float64}() : _population_rollup(population, graph.resolution)
    all(w -> w <= floatmax(Float32), values(weights)) ||
        throw(ArgumentError("population weights exceed the finite Float32 range"))
    base_weights = Float32[get(weights, h, 0.0) for h in p.output_cells]
    host = (; out_ptr=graph.out_ptr, to=graph.edge_to, schedule=graph.schedule_ptr,
        departure=graph.departure, arrival=graph.arrival,
        walk_ptr=p.graph.offsets, walk_to=p.graph.targets, walk_ms=p.graph.durations,
        geo_ptr=p.output.offsets, geo_to=p.output.targets, geo_ms=p.output.durations,
        node_output=Int32[p.output_id[h] for h in graph.h3], weights=base_weights)
    device = merge(map(a -> _population_upload(backend, a), host),
        (; labels=ntuple(_ -> KA.allocate(backend, UInt32, 32length(graph.h3)), 4),
           cutoffs=KA.allocate(backend, UInt32, 32), changed=KA.allocate(backend, UInt32, 1)))
    return PopulationKernelRouter(graph, walking_index, weights, base_weights, backend, device)
end

@kernel function population_seed!(A, E, cutoffs, coverage, sources, access_ptr, access_to,
                                  access_ms, direct_ptr, direct_to, base, count, lanes,
                                  ready, sample_base, step, budget)
    q = @index(Global, Linear)
    @inbounds begin
        cutoffs[q] = UInt32(0)
        if q <= lanes
            origin = base + (q - 1) % count
            start = UInt32(ready + (sample_base + (q - 1) ÷ count) * step)
            cutoffs[q] = start + budget
            source = sources[origin]
            if source != 0
                A[q + 32 * (source - 1)] = start
                E[q + 32 * (source - 1)] = start
            end
            for j in access_ptr[origin]:(access_ptr[origin + 1] - 1)
                A[q + 32 * (access_to[j] - 1)] = start + access_ms[j]
            end
            for j in direct_ptr[origin]:(direct_ptr[origin + 1] - 1)
                Atomix.@atomic coverage[direct_to[j]] |= UInt32(1) << (q - 1)
            end
        end
    end
end

@kernel function population_prepare!(A, E, nextA, nextE, changed)
    i = @index(Global, Linear)
    @inbounds nextA[i] = A[i]
    @inbounds nextE[i] = E[i]
    if i == 1
        @inbounds changed[1] = UInt32(0)
    end
end

# A permits transit. E also permits one walk; walking cannot update E.
@kernel function population_relax!(A, E, nextA, nextE, changed, cutoffs, out_ptr, to,
                                   schedule, departure, arrival, walk_ptr, walk_to, walk_ms, limit)
    i = @index(Global, Linear)
    q, node = _window_position(Int32(i), Int32(32), Int32(5))
    @inbounds begin
        cutoff = cutoffs[q]
        if A[i] != INF
            for e in out_ptr[node]:(out_ptr[node + 1] - 1)
                candidate = next_arrival(schedule, departure, arrival, e, A[i], cutoff)
                if candidate != INF
                    target = q + Int32(32) * (to[e] - Int32(1))
                    oldA, _ = Atomix.@atomic nextA[target] min candidate
                    # Transit restores walking eligibility, even on a self-edge.
                    oldE, _ = Atomix.@atomic nextE[target] min candidate
                    if candidate < oldA || candidate < oldE
                        Atomix.@atomic changed[1] |= UInt32(1)
                    end
                end
            end
        end
        if E[i] <= cutoff && limit != 0
            for j in walk_ptr[node]:(walk_ptr[node + 1] - 1)
                duration = walk_ms[j]
                if duration <= limit && duration <= cutoff - E[i]
                    candidate = E[i] + duration
                    target = q + Int32(32) * (walk_to[j] - Int32(1))
                    old, _ = Atomix.@atomic nextA[target] min candidate
                    if candidate < old
                        Atomix.@atomic changed[1] |= UInt32(1)
                    end
                end
            end
        end
    end
end

@kernel function population_cover!(coverage, A, E, cutoffs, node_output, geo_ptr, geo_to, geo_ms, limit)
    i = @index(Global, Linear)
    q, node = _window_position(Int32(i), Int32(32), Int32(5))
    bit = UInt32(1) << (q - Int32(1))
    @inbounds begin
        cutoff = cutoffs[q]
        if A[i] <= cutoff
            Atomix.@atomic coverage[node_output[node]] |= bit
        end
        if E[i] <= cutoff && limit != 0
            for j in geo_ptr[node]:(geo_ptr[node + 1] - 1)
                duration = geo_ms[j]
                if duration <= limit && duration <= cutoff - E[i]
                    Atomix.@atomic coverage[geo_to[j]] |= bit
                end
            end
        end
    end
end

@kernel function population_project!(persistent, coverage, masks, count, union_mode)
    cell = @index(Global, Linear)
    bits = UInt32(0)
    @inbounds for o in 1:count
        selected = coverage[cell] & masks[o]
        if union_mode ? selected != 0 : selected == masks[o]
            bits |= UInt32(1) << (o - 1)
        end
    end
    @inbounds persistent[cell] = union_mode ? persistent[cell] | bits : persistent[cell] & bits
end

@kernel function population_reduce!(partials, coverage, weights, masks, parts, weighted, samples)
    group = @index(Group, Linear)
    thread = @index(Local, Linear)
    origin, part = (group - 1) ÷ parts + 1, (group - 1) % parts + 1
    total = 0f0
    @inbounds for cell in (thread + 256 * (part - 1)):(256 * parts):length(weights)
        fraction = weighted ? Float32(count_ones(coverage[cell] & masks[origin])) / Float32(samples) :
            Float32((coverage[cell] >> (origin - 1)) & UInt32(1))
        total += weights[cell] * fraction
    end
    scratch = @localmem Float32 (256,)
    @inbounds scratch[thread] = total
    @synchronize
    for stride in (128, 64, 32, 16, 8, 4, 2, 1)
        if thread <= stride
            @inbounds scratch[thread] += scratch[thread + stride]
        end
        @synchronize
    end
    if thread == 1
        @inbounds partials[group] = scratch[1]
    end
end

@kernel function population_finish!(totals, partials, base, parts)
    origin = @index(Global, Linear)
    total = 0f0
    @inbounds for part in 1:parts
        total += partials[(origin - 1) * parts + part]
    end
    @inbounds totals[base + origin - 1] += total
end

function _route_population_gpu(router::PopulationKernelRouter, origin, departure_ms, budget_ms;
                               origin_radius=0, window_ms=0, step_ms=60_000,
                               max_walk_ms=3_600_000, window_mode=:mean_intersection,
                               origins_per_tile=window_ms > 0 && step_ms > 0 ? 8 : 32)
    started = time_ns()
    graph, index, backend, d = router.graph, router.walking_index, router.backend, router.device
    ready, _ = query_times(graph, origin, departure_ms, budget_ms)
    radius = _origin_radius(string(origin_radius))
    window_ms isa Integer && window_ms >= 0 || throw(ArgumentError("window must be nonnegative"))
    step_ms isa Integer && step_ms >= 0 || throw(ArgumentError("sample step must be nonnegative"))
    active = window_ms > 0 && step_ms > 0
    step, samples = active ? _window_times(ready, budget_ms, window_ms, step_ms)[1:2] : (0, 1)
    mode = active ? _window_mode(window_mode) : :mean_intersection
    requested = _walking_limit(max_walk_ms)
    requested <= index.prepared.limit || throw(ArgumentError("max_walk_ms exceeds the prepared GPU limit"))
    limit = min(requested, UInt32(budget_ms))
    origins_per_tile isa Integer && 1 <= origins_per_tile <= 32 ||
        throw(ArgumentError("origins_per_tile must be between 1 and 32"))
    origins = H3.API.gridDisk(origin, radius)
    origins isa Vector{UInt64} || throw(ArgumentError("H3 origin disk failed"))
    sort!(filter!(!iszero, origins))
    cells = copy(index.prepared.output_cells)
    ids = copy(index.prepared.output_id)
    function output_id(h)
        get!(ids, h) do
            length(cells) < typemax(Int32) || throw(ArgumentError("output IDs exceed Int32"))
            push!(cells, h)
            Int32(length(cells))
        end
    end
    sources, access_to, direct_to = Int32[], Int32[], Int32[]
    access_ptr, direct_ptr, access_ms = [1], [1], UInt32[]
    for h in origins
        source = get(graph.node_id, h, Int32(0))
        push!(sources, source)
        push!(direct_to, output_id(h))
        if source == 0
            for hop in walking_neighbors(index, h, limit)
                push!(access_to, graph.node_id[hop.cell])
                push!(access_ms, hop.duration_ms)
            end
            for hop in walking_cells(index, h, limit)
                push!(direct_to, output_id(hop.cell))
            end
        end
        push!(access_ptr, length(access_to) + 1)
        push!(direct_ptr, length(direct_to) + 1)
    end
    weights = length(cells) == length(router.base_weights) ? router.base_weights :
        [router.base_weights; Float32[get(router.weights, cells[i], 0.0) for i in (length(router.base_weights) + 1):length(cells)]]
    planning_s = (time_ns() - started) / 1e9
    started = time_ns()
    request = map(a -> _population_upload(backend, a), (; sources, access_ptr, access_to, access_ms, direct_ptr, direct_to))
    device_weights = length(weights) == length(router.base_weights) ? d.weights : _population_upload(backend, weights)
    coverage = KA.allocate(backend, UInt32, length(cells))
    persistent = KA.allocate(backend, UInt32, length(cells))
    masks = KA.allocate(backend, UInt32, 32)
    parts = min(256, cld(length(cells), 256))
    partials = KA.allocate(backend, Float32, parts * origins_per_tile)
    totals = KA.zeros(backend, Float32, length(origins))
    KA.synchronize(backend)
    upload_s = (time_ns() - started) / 1e9
    host_masks, host_changed = zeros(UInt32, 32), UInt32[0]
    rounds = batches = 0
    weighted, union_mode = mode == :reachable_union, mode in (:min_union, :diff_union)
    device_s = 0.0
    for base in 1:origins_per_tile:length(origins)
        count = min(origins_per_tile, length(origins) - base + 1)
        block_samples = 32 ÷ count
        started = time_ns()
        fill!(persistent, union_mode ? UInt32(0) : typemax(UInt32) >> (32 - count))
        KA.synchronize(backend)
        device_s += (time_ns() - started) / 1e9
        for sample_base in 0:block_samples:(samples - 1)
            wave = min(block_samples, samples - sample_base)
            fill!(host_masks, 0)
            for s in 0:(wave - 1), o in 1:count
                host_masks[o] |= UInt32(1) << (s * count + o - 1)
            end
            started = time_ns()
            GC.@preserve host_masks begin
                KA.copyto!(backend, masks, host_masks)
                KA.synchronize(backend)
            end
            upload_s += (time_ns() - started) / 1e9
            started = time_ns()
            A, E, nextA, nextE = d.labels
            fill!(A, INF); fill!(E, INF); fill!(coverage, UInt32(0))
            population_seed!(backend, 32)(A, E, d.cutoffs, coverage, request.sources,
                request.access_ptr, request.access_to, request.access_ms, request.direct_ptr,
                request.direct_to, base, count, count * wave, Int64(ready), sample_base, step,
                UInt32(budget_ms); ndrange=32)
            while !isempty(A)
                population_prepare!(backend, 256)(A, E, nextA, nextE, d.changed; ndrange=length(A))
                population_relax!(backend, 256)(A, E, nextA, nextE, d.changed, d.cutoffs,
                    d.out_ptr, d.to, d.schedule, d.departure, d.arrival,
                    d.walk_ptr, d.walk_to, d.walk_ms, limit; ndrange=length(A))
                A, nextA = nextA, A
                E, nextE = nextE, E
                KA.copyto!(backend, host_changed, d.changed)
                KA.synchronize(backend)
                rounds += 1
                iszero(host_changed[1]) && break
            end
            isempty(A) || population_cover!(backend, 256)(coverage, A, E, d.cutoffs,
                d.node_output, d.geo_ptr, d.geo_to, d.geo_ms, limit; ndrange=length(A))
            weighted || population_project!(backend, 256)(persistent, coverage, masks,
                count, union_mode; ndrange=length(cells))
            if weighted || sample_base + wave == samples
                population_reduce!(backend, 256)(partials, weighted ? coverage : persistent,
                    device_weights, masks, parts, weighted, samples; ndrange=256 * parts * count)
                population_finish!(backend, 32)(totals, partials, base, parts; ndrange=count)
            end
            KA.synchronize(backend)
            device_s += (time_ns() - started) / 1e9
            batches += 1
        end
    end
    started = time_ns()
    values = Vector{Float32}(undef, length(origins))
    KA.copyto!(backend, values, totals)
    KA.synchronize(backend)
    download_s = (time_ns() - started) / 1e9
    all(isfinite, values) || throw(ArgumentError("accessible population exceeds the finite Float32 accumulation range"))
    return (; h3=origins, value=Float64.(values), samples, batches, rounds, planning_s, upload_s,
        device_s, download_s, bytes_downloaded=sizeof(values) + rounds * sizeof(UInt32),
        weight_and_accumulator_precision="Float32",
        output_cells=length(cells), backend=backend isa KA.CPU ? "ka_cpu_population" : "gpu_population")
end
