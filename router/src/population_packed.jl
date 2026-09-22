export PopulationWorkspacePool, population_workspace_estimate, population_workspace_stats, PopulationMemoryError

struct PopulationMemoryError <: Exception
    estimated_bytes::UInt128
    max_bytes::Int
end
Base.showerror(io::IO, e::PopulationMemoryError) = print(io,
    "population memory estimate ", e.estimated_bytes, " bytes exceeds limit ", e.max_bytes, " bytes")

"""Estimate fixed array payloads. Exclude queues, dictionaries, input graphs, and results."""
function population_workspace_estimate(nodes::Integer, destinations::Integer, range_origins::Integer, workers::Integer)
    for count in (nodes, destinations, range_origins, workers)
        count >= 0 || throw(ArgumentError("workspace counts must be nonnegative"))
        count <= typemax(Int) || throw(PopulationMemoryError(UInt128(typemax(Int)) + 1, typemax(Int)))
    end
    n, d, r, w = UInt128.((nodes, destinations, range_origins, workers))
    iszero(w) && return 0
    per_node = 16 + sizeof(Int) + (iszero(r) ? 0 : 24) + 8r
    iszero(n) || per_node <= div(UInt128(typemax(Int)), n) ||
        throw(PopulationMemoryError(UInt128(typemax(Int)) + 1, typemax(Int)))
    bytes = n * per_node + 16d
    bytes <= div(UInt128(typemax(Int)), w) ||
        throw(PopulationMemoryError(max(bytes, UInt128(typemax(Int)) + 1), typemax(Int)))
    return Int(bytes * w)
end

mutable struct PopulationWorkspaceLease
    graph::Any
    prepared::Any
    workspaces::Vector
    estimated_bytes::Int
    retained_bytes::Int
    reused_workers::Int
    on_wait::Any
end

"""Share exclusive packed workspaces. The budget is not an RSS limit."""
mutable struct PopulationWorkspacePool
    lock::ReentrantLock
    changed::Threads.Condition
    max_bytes::Int
    idle::Vector{PopulationWorkspaceLease}
    active::IdDict{Task,PopulationWorkspaceLease}
    estimated_bytes::Int
    retained_bytes::Int
    reused_workers::Int
    workers::Int
end

function PopulationWorkspacePool(; max_bytes::Integer=8*1024^3)
    0 < max_bytes <= typemax(Int) || throw(ArgumentError("workspace max_bytes must be a positive Int"))
    guard = ReentrantLock()
    return PopulationWorkspacePool(guard, Threads.Condition(guard), Int(max_bytes),
        PopulationWorkspaceLease[], IdDict{Task,PopulationWorkspaceLease}(), 0, 0, 0, 0)
end

function Base.empty!(pool::PopulationWorkspacePool)
    lock(pool.lock) do
        empty!(pool.idle)
        pool.retained_bytes = sum(l -> l.retained_bytes, values(pool.active); init=0)
    end
    return pool
end

function population_workspace_stats(pool::PopulationWorkspacePool)
    lock(pool.lock) do
        pool.retained_bytes = sum(l -> l.retained_bytes, pool.idle; init=0) +
            sum(l -> max(l.estimated_bytes, l.retained_bytes), values(pool.active); init=0)
        return (; pool.estimated_bytes, pool.retained_bytes, pool.reused_workers, pool.workers, pool.max_bytes)
    end
end

function _population_request(f, pool::PopulationWorkspacePool; on_wait=nothing)
    lock(pool.lock) do
        @assert !haskey(pool.active, current_task())
        pool.active[current_task()] = PopulationWorkspaceLease(nothing, nothing, Any[], 0, 0, 0, on_wait)
    end
    success = false
    local result, lease, retained
    try
        result = f()
        success = true
    finally
        # All routing workers have joined before this scope returns, including errors.
        lease = lock(() -> pool.active[current_task()], pool.lock)
        measured = success && !isempty(lease.workspaces) ? Base.summarysize(lease.workspaces) : 0
        lock(pool.lock) do
            delete!(pool.active, current_task())
            lease.on_wait = nothing
            lease.retained_bytes = measured
            used = sum(l -> max(l.estimated_bytes, l.retained_bytes), values(pool.active); init=0)
            while !isempty(pool.idle) && used + measured + sum(l -> l.retained_bytes, pool.idle; init=0) > pool.max_bytes
                popfirst!(pool.idle)
            end
            success && !isempty(lease.workspaces) && measured <= pool.max_bytes - used && push!(pool.idle, lease)
            retained = pool.retained_bytes = used + sum(l -> l.retained_bytes, pool.idle; init=0)
            pool.estimated_bytes = lease.estimated_bytes
            pool.reused_workers = lease.reused_workers
            pool.workers = length(lease.workspaces)
            notify(pool.changed; all=true)
        end
    end
    return merge(result, (; workspace_estimated_bytes=lease.estimated_bytes,
        workspace_retained_bytes=retained, workspace_reused_workers=lease.reused_workers))
end

struct PopulationWorkspace{H}
    settled::Vector{UInt64}
    settled_ids::Vector{Int}
    reached::Vector{UInt64}
    reached_ids::Vector{Int32}
    coverage::Vector{UInt64}
    coverage_ids::Vector{Int32}
    pending::Dict{UInt64,UInt64}
    queue::BinaryMinHeap{UInt64}
    heads::Vector{Int}
    egress_nodes::Vector{Int32}
    times::Vector{UInt32}
    masks::Vector{UInt64}
    links::Vector{Int}
    radii::Vector{Tuple{UInt32,UInt64}}
    projected::Dict{UInt64,UInt64}
    totals::Dict{UInt64,Float64}
    range_pending::Vector{UInt64}
    range_queued::Vector{UInt32}
    arrivals::Matrix{UInt32}
    schedule_hints::H
end

PopulationWorkspace(n, destinations, origins=0; schedule_hints=nothing) = PopulationWorkspace(zeros(UInt64, 2n), Int[],
    zeros(UInt64, destinations), Int32[], zeros(UInt64, destinations), Int32[],
    Dict{UInt64,UInt64}(), BinaryMinHeap{UInt64}(), zeros(Int, n), Int32[],
    UInt32[], UInt64[], Int[], Tuple{UInt32,UInt64}[], Dict{UInt64,UInt64}(), Dict{UInt64,Float64}(),
    zeros(UInt64, iszero(origins) ? 0 : 2n), fill(INF, iszero(origins) ? 0 : 2n),
    Matrix{UInt32}(undef, origins, 2n), schedule_hints)

function _population_workspaces!(pool, graph, prepared, destinations, range_origins, requested, schedule_hints)
    bytes = population_workspace_estimate(prepared.node_count, destinations, range_origins, 1)
    bytes <= pool.max_bytes || throw(PopulationMemoryError(UInt128(bytes), pool.max_bytes))
    resume = nothing
    lease, workers = lock(pool.lock) do
        lease = pool.active[current_task()]
        while true
            used = sum(l -> max(l.estimated_bytes, l.retained_bytes), values(pool.active); init=0)
            available = pool.max_bytes - used
            if available >= bytes
                workers = min(requested, div(available, bytes))
                match = findlast(pool.idle) do l
                    l.graph === graph && l.prepared === prepared && length(l.workspaces) >= workers &&
                        size(first(l.workspaces).arrivals, 1) == range_origins &&
                        first(l.workspaces).schedule_hints === schedule_hints &&
                        l.retained_bytes + 16max(0, destinations - length(first(l.workspaces).reached)) * workers <= available
                end
                if !isnothing(match)
                    on_wait = lease.on_wait
                    lease = splice!(pool.idle, match)
                    lease.on_wait = on_wait
                    resize!(lease.workspaces, workers)
                    lease.reused_workers = workers
                    lease.retained_bytes += 16max(0, destinations - length(first(lease.workspaces).reached)) * workers
                else
                    lease.graph, lease.prepared = graph, prepared
                end
                lease.estimated_bytes = bytes * workers
                pool.active[current_task()] = lease
                while !isempty(pool.idle) && used + max(lease.estimated_bytes, lease.retained_bytes) +
                        sum(l -> l.retained_bytes, pool.idle; init=0) > pool.max_bytes
                    popfirst!(pool.idle)
                end
                return lease, workers
            end
            isnothing(resume) && !isnothing(lease.on_wait) && (resume = lease.on_wait())
            wait(pool.changed)
        end
    end
    isnothing(resume) || resume()
    # Keep a concrete element type for the worker hot path.
    workspaces = PopulationWorkspace{typeof(schedule_hints)}[w for w in lease.workspaces]
    lease.workspaces = workspaces
    for w in workspaces, buffer in (w.reached, w.coverage)
        old = length(buffer)
        if old < destinations
            resize!(buffer, destinations)
            fill!(@view(buffer[(old + 1):end]), 0)
        end
    end
    for _ in (length(workspaces) + 1):workers
        push!(workspaces, PopulationWorkspace(prepared.node_count, destinations, range_origins; schedule_hints))
    end
    return workspaces
end

@inline _population_next_arrival(::Nothing, graph, edge, ready, cutoff) =
    next_arrival(graph.schedule_ptr, graph.departure, graph.arrival, edge, ready, cutoff)

@inline function _population_next_arrival(hints::Matrix{Int32}, graph, edge, ready::UInt32, cutoff::UInt32)
    base = div(ready, PERIOD) * PERIOD
    base > cutoff && return INF
    time = ready % PERIOD
    bin = Int(div(time, div(PERIOD, UInt32(8)))) + 1
    @inbounds lo, stop = hints[bin, edge], graph.schedule_ptr[edge + 1]
    # Include the first departure beyond the bin, including next-day profiles.
    @inbounds hi = bin == 8 ? stop : min(stop - Int32(1), hints[bin + 1, edge]) + Int32(1)
    while lo < hi
        mid = lo + ((hi - lo) >> 1)
        @inbounds if graph.departure[mid] < time
            lo = mid + Int32(1)
        else
            hi = mid
        end
    end
    lo == stop && return INF
    @inbounds relative = graph.arrival[lo]
    relative <= cutoff - base || return INF
    return base + relative
end

@inline function _population_deadline(cutoffs, time)
    first = searchsortedfirst(cutoffs, time)
    first > length(cutoffs) && return UInt64(0)
    return (typemax(UInt64) >> (64 - length(cutoffs))) & (typemax(UInt64) << (first - 1))
end

@inline function _population_enqueue!(w, time, node, walk, mask, cutoffs, population, limit)
    state = 2Int(node) - 1 + walk
    mask &= _population_deadline(cutoffs, time) & ~w.settled[state]
    iszero(mask) && return
    if walk
        iszero(limit) && return
        population.walk_min[node] <= min(limit, cutoffs[64 - leading_zeros(mask)] - time) || return
    end
    # Low bits hold a zero-based node ID and its walking state.
    key = (UInt64(time) << 32) | UInt64(state - 1)
    previous = get(w.pending, key, UInt64(0))
    iszero(previous) && push!(w.queue, key)
    w.pending[key] = previous | mask
    return nothing
end

@inline function _population_credit!(w, id, mask)
    iszero(mask) && return
    iszero(w.reached[id]) && push!(w.reached_ids, id)
    w.reached[id] |= mask
    return nothing
end

function _population_sources(index, population, weights, origins, limit)
    prepared = index.prepared::WalkingAdjacency
    extra = Dict{UInt64,Int32}()
    extra_weights = Float64[]
    sources = Int32[get(prepared.node_id, h, Int32(0)) for h in origins]
    direct = [Int32[] for _ in origins]
    access = [Tuple{Int32,UInt32}[] for _ in origins]
    function destination(cell)
        id = get(prepared.output_id, cell, Int32(0))
        !iszero(id) && return id
        return get!(extra, cell) do
            push!(extra_weights, weights[cell])
            Int32(length(population.weights) + length(extra_weights))
        end
    end
    for (i, cell) in enumerate(origins)
        iszero(sources[i]) || continue
        get(weights, cell, 0.0) > 0 && push!(direct[i], destination(cell))
        iszero(limit) && continue
        for hop in walking_cells(index, cell, limit)
            node = get(prepared.node_id, hop.cell, Int32(0))
            iszero(node) || push!(access[i], (node, hop.duration_ms))
            get(weights, hop.cell, 0.0) > 0 && push!(direct[i], destination(hop.cell))
        end
    end
    aligned = isempty(extra_weights) ? population.weights : vcat(population.weights, extra_weights)
    return (; sources, direct, access, weights=aligned)
end

function _population_sample_packed!(w, graph, network, population, sources, ids, ready, cutoffs, limit)
    for id in w.settled_ids
        w.settled[id] = 0
    end
    empty!(w.settled_ids)
    for id in w.reached_ids
        w.reached[id] = 0
    end
    empty!(w.reached_ids)
    # Source preparation clips direct walks to the common per-sample budget.
    for (slot, i) in enumerate(ids)
        mask = sum(UInt64(1) << (lane - 1) for lane in slot:length(ids):length(ready))
        for id in sources.direct[i]
            _population_credit!(w, id, mask)
        end
    end
    for lane in eachindex(ready)
        i = ids[mod1(lane, length(ids))]
        mask, time = UInt64(1) << (lane - 1), ready[lane]
        node = sources.sources[i]
        if iszero(node)
            for (target, duration) in sources.access[i]
                _population_enqueue!(w, time + duration, target, false, mask, cutoffs, population, limit)
            end
        else
            _population_enqueue!(w, time, node, false, mask, cutoffs, population, limit)
            _population_enqueue!(w, time, node, true, mask, cutoffs, population, limit)
        end
    end
    shared = separate = 0
    while !isempty(w.queue)
        key = pop!(w.queue)
        time, state = UInt32(key >> 32), Int(key % UInt32) + 1
        node, walk = (state + 1) >> 1, iseven(state)
        previous = w.settled[state]
        mask = pop!(w.pending, key) & ~previous
        iszero(mask) && continue
        iszero(previous) && push!(w.settled_ids, state)
        w.settled[state] = previous | mask
        shared += 1
        separate += count_ones(mask)
        cutoff = cutoffs[64 - leading_zeros(mask)]
        if walk
            remaining = min(limit, cutoff - time)
            first, stop = population.offsets[node], population.offsets[node + 1]
            if first < stop && population.durations[first] <= remaining
                iszero(w.heads[node]) && push!(w.egress_nodes, Int32(node))
                push!(w.times, time)
                push!(w.masks, mask)
                push!(w.links, w.heads[node])
                w.heads[node] = length(w.times)
            end
            for j in network.offsets[node]:(network.offsets[node + 1] - 1)
                duration = network.durations[j]
                duration <= remaining || continue
                _population_enqueue!(w, time + duration, network.targets[j], false,
                                     mask, cutoffs, population, limit)
            end
        else
            population.weights[node] > 0 && _population_credit!(w, Int32(node), mask)
            graph.out_ptr[node] == graph.out_ptr[node + 1] && continue
            for edge in graph.out_ptr[node]:(graph.out_ptr[node + 1] - Int32(1))
                arrival = _population_next_arrival(w.schedule_hints, graph, edge, time, cutoff)
                arrival == INF && continue
                target = graph.edge_to[edge]
                _population_enqueue!(w, arrival, target, false, mask, cutoffs, population, limit)
                _population_enqueue!(w, arrival, target, true, mask, cutoffs, population, limit)
            end
        end
    end
    _population_cover!(w, population, cutoffs, limit)
    return shared, separate
end

function _population_cover!(w, population, cutoffs, limit)
    # Remove lanes as sorted walk lengths exceed their radii.
    for node in w.egress_nodes
        empty!(w.radii)
        event, active = w.heads[node], UInt64(0)
        while event != 0
            bits = w.masks[event]
            active |= bits
            while bits != 0
                lane = trailing_zeros(bits) + 1
                push!(w.radii, (min(limit, cutoffs[lane] - w.times[event]), UInt64(1) << (lane - 1)))
                bits &= bits - UInt64(1)
            end
            event = w.links[event]
        end
        sort!(w.radii; by=first, alg=InsertionSort)
        at = 1
        for j in population.offsets[node]:(population.offsets[node + 1] - 1)
            while at <= length(w.radii) && w.radii[at][1] < population.durations[j]
                active &= ~w.radii[at][2]
                at += 1
            end
            iszero(active) && break
            _population_credit!(w, population.targets[j], active)
        end
        w.heads[node] = 0
    end
    empty!(w.egress_nodes)
    empty!(w.times)
    empty!(w.masks)
    empty!(w.links)
    return nothing
end

function _population_tile!(w, graph, network, population, sources, ids, ready, budget, step, samples, limit, mode, own_ids=nothing)
    values = zeros(Float64, length(ids))
    block_samples = fld(64, length(ids))
    reuse = samples > block_samples && !isempty(w.arrivals)
    labels = w.arrivals
    # Clear old range labels even when this tile uses the packed path.
    if !isempty(labels)
        isempty(w.settled_ids) && fill!(labels, INF)
        for state in w.settled_ids
            fill!(@view(labels[:, state]), INF)
            w.settled[state] = 0
        end
        empty!(w.settled_ids)
    end
    union_mode = mode in (:min_union, :diff_union)
    shared = separate = 0
    blocks = 0:block_samples:(samples - 1)
    for block_index in eachindex(blocks)
        block = blocks[reuse ? length(blocks) - block_index + 1 : block_index]
        count = min(block_samples, samples - block)
        lane_ready = UInt32[ready + sample * step for sample in block:(block + count - 1) for _ in ids]
        cutoffs = lane_ready .+ budget
        masks = [sum(UInt64(1) << (sample * length(ids) + i - 1)
                     for sample in 0:(count - 1)) for i in 1:length(ids)]
        expansions, queries = if reuse
            _population_sample_range!(w, graph, network, population, sources, ids, lane_ready, cutoffs, limit, labels)
        else
            _population_sample_packed!(w, graph, network, population, sources, ids, lane_ready, cutoffs, limit)
        end
        shared += expansions
        separate += queries
        # Exclude only this origin's credit, after traversal and before floating-point sums.
        if !isnothing(own_ids)
            for (i, source) in enumerate(ids)
                id = own_ids[source]
                iszero(id) || (w.reached[id] &= ~masks[i])
            end
        end
        empty!(w.totals)
        if mode == :reachable_union
            for id in w.reached_ids
                bits = w.reached[id]
                w.totals[bits] = get(w.totals, bits, 0.0) + sources.weights[id]
            end
            for (bits, total) in w.totals, i in eachindex(masks)
                hits = count_ones(bits & masks[i])
                iszero(hits) || (values[i] += total * (hits / samples))
            end
        else
            empty!(w.projected)
            for id in w.reached_ids
                bits = get!(w.projected, w.reached[id]) do
                    projected = UInt64(0)
                    for i in eachindex(masks)
                        selected = w.reached[id] & masks[i]
                        (union_mode ? !iszero(selected) : selected == masks[i]) &&
                            (projected |= UInt64(1) << (i - 1))
                    end
                    projected
                end
                w.reached[id] = bits
                if (block_index == 1 || union_mode) && !iszero(bits)
                    iszero(w.coverage[id]) && push!(w.coverage_ids, id)
                    w.coverage[id] |= bits
                end
            end
            if block_index != 1 && !union_mode
                for id in w.coverage_ids
                    w.coverage[id] &= w.reached[id]
                end
            end
        end
    end
    for id in w.coverage_ids
        bits = w.coverage[id]
        iszero(bits) || (w.totals[bits] = get(w.totals, bits, 0.0) + sources.weights[id])
        w.coverage[id] = 0
    end
    empty!(w.coverage_ids)
    if mode != :reachable_union
        for (bits, total) in w.totals
            while bits != 0
                values[trailing_zeros(bits) + 1] += total
                bits &= bits - UInt64(1)
            end
        end
    end
    return (; value=values, shared, separate)
end

const PopulationCacheKey = Tuple{UInt64,UInt32,UInt32,Int64,Int64,UInt32,Symbol,Bool}

# Only lookup and publication hold this lock. Inputs must stay unchanged.
mutable struct PopulationResultCache
    lock::ReentrantLock
    graph::Graph
    population::Population
    walking_index::WalkingIndex
    totals::Dict{PopulationCacheKey,Float64}
    order::Vector{PopulationCacheKey}
    next::Int
    capacity::Int
end

function PopulationResultCache(graph, population, walking_index; capacity=100_000)
    capacity > 0 || throw(ArgumentError("population cache capacity must be positive"))
    return PopulationResultCache(ReentrantLock(), graph, population, walking_index,
        Dict{PopulationCacheKey,Float64}(), PopulationCacheKey[], 1, capacity)
end

function route_population(graph, population::Population, origin, departure_ms, budget_ms; kwargs...)
    return _route_population(graph, population, origin, departure_ms, budget_ms; kwargs...)
end

function _cached_route_population(cache::PopulationResultCache, origin, departure_ms, budget_ms; kwargs...)
    return _route_population(cache.graph, cache.population, origin, departure_ms, budget_ms;
        walking_index=cache.walking_index, result_cache=cache, kwargs...)
end

function _route_population(args...; workspace_pool=nothing, workspace_wait=nothing, kwargs...)
    isnothing(workspace_pool) && return _route_population_impl(args...; kwargs...)
    return _population_request(workspace_pool; on_wait=workspace_wait) do
        _route_population_impl(args...; workspace_pool, kwargs...)
    end
end

function _route_population_impl(graph, population::Population, origin, departure_ms, budget_ms;
                          origin_radius=0, window_ms=0, step_ms=60_000,
                          max_walk_ms=3_600_000, window_mode=:mean_intersection,
                          walking_index=WalkingIndex(graph), prepared_population=nothing,
                          origin_batch_size=nothing, exclude_origin_population::Bool=false,
                          result_cache=nothing, workspace_pool=nothing, origins=nothing,
                          workers::Integer=Threads.nthreads(:default), probe_only::Bool=false,
                          normalisation=:none, normalisation_param=nothing, stats=nothing)
    workers > 0 || throw(ArgumentError("workers must be positive"))
    normalisation, normalisation_param = _population_normalisation(normalisation, normalisation_param)
    ready, _ = query_times(graph, origin, departure_ms, budget_ms)
    radius = _origin_radius(string(origin_radius))
    normalisation_resolution, normalisation_radius = normalisation == :pop ?
        _population_normalisation_grid(origin, graph.resolution, normalisation_param) :
        (graph.resolution, 0)
    if !isnothing(workspace_pool)
        # Bound H3 disks and the output estimate before H3 allocates them.
        r = UInt128(radius)
        cells = 3r * (r + 1) + 1
        nr = UInt128(normalisation_radius)
        normalisation_cells = 3nr * (nr + 1) + 1
        estimated = max(cells * 16, normalisation_cells * 8)
        estimated <= workspace_pool.max_bytes ||
            throw(PopulationMemoryError(estimated, workspace_pool.max_bytes))
    end
    window_ms isa Integer && window_ms >= 0 || throw(ArgumentError("window must be nonnegative"))
    step_ms isa Integer && step_ms >= 0 || throw(ArgumentError("sample step must be nonnegative"))
    active = window_ms > 0 && step_ms > 0
    step, samples = active ? _window_times(ready, budget_ms, window_ms, step_ms)[1:2] : (0, 1)
    mode = active ? _window_mode(window_mode) : :mean_intersection
    limit = min(_walking_limit(max_walk_ms), UInt32(budget_ms))
    walking_index.resolution == graph.resolution && walking_index.cells == graph.h3 ||
        throw(ArgumentError("walking index does not match graph"))
    isnothing(origin_batch_size) || (origin_batch_size isa Integer && 1 <= origin_batch_size <= 64) ||
        throw(ArgumentError("origin_batch_size must be in 1..64"))
    prepared = _prepare_population(population, walking_index)
    isnothing(prepared_population) || prepared_population === prepared ||
        throw(ArgumentError("prepared population does not match population and walking index"))
    origins = isnothing(origins) ? H3.API.gridDisk(origin, radius) : origins
    origins isa Vector{UInt64} || throw(ArgumentError("H3 origin disk failed"))
    sort!(filter!(!iszero, origins))
    isempty(origins) && return (; h3=origins, value=Float64[], origin_count=0,
        shared_expansions=0, query_expansions=0, workers=0, cache_hits=0, cache_misses=0)
    weights = _population_rollup(population, graph.resolution)
    function route_missing(selected)
        if !isnothing(graph.trip_id)
            return _route_population_trip_reference(graph, population, origin, departure_ms, budget_ms;
                origin_radius, window_ms, step_ms, max_walk_ms, window_mode, walking_index,
                origin_batch_size, exclude_origin_population, origins=selected, workers, stats)
        elseif isnothing(prepared) || limit > walking_index.prepared.limit
            return _route_population_reference(graph, population, origin, departure_ms, budget_ms;
                origin_radius, window_ms, step_ms, max_walk_ms, window_mode, walking_index,
                origin_batch_size, exclude_origin_population, origins=selected, workers, stats)
        end
        return _route_population_origins_impl(graph, walking_index, population, prepared, weights,
            selected, ready, budget_ms, step, samples, limit, mode, origin_batch_size,
            exclude_origin_population; workspace_pool, workers)
    end
    if isnothing(result_cache)
        result = route_missing(origins)
        return _apply_population_normalisation(result, population, graph.resolution,
            normalisation, normalisation_param, exclude_origin_population,
            normalisation_radius, normalisation_resolution)
    end
    cache = result_cache
    # Population modes use coverage, not elapsed-time statistics.
    semantic_mode = samples == 1 ? :mean_intersection :
        mode in (:min_union, :diff_union) ? :min_union :
        mode == :reachable_union ? mode : :mean_intersection
    keys = PopulationCacheKey[(cell, ready, UInt32(budget_ms), samples == 1 ? 0 : step,
        samples, limit, semantic_mode, exclude_origin_population) for cell in origins]
    missing, values = lock(cache.lock) do
        findall(key -> !haskey(cache.totals, key), keys), Float64[get(cache.totals, key, 0.0) for key in keys]
    end
    probe_only && !isempty(missing) && return (; cache_misses=length(missing))
    result = isempty(missing) ? (; shared_expansions=0, query_expansions=0, workers=0) :
        route_missing(length(missing) == length(origins) ? origins : origins[missing])
    if !isempty(missing)
        values[missing] = result.value
        # Publish only after all workers and finite-total checks succeed.
        lock(cache.lock) do
            for i in missing
                key = keys[i]
                haskey(cache.totals, key) && continue
                if length(cache.order) < cache.capacity
                    push!(cache.order, key)
                else
                    delete!(cache.totals, cache.order[cache.next])
                    cache.order[cache.next] = key
                    cache.next = mod1(cache.next + 1, cache.capacity)
                end
                cache.totals[key] = values[i]
            end
        end
    end
    result = (; h3=origins, value=values, result.shared_expansions, result.query_expansions,
        result.workers, cache_hits=length(origins) - length(missing), cache_misses=length(missing))
    return _apply_population_normalisation(result, population, graph.resolution,
        normalisation, normalisation_param, exclude_origin_population,
        normalisation_radius, normalisation_resolution)
end

function _route_population_origins(args...; workspace_pool=nothing, workers::Integer=Threads.nthreads(:default))
    workers > 0 || throw(ArgumentError("workers must be positive"))
    isnothing(workspace_pool) && return _route_population_origins_impl(args...; workers)
    return _population_request(workspace_pool) do
        _route_population_origins_impl(args...; workspace_pool, workers)
    end
end

function _route_population_origins_impl(graph, walking_index, population, prepared, weights,
        origins, ready, budget_ms, step, samples, limit, mode, origin_batch_size,
        exclude_origin_population; workspace_pool=nothing, workers::Integer=Threads.nthreads(:default))
    values = zeros(Float64, length(origins))
    isempty(weights) && return (; h3=origins, value=values, shared_expansions=0, query_expansions=0, workers=0)
    sources = _population_sources(walking_index, prepared, weights, origins, limit)
    own_ids = exclude_origin_population ? Int32[
        get(weights, cell, 0.0) > 0 ? get(walking_index.prepared.output_id, cell) do
            first(sources.direct[i])
        end : 0 for (i, cell) in enumerate(origins)] : nothing
    heavy = Int[]
    network = walking_index.prepared.graph
    boardable(node) = graph.out_ptr[node] < graph.out_ptr[node + 1]
    for i in eachindex(origins)
        node = sources.sources[i]
        transit = if iszero(node)
            any(hop -> boardable(first(hop)), sources.access[i])
        else
            boardable(node) || any(j -> network.durations[j] <= limit && boardable(network.targets[j]),
                network.offsets[node]:(network.offsets[node + 1] - 1))
        end
        if transit
            push!(heavy, i)
            continue
        end
        # A connection, including a self-connection, can permit another walk.
        # Without one, only the source and its initial geographic walk contribute.
        own = isnothing(own_ids) ? Int32(0) : own_ids[i]
        if iszero(node)
            for id in sources.direct[i]
                id == own || (values[i] += sources.weights[id])
            end
        else
            node == own || (values[i] += prepared.weights[node])
            for j in prepared.offsets[node]:(prepared.offsets[node + 1] - 1)
                prepared.durations[j] <= limit || break
                id = prepared.targets[j]
                id == own || (values[i] += prepared.weights[id])
            end
        end
    end
    all(isfinite, values) || throw(ArgumentError("accessible population is not finite"))
    isempty(heavy) && return (; h3=origins, value=values, shared_expansions=0, query_expansions=0, workers=0)
    # Keep short windows on the one-block path; otherwise require two full tiles per worker.
    default_tile = samples == 1 || (samples > 4 && length(heavy) >= 128 * Threads.nthreads(:default)) ? 64 : 16
    tile_size = min(length(heavy), isnothing(origin_batch_size) ? default_tile : Int(origin_batch_size))
    tiles = cld(length(heavy), tile_size)
    workers = min(workers, Threads.nthreads(:default), tiles)
    range_origins = samples > fld(64, tile_size) ? tile_size : 0
    schedule_hints = _population_schedule_hints(population, graph)
    workspaces = isnothing(workspace_pool) ?
        [PopulationWorkspace(prepared.node_count, length(sources.weights), range_origins; schedule_hints) for _ in 1:workers] :
        _population_workspaces!(workspace_pool, graph, prepared, length(sources.weights), range_origins, workers, schedule_hints)
    workers = length(workspaces)
    next_tile = Threads.Atomic{Int}(1)
    failed = Threads.Atomic{Bool}(false)
    counts = Vector{Tuple{Int,Int}}(undef, workers)
    @sync for worker in 1:workers
        Threads.@spawn begin
            local shared, separate
            shared = separate = 0
            try
                while !failed[]
                    tile = Threads.atomic_add!(next_tile, 1)
                    tile > tiles && break
                    ids = @view heavy[((tile - 1) * tile_size + 1):min(tile * tile_size, length(heavy))]
                    result = _population_tile!(workspaces[worker], graph, walking_index.prepared.graph,
                        prepared, sources, ids, ready, UInt32(budget_ms), step, samples, limit, mode, own_ids)
                    values[ids] = result.value
                    shared += result.shared
                    separate += result.separate
                    yield()
                end
            catch
                failed[] = true
                rethrow()
            end
            counts[worker] = (shared, separate)
        end
    end
    all(isfinite, values) || throw(ArgumentError("accessible population is not finite"))
    shared, separate = sum(first, counts), sum(last, counts)
    return (; h3=origins, value=values, shared_expansions=shared, query_expansions=separate, workers)
end
