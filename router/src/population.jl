export load_population, route_population

struct Population{H,P}
    h3::H
    weights::P
    rollups::Dict{Int,Dict{UInt64,Float64}}
    lock::ReentrantLock
end

function load_population(path::AbstractString; progress::Bool=false)
    table = _startup_stage(progress, "Opening population file $path") do _
        Arrow.Table(path)
    end
    all(name -> name in propertynames(table), (:h3, :population)) ||
        throw(ArgumentError("population input requires h3 and population columns"))
    return _population(table.h3, table.population; progress)
end

function _population(cells, weights; progress::Bool=false)
    eltype(cells) == UInt64 || throw(ArgumentError("population h3 must be UInt64"))
    eltype(weights) <: Real || throw(ArgumentError("population must have a non-null numeric type"))
    length(cells) == length(weights) || throw(ArgumentError("population column lengths differ"))
    _startup_stage(progress, "Validating population rows"; total=length(cells)) do meter
        for (i, (cell, weight)) in enumerate(zip(cells, weights))
            validate_cell(cell, 8)
            isfinite(weight) && weight >= 0 && isfinite(Float64(weight)) ||
                throw(ArgumentError("population must be finite and nonnegative"))
            i % 10_000 == 0 && _startup_advance(meter, 10_000)
        end
        _startup_advance(meter, length(cells) % 10_000)
    end
    _startup_stage(progress, "Checking population cell uniqueness") do _
        ordered = issorted(cells) ? cells : sort!(collect(cells))
        previous = UInt64(0)
        for cell in ordered
            cell != previous || throw(ArgumentError("population h3 cells must be unique"))
            previous = cell
        end
    end
    return Population(cells, weights, Dict{Int,Dict{UInt64,Float64}}(), ReentrantLock())
end

function _population_rollup(population::Population, resolution; progress::Bool=false)
    0 <= resolution <= 8 || throw(ArgumentError("population requires a routing resolution in 0..8"))
    return lock(population.lock) do
        get!(population.rollups, resolution) do
            _startup_stage(progress, "Aggregating population at res$resolution"; total=length(population.h3)) do meter
                result = Dict{UInt64,Float64}()
                for (i, (cell, weight)) in enumerate(zip(population.h3, population.weights))
                    if !iszero(weight)
                        parent = resolution == 8 ? cell : H3.API.cellToParent(cell, resolution)
                        total = get(result, parent, 0.0) + Float64(weight)
                        isfinite(total) || throw(ArgumentError("population total is not finite"))
                        result[parent] = total
                    end
                    i % 10_000 == 0 && _startup_advance(meter, 10_000)
                end
                _startup_advance(meter, length(population.h3) % 10_000)
                result
            end
        end
    end
end

function _origin_radius(text)
    radius = occursin(r"^[0-9]+\z", text) ? tryparse(Int, text) : nothing
    !isnothing(radius) && 0 <= radius <= typemax(Cint) ||
        throw(ArgumentError("origin_radius must be a nonnegative H3 Cint grid radius"))
    size = H3.API.maxGridDiskSize(radius)
    size isa Int64 && 0 < size <= div(typemax(Int), sizeof(UInt64)) ||
        throw(ArgumentError("origin_radius exceeds the H3 grid allocation range"))
    return radius
end

# Each bit is an origin/sample query. Cutoffs follow sample-major lane order.
function _population_sample(graph, topology, origins, ready, cutoffs, weights)
    State = Tuple{UInt32,UInt64,Bool}
    queue = BinaryMinHeap{State}()
    pending = Dict{State,UInt64}()
    settled = Dict{Tuple{UInt64,Bool},UInt64}()
    reached = Dict{UInt64,UInt64}()
    used = typemax(UInt64) >> (64 - length(origins))
    function deadline_mask(time)
        first = searchsortedfirst(cutoffs, time)
        return first > length(cutoffs) ? UInt64(0) : used & (typemax(UInt64) << (first - 1))
    end
    function enqueue(time, cell, walk, mask)
        mask &= deadline_mask(time) & ~get(settled, (cell, walk), UInt64(0))
        iszero(mask) && return
        key = (time, cell, walk)
        haskey(pending, key) || push!(queue, key)
        pending[key] = get(pending, key, UInt64(0)) | mask
    end
    function credit(cell, mask)
        iszero(mask) && return
        get(weights, cell, 0.0) > 0 || return
        reached[cell] = get(reached, cell, UInt64(0)) | mask
    end
    for (i, cell) in enumerate(origins)
        mask = UInt64(1) << (i - 1)
        enqueue(ready[i], cell, false, mask)
        iszero(topology.limit) || enqueue(ready[i], cell, true, mask)
    end
    expansions = queries = 0
    while !isempty(queue)
        key = pop!(queue)
        time, cell, walk = key
        previous = get(settled, (cell, walk), UInt64(0))
        mask = pop!(pending, key) & ~previous
        iszero(mask) && continue
        settled[(cell, walk)] = previous | mask
        expansions += 1
        queries += count_ones(mask)
        credit(cell, mask)
        cutoff = cutoffs[64 - leading_zeros(mask)]
        if walk
            limit = min(topology.limit, cutoff - time)
            for hop in _walking_hops(topology, cell; geographic=true, limit)
                hop.duration_ms <= limit || continue
                credit(hop.cell, mask & deadline_mask(time + hop.duration_ms))
            end
            for hop in _walking_hops(topology, cell; limit)
                hop.duration_ms <= limit || continue
                target = hop.cell isa Int32 ? graph.h3[hop.cell] : hop.cell
                enqueue(time + hop.duration_ms, target, false, mask)
            end
        else
            u = get(graph.node_id, cell, Int32(0))
            iszero(u) && continue
            for edge in graph.out_ptr[u]:(graph.out_ptr[u + 1] - Int32(1))
                arrival = next_arrival(graph.schedule_ptr, graph.departure, graph.arrival, edge, time, cutoff)
                arrival == INF && continue
                target = graph.h3[graph.edge_to[edge]]
                enqueue(arrival, target, false, mask)
                iszero(topology.limit) || enqueue(arrival, target, true, mask)
            end
        end
    end
    return reached, expansions, queries
end

function _population_totals!(values, ids, coverage, weights, divisor)
    totals = Dict{UInt64,Float64}()
    for (cell, bits) in coverage
        totals[bits] = get(totals, bits, 0.0) + weights[cell] / divisor
    end
    for (bits, total) in totals
        while bits != 0
            values[ids[trailing_zeros(bits) + 1]] += total
            bits &= bits - UInt64(1)
        end
    end
end

function route_population(graph, population::Population, origin, departure_ms, budget_ms;
                          origin_radius=0, window_ms=0, step_ms=60_000,
                          max_walk_ms=3_600_000, window_mode=:mean_intersection,
                          walking_index=WalkingIndex(graph))
    ready, _ = query_times(graph, origin, departure_ms, budget_ms)
    radius = _origin_radius(string(origin_radius))
    window_ms isa Integer && window_ms >= 0 || throw(ArgumentError("window must be nonnegative"))
    step_ms isa Integer && step_ms >= 0 || throw(ArgumentError("sample step must be nonnegative"))
    active = window_ms > 0 && step_ms > 0
    step, samples = active ? _window_times(ready, budget_ms, window_ms, step_ms)[1:2] : (0, 1)
    mode = active ? _window_mode(window_mode) : :mean_intersection
    limit = min(_walking_limit(max_walk_ms), UInt32(budget_ms))
    walking_index.resolution == graph.resolution && walking_index.cells == graph.h3 ||
        throw(ArgumentError("walking index does not match graph"))
    origins = H3.API.gridDisk(origin, radius)
    origins isa Vector{UInt64} || throw(ArgumentError("H3 origin disk failed"))
    sort!(filter!(!iszero, origins))
    weights = _population_rollup(population, graph.resolution)
    values = zeros(Float64, length(origins))
    isempty(weights) && return (; h3=origins, value=values, shared_expansions=0, query_expansions=0, workers=0)
    tile_size = samples == 1 ? 64 : min(8, length(origins))
    block_samples = fld(64, tile_size)
    time_blocks = cld(samples, block_samples)
    jobs = cld(length(origins), tile_size) * time_blocks
    workers = min(Threads.nthreads(:default), jobs)
    shared_geometry = workers > 1 ? WalkingGeometryCache() : nothing
    topologies = [WalkingTopology(walking_index, limit, shared_geometry) for _ in 1:workers]
    coverage = Dict{Int,Dict{UInt64,UInt64}}()
    expansions = queries = 0
    union_mode = mode in (:min_union, :diff_union)
    for wave in 1:workers:jobs
        tasks = @sync map(0:min(workers - 1, jobs - wave)) do worker
            Threads.@spawn begin
                tile, block = divrem(wave + worker - 1, time_blocks)
                ids = (tile * tile_size + 1):min((tile + 1) * tile_size, length(origins))
                times = (block * block_samples):min((block + 1) * block_samples - 1, samples - 1)
                lane_origins = repeat(origins[ids], length(times))
                lane_ready = UInt32[ready + sample * step for sample in times for _ in ids]
                cutoffs = lane_ready .+ UInt32(budget_ms)
                masks = [sum(UInt64(1) << (sample * length(ids) + i - 1)
                    for sample in 0:(length(times) - 1)) for i in 1:length(ids)]
                reached, shared, separate = _population_sample(graph, topologies[worker + 1],
                    lane_origins, lane_ready, cutoffs, weights)
                (; tile, block, ids, masks, reached, shared, separate)
            end
        end
        # Reduce in tile/time order. Only unfinished tiles retain coverage.
        for task in tasks
            (; tile, block, ids, masks, reached, shared, separate) = fetch(task)
            expansions += shared
            queries += separate
            if mode == :reachable_union
                totals = Dict{UInt64,Float64}()
                for (cell, bits) in reached
                    totals[bits] = get(totals, bits, 0.0) + weights[cell]
                end
                for (bits, total) in totals, i in eachindex(masks)
                    count = count_ones(bits & masks[i])
                    iszero(count) || (values[ids[i]] += total * (count / samples))
                end
            else
                projected = Dict{UInt64,UInt64}()
                for (cell, bits) in reached
                    origins_bits = UInt64(0)
                    for i in eachindex(masks)
                        selected = bits & masks[i]
                        (union_mode ? !iszero(selected) : selected == masks[i]) &&
                            (origins_bits |= UInt64(1) << (i - 1))
                    end
                    iszero(origins_bits) || (projected[cell] = origins_bits)
                end
                if block == 0
                    coverage[tile] = projected
                elseif union_mode
                    for (cell, bits) in projected
                        coverage[tile][cell] = get(coverage[tile], cell, UInt64(0)) | bits
                    end
                else
                    for (cell, bits) in coverage[tile]
                        bits &= get(projected, cell, UInt64(0))
                        if iszero(bits)
                            delete!(coverage[tile], cell)
                        else
                            coverage[tile][cell] = bits
                        end
                    end
                end
                block == time_blocks - 1 &&
                    _population_totals!(values, ids, pop!(coverage, tile), weights, 1)
            end
            all(isfinite, @view(values[ids])) || throw(ArgumentError("accessible population is not finite"))
        end
    end
    return (; h3=origins, value=values, shared_expansions=expansions,
            query_expansions=queries, workers)
end
