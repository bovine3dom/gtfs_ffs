export load_population, route_population

# Graph nodes are the weight prefix. The CSR contains only positive population destinations.
struct PreparedPopulation
    cells::Vector{UInt64}
    weights::Vector{Float64}
    node_count::Int
    offsets::Vector{Int}
    targets::Vector{Int32}
    durations::Vector{UInt32}
    walk_min::Vector{UInt32}
end

struct Population{H,P}
    h3::H
    weights::P
    rollups::Dict{Int,Dict{UInt64,Float64}}
    prepared::IdDict{WalkingIndex,PreparedPopulation}
    schedule_hints::IdDict{Graph,Matrix{Int32}}
    lock::ReentrantLock
end

function load_population(path::AbstractString; progress::Bool=false)
    table = _startup_stage(progress, "Opening population file $path") do _
        Arrow.Table(path)
    end
    all(name -> name in propertynames(table), (:h3, :population)) ||
        throw(ArgumentError("population input requires h3 and population columns"))
    population = _population(table.h3, table.population; progress)
    for resolution in 0:3
        _population_rollup(population, resolution; progress)
    end
    return population
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
    return Population(cells, weights, Dict{Int,Dict{UInt64,Float64}}(),
                      IdDict{WalkingIndex,PreparedPopulation}(), IdDict{Graph,Matrix{Int32}}(), ReentrantLock())
end

function _population_schedule_hints(population::Population, graph::Graph)
    return lock(population.lock) do
        get!(population.schedule_hints, graph) do
            hints = Matrix{Int32}(undef, 8, length(graph.edge_to))
            for edge in eachindex(graph.edge_to)
                lo, stop = graph.schedule_ptr[edge], graph.schedule_ptr[edge + 1]
                for bin in 1:8
                    time = UInt32(bin - 1) * div(PERIOD, UInt32(8))
                    while lo < stop && graph.departure[lo] < time
                        lo += Int32(1)
                    end
                    hints[bin, edge] = lo
                end
            end
            hints
        end
    end
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

function _prepare_population(population::Population, index::WalkingIndex; progress::Bool=false)
    adjacency = index.prepared
    isnothing(adjacency) && return nothing
    return lock(population.lock) do
        get!(population.prepared, index) do
            weights = _population_rollup(population, index.resolution; progress)
            _startup_stage(progress, "Preparing indexed population weights and walks") do _
                aligned = Float64[get(weights, h, 0.0) for h in adjacency.output_cells]
                offsets, targets, durations = Int[1], Int32[], UInt32[]
                walk_min = fill(INF, length(index.cells))
                output, network = adjacency.output, adjacency.graph
                for u in eachindex(index.cells)
                    hops = [j for j in output.offsets[u]:(output.offsets[u + 1] - 1)
                            if aligned[output.targets[j]] > 0]
                    sort!(hops; by=j -> output.durations[j])
                    append!(targets, output.targets[hops])
                    append!(durations, output.durations[hops])
                    isempty(hops) || (walk_min[u] = output.durations[first(hops)])
                    for j in network.offsets[u]:(network.offsets[u + 1] - 1)
                        walk_min[u] = min(walk_min[u], network.durations[j])
                    end
                    push!(offsets, length(targets) + 1)
                end
                PreparedPopulation(adjacency.output_cells, aligned, length(index.cells),
                                   offsets, targets, durations, walk_min)
            end
        end
    end
end

function _exclude_origin_population(params)
    text = get(params, "exclude_origin_population", "false")
    text in ("true", "false", "1", "0") ||
        throw(ArgumentError("exclude_origin_population must be true, false, 1 or 0"))
    return text in ("true", "1")
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

function _normalisation_mode(value)
    mode = value isa Symbol ? value : value isa AbstractString ? Symbol(value) : nothing
    mode in (:none, :pop) ||
        throw(ArgumentError("normalisation must be none or pop"))
    return mode
end

function _normalisation_radius_km(value)
    radius = value isa Real ? tryparse(Float64, string(value)) :
        value isa AbstractString ? tryparse(Float64, value) : nothing
    !isnothing(radius) && isfinite(radius) && !signbit(radius) ||
        throw(ArgumentError("normalisation_param must be finite, nonnegative kilometres"))
    return radius
end

function _population_normalisation(normalisation, normalisation_param=nothing)
    mode = _normalisation_mode(normalisation)
    mode == :none && return mode, nothing
    isnothing(normalisation_param) &&
        throw(ArgumentError("normalisation_param is required for normalisation=pop"))
    return mode, _normalisation_radius_km(normalisation_param)
end

const NORMALISATION_TARGET_GRID_RADIUS = 5

function _population_normalisation_grid_radius(cell, radius_km)
    iszero(radius_km) && return 0
    edges = H3.API.originToDirectedEdges(cell)
    edge = findfirst(H3.API.isValidDirectedEdge, edges)
    isnothing(edge) && throw(ArgumentError("could not determine the H3 edge length"))
    edge_km = H3.API.edgeLengthKm(edges[edge])
    isfinite(edge_km) && edge_km > 0 ||
        throw(ArgumentError("could not determine the H3 edge length"))
    grid_radius = ceil(radius_km / edge_km)
    isfinite(grid_radius) && grid_radius <= typemax(Cint) ||
        throw(ArgumentError("normalisation_param exceeds the H3 grid allocation range"))
    return _origin_radius(string(Int(grid_radius)))
end

function _population_normalisation_grid(origin, resolution, radius_km)
    selected_resolution = 0
    selected_radius = _population_normalisation_grid_radius(
        resolution == 0 ? origin : H3.API.cellToParent(origin, 0), radius_km)
    for candidate in 0:resolution
        cell = candidate == resolution ? origin : H3.API.cellToParent(origin, candidate)
        radius = _population_normalisation_grid_radius(cell, radius_km)
        if radius <= NORMALISATION_TARGET_GRID_RADIUS
            selected_resolution = candidate
            selected_radius = radius
        end
    end
    return selected_resolution, selected_radius
end

function _apply_population_normalisation(result, population, resolution, normalisation,
                                          normalisation_param, exclude_origin_population,
                                          radius=nothing, population_resolution=resolution)
    normalisation == :none && return result
    hasproperty(result, :h3) || return result
    origins = result.h3
    isempty(origins) && return merge(result, (origin_count=0,))
    radius = isnothing(radius) ?
        _population_normalisation_grid_radius(first(origins), normalisation_param) : radius
    centre_resolution = population_resolution
    weights = _population_rollup(population, centre_resolution)
    exact_weights = exclude_origin_population && centre_resolution != resolution ?
        _population_rollup(population, resolution) : weights
    totals = Dict{UInt64,Float64}()
    nearby = zeros(Float64, length(origins))
    for (i, origin) in enumerate(origins)
        centre = centre_resolution == resolution ? origin :
            H3.API.cellToParent(origin, centre_resolution)
        total = get!(totals, centre) do
            total = 0.0
            for cell in H3.API.gridDisk(centre, radius)
                iszero(cell) || (total += get(weights, cell, 0.0))
            end
            isfinite(total) || throw(ArgumentError("normalisation population is not finite"))
            total
        end
        nearby[i] = total
        if exclude_origin_population
            nearby[i] -= get(exact_weights, origin, 0.0)
            nearby[i] = max(nearby[i], 0.0)
        end
        isfinite(nearby[i]) || throw(ArgumentError("normalisation population is not finite"))
    end
    keep = findall(!iszero, nearby)
    return merge(result, (h3=origins[keep], value=result.value[keep] ./ nearby[keep],
        origin_count=length(origins)))
end

function _trip_population_jobs(origins::Integer, samples::Integer)
    origins <= 0 && return 0
    tile_size = samples == 1 ? 64 : min(16, origins)
    samples_per_block = fld(64, tile_size)
    return cld(origins, tile_size) * cld(samples, samples_per_block)
end

function _trip_population_worker_bytes(graph, origins::Integer, samples::Integer, destinations::Integer)
    origins <= 0 && return 65_536
    tile_size = samples == 1 ? 64 : min(16, origins)
    lanes = min(origins, tile_size) * min(samples, fld(64, tile_size))
    n, d, edges, lanes = UInt128.((length(graph.h3), destinations, length(graph.edge_to), lanes))
    bytes = UInt128(65_536) + 128n + 128d + 8edges + 12n * lanes
    bytes <= typemax(Int) || throw(PopulationMemoryError(bytes, typemax(Int)))
    return Int(bytes)
end

function _route_population_trip_reference(graph, population::Population, origin, departure_ms, budget_ms;
        origin_radius=0, window_ms=0, step_ms=60_000, max_walk_ms=3_600_000,
        window_mode=:mean_intersection, walking_index=WalkingIndex(graph), origin_batch_size=nothing,
        origins=nothing, exclude_origin_population::Bool=false,
        workers::Integer=Threads.nthreads(:default), stats=nothing)
    return _route_population_reference(graph, population, origin, departure_ms, budget_ms;
        origin_radius, window_ms, step_ms, max_walk_ms, window_mode, walking_index,
        origin_batch_size, exclude_origin_population, origins, workers, stats)
end

function _population_sample_trip(graph, topology, origins, ready, cutoffs, weights, stats=nothing)
    _with_trip_workspace(UInt128, length(graph.h3), length(origins)) do ws
        _population_trip_workspace(graph, topology, origins, ready, cutoffs, weights, stats, ws)
    end
end

function _population_trip_workspace(graph, topology, origins, ready, cutoffs, weights, stats, ws)
    queue = ws.queue
    settled = ws.settled
    reached = Dict{UInt64,UInt64}()
    used = typemax(UInt64) >> (64 - length(origins))
    best_any = reshape(ws.best, length(graph.h3), length(origins))
    transferred = reshape(ws.transferred, length(graph.h3), length(origins))
    best_walk = reshape(ws.walk, length(graph.h3), length(origins))
    dominance_cache = ws.dominance
    dominance_cache_limit = 200_000
    seen_trip = ws.seen_trip
    generation = 0
    function deadline_mask(time)
        first = searchsortedfirst(cutoffs, time)
        first > length(cutoffs) ? UInt64(0) : used & (typemax(UInt64) << (first - 1))
    end
    function enqueue(time, cell, trip, walk, mask)
        state_key = _population_state_key(cell, trip, walk)
        state_id = get(ws.ids, state_key, UInt32(0))
        mask &= deadline_mask(time) & ~(state_id == 0 ? UInt64(0) : settled[state_id])
        iszero(mask) && return
        node = get(graph.node_id, cell, Int32(0))
        if node != 0
            threshold = walk ? time : time < trip_connection_ms(graph) ? UInt32(0) :
                time - trip_connection_ms(graph)
            best = walk ? best_walk : best_any
            dominated = UInt64(0)
            lane_count = count_ones(mask)
            if lane_count >= 8
                cache_key = UInt128(mask) | (UInt128(UInt32(node)) << 64) |
                    (walk ? UInt128(1) << 96 : UInt128(0))
                cached = get(dominance_cache, cache_key, INF)
                if cached <= threshold
                    dominated = mask
                    isnothing(stats) || (stats.dominance_cache_hits += 1)
                else
                    max_best = UInt32(0)
                    bits = mask
                    while bits != 0
                        lane = trailing_zeros(bits) + 1
                        @inbounds value = best[node, lane]
                        max_best = max(max_best, value)
                        value <= threshold && (dominated |= UInt64(1) << (lane - 1))
                        bits &= bits - UInt64(1)
                    end
                    if max_best != INF && max_best < cached &&
                       (cached != INF || length(dominance_cache) < dominance_cache_limit)
                        dominance_cache[cache_key] = max_best
                    end
                end
            else
                bits = mask
                while bits != 0
                    lane = trailing_zeros(bits) + 1
                    @inbounds best[node, lane] <= threshold && (dominated |= UInt64(1) << (lane - 1))
                    bits &= bits - UInt64(1)
                end
            end
            mask &= ~dominated
            isnothing(stats) || (stats.state_dominance_discards += count_ones(dominated))
            iszero(mask) && return
            bits = mask
            while bits != 0
                lane = trailing_zeros(bits) + 1
                @inbounds best[node, lane] = min(best[node, lane], time)
                @inbounds best_any[node, lane] = min(best_any[node, lane], time)
                bits &= bits - UInt64(1)
            end
        end
        state_id == 0 && (state_id = _trip_id!(ws,state_key))
        # Most states have short pending lists. Long lists use a sparse fallback index.
        event = _pending_event(ws,state_id,time,stats)
        if event != 0
            isnothing(stats) || (stats.mask_merges += 1)
            ws.event_mask[event] |= mask
        else
            if isempty(ws.free_events)
                length(ws.event_state) < typemax(UInt32) || throw(ArgumentError("too many trip events"))
                push!(ws.event_state,state_id); push!(ws.event_mask,mask)
                push!(ws.event_time,time); push!(ws.event_next,UInt32(0)); push!(ws.event_prev,UInt32(0))
                event = UInt32(length(ws.event_state))
            else
                event = pop!(ws.free_events)
                ws.event_state[event] = state_id; ws.event_mask[event] = mask
                ws.event_time[event] = time
            end
            head = ws.heads[state_id]
            ws.event_next[event] = head; ws.event_prev[event] = UInt32(0)
            head == 0 || (ws.event_prev[head] = event)
            ws.heads[state_id] = event
            ws.indexed_pending[state_id] && (ws.pending_index[_pending_key(state_id,time)] = event)
            push!(queue, time, event)
            isnothing(stats) || (stats.state_enqueues += 1; stats.queue_peak = max(stats.queue_peak, length(queue)))
        end
    end
    function credit(cell, mask)
        iszero(mask) || get(weights, cell, 0.0) <= 0 ||
            (reached[cell] = get(reached, cell, UInt64(0)) | mask)
    end
    for (i, cell) in enumerate(origins)
        mask = UInt64(1) << (i - 1)
        enqueue(ready[i], cell, UInt32(0), false, mask)
        iszero(topology.limit) || enqueue(ready[i], cell, UInt32(0), true, mask)
    end
    expansions = queries = 0
    while !isempty(queue)
        time, event = pop!(queue)
        state_id = ws.event_state[event]
        previous_event, next_event = ws.event_prev[event], ws.event_next[event]
        if previous_event == 0
            ws.heads[state_id] = next_event
        else
            ws.event_next[previous_event] = next_event
        end
        next_event == 0 || (ws.event_prev[next_event] = previous_event)
        if ws.indexed_pending[state_id]
            delete!(ws.pending_index,_pending_key(state_id,time))
            ws.heads[state_id] == 0 && (ws.indexed_pending[state_id] = false)
        end
        isnothing(stats) || (stats.state_pops += 1)
        state_key = ws.keys[state_id]
        cell = _population_state_cell(state_key)
        current_trip = _population_state_trip(state_key)
        walk = _population_state_walk(state_key)
        previous = settled[state_id]
        mask = ws.event_mask[event] & ~previous
        push!(ws.free_events,event)
        if iszero(mask)
            isnothing(stats) || (stats.stale_pops += 1)
            continue
        end
        settled[state_id] = previous | mask
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
                enqueue(time + hop.duration_ms, target, UInt32(0), false, mask)
            end
        else
            node = get(graph.node_id, cell, Int32(0))
            iszero(node) && continue
            base = (time ÷ PERIOD) * PERIOD
            other_ready = _trip_other_ready(graph, time, current_trip, cutoff)
            transfer_mask = UInt64(0)
            bits = mask
            while bits != 0
                lane = trailing_zeros(bits) + 1
                if other_ready < transferred[node, lane]
                    transferred[node, lane] = other_ready
                    transfer_mask |= UInt64(1) << (lane - 1)
                end
                bits &= bits - UInt64(1)
            end
            isnothing(stats) || transfer_mask != 0 || (stats.transfer_scans_skipped += 1)
            for edge in _trip_edges(graph, node, current_trip, transfer_mask != 0)
                generation += 1
                isnothing(stats) || (stats.edge_queries += 1)
                target_node = graph.edge_to[edge]
                target = graph.h3[target_node]
                dominance_limit = _trip_lane_dominance_limit(graph, best_any, best_walk, target_node, mask)
                lower = time - base
                upper = cutoff - base

                if current_trip != 0
                    first_group, stop = _trip_group_range(graph, edge, current_trip, stats)
                    if first_group != 0
                        connection = _trip_group_lower_bound(graph, first_group, stop, lower, stats)
                        if connection < stop
                            isnothing(stats) || (stats.event_rows_scanned += 1)
                            @inbounds d = graph.departure[connection]
                            if d <= upper
                                isnothing(stats) || (stats.event_groups += 1)
                                @inbounds a = graph.arrival[connection]
                                if a <= upper
                                    arrival = base + a
                                    enqueue(arrival, target, current_trip, false, mask)
                                    enqueue(arrival, target, UInt32(0), true, mask)
                                end
                            end
                        end
                    end
                end

                if transfer_mask != 0
                    event_clock = current_trip == 0 ? lower : other_ready - base
                    slot = _trip_event_lower_bound(graph, edge, event_clock, stats)
                    stop = graph.trip_event_ptr[edge + Int32(1)]
                    while slot < stop
                        if _trip_suffix_reaches_limit(graph, slot, base, dominance_limit)
                            isnothing(stats) || (stats.event_dominance_breaks += 1)
                            break
                        end
                        connection = graph.trip_event_index[slot]
                        isnothing(stats) || (stats.event_rows_scanned += 1)
                        @inbounds d = graph.departure[connection]
                        d > upper && break
                        @inbounds next_trip = graph.trip_id[connection]
                        current_trip != 0 && next_trip == current_trip || begin
                            get(seen_trip, next_trip, 0) == generation || begin
                                @inbounds a = graph.arrival[connection]
                                seen_trip[next_trip] = generation
                                isnothing(stats) || (stats.event_groups += 1)
                                if a <= upper
                                    arrival = base + a
                                    enqueue(arrival, target, next_trip, false, transfer_mask)
                                    enqueue(arrival, target, UInt32(0), true, transfer_mask)
                                end
                            end
                        end
                        slot += Int32(1)
                    end
                end
            end
        end
    end
    return reached, expansions, queries
end

# Each bit is an origin/sample query. Cutoffs follow sample-major lane order.
function _population_sample(graph, topology, origins, ready, cutoffs, weights, stats=nothing)
    isnothing(graph.trip_id) || return _population_sample_trip(graph, topology, origins, ready, cutoffs, weights, stats)
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

function _route_population_reference(graph, population::Population, origin, departure_ms, budget_ms;
                          origin_radius=0, window_ms=0, step_ms=60_000,
                          max_walk_ms=3_600_000, window_mode=:mean_intersection,
                          walking_index=WalkingIndex(graph), origin_batch_size=nothing,
                          exclude_origin_population::Bool=false, origins=nothing,
                          workers::Integer=Threads.nthreads(:default), stats=nothing)
    workers > 0 || throw(ArgumentError("workers must be positive"))
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
    origins = isnothing(origins) ? H3.API.gridDisk(origin, radius) : origins
    origins isa Vector{UInt64} || throw(ArgumentError("H3 origin disk failed"))
    sort!(filter!(!iszero, origins))
    weights = _population_rollup(population, graph.resolution)
    values = zeros(Float64, length(origins))
    isempty(weights) && return (; h3=origins, value=values, shared_expansions=0, query_expansions=0, workers=0)
    isnothing(origin_batch_size) || (origin_batch_size isa Integer && 1 <= origin_batch_size <= 64) ||
        throw(ArgumentError("origin_batch_size must be in 1..64"))
    tile_size = isnothing(origin_batch_size) ? (samples == 1 ? 64 : min(16, length(origins))) :
        min(Int(origin_batch_size), length(origins))
    block_samples = fld(64, tile_size)
    time_blocks = cld(samples, block_samples)
    jobs = cld(length(origins), tile_size) * time_blocks
    workers = min(workers, Threads.nthreads(:default), jobs)
    shared_geometry = workers > 1 ? WalkingGeometryCache() : nothing
    topologies = [WalkingTopology(walking_index, limit, shared_geometry) for _ in 1:workers]
    coverage = Dict{Int,Dict{UInt64,UInt64}}()
    expansions = queries = 0
    union_mode = mode in (:min_union, :diff_union)
    for wave in 1:workers:jobs
        yield()
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
                task_stats = isnothing(stats) ? nothing : TripRouteStats()
                reached, shared, separate = _population_sample(graph, topologies[worker + 1],
                    lane_origins, lane_ready, cutoffs, weights, task_stats)
                (; tile, block, ids, masks, reached, shared, separate, stats=task_stats)
            end
        end
        # Reduce in tile/time order. Only unfinished tiles retain coverage.
        for task in tasks
            task_result = fetch(task)
            (; tile, block, ids, masks, reached, shared, separate) = task_result
            task_stats = task_result.stats
            expansions += shared
            queries += separate
            isnothing(stats) || _merge_trip_stats!(stats, task_stats)
            if exclude_origin_population
                for (i, id) in enumerate(ids)
                    cell = origins[id]
                    haskey(reached, cell) && (reached[cell] &= ~masks[i])
                end
            end
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
