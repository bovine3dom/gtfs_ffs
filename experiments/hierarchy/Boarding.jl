export prepare_boarding, route_boarding

struct BoardingGraph
    graph::R.Graph
    boarding::Vector{UInt8}
    arrival_child::Vector{UInt8}
    gaps::Vector{Matrix{UInt32}}
    walk_child::Vector{UInt8}
    hints::Matrix{Int32}
    history::Symbol
    correction::Bool
end
@inline Base.getproperty(g::BoardingGraph, s::Symbol) = s in fieldnames(BoardingGraph) ?
    getfield(g, s) : getproperty(getfield(g, :graph), s)
R._population_schedule_hints(population, g::BoardingGraph) = g.hints

"""Keep boarding-child profiles before the parent envelope removes alternatives."""
function prepare_boarding(h::Hierarchy)
    h.core_resolution in (6, 7, 8) || throw(ArgumentError("boarding requires resolution 6, 7, or 8"))
    f, nc = h.fine, h.core_nodes
    core_id = Dict(h.graph.h3[i] => Int32(i) for i in 1:nc)
    parent = Int32[core_id[H3.API.cellToParent(c, h.core_resolution)] for c in f.h3]
    children = [Int[] for _ in 1:nc]
    for u in sortperm(f.h3)
        push!(children[parent[u]], u)
    end
    tags = zeros(UInt8, length(f.h3))
    for group in children, (tag, u) in enumerate(group)
        tags[u] = UInt8(tag)
    end
    gaps = [fill(R.INF, length(group), length(group)) for group in children]
    for u in eachindex(f.h3)
        gaps[parent[u]][tags[u], tags[u]] = 0
    end
    network = h.walking.prepared.graph
    # The prepared fine network uses ceil-ms centre distances at 5 km/h.
    endpoints = Dict{Tuple{Int32,Int32},Tuple{UInt32,UInt64,UInt8}}()
    for u in eachindex(f.h3), j in network.offsets[u]:(network.offsets[u + 1] - 1)
        v, d = network.targets[j], network.durations[j]
        d <= h.max_walk_ms || continue
        parent[u] == parent[v] && (gaps[parent[u]][tags[u], tags[v]] = d)
        key, value = (parent[u], parent[v]), (d, f.h3[v], tags[v])
        endpoints[key] = min(get(endpoints, key, (R.INF, typemax(UInt64), UInt8(0))), value)
    end
    walk_child = UInt8[endpoints[(Int32(u), h.prepared.graph.targets[j])][3]
        for u in eachindex(h.graph.h3) for j in h.prepared.graph.offsets[u]:(h.prepared.graph.offsets[u + 1] - 1)]
    Event = Tuple{UInt32,UInt32,UInt8}
    function add!(row, u, walk, board)
        for e in f.out_ptr[u]:(f.out_ptr[u + 1] - 1)
            v = f.edge_to[e]
            events = get!(Vector{Event}, row, (parent[v], board))
            for j in f.schedule_ptr[e]:(f.schedule_ptr[e + 1] - 1)
                d = UInt32(mod(Int64(f.departure[j]) - walk, Int64(R.PERIOD)))
                a = d + (f.arrival[j] - f.departure[j]) + walk
                push!(events, (d, a, tags[v]))
            end
        end
    end
    origins = sort!(collect(keys(h.origins)); by=c -> h.origins[c])
    from, to, ptr, out = Int32[], Int32[], Int32[1], Int32[1]
    departure, arrival, board, destination = UInt32[], UInt32[], UInt8[], UInt8[]
    for u in eachindex(h.graph.h3)
        row = Dict{Tuple{Int32,UInt8},Vector{Event}}()
        if u <= nc
            for child in children[u]
                add!(row, child, UInt32(0), tags[child])
            end
        else
            cell = origins[u - nc]
            v = get(f.node_id, cell, Int32(0))
            iszero(v) || add!(row, v, UInt32(0), UInt8(0))
            for hop in R.walking_cells(h.walking, cell, h.max_walk_ms)
                v = get(f.node_id, hop.cell, Int32(0))
                iszero(v) || add!(row, v, hop.duration_ms, UInt8(0))
            end
        end
        for (v, tag) in sort!(collect(keys(row)))
            events = sort!(unique!(row[(v, tag)]); by=x -> (x[1], -Int64(x[2]), -Int(x[3])))
            retained = Event[]
            best = (R.INF, typemax(UInt8))
            for offset in (R.PERIOD, UInt32(0)), (d, a, child) in Iterators.reverse(events)
                if (a + offset, child) < best
                    push!(retained, (d + offset, a + offset, child))
                    best = (a + offset, child)
                end
            end
            push!(from, u); push!(to, v); push!(board, tag)
            for (d, a, child) in Iterators.reverse(retained)
                push!(departure, d); push!(arrival, a); push!(destination, child)
            end
            length(departure) < typemax(Int32) || throw(ArgumentError("too many boarding profiles"))
            push!(ptr, length(departure) + 1)
        end
        push!(out, length(to) + 1)
    end
    graph = R.Graph(h.graph.h3, h.graph.node_id, out, from, to, ptr, departure, arrival, 8, nothing)
    hints = R._population_schedule_hints(R._population(UInt64[], Float64[]), graph)
    return BoardingGraph(graph, board, destination, gaps, walk_child, hints, :descending, true)
end

struct BoardingWorkspace{W}
    base::W
    arrivals::Matrix{UInt32}
    child::Matrix{UInt8}
    used::Matrix{UInt32}
    range_pending::Vector{UInt64}
    range_queued::Vector{UInt32}
    groups::Vector{UInt64}
    snapshot_used::Vector{UInt32}
end
@inline Base.getproperty(w::BoardingWorkspace, s::Symbol) = s in fieldnames(typeof(w)) ?
    getfield(w, s) : getproperty(getfield(w, :base), s)
function BoardingWorkspace(base, lanes)
    states = length(base.settled)
    fill!(base.settled, 0); empty!(base.settled_ids)
    labels = size(base.arrivals) == (lanes, states) ? base.arrivals : Matrix{UInt32}(undef, lanes, states)
    fill!(labels, R.INF)
    return BoardingWorkspace(base, labels, zeros(UInt8, lanes, states), zeros(UInt32, lanes, states),
        zeros(UInt64, states), fill(R.INF, states), zeros(UInt64, 50), zeros(UInt32, lanes))
end

@inline function boarding_enqueue!(w, time, state, child, used, cutoff, mask=UInt64(1))
    time <= cutoff || return
    improved = UInt64(0)
    while !iszero(mask)
        lane = trailing_zeros(mask) + 1
        @inbounds if (time, used, child) < (w.arrivals[lane, state], w.used[lane, state], w.child[lane, state])
            w.arrivals[lane, state], w.used[lane, state], w.child[lane, state] = time, used, child
            improved |= UInt64(1) << (lane - 1)
        end
        mask &= mask - UInt64(1)
    end
    iszero(improved) && return
    iszero(w.settled[state]) && push!(w.settled_ids, state)
    w.settled[state] |= improved
    w.range_pending[state] |= improved
    if w.range_queued[state] != time
        push!(w.queue, (UInt64(time) << 32) | UInt64(state - 1))
        w.range_queued[state] = time
    end
    return nothing
end

function boarding_samples!(w, g, network, population, sources, ids, ready, cutoffs, limit)
    for id in w.reached_ids
        w.reached[id] = 0
    end
    empty!(w.reached_ids)
    count = length(ids)
    for (slot, i) in enumerate(ids)
        mask = sum(UInt64(1) << (lane - 1) for lane in slot:count:length(ready))
        for id in sources.direct[i]
            R._population_credit!(w, id, mask)
        end
    end
    shared = separate = 0
    for sample in reverse(0:(div(length(ready), count) - 1))
        offset = sample * count
        time, cutoff = ready[offset + 1], cutoffs[offset + 1]
        if g.history == :independent
            fill!(w.arrivals, R.INF)
            for state in w.settled_ids
                w.settled[state] = 0
            end
            empty!(w.settled_ids)
        end
        for (slot, i) in enumerate(ids)
            boarding_enqueue!(w, time, 2Int(sources.sources[i]) - 1, UInt8(0), UInt32(0), cutoff, UInt64(1) << (slot - 1))
        end
        while !isempty(w.queue)
            key = pop!(w.queue)
            time, state = UInt32(key >> 32), Int(key % UInt32) + 1
            w.range_queued[state] == time && (w.range_queued[state] = R.INF)
            pending, valid, present = w.range_pending[state], UInt64(0), UInt64(0)
            fill!(w.groups, 0)
            # Snapshot before any self-edge can change the winning metadata.
            while !iszero(pending)
                lane = trailing_zeros(pending) + 1
                bit = UInt64(1) << (lane - 1)
                @inbounds if w.arrivals[lane, state] == time
                    valid |= bit
                    child = w.child[lane, state]
                    w.groups[child + 1] |= bit
                    present |= UInt64(1) << child
                    w.snapshot_used[lane] = w.used[lane, state]
                end
                pending &= pending - UInt64(1)
            end
            w.range_pending[state] &= ~valid
            iszero(valid) && continue
            shared += 1; separate += count_ones(valid)
            u = (state + 1) >> 1
            if iseven(state)
                for j in network.offsets[u]:(network.offsets[u + 1] - 1)
                    d = network.durations[j]
                    d <= min(limit, cutoff - time) || continue
                    boarding_enqueue!(w, time + d, 2Int(network.targets[j]) - 1, g.walk_child[j], d, cutoff, valid)
                end
            else
                for e in g.out_ptr[u]:(g.out_ptr[u + 1] - 1)
                    v, board, groups = Int(g.edge_to[e]), g.boarding[e], present
                    while !iszero(groups)
                        child = trailing_zeros(groups)
                        groups &= groups - UInt64(1)
                        gap = !g.correction || iszero(board) ? UInt32(0) : g.gaps[u][child, board]
                        gap <= min(limit, cutoff - time) || continue
                        pending, eligible = w.groups[child + 1], UInt64(0)
                        while !iszero(pending)
                            lane = trailing_zeros(pending) + 1
                            @inbounds if w.snapshot_used[lane] <= limit - gap &&
                                    time <= max(w.arrivals[lane, 2v - 1], w.arrivals[lane, 2v])
                                eligible |= UInt64(1) << (lane - 1)
                            end
                            pending &= pending - UInt64(1)
                        end
                        iszero(eligible) && continue
                        clock = time + gap
                        base, relative = div(clock, R.PERIOD) * R.PERIOD, clock % R.PERIOD
                        bin = Int(div(relative, div(R.PERIOD, UInt32(8)))) + 1
                        lo, stop = g.hints[bin, e], g.schedule_ptr[e + 1]
                        hi = bin == 8 ? stop : min(stop - Int32(1), g.hints[bin + 1, e]) + Int32(1)
                        while lo < hi
                            mid = lo + ((hi - lo) >> 1)
                            if g.departure[mid] < relative
                                lo = mid + Int32(1)
                            else
                                hi = mid
                            end
                        end
                        lo == stop && continue
                        g.arrival[lo] <= cutoff - base || continue
                        arrival, tag = base + g.arrival[lo], g.arrival_child[lo]
                        boarding_enqueue!(w, arrival, 2v - 1, tag, UInt32(0), cutoff, eligible)
                        boarding_enqueue!(w, arrival, 2v, tag, UInt32(0), cutoff, eligible)
                    end
                end
            end
        end
        for state in w.settled_ids
            iseven(state) || continue
            u, pending = state >> 1, w.settled[state]
            first = population.offsets[u]
            first < population.offsets[u + 1] || continue
            while !iszero(pending)
                lane = trailing_zeros(pending) + 1
                time, group = w.arrivals[lane, state], UInt64(1) << (lane - 1)
                pending &= pending - UInt64(1)
                time <= cutoff && population.durations[first] <= min(limit, cutoff - time) || continue
                others = pending
                while !iszero(others)
                    lane = trailing_zeros(others) + 1
                    w.arrivals[lane, state] == time && (group |= UInt64(1) << (lane - 1))
                    others &= others - UInt64(1)
                end
                pending &= ~group
                event = w.heads[u]
                if event != 0 && w.times[event] == time
                    w.masks[event] |= group << offset
                else
                    event == 0 && push!(w.egress_nodes, Int32(u))
                    push!(w.times, time); push!(w.masks, group << offset); push!(w.links, event)
                    w.heads[u] = length(w.times)
                end
            end
        end
    end
    R._population_cover!(w, population, cutoffs, limit)
    return shared, separate
end
R._population_sample_range!(w::BoardingWorkspace, g::BoardingGraph, network, population, sources, ids, ready, cutoffs, limit, labels) =
    boarding_samples!(w, g, network, population, sources, ids, ready, cutoffs, limit)
R._population_sample_packed!(w::BoardingWorkspace, g::BoardingGraph, network, population, sources, ids, ready, cutoffs, limit) =
    boarding_samples!(w, g, network, population, sources, ids, ready, cutoffs, limit)

function boarding_sample!(w, g, h, source, ready, cutoff, limit)
    sources = (; sources=[source], direct=[Int32[]])
    point = BoardingGraph(g.graph, g.boarding, g.arrival_child, g.gaps, g.walk_child, g.hints, :descending, g.correction)
    return boarding_samples!(w, point, h.prepared.graph, h.destinations, sources, 1:1, UInt32[ready], UInt32[cutoff], limit)
end

# Dispatch only on experimental types. The production scheduler remains unchanged.
struct BoardingIndex{H}
    hierarchy::H
    prepared::@NamedTuple{graph::R.PackedWalking{Int32}, output_id::Dict{UInt64,Int32}}
end

function R._population_tile!(credit, g::BoardingGraph, network, population, sources, ids,
        ready, budget, step, samples, limit, mode, own_ids=nothing)
    w = BoardingWorkspace(credit, length(ids))
    # Select the unchanged generic reducer, not this experimental graph method.
    return invoke(R._population_tile!, NTuple{13,Any}, w, g, network, population, sources, ids,
        ready, budget, step, samples, limit, mode, own_ids)
end
R._population_sources(h::BoardingIndex, population, weights, origins, limit) =
    R._population_sources(h.hierarchy, population, weights, origins, limit)

"""Route with one child-tagged winner per parent state. Production defaults do not change."""
function route_boarding(h::Hierarchy, candidate::BoardingGraph, origins, departure_ms, budget_ms;
        window_ms=0, step_ms=60_000, window_mode=:mean_intersection,
        max_walk_ms=h.max_walk_ms, exclude_origin_population=false, origin_batch_size=nothing,
        history=:descending, correction=true)
    history in (:descending, :independent) || throw(ArgumentError("unknown boarding history mode"))
    candidate.graph.h3 === h.graph.h3 || throw(ArgumentError("boarding profiles do not match hierarchy"))
    if iszero(max_walk_ms) || iszero(budget_ms)
        return route_hierarchy(h, origins, departure_ms, budget_ms; window_ms, step_ms, window_mode,
            max_walk_ms, exclude_origin_population, origin_batch_size)
    end
    origins = selected_cells(origins, 8)
    all(o -> haskey(h.origins, o), origins) || throw(ArgumentError("origin was not prepared"))
    ready, _ = R.query_times(h.fine, first(origins), departure_ms, budget_ms)
    window_ms >= 0 && step_ms >= 0 || throw(ArgumentError("window and step must be nonnegative"))
    mode = R._window_mode(window_mode)
    step, samples = window_ms > 0 && step_ms > 0 ? R._window_times(ready, budget_ms, window_ms, step_ms)[1:2] : (0, 1)
    max_walk_ms == h.max_walk_ms || throw(ArgumentError("walking limit differs from prepared profiles"))
    isnothing(origin_batch_size) || (origin_batch_size isa Integer && 1 <= origin_batch_size <= 64) ||
        throw(ArgumentError("origin_batch_size must be in 1..64"))
    g = BoardingGraph(candidate.graph, candidate.boarding, candidate.arrival_child,
        candidate.gaps, candidate.walk_child, candidate.hints, history, correction)
    index = BoardingIndex(h, h.prepared)
    result = R._route_population_origins(g, index, h.population, h.destinations,
        R._population_rollup(h.population, 8), origins, ready, budget_ms, step, samples,
        min(UInt32(max_walk_ms), UInt32(budget_ms)), mode, origin_batch_size, exclude_origin_population)
    return merge(result, (; backend=:hierarchy_boarding, core_resolution=h.core_resolution, history, correction))
end
