export CoarseRouter, prepare_coarse_router, route_coarse_population, route_coarse_time

struct CoarseRouter{P}
    fine::Graph
    walking::WalkingIndex
    core_resolution::Int
    population::P
    h3::Vector{UInt64}
    parents::Vector{Int32}
    tags::Vector{UInt16}
    children::Vector{Vector{Int32}}
    out_ptr::Vector{Int32}
    edge_to::Vector{Int32}
    boarding::Vector{UInt16}
    schedule_ptr::Vector{Int32}
    refs::Vector{Int32}
    arrival_child::Vector{UInt16}
    hints::Matrix{Int32}
    gaps::Vector{Matrix{UInt32}}
    walk_child::Vector{UInt16}
    prepared::@NamedTuple{graph::PackedWalking{Int32}, output_id::Dict{UInt64,Int32}}
    destinations::Union{Nothing,PreparedPopulation}
end

"""Prepare one global fine-access model. Query origins are not part of this index."""
function prepare_coarse_router(fine::Graph, walking::WalkingIndex, target::Integer;
        population=nothing, progress=false)
    6 <= fine.resolution <= 8 && 5 <= target < fine.resolution ||
        throw(ArgumentError("coarse routing requires 5 <= target < fine resolution <= 8"))
    walking.cells == fine.h3 && walking.resolution == fine.resolution && !isnothing(walking.prepared) ||
        throw(ArgumentError("prepare matching fine walking geometry first"))
    return _startup_stage(progress, "Preparing global res$(fine.resolution) to res$target router") do _
        parent_cells = H3.API.cellToParent.(fine.h3, target)
        cells = sort!(unique(parent_cells))
        ids = Dict(h => Int32(i) for (i, h) in enumerate(cells))
        parents = Int32[ids[h] for h in parent_cells]
        children = [Int32[] for _ in cells]
        tags = zeros(UInt16, length(fine.h3))
        for u in sortperm(fine.h3)
            group = children[parents[u]]
            push!(group, u)
            tags[u] = UInt16(length(group))
        end
        # Merge existing two-day profiles. References retain the selected fine km.
        out, to, board, ptr, refs, arrival_child = Int32[1], Int32[], UInt16[], Int32[1], Int32[], UInt16[]
        heap = BinaryMinHeap{Tuple{Int64,UInt32,UInt16,Int32,Int32}}()
        event(p, e) = (-Int64(fine.departure[p]), fine.arrival[p], tags[fine.edge_to[e]], p, e)
        for group in children
            edges = Int32[e for u in group for e in fine.out_ptr[u]:(fine.out_ptr[u + 1] - 1)]
            sort!(edges; by=e -> (parents[fine.edge_to[e]], tags[fine.edge_from[e]]))
            first = 1
            while first <= length(edges)
                e = edges[first]
                key = (parents[fine.edge_to[e]], tags[fine.edge_from[e]])
                last = first
                while last <= length(edges)
                    e = edges[last]
                    (parents[fine.edge_to[e]], tags[fine.edge_from[e]]) == key || break
                    p = fine.schedule_ptr[e + 1] - Int32(1)
                    p >= fine.schedule_ptr[e] && push!(heap, event(p, e))
                    last += 1
                end
                start = length(refs) + 1
                best = (INF, typemax(UInt16))
                while !isempty(heap)
                    _, a, child, p, e = pop!(heap)
                    if (a, child) < best
                        push!(refs, p); push!(arrival_child, child)
                        best = (a, child)
                    end
                    p > fine.schedule_ptr[e] && push!(heap, event(p - Int32(1), e))
                end
                reverse!(refs, start, length(refs)); reverse!(arrival_child, start, length(refs))
                length(refs) < typemax(Int32) || throw(ArgumentError("too many coarse profiles"))
                push!(to, key[1]); push!(board, key[2]); push!(ptr, length(refs) + 1)
                first = last
            end
            push!(out, length(to) + 1)
        end
        hints = Matrix{Int32}(undef, 8, length(to))
        for e in eachindex(to)
            p, stop = ptr[e], ptr[e + 1]
            for bin in 1:8
                clock = UInt32(bin - 1) * div(PERIOD, UInt32(8))
                while p < stop && fine.departure[refs[p]] < clock
                    p += Int32(1)
                end
                hints[bin, e] = p
            end
        end
        gaps = [fill(INF, length(group), length(group)) for group in children]
        network = walking.prepared.graph
        offsets, targets, durations, distances, walk_child = Int[1], Int32[], UInt32[], Float64[], UInt16[]
        fp = isnothing(population) ? nothing : _prepare_population(population, walking)
        poffsets, ptargets, pdurations, walk_min = Int[1], Int32[], UInt32[], UInt32[]
        for (parent, group) in enumerate(children)
            walks = Dict{Int32,Tuple{UInt32,UInt64,Float64,UInt16}}()
            output = Dict{Int32,UInt32}()
            for u in group
                gaps[parent][tags[u], tags[u]] = 0
                for j in network.offsets[u]:(network.offsets[u + 1] - 1)
                    v, d = network.targets[j], network.durations[j]
                    parents[v] == parent && (gaps[parent][tags[u], tags[v]] = d)
                    value = (d, fine.h3[v], network.distances[j], tags[v])
                    old = get(walks, parents[v], (INF, typemax(UInt64), Inf, UInt16(0)))
                    walks[parents[v]] = min(old, value)
                end
                if !isnothing(fp)
                    fp.weights[u] > 0 && (output[u] = 0)
                    for j in fp.offsets[u]:(fp.offsets[u + 1] - 1)
                        v, d = fp.targets[j], fp.durations[j]
                        output[v] = min(get(output, v, INF), d)
                    end
                end
            end
            for (v, (d, _, km, child)) in sort!(collect(walks); by=p -> (last(p)[1], first(p)))
                push!(targets, v); push!(durations, d); push!(distances, km); push!(walk_child, child)
            end
            push!(offsets, length(targets) + 1)
            for (v, d) in sort!(collect(output); by=p -> (last(p), first(p)))
                push!(ptargets, v); push!(pdurations, d)
            end
            push!(poffsets, length(ptargets) + 1)
            push!(walk_min, min(minimum(values(output); init=INF), minimum((v[1] for v in values(walks)); init=INF)))
        end
        packed = PackedWalking(offsets, targets, durations, distances)
        destinations = isnothing(fp) ? nothing : PreparedPopulation(fp.cells, fp.weights, length(cells),
            poffsets, ptargets, pdurations, walk_min)
        CoarseRouter(fine, walking, Int(target), population, cells, parents, tags, children, out, to,
            board, ptr, refs, arrival_child, hints, gaps, walk_child,
            (; graph=packed, output_id=walking.prepared.output_id), destinations)
    end
end

function _coarse_access(model, origins, limit)
    fine, wi = model.fine, model.walking
    access = [Tuple{Int32,UInt32,Float64}[] for _ in origins]
    hops = Vector{Vector{WalkingNeighbor}}(undef, length(origins))
    Threads.@threads for i in eachindex(origins)
        cell = origins[i]
        u = get(fine.node_id, cell, Int32(0))
        iszero(u) || push!(access[i], (u, UInt32(0), 0.0))
        hops[i] = walking_cells(wi, cell, limit)
        for hop in hops[i]
            v = get(fine.node_id, hop.cell, Int32(0))
            iszero(v) || push!(access[i], (v, hop.duration_ms, hop.distance_km))
        end
    end
    return access, hops
end

function _population_sources(model::CoarseRouter, prepared, weights, origins, limit)
    access_fine, hops = _coarse_access(model, origins, limit)
    direct = [Int32[] for _ in origins]
    extra, extra_weights = Dict{UInt64,Int32}(), Float64[]
    function destination(cell)
        id = get(model.prepared.output_id, cell, Int32(0))
        !iszero(id) && return id
        get!(extra, cell) do
            push!(extra_weights, weights[cell])
            Int32(length(prepared.weights) + length(extra_weights))
        end
    end
    for (i, cell) in enumerate(origins)
        get(weights, cell, 0.0) > 0 && push!(direct[i], destination(cell))
        for hop in hops[i]
            get(weights, hop.cell, 0.0) > 0 && push!(direct[i], destination(hop.cell))
        end
    end
    # The generic scheduler sees parent IDs only; the kernel seeds real fine legs.
    access = [[(model.parents[u], UInt32(0)) for (u, _, _) in row
        if model.fine.out_ptr[u] < model.fine.out_ptr[u + 1]] for row in access_fine]
    return (; sources=zeros(Int32, length(origins)), direct, access, access_fine,
        weights=isempty(extra_weights) ? prepared.weights : vcat(prepared.weights, extra_weights))
end
_population_schedule_hints(population, model::CoarseRouter) = model.hints

struct CoarseWorkspace{W,D}
    base::W
    arrivals::Matrix{UInt32}
    child::Matrix{UInt16}
    used::Matrix{UInt32}
    range_pending::Vector{UInt64}
    range_queued::Vector{UInt32}
    groups::Vector{UInt64}
    active::Vector{UInt16}
    snapshot_used::Vector{UInt32}
    distances::D
    snapshot_km::Vector{Float64}
    lookups::Base.RefValue{Int}
end
@inline Base.getproperty(w::CoarseWorkspace, s::Symbol) = s in fieldnames(typeof(w)) ?
    getfield(w, s) : getproperty(getfield(w, :base), s)
function CoarseWorkspace(base, lanes, model, track=false)
    states = 2length(model.h3)
    labels = size(base.arrivals) == (lanes, states) ? base.arrivals : Matrix{UInt32}(undef, lanes, states)
    fill!(labels, INF); fill!(base.settled, 0); empty!(base.settled_ids)
    CoarseWorkspace(base, labels, zeros(UInt16, lanes, states), zeros(UInt32, lanes, states),
        zeros(UInt64, states), fill(INF, states), zeros(UInt64, maximum(length, model.children; init=0) + 1),
        UInt16[], zeros(UInt32, lanes), track ? fill(NaN, lanes, states) : nothing, zeros(lanes), Ref(0))
end

@inline function _coarse_enqueue!(w, time, state, child, used, cutoff, mask, km=0.0)
    time <= cutoff || return
    improved = UInt64(0)
    while !iszero(mask)
        lane = trailing_zeros(mask) + 1
        @inbounds if (time, used, child) < (w.arrivals[lane, state], w.used[lane, state], w.child[lane, state]) ||
                (!isnothing(w.distances) && (time, used, child) == (w.arrivals[lane, state], w.used[lane, state], w.child[lane, state]) && km < w.distances[lane, state])
            w.arrivals[lane, state], w.used[lane, state], w.child[lane, state] = time, used, child
            isnothing(w.distances) || (w.distances[lane, state] = km)
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
end

@inline function _coarse_connection(g, e, clock, cutoff)
    base, relative = div(clock, PERIOD) * PERIOD, clock % PERIOD
    bin = Int(div(relative, div(PERIOD, UInt32(8)))) + 1
    @inbounds lo, stop = g.hints[bin, e], g.schedule_ptr[e + 1]
    @inbounds hi = bin == 8 ? stop : min(stop - Int32(1), g.hints[bin + 1, e]) + Int32(1)
    while lo < hi
        mid = lo + ((hi - lo) >> 1)
        @inbounds if g.fine.departure[g.refs[mid]] < relative
            lo = mid + Int32(1)
        else
            hi = mid
        end
    end
    lo == stop && return Int32(0)
    @inbounds g.fine.arrival[g.refs[lo]] <= cutoff - base || return Int32(0)
    return lo
end

function _coarse_expand!(w, g, access, ids, ready, cutoff, limit)
    f, network = g.fine, g.prepared.graph
    for (slot, i) in enumerate(ids), (u, d, km) in access[i]
        d <= min(limit, cutoff - ready) || continue
        clock = ready + d
        for e in f.out_ptr[u]:(f.out_ptr[u + 1] - 1)
            w.lookups[] += 1
            p = next_connection(f.schedule_ptr, f.departure, f.arrival, e, clock, cutoff)
            iszero(p) && continue
            arrival = div(clock, PERIOD) * PERIOD + f.arrival[p]
            v = f.edge_to[e]
            total = isnothing(w.distances) ? 0.0 : isnothing(f.distance_km) ? NaN : km + f.distance_km[p]
            mask = UInt64(1) << (slot - 1)
            _coarse_enqueue!(w, arrival, 2Int(g.parents[v])-1, g.tags[v], UInt32(0), cutoff, mask, total)
            _coarse_enqueue!(w, arrival, 2Int(g.parents[v]), g.tags[v], UInt32(0), cutoff, mask, total)
        end
    end
    shared = separate = 0
    while !isempty(w.queue)
        key = pop!(w.queue)
        time, state = UInt32(key >> 32), Int(key % UInt32) + 1
        w.range_queued[state] == time && (w.range_queued[state] = INF)
        pending, valid = w.range_pending[state], UInt64(0)
        for child in w.active
            w.groups[child + 1] = 0
        end
        empty!(w.active)
        while !iszero(pending)
            lane = trailing_zeros(pending) + 1
            bit = UInt64(1) << (lane - 1)
            @inbounds if w.arrivals[lane, state] == time
                valid |= bit
                child = w.child[lane, state]
                iszero(w.groups[child + 1]) && push!(w.active, child)
                w.groups[child + 1] |= bit
                w.snapshot_used[lane] = w.used[lane, state]
                isnothing(w.distances) || (w.snapshot_km[lane] = w.distances[lane, state])
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
                km = isnothing(w.distances) ? 0.0 : w.snapshot_km[1] + network.distances[j]
                _coarse_enqueue!(w, time+d, 2Int(network.targets[j])-1, g.walk_child[j], d, cutoff, valid, km)
            end
        else
            sort!(w.active)
            for e in g.out_ptr[u]:(g.out_ptr[u + 1] - 1), child in w.active
                v, board = Int(g.edge_to[e]), g.boarding[e]
                gap = g.gaps[u][child, board]
                gap <= min(limit, cutoff - time) || continue
                pending, eligible = w.groups[child + 1], UInt64(0)
                while !iszero(pending)
                    lane = trailing_zeros(pending) + 1
                    @inbounds if w.snapshot_used[lane] <= limit-gap &&
                            time <= max(w.arrivals[lane, 2v-1], w.arrivals[lane, 2v])
                        eligible |= UInt64(1) << (lane - 1)
                    end
                    pending &= pending - UInt64(1)
                end
                iszero(eligible) && continue
                clock = time + gap
                w.lookups[] += 1
                p = _coarse_connection(g, e, clock, cutoff)
                iszero(p) && continue
                arrival = div(clock, PERIOD) * PERIOD + f.arrival[g.refs[p]]
                km = 0.0
                if !isnothing(w.distances)
                    a, b = g.children[u][child], g.children[u][board]
                    gap_km = H3.Lib.greatCircleDistanceKm(Ref(g.walking.centres[a]), Ref(g.walking.centres[b]))
                    km = isnothing(f.distance_km) ? NaN : w.snapshot_km[1] + gap_km + f.distance_km[g.refs[p]]
                    isinf(km) && throw(ArgumentError("accumulated route distance is not finite"))
                end
                tag = g.arrival_child[p]
                _coarse_enqueue!(w, arrival, 2v-1, tag, UInt32(0), cutoff, eligible, km)
                _coarse_enqueue!(w, arrival, 2v, tag, UInt32(0), cutoff, eligible, km)
            end
        end
    end
    return shared, separate
end

function _coarse_samples!(w, g, population, sources, ids, ready, cutoffs, limit)
    for id in w.reached_ids
        w.reached[id] = 0
    end
    empty!(w.reached_ids)
    count = length(ids)
    for (slot, i) in enumerate(ids)
        mask = sum(UInt64(1) << (lane-1) for lane in slot:count:length(ready))
        for id in sources.direct[i]
            _population_credit!(w, id, mask)
        end
    end
    shared = separate = 0
    for sample in reverse(0:(div(length(ready), count)-1))
        offset = sample * count
        time, cutoff = ready[offset+1], cutoffs[offset+1]
        a, b = _coarse_expand!(w, g, sources.access_fine, ids, time, cutoff, limit)
        shared += a; separate += b
        for state in w.settled_ids
            iseven(state) || continue
            u, pending = state >> 1, w.settled[state]
            first = population.offsets[u]
            first < population.offsets[u+1] || continue
            while !iszero(pending)
                lane = trailing_zeros(pending) + 1
                time, group = w.arrivals[lane, state], UInt64(1) << (lane-1)
                pending &= pending-UInt64(1)
                time <= cutoff && population.durations[first] <= min(limit, cutoff-time) || continue
                others = pending
                while !iszero(others)
                    lane = trailing_zeros(others) + 1
                    w.arrivals[lane, state] == time && (group |= UInt64(1) << (lane-1))
                    others &= others-UInt64(1)
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
    _population_cover!(w, population, cutoffs, limit)
    return shared, separate
end
_population_sample_range!(w::CoarseWorkspace, g::CoarseRouter, network, population, sources, ids, ready, cutoffs, limit, labels) =
    _coarse_samples!(w, g, population, sources, ids, ready, cutoffs, limit)
_population_sample_packed!(w::CoarseWorkspace, g::CoarseRouter, network, population, sources, ids, ready, cutoffs, limit) =
    _coarse_samples!(w, g, population, sources, ids, ready, cutoffs, limit)
function _population_tile!(credit, g::CoarseRouter, network, population, sources, ids,
        ready, budget, step, samples, limit, mode, own_ids=nothing)
    w = CoarseWorkspace(credit, length(ids), g)
    invoke(_population_tile!, NTuple{13,Any}, w, g, network, population, sources, ids,
        ready, budget, step, samples, limit, mode, own_ids)
end

function route_coarse_population(model::CoarseRouter, origins, departure_ms, budget_ms;
        window_ms=0, step_ms=60_000, max_walk_ms=model.walking.prepared.limit,
        window_mode=:mean_intersection, exclude_origin_population=false, origin_batch_size=nothing)
    isnothing(model.population) && throw(ArgumentError("coarse model has no population input"))
    origins = sort!(unique(collect(origins)))
    isempty(origins) && throw(ArgumentError("origins must not be empty"))
    foreach(h -> validate_cell(h, model.fine.resolution), origins)
    ready, _ = query_times(model.fine, first(origins), departure_ms, budget_ms)
    window_ms isa Integer && window_ms >= 0 && step_ms isa Integer && step_ms >= 0 ||
        throw(ArgumentError("window and step must be nonnegative integers"))
    mode = _window_mode(window_mode)
    step, samples = window_ms > 0 && step_ms > 0 ? _window_times(ready, budget_ms, window_ms, step_ms)[1:2] : (0, 1)
    limit = min(_walking_limit(max_walk_ms), UInt32(budget_ms))
    limit <= model.walking.prepared.limit || throw(ArgumentError("walking limit exceeds prepared geometry"))
    isnothing(origin_batch_size) || (origin_batch_size isa Integer && 1 <= origin_batch_size <= 64) ||
        throw(ArgumentError("origin_batch_size must be in 1..64"))
    fallback = iszero(limit)
    graph, index, prepared = fallback ? (model.fine, model.walking, _prepare_population(model.population, model.walking)) :
        (model, model, model.destinations)
    result = _route_population_origins(graph, index, model.population, prepared,
        _population_rollup(model.population, model.fine.resolution), origins, ready, budget_ms, step,
        samples, limit, mode, origin_batch_size, exclude_origin_population)
    return merge(result, (; backend=fallback ? :fine_fallback : :coarse, core_resolution=model.core_resolution))
end

function _coarse_time_point(w, model, origin, hops, ready, cutoff, limit, track)
    result = Dict{UInt64,Tuple{UInt32,Float64}}(origin => (ready, 0.0))
    for hop in hops
        hop.duration_ms <= min(limit, cutoff-ready) || continue
        result[hop.cell] = (ready+hop.duration_ms, hop.distance_km)
    end
    geographic = model.walking.prepared.geographic
    for state in w.settled_ids
        iseven(state) || continue
        time = w.arrivals[1, state]
        time <= cutoff || continue
        km = track ? w.distances[1, state] : 0.0
        for u in model.children[state >> 1]
            cell = model.fine.h3[u]
            time < get(result, cell, (INF, NaN))[1] && (result[cell] = (time, km))
            for j in geographic.offsets[u]:(geographic.offsets[u+1]-1)
                d = geographic.durations[j]
                d <= min(limit, cutoff-time) || continue
                cell, arrival = geographic.targets[j], time+d
                arrival < get(result, cell, (INF, NaN))[1] || continue
                total = km + geographic.distances[j]
                isinf(total) && throw(ArgumentError("accumulated route distance is not finite"))
                result[cell] = (arrival, total)
            end
        end
    end
    h3 = sort!(collect(keys(result)))
    return (; h3, arrival=UInt32[result[h][1] for h in h3],
        distance_km=track ? Float64[result[h][2] for h in h3] : _od_distances(origin, h3))
end

function route_coarse_time(model::CoarseRouter, origin, ready, budget, window, step,
        max_walk_ms, distance_mode, window_mode)
    ready, _ = query_times(model.fine, origin, ready, budget)
    mode, distance = _window_mode(window_mode), _distance_mode(distance_mode)
    window isa Integer && window >= 0 && step isa Integer && step >= 0 ||
        throw(ArgumentError("window and step must be nonnegative integers"))
    limit = min(_walking_limit(max_walk_ms), UInt32(budget))
    iszero(limit) && return _route_request(model.fine, model.walking, origin, ready, budget,
        window, step, max_walk_ms, distance, mode)
    limit <= model.walking.prepared.limit || throw(ArgumentError("walking limit exceeds prepared geometry"))
    interval, samples = window > 0 ? _window_times(ready, budget, window, step)[1:2] : (0, 1)
    track = distance == :itinerary
    access, hops = _coarse_access(model, [origin], limit)
    w = CoarseWorkspace(PopulationWorkspace(length(model.h3), 0), 1, model, track)
    acc = _walking_window_accumulator(track, mode)
    for sample in reverse(0:(samples-1))
        clock = UInt32(ready + sample*interval)
        cutoff = clock + UInt32(budget)
        _coarse_expand!(w, model, access, 1:1, clock, cutoff, limit)
        point = _coarse_time_point(w, model, origin, only(hops), clock, cutoff, limit, track)
        window == 0 && return merge(point, (; backend="coarse"))
        # Samples run backwards. Equal extrema must retain the earlier departure's km.
        if track && mode in (:min_union, :max_intersection, :diff_union, :diff_intersection)
            stats = acc isa NamedTuple ? acc.stats : acc
            best = mode != :max_intersection
            for (i, cell) in enumerate(point.h3)
                previous = get(stats, cell, nothing)
                isnothing(previous) && continue
                elapsed = point.arrival[i] - clock
                if elapsed == previous[best ? 4 : 5]
                    stats[cell] = (previous[1], previous[2], point.distance_km[i], previous[4:end]...)
                end
            end
        end
        _accumulate_walking!(acc, point, clock, UInt32(budget), samples)
    end
    return _finish_walking_window(acc, samples; budget=UInt32(budget), origin, backend="coarse",
        searches=samples, reused_samples=0, full_searches=1, repair_searches=samples-1,
        profile_lookups=w.lookups[], workers=1)
end
