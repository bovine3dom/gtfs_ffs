@inline function _population_range_enqueue!(w, labels, time, node, walk, mask, cutoff, population, limit)
    time <= cutoff || return
    if walk
        iszero(limit) && return
        population.walk_min[node] <= min(limit, cutoff - time) || return
    end
    state = 2Int(node) - 1 + walk
    improved = UInt64(0)
    while !iszero(mask)
        lane = trailing_zeros(mask) + 1
        bit = UInt64(1) << (lane - 1)
        if time < labels[lane, state]
            labels[lane, state] = time
            improved |= bit
        end
        mask &= mask - UInt64(1)
    end
    iszero(improved) && return
    iszero(w.settled[state]) && push!(w.settled_ids, state)
    w.settled[state] |= improved
    key = (UInt64(time) << 32) | UInt64(state - 1)
    previous = get(w.pending, key, UInt64(0))
    iszero(previous) && push!(w.queue, key)
    w.pending[key] = previous | improved
    return nothing
end

# Each origin retains independent A/E labels. Earlier departures repair only strict improvements.
function _population_sample_range!(w, graph, network, population, sources, ids, ready, cutoffs, limit, labels)
    for id in w.reached_ids
        w.reached[id] = 0
    end
    empty!(w.reached_ids)
    count = length(ids)
    for (slot, i) in enumerate(ids)
        mask = sum(UInt64(1) << (lane - 1) for lane in slot:count:length(ready))
        for id in sources.direct[i]
            _population_credit!(w, id, mask)
        end
    end
    shared = separate = 0
    for sample in reverse(0:(div(length(ready), count) - 1))
        offset = sample * count
        time, cutoff = ready[offset + 1], cutoffs[offset + 1]
        for (slot, i) in enumerate(ids)
            mask = UInt64(1) << (slot - 1)
            node = sources.sources[i]
            if iszero(node)
                for (target, duration) in sources.access[i]
                    _population_range_enqueue!(w, labels, time + duration, target, false, mask, cutoff, population, limit)
                end
            else
                _population_range_enqueue!(w, labels, time, node, false, mask, cutoff, population, limit)
                _population_range_enqueue!(w, labels, time, node, true, mask, cutoff, population, limit)
            end
        end
        while !isempty(w.queue)
            key = pop!(w.queue)
            time, state = UInt32(key >> 32), Int(key % UInt32) + 1
            node, walk = (state + 1) >> 1, iseven(state)
            mask = pop!(w.pending, key)
            valid = UInt64(0)
            while !iszero(mask)
                lane = trailing_zeros(mask) + 1
                labels[lane, state] == time && (valid |= UInt64(1) << (lane - 1))
                mask &= mask - UInt64(1)
            end
            mask = valid
            iszero(mask) && continue
            shared += 1
            separate += count_ones(mask)
            if walk
                remaining = min(limit, cutoff - time)
                for j in network.offsets[node]:(network.offsets[node + 1] - 1)
                    duration = network.durations[j]
                    duration <= remaining || continue
                    _population_range_enqueue!(w, labels, time + duration, network.targets[j], false, mask, cutoff, population, limit)
                end
            else
                for edge in graph.out_ptr[node]:(graph.out_ptr[node + 1] - Int32(1))
                    arrival = _population_next_arrival(w.schedule_hints, graph, edge, time, cutoff)
                    arrival == INF && continue
                    target = graph.edge_to[edge]
                    _population_range_enqueue!(w, labels, arrival, target, false, mask, cutoff, population, limit)
                    _population_range_enqueue!(w, labels, arrival, target, true, mask, cutoff, population, limit)
                end
            end
        end
        # Unchanged labels also contribute. The earlier sample has a smaller cutoff.
        for state in w.settled_ids
            node, walk = (state + 1) >> 1, iseven(state)
            mask = w.settled[state]
            if !walk
                population.weights[node] > 0 || continue
                active = UInt64(0)
                while !iszero(mask)
                    slot = trailing_zeros(mask) + 1
                    labels[slot, state] <= cutoff && (active |= UInt64(1) << (slot - 1))
                    mask &= mask - UInt64(1)
                end
                _population_credit!(w, Int32(node), active << offset)
                continue
            end
            first, stop = population.offsets[node], population.offsets[node + 1]
            first < stop || continue
            while !iszero(mask)
                slot = trailing_zeros(mask) + 1
                time = labels[slot, state]
                group = UInt64(1) << (slot - 1)
                mask &= mask - UInt64(1)
                time <= cutoff && population.durations[first] <= min(limit, cutoff - time) || continue
                pending = mask
                while !iszero(pending)
                    lane = trailing_zeros(pending) + 1
                    labels[lane, state] == time && (group |= UInt64(1) << (lane - 1))
                    pending &= pending - UInt64(1)
                end
                mask &= ~group
                event = w.heads[node]
                if !iszero(event) && w.times[event] == time
                    w.masks[event] |= group << offset
                else
                    iszero(event) && push!(w.egress_nodes, Int32(node))
                    push!(w.times, time)
                    push!(w.masks, group << offset)
                    push!(w.links, event)
                    w.heads[node] = length(w.times)
                end
            end
        end
    end
    _population_cover!(w, population, cutoffs, limit)
    return shared, separate
end
