# This workspace exists only in the experimental module.
const EXPIRY_RECORDS = Threads.Atomic{Int}(0)
const EXPIRY_PEAK_RECORDS = Threads.Atomic{Int}(0)
function _expiry_workspace(origins, destinations, samples)
    return (; times=Matrix{UInt32}(undef, origins, destinations),
        seen=zeros(UInt64, destinations), intersection=zeros(UInt64, destinations), touched=Int[],
        heads=zeros(Int, samples), cells=Int[], masks=UInt64[], links=Int[])
end

function _population_tile_expiry!(x, w, graph, network, population, sources, ids,
                                  ready, budget, step, samples, limit, mode, own_ids=nothing)
    samples <= fld(64, length(ids)) && return _population_tile!(w, graph, network,
        population, sources, ids, ready, budget, step, samples, limit, mode, own_ids)
    for id in x.touched
        x.seen[id] = x.intersection[id] = 0
    end
    empty!(x.touched)
    fill!(x.heads, 0)
    empty!(x.cells); empty!(x.masks); empty!(x.links)
    labels = w.arrivals
    fill!(labels, INF)
    for state in w.settled_ids
        w.settled[state] = 0
    end
    empty!(w.settled_ids)
    total, ever, common, values = (zeros(length(ids)) for _ in 1:4)
    active_count, common_count = zeros(Int, length(ids)), zeros(Int, length(ids))
    last_cutoff = Int(ready) + (samples - 1) * step + Int(budget)
    expiry(t) = iszero(t) ? samples : min(samples, fld(last_cutoff - Int(t), step) + 1)
    function improve!(id, time, mask, k)
        scheduled = UInt64(0)
        next = expiry(time)
        while !iszero(mask)
            slot = trailing_zeros(mask) + 1
            bit = UInt64(1) << (slot - 1)
            mask &= mask - UInt64(1)
            !isnothing(own_ids) && own_ids[ids[slot]] == id && continue
            seen = !iszero(x.seen[id] & bit)
            seen && time >= x.times[slot, id] && continue
            old = seen ? expiry(x.times[slot, id]) : -1
            if !seen
                iszero(x.seen[id]) && push!(x.touched, id)
                x.seen[id] |= bit
                ever[slot] += sources.weights[id]
                if k == 0
                    x.intersection[id] |= bit
                    common[slot] += sources.weights[id]
                    common_count[slot] += 1
                end
            end
            if old < k
                total[slot] += sources.weights[id]
                active_count[slot] += 1
            end
            x.times[slot, id] = time
            next != old && next < samples && (scheduled |= bit)
        end
        if !iszero(scheduled)
            push!(x.cells, id); push!(x.masks, scheduled); push!(x.links, x.heads[next + 1])
            x.heads[next + 1] = length(x.cells)
        end
    end
    # Direct geographic coverage is valid at every sample.
    for (slot, i) in enumerate(ids)
        mask = UInt64(1) << (slot - 1)
        for id in sources.direct[i]
            improve!(id, UInt32(0), mask, 0)
        end
        node = sources.sources[i]
        iszero(node) && continue
        population.weights[node] > 0 && improve!(node, UInt32(0), mask, 0)
        for j in population.offsets[node]:(population.offsets[node + 1] - 1)
            population.durations[j] <= limit || break
            improve!(population.targets[j], UInt32(0), mask, 0)
        end
    end
    shared = separate = 0
    for k in 0:(samples - 1)
        time = UInt32(Int(ready) + (samples - 1 - k) * step)
        cutoff = UInt32(last_cutoff - k * step)
        # NETWORK_REPAIR
        # Repair precedes expiry, so an improvement can postpone the current loss.
        event = x.heads[k + 1]
        while !iszero(event)
            id, mask = x.cells[event], x.masks[event]
            while !iszero(mask)
                slot = trailing_zeros(mask) + 1
                bit = UInt64(1) << (slot - 1)
                mask &= mask - UInt64(1)
                expiry(x.times[slot, id]) == k || continue
                total[slot] -= sources.weights[id]
                active_count[slot] -= 1
                if !iszero(x.intersection[id] & bit)
                    x.intersection[id] &= ~bit
                    common[slot] -= sources.weights[id]
                    common_count[slot] -= 1
                end
            end
            event = x.links[event]
        end
        for slot in eachindex(ids)
            bit = UInt64(1) << (slot - 1)
            if active_count[slot] == 0
                total[slot] = 0
            elseif total[slot] <= 0
                total[slot] = sum((sources.weights[id] for id in x.touched
                    if !iszero(x.seen[id] & bit) && expiry(x.times[slot, id]) > k); init=0.0)
            end
            if common_count[slot] == 0
                common[slot] = 0
            elseif common[slot] <= 0
                common[slot] = sum((sources.weights[id] for id in x.touched
                    if !iszero(x.intersection[id] & bit)); init=0.0)
            end
            values[slot] += total[slot] / samples
        end
    end
    value = mode == :reachable_union ? values : mode in (:min_union, :diff_union) ? ever : common
    Threads.atomic_add!(EXPIRY_RECORDS, length(x.cells))
    Threads.atomic_max!(EXPIRY_PEAK_RECORDS, length(x.cells))
    return (; value, shared, separate)
end
