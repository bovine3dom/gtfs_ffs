# The independent point/window oracle keeps its H3 dictionaries. Only prepared
# catch-up windows use these integer output IDs, including request-local access.
function _walking_output_plan(plan, origin)
    prepared = plan.index.prepared
    (isnothing(prepared) || plan.limit > prepared.limit) && return nothing
    cells = prepared.output_cells
    direct = PackedWalking([1], Int32[], UInt32[], Float64[])
    source = get(prepared.node_id, origin, Int32(0))
    origin_id = get(prepared.output_id, origin, Int32(0))
    if source == 0
        cells = copy(cells)
        if origin_id == 0
            push!(cells, origin)
            origin_id = Int32(length(cells))
        end
        # Enumerate once at the request radius, not once per sample or worker.
        for hop in walking_cells(plan.index, origin, plan.limit)
            id = get(prepared.output_id, hop.cell, Int32(0))
            if id == 0
                push!(cells, hop.cell)
                id = Int32(length(cells))
            end
            push!(direct.targets, id)
            push!(direct.durations, hop.duration_ms)
            push!(direct.distances, hop.distance_km)
        end
    end
    return (; cells, origin=origin_id, direct, packed=prepared.output)
end

struct WalkingOutputWorkspace{P,D}
    plan::P
    arrival::Vector{UInt32}
    distance::D
    touched::Vector{Int32}
end
WalkingOutputWorkspace(plan, track_distance=true) = WalkingOutputWorkspace(plan, fill(INF, length(plan.cells)),
    track_distance ? Vector{Float64}(undef, length(plan.cells)) : nothing, Int32[])

const WalkingIndexedPoint = @NamedTuple{ids::Vector{Int32}, arrival::Vector{UInt32}, distance_km::Vector{Float64}}

function _walking_relax_output!(out, packed::PackedWalking{Int32}, range, time, km, limit)
    @inbounds for i in range
        packed.durations[i] <= limit || continue
        v = packed.targets[i]
        candidate = time + packed.durations[i]
        previous = out.arrival[v]
        candidate < previous || continue
        if !isnothing(out.distance)
            total_km = km + packed.distances[i]
            isinf(total_km) && throw(ArgumentError("accumulated route distance is not finite"))
            out.distance[v] = total_km
        end
        previous == INF && push!(out.touched, v)
        out.arrival[v] = candidate
    end
end

function _walking_indexed_result!(point, out, ready, cutoff, limit, arrival, eligible, kmA, kmE)
    @inbounds for v in out.touched
        out.arrival[v] = INF
    end
    empty!(out.touched)
    origin = out.plan.origin
    out.arrival[origin] = ready
    isnothing(out.distance) || (out.distance[origin] = 0.0)
    push!(out.touched, origin)
    @inbounds for v in eachindex(arrival)
        arrival[v] <= cutoff || continue
        out.arrival[v] == INF && push!(out.touched, Int32(v))
        out.arrival[v] = arrival[v]
        isnothing(out.distance) || (out.distance[v] = kmA[v])
    end
    # Preserve the oracle's off-graph-origin-first, then graph-ID egress order.
    direct, packed = out.plan.direct, out.plan.packed
    _walking_relax_output!(out, direct, eachindex(direct.targets), ready, 0.0, min(limit, cutoff - ready))
    @inbounds for u in eachindex(eligible)
        eligible[u] <= cutoff || continue
        _walking_relax_output!(out, packed, packed.offsets[u]:(packed.offsets[u + 1] - 1),
                               eligible[u], isnothing(kmE) ? nothing : kmE[u], min(limit, cutoff - eligible[u]))
    end
    n = length(out.touched)
    resize!(point.ids, n)
    resize!(point.arrival, n)
    isnothing(out.distance) || resize!(point.distance_km, n)
    @inbounds for i in 1:n
        v = out.touched[i]
        point.ids[i], point.arrival[i] = v, out.arrival[v]
        isnothing(out.distance) || (point.distance_km[i] = out.distance[v])
    end
    return point
end

struct WalkingOutputAccumulator{D}
    cells::Vector{UInt64}
    total::Vector{UInt64}
    reached::Vector{UInt32}
    km::D
end
WalkingOutputAccumulator(cells, samples, budget, track_distance=true) = WalkingOutputAccumulator(cells,
    fill(UInt64(samples) * UInt64(budget), length(cells)), zeros(UInt32, length(cells)),
    track_distance ? Vector{Float64}(undef, length(cells)) : nothing)

function _accumulate_walking!(acc::WalkingOutputAccumulator, point, ready, budget, samples)
    @inbounds for i in eachindex(point.ids)
        v = point.ids[i]
        acc.total[v] -= UInt64(budget - (point.arrival[i] - ready))
        reached = acc.reached[v] += UInt32(1)
        if !isnothing(acc.km)
            acc.km[v] = reached == 1 ? point.distance_km[i] :
                acc.km[v] + (point.distance_km[i] - acc.km[v]) * (1 / reached)
        end
    end
end

function _finish_walking_window(acc::WalkingOutputAccumulator, samples; budget, origin=nothing, kwargs...)
    ids = sort!(findall(!iszero, acc.reached); by=i -> acc.cells[i])
    h3, elapsed_sum_ms, reachable_samples = acc.cells[ids], acc.total[ids], acc.reached[ids]
    distance_km = isnothing(acc.km) ? _od_distances(origin, h3) : acc.km[ids]
    elapsed_ms = Float64.(elapsed_sum_ms) ./ samples
    reachable_elapsed_ms = [(elapsed_sum_ms[i] - UInt64(samples - reachable_samples[i]) * UInt64(budget)) /
                            reachable_samples[i] for i in eachindex(h3)]
    return (; h3, elapsed_ms, reachable_elapsed_ms, distance_km, reachable_samples,
            sample_count=UInt32(samples), elapsed_sum_ms, kwargs...)
end
