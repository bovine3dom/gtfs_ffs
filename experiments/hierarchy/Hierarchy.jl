module HierarchyResearch
using H3
import ..Reachability as R
export prepare_hierarchy, route_hierarchy

const Events = Vector{Tuple{UInt32,UInt32}}

struct Hierarchy{P,S}
    fine::R.Graph
    walking::R.WalkingIndex
    population::P
    graph::R.Graph
    prepared::@NamedTuple{graph::R.PackedWalking{Int32}, output_id::Dict{UInt64,Int32}}
    destinations::R.PreparedPopulation
    direct::R.PackedWalking{Int32}
    origins::Dict{UInt64,Int32}
    core_nodes::Int
    core_resolution::Int
    max_walk_ms::UInt32
    stats::S
end

function selected_cells(cells, resolution)
    result = sort!(unique(collect(cells)))
    isempty(result) && throw(ArgumentError("origins must not be empty"))
    foreach(h -> R.validate_cell(h, resolution), result)
    return result
end

function csr(rows)
    offsets, targets, durations = Int[1], Int32[], UInt32[]
    for row in rows
        for (target, duration) in sort!(collect(row); by=p -> (last(p), first(p)))
            push!(targets, target)
            push!(durations, duration)
        end
        push!(offsets, length(targets) + 1)
    end
    return R.PackedWalking(offsets, targets, durations, Float64[])
end

# Normalize both retained day copies. Day-zero entries alone can omit useful events.
function add_events!(row, fine, parents, u, walk)
    for edge in fine.out_ptr[u]:(fine.out_ptr[u + 1] - 1)
        events = get!(Events, row, parents[fine.edge_to[edge]])
        for j in fine.schedule_ptr[edge]:(fine.schedule_ptr[edge + 1] - 1)
            departure, duration = fine.departure[j], fine.arrival[j] - fine.departure[j]
            d = mod(Int64(departure) - walk, Int64(R.PERIOD))
            a = d + Int64(duration) + walk
            a + R.PERIOD < R.INF || throw(ArgumentError("shifted access profile exceeds UInt32; use the fine router"))
            push!(events, (UInt32(d), UInt32(a)))
        end
    end
end

function envelope!(row)
    for events in values(row)
        sort!(events; by=p -> (p[1], -Int64(p[2])))
        retained = Events()
        best = R.INF
        for offset in (R.PERIOD, UInt32(0)), (d, a) in Iterators.reverse(events)
            if a + offset < best
                push!(retained, (d + offset, a + offset))
                best = a + offset
            end
        end
        empty!(events)
        append!(events, Iterators.reverse(retained))
    end
    return row
end

"""Prepare daily access profiles for a fixed set of fine origins and one walking limit."""
function prepare_hierarchy(fine::R.Graph, population::R.Population, origins;
        core_resolution::Integer, max_walk_ms::Integer=3_600_000, walking_index=nothing)
    started = time_ns()
    fine.resolution == 8 || throw(ArgumentError("this experiment requires a res8 fine graph"))
    0 <= core_resolution <= 8 || throw(ArgumentError("core_resolution must be in 0..8"))
    0 < max_walk_ms < R.PERIOD || throw(ArgumentError("prepare a positive walking limit below one day"))
    all(Int64(a) - d + max_walk_ms + 2Int64(R.PERIOD) - 1 < R.INF
        for (d, a) in zip(fine.departure, fine.arrival)) ||
        throw(ArgumentError("insufficient UInt32 headroom for access profiles; use the fine router"))
    limit = UInt32(max_walk_ms)
    origins = selected_cells(origins, 8)
    walking = isnothing(walking_index) ? R.prepare_walking(R.WalkingIndex(fine); max_walk_ms) : walking_index
    walking.cells == fine.h3 && walking.resolution == 8 && !isnothing(walking.prepared) &&
        walking.prepared.limit >= limit || throw(ArgumentError("prepare matching fine walking geometry first"))
    fp = R._prepare_population(population, walking)
    weights = R._population_rollup(population, 8)
    parent_cells = H3.API.cellToParent.(fine.h3, core_resolution)
    core = sort!(unique(parent_cells))
    parent_id = Dict(h => Int32(i) for (i, h) in enumerate(core))
    parents = [parent_id[h] for h in parent_cells]
    nc, n = length(core), length(core) + length(origins)
    n < typemax(Int32) || throw(ArgumentError("too many graph nodes"))
    children = [Int[] for _ in 1:nc]
    for u in eachindex(fine.h3)
        push!(children[parents[u]], u)
    end
    from, to, ptr, out = Int32[], Int32[], Int32[1], Int32[1]
    departures, arrivals = UInt32[], UInt32[]
    function pack_row!(row, u)
        for v in sort!(collect(keys(row)))
            length(departures) + length(row[v]) < typemax(Int32) || throw(ArgumentError("too many profiles"))
            push!(from, Int32(u)); push!(to, v)
            for (d, a) in row[v]
                push!(departures, d); push!(arrivals, a)
            end
            push!(ptr, Int32(length(departures) + 1))
        end
        push!(out, Int32(length(to) + 1))
        empty!(row)
    end
    for parent in 1:nc
        row = Dict{Int32,Events}()
        for u in children[parent]
            add_events!(row, fine, parents, u, UInt32(0))
        end
        pack_row!(envelope!(row), parent)
    end
    core_profiles = length(departures)
    core_done = time_ns()
    rows = [Dict{Int32,Events}() for _ in origins]
    access_counts = zeros(Int, length(origins))
    direct_cells = Vector{Dict{UInt64,UInt32}}(undef, length(origins))
    Threads.@threads for i in eachindex(origins)
        cell = origins[i]
        u = get(fine.node_id, cell, Int32(0))
        hops = iszero(u) ? R.walking_cells(walking, cell, limit) :
            R.WalkingRange(walking.prepared.geographic, walking.prepared.geographic.offsets[u],
                walking.prepared.geographic.offsets[u + 1] - walking.prepared.geographic.offsets[u])
        local_access = iszero(u) ? Tuple{Int32,UInt32}[] : [(u, UInt32(0))]
        origin_direct = Dict{UInt64,UInt32}()
        get(weights, cell, 0.0) > 0 && (origin_direct[cell] = 0)
        for hop in hops
            hop.duration_ms <= limit || continue
            v = get(fine.node_id, hop.cell, Int32(0))
            iszero(v) || push!(local_access, (v, hop.duration_ms))
            get(weights, hop.cell, 0.0) > 0 && (origin_direct[hop.cell] = hop.duration_ms)
        end
        for (v, duration) in local_access
            add_events!(rows[i], fine, parents, v, duration)
        end
        envelope!(rows[i])
        access_counts[i], direct_cells[i] = length(local_access), origin_direct
    end
    access_done = time_ns()
    cells = vcat(core, origins)
    for (i, row) in enumerate(rows)
        pack_row!(row, nc + i)
    end
    graph = R.Graph(cells, Dict(h => Int32(i) for (i, h) in enumerate(cells)), out,
        from, to, ptr, departures, arrivals, 8, nothing)
    # Only the suffix has population weight. Prefix IDs are private, zero-weight states.
    destinations = fp.cells[findall(>(0), fp.weights)]
    for direct in direct_cells
        append!(destinations, keys(direct))
    end
    sort!(unique!(destinations))
    n + length(destinations) < typemax(Int32) || throw(ArgumentError("too many destinations"))
    output_id = Dict(h => Int32(n + i) for (i, h) in enumerate(destinations))
    aligned = vcat(zeros(n), Float64[weights[h] for h in destinations])
    network_rows = [Dict{Int32,UInt32}() for _ in 1:n]
    output_rows = [Dict{Int32,UInt32}() for _ in 1:n]
    output_mapping = Int32[get(output_id, h, Int32(0)) for h in fp.cells]
    for u in eachindex(fine.h3)
        row = output_rows[parents[u]]
        own = get(output_id, fine.h3[u], Int32(0))
        iszero(own) || (row[own] = 0)
        for (source, target_rows, mapping) in ((walking.prepared.graph, network_rows, parents),
                (fp, output_rows, output_mapping))
            target_row = target_rows[parents[u]]
            for j in source.offsets[u]:(source.offsets[u + 1] - 1)
                d = source.durations[j]
                d <= limit || continue
                v = mapping[source.targets[j]]
                target_row[v] = min(get(target_row, v, R.INF), d)
            end
        end
    end
    network, output = csr(network_rows), csr(output_rows)
    walk_min = UInt32[min(minimum(values(network_rows[u]); init=R.INF),
        minimum(values(output_rows[u]); init=R.INF)) for u in 1:n]
    prepared = R.PreparedPopulation(vcat(zeros(UInt64, n), destinations), aligned, n,
        output.offsets, output.targets, output.durations, walk_min)
    direct = csr([Dict(output_id[h] => d for (h, d) in row) for row in direct_cells])
    # Keep discarded regional cores out of the shared population's hint cache.
    population = R.Population(population.h3, population.weights, population.rollups, population.prepared,
        IdDict(fine => R._population_schedule_hints(population, fine)), population.lock)
    R._population_schedule_hints(population, graph)
    stats = (; preparation_s=(time_ns() - started) / 1e9, core_s=(core_done - started) / 1e9,
        access_s=(access_done - core_done) / 1e9, origins=length(origins), core_nodes=nc,
        core_edges=Int(out[nc + 1] - 1), core_profiles, source_profiles=length(departures) - core_profiles,
        network_walks=length(network.targets), output_walks=length(output.targets),
        fine_destinations=length(destinations), access_nodes=sum(access_counts))
    return Hierarchy(fine, walking, population, graph, (; graph=network, output_id), prepared,
        direct, Dict(h => Int32(i) for (i, h) in enumerate(origins)), nc, Int(core_resolution), limit, stats)
end

# This adapter also lets the production scheduler classify walking-only origins.
function R._population_sources(index::Hierarchy, population, weights, origins, limit)
    selected = [index.origins[h] for h in origins]
    nodes = Int32[index.core_nodes + i for i in selected]
    sources = [index.graph.out_ptr[u] == index.graph.out_ptr[u + 1] ? Int32(0) : u for u in nodes]
    p = index.direct
    direct = [Int32[p.targets[j] for j in p.offsets[i]:(p.offsets[i + 1] - 1)
        if p.durations[j] <= limit] for i in selected]
    return (; sources, direct, access=[Tuple{Int32,UInt32}[] for _ in origins], weights=population.weights)
end

"""Route a subset of prepared fine origins. Zero walking or budget uses the fine router."""
function route_hierarchy(index::Hierarchy, origins, departure_ms::Integer, budget_ms::Integer;
        window_ms::Integer=0, step_ms::Integer=60_000, window_mode=:mean_intersection,
        max_walk_ms::Integer=index.max_walk_ms, exclude_origin_population::Bool=false,
        origin_batch_size=nothing)
    origins = selected_cells(origins, 8)
    all(h -> haskey(index.origins, h), origins) || throw(ArgumentError("origin was not prepared"))
    ready, _ = R.query_times(index.fine, first(origins), departure_ms, budget_ms)
    window_ms >= 0 && step_ms >= 0 || throw(ArgumentError("window and step must be nonnegative"))
    mode = R._window_mode(window_mode)
    step, samples = window_ms > 0 && step_ms > 0 ? R._window_times(ready, budget_ms, window_ms, step_ms)[1:2] : (0, 1)
    walk = R._walking_limit(max_walk_ms)
    walk in (UInt32(0), index.max_walk_ms) || throw(ArgumentError("walking limit differs from prepared access profiles"))
    isnothing(origin_batch_size) || (origin_batch_size isa Integer && 1 <= origin_batch_size <= 64) ||
        throw(ArgumentError("origin_batch_size must be in 1..64"))
    fallback = iszero(walk) || iszero(budget_ms)
    graph, walking, prepared = fallback ?
        (index.fine, index.walking, R._prepare_population(index.population, index.walking)) :
        (index.graph, index, index.destinations)
    result = R._route_population_origins(graph, walking, index.population, prepared,
        R._population_rollup(index.population, 8), origins, ready, budget_ms, step, samples,
        min(walk, UInt32(budget_ms)), mode, origin_batch_size, exclude_origin_population)
    return merge(result, (; backend=fallback ? :fine_fallback : :hierarchy, core_resolution=index.core_resolution))
end
include("Boarding.jl")
end
