"""Walking windows with backward arrival repair and optional canonical distance replay.

Lookup and expansion counters cover arrival routing, not distance replay or geography.
"""
function route_window_walking_cached(graph::Graph, origin::UInt64, departure_ms::Integer,
                                     budget_ms::Integer, window_ms::Integer;
                                     step_ms::Integer=60_000, max_walk_s::Integer=3600,
                                     walking_index::Union{Nothing,WalkingIndex}=nothing,
                                     chunk_size::Integer=64,
                                      workers::Integer=min(4, Threads.nthreads(:default)),
                                      distance_mode="itinerary")
    chunk_size > 0 || throw(ArgumentError("chunk_size must be positive"))
    workers > 0 || throw(ArgumentError("workers must be positive"))
    plan = _walking_window_plan(graph, origin, departure_ms, budget_ms, window_ms;
                                step_ms, max_walk_s, walking_index, distance_mode)
    worker_count = Int(min(workers, Threads.nthreads(:default), plan.samples))
    width = Int(min(chunk_size, cld(plan.samples, worker_count)))
    full_searches = cld(plan.samples, width)
    worker_count = min(worker_count, full_searches)
    shared = worker_count > 1 ? WalkingGeometryCache() : nothing
    output = _walking_output_plan(plan, origin)
    workspaces = [_walking_catchup_workspace(graph, plan, width, shared, output) for _ in 1:worker_count]
    outcomes = Vector{Any}(undef, worker_count)
    acc = isnothing(output) ? (plan.track_distance ? Dict{UInt64,Tuple{UInt64,UInt32,Float64}}() :
          Dict{UInt64,Tuple{UInt64,UInt32}}()) :
          WalkingOutputAccumulator(output.cells, plan.samples, plan.budget, plan.track_distance)
    profile_lookups = routing_expansions = 0
    for wave in 1:worker_count:full_searches
        active = min(worker_count, full_searches - wave + 1)
        if active == 1
            first = (wave - 1) * width + 1
            outcomes[1] = _walking_catchup_chunk!(workspaces[1], graph, origin, plan,
                                                 first, min(first + width - 1, plan.samples))
        else
            # Slot ownership remains exclusive even when spawned tasks migrate.
            @sync for slot in 1:active
                let slot=slot, first=(wave + slot - 2) * width + 1
                    Threads.@spawn begin
                        outcomes[slot] = try
                            _walking_catchup_chunk!(workspaces[slot], graph, origin, plan,
                                                    first, min(first + width - 1, plan.samples), slot, active)
                        catch error
                            error
                        end
                    end
                end
            end
        end
        # Join the entire wave before throwing or consuming reusable point buffers.
        for slot in 1:active
            outcomes[slot] isa Exception && throw(outcomes[slot])
        end
        for slot in 1:active
            profile_lookups += outcomes[slot][1]
            routing_expansions += outcomes[slot][2]
            first = (wave + slot - 2) * width + 1
            for sample in first:min(first + width - 1, plan.samples)
                ready = UInt32(Int64(plan.ready) + (sample - 1) * plan.step)
                _accumulate_walking!(acc, workspaces[slot].points[sample - first + 1],
                                     ready, plan.budget, plan.samples)
            end
        end
    end
    return _finish_walking_window(acc, plan.samples; budget=plan.budget, origin,
                                  searches=plan.samples, reused_samples=0,
                                  backend="walking_catchup", workers=worker_count, full_searches,
                                  repair_searches=plan.samples - full_searches,
                                  profile_lookups, routing_expansions)
end

function _walking_catchup_workspace(graph, plan, width, shared=nothing, output=nothing)
    n = length(graph.h3)
    track = plan.track_distance
    points = !track ? (isnothing(output) ?
        Vector{@NamedTuple{h3::Vector{UInt64}, arrival::Vector{UInt32}}}(undef, width) :
        [(ids=Int32[], arrival=UInt32[]) for _ in 1:width]) : isnothing(output) ?
        Vector{@NamedTuple{h3::Vector{UInt64}, arrival::Vector{UInt32}, distance_km::Vector{Float64}}}(undef, width) :
        WalkingIndexedPoint[(ids=Int32[], arrival=UInt32[], distance_km=Float64[]) for _ in 1:width]
    return (arrival=Vector{UInt32}(undef, n), eligible=Vector{UInt32}(undef, n),
            connections=track ? zeros(Int32, length(graph.edge_to)) : nothing,
            seenA=track ? Vector{UInt32}(undef, n) : nothing, seenE=track ? Vector{UInt32}(undef, n) : nothing,
            kmA=track ? Vector{Float64}(undef, n) : nothing, kmE=track ? Vector{Float64}(undef, n) : nothing,
            queue=BinaryMinHeap{Tuple{UInt32,Int32,Int}}(),
            topology=WalkingTopology(plan.index, plan.limit, shared),
            output=isnothing(output) ? nothing : WalkingOutputWorkspace(output, track), points=points)
end

function _walking_catchup_chunk!(workspace, graph, origin, plan, first, last, slot=1, workers=1)
    (; arrival, eligible, connections, queue, topology) = workspace
    fill!(arrival, INF)
    fill!(eligible, INF)
    empty!(queue)
    source = get(graph.node_id, origin, Int32(0))
    profile_lookups = routing_expansions = 0
    for sample in last:-1:first
        ready = UInt32(Int64(plan.ready) + (sample - 1) * plan.step)
        cutoff = ready + plan.budget
        if source != 0
            arrival[source] = eligible[source] = ready
            push!(queue, (ready, source, 0), (ready, source, 1))
        else
            for hop in _walking_hops(topology, origin)
                hop.duration_ms <= min(topology.limit, cutoff - ready) || continue
                candidate, v = ready + hop.duration_ms, _walking_node(graph, hop.cell)
                candidate < arrival[v] || continue
                arrival[v] = candidate
                push!(queue, (candidate, v, 0))
            end
        end
        while !isempty(queue)
            time, u, state = pop!(queue)
            time == (state == 0 ? arrival[u] : eligible[u]) || continue
            routing_expansions += 1
            if state == 0
                for edge in graph.out_ptr[u]:(graph.out_ptr[u + 1] - Int32(1))
                    connection = next_connection(graph.schedule_ptr, graph.departure,
                                                 graph.arrival, edge, time, cutoff)
                    isnothing(connections) || (connections[edge] = connection)
                    profile_lookups += 1
                    connection == 0 && continue
                    candidate = (time ÷ PERIOD) * PERIOD + graph.arrival[connection]
                    v = graph.edge_to[edge]
                    candidate < eligible[v] || continue
                    eligible[v] = candidate
                    push!(queue, (candidate, v, 1))
                    if candidate < arrival[v]
                        arrival[v] = candidate
                        push!(queue, (candidate, v, 0))
                    end
                end
            else
                for hop in _walking_hops(topology, graph.h3[u])
                    hop.duration_ms <= min(topology.limit, cutoff - time) || continue
                    candidate, v = time + hop.duration_ms, _walking_node(graph, hop.cell)
                    candidate < arrival[v] || continue
                    arrival[v] = candidate
                    push!(queue, (candidate, v, 0))
                end
            end
        end
        # Retain labels above shrinking cutoffs for repair; replay and output mask them.
        plan.track_distance && _walking_catchup_replay!(workspace, graph, origin, source, ready, cutoff)
        if sample == last && workers > 1 && isnothing(workspace.output)
            # Warm different parts of the shared surface first, rather than having
            # every worker queue behind the same H3 entry. Merge order stays canonical.
            sources = findall(t -> t <= cutoff, eligible)
            offset = fld(length(sources) * (slot - 1), workers)
            for i in eachindex(sources)
                u = sources[mod1(i + offset, length(sources))]
                _walking_hops(topology, graph.h3[u]; geographic=true,
                              limit=min(topology.limit, cutoff - eligible[u]))
            end
        end
        point = sample - first + 1
        if isnothing(workspace.output)
            workspace.points[point] = _walking_result(
                graph, topology, origin, ready, cutoff, arrival, eligible, workspace.kmA, workspace.kmE)
        else
            _walking_indexed_result!(workspace.points[point], workspace.output, ready, cutoff,
                                     topology.limit, arrival, eligible, workspace.kmA, workspace.kmE)
        end
    end
    return profile_lookups, routing_expansions
end

function _walking_catchup_replay!(workspace, graph, origin, source, ready, cutoff)
    (; arrival, eligible, connections, seenA, seenE, kmA, kmE, queue, topology) = workspace
    fill!(seenA, INF)
    fill!(seenE, INF)
    fill!(kmA, NaN)
    fill!(kmE, NaN)
    empty!(queue)
    if source != 0
        seenA[source] = seenE[source] = ready
        kmA[source] = kmE[source] = 0.0
        push!(queue, (ready, source, 0), (ready, source, 1))
    else
        for hop in _walking_hops(topology, origin)
            hop.duration_ms <= min(topology.limit, cutoff - ready) || continue
            candidate, v = ready + hop.duration_ms, _walking_node(graph, hop.cell)
            candidate < seenA[v] || continue
            seenA[v] = candidate
            if candidate == arrival[v]
                kmA[v] = hop.distance_km
                push!(queue, (candidate, v, 0))
            end
        end
    end
    visited = 0
    while !isempty(queue)
        time, u, state = pop!(queue)
        visited += 1
        if state == 0
            base = (time ÷ PERIOD) * PERIOD
            for edge in graph.out_ptr[u]:(graph.out_ptr[u + 1] - Int32(1))
                connection = connections[edge]
                connection == 0 && continue
                candidate, v = base + graph.arrival[connection], graph.edge_to[edge]
                candidate <= cutoff && candidate < seenE[v] || continue
                candidate >= eligible[v] || error("cached walking eligibility labels are not optimal")
                # Tentative nonfinal discoveries still perform the reference's km checks.
                km = isnothing(graph.distance_km) ? NaN : kmA[u] + graph.distance_km[connection]
                isinf(km) && throw(ArgumentError("accumulated route distance is not finite"))
                seenE[v] = candidate
                if candidate == eligible[v]
                    kmE[v] = km
                    push!(queue, (candidate, v, 1))
                end
                if candidate < seenA[v]
                    candidate >= arrival[v] || error("cached walking arrival labels are not optimal")
                    seenA[v] = candidate
                    if candidate == arrival[v]
                        kmA[v] = km
                        push!(queue, (candidate, v, 0))
                    end
                end
            end
        else
            for hop in _walking_hops(topology, graph.h3[u])
                hop.duration_ms <= min(topology.limit, cutoff - time) || continue
                candidate, v = time + hop.duration_ms, _walking_node(graph, hop.cell)
                candidate < seenA[v] || continue
                candidate >= arrival[v] || error("cached walking arrival labels are not optimal")
                km = kmE[u] + hop.distance_km
                isinf(km) && throw(ArgumentError("accumulated route distance is not finite"))
                seenA[v] = candidate
                if candidate == arrival[v]
                    kmA[v] = km
                    push!(queue, (candidate, v, 0))
                end
            end
        end
    end
    visited == count(t -> t <= cutoff, arrival) + count(t -> t <= cutoff, eligible) ||
        error("cached connections do not reproduce final walking labels")
    return visited
end
