"""Merge packed profiles at logical H3 parents. The same resolution returns `fine`."""
function coarsen_graph(fine::Graph, target::Integer; progress::Bool=false)
    0 <= target <= fine.resolution || throw(ArgumentError("target resolution must be in 0..$(fine.resolution)"))
    target == fine.resolution && return fine
    if !isnothing(fine.trip_id)
        parent_cells = [H3.API.cellToParent(h, target) for h in fine.h3]
        cells = sort!(unique(parent_cells))
        node_id = Dict(h => Int32(i) for (i, h) in enumerate(cells))
        parents = Int32[node_id[h] for h in parent_cells]
        order = sort!(collect(Int32, eachindex(fine.edge_to));
                      by=e -> (parents[fine.edge_from[e]], parents[fine.edge_to[e]]))
        edge_from, edge_to, schedule_ptr = Int32[], Int32[], Int32[1]
        departure, arrival, trip_id = UInt32[], UInt32[], UInt32[]
        distance_km = isnothing(fine.distance_km) ? nothing : Float64[]
        first_edge = 1
        while first_edge <= length(order)
            e = order[first_edge]
            from, to = parents[fine.edge_from[e]], parents[fine.edge_to[e]]
            rows = Tuple{UInt32,UInt32,UInt32,Float64}[]
            last_edge = first_edge
            while last_edge <= length(order)
                e = order[last_edge]
                (parents[fine.edge_from[e]], parents[fine.edge_to[e]]) == (from, to) || break
                for p in fine.schedule_ptr[e]:(fine.schedule_ptr[e + 1] - Int32(1))
                    km = isnothing(fine.distance_km) ? 0.0 : fine.distance_km[p]
                    push!(rows, (fine.departure[p], fine.arrival[p], fine.trip_id[p], km))
                end
                last_edge += 1
            end
            sort!(rows; by=r -> (r[3], r[1], -Int64(r[2]), r[4]))
            first_group = 1
            while first_group <= length(rows)
                trip = rows[first_group][3]
                last_group = first_group
                while last_group <= length(rows) && rows[last_group][3] == trip
                    last_group += 1
                end
                retained = Tuple{UInt32,UInt32,UInt32,Float64}[]
                best = INF
                for (d, a, group_trip, km) in Iterators.reverse(@view rows[first_group:last_group-1])
                    if a < best
                        push!(retained, (d, a, group_trip, km))
                        best = a
                    end
                end
                for (d, a, group_trip, km) in Iterators.reverse(retained)
                    push!(departure, d); push!(arrival, a); push!(trip_id, group_trip)
                    isnothing(distance_km) || push!(distance_km, km)
                end
                first_group = last_group
            end
            push!(edge_from, from); push!(edge_to, to)
            push!(schedule_ptr, Int32(length(departure) + 1))
            first_edge = last_edge
        end
        out_ptr = zeros(Int32, length(cells) + 1)
        out_ptr[1] = 1
        for u in edge_from
            out_ptr[u + 1] += 1
        end
        cumsum!(out_ptr, out_ptr)
        event_ptr, event_index, event_suffix = _trip_events(departure, arrival, trip_id, schedule_ptr)
        return Graph(cells, node_id, out_ptr, edge_from, edge_to, schedule_ptr,
                     departure, arrival, target, distance_km, trip_id, event_ptr, event_index,
                     event_suffix)
    end
    parents, cells, node_id, order = _startup_stage(progress, "Grouping resolution $target edges") do _
        local parents, cells, node_id, order
        parent_cells = [H3.API.cellToParent(h, target) for h in fine.h3]
        cells = sort!(unique(parent_cells))
        node_id = Dict(h => Int32(i) for (i, h) in enumerate(cells))
        parents = [node_id[h] for h in parent_cells]
        order = collect(Int32, eachindex(fine.edge_to))
        sort!(order; by=e -> (parents[fine.edge_from[e]], parents[fine.edge_to[e]]))
        parents, cells, node_id, order
    end
    return _startup_stage(progress, "Merging resolution $target profiles"; total=length(order)) do meter
        edge_from, edge_to, schedule_ptr = Int32[], Int32[], Int32[1]
        departure, arrival = UInt32[], UInt32[]
        distance_km = isnothing(fine.distance_km) ? nothing : Float64[]
        # Reverse departure order, then earliest arrival and shortest identical segment.
        # One cursor per fine edge avoids raw rows and per-group profile expansion.
        heap = BinaryMinHeap{Tuple{Int64,UInt32,Float64,Int32,Int32}}()
        candidate(p, e) = (-Int64(fine.departure[p]), fine.arrival[p],
            isnothing(fine.distance_km) ? 0.0 : fine.distance_km[p], p, e)
        first_edge = 1
        while first_edge <= length(order)
            e = order[first_edge]
            from, to = parents[fine.edge_from[e]], parents[fine.edge_to[e]]
            last_edge = first_edge
            while last_edge <= length(order)
                e = order[last_edge]
                (parents[fine.edge_from[e]], parents[fine.edge_to[e]]) == (from, to) || break
                p = fine.schedule_ptr[e + 1] - Int32(1)
                p >= fine.schedule_ptr[e] && push!(heap, candidate(p, e))
                last_edge += 1
            end
            start = length(departure) + 1
            best = INF
            while !isempty(heap)
                _, a, km, p, e = pop!(heap)
                if a < best
                    push!(departure, fine.departure[p])
                    push!(arrival, a)
                    isnothing(distance_km) || push!(distance_km, km)
                    best = a
                end
                p > fine.schedule_ptr[e] && push!(heap, candidate(p - Int32(1), e))
            end
            reverse!(departure, start, length(departure))
            reverse!(arrival, start, length(arrival))
            isnothing(distance_km) || reverse!(distance_km, start, length(distance_km))
            push!(edge_from, from)
            push!(edge_to, to)
            push!(schedule_ptr, Int32(length(departure) + 1))
            _startup_advance(meter, last_edge - first_edge)
            first_edge = last_edge
        end
        out_ptr = zeros(Int32, length(cells) + 1)
        out_ptr[1] = 1
        for u in edge_from
            out_ptr[u + 1] += 1
        end
        cumsum!(out_ptr, out_ptr)
        Graph(cells, node_id, out_ptr, edge_from, edge_to, schedule_ptr,
            departure, arrival, Int(target), distance_km)
    end
end
