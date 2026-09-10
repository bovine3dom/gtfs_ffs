# Load only in the resident benchmark. Each module shares the graph vectors.
function candidate_module(name; incremental=false, expiry=false, bins=0, workers=0)
    wrapper = Module(name)
    source = read(joinpath(ROOT, "router/src/Reachability.jl"), String)
    for file in ("population_packed.jl", "population_range.jl")
        text = read(joinpath(ROOT, "router/src", file), String)
        workers > 0 && (text = replace(text, "min(Threads.nthreads(:default), tiles)" => "min($workers, tiles)"))
        if expiry && file == "population_packed.jl"
            text = replace(text,
                "next_tile = Threads.Atomic{Int}(1)" => "extras = [range_origins == 0 ? nothing : _expiry_workspace(range_origins, length(sources.weights), samples) for _ in 1:workers]\n    next_tile = Threads.Atomic{Int}(1)",
                "result = _population_tile!(workspaces[worker]," => "result = _population_tile_expiry!(extras[worker], workspaces[worker],")
        end
        if incremental && file == "population_packed.jl"
            text = replace(text,
                "arrivals::Matrix{UInt32}" => "arrivals::Matrix{UInt32}\n    population_arrivals::Matrix{UInt32}\n    population_ids::Vector{Int32}",
                "Matrix{UInt32}(undef, origins, 2n))" => "Matrix{UInt32}(undef, origins, 2n), fill(INF, origins, destinations), Int32[])",
                "fill!(labels, INF)" => "fill!(labels, INF)\n        fill!(w.population_arrivals, INF)\n        empty!(w.population_ids)")
        elseif incremental
            text = replace(text, "iszero(improved) && return" => """
                iszero(improved) && return
                if walk
                    for j in population.offsets[node]:(population.offsets[node + 1] - 1)
                        duration = population.durations[j]
                        duration <= min(limit, cutoff - time) || break
                        _population_improve!(w, population.targets[j], time + duration, improved)
                    end
                elseif population.weights[node] > 0
                    _population_improve!(w, Int32(node), time, improved)
                end
                """)
            start = findfirst("        # Unchanged labels", text).start
            stop = findfirst("    _population_cover!", text).start
            text = text[1:start-1] * """
                    for id in w.population_ids
                        active = UInt64(0)
                        for slot in 1:count
                            w.population_arrivals[slot, id] <= cutoff && (active |= UInt64(1) << (slot - 1))
                        end
                        _population_credit!(w, id, active << offset)
                    end
                end
            """ * text[stop:end]
            text *= """
            function _population_improve!(w, id, time, mask)
                all(==(INF), @view(w.population_arrivals[:, id])) && push!(w.population_ids, id)
                while !iszero(mask)
                    slot = trailing_zeros(mask) + 1
                    w.population_arrivals[slot, id] = min(w.population_arrivals[slot, id], time)
                    mask &= mask - UInt64(1)
                end
                return nothing
            end
            """
        end
        if bins > 0
            text = replace(text, "next_arrival(graph.schedule_ptr, graph.departure, graph.arrival, edge, time, cutoff)" =>
                "_population_next_arrival(graph.schedule_ptr, graph.departure, graph.arrival, edge, time, cutoff)")
        end
        source = replace(source, "include(\"$file\")" => "include_string(@__MODULE__, $(repr(text)), $(repr(file)))")
    end
    # Keep all remaining includes relative to the production source directory.
    source = replace(source, r"include\(\"([^\"]+)\"\)" => text ->
        "include($(repr(joinpath(ROOT, "router/src", match(r"\"([^\"]+)\"", text)[1]))))")
    Base.include_string(wrapper, source, joinpath(ROOT, "router/src/Reachability.jl"))
    M = Base.invokelatest(getproperty, wrapper, :Reachability)
    if expiry
        text = read(joinpath(@__DIR__, "population-expiry.jl"), String)
        kernel = read(joinpath(ROOT, "router/src/population_range.jl"), String)
        start = findfirst("        for (slot, i) in enumerate(ids)", kernel).start
        stop = findfirst("        # Unchanged labels", kernel).start
        kernel = kernel[start:stop-1]
        kernel = replace(kernel, "            if walk\n" => """
                    if walk
                        for j in population.offsets[node]:(population.offsets[node + 1] - 1)
                            duration = population.durations[j]
                            duration <= min(limit, cutoff - time) || break
                            improve!(population.targets[j], time + duration, mask, k)
                        end
                    elseif population.weights[node] > 0
                        improve!(node, time, mask, k)
                    end
                    if walk
            """)
        Base.include_string(M, replace(text, "        # NETWORK_REPAIR" => kernel), "population-expiry.jl")
    end
    if bins > 0
        Base.include_string(M, """
        const POPULATION_BINS = $bins
        const POPULATION_HINTS = Ref{Matrix{Int32}}()
        @inline function _population_next_arrival(ptr, departure, arrival, edge, ready::UInt32, cutoff::UInt32)
            base = (ready ÷ PERIOD) * PERIOD
            base > cutoff && return INF
            t = ready % PERIOD
            bin = Int(t ÷ (PERIOD ÷ UInt32(POPULATION_BINS))) + 1
            hints = POPULATION_HINTS[]
            @inbounds lo = hints[bin, edge]
            @inbounds stop = ptr[edge + 1]
            @inbounds hi = bin == POPULATION_BINS ? stop : min(stop, hints[bin + 1, edge] + Int32(1))
            while lo < hi
                mid = lo + ((hi - lo) >> 1)
                @inbounds if departure[mid] < t
                    lo = mid + Int32(1)
                else
                    hi = mid
                end
            end
            lo == stop && return INF
            @inbounds relative = arrival[lo]
            relative <= cutoff - base || return INF
            return base + relative
        end
        """)
        elapsed = @elapsed hints = population_hints(graph, bins)
        Base.invokelatest(getproperty, M, :POPULATION_HINTS)[] = hints
        logline("HINTS bins=$bins bytes=$(sizeof(hints)) build_s=$elapsed")
    end
    return Base.invokelatest(shared_inputs, M)
end

function population_hints(graph, bins)
    hints = Matrix{Int32}(undef, bins, length(graph.edge_to))
    for edge in eachindex(graph.edge_to)
        lo, stop = graph.schedule_ptr[edge], graph.schedule_ptr[edge+1]
        for bin in 1:bins
            t = (bin - 1) * (Int(B.PERIOD) ÷ bins)
            while lo < stop && graph.departure[lo] < t
                lo += Int32(1)
            end
            hints[bin, edge] = lo
        end
    end
    return hints
end
