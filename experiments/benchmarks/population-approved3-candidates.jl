include("population-10k-candidates.jl")

function approved_module(name; labels=false, radix=false, vector=false, gate=nothing, update=false, histogram=false,
                          sites=:both, kind=:native)
    wrapper = Module(name)
    dir = joinpath(SNAPSHOT, "router/src")
    source = read(joinpath(dir, "Reachability.jl"), String)
    for file in ("population_packed.jl", "population_range.jl")
        text = read(joinpath(dir, file), String)
        if radix
            text = replace(text, "queue::BinaryMinHeap{UInt64}" => "queue::TimestampRadixQueue",
                "BinaryMinHeap{UInt64}()" => "TimestampRadixQueue()",
                "    # Source preparation" => "    empty!(w.queue)\n    # Source preparation",
                "        offset = sample * count" => "        empty!(w.queue)\n        offset = sample * count")
        end
        if labels && file == "population_range.jl"
            helper = vector ? "vector_hybrid_mask" : "hybrid_label_mask"
            readmask(op, time) = isnothing(gate) ? "$helper(labels, state, $time, mask, $op)" :
                "gated_read(labels, state, $time, mask, $op, Val(:$gate), Val(:$kind))"
            if sites === :both
                text = replace(text,r"valid = UInt64\(0\)\n.*?mask = valid"s => "mask = $(readmask("==", "time"))")
                @assert occursin("mask = $(readmask("==", "time"))",text)
            end
            text = replace(text,r"active = UInt64\(0\)\n.*?                end"s => "active = $(readmask("<=", "cutoff"))")
            @assert occursin("active = $(readmask("<=", "cutoff"))", text)
        end
        if update && file == "population_range.jl"
            text = replace(text,r"    improved = UInt64\(0\)\n.*?    iszero\(improved\) && return"s =>
                "    improved = gated_update!(labels, state, time, mask, Val(:$gate), Val(:native))\n    iszero(improved) && return")
            @assert occursin("improved = gated_update!",text)
        end
        if histogram && file == "population_range.jl"
            text = replace(text,"    improved = UInt64(0)"=>"    record_density(size(labels,1),mask,1)\n    improved = UInt64(0)",
                "            mask = valid"=>"            mask = valid\n            record_density(size(labels,1),mask,2)",
                "                active = UInt64(0)"=>"                record_density(size(labels,1),mask,3)\n                active = UInt64(0)")
        end
        if file == "population_packed.jl"
            prelude = read(joinpath(@__DIR__, "population-approved3-kernels.jl"), String)
            if !isnothing(gate)
                prelude *= replace(read(joinpath(@__DIR__,"population-simd-followup-kernels.jl"),String),
                    "include(\"population-approved3-kernels.jl\")"=>"")
            end
            if histogram
                prelude *= """
                const MASK_DENSITY = [zeros(Int,3,64,65) for _ in 1:Threads.maxthreadid()]
                @inline function record_density(width,mask,kind)
                    @inbounds MASK_DENSITY[Threads.threadid()][kind,width,count_ones(mask)+1] += 1
                    return nothing
                end
                """
            end
            text = prelude * "\n" * text
        end
        source = replace(source, "include(\"$file\")" => "include_string(@__MODULE__, $(repr(text)), $(repr(file)))")
    end
    source = replace(source, r"include\(\"([^\"]+)\"\)" => text ->
        "include($(repr(joinpath(dir, match(r"\"([^\"]+)\"", text)[1]))))")
    Base.include_string(wrapper, source, joinpath(dir, "Reachability.jl"))
    M = Base.invokelatest(getproperty, wrapper, :Reachability)
    return Base.invokelatest(shared_inputs, M)
end
