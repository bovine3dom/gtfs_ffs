include("src/Reachability.jl")
using .Reachability

function _spec(path)
    matched = match(r"^(.+)_res([0-9]+)\.arrow$", basename(path))
    isnothing(matched) && throw(ArgumentError("filename must be [name]_res[N].arrow: $path"))
    resolution = tryparse(Int, matched[2])
    (!isnothing(resolution) && 0 <= resolution <= 15) ||
        throw(ArgumentError("filename resolution must be in 0..15: $path"))
    return String(matched[1]), resolution
end

function _prepare_one(output_dir, name, resolution, source_path, graph)
    destination = joinpath(output_dir, "$(name)_res$(resolution)")
    @info "Preparing trip shards" network=name resolution source=source_path destination
    set = TripShardSet(source_path, graph; progress=true)
    prepare_trip_shard_set!(set, destination; progress=true)
    GC.gc(false)
end

function main(args)
    length(args) >= 2 || error("usage: julia --project=router prepare_trip_shards.jl OUTPUT_DIR graph_resN.arrow ...")
    output_dir = abspath(args[1])
    sources = Dict{String,Dict{Int,String}}()
    for path in args[2:end]
        name, resolution = _spec(path)
        by_resolution = get!(Dict{Int,String}, sources, name)
        haskey(by_resolution, resolution) && throw(ArgumentError("duplicate graph: $path"))
        by_resolution[resolution] = path
    end
    for name in sort!(collect(keys(sources)))
        by_resolution = sources[name]
        processed = Set{Int}()
        derived = nothing
        if haskey(by_resolution, 8)
            source_path = by_resolution[8]
            derived = pack_graph(source_path; skip_invalid_durations=true,
                badajoz_shuttle=true, trip_aware=false, progress=true)
            _prepare_one(output_dir, name, 8, source_path, derived)
            push!(processed, 8)
            for resolution in 7:-1:5
                if haskey(by_resolution, resolution)
                    graph = pack_graph(by_resolution[resolution]; skip_invalid_durations=true,
                        badajoz_shuttle=true, trip_aware=false, progress=true)
                    _prepare_one(output_dir, name, resolution, by_resolution[resolution], graph)
                    push!(processed, resolution)
                else
                    derived = coarsen_graph(derived, resolution; progress=true)
                    _prepare_one(output_dir, name, resolution, source_path, derived)
                    push!(processed, resolution)
                end
            end
        end
        for resolution in sort!(collect(keys(by_resolution)))
            resolution in processed && continue
            source_path = by_resolution[resolution]
            graph = pack_graph(source_path; skip_invalid_durations=true,
                badajoz_shuttle=true, trip_aware=false, progress=true)
            _prepare_one(output_dir, name, resolution, source_path, graph)
        end
    end
    return nothing
end

if abspath(PROGRAM_FILE) == @__FILE__
    main(ARGS)
end
