include("src/Reachability.jl")
using .Reachability

function main(args)
    paths = String[]
    max_bytes = 512*1024^2
    for arg in args
        if startswith(arg,"--max-index-mib=")
            value = parse(Int,split(arg,'=';limit=2)[2])
            0 < value <= div(typemax(Int),1024^2) || error("invalid index memory limit")
            max_bytes = value*1024^2
        else
            push!(paths,arg)
        end
    end
    isempty(paths) && error("usage: prepare_continuation.jl [--max-index-mib=512] SHARD_OR_DIRECTORY ...")
    files = String[]
    for path in paths
        if isdir(path)
            for (root, _, names) in walkdir(path), name in names
                occursin(r"^shard_[0-9]+\.bin$",name) && push!(files,joinpath(root,name))
            end
        else
            push!(files,path)
        end
    end
    for path in sort!(unique(files))
        @info "Preparing continuation index" path max_bytes
        Reachability.write_continuation(path; max_bytes)
        GC.gc()
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    main(ARGS)
end
