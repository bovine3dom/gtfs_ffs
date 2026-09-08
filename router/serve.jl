include("src/Reachability.jl")
using .Reachability
import HTTP

function load_handlers(paths)
    if isempty(paths) || ("--demo" in paths && paths != ["--demo"])
        error("usage: julia --threads=8 --project=router router/serve.jl <edgelist.arrow> [edgelist.arrow ...] | --demo")
    end
    handlers = Dict{Int,Any}()
    sources = Dict{Int,String}()
    # One CPU job at a time across graphs; each window can use the whole default pool.
    request_lock = ReentrantLock()
    for path in paths
        Reachability._startup_stage(true, "Loading graph $path") do _
            graph = path == "--demo" ? pack_graph(fixture_table(); progress=true) :
                pack_graph(path; skip_invalid_durations=true, badajoz_shuttle=true, progress=true)
            haskey(handlers, graph.resolution) && error("duplicate graph for H3 resolution $(graph.resolution): $(sources[graph.resolution]) and $path")
            @info "Preparing CPU graph" resolution=graph.resolution nodes=length(graph.h3) edges=length(graph.edge_to) workers=Threads.nthreads(:default)
            handlers[graph.resolution] = make_handler(graph; request_lock, progress=true)
            sources[graph.resolution] = path
        end
    end
    return make_resolution_handler(handlers)
end

if abspath(PROGRAM_FILE) == @__FILE__
    ARGS == ["--demo"] && include("fixture.jl")
    handler = load_handlers(ARGS)
    warmup_server()
    host = get(ENV, "ROUTER_HOST", "127.0.0.1")
    port = parse(Int, get(ENV, "ROUTER_PORT", "1988"))
    @info "Starting router" host port
    HTTP.serve(make_stream_handler(handler), host, port; stream=true)
end
