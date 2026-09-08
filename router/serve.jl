include("src/Reachability.jl")
using .Reachability
import HTTP
import KernelAbstractions as KA

if get(ENV, "ROUTER_BACKEND", "cpu") == "oneapi" || get(ENV, "ROUTER_WINDOW_BACKEND", "catchup") == "oneapi"
    import oneAPI
end

function configured_handler(graph; request_lock=ReentrantLock())
    backend_name = get(ENV, "ROUTER_BACKEND", "cpu")
    workspace = nothing
    route = if backend_name == "reference"
        (h, t, b) -> route_cpu(graph, h, t, b)
    else
        backend = if backend_name == "cpu"
            KA.CPU()
        elseif backend_name == "oneapi"
            oneAPI.functional() || error("oneAPI requested but no device is available")
            oneAPI.oneAPIBackend()
        else
            error("ROUTER_BACKEND must be cpu, reference, or oneapi")
        end
        workspace = KernelRouter(graph, backend)
        (h, t, b) -> route_kernel!(workspace, h, t, b)
    end

    window_backend = get(ENV, "ROUTER_WINDOW_BACKEND", "catchup")
    chunk = parse(Int, get(ENV, "ROUTER_WINDOW_CHUNK", "64"))
    workers = Threads.nthreads(:default)
    chunk > 0 || error("ROUTER_WINDOW_CHUNK must be positive")
    window_route = if window_backend == "origin"
        (h, t, b, w, s; window_mode=:mean_intersection) -> route_window(graph, h, t, b, w; step_ms=s, window_mode)
    elseif window_backend == "catchup"
        1 <= chunk <= 256 || error("ROUTER_WINDOW_CHUNK must be between 1 and 256")
        (h, t, b, w, s; window_mode=:mean_intersection) -> route_window_cached(graph, h, t, b, w; step_ms=s, chunk_size=chunk, workers=workers, window_mode)
    elseif window_backend in ("oneapi", "ka_cpu")
        if window_backend == "oneapi"
            oneAPI.functional() || error("oneAPI window backend requested but unavailable")
        end
        graph_workspace = if window_backend == "oneapi" && backend_name == "oneapi"
            workspace
        else
            KernelRouter(graph, window_backend == "ka_cpu" ? KA.CPU() : oneAPI.oneAPIBackend())
        end
        batch = parse(Int, get(ENV, "ROUTER_WINDOW_BATCH", "32"))
        checks = parse(Int, get(ENV, "ROUTER_WINDOW_CHECK_EVERY", "4"))
        window_workspace = WindowKernelRouter(graph_workspace; batch_size=batch, check_every=checks)
        (h, t, b, w, s; window_mode=:mean_intersection) -> route_window_kernel!(window_workspace, h, t, b, w; step_ms=s, window_mode)
    else
        error("ROUTER_WINDOW_BACKEND must be origin, catchup, oneapi, or ka_cpu")
    end

    walking_window_route = (h, t, b, w, s, m, index; window_mode=:mean_intersection) -> if window_backend == "origin"
        route_window_walking(graph, h, t, b, w; step_ms=s, max_walk_ms=m, walking_index=index, window_mode)
    else
        route_window_walking_cached(graph, h, t, b, w; step_ms=s, max_walk_ms=m,
                                    walking_index=index, chunk_size=chunk, workers, window_mode)
    end

    straight_window_route = (h, t, b, w, s, m, index; window_mode=:mean_intersection) -> if m == 0
        route_window_cached(graph, h, t, b, w; step_ms=s, chunk_size=chunk, workers, distance_mode=:straight_line, window_mode)
    elseif window_backend == "origin"
        route_window_walking(graph, h, t, b, w; step_ms=s, max_walk_ms=m, walking_index=index, distance_mode=:straight_line, window_mode)
    else
        route_window_walking_cached(graph, h, t, b, w; step_ms=s, max_walk_ms=m,
            walking_index=index, chunk_size=chunk, workers, distance_mode=:straight_line, window_mode)
    end

    @info "Preparing graph" backend=backend_name resolution=graph.resolution distance_available=!isnothing(graph.distance_km) nodes=length(graph.h3) edges=length(graph.edge_to) profiles=length(graph.departure)
    @info "Window routing" window_backend
    @info "Single-departure route distances use CPU Dijkstra"
    @info "Walking uses CPU routing; window catch-up unless ROUTER_WINDOW_BACKEND=origin" default_max_walk_h=1.0 workers chunk
    @info "Preparing resident walking adjacency before accepting requests" max_walk_h=1.0 preparation_workers=min(4, Threads.nthreads(:default))
    @info "Straight-line distance uses CPU arrival-only routing, including transit-only requests"
    return make_handler(graph; route, window_route, walking_window_route, straight_window_route, request_lock)
end

function load_handlers(paths)
    if isempty(paths) || ("--demo" in paths && paths != ["--demo"])
        error("usage: julia --project=router router/serve.jl <edgelist.arrow> [edgelist.arrow ...] | --demo")
    end
    handlers = Dict{Int,Any}()
    sources = Dict{Int,String}()
    request_lock = ReentrantLock()
    for path in paths
        graph = path == "--demo" ? pack_graph(fixture_table()) :
            pack_graph(path; skip_invalid_durations=true, badajoz_shuttle=true)
        haskey(handlers, graph.resolution) && error("duplicate graph for H3 resolution $(graph.resolution): $(sources[graph.resolution]) and $path")
        handlers[graph.resolution] = configured_handler(graph; request_lock)
        sources[graph.resolution] = path
    end
    return make_resolution_handler(handlers)
end

if abspath(PROGRAM_FILE) == @__FILE__
    ARGS == ["--demo"] && include("fixture.jl")
    handler = load_handlers(ARGS)
    host = get(ENV, "ROUTER_HOST", "127.0.0.1")
    port = parse(Int, get(ENV, "ROUTER_PORT", "1988"))
    @info "Starting router" host port
    origins = filter(!isempty, strip.(split(get(ENV, "ROUTER_WS_ORIGINS", ""), ',')))
    @info "Query WebSocket browser origins" policy=isempty(origins) ? "allow all" : "allowlist" origins
    HTTP.serve(make_stream_handler(handler; origins), host, port; stream=true)
end
