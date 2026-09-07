include("src/Reachability.jl")
using .Reachability
import HTTP
import KernelAbstractions as KA

length(ARGS) == 1 || error("usage: julia --project=router router/serve.jl <edgelist.arrow|--demo>")
graph = if only(ARGS) == "--demo"
    include("fixture.jl")
    pack_graph(fixture_table())
else
    pack_graph(only(ARGS); skip_invalid_durations=true)
end

backend_name = get(ENV, "ROUTER_BACKEND", "cpu")
workspace = nothing
route = if backend_name == "reference"
    (h, t, b) -> route_cpu(graph, h, t, b)
else
    backend = if backend_name == "cpu"
        KA.CPU()
    elseif backend_name == "oneapi"
        @eval import oneAPI
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
workers = parse(Int, get(ENV, "ROUTER_WINDOW_WORKERS", string(min(4, Threads.nthreads(:default)))))
chunk > 0 || error("ROUTER_WINDOW_CHUNK must be positive")
workers > 0 || error("ROUTER_WINDOW_WORKERS must be positive")
window_route = if window_backend == "origin"
    (h, t, b, w, s) -> route_window(graph, h, t, b, w; step_ms=s)
elseif window_backend == "catchup"
    1 <= chunk <= 256 || error("ROUTER_WINDOW_CHUNK must be between 1 and 256")
    1 <= workers <= 256 || error("ROUTER_WINDOW_WORKERS must be between 1 and 256")
    (h, t, b, w, s) -> route_window_cached(graph, h, t, b, w; step_ms=s, chunk_size=chunk, workers=workers)
elseif window_backend in ("oneapi", "ka_cpu")
    if window_backend == "oneapi"
        @eval import oneAPI
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
    (h, t, b, w, s) -> route_window_kernel!(window_workspace, h, t, b, w; step_ms=s)
else
    error("ROUTER_WINDOW_BACKEND must be origin, catchup, oneapi, or ka_cpu")
end

walking_window_route = (h, t, b, w, s, m, index) -> if window_backend == "origin"
    route_window_walking(graph, h, t, b, w; step_ms=s, max_walk_s=m, walking_index=index)
else
    route_window_walking_cached(graph, h, t, b, w; step_ms=s, max_walk_s=m,
                                walking_index=index, chunk_size=chunk, workers)
end

host = get(ENV, "ROUTER_HOST", "127.0.0.1")
port = parse(Int, get(ENV, "ROUTER_PORT", "1988"))
@info "Starting router" host port backend=backend_name resolution=graph.resolution distance_available=!isnothing(graph.distance_km) nodes=length(graph.h3) edges=length(graph.edge_to) profiles=length(graph.departure)
@info "Window routing" window_backend
@info "Single-departure route distances use CPU Dijkstra"
@info "Walking uses CPU routing; window catch-up unless ROUTER_WINDOW_BACKEND=origin" default_max_walk_s=3600 workers chunk
@info "Preparing resident walking adjacency before accepting requests" max_walk_s=3600 preparation_workers=min(4, Threads.nthreads(:default))
HTTP.serve(make_handler(graph; route, window_route, walking_window_route), host, port)
