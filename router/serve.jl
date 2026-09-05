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

host = get(ENV, "ROUTER_HOST", "127.0.0.1")
port = parse(Int, get(ENV, "ROUTER_PORT", "1988"))
@info "Starting res5 transit-only router" host port backend=backend_name nodes=length(graph.h3) edges=length(graph.edge_to) profiles=length(graph.departure)
HTTP.serve(make_handler(graph; route), host, port)
