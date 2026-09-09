include("src/Reachability.jl")
using .Reachability
import HTTP

function parse_cli(args)
    paths = String[]
    population_path = ""
    i = 1
    while i <= length(args)
        arg = args[i]
        if arg == "--population" || startswith(arg, "--population=")
            isempty(population_path) || throw(ArgumentError("duplicate --population option"))
            if arg == "--population"
                i += 1
                i <= length(args) || throw(ArgumentError("--population requires a path"))
                population_path = args[i]
            else
                population_path = split(arg, '='; limit=2)[2]
            end
            (!isempty(population_path) && !startswith(population_path, "--")) ||
                throw(ArgumentError("--population requires a path"))
        else
            push!(paths, arg)
        end
        i += 1
    end
    return (; paths, population_path)
end

function load_handlers(paths; population_path="")
    if isempty(paths) || ("--demo" in paths && paths != ["--demo"])
        error("usage: julia --threads=8 --project=router router/serve.jl [--population <path>] (<name_resN.arrow> [name_resN.arrow ...] | --demo)")
    end
    sources = Dict{Tuple{String,Int},String}()
    specs = map(paths) do path
        name, resolution = if path == "--demo"
            "demo", 5
        else
            matched = match(r"^(.+)_res([0-9]+)\.arrow$", basename(path))
            isnothing(matched) && throw(ArgumentError("filename must be [name]_res[N].arrow (nonempty name, lowercase .arrow): $path"))
            resolution = tryparse(Int, matched[2])
            (!isnothing(resolution) && 0 <= resolution <= 15) ||
                throw(ArgumentError("filename resolution must be in 0..15: $path"))
            String(matched[1]), resolution
        end
        key = (name, resolution)
        haskey(sources, key) && throw(ArgumentError("duplicate graph for network $(repr(name)), H3 resolution $resolution: $(sources[key]) and $path"))
        sources[key] = path
        key
    end
    default_network = first(specs)[1]
    @info "Router networks" default_network
    for name in unique(first.(specs))
        @info "Available network" network=name resolutions=sort([res for (network, res) in specs if network == name])
    end
    handlers = Dict{Tuple{String,Int},Any}()
    # One CPU job at a time across graphs; each window can use the whole default pool.
    request_lock = ReentrantLock()
    population = isempty(population_path) ? nothing : load_population(population_path; progress=true)
    for (path, (name, resolution)) in zip(paths, specs)
        Reachability._startup_stage(true, "Loading graph $path") do _
            graph = path == "--demo" ? pack_graph(fixture_table(); progress=true) :
                pack_graph(path; skip_invalid_durations=true, badajoz_shuttle=true, progress=true)
            graph.resolution == resolution || throw(ArgumentError("filename H3 resolution $resolution does not match graph resolution $(graph.resolution): $path"))
            @info "Preparing CPU graph" network=name resolution=graph.resolution nodes=length(graph.h3) edges=length(graph.edge_to) workers=Threads.nthreads(:default)
            handlers[(name, resolution)] = make_handler(graph; request_lock, progress=true, population)
        end
    end
    return make_network_handler(handlers; default_network)
end

if abspath(PROGRAM_FILE) == @__FILE__
    paths, population_path = parse_cli(ARGS)
    paths == ["--demo"] && include("fixture.jl")
    handler = load_handlers(paths; population_path)
    warmup_server()
    host = get(ENV, "ROUTER_HOST", "127.0.0.1")
    port = parse(Int, get(ENV, "ROUTER_PORT", "1988"))
    @info "Starting router" host port
    HTTP.serve(make_stream_handler(handler), host, port; stream=true)
end
