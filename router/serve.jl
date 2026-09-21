include("src/Reachability.jl")
using .Reachability
import HTTP

function parse_cli(args)
    paths = String[]
    population_path = ""
    limits = Dict("--max-pending" => 128, "--workspace-memory-gib" => 8)
    seen = Set{String}()
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
        elseif first(split(arg, '='; limit=2)) in keys(limits)
            name = first(split(arg, '='; limit=2))
            name in seen && throw(ArgumentError("duplicate $name option"))
            push!(seen, name)
            text = if arg == name
                i += 1
                i <= length(args) || throw(ArgumentError("$name requires an integer"))
                args[i]
            else
                split(arg, '='; limit=2)[2]
            end
            value = occursin(r"^[0-9]+\z", text) ? tryparse(Int, text) : nothing
            minimum, maximum = name == "--max-pending" ? (0, typemax(Int) - 1) : (1, div(typemax(Int), 1024^3))
            !isnothing(value) && minimum <= value <= maximum ||
                throw(ArgumentError("$name requires an integer in $minimum..$maximum"))
            limits[name] = value
        else
            push!(paths, arg)
        end
        i += 1
    end
    return (; paths, population_path, max_pending=limits["--max-pending"],
            workspace_bytes=limits["--workspace-memory-gib"] * 1024^3)
end

function load_handlers(paths; population_path="", max_pending=128, workspace_bytes=8*1024^3)
    admission = RequestAdmission(; max_pending, memory_bytes=workspace_bytes)
    workspace_pool = PopulationWorkspacePool(; max_bytes=workspace_bytes)
    if isempty(paths) || ("--demo" in paths && paths != ["--demo"])
        error("usage: julia --threads=8 --project=router router/serve.jl [--population <path>] [--max-pending 128] [--workspace-memory-gib 8] (<name_resN.arrow> [name_resN.arrow ...] | --demo)")
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
    @info "Router request limits" max_pending workspace_mib=workspace_bytes / 1024^2 resources=scheduler_stats(admission)
    @info "Router networks" default_network
    handlers = Dict{Tuple{String,Int},Any}()
    response_cache = ResponseCache()
    population = isempty(population_path) ? nothing : load_population(population_path; progress=true)
    graphs = Dict{Tuple{String,Int},Graph}()
    for (path, (name, resolution)) in zip(paths, specs)
        Reachability._startup_stage(true, "Loading graph $path") do _
            graph = path == "--demo" ? pack_graph(fixture_table(); progress=true) :
                pack_graph(path; skip_invalid_durations=true, badajoz_shuttle=true, progress=true)
            graph.resolution == resolution || throw(ArgumentError("filename H3 resolution $resolution does not match graph resolution $(graph.resolution): $path"))
            graphs[(name, resolution)] = graph
            @info "Supplied graph" network=name resolution source=path
        end
    end
    # Validate all explicit inputs first. An explicit resolution always takes precedence.
    for (name, resolution) in specs
        graph = graphs[(name, resolution)]
        resolution == 8 || continue
        for target in 7:-1:5
            haskey(sources, (name, target)) && continue
            derived = coarsen_graph(graph, target; progress=true)
            @info "Derived graph" network=name resolution=target source=sources[(name, 8)] source_resolution=8 nodes=length(derived.h3) edges=length(derived.edge_to)
            graphs[(name, target)] = derived
            graph = derived
        end
    end
    for ((name, resolution), graph) in sort!(collect(graphs); by=first)
        @info "Preparing CPU graph" network=name resolution nodes=length(graph.h3) edges=length(graph.edge_to) workers=Threads.nthreads(:default)
        handlers[(name, resolution)] = make_handler(graph; workspace_pool, admission, progress=true, population,
            response_cache)
    end
    for name in unique(first.(specs))
        @info "Available network" network=name resolutions=sort([res for (network, res) in keys(handlers) if network == name])
    end
    return make_network_handler(handlers; default_network, admission)
end

if abspath(PROGRAM_FILE) == @__FILE__
    options = parse_cli(ARGS)
    options.paths == ["--demo"] && include("fixture.jl")
    handler = load_handlers(options.paths; options.population_path, options.max_pending, options.workspace_bytes)
    warmup_server()
    host = get(ENV, "ROUTER_HOST", "127.0.0.1")
    port = parse(Int, get(ENV, "ROUTER_PORT", "1988"))
    @info "Starting router" host port
    HTTP.serve(make_stream_handler(handler), host, port; stream=true)
end
