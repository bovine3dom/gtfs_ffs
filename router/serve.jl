include("src/Reachability.jl")
using .Reachability
import HTTP

function parse_cli(args)
    paths = String[]
    population_path = ""
    trip_shards_path = ""
    threads = Threads.nthreads(:default)
    short_workers = cld(threads, 4)
    bulk_workers = max(1, threads - short_workers)
    limits = Dict("--max-pending" => 128, "--workspace-memory-gib" => 8,
        "--short-workers" => short_workers,
        "--max-workers-per-request" => cld(bulk_workers, 2))
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
        elseif arg == "--trip-shards" || startswith(arg, "--trip-shards=")
            isempty(trip_shards_path) || throw(ArgumentError("duplicate --trip-shards option"))
            if arg == "--trip-shards"
                i += 1
                i <= length(args) || throw(ArgumentError("--trip-shards requires a path"))
                trip_shards_path = args[i]
            else
                trip_shards_path = split(arg, '='; limit=2)[2]
            end
            (!isempty(trip_shards_path) && !startswith(trip_shards_path, "--")) ||
                throw(ArgumentError("--trip-shards requires a path"))
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
            minimum, maximum = if name == "--max-pending"
                (0, typemax(Int) - 1)
            elseif name == "--workspace-memory-gib"
                (1, div(typemax(Int), 1024^3))
            else
                (1, threads)
            end
            !isnothing(value) && minimum <= value <= maximum ||
                throw(ArgumentError("$name requires an integer in $minimum..$maximum"))
            limits[name] = value
        else
            push!(paths, arg)
        end
        i += 1
    end
    return (; paths, population_path, trip_shards_path,
            max_pending=limits["--max-pending"],
            workspace_bytes=limits["--workspace-memory-gib"] * 1024^3,
            short_workers=limits["--short-workers"],
            max_workers_per_request="--max-workers-per-request" in seen ?
                limits["--max-workers-per-request"] :
                cld(max(1, threads - limits["--short-workers"]), 2))
end

function _startup_signature(path)
    path == "--demo" && return (path, "demo-v1")
    info = stat(path)
    return (abspath(path), info.size, info.mtime)
end

function load_handlers(paths; population_path="", trip_shards_path="", max_pending=128,
                       workspace_bytes=8*1024^3, short_workers=cld(Threads.nthreads(:default), 4),
                       max_workers_per_request=cld(max(1, Threads.nthreads(:default) - short_workers), 2))
    admission = RequestAdmission(; max_pending, memory_bytes=workspace_bytes,
        short_workers, max_workers_per_request)
    workspace_pool = PopulationWorkspacePool(; max_bytes=workspace_bytes)
    startup_cache = StartupCache()
    if isempty(paths) || ("--demo" in paths && paths != ["--demo"])
        error("usage: julia --threads=8 --project=router router/serve.jl [--population <path>] [--trip-shards <dir>] [--max-pending 128] [--workspace-memory-gib 8] [--short-workers N] [--max-workers-per-request N] (<name_resN.arrow> [name_resN.arrow ...] | --demo)")
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
    population_signature = isempty(population_path) ? nothing : _startup_signature(population_path)
    graphs = Dict{Tuple{String,Int},Graph}()
    for (path, (name, resolution)) in zip(paths, specs)
        Reachability._startup_stage(true, "Loading graph $path") do _
            key = (:graph, :legacy, _startup_signature(path), name, resolution)
            graph = startup_cache_load(startup_cache, key)
            if !(graph isa Graph)
                graph = path == "--demo" ? pack_graph(fixture_table(); progress=true, trip_aware=false) :
                    pack_graph(path; skip_invalid_durations=true, badajoz_shuttle=true, progress=true, trip_aware=false)
                startup_cache_save!(startup_cache, key, graph)
            else
                @info "Startup cache hit" kind="packed graph" source=path
            end
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
            key = (:derived_graph, :legacy, _startup_signature(sources[(name, 8)]), name, target)
            derived = startup_cache_load(startup_cache, key)
            if !(derived isa Graph)
                derived = coarsen_graph(graph, target; progress=true)
                startup_cache_save!(startup_cache, key, derived)
            else
                @info "Startup cache hit" kind="derived graph" network=name resolution=target
            end
            @info "Derived graph" network=name resolution=target source=sources[(name, 8)] source_resolution=8 nodes=length(derived.h3) edges=length(derived.edge_to)
            graphs[(name, target)] = derived
            graph = derived
        end
    end
    trip_shard_cache = TripShardCache()
    trip_shard_sets = Dict{Tuple{String,Int},Any}()
    for ((name, resolution), graph) in graphs
        path = haskey(sources, (name, resolution)) ? sources[(name, resolution)] : sources[(name, 8)]
        path == "--demo" && continue
        prepared_dir = isempty(trip_shards_path) ? nothing :
            joinpath(trip_shards_path, "$(name)_res$(resolution)")
        trip_shard_sets[(name, resolution)] = TripShardSet(path, graph;
            namespace=(name, resolution), cache=trip_shard_cache, startup_cache,
            disk_key_prefix=(:trip_shard, 2, _startup_signature(path), name, resolution),
            prepared_dir, progress=true)
    end
    for ((name, resolution), graph) in sort!(collect(graphs); by=first)
        @info "Preparing CPU graph" network=name resolution nodes=length(graph.h3) edges=length(graph.edge_to) workers=Threads.nthreads(:default)
        source_path = haskey(sources, (name, resolution)) ? sources[(name, resolution)] : sources[(name, 8)]
        state_key = (:prepared, :legacy, _startup_signature(source_path), name, resolution, population_signature)
        state = Ref{Any}(startup_cache_load(startup_cache, state_key))
        shard_set = get(trip_shard_sets, (name, resolution), nothing)
        loader = isnothing(shard_set) ? (() -> graph) :
            ((origin, max_walk_ms) -> trip_shard_acquire!(shard_set, origin, max_walk_ms))
        handlers[(name, resolution)] = make_handler(graph; workspace_pool, admission, progress=true, population,
            response_cache, startup_state=state, trip_graph_set=shard_set, trip_graph_loader=loader)
        isnothing(state[]) || startup_cache_save!(startup_cache, state_key, state[])
    end
    for name in unique(first.(specs))
        @info "Available network" network=name resolutions=sort([res for (network, res) in keys(handlers) if network == name])
    end
    return make_network_handler(handlers; default_network, admission)
end

if abspath(PROGRAM_FILE) == @__FILE__
    options = parse_cli(ARGS)
    options.paths == ["--demo"] && include("fixture.jl")
    handler = load_handlers(options.paths; options.population_path, options.trip_shards_path,
        options.max_pending, options.workspace_bytes, options.short_workers,
        options.max_workers_per_request)
    warmup_server()
    host = get(ENV, "ROUTER_HOST", "127.0.0.1")
    port = parse(Int, get(ENV, "ROUTER_PORT", "1988"))
    @info "Starting router" host port
    HTTP.serve(make_stream_handler(handler), host, port; stream=true)
end
