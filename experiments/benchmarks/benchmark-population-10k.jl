# Run with julia --threads=8,1 --project=router.
ENV["POP_SNAPSHOT"] = get(ENV, "POP_SNAPSHOT", "/tmp/opencode/population-10k-frozen")
ENV["POP_ARTIFACTS"] = get(ENV, "POP_ARTIFACTS", ENV["POP_SNAPSHOT"])
include("benchmark-population-optimization.jl")

# /proc/stat includes guest time in user/nice already. Do not count it twice.
function cpu_ticks()
    fields = parse.(Int, split(first(split(read("/proc/stat", String), '\n')))[2:9])
    return sum(fields[[1,2,3,6,7,8]])
end
function measured(f, label; limit=600, idle=false)
    GC.gc()
    ticks = ccall(:sysconf, Clong, (Cint,), 2)
    own, cpu = process_stats(getpid()), cpu_ticks()
    started = time()
    peak = Ref(memory_stats().rss)
    observer = Timer(0; interval=0.05) do _
        peak[] = max(peak[], memory_stats().rss)
    end
    watchdog = Timer(limit) do _
        logline("TIMEOUT $label benchmark_pid=$(getpid())")
        exit(124)
    end
    trial = try
        fetch(Threads.@spawn @timed f())
    finally
        close(watchdog)
        close(observer)
    end
    after = process_stats(getpid())
    wall = time() - started
    own_cpu = after.cpu - own.cpu
    external = (cpu_ticks() - cpu) / ticks - own_cpu
    logline("RUN $label seconds=$(trial.time) wall_s=$wall own_cpu_s=$own_cpu bytes=$(trial.bytes) rss=$(peak[]) external_cpu_s=$external external_cores=$(external/wall) shared_host=true faults=$(after.faults-own.faults)")
    return (; trial..., wall, own_cpu, external, external_cores=external/wall, clean=false, rss=peak[])
end
function clean_trial(f, label; limit=600)
    return measured(f, label; limit)
end
function parity(a, b)
    @assert a.h3 == b.h3
    @assert iszero.(a.value) == iszero.(b.value)
    @assert all(isapprox.(a.value, b.value; rtol=1e-12, atol=1e-6))
end
function shared_inputs(M)
    g, w, pp = borrow(M, graph), borrow(M, walking), borrow(M, prepared)
    p = M.Population(population.h3, population.weights, population.rollups,
        IdDict{M.WalkingIndex,M.PreparedPopulation}(w => pp), ReentrantLock())
    @assert g.departure === graph.departure
    @assert w.prepared.graph.targets === walking.prepared.graph.targets
    @assert M._prepare_population(p, w).weights === prepared.weights
    return (M, g, p, w)
end
const PARIS = H3.API.latLngToCell(H3.API.cellToLatLng(ORIGIN), 7)
const RURAL = H3.API.latLngToCell(H3.API.LatLng(deg2rad(46.6), deg2rad(2.5)), 7)
function query10(args; origin=PARIS, radius=18, budget=3, samples=96,
                 mode=:mean_intersection, exclude=false, tile=nothing)
    M, g, p, w = args
    Base.invokelatest(Base.invokelatest(getproperty, M, :route_population), g, p, origin, 28_800_000, budget * 3_600_000;
        origin_radius=radius, window_ms=samples == 1 ? 0 : samples * 900_000,
        step_ms=900_000, max_walk_ms=3_600_000, window_mode=mode, walking_index=w,
        exclude_origin_population=exclude, origin_batch_size=tile)
end
function main10()
    logline("ENV julia=$VERSION threads=$(Threads.nthreads(:default)) cpu=$(Sys.cpu_info()[1].model) pid=$(getpid()) memory=$(memory_stats())")
    for directory in (joinpath(SNAPSHOT, "router/src"), joinpath(ROOT, "router/src"))
        for path in sort!(readdir(directory; join=true))
            endswith(path, ".jl") || continue
            logline("SOURCE path=$path bytes=$(filesize(path)) sha256=$(bytes2hex(open(sha256, path)))")
        end
    end
    for file in ("everything_res6.arrow", "everything_res7.arrow", "everything_res8.arrow", "kontur_h3.arrow")
        path = joinpath(ROOT, "data", file)
        table = Arrow.Table(path)
        logline("INPUT file=$file bytes=$(filesize(path)) rows=$(length(first(Tuple(table)))) sha256=$(bytes2hex(open(sha256, path)))")
    end
    global graph = measured(() -> B.pack_graph(joinpath(ROOT, "data/everything_res7.arrow");
        skip_invalid_durations=true, badajoz_shuttle=true, progress=true), "pack"; limit=900, idle=false).value
    global walking = measured(() -> B.prepare_walking(B.WalkingIndex(graph); progress=true), "walking"; limit=900, idle=false).value
    global population = measured(() -> B.load_population(joinpath(ROOT, "data/kontur_h3.arrow"); progress=true), "population"; limit=900, idle=false).value
    global prepared = measured(() -> B._prepare_population(population, walking; progress=true), "sidecar"; limit=900, idle=false).value
    global BASE = (B, graph, population, walking)
    logline("GRAPH nodes=$(length(graph.h3)) edges=$(length(graph.edge_to)) profiles=$(length(graph.departure)) walks=$(length(walking.prepared.graph.targets)) population_walks=$(length(prepared.targets)) memory=$(memory_stats())")
    for (name, origin) in (("Paris", PARIS), ("rural", RURAL))
        logline("ORIGIN name=$name h3=$(string(origin; base=16)) coordinates=$(H3.API.cellToLatLng(origin)) on_graph=$(haskey(graph.node_id, origin)) disk18=$(length(H3.API.gridDisk(origin,18))) disk57=$(length(H3.API.gridDisk(origin,57)))")
    end
    command = 1
    while true
        path = joinpath(ARTIFACTS, "command-$command.jl")
        if isfile(path)
            try
                Base.include(Main, path)
            catch error
                logline("COMMAND_ERROR " * sprint(showerror, error, catch_backtrace()))
            end
            command += 1
        end
        logline("RESIDENT next=$(joinpath(ARTIFACTS, "command-$command.jl"))")
        while !isfile(joinpath(ARTIFACTS, "command-$command.jl"))
            sleep(1)
        end
    end
end
if abspath(PROGRAM_FILE) == @__FILE__
    main10()
end
