ENV["POP_SNAPSHOT"] = get(ENV, "POP_SNAPSHOT", "/tmp/opencode/population-cpu-frozen")
ENV["POP_ARTIFACTS"] = get(ENV, "POP_ARTIFACTS", "/tmp/opencode/population-cpu-frozen")
ENV["POP_SERVER_PID"] = get(ENV, "POP_SERVER_PID", "965904")
include("benchmark-population-optimization.jl")

const ORIGIN7 = H3.API.latLngToCell(H3.API.cellToLatLng(ORIGIN), 7)
function query7(M, g, p, index, radius=6, budget=3; mode=:mean_intersection, exclude=false)
    M.route_population(g, p, ORIGIN7, 28_800_000, budget * 3_600_000;
        origin_radius=radius, window_ms=86_400_000, step_ms=900_000,
        max_walk_ms=3_600_000, window_mode=mode, walking_index=index,
        exclude_origin_population=exclude)
end

function main_queue()
    logline("ENV julia=$VERSION threads=$(Threads.nthreads(:default)) pid=$(getpid()) cpu=$(Sys.cpu_info()[1].model) memory=$(memory_stats())")
    for file in ("everything_res6.arrow", "everything_res7.arrow", "everything_res8.arrow", "kontur_h3.arrow")
        path = joinpath(ROOT, "data", file)
        table = Arrow.Table(path)
        logline("INPUT file=$file bytes=$(filesize(path)) rows=$(length(first(Tuple(table)))) sha256=$(bytes2hex(open(sha256, path)))")
    end
    global graph = measured(() -> B.pack_graph(joinpath(ROOT, "data/everything_res7.arrow");
        skip_invalid_durations=true, badajoz_shuttle=true, progress=true), "pack res7"; limit=900).value
    global walking = measured(() -> B.prepare_walking(B.WalkingIndex(graph); progress=true), "prepare walk1"; limit=900).value
    global population = measured(() -> B.load_population(joinpath(ROOT, "data/kontur_h3.arrow"); progress=true), "population"; limit=900).value
    global prepared = measured(() -> B._prepare_population(population, walking; progress=true), "sidecar"; limit=900).value
    logline("GRAPH nodes=$(length(graph.h3)) edges=$(length(graph.edge_to)) profiles=$(length(graph.departure)) network_walks=$(length(walking.prepared.graph.targets)) population_walks=$(length(prepared.targets)) graph_bytes=$(Base.summarysize(graph)) walking_bytes=$(Base.summarysize(walking)) sidecar_bytes=$(Base.summarysize(prepared)) rollup_bytes=$(Base.summarysize(population.rollups[7])) population_sum=$(sum(values(population.rollups[7])))")
    measured(() -> query7(B, graph, population, walking), "baseline warm")
    global baseline = measured(() -> query7(B, graph, population, walking), "baseline k6 b3")
    logline("BASELINE shared=$(baseline.value.shared_expansions) queries=$(baseline.value.query_expansions)")
    profile_case(() -> query7(B, graph, population, walking), "packed-res7"; source_file="population_packed.jl")
    logline("RESIDENT next=$ARTIFACTS/queue-command-1.jl")
    command = 1
    while true
        path = joinpath(ARTIFACTS, "queue-command-$command.jl")
        if isfile(path)
            try
                Base.include(Main, path)
            catch error
                showerror(stderr, error, catch_backtrace())
                println(stderr)
            end
            command += 1
            logline("COMMAND_COMPLETE next=$ARTIFACTS/queue-command-$command.jl")
        end
        sleep(1)
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    main_queue()
end
