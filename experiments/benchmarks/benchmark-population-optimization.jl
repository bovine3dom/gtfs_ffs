using Arrow, Dates, H3, Profile, Serialization, SHA, Statistics

const ROOT = normpath(joinpath(@__DIR__, "../.."))
const SNAPSHOT = get(ENV, "POP_SNAPSHOT", "/tmp/opencode/population-3980e5f")
const ARTIFACTS = get(ENV, "POP_ARTIFACTS", SNAPSHOT)
const SERVER_PID = parse(Int, get(ENV, "POP_SERVER_PID", "915022"))
const CALL_LIMIT = parse(Float64, get(ENV, "POP_CALL_LIMIT", "180"))
const ORIGIN = UInt64(0x861fb4667ffffff)
const RESULTS = Dict{Tuple,Any}()

module Frozen
    include(joinpath(Main.SNAPSHOT, "router/src/Reachability.jl"))
end
const B = Frozen.Reachability

logline(text) = (println(now(), " ", text); flush(stdout))

function process_stats(pid)
    isfile("/proc/$pid/stat") || return (cpu=NaN, faults=0)
    fields = split(split(read("/proc/$pid/stat", String), ") "; limit=2)[2])
    ticks = ccall(:sysconf, Clong, (Cint,), 2) # Linux _SC_CLK_TCK.
    return (cpu=(parse(Int, fields[12]) + parse(Int, fields[13])) / ticks,
            faults=parse(Int, fields[10]))
end

function memory_stats()
    fields = Dict(first(parts) => parse(Int, parts[2]) * 1024
        for parts in split.(split(strip(read("/proc/meminfo", String)), '\n')))
    status = read("/proc/self/status", String)
    rss = parse(Int, match(r"VmRSS:\s+(\d+)", status)[1]) * 1024
    return (; available=fields["MemAvailable:"], swap=fields["SwapTotal:"] - fields["SwapFree:"], rss,
            peak=Sys.maxrss())
end

function wait_for_idle()
    for attempt in 1:15
        before = process_stats(SERVER_PID)
        started = time()
        sleep(2)
        cpu = (process_stats(SERVER_PID).cpu - before.cpu) / (time() - started)
        cpu <= 0.05 && return
        logline("WAIT server_pid=$SERVER_PID cores=$cpu attempt=$attempt")
    end
    error("The live server is busy. Do not use concurrent timings.")
end

function measured(f, label; limit=CALL_LIMIT, idle=true)
    idle && wait_for_idle()
    GC.gc()
    before, own = process_stats(SERVER_PID), process_stats(getpid())
    logline("BEGIN $label memory=$(memory_stats()) limit_s=$limit")
    # The timer runs on the interactive thread, not on a routing worker.
    watchdog = Timer(limit) do _
        logline("TIMEOUT $label; stop only benchmark pid=$(getpid())")
        exit(124)
    end
    trial = try
        fetch(Threads.@spawn @timed f())
    finally
        close(watchdog)
    end
    server_cpu = process_stats(SERVER_PID).cpu - before.cpu
    after = process_stats(getpid())
    logline("RUN $label seconds=$(trial.time) bytes=$(trial.bytes) allocations=$(Base.gc_alloc_count(trial.gcstats)) gc_s=$(trial.gctime) own_cpu_s=$(after.cpu-own.cpu) major_faults=$(after.faults-own.faults) server_cpu_s=$server_cpu contaminated=$(server_cpu > max(0.05, trial.time*0.05)) memory=$(memory_stats())")
    return trial
end

function query(M, g, p, index, radius, budget_h, samples, mode)
    M.route_population(g, p, ORIGIN, 28_800_000, budget_h * 3_600_000;
        origin_radius=radius, window_ms=samples * 900_000, step_ms=900_000,
        max_walk_ms=3_600_000, window_mode=mode, walking_index=index)
end

function run_case(M, g, p, index, radius, budget_h, samples;
                  mode=:mean_intersection, tag="baseline")
    key = (radius, budget_h, samples, mode)
    label = "$tag k=$radius B_h=$budget_h S=$samples mode=$mode"
    f = () -> query(M, g, p, index, key...)
    warm = measured(f, "$label warm")
    trials = [measured(f, "$label trial=1")]
    if warm.time < 30 && trials[1].time < 30
        for run in 2:3
            push!(trials, measured(f, "$label trial=$run"))
            if last(trials).time >= 30
                trials = trials[end:end]
                break
            end
        end
    end
    result = last(trials).value
    @assert length(result.h3) == 1 + 3radius * (radius + 1)
    @assert issorted(result.h3) && all(isfinite, result.value)
    if tag == "baseline"
        RESULTS[key] = result
        serialize(joinpath(ARTIFACTS, "baseline-k$radius-b$budget_h-s$samples-$mode.jls"), result)
    else
        expected = RESULTS[key]
        @assert result.h3 == expected.h3
        @assert all(isapprox.(result.value, expected.value; rtol=1e-12, atol=1e-6))
        logline("PARITY $label PASS")
    end
    seconds = median(t.time for t in trials)
    logline("RESULT $label n=$(length(trials)) median_s=$seconds median_bytes=$(median(t.bytes for t in trials)) origins=$(length(result.h3)) workers=$(result.workers) shared=$(result.shared_expansions) queries=$(result.query_expansions) value_sum=$(sum(result.value))")
    return seconds
end

function profile_case(f, label; source_file="population.jl")
    Profile.clear()
    Profile.init(n=5_000_000, delay=0.002)
    measured(() -> Profile.@profile(f()), "$label CPU_profile")
    data, dict = Profile.retrieve()
    serialize(joinpath(ARTIFACTS, "$label.profile.jls"), (data, dict))
    for format in (:flat, :tree)
        open(joinpath(ARTIFACTS, "$label.$format.txt"), "w") do io
            Profile.print(IOContext(io, :displaysize => (1000, 240)), data, dict;
                format, C=true, sortedby=:count, mincount=5, groupby=:none)
        end
    end
    # Attribute each routed stack to its innermost population source line.
    raw, lines = Profile.retrieve(include_meta=false)
    counts = Dict{Int,Int}()
    inclusive = Dict{String,Int}()
    stack = UInt64[]
    snapshots = routed = 0
    for ip in raw
        if ip != 0
            push!(stack, ip)
            continue
        end
        isempty(stack) && continue
        snapshots += 1
        frames = [frame for address in stack for frame in get(lines, address, [])]
        at = findfirst(frame -> endswith(string(frame.file), "/$source_file"), frames)
        if !isnothing(at)
            routed += 1
            line = frames[at].line
            counts[line] = get(counts, line, 0) + 1
            for name in unique(string(frame.func) for frame in frames)
                inclusive[name] = get(inclusive, name, 0) + 1
            end
        end
        empty!(stack)
    end
    open(joinpath(ARTIFACTS, "$label.attribution.txt"), "w") do io
        println(io, "snapshots=$snapshots routed=$routed; source lines are exclusive; function counts overlap")
        for (line, count) in sort!(collect(counts); by=last, rev=true)
            println(io, "$source_file:$line count=$count percent=", round(100count/routed; digits=2))
        end
        println(io, "INCLUSIVE FUNCTIONS")
        for (name, count) in sort!(collect(inclusive); by=last, rev=true)
            count >= max(5, routed / 1000) && println(io, "$name count=$count percent=", round(100count/routed; digits=2))
        end
    end
    logline("PROFILE_READY $label snapshots=$snapshots routed=$routed files=$ARTIFACTS/$label.{flat,tree,attribution}.txt")
end

function compare_population_case(f, expected, label; baseline_s=nothing, exact=true)
    function validate(result)
        @assert result.h3 == expected.h3
        @assert all(isfinite, result.value)
        @assert exact ? result.value == expected.value :
            all(isapprox.(result.value, expected.value; rtol=1e-12, atol=1e-6))
    end
    warm = measured(f, "$label warm"; limit=120)
    validate(warm.value)
    trials = [measured(f, "$label trial=1"; limit=120)]
    validate(trials[1].value)
    if warm.time < 30 && trials[1].time < 30
        for run in 2:3
            trial = measured(f, "$label trial=$run"; limit=120)
            validate(trial.value)
            trial.time >= 30 && empty!(trials)
            push!(trials, trial)
            trial.time >= 30 && break
        end
    end
    seconds, bytes = median(t.time for t in trials), median(t.bytes for t in trials)
    result = last(trials).value
    speedup = isnothing(baseline_s) ? NaN : baseline_s / seconds
    logline("COMPARE $label parity=PASS exact=$exact n=$(length(trials)) seconds=$seconds bytes=$bytes allocations=$(median(Base.gc_alloc_count(t.gcstats) for t in trials)) gc_s=$(median(t.gctime for t in trials)) baseline_s=$baseline_s speedup=$speedup workers=$(result.workers) shared=$(result.shared_expansions) queries=$(result.query_expansions)")
    return (; result, seconds, bytes, speedup, times=[t.time for t in trials],
            gc_s=median(t.gctime for t in trials))
end

# Rebuild module-specific wrappers. Keep the same graph and adjacency vectors.
function borrow(M, x)
    parentmodule(typeof(x)) == B || return x
    constructor = getproperty(M, nameof(typeof(x)))
    return constructor((borrow(M, getfield(x, field)) for field in fieldnames(typeof(x)))...)
end

function main()
    logline("ENV julia=$VERSION threads=$(Threads.nthreads(:default)) interactive_threads=$(Threads.nthreads(:interactive)) pid=$(getpid()) cpu=$(Sys.cpu_info()[1].model) memory=$(memory_stats())")
    for path in sort!(readdir(joinpath(SNAPSHOT, "router/src"); join=true))
        endswith(path, ".jl") || continue
        current = joinpath(ROOT, "router/src", basename(path))
        logline("SOURCE file=$(basename(path)) frozen_sha256=$(bytes2hex(sha256(read(path)))) current_sha256=$(bytes2hex(sha256(read(current))))")
        @assert read(path) == read(current)
    end
    network, popfile = joinpath(ROOT, "data/everything_res6.arrow"), joinpath(ROOT, "data/kontur_h3.arrow")
    for path in (network, popfile)
        hash = open(sha256, path)
        logline("INPUT path=$path bytes=$(filesize(path)) mtime=$(stat(path).mtime) sha256=$(bytes2hex(hash))")
    end
    table = Arrow.Table(network)
    @assert length(table.from_h3) == 205_357_244
    logline("NETWORK rows=$(length(table.from_h3))")
    table = nothing
    global graph = measured(() -> B.pack_graph(network; skip_invalid_durations=true,
        badajoz_shuttle=true, progress=true), "pack"; limit=900).value
    global walking = measured(() -> B.prepare_walking(B.WalkingIndex(graph); progress=true), "prepare_walking").value
    global population = measured(() -> B.load_population(popfile; progress=true), "load_population").value
    @assert length(population.h3) == 32_957_699
    rolled = measured(() -> B._population_rollup(population, 6), "rollup_res6")
    @assert length(rolled.value) == 2_016_971
    @assert sum(values(rolled.value)) == 8_031_924_024
    @assert collect(keys(population.rollups)) == [6]
    logline("PREPARED graph_bytes=$(Base.summarysize(graph)) walking_bytes=$(Base.summarysize(walking)) rollup_bytes=$(Base.summarysize(rolled.value)) vertices=$(length(graph.h3)) edges=$(length(graph.edge_to)) profiles=$(length(graph.departure))")
    run_case(B, graph, population, walking, 6, 3, 96)
    profile_case(() -> query(B, graph, population, walking, 6, 3, 96, :mean_intersection), "baseline-k6-b3-s96")
    logline("BASELINE_PROFILE_DONE production_edits_can_begin=true")
    for radius in (10, 18)
        run_case(B, graph, population, walking, radius, 3, 96)
    end
    seconds = run_case(B, graph, population, walking, 6, 168, 4)
    estimate = seconds * (96 / 4)
    if estimate < CALL_LIMIT / 2
        run_case(B, graph, population, walking, 6, 168, 96)
    else
        logline("UNRUN k=6 B_h=168 S=96 estimated_s=$estimate benchmark_limit_s=$CALL_LIMIT")
    end
    logline("BASELINE_COMPLETE memory=$(memory_stats())")
    # Keep this one loaded dataset for a bounded follow-up phase.
    deadline = time() + parse(Float64, get(ENV, "POP_IDLE_SECONDS", "1800"))
    command = 1
    logline("RESIDENT until_unix=$deadline next=$ARTIFACTS/command-$command.jl")
    while time() < deadline
        path = joinpath(ARTIFACTS, "command-$command.jl")
        if isfile(path)
            try
                Base.include(Main, path)
            catch error
                showerror(stderr, error, catch_backtrace())
                println(stderr)
            end
            command += 1
            logline("COMMAND_COMPLETE next=$ARTIFACTS/command-$command.jl")
        end
        sleep(1)
    end
    logline("EXIT resident timeout; no user process changed")
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
