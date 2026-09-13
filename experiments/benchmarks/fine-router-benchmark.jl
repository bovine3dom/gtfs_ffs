module FineRouterBenchmark
using H3, SHA, Dates, Profile
const ROOT = normpath(joinpath(@__DIR__, "../.."))
const SRC = joinpath(ROOT, "router/src")
include(joinpath(SRC, "Reachability.jl"))
const R = Reachability
function engine(path)
    m = Module(:FineBaseline)
    Core.eval(m, :(using DataStructures, H3))
    for key in (:Graph, :Population, :WalkingIndex, :WalkingAdjacency, :INF, :PERIOD,
            :query_times, :_origin_radius, :_window_times, :_window_mode, :_walking_limit,
            :_prepare_population, :_population_rollup, :_route_population_reference,
            :_population_schedule_hints, :next_arrival, :walking_cells)
        Core.eval(m, :(const $key = $(getproperty(R, key))))
    end
    for file in ("population_packed.jl", "population_range.jl")
        for (name, dir) in (("baseline", path), ("candidate", SRC))
            println("SOURCE $name $file $(bytes2hex(sha256(read(joinpath(dir, file)))))")
        end
        Base.include(m, joinpath(path, file))
    end
    m
end
cell(lat, lon) = H3.API.latLngToCell(H3.API.LatLng(deg2rad(lat), deg2rad(lon)), 8)::UInt64
const CASES = [("short", UInt64(0x881fa44181fffff), 10, 18_000_000, 46_800_000, 216_000, 1_800_000, 360_000),
    ("Paris", cell(48.8566, 2.3522), 18, 28_800_000, 5_760_000, 60_000, 10_800_000, 3_600_000),
    ("London", cell(51.5074, -0.1278), 18, 28_800_000, 5_760_000, 60_000, 10_800_000, 3_600_000),
    ("long", cell(48.8566, 2.3522), 6, 28_800_000, 5_760_000, 60_000, 604_800_000, 3_600_000),
    ("transit", cell(48.8566, 2.3522), 18, 28_800_000, 5_760_000, 60_000, 10_800_000, 0),
    ("rural", cell(48.3, 3.0), 18, 28_800_000, 5_760_000, 60_000, 10_800_000, 3_600_000),
    [("Paris-S$s", cell(48.8566, 2.3522), 18, 28_800_000, s == 1 ? 0 : s * 60_000,
        60_000, 10_800_000, 3_600_000) for s in (1, 2, 4)]...]
function query(m, data, c, batch)
    m.route_population(data.graph, data.population, c[2], c[4], c[7]; walking_index=data.index,
        origin_radius=c[3], window_ms=c[5], step_ms=c[6], max_walk_ms=c[8], origin_batch_size=batch)
end
cpu() = ccall(:clock, Clong, ()) / 1e6
function hostcpu()
    x = parse.(Int, split(first(eachline("/proc/stat")))[2:end])
    sum(x[[1, 2, 3, 6, 7, 8]]) / 100
end
rss() = 1024parse(Int, match(r"VmRSS:\s+(\d+)", read("/proc/self/status", String))[1])
function classify(data, c, batch)
    g, wi, p = data.graph, data.index, data.population
    prepared = R._prepare_population(p, wi)
    origins = sort!(filter(!iszero, H3.API.gridDisk(c[2], c[3])))
    sources = R._population_sources(wi, prepared, R._population_rollup(p, 8), origins, UInt32(c[8]))
    network = wi.prepared.graph
    boardable(n) = g.out_ptr[n] < g.out_ptr[n + 1]
    heavy = count(eachindex(origins)) do i
        n = sources.sources[i]
        iszero(n) ? any(h -> boardable(first(h)), sources.access[i]) :
            boardable(n) || any(j -> network.durations[j] <= c[8] && boardable(network.targets[j]),
                network.offsets[n]:(network.offsets[n + 1] - 1))
    end
    samples = c[5] == 0 ? 1 : R._window_times(UInt32(c[4]), c[7], c[5], c[6])[2]
    tile = min(heavy, isnothing(batch) ? (samples == 1 || (samples > 4 && heavy >= 128Threads.nthreads()) ? 64 : 16) : batch)
    workers = iszero(tile) ? 0 : min(Threads.nthreads(), cld(heavy, tile))
    label_bytes = tile > 0 && samples > fld(64, tile) ? 8length(g.h3)*tile*workers : 0
    (; heavy, tile, workers, samples, label_bytes)
end
function run(baseline, data, output; cases=CASES, rounds=4)
    open(output, "w") do io
        println(io, "timestamp,case,batch,origins,heavy,tile,workers,samples,label_bytes,variant,pair,wall_s,cpu_s,bytes,gc_s,compile_s,rss_bytes,external_cores,shared,independent,max_error")
        for c in cases, batch in (c[1] == "short" ? (nothing, 64) : (nothing,))
            metadata = classify(data, c, batch)
            expected = nothing
            for pair in 0:rounds, m in (iseven(pair) ? (R, baseline) : (baseline, R))
                GC.gc()
                timestamp = now(UTC)
                before_cpu, before_host = cpu(), hostcpu()
                t = @timed query(m, data, c, batch)
                elapsed_cpu, host = cpu() - before_cpu, hostcpu() - before_host
                a = t.value
                if isnothing(expected)
                    expected = a
                end
                @assert a.h3 == expected.h3 && iszero.(a.value) == iszero.(expected.value)
                @assert all(isapprox.(a.value, expected.value; rtol=1e-12, atol=1e-6))
                @assert (a.shared_expansions, a.query_expansions) == (expected.shared_expansions, expected.query_expansions)
                @assert pair == 0 || iszero(t.compile_time)
                row = (timestamp, c[1], isnothing(batch) ? "default" : batch, length(a.h3),
                    metadata..., m === R ? "candidate" : "baseline", pair, t.time, elapsed_cpu,
                    t.bytes, t.gctime, t.compile_time, rss(), max(0, host - elapsed_cpu)/t.time,
                    a.shared_expansions, a.query_expansions, maximum(abs.(a.value - expected.value); init=0.0))
                println(io, join(row, ',')); flush(io)
                println(join(row, ',')); flush(stdout)
            end
            if c[1] == "short" && isnothing(batch)
                for m in (baseline, R)
                    Profile.clear()
                    Profile.@profile query(m, data, c, batch)
                    open(output * (m === R ? ".candidate-profile" : ".baseline-profile"), "w") do out
                        Profile.print(out; format=:flat, sortedby=:count, C=true)
                    end
                end
            end
        end
    end
end
function validate(baseline, data, output)
    open(output, "w") do io
        println(io, "case,mode,exclude,origins,max_error")
        for c in CASES[1:2], mode in (:mean_intersection, :max_intersection, :diff_intersection,
                :min_union, :diff_union, :reachable_union), exclude in (false, true)
            a, b = map((baseline, R)) do m
                m.route_population(data.graph, data.population, c[2], c[4], c[7]; walking_index=data.index,
                    origin_radius=c[1] == "short" ? c[3] : 1, window_ms=c[5], step_ms=c[6], max_walk_ms=c[8],
                    window_mode=mode, exclude_origin_population=exclude)
            end
            @assert a.h3 == b.h3 && iszero.(a.value) == iszero.(b.value)
            @assert all(isapprox.(a.value, b.value; rtol=1e-12, atol=1e-6))
            println(io, join((c[1], mode, exclude, length(a.h3), maximum(abs.(a.value - b.value); init=0.0)), ','))
            flush(io)
        end
    end
end
function prepare(path)
    println("START $(now(UTC)) julia=$VERSION threads=$(Threads.nthreads()) cpu=$(Sys.cpu_info()[1].model)")
    graph = R.pack_graph(path; skip_invalid_durations=true, badajoz_shuttle=true, progress=true)
    index = R.prepare_walking(R.WalkingIndex(graph); progress=true)
    population = R.load_population(joinpath(ROOT, "data/kontur_h3.arrow"))
    R._prepare_population(population, index; progress=true)
    R._population_schedule_hints(population, graph)
    println("READY $(now(UTC)) nodes=$(length(graph.h3)) edges=$(length(graph.edge_to)) profiles=$(length(graph.departure)) rss=$(rss())")
    println("DATA population_cells=$(length(population.h3)) geographic_walks=$(length(index.prepared.geographic.targets)) network_walks=$(length(index.prepared.graph.targets))")
    (; graph, index, population)
end
function main(args)
    length(args) == 3 || error("Use: fine-router-benchmark.jl BASELINE_DIR GRAPH.arrow OUTPUT.csv")
    baseline = engine(args[1])
    data = prepare(args[2])
    Base.invokelatest(run, baseline, data, args[3])
    Base.invokelatest(validate, baseline, data, args[3] * ".parity.csv")
end
end
if abspath(PROGRAM_FILE) == @__FILE__
    FineRouterBenchmark.main(ARGS)
end
