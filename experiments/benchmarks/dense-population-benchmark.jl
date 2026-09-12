module DensePopulationBenchmark
using H3, SHA

const ROOT = normpath(joinpath(@__DIR__, "../.."))
include(joinpath(ROOT, "router/src/Reachability.jl"))
const CASES = [("Paris", 18, 96, 3), ("Paris", 57, 96, 3), ("Paris", 6, 96, 168),
    ("Paris", 18, 96, 12), ("London", 18, 96, 3), ("Rural", 18, 96, 3),
    ("Chad", 57, 96, 3), [(city, 18, samples, 3) for city in ("Paris", "Rural") for samples in (1, 2, 4)]...]
const MODES = (:mean_intersection, :max_intersection, :diff_intersection,
    :min_union, :diff_union, :reachable_union)

function engine(shared, source, name)
    m = Module(name)
    Core.eval(m, :(using DataStructures, H3))
    for key in (:Graph, :Population, :WalkingIndex, :WalkingAdjacency, :INF, :PERIOD,
            :query_times, :_origin_radius, :_window_times, :_window_mode, :_walking_limit,
            :_prepare_population, :_population_rollup, :_route_population_reference,
            :_population_schedule_hints, :next_arrival, :walking_cells)
        Core.eval(m, :(const $key = $(getproperty(shared, key))))
    end
    for file in ("population_packed.jl", "population_range.jl")
        path = joinpath(source, "router/src", file)
        println("SOURCE $name $file $(bytes2hex(sha256(read(path))))")
        Base.include(m, path)
    end
    m
end

function query(m, data, case; mode=:mean_intersection, exclude=false)
    city, radius, samples, hours = case
    origin = city == "Paris" ? UInt64(0x871fb4660ffffff) : city == "Rural" ? UInt64(0x871f94d80ffffff) :
        city == "Chad" ? UInt64(0x876bac79cffffff) :
        H3.API.latLngToCell(H3.API.LatLng(deg2rad(51.5074), deg2rad(-0.1278)), 7)::UInt64
    m.route_population(data.graph, data.population, origin, 28_800_000, hours * 3_600_000;
        walking_index=data.index, origin_radius=radius, window_ms=samples == 1 ? 0 : samples * 900_000,
        step_ms=900_000, window_mode=mode, exclude_origin_population=exclude)
end

function parity(a, b)
    @assert a.h3 == b.h3
    @assert iszero.(a.value) == iszero.(b.value)
    @assert all(isapprox.(a.value, b.value; rtol=1e-12, atol=1e-6))
    maximum(abs.(a.value .- b.value); init=0.0)
end

cpu() = ccall(:clock, Clong, ()) / 1e6
function hostcpu()
    x = parse.(Int, split(first(eachline("/proc/stat")))[2:end])
    sum(x[[1, 2, 3, 6, 7, 8]]) / 100
end
rss() = 1024parse(Int, match(r"VmRSS:\s+(\d+)", read("/proc/self/status", String))[1])

function run(baseline, candidate, data, io; cases=CASES, rounds=3)
    println(io, "city,radius,origins,samples,budget_h,variant,pair,wall_s,cpu_s,bytes,gc_s,compile_s,rss_bytes,external_cores,shared,independent,max_error")
    for case in cases
        expected = nothing
        for pair in 0:rounds, m in (iseven(pair) ? (candidate, baseline) : (baseline, candidate))
            GC.gc()
            c, h = cpu(), hostcpu()
            t = @timed query(m, data, case)
            elapsed_cpu, external = cpu() - c, hostcpu() - h
            max_error = isnothing(expected) ? 0.0 : parity(expected, t.value)
            isnothing(expected) && (expected = t.value)
            row = (case[1], case[2], length(t.value.h3), case[3], case[4],
                m === baseline ? "baseline" : "candidate", pair, t.time, elapsed_cpu, t.bytes,
                t.gctime, t.compile_time, rss(), max(0.0, external - elapsed_cpu) / t.time,
                t.value.shared_expansions, t.value.query_expansions, max_error)
            println(io, join(row, ',')); flush(io)
            println("TRIAL ", join(row, ',')); flush(stdout)
            pair == 0 || @assert t.compile_time == 0
        end
    end
end

function validate(baseline, candidate, data)
    for case in (("Paris", 6, 96, 3), ("Paris", 18, 96, 3)), mode in MODES, exclude in (false, true)
        case[2] == 18 && !(mode in (:mean_intersection, :reachable_union)) && continue
        a = query(baseline, data, case; mode, exclude)
        b = query(candidate, data, case; mode, exclude)
        println("PARITY case=$case mode=$mode exclude=$exclude max_error=$(parity(a, b))")
        flush(stdout)
    end
end

function main(args)
    length(args) == 2 || error("Use: dense-population-benchmark.jl FROZEN_SOURCE OUTPUT.csv")
    frozen, output = args
    # Share unchanged types, geometry, weights, and schedule hints. Do not clone the graph.
    for file in readdir(joinpath(ROOT, "router/src"))
        file in ("population_packed.jl", "population_range.jl") && continue
        @assert read(joinpath(ROOT, "router/src", file)) == read(joinpath(frozen, "router/src", file))
    end
    r = Reachability
    baseline = engine(r, frozen, :Baseline)
    data = Base.invokelatest() do
        packed = @timed r.pack_graph(joinpath(ROOT, "data/everything_res7.arrow");
            skip_invalid_durations=true, badajoz_shuttle=true)
        graph = packed.value
        walking = @timed r.prepare_walking(r.WalkingIndex(graph))
        population = r.load_population(joinpath(ROOT, "data/kontur_h3.arrow"))
        r._prepare_population(population, walking.value)
        r._population_schedule_hints(population, graph)
        println("STARTUP julia=$VERSION threads=$(Threads.nthreads()) pack_s=$(packed.time) walk_s=$(walking.time) maxrss=$(Sys.maxrss())")
        (; graph, index=walking.value, population)
    end
    open(output, "w") do io
        Base.invokelatest(run, baseline, r, data, io)
    end
    Base.invokelatest(validate, baseline, r, data)
end
end

if abspath(PROGRAM_FILE) == @__FILE__
    DensePopulationBenchmark.main(ARGS)
end
