using Arrow, H3, SHA, Statistics
include("../gpu/Reachability.jl")
using .Reachability
import KernelAbstractions as KA
const R = Reachability
if "--backend=oneapi" in ARGS
    import oneAPI
elseif "--backend=cuda" in ARGS
    import CUDA
end

function benchmark(backend_name, cases)
    backend = if backend_name == "oneapi"
        oneAPI.functional() || error("oneAPI unavailable")
        oneAPI.allowscalar(false)
        oneAPI.versioninfo()
        oneAPI.oneAPIBackend()
    elseif backend_name == "cuda"
        CUDA.functional() || error("CUDA unavailable")
        CUDA.allowscalar(false)
        CUDA.versioninfo()
        CUDA.CUDABackend()
    elseif backend_name == "cpu"
        nothing
    else
        error("backend must be cpu, oneapi, or cuda")
    end
    println("julia=", VERSION, " threads=", Threads.nthreads(), " backend=", backend_name)
    graphs = map(("data/austria_adjacent_res8.arrow", "data/austria_shortcuts_res8.arrow")) do path
        measured = @timed pack_graph(path)
        g = measured.value
        println("graph=", path, " sha256=", open(sha256, path) |> bytes2hex,
            " load_pack_s=", measured.time, " nodes=", length(g.h3), " edges=", length(g.edge_to),
            " profile_entries=", length(g.departure), " packed_array_bytes=",
            sum(sizeof, (g.h3, g.out_ptr, g.edge_from, g.edge_to, g.schedule_ptr, g.departure, g.arrival, g.distance_km)),
            " graph_summary_bytes=", Base.summarysize(g))
        flush(stdout)
        g
    end
    g, s = graphs
    g.h3 == s.h3 || error("Node sets differ")
    centre = H3.API.LatLng(deg2rad(48.1855), deg2rad(16.3768))
    distances = [H3.Lib.greatCircleDistanceKm(Ref(centre), Ref(H3.API.cellToLatLng(h))) for h in g.h3]
    origin = g.h3[argmin(distances)]
    println("origin=", string(origin; base=16), " Vienna_Hbf_distance_km=", minimum(distances),
        " centre_parent=", string(H3.API.cellToParent(H3.API.latLngToCell(centre, 11), 8); base=16))
    for h in unique([origin; g.h3[round.(Int, range(1, length(g.h3); length=7))]]),
            ready in (28_799_999, 28_800_000, 28_800_001), budget in (10_800_000, 604_800_000)
        route_cpu(g, h, ready, budget) == route_cpu(s, h, ready, budget) || error("Transit label parity failed")
    end
    for (name, graph) in zip(("adjacent", "shortcuts"), graphs)
        f() = route_cpu(graph, origin, 28_800_000, 10_800_000)
        f()
        times = [(@timed f()).time for _ in 1:3]
        println("point graph=", name, " median_s=", median(times), " reached=", count(!=(R.INF), f()))
    end
    prepared = @timed prepare_walking(WalkingIndex(g); max_walk_ms=3_600_000)
    index = prepared.value
    p = index.prepared
    println("walking_prepare_s=", prepared.time, " network_walk_rows=", length(p.graph.targets),
        " output_walk_rows=", length(p.output.targets), " output_cells=", length(p.output_cells))
    for ready in (28_799_999, 28_800_000, 28_800_001)
        a = route_walking(g, origin, ready, 10_800_000; walking_index=index, distance_mode=:straight_line)
        b = route_walking(s, origin, ready, 10_800_000; walking_index=index, distance_mode=:straight_line)
        a.h3 == b.h3 && a.arrival == b.arrival || error("Walking arrival parity failed")
        println("walking_point ready=", ready, " covered_cells=", length(a.h3), " parity=exact")
    end
    loaded = @timed load_population("data/kontur_h3.arrow")
    population = loaded.value
    println("population_load_s=", loaded.time, " raw_rows=", length(population.h3),
        " sum_population=", sum(population.weights), " sha256=", open(sha256, "data/kontur_h3.arrow") |> bytes2hex)
    flush(stdout)
    routers = isnothing(backend) ? nothing : map(graphs) do graph
        uploaded = @timed PopulationKernelRouter(graph, population, backend; walking_index=index)
        router = uploaded.value
        bytes = sum(v -> v isa Tuple ? sum(sizeof, v) : sizeof(v), values(router.device))
        println("resident_upload_s=", uploaded.time, " device_array_bytes=", bytes,
            " label_bytes=", 4*32*length(graph.h3)*sizeof(UInt32))
        flush(stdout)
        router
    end
    function encode(result)
        io = IOBuffer()
        Arrow.write(io, (; h3=result.h3, value=result.value); file=true, compress=nothing)
        return (; result, encoded=take!(io))
    end
    function parity(actual, expected; exact=true)
        actual.h3 == expected.h3 && iszero.(actual.value) == iszero.(expected.value) || error("Origin/zero parity failed")
        ok = exact ? actual.value == expected.value : all(isapprox.(actual.value, expected.value;
            rtol=R.GPU_FLOAT32_RTOL, atol=R.GPU_FLOAT32_ATOL))
        ok || error("Population parity failed; max error=$(maximum(abs.(actual.value .- expected.value)))")
        return maximum(abs.(actual.value .- expected.value); init=0.0)
    end
    for (radius, samples) in cases
        options = (; walking_index=index, origin_radius=radius, max_walk_ms=3_600_000,
            window_ms=samples == 1 ? 0 : samples*900_000, step_ms=900_000)
        expected = nothing
        for (i, name) in enumerate(("adjacent", "shortcuts"))
            cpu() = encode(route_population(graphs[i], population, origin, 28_800_000, 10_800_000; options...))
            warm = @timed cpu()
            i == 1 && (expected = warm.value.result)
            parity(warm.value.result, expected)
            times = Float64[]
            allocations = Int[]
            result = warm.value.result
            for _ in 1:(warm.time > 10 ? 1 : 3)
                run = @timed cpu()
                parity(run.value.result, expected)
                push!(times, run.time); push!(allocations, run.bytes)
                result = run.value.result
            end
            println("cpu graph=", name, " radius=", radius, " origins=", length(result.h3), " samples=", samples,
                " warm_s=", warm.time, " repetitions=", length(times), " median_s=", median(times),
                " allocation_bytes=", maximum(allocations), " workers=", result.workers,
                " shared_expansions=", result.shared_expansions, " query_expansions=", result.query_expansions,
                " min_population=", minimum(result.value), " max_population=", maximum(result.value),
                " encoded_bytes=", length(warm.value.encoded), " parity=exact")
            flush(stdout)
        end
        isnothing(routers) && continue
        for (i, name) in enumerate(("adjacent", "shortcuts"))
            device() = encode(R._route_population_gpu(routers[i], origin, 28_800_000, 10_800_000;
                origin_radius=radius, max_walk_ms=3_600_000,
                window_ms=options.window_ms, step_ms=options.step_ms))
            GC.gc(true)
            warm = @timed device()
            max_error = parity(warm.value.result, expected; exact=false)
            println("device_warm graph=", name, " radius=", radius, " samples=", samples,
                " total_s=", warm.time, " rounds=", warm.value.result.rounds, " max_abs_error=", max_error)
            flush(stdout)
            times = Float64[]
            result = warm.value.result
            for _ in 1:(warm.time > 10 ? 1 : 3)
                GC.gc(true)
                run = @timed device()
                max_error = max(max_error, parity(run.value.result, expected; exact=false))
                push!(times, run.time)
                result = run.value.result
            end
            println("device graph=", name, " radius=", radius, " origins=", length(result.h3), " samples=", samples,
                " repetitions=", length(times), " median_s=", median(times), " rounds=", result.rounds,
                " batches=", result.batches, " planning_s=", result.planning_s, " upload_s=", result.upload_s,
                " device_checks_s=", result.device_s, " download_s=", result.download_s,
                " bytes_downloaded=", result.bytes_downloaded, " max_abs_error=", max_error,
                " encoded_bytes=", length(warm.value.encoded))
            flush(stdout)
        end
    end
end

function main(args)
    backend = "cpu"
    cases = Tuple{Int,Int}[]
    logfile = nothing
    for arg in args
        if startswith(arg, "--backend=")
            backend = split(arg, '='; limit=2)[2]
        elseif startswith(arg, "--case=")
            push!(cases, Tuple(parse.(Int, split(split(arg, '='; limit=2)[2], ':'))))
        elseif startswith(arg, "--log=")
            logfile = split(arg, '='; limit=2)[2]
        else
            error("Usage: benchmark.jl [--backend=cpu|oneapi|cuda] [--case=radius:samples] [--log=fresh_path]")
        end
    end
    isempty(cases) && append!(cases, backend == "cpu" ? [(0,1), (6,1), (6,4), (6,96), (10,96), (18,96)] : [(0,1), (6,1), (6,4)])
    all(c -> c[1] >= 0 && c[2] >= 1, cases) || error("Cases require a nonnegative radius and at least one sample")
    if isnothing(logfile)
        benchmark(backend, cases)
    else
        ispath(logfile) && error("Log already exists: $logfile")
        open(logfile, "w") do io
            redirect_stdout(io) do
                benchmark(backend, cases)
            end
        end
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    main(ARGS)
end
