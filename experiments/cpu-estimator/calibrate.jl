using H3, HTTP, JSON, Dates, SHA
include("../../router/src/Reachability.jl")
const R = Reachability
const ROOT = normpath(joinpath(@__DIR__, "../.."))
const CITIES = [("Paris", 48.8566, 2.3522, "train"), ("Berlin", 52.52, 13.405, "train"),
    ("Vienna", 48.2082, 16.3738, "train"), ("rural", 48.3, 3.0, "train"),
    ("Munich", 48.1351, 11.582, "calibration"), ("London", 51.5074, -0.1278, "test")]
const MODES = ["mean_intersection", "min_union", "max_intersection", "diff_union", "diff_intersection", "reachable_union"]
cpu() = ccall(:clock, Clong, ()) / 1e6
rss() = 1024parse(Int, match(r"VmRSS:\s+(\d+)", read("/proc/self/status", String))[1])
function hostseconds()
    x = parse.(Int, split(first(eachline("/proc/stat")))[2:end])
    sum(x[[1, 2, 3, 6, 7, 8]]) / ccall(:sysconf, Clong, (Cint,), 2)
end
function normalized(url, graph)
    uri = HTTP.URI(url)
    o, d, b, e, w, s, m, l, dm, wm = R.parse_query(uri, graph)
    p = R._query_params(uri)
    pop = m == "accessible_population"
    r = pop ? R._origin_radius(get(p, "origin_radius", "0")) : 0
    ex = pop ? R._exclude_origin_population(p) : false
    step = w > 0 ? min(s, w) : 0
    samples = w > 0 ? cld(w, step) : 1
    Dict("network"=>get(p, "network", "everything"), "index"=>string(o; base=16),
        "resolution"=>Int(H3.API.getResolution(o)), "departureMs"=>Int(d), "budgetMs"=>b,
        "encoding"=>e, "windowMs"=>w, "stepMs"=>step, "samples"=>samples,
        "spanMs"=>(samples-1)*step, "metric"=>m, "maxWalkMs"=>l,
        "distanceMode"=>pop ? "itinerary" : string(dm), "windowMode"=>string(wm),
        "originRadius"=>r, "excludeOriginPopulation"=>ex)
end
function compute(url, graph, index, population, prepared, pool; body_only=false)
    uri = HTTP.URI(url)
    o, d, b, e, w, s, m, l, dm, wm = R.parse_query(uri, graph)
    if m == "accessible_population"
        p = R._query_params(uri)
        radius = R._origin_radius(get(p, "origin_radius", "0"))
        count = Base.count(!iszero, H3.API.gridDisk(o, radius))
        samples = w == 0 ? 1 : cld(w, min(s, w))
        tile = min(count, samples == 1 || (samples > 4 && count >= 128Threads.nthreads(:default)) ? 64 : 16)
        workers = count <= 16 && samples <= 4 && b <= 10_800_000 && l <= 3_600_000 ? 1 : min(3, cld(count, tile))
        a = R._route_population(graph, population, o, d, b; walking_index=index,
            prepared_population=prepared, workspace_pool=pool, result_cache=nothing,
            origin_radius=radius, window_ms=w, step_ms=s, max_walk_ms=l, window_mode=wm,
            exclude_origin_population=R._exclude_origin_population(p), workers)
        ids = findall(>(0), a.value)
        body = R.arrow_table(a.h3[ids], (value=a.value[ids],), e)
        body_only && return body
        return (; bytes=length(body), origins=length(a.h3), workers=a.workers,
            reused=get(a, :workspace_reused_workers, 0))
    end
    a = R._route_request(graph, index, o, d, b, w, s, l, dm, wm; workers=w > 0 ? 3 : 1)
    body = w > 0 ? R.window_arrow(graph, a, o, e; metric=m, window_mode=wm) :
        R.arrow_result(graph, a.arrival, o, d, e; distance_km=a.distance_km, metric=m, h3=a.h3)
    body_only && return body
    (; bytes=length(body), origins=1, workers=get(a, :workers, 1), reused=0)
end
function cases(network, resolution)
    rows = []
    for (ci, (city, lat, lon, fold)) in enumerate(CITIES), family in 0:3, variant in 0:1
        origin = H3.API.latLngToCell(H3.API.LatLng(deg2rad(lat), deg2rad(lon)), resolution)
        pop, window = family >= 2, isodd(family)
        budget = (0.0, 0.25, 0.5, 1.0, 3.0, 6.0)[mod1(ci + 2variant + family, 6)]
        walk = (0.0, 0.1, 0.5, 1.0)[mod1(ci + variant + family, 4)]
        samples = window ? (2, 4, 16, 96, 217, 16)[mod1(ci + variant + family, 6)] : 1
        radius = pop ? (0, 1, 2, 5, 10, 18)[mod1(ci + 2variant + family, 6)] : 0
        mode = MODES[mod1(ci + variant, 6)]
        metric = pop ? "accessible_population" : variant == 1 && mode != "reachable_union" ? "time_distance_quantile" : "time"
        step = samples == 217 ? 0.06 : 1/60
        url = "/reachable?network=$network&index=$(string(origin; base=16))&departure_h=$(variant == 0 ? 8 : 17)&budget_h=$budget&max_walk_h=$walk&metric=$metric&encoding=$(variant == 0 ? "split" : "string")&distance_mode=$(variant == 0 ? "itinerary" : "straight_line")"
        pop && (url *= "&origin_radius=$radius&exclude_origin_population=$(variant == 1)")
        window && (url *= "&window_h=$(samples*step)&step_h=$step&window_mode=$mode")
        push!(rows, (; city, fold, family, variant, url))
    end
    rows
end
function longcases(network, resolution)
    rows = []
    designs = [(100, false, 0, 1, 0.1), (100, true, 2, 4, 1.0), (100, true, 6, 1, 0.1),
        (168, false, 0, 4, 0.1), (168, true, 2, 96, 0.1)]
    network == "everything" && resolution == 8 && push!(designs, (100, true, 18, 1, 1.0))
    for (variant, (budget, pop, radius, samples, walk)) in enumerate(designs)
        holdout = budget == 168
        lat, lon = holdout ? (53.5511, 9.9937) : (48.8566, 2.3522)
        origin = H3.API.latLngToCell(H3.API.LatLng(deg2rad(lat), deg2rad(lon)), resolution)
        metric = pop ? "accessible_population" : "time"
        url = "/reachable?network=$network&index=$(string(origin; base=16))&departure_h=5&budget_h=$budget&max_walk_h=$walk&metric=$metric&origin_radius=$radius"
        samples > 1 && (url *= "&window_h=$(samples/60)&step_h=$(1/60)")
        push!(rows, (; city=holdout ? "Hamburg" : "Paris", fold=holdout ? "long_test" : "long_train",
            family=2Int(pop)+Int(samples>1), variant, url))
    end
    rows
end
function main()
    Threads.nthreads(:default) == 8 || error("Calibration requires --threads=8 for the measured scheduler policy")
    network = length(ARGS) > 0 ? ARGS[1] : "rail_and_friends"
    network in ("everything", "rail_and_friends") || error("Unknown calibration network")
    maxseconds = length(ARGS) > 1 ? parse(Float64, ARGS[2]) : 2700.0
    long = "--long" in ARGS
    label = network * (long ? "-long" : "")
    resolutions = long ? (6, 8) : (5, 6, 7, 8)
    started = time()
    available = 1024parse(Int, match(r"MemAvailable:\s+(\d+)", read("/proc/meminfo", String))[1])
    available >= 30*1024^3 || error("At least 30 GiB MemAvailable is required before graph load")
    source = joinpath(ROOT, "data", network*"_res8.arrow")
    hashes = Dict(basename(p)=>bytes2hex(sha256(read(p))) for p in
        sort(filter(p->endswith(p, ".jl"), readdir(joinpath(ROOT, "router/src"); join=true))))
    meta = Dict{String,Any}("network"=>network, "started"=>string(now(UTC)), "julia"=>string(VERSION),
        "threads"=>Threads.nthreads(:default), "cpu"=>Sys.cpu_info()[1].model,
        "source"=>basename(source), "sourceBytes"=>filesize(source), "sourceMtime"=>stat(source).mtime,
        "populationBytes"=>filesize(joinpath(ROOT, "data/kontur_h3.arrow")), "routerSha256"=>hashes,
        "graphs"=>Dict{String,Any}())
    println("LOAD $network available_GiB=$(available/1024^3)"); flush(stdout)
    g8 = R.pack_graph(source; skip_invalid_durations=true, badajoz_shuttle=true, progress=true)
    graphs = Dict(8=>g8)
    for res in 7:-1:minimum(resolutions)
        graphs[res] = R.coarsen_graph(graphs[res+1], res; progress=true)
    end
    population = R.load_population(joinpath(ROOT, "data/kontur_h3.arrow"); progress=true)
    open(joinpath(@__DIR__, "$label.jsonl"), "w") do out
        for resolution in resolutions
            time()-started < maxseconds || break
            graph = graphs[resolution]
            println("PREPARE $network $resolution rss_GiB=$(rss()/1024^3)"); flush(stdout)
            index = R.prepare_walking(R.WalkingIndex(graph); progress=true)
            prepared = R._prepare_population(population, index; progress=true)
            R._population_schedule_hints(population, graph)
            pool = R.PopulationWorkspacePool()
            meta["graphs"][string(resolution)] = (; nodes=length(graph.h3), profiles=length(graph.departure))
            open(io->JSON.print(io, meta), joinpath(@__DIR__, "$label-metadata.json"), "w")
            for c in (long ? longcases(network, resolution) : cases(network, resolution))
                time()-started < maxseconds || break
                n = normalized(c.url, graph)
                # Each URL is warmed without a result cache. Timed repeats must have no compilation.
                compute(c.url, graph, index, population, prepared, pool)
                for attempt in 1:3
                    beforehost, beforecpu = hostseconds(), cpu()
                    t = @timed compute(c.url, graph, index, population, prepared, pool)
                    elapsedcpu = cpu()-beforecpu
                    external = max(0, hostseconds()-beforehost-elapsedcpu)/t.time
                    if t.compile_time > 0
                        attempt == 3 && error("Compilation persists for $(c.url)")
                        continue
                    end
                    row = merge(c, (; normalized=n, cpuMs=1000elapsedcpu, wallMs=1000t.time,
                        gcMs=1000t.gctime, allocatedBytes=t.bytes, compileMs=1000t.compile_time,
                        rssBytes=rss(), externalCores=external, timestamp=string(now(UTC))), t.value)
                    JSON.print(out, row); println(out); flush(out)
                    println("MEASURE $network $resolution $(c.city) $(c.family)/$(c.variant) CPU_ms=$(round(1000elapsedcpu; digits=2)) reused=$(t.value.reused)"); flush(stdout)
                    break
                end
            end
            empty!(pool)
            empty!(population.prepared)
            empty!(population.schedule_hints)
            index = prepared = pool = nothing
            GC.gc()
        end
    end
    meta["finished"] = string(now(UTC))
    meta["elapsedSeconds"] = time()-started
    open(io->JSON.print(io, meta), joinpath(@__DIR__, "$label-metadata.json"), "w")
end
if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
