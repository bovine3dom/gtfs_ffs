include("../../router/serve.jl")
using H3, Statistics

function csvrow(io, values)
    println(io, join(values, ','))
    flush(io)
end

function positive_env(name, default)
    text = get(ENV, name, string(default))
    value = tryparse(Int, text)
    !isnothing(value) && value > 0 || error("$name must be a positive integer")
    return value
end

function radii_env()
    text = get(ENV, "TRIP_POPULATION_RADII", "0,1,3,6,10")
    values = tryparse.(Int, split(text, ','))
    all(x -> !isnothing(x) && x >= 0, values) ||
        error("TRIP_POPULATION_RADII must be comma-separated nonnegative integers")
    radii = Int[x for x in values]
    !isempty(radii) && length(unique(radii)) == length(radii) ||
        error("TRIP_POPULATION_RADII must contain unique radii")
    return radii
end

function request_path(network, origin, radius, departure_ms)
    departure_h = departure_ms / 3_600_000
    return "/reachable?network=$network&index=$(string(origin; base=16))" *
        "&departure_h=$departure_h&budget_h=3&max_walk_h=1" *
        "&metric=accessible_population&origin_radius=$radius" *
        "&window_h=1.6&step_h=$(1 / 60)&trip_aware=true"
end

function run_request(handler, network, origin, radius, departure_ms, expected_origins)
    response = handler(HTTP.Request("GET", request_path(network, origin, radius, departure_ms)))
    response.status == 200 || error("radius=$radius returned HTTP $(response.status): $(String(response.body))")
    HTTP.header(response, "X-Router-Trip-Aware") == "true" ||
        error("radius=$radius did not use trip-aware routing")
    origin_count = parse(Int, HTTP.header(response, "X-Router-Origin-Count"))
    origin_count == expected_origins ||
        error("radius=$radius returned $origin_count origins; expected $expected_origins")
    return response
end

function main(args)
    length(args) == 4 || error("Use: trip-population-benchmark.jl GRAPH.arrow POPULATION.arrow TRIP_SHARDS_DIR OUTPUT.csv")
    graph_path, population_path, trip_shards_path, output_path = abspath.(args)
    all(isfile, (graph_path, population_path)) || error("graph and population files must exist")
    isdir(trip_shards_path) || error("trip shard directory does not exist: $trip_shards_path")
    matched = match(r"^(.+)_res([0-9]+)\.arrow$", basename(graph_path))
    isnothing(matched) && error("graph filename must use the form [name]_res[N].arrow")
    network = matched[1]
    resolution = parse(Int, matched[2])
    0 <= resolution <= 8 || error("population benchmark requires graph resolution 0 through 8")

    radii = radii_env()
    rounds = positive_env("TRIP_POPULATION_ROUNDS", 3)
    threads = Threads.nthreads(:default)
    workspace_gib = positive_env("TRIP_POPULATION_WORKSPACE_GIB", 8)
    workspace_bytes = workspace_gib * 1024^3
    short_workers = positive_env("TRIP_POPULATION_SHORT_WORKERS", cld(threads, 4))
    bulk_workers = max(1, threads - short_workers)
    max_workers_per_request = positive_env("TRIP_POPULATION_MAX_WORKERS_PER_REQUEST", cld(bulk_workers, 2))
    println("LOAD threads=$threads graph=$graph_path population=$population_path trip_shards=$trip_shards_path")
    println("CONFIG network=$network resolution=$resolution radii=$(join(radii, ',')) rounds=$rounds workspace_gib=$workspace_gib short_workers=$short_workers max_workers_per_request=$max_workers_per_request window_h=1.6 step_min=1 budget_h=3 max_walk_h=1")
    flush(stdout)
    handler = load_handlers([graph_path]; population_path, trip_shards_path, workspace_bytes,
        short_workers, max_workers_per_request)
    origin = H3.API.latLngToCell(H3.API.LatLng(deg2rad(48.8566), deg2rad(2.3522)), resolution)::UInt64
    sequence = 0
    mkpath(dirname(output_path))
    open(output_path, "w") do io
        csvrow(io, ("radius", "kind", "round", "origins", "cache_hits", "cache_misses", "workers",
            "shared_expansions", "query_expansions", "wall_s", "allocated_bytes", "gc_s", "rss_bytes",
            "response_bytes"))
        for radius in radii
            origins = H3.API.gridDisk(origin, radius)
            filter!(!iszero, origins)
            expected_origins = length(origins)

            sequence += 1
            departure_ms = 28_800_000 + sequence * 1_000
            warm = @timed run_request(handler, network, origin, radius, departure_ms, expected_origins)
            warm_response = warm.value
            row = (radius, "warmup", 0,
                parse(Int, HTTP.header(warm_response, "X-Router-Origin-Count")),
                HTTP.header(warm_response, "X-Router-Cache-Hits"),
                HTTP.header(warm_response, "X-Router-Cache-Misses"),
                HTTP.header(warm_response, "X-Router-Workers"),
                HTTP.header(warm_response, "X-Router-Shared-Expansions"),
                HTTP.header(warm_response, "X-Router-Query-Expansions"), warm.time, warm.bytes,
                warm.gctime, Sys.maxrss(), length(warm_response.body))
            csvrow(io, row)
            println("WARMUP radius=$radius origins=$expected_origins wall_s=$(warm.time) allocated_bytes=$(warm.bytes) workers=$(row[7]) cache_misses=$(row[6])")
            flush(stdout)

            times = Float64[]
            for trial in 1:rounds
                GC.gc()
                sequence += 1
                departure_ms = 28_800_000 + sequence * 1_000
                measured = @timed run_request(handler, network, origin, radius, departure_ms, expected_origins)
                response = measured.value
                push!(times, measured.time)
                csvrow(io, (radius, "measured", trial,
                    parse(Int, HTTP.header(response, "X-Router-Origin-Count")),
                    HTTP.header(response, "X-Router-Cache-Hits"),
                    HTTP.header(response, "X-Router-Cache-Misses"),
                    HTTP.header(response, "X-Router-Workers"),
                    HTTP.header(response, "X-Router-Shared-Expansions"),
                    HTTP.header(response, "X-Router-Query-Expansions"), measured.time,
                    measured.bytes, measured.gctime, Sys.maxrss(), length(response.body)))
                workers = HTTP.header(response, "X-Router-Workers")
                misses = HTTP.header(response, "X-Router-Cache-Misses")
                println("RUN radius=$radius trial=$trial origins=$expected_origins wall_s=$(measured.time) allocated_bytes=$(measured.bytes) gc_s=$(measured.gctime) workers=$workers cache_misses=$misses")
                flush(stdout)
            end
            println("SUMMARY radius=$radius origins=$expected_origins median_s=$(median(times))")
            flush(stdout)
        end
    end
    println("COMPLETE output=$output_path peak_rss_bytes=$(Sys.maxrss())")
end

main(ARGS)
