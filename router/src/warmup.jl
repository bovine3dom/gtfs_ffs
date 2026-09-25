export warmup_server

function _warmup_table()
    origin = H3.API.latLngToCell(H3.API.LatLng(deg2rad(51.5), deg2rad(-0.1)), 8)
    cells = H3.API.gridDisk(origin, 1)
    push!(cells, first(setdiff(H3.API.gridDisk(origin, 2), cells)))
    departures = UInt32.(0:60_000:128 * 60_000)
    rows = [(a, b, d) for (a, b) in zip(cells, circshift(cells, 1)) for d in departures]
    return (from_h3=first.(rows), to_h3=getindex.(rows, 2), departure_ms=last.(rows),
            duration_ms=fill(Int64(60_000), length(rows)), distance_km=fill(1.0, length(rows)))
end

function _warmup_population(table)
    cells = unique(table.from_h3)
    middle = length(cells) ÷ 2
    # Match the chained Arrow columns in the production population file.
    mktempdir() do dir
        path = joinpath(dir, "population.arrow")
        Arrow.write(path, (h3=cells[1:middle], population=ones(middle)); file=false)
        Arrow.append(path, (h3=cells[middle+1:end], population=ones(length(cells) - middle)))
        data = Arrow.Table(read(path))
        _population(data.h3, data.population)
    end
end

"""Compile request and transport paths on synthetic data before opening the public listener."""
function warmup_server(; progress::Bool=true)
    table = _warmup_table()
    origin = first(table.from_h3)
    offgraph = first(setdiff(H3.API.gridDisk(origin, 3), table.from_h3))
    jobs = Tuple{Any,String}[]
    population = _warmup_population(table)
    for distances in (true, false)
        graph = pack_graph(distances ? table : Base.structdiff(table, NamedTuple{(:distance_km,)}))
        handler = make_network_handler(Dict(("warmup", 8) => make_handler(graph; population)); default_network="warmup")
        for walk in (0, 0.25), window in (0, 0.05, 0.1), encoding in ("split", "string"),
                mode in (window == 0 ? ("mean_intersection",) : ("mean_intersection", "min_union", "reachable_union"))
            push!(jobs, (handler, "/reachable?index=$(string(origin; base=16))&departure_h=0&budget_h=0.5&max_walk_h=$walk&window_h=$window&window_mode=$mode&encoding=$encoding&metric=accessible_population&origin_radius=5"))
        end
        modes = distances ? ("point", "mean_intersection", "min_union", "max_intersection", "diff_union", "diff_intersection", "reachable_union") :
                            ("point", "mean_intersection")
        for mode in modes, distance in ("itinerary", "straight_line"), walk in (0, 0.25, 2),
                encoding in ("split", "string"),
                metric in (distances && mode != "reachable_union" ? ("time", "time_distance_quantile") : ("time",))
            index = encoding == "string" ? "index=$(string(origin; base=16))" :
                "index_lower=$(origin % UInt32)&index_upper=$((origin >> 32) % UInt32)"
            # Transit needs more than 64 distinct groups to start parallel chunks.
            samples = walk == 0 ? 129 : 16
            window = mode == "point" ? "window_h=0" : "window_h=$(samples/60)&step_h=$(1/60)&window_mode=$mode"
            # The larger walking limit exceeds the resident one-hour adjacency,
            # exercising fallback geography while the tiny graph bounds the work.
            push!(jobs, (handler, "/reachable?$index&departure_h=0&budget_h=1.1&max_walk_h=$walk&distance_mode=$distance&encoding=$encoding&metric=$metric&$window"))
        end
        for distance in ("itinerary", "straight_line"), window in (0, 16/60)
            push!(jobs, (handler, "/reachable?index=$(string(offgraph; base=16))&departure_h=0&budget_h=0.25&max_walk_h=0.25&distance_mode=$distance&window_h=$window&step_h=$(1/60)"))
        end
    end
    trip_graph = pack_graph(merge(table, (trip_id=fill(UInt32(1), length(table.from_h3)),)))
    bulk = max(1, Threads.nthreads(:default) - cld(Threads.nthreads(:default), 4))
    trip_admission = RequestAdmission(; max_workers_per_request=min(2, bulk))
    trip_handler = make_handler(trip_graph; population, admission=trip_admission)
    for metric in ("accessible_population", "time_distance_quantile"), window in (0, 0.1),
            walk in (0, 0.25), mode in ("mean_intersection", "min_union")
        push!(jobs, (trip_handler, "/reachable?index=$(string(origin; base=16))&departure_h=0&budget_h=0.5" *
            "&max_walk_h=$walk&window_h=$window&step_h=$(1/60)&window_mode=$mode" *
            "&encoding=split&distance_mode=straight_line&metric=$metric&origin_radius=5&trip_aware=true"))
    end
    return _startup_stage(progress, "Compiling routing and Arrow responses"; total=length(jobs) + 2) do meter
        progress && @info "Synthetic routing warmup" queries=length(jobs) + 2
        for (handler, url) in jobs
            response = handler(HTTP.Request("GET", url))
            response.status == 200 || error("synthetic warmup failed: $(String(response.body))")
            _startup_advance(meter, 1)
        end
        handler, url = first(jobs)
        server = HTTP.serve!(make_stream_handler(handler), "127.0.0.1", 0;
                             stream=true, listenany=true, verbose=-1)
        try
            port = HTTP.port(server)
            response = HTTP.get("http://127.0.0.1:$port$url"; closeimmediately=true, proxy=nothing)
            response.status == 200 || error("HTTP warmup failed")
            _startup_advance(meter, 1)
            HTTP.WebSockets.open("ws://127.0.0.1:$port/query"; proxy=nothing) do ws
                HTTP.closewrite(ws.io)
                HTTP.WebSockets.send(ws, JSON.json((type="query", id=1, url=url)))
                bytes = HTTP.WebSockets.receive(ws)
                bytes == vcat(UInt8[0, 0, 0, 1], response.body) || error("WebSocket warmup failed")
            end
            _startup_advance(meter, 1)
        finally
            close(server)
        end
        length(jobs) + 2
    end
end
