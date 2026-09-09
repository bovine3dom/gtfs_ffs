import Sockets

@testset "Resident network and resolution dispatch" begin
    cell_at(lat, lon, res) = H3.API.latLngToCell(H3.API.LatLng(deg2rad(lat), deg2rad(lon)), res)
    tables = map(5:7) do res
        cells = [cell_at(51.5, lon, res) for lon in (-0.1, 0.1)]
        (from_h3=fill(cells[1], 2), to_h3=fill(cells[2], 2),
         departure_ms=UInt32[0, 60_000], duration_ms=fill(Int64(res * 60_000), 2),
         distance_km=fill(Float64(res * 2), 2))
    end
    graphs = pack_graph.(tables)
    other_table = merge(tables[1], (departure_ms=UInt32[30_000, 90_000],
        duration_ms=fill(Int64(12 * 60_000), 2), distance_km=fill(31.0, 2)))
    other_graph = pack_graph(other_table)
    paths = ["/reachable?index=$(H3.API.h3ToString(first(t.from_h3)))&departure_h=0&budget_h=1&max_walk_h=0&encoding=string" for t in tables]
    paths[3] *= "&network=everything"
    missing = "/reachable?index=$(H3.API.h3ToString(cell_at(51.5, -0.1, 8)))&departure_h=0&budget_h=1"
    request_lock = ReentrantLock()
    handlers = Dict((g.resolution == 7 ? "everything" : "rail_and_friends", g.resolution) => make_handler(g; request_lock) for g in graphs)
    handlers[("everything", 5)] = make_handler(other_graph; request_lock)
    handler = make_network_handler(handlers; default_network="rail_and_friends")
    @test_throws ArgumentError make_network_handler(handlers; default_network="absent")
    empty!(handlers) # the dispatch table is a private snapshot
    @test_throws ArgumentError make_network_handler(Dict{Tuple{String,Int},Any}(); default_network="test")
    tasks = Task[]
    lock(request_lock)
    try
        for path in [paths; paths[1] * "&network=everything"]
            push!(tasks, Threads.@spawn handler(HTTP.Request("GET", path)))
        end
        sleep(0.1)
        @test all(!istaskdone(t) for t in tasks)
    finally
        unlock(request_lock)
    end
    @test all(fetch(t).status == 200 for t in tasks)
    for (i, path) in enumerate(paths)
        result = Arrow.Table(handler(HTTP.Request("GET", path)).body)
        @test sort(collect(result.elapsed_h)) == [0, (i + 4) / 60]
        @test sort(collect(result.distance_km)) == [0, (i + 4) * 2]
        @test handler(HTTP.Request("GET", path)).body == make_handler(graphs[i])(HTTP.Request("GET", path)).body
    end
    response = handler(HTTP.Request("GET", missing))
    @test response.status == 400
    @test occursin("no graph loaded for H3 resolution 8", String(copy(response.body)))
    @test HTTP.header(response, "Access-Control-Allow-Origin") == "*"
    @test occursin("rail_and_friends", String(copy(response.body)))
    for (path, message) in ((paths[1] * "&network=unknown", "unknown network"),
                            (paths[2] * "&network=everything", "resolution 6 in network \"everything\""),
                            (replace(paths[3], "&network=everything" => ""), "resolution 7 in network \"rail_and_friends\""))
        response = handler(HTTP.Request("GET", path))
        @test response.status == 400
        @test occursin(message, String(response.body))
    end
    for network in ("", "%ZZ", "everything&network=rail_and_friends")
        @test handler(HTTP.Request("GET", paths[1] * "&network=$network")).status == 400
    end
    named = make_network_handler(Dict(("rail-\u00e9t\u00e9_res2 friends", 5) => make_handler(graphs[1])); default_network="rail-\u00e9t\u00e9_res2 friends")
    @test named(HTTP.Request("GET", paths[1] * "&network=rail-%C3%A9t%C3%A9_res2%20friends")).body == handler(HTTP.Request("GET", paths[1])).body
    @test make_handler(graphs[1])(HTTP.Request("GET", paths[1] * "&network=ignored")).status == 200
    for query in ("", "index=000000000000000", "index=85075dd7fffffff&index=85075dd7fffffff",
                  "index=85075dd7fffffff&index_lower=0", "index_lower=0", "index_lower=0&index_upper=0",
                  "index=85075dd7fffffff", "index=85075dd7fffffff&departure_ms=0&budget_h=1")
        @test handler(HTTP.Request("GET", "/reachable?$query")).status == 400
    end
    for (method, path, status) in (("OPTIONS", "/reachable", 204), ("GET", "/missing", 404), ("POST", "/reachable", 405))
        @test handler(HTTP.Request(method, path)).status == status
    end
    @test_throws r"usage:" load_handlers(String[])
    @test_throws r"usage:" load_handlers(["--demo", "nonexistent.arrow"])
    @test_throws r"usage:" load_handlers(["--demo", "--demo"])
    @test load_handlers(["--demo"])(HTTP.Request("GET", "/reachable?index=85075dd7fffffff&departure_h=8&budget_h=1&max_walk_h=0")).status == 200
    mktempdir() do dir
        dir = mkpath(joinpath(dir, "paths with spaces"))
        files = [joinpath(dir, "$(res == 7 ? "everything" : "rail_and_friends")_res$res.arrow") for res in 5:7]
        for (file, table) in zip(files, tables)
            Arrow.write(file, table)
        end
        let
            @test_throws r"duplicate graph for network.*H3 resolution 5" load_handlers([files[1], files[1]])
            # Preflight must fail before trying to open even the first nonexistent file.
            absent = joinpath(dir, "absent", "rail_res6.arrow")
            @test_throws r"duplicate graph for network" load_handlers([absent, joinpath(dir, "rail_res06.arrow")])
            for bad in ("arbitrary.arrow", "_res6.arrow", "rail_res-1.arrow", "rail_res16.arrow",
                        "rail_res999999999999999999999999.arrow", "rail_res6.ARROW", "rail_res6.arrow.bak")
                @test_throws r"filename" load_handlers([absent, joinpath(dir, bad)])
            end
            mismatch = joinpath(dir, "wrong_res6.arrow")
            Arrow.write(mismatch, tables[1])
            @test_throws r"filename H3 resolution 6 does not match graph resolution 5" load_handlers([mismatch])
            empty_file = joinpath(dir, "empty_res7.arrow")
            Arrow.write(empty_file, map(v -> v[1:0], tables[1]))
            @test_throws r"filename H3 resolution 7 does not match graph resolution 5" load_handlers([empty_file])
            leading_zero = joinpath(dir, "rail-\u00e9t\u00e9_res2 friends_res06.arrow")
            Arrow.write(leading_zero, tables[2])
            @test load_handlers([leading_zero])(HTTP.Request("GET", paths[2] * "&network=rail-%C3%A9t%C3%A9_res2%20friends")).status == 200
        end
        push!(files, joinpath(dir, "everything_res5.arrow"))
        Arrow.write(last(files), other_table)
        reversed = load_handlers(reverse(files))
        @test reversed(HTTP.Request("GET", paths[2])).status == 400
        @test reversed(HTTP.Request("GET", paths[2] * "&network=rail_and_friends")).status == 200
        @test reversed(HTTP.Request("GET", paths[1])).body == make_handler(other_graph)(HTTP.Request("GET", paths[1])).body
        @test reversed(HTTP.Request("GET", paths[1])).body != handler(HTTP.Request("GET", paths[1])).body
        # Own child process only: no existing server or real datasets are touched.
        listener = Sockets.listen(Sockets.ip"127.0.0.1", 0)
        port = Sockets.getsockname(listener)[2]
        close(listener)
        command = `$(Base.julia_cmd()) --threads=4 --project=$(dirname(@__DIR__)) $(joinpath(dirname(@__DIR__), "serve.jl")) $files`
        log = open(joinpath(dir, "server.log"), "w+")
        process = run(pipeline(addenv(command, "ROUTER_PORT" => string(port), "ROUTER_HOST" => "127.0.0.1"), stdout=log, stderr=log); wait=false)
        http = "http://127.0.0.1:$port"
        try
            @test timedwait(() -> begin
                process_exited(process) && error("router child exited: $(read(seekstart(log), String))")
                try
                    HTTP.get(http * "/missing"; status_exception=false, connect_timeout=1).status == 404
                catch
                    false
                end
            end, 600; pollint=0.1) == :ok
            startup_log = read(joinpath(dir, "server.log"), String)
            @test !occursin('\e', startup_log)
            for stage in ("Opening Arrow file", "Validating and filtering rows", "Indexing and validating H3 endpoints",
                          "Sorting connections by edge", "Packing daily profiles",
                          "Enumerating walking geometry", "Packing walking adjacency", "Indexing walking output IDs")
                @test occursin("Startup: $stage", startup_log)
                @test occursin("Startup complete: $stage", startup_log)
            end
            @test occursin("elapsed_s", startup_log)
            @test occursin("default_network = \"rail_and_friends\"", startup_log)
            @test occursin("Available network", startup_log)
            socket_open("ws://127.0.0.1:$port/query") do ws
                id = UInt32(0xfedcba98)
                for (g, path) in zip([graphs; other_graph], [paths; paths[1] * "&network=everything"]), mode in ("itinerary", "straight_line"), window in ("0", "0.03333333333333333"), walk in ("0", "0.001")
                    query = replace(path, "max_walk_h=0" => "max_walk_h=$walk") * "&distance_mode=$mode&window_h=$window"
                    expected = make_handler(g)(HTTP.Request("GET", query))
                    response = HTTP.get(http * query)
                    @test response.body == expected.body
                    socket_query(ws, id, query)
                    bytes = socket_receive(ws)
                    @test socket_id(bytes) == id
                    @test bytes[5:end] == response.body
                    id += 1
                    h = first(tables[g.resolution - 4].from_h3)
                    split = replace(query, "index=$(H3.API.h3ToString(h))" => "index_lower=$(h % UInt32)&index_upper=$((h >> 32) % UInt32)")
                    @test HTTP.get(http * split).body == response.body
                end
                @test HTTP.get(http * missing; status_exception=false).status == 400
                socket_query(ws, id, missing)
                @test JSON.parse(socket_receive(ws))["type"] == "error"
                socket_query(ws, id + 1, first(paths))
                @test socket_id(socket_receive(ws)) == id + 1
                id += 2
                for query in (paths[1], paths[1] * "&network=everything", paths[1] * "&network=unknown", paths[1])
                    response = HTTP.get(http * query; status_exception=false)
                    socket_query(ws, id, query)
                    bytes = socket_receive(ws)
                    if response.status == 400
                        @test JSON.parse(bytes)["type"] == "error"
                    else
                        @test socket_id(bytes) == id
                        @test bytes[5:end] == response.body
                    end
                    id += 1
                end
                for network in ("rail_and_friends", "everything"), mode in ("itinerary", "straight_line"), window_mode in ("min_union", "diff_intersection"), metric in ("time", "time_distance_quantile")
                    query = paths[1] * "&network=$network&distance_mode=$mode&window_mode=$window_mode&window_h=0.03333333333333333&metric=$metric"
                    graph = network == "everything" ? other_graph : graphs[1]
                    response = HTTP.get(http * query)
                    @test response.body == make_handler(graph)(HTTP.Request("GET", query)).body
                    socket_query(ws, id, query)
                    bytes = socket_receive(ws)
                    @test socket_id(bytes) == id
                    @test bytes[5:end] == response.body
                    id += 1
                end
            end
            for (network, res) in (("rail_and_friends", 5), ("rail_and_friends", 6), ("everything", 5), ("everything", 7))
                shuttle = Reachability._badajoz_shuttle(res)
                origin, destination = first(shuttle.from_h3), first(shuttle.to_h3)
                query = "/reachable?index=$(H3.API.h3ToString(origin))&departure_h=8&budget_h=0.25&max_walk_h=0&encoding=string&network=$network"
                result = Arrow.Table(HTTP.get(http * query).body)
                at = findfirst(==(H3.API.h3ToString(destination)), result.index)
                @test !isnothing(at)
                @test result.elapsed_h[at] == 0.25
                @test result.distance_km[at] == 13.88
            end
        finally
            process_exited(process) || kill(process)
            wait(process)
            close(log)
        end
    end
end
