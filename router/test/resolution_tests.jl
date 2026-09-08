import Sockets

@testset "Resident resolution dispatch" begin
    cell_at(lat, lon, res) = H3.API.latLngToCell(H3.API.LatLng(deg2rad(lat), deg2rad(lon)), res)
    tables = map(5:7) do res
        cells = [cell_at(51.5, lon, res) for lon in (-0.1, 0.1)]
        (from_h3=fill(cells[1], 2), to_h3=fill(cells[2], 2),
         departure_ms=UInt32[0, 60_000], duration_ms=fill(Int64(res * 60_000), 2),
         distance_km=fill(Float64(res * 2), 2))
    end
    graphs = pack_graph.(tables)
    paths = ["/reachable?index=$(H3.API.h3ToString(first(t.from_h3)))&departure_h=0&budget_h=1&max_walk_h=0&encoding=string" for t in tables]
    missing = "/reachable?index=$(H3.API.h3ToString(cell_at(51.5, -0.1, 8)))&departure_h=0&budget_h=1"
    request_lock = ReentrantLock()
    handlers = Dict(g.resolution => make_handler(g; request_lock) for g in graphs)
    handler = make_resolution_handler(handlers)
    empty!(handlers) # the dispatch table is a private snapshot
    @test_throws ArgumentError make_resolution_handler(Dict{Int,Any}())
    tasks = Task[]
    lock(request_lock)
    try
        for path in paths
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
    @test occursin("no graph loaded for H3 resolution 8", String(response.body))
    @test HTTP.header(response, "Access-Control-Allow-Origin") == "*"
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
        files = [joinpath(dir, "graph-$res.arrow") for res in 5:7]
        for (file, table) in zip(files, tables)
            Arrow.write(file, table)
        end
        let
            @test_throws r"duplicate graph for H3 resolution 5" load_handlers([files[1], files[1]])
            other = joinpath(dir, "other.arrow")
            Arrow.write(other, tables[1])
            @test_throws r"duplicate graph for H3 resolution 5" load_handlers([files[1], other])
        end
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
            end, 120; pollint=0.1) == :ok
            startup_log = read(joinpath(dir, "server.log"), String)
            @test !occursin('\e', startup_log)
            for stage in ("Opening Arrow file", "Validating and filtering rows", "Indexing and validating H3 endpoints",
                          "Sorting connections by edge", "Packing daily profiles",
                          "Enumerating walking geometry", "Packing walking adjacency", "Indexing walking output IDs")
                @test occursin("Startup: $stage", startup_log)
                @test occursin("Startup complete: $stage", startup_log)
            end
            @test occursin("elapsed_s", startup_log)
            socket_open("ws://127.0.0.1:$port/query") do ws
                id = UInt32(0xfedcba98)
                for (g, path) in zip(graphs, paths), mode in ("itinerary", "straight_line"), window in ("0", "0.03333333333333333"), walk in ("0", "0.001")
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
            end
            for res in 5:7
                shuttle = Reachability._badajoz_shuttle(res)
                origin, destination = first(shuttle.from_h3), first(shuttle.to_h3)
                query = "/reachable?index=$(H3.API.h3ToString(origin))&departure_h=8&budget_h=0.25&max_walk_h=0&encoding=string"
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
