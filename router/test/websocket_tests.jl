import JSON

const WS = HTTP.WebSockets

function socket_open(f, url; kwargs...)
    WS.open(url; kwargs...) do ws
        # Finish the HTTP upgrade write before a fast WS close; HTTP 1.x otherwise
        # attempts its final flush after the socket has already closed.
        HTTP.closewrite(ws.io)
        f(ws)
    end
end

function socket_test(f, handler)
    server = HTTP.serve!(make_stream_handler(handler), "127.0.0.1", 0;
                         stream=true, listenany=true, verbose=-1)
    try
        f("ws://127.0.0.1:$(HTTP.port(server))/query", "http://127.0.0.1:$(HTTP.port(server))")
    finally
        close(server)
    end
end

function socket_receive(ws)
    task = @async WS.receive(ws)
    if timedwait(() -> istaskdone(task), 20) != :ok
        close(ws.io)
        error("WebSocket receive timed out")
    end
    fetch(task)
end

socket_query(ws, id, url) = WS.send(ws, JSON.json((type="query", id=id, url=url)))
socket_id(bytes) = foldl((a, b) -> (a << 8) | UInt32(b), bytes[1:4]; init=UInt32(0))

@testset "Population HTTP and WebSocket parity" begin
    population = Reachability._population(
        UInt64[first(H3.API.cellToChildren(DEMO_ORIGIN, 8))], [12.5])
    handler = make_handler(pack_graph(fixture_table()); population)
    socket_test(handler) do url, http
        socket_open(url) do ws
            id = 0
            for exclude in (false, true), encoding in ("split", "string"), window in ("window_h=0&window_mode=ignored",
                    "window_h=1&step_h=0&window_mode=ignored", "window_h=0.01&window_mode=reachable_union")
                path = "/reachable?index=$(string(DEMO_ORIGIN; base=16))&departure_h=0&budget_h=0&metric=accessible_population&origin_radius=1&encoding=$encoding&$window&exclude_origin_population=$exclude"
                socket_query(ws, id += 1, path)
                bytes = socket_receive(ws)
                @test socket_id(bytes) == id
                @test bytes[5:end] == HTTP.get(http * path).body
                @test sum(Arrow.Table(bytes[5:end]).value) == (exclude ? 0.0 : 12.5)
            end
        end
    end
end

@testset "Population positive totals and empty Arrow parity" begin
    seed = first(H3.API.cellToChildren(DEMO_ORIGIN, 8))
    origins = sort!(filter(!iszero, H3.API.gridDisk(seed, 1)))
    source = first(filter(!=(seed), origins))
    target = first(setdiff(H3.API.gridDisk(seed, 2), origins))
    graph = pack_graph((from_h3=[source], to_h3=[target],
        departure_ms=UInt32[0], duration_ms=Int64[0]))
    for weight in (0.0, 3.0)
        population = Reachability._population([target], [weight])
        handler = make_handler(graph; population)
        socket_test(handler) do url, http
            socket_open(url) do ws
                id = 0
                for encoding in ("string", "split"), radius in (0, 1), window in (0, 2),
                        mode in ("mean_intersection", "max_intersection", "diff_intersection",
                                 "min_union", "diff_union", "reachable_union")
                    path = "/reachable?index=$(string(seed; base=16))&departure_h=0&budget_h=0&max_walk_h=0&metric=accessible_population&origin_radius=$radius&encoding=$encoding&window_h=$(window / 3600)&step_h=$(1 / 3600)&window_mode=$mode"
                    response = HTTP.get(http * path)
                    socket_query(ws, id += 1, path)
                    bytes = socket_receive(ws)
                    @test socket_id(bytes) == id
                    @test bytes[5:end] == response.body
                    repeated = HTTP.get(http * path)
                    @test HTTP.header(repeated, "X-Router-Cache-Misses") == "0"
                    @test repeated.body == response.body
                    table = Arrow.Table(response.body)
                    positive = weight > 0 && radius == 1 && (window == 0 || endswith(mode, "_union"))
                    @test collect(table.value) == (positive ? [window > 0 && mode == "reachable_union" ? weight / 2 : weight] : Float64[])
                    @test eltype(table.value) == Float64
                    @test Set(propertynames(table)) == Set(encoding == "string" ? (:index, :value) : (:index_lower, :index_upper, :value))
                    if encoding == "string"
                        @test eltype(table.index) <: AbstractString
                        cells = parse.(UInt64, table.index; base=16)
                    else
                        @test eltype(table.index_lower) == eltype(table.index_upper) == UInt32
                        cells = UInt64.(table.index_lower) .| (UInt64.(table.index_upper) .<< 32)
                    end
                    @test cells == (positive ? [source] : UInt64[])
                    @test HTTP.header(response, "X-Router-Origin-Count") == string(radius == 0 ? 1 : length(origins))
                    internal = route_population(graph, population, seed, 0, 0;
                        origin_radius=radius, max_walk_ms=0, window_ms=window * 1000,
                        step_ms=1000, window_mode=Symbol(mode))
                    @test internal.h3 == (radius == 0 ? [seed] : origins)
                    @test count(>(0), internal.value) == length(table.value)
                end
            end
        end
    end
end

@testset "WebSocket metric-specific radius validation" begin
    population = Reachability._population(
        UInt64[first(H3.API.cellToChildren(DEMO_ORIGIN, 8))], [1.0])
    socket_test(make_handler(pack_graph(fixture_table()); population)) do url, http
        socket_open(url) do ws
            id = 0
            for metric in ("time", "time_distance_quantile", "accessible_population"), window in (0, 0.01)
                path = "/reachable?index=$(string(DEMO_ORIGIN; base=16))&departure_h=0&budget_h=0&max_walk_h=0&distance_mode=straight_line&metric=$metric&window_h=$window"
                expected = HTTP.get(http * path).body
                for parameter in ("origin_radius=-1", "origin_radius=", "origin_radius=not-a-number",
                                  "origin_radius=2147483648", "origin_radius=999999999999999999999",
                                  "origin_radius=0&origin_radius=0", "exclude_origin_population=",
                                  "exclude_origin_population=nonsense", "exclude_origin_population=True",
                                  "exclude_origin_population=2", "exclude_origin_population=true&exclude_origin_population=false")
                    query = path * "&" * parameter
                    response = HTTP.get(http * query; status_exception=false)
                    socket_query(ws, id += 1, query)
                    reply = socket_receive(ws)
                    if metric == "accessible_population" || occursin('&', parameter)
                        @test response.status == 400
                        error = JSON.parse(reply)
                        @test error["type"] == "error"
                        @test error["id"] == id
                    else
                        @test response.status == 200
                        @test socket_id(reply) == id
                        @test reply[5:end] == response.body == expected
                    end
                end
            end
        end
    end
end

@testset "WebSocket HTTP/Arrow parity and origins" begin
    graph = pack_graph(merge(fixture_table(), (distance_km=fill(2.0, 6),)))
    handler = make_handler(graph)
    socket_test(handler) do url, http
        for origin in ("http://localhost:8000", "https://maps.example.org", "null")
            socket_open(url; headers=["Origin" => origin]) do ws
                path = "/reachable?index=85075dd7fffffff&departure_h=8&budget_h=1&max_walk_h=0"
                socket_query(ws, 1, path)
                bytes = socket_receive(ws)
                @test socket_id(bytes) == 1
                @test bytes[5:end] == HTTP.get(http * path).body
            end
        end
    end
    socket_test(handler) do url, http
        @test HTTP.get("$http/query"; status_exception=false).status == 426
        @test HTTP.get("$http/missing"; status_exception=false).status == 404
        for origin in ("null", "http://localhost:8000.evil", "http://localhost:8001", "*")
            @test HTTP.get("$http/query", ["Origin" => origin]; status_exception=false).status == 426
        end
        socket_open(url; headers=["Origin" => "http://localhost:8000"]) do ws
            id = UInt32(0xfedcba98)
            for encoding in ("split", "string"), metric in ("time", "time_distance_quantile"),
                    mode in ("itinerary", "straight_line"), walk in (0, 3600), window in (0, 120),
                    cell in (DEMO_ORIGIN, DEMO_CELLS[7])
                path = "/reachable?index=$(H3.API.h3ToString(cell))&departure_h=8&budget_h=1&encoding=$encoding&metric=$metric&distance_mode=$mode&max_walk_h=$(walk / 3600)&window_h=$(window / 3600)"
                socket_query(ws, id, path)
                bytes = socket_receive(ws)
                @test bytes isa Vector{UInt8}
                @test socket_id(bytes) == id
                expected = HTTP.get(http * path).body
                @test bytes[5:end] == expected
                table = Arrow.Table(bytes[5:end])
                @test :value in propertynames(table)
                @test eltype(table.elapsed_h) == Float64
                metric == "time_distance_quantile" && @test table.value == table.time_quantile - table.distance_quantile
                @test all(!endswith(string(f), "_ms") for f in propertynames(table))
                window == 0 || @test all(==(1.0), table.reachable_fraction)
                id += 1
            end
        end
    end
end

@testset "WebSocket rank sign and removed metric" begin
    graph = pack_graph(distance_table([(1, 2, 0, 30, 10.0), (1, 2, 60, 0, 1.0),
                                      (1, 3, 0, 40, 2.0), (1, 3, 60, 20, 2.0)]))
    socket_test(make_handler(graph)) do url, http
        socket_open(url) do ws
            id = 0
            for encoding in ("string", "split"), window in ("window_h=0&window_mode=unknown", "window_h=1&step_h=0&window_mode=unknown", "window_h=$(61/3600)&step_h=$(60/3600)")
                index = encoding == "string" ? "index=$(string(DEMO_ORIGIN; base=16))" :
                    "index_lower=$(DEMO_ORIGIN % UInt32)&index_upper=$((DEMO_ORIGIN >> 32) % UInt32)"
                path = "/reachable?$index&departure_h=0&budget_h=$(100/3600)&max_walk_h=0&encoding=$encoding&$window"
                for metric in ("time_distance_quantile", "distance_time_quantile")
                    socket_query(ws, id += 1, "$path&metric=$metric")
                    reply = socket_receive(ws)
                    response = HTTP.get("$http$path&metric=$metric"; status_exception=false)
                    if metric == "distance_time_quantile"
                        @test response.status == 400
                        @test JSON.parse(reply)["type"] == "error"
                        @test JSON.parse(reply)["id"] == id
                    else
                        @test response.status == 200
                        @test socket_id(reply) == id
                        @test reply[5:end] == response.body
                        table = Arrow.Table(reply[5:end])
                        cells = encoding == "string" ? parse.(UInt64, table.index; base=16) : UInt64.(table.index_lower) .| (UInt64.(table.index_upper) .<< 32)
                        at = [findfirst(==(h), cells) for h in DEMO_CELLS[1:3]]
                        @test table.value[at] == [0, -0.5, 0.5]
                        @test table.value == table.time_quantile - table.distance_quantile
                    end
                end
            end
        end
    end
end

@testset "WebSocket validation and recovery" begin
    handler = make_handler(pack_graph(fixture_table()))
    good = "/reachable?index=85075dd7fffffff&departure_h=8&budget_h=1&max_walk_h=0"
    socket_test(handler) do url, http
        socket_open(url) do ws
            for (id, path) in enumerate(("https://secret@example.org/reachable", "/reachable#secret",
                    "//example.org/reachable", "/missing?secret", "/reachable?secret=secret", 123, "/reachable\\secret"))
                socket_query(ws, id, path)
                reply = JSON.parse(socket_receive(ws))
                @test reply["id"] == id
                @test reply["type"] == "error"
                @test !occursin("secret", reply["message"])
            end
            WS.send(ws, "{\"type\":\"query\",\"id\":8.0,\"url\":$(JSON.json(good))}")
            @test socket_id(socket_receive(ws)) == 8
            WS.send(ws, JSON.json((type="other", id=9, url=good)))
            @test JSON.parse(socket_receive(ws))["id"] == 9
            socket_query(ws, 10, good)
            @test socket_id(socket_receive(ws)) == 10
        end
        for text in ("{secret", "null", "[]", "{}", "{\"id\":true}", "{\"id\":0}",
                "{\"id\":-1}", "{\"id\":1.5}", "{\"id\":4294967296}", "{\"id\":\"1\"}", UInt8[1, 2])
            socket_open(url) do ws
                WS.send(ws, text)
                error = try WS.receive(ws) catch e; e end
                @test error isa WS.WebSocketError
                @test error.message.status == 1008
            end
        end
        for repeated in (1, 2)
            socket_open(url) do ws
                socket_query(ws, 2, "/missing")
                @test JSON.parse(socket_receive(ws))["id"] == 2
                socket_query(ws, repeated, good)
                error = try WS.receive(ws) catch e; e end
                @test error.message.status == 1008
            end
        end
    end
end

@testset "WebSocket newest pending, failure and disconnect" begin
    for fails in (false, true), disconnect in (false, true)
        entered = Channel{String}(8)
        release = Channel{Nothing}(1)
        finished = Channel{Nothing}(1)
        handler = request -> begin
            put!(entered, request.target)
            if request.target == "/reachable?active"
                take!(release)
                put!(finished, nothing)
                fails && error("secret internal failure")
            end
            HTTP.Response(200, UInt8[0x41, 0x52, 0x52, 0x4f, 0x57])
        end
        socket_test(handler) do url, http
            socket_open(url) do ws
                socket_query(ws, 42, "/reachable?active")
                @test timedwait(() -> isready(entered), 20) == :ok
                @test take!(entered) == "/reachable?active"
                for id in 43:45
                    socket_query(ws, id, "/reachable?$id")
                end
                # A ping/pong round trip proves the reader consumed the preceding burst.
                WS.ping(ws)
                frame = WS.readframe(ws)
                @test frame.flags.opcode == WS.PONG
                if disconnect
                    close(ws)
                end
                put!(release, nothing)
                @test timedwait(() -> isready(finished), 20) == :ok
                take!(finished)
                if !disconnect
                    reply = socket_receive(ws)
                    if fails
                        @test JSON.parse(reply) == Dict("type" => "error", "id" => 42, "message" => "query failed")
                    else
                        @test socket_id(reply) == 42
                    end
                    @test socket_id(socket_receive(ws)) == 45
                    @test take!(entered) == "/reachable?45"
                end
            end
            socket_open(url) do ws
                socket_query(ws, 1, "/reachable?fresh")
                @test socket_id(socket_receive(ws)) == 1
                @test take!(entered) == "/reachable?fresh"
            end
            @test !isready(entered)
        end
    end
end

@testset "WebSocket pending while shared HTTP workspace is locked" begin
    dispatched = Channel{Nothing}(8)
    entered = Channel{Nothing}(8)
    release = Channel{Nothing}(8)
    graph = pack_graph(fixture_table())
    resident = make_handler(graph)
    request_lock = ReentrantLock()
    handler = request -> begin
        put!(dispatched, nothing)
        lock(request_lock) do
            put!(entered, nothing)
            take!(release)
            resident(request)
        end
    end
    path = "/reachable?index=85075dd7fffffff&departure_h=8&budget_h=1&max_walk_h=0"
    socket_test(handler) do url, http
        http_task = @async HTTP.get(http * path)
        @test timedwait(() -> isready(entered), 20) == :ok
        take!(entered)
        take!(dispatched)
        socket_open(url) do a
            socket_query(a, 42, path)
            # The first socket job dispatches and waits for the HTTP-held lock.
            @test timedwait(() -> isready(dispatched), 20) == :ok
            take!(dispatched)
            socket_open(url) do b
                socket_query(b, 1, path)
                @test timedwait(() -> isready(dispatched), 20) == :ok
                take!(dispatched)
                for id in 43:45
                    socket_query(a, id, path)
                end
                WS.ping(a)
                @test WS.readframe(a).flags.opcode == WS.PONG
                @test !isready(entered)
                for _ in 1:4
                    put!(release, nothing)
                end
                @test fetch(http_task).status == 200
                @test socket_id(socket_receive(a)) == 42
                @test socket_id(socket_receive(a)) == 45
                @test socket_id(socket_receive(b)) == 1
            end
        end
    end
end
