import JSON

const WS = HTTP.WebSockets

function socket_test(f, handler; origins=String[])
    server = HTTP.serve!(make_stream_handler(handler; origins), "127.0.0.1", 0;
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

@testset "WebSocket HTTP/Arrow parity and origins" begin
    graph = pack_graph(merge(fixture_table(), (distance_km=fill(2.0, 6),)))
    handler = make_handler(graph)
    socket_test(handler) do url, http
        for origin in ("http://localhost:8000", "https://maps.example.org", "null")
            WS.open(url; headers=["Origin" => origin]) do ws
                path = "/reachable?index=85075dd7fffffff&departure=08:00:00&budget_s=3600&max_walk_s=0"
                socket_query(ws, 1, path)
                bytes = socket_receive(ws)
                @test socket_id(bytes) == 1
                @test bytes[5:end] == HTTP.get(http * path).body
            end
        end
    end
    socket_test(handler; origins=["http://localhost:8000"]) do url, http
        @test HTTP.get("$http/query"; status_exception=false).status == 426
        @test HTTP.get("$http/missing"; status_exception=false).status == 404
        for origin in ("null", "http://localhost:8000.evil", "http://localhost:8001", "*")
            @test HTTP.get("$http/query", ["Origin" => origin]; status_exception=false).status == 403
        end
        WS.open(url; headers=["Origin" => "http://localhost:8000"]) do ws
            id = UInt32(0xfedcba98)
            for encoding in ("split", "string"), metric in ("time", "distance_time_quantile"),
                    mode in ("itinerary", "straight_line"), walk in (0, 3600), window in (0, 120),
                    cell in (DEMO_ORIGIN, DEMO_CELLS[7])
                path = "/reachable?index=$(H3.API.h3ToString(cell))&departure=08:00:00&budget_s=3600&encoding=$encoding&metric=$metric&distance_mode=$mode&max_walk_s=$walk&window_s=$window"
                socket_query(ws, id, path)
                bytes = socket_receive(ws)
                @test bytes isa Vector{UInt8}
                @test socket_id(bytes) == id
                expected = HTTP.get(http * path).body
                @test bytes[5:end] == expected
                table = Arrow.Table(bytes[5:end])
                @test :value in propertynames(table)
                id += 1
            end
        end
    end
end

@testset "WebSocket validation and recovery" begin
    handler = make_handler(pack_graph(fixture_table()))
    good = "/reachable?index=85075dd7fffffff&departure=08:00:00&budget_s=3600&max_walk_s=0"
    socket_test(handler) do url, http
        WS.open(url) do ws
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
            WS.open(url) do ws
                WS.send(ws, text)
                error = try WS.receive(ws) catch e; e end
                @test error isa WS.WebSocketError
                @test error.message.status == 1008
            end
        end
        for repeated in (1, 2)
            WS.open(url) do ws
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
            WS.open(url) do ws
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
            WS.open(url) do ws
                socket_query(ws, 1, "/reachable?fresh")
                @test socket_id(socket_receive(ws)) == 1
                @test take!(entered) == "/reachable?fresh"
            end
            @test !isready(entered)
        end
    end
end

@testset "WebSocket pending while shared HTTP workspace is locked" begin
    entered = Channel{Nothing}(8)
    release = Channel{Nothing}(8)
    graph = pack_graph(fixture_table())
    handler = make_handler(graph; route=(h, t, b) -> begin
        put!(entered, nothing)
        take!(release)
        route_cpu(graph, h, t, b)
    end)
    path = "/reachable?index=85075dd7fffffff&departure=08:00:00&budget_s=3600&max_walk_s=0"
    socket_test(handler) do url, http
        http_task = @async HTTP.get(http * path)
        @test timedwait(() -> isready(entered), 20) == :ok
        take!(entered)
        WS.open(url) do a
            socket_query(a, 42, path)
            # The first socket job dispatches and waits for the HTTP-held lock.
            WS.ping(a)
            @test WS.readframe(a).flags.opcode == WS.PONG
            yield()
            WS.open(url) do b
                socket_query(b, 1, path)
                WS.ping(b)
                @test WS.readframe(b).flags.opcode == WS.PONG
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
