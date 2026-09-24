import JSON

admitted(a) = sum(scheduler_stats(a).workers) + (sum(scheduler_stats(a).output_bytes) > 0)
admission_wait(f) = @test timedwait(f, 20) == :ok

function admission_server(f, handler)
    server = HTTP.serve!(make_stream_handler(handler), "127.0.0.1", 0;
                         stream=true, listenany=true, verbose=-1)
    try
        f("http://127.0.0.1:$(HTTP.port(server))", "ws://127.0.0.1:$(HTTP.port(server))/query")
    finally
        close(server)
    end
end

@testset "Admission settings" begin
    @test sum(scheduler_stats(RequestAdmission()).pending_limit) == 128
    @test scheduler_stats(RequestAdmission(; max_pending=0)).pending_limit == (0, 0)
    @test_throws ArgumentError RequestAdmission(; max_pending=-1)
    @test_throws ArgumentError RequestAdmission(; max_pending=typemax(Int))
    threads = Threads.nthreads(:default)
    cli = parse_cli(["--demo"])
    @test cli.short_workers == cld(threads, 4)
    @test cli.max_workers_per_request == cld(max(1, threads - cli.short_workers), 2)
    short_workers = threads == 1 ? 1 : cld(threads, 10)
    bulk_workers = max(1, threads - short_workers)
    custom = parse_cli(["--demo", "--short-workers=$short_workers",
        "--max-workers-per-request=$bulk_workers"])
    @test (custom.short_workers, custom.max_workers_per_request) == (short_workers, bulk_workers)
    changed_reserve = parse_cli(["--demo", "--short-workers=$short_workers"])
    @test changed_reserve.max_workers_per_request == cld(bulk_workers, 2)
    for option in ("--max-pending", "--workspace-memory-gib", "--short-workers", "--max-workers-per-request"),
        value in ("", "-1", "1.5", "true", "Inf", string(typemax(Int)))
        @test_throws ArgumentError parse_cli(["$option=$value", "--demo"])
    end
    for option in ("--workspace-memory-gib", "--short-workers", "--max-workers-per-request")
        @test_throws ArgumentError parse_cli(["$option=0", "--demo"])
    end
    for option in ("--max-pending", "--workspace-memory-gib", "--short-workers", "--max-workers-per-request")
        @test_throws ArgumentError parse_cli([option])
        @test_throws ArgumentError parse_cli(["$option=1", option, "2"])
    end
    for args in (["--max-pending=0", "--workspace-memory-gib=1"],
                 ["--max-pending", "0", "--workspace-memory-gib", "1"])
        opts = parse_cli(["--demo"; args])
        @test opts.max_pending == 0
        @test opts.workspace_bytes == 1024^3
        @test opts.paths == ["--demo"]
    end
    @test_throws ArgumentError load_handlers(["missing_res5.arrow"]; max_pending=-1)
    @test_throws ArgumentError load_handlers(["missing_res5.arrow"]; workspace_bytes=0)
end

@testset "Bounded waiting room and bypasses" begin
    for pending in (0, 2)
        gate = RequestAdmission(; workers=1, max_pending=pending)
        cpu = Reachability.ComputeLease(gate, 1, 0, 0, UInt64(0))
        pool = PopulationWorkspacePool()
        handler = make_handler(pack_graph(fixture_table()); admission=gate, workspace_pool=pool)
        path = "/reachable?index=$(string(DEMO_ORIGIN; base=16))&departure_h=0&budget_h=0&max_walk_h=0"
        request = HTTP.Request("GET", path)
        Reachability._acquire_compute!(cpu, 1, 1)
        tasks = Task[]
        try
            for i in 1:gate.pending_limit[1]
                push!(tasks, Threads.@spawn handler(HTTP.Request("GET", path)))
                admission_wait(() -> scheduler_stats(gate).pending[1] == i)
            end
            response = handler(request)
            @test response.status == 503
            @test String(response.body) == "router busy; retry later"
            @test HTTP.header(response, "Retry-After") == "1"
            @test HTTP.header(response, "Access-Control-Allow-Origin") == "*"
            @test population_workspace_stats(pool).retained_bytes == 0
            @test handler(HTTP.Request("GET", "/reachable?bad")).status == 400
            @test handler(HTTP.Request("OPTIONS", "/reachable")).status == 204
            @test handler(HTTP.Request("GET", "/missing")).status == 404
            @test handler(HTTP.Request("POST", "/reachable")).status == 405
        finally
            Reachability._release_compute!(cpu)
        end
        for task in tasks
            response = fetch(task)
            @test response.status == 200
            @test parse(Float64, HTTP.header(response, "X-Router-Queue-Wait-Ms")) >= 0
            @test !HTTP.hasheader(response, "X-Router-Workspace-Estimated-Bytes")
        end
        @test admitted(gate) == 0
        @test handler(request).status == 200
        @test !haskey(request.context, :router_admission)
        same = make_network_handler(Dict(("a", 5) => handler, ("b", 5) => handler); default_network="a")
        @test same.admission === gate
        other = make_handler(pack_graph(fixture_table()))
        mixed = make_network_handler(Dict(("a", 5) => handler, ("b", 5) => other); default_network="a")
        @test !(mixed isa Reachability.AdmittedHandler)
    end
end

# Block inside HTTP.streamhandler's response write, without large socket buffers.
struct AdmissionTestBody
    entered::Channel{Nothing}
    release::Channel{Nothing}
    fails::Bool
end
Base.length(::AdmissionTestBody) = 1
function Base.write(stream::HTTP.Stream, body::AdmissionTestBody)
    put!(body.entered, nothing)
    take!(body.release)
    body.fails && throw(Base.IOError("test disconnect", Base.UV_EPIPE))
    write(stream, UInt8[0x41])
end

struct AdmissionTestSocket
    messages::Channel{String}
    entered::Channel{Nothing}
    release::Channel{Nothing}
    io::IOBuffer
end
Base.iterate(ws::AdmissionTestSocket, state...) = iterate(ws.messages, state...)
function HTTP.WebSockets.send(ws::AdmissionTestSocket, outcome)
    put!(ws.entered, nothing)
    take!(ws.release)
end

@testset "WebSocket lease covers response send" begin
    gate = RequestAdmission(; workers=1, max_pending=0, output_bytes=65584)
    handler = request -> Reachability._with_admission(() -> HTTP.Response(200, UInt8[0x41]), request, gate)
    ws = AdmissionTestSocket(Channel{String}(1), Channel{Nothing}(1), Channel{Nothing}(1), IOBuffer())
    reader = @async Reachability.query_socket(ws, handler)
    put!(ws.messages, JSON.json((type="query", id=1, url="/reachable")))
    admission_wait(() -> isready(ws.entered))
    @test admitted(gate) == 1
    @test handler(HTTP.Request("GET", "/reachable")).status == 503
    close(ws.messages)
    wait(reader)
    @test admitted(gate) == 1
    put!(ws.release, nothing)
    admission_wait(() -> admitted(gate) == 0)
end

@testset "Population pool headers and memory rejection" begin
    source = first(H3.API.cellToChildren(DEMO_ORIGIN, 8))
    target = first(filter(!=(source), H3.API.gridDisk(source, 1)))
    graph = pack_graph((from_h3=[source], to_h3=[target], departure_ms=UInt32[0], duration_ms=Int64[0]))
    population = Reachability._population([target], [1.0])
    path = "/reachable?index=$(string(source; base=16))&departure_h=0&budget_h=0&max_walk_h=0&metric=accessible_population"
    gate = RequestAdmission(; max_pending=0)
    handler = make_handler(graph; population, admission=gate)
    for _ in 1:2
        response = handler(HTTP.Request("GET", path))
        @test response.status == 200
        for name in ("Estimated-Bytes", "Retained-Bytes", "Reused-Workers")
            header = "X-Router-Workspace-$name"
            @test parse(Int, HTTP.header(response, header)) >= 0
            @test occursin(header, HTTP.header(response, "Access-Control-Expose-Headers"))
        end
    end
    limited = make_handler(graph; population, admission=gate, workspace_pool=PopulationWorkspacePool(; max_bytes=1))
    response = limited(HTTP.Request("GET", path))
    @test response.status == 422
    @test String(response.body) == "request exceeds configured routing memory budget"
    @test !HTTP.hasheader(response, "Retry-After")
    @test admitted(gate) == 0
    admission_server(limited) do _, wsurl
        HTTP.WebSockets.open(wsurl) do ws
            HTTP.closewrite(ws.io)
            HTTP.WebSockets.send(ws, JSON.json((type="query", id=1, url=path)))
            @test JSON.parse(HTTP.WebSockets.receive(ws)) == Dict("type" => "error", "id" => 1,
                "message" => "request exceeds configured routing memory budget")
        end
        admission_wait(() -> admitted(gate) == 0)
    end
end

@testset "HTTP lease covers response writes and failures" begin
    for fails in (false, true)
        gate = RequestAdmission(; workers=1, max_pending=0, output_bytes=65584)
        body = AdmissionTestBody(Channel{Nothing}(1), Channel{Nothing}(1), fails)
        calls = Threads.Atomic{Int}(0)
        handler = request -> Reachability._with_admission(request, gate) do
            Threads.atomic_add!(calls, 1)
            HTTP.Response(200, ["Content-Length" => "1"], body)
        end
        admission_server(handler) do http, wsurl
            first = @async try HTTP.get(http * "/reachable"; retry=false) catch e; e end
            admission_wait(() -> isready(body.entered))
            @test admitted(gate) == 1
            @test HTTP.get(http * "/reachable"; status_exception=false).status == 503
            @test calls[] == 2 # CPU work can finish while another response waits to write.
            @test sum(scheduler_stats(gate).workers) == 0
            put!(body.release, nothing)
            admission_wait(() -> istaskdone(first))
            @test fails ? fetch(first) isa Exception : fetch(first).status == 200
            admission_wait(() -> admitted(gate) == 0)
        end
    end
end

@testset "Lease cleanup on exceptions and request reuse" begin
    gate = RequestAdmission(; workers=1, max_pending=0)
    request = HTTP.Request("GET", "/reachable")
    for transport in (false, true)
        operation = () -> Reachability._with_admission(() -> error("test failure"), request, gate)
        @test_throws ErrorException transport ? Reachability._with_response_admission(operation, request) : operation()
        @test admitted(gate) == 0
        @test !haskey(request.context, :router_admission)
    end
    for _ in 1:2
        Reachability._with_response_admission(request) do
            @test Reachability._with_admission(() -> HTTP.Response(200, UInt8[42]), request, gate).body == UInt8[42]
            @test Reachability._with_admission(() -> HTTP.Response(200, UInt8[43]), request, gate).body == UInt8[43]
            @test admitted(gate) == 1
        end
        @test admitted(gate) == 0
    end
end

@testset "HTTP and WebSocket share admission; disconnect joins CPU work" begin
    gate = RequestAdmission(; workers=1, max_pending=0)
    entered, release = Channel{Nothing}(1), Channel{Nothing}(1)
    handler = request -> Reachability._with_admission(request, gate) do
        if endswith(request.target, "active")
            put!(entered, nothing)
            take!(release)
        end
        HTTP.Response(200, UInt8[0x41])
    end
    admission_server(handler) do http, wsurl
        HTTP.WebSockets.open(wsurl) do ws
            HTTP.closewrite(ws.io)
            send(id, url) = HTTP.WebSockets.send(ws, JSON.json((type="query", id=id, url=url)))
            lease = Reachability.ComputeLease(gate, 1, 0, 0, UInt64(0))
            Reachability._acquire_compute!(lease, 1, 1)
            try
                send(1, "/reachable")
                reply = JSON.parse(HTTP.WebSockets.receive(ws))
                @test reply == Dict("type" => "error", "id" => 1, "message" => "router busy; retry later")
            finally
                Reachability._release_compute!(lease)
            end
            send(2, "/reachable")
            @test HTTP.WebSockets.receive(ws) == UInt8[0, 0, 0, 2, 0x41]
            admission_wait(() -> admitted(gate) == 0)
            send(3, "/reachable?active")
            admission_wait(() -> isready(entered))
            @test HTTP.get(http * "/reachable"; status_exception=false).status == 503
            close(ws)
            @test admitted(gate) == 1
            put!(release, nothing)
            admission_wait(() -> admitted(gate) == 0)
        end
        @test HTTP.get(http * "/reachable").status == 200
    end
end
