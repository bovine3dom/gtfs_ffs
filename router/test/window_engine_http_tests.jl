@testset "Window engine HTTP parity" begin
    graph = pack_graph(distance_table([(1, 2, 0, 10, 1.0), (1, 2, 30, 10, 8.0),
                                      (1, 2, 60, 10, 4.0), (2, 3, 90, 10, 2.0)]))
    baseline = (h, t, b, w, s) -> route_window(graph, h, t, b, w; step_ms=s)
    expected_handler = make_handler(graph; window_route=baseline)
    engines = Any[("origin", baseline), ("catchup", (h, t, b, w, s) -> route_window_cached(graph, h, t, b, w; step_ms=s))]
    backends = "--backend=oneapi" in ARGS ? [KA.CPU(), oneAPI.oneAPIBackend()] : [KA.CPU()]
    for backend in backends
        router = WindowKernelRouter(graph, backend; batch_size=3)
        name = backend isa KA.CPU ? "ka_cpu_batched" : "gpu_batched"
        push!(engines, (name, (h, t, b, w, s) -> route_window_kernel!(router, h, t, b, w; step_ms=s)))
    end
    for (name, engine) in engines
        handler = make_handler(graph; window_route=engine)
        for origin in DEMO_CELLS[[1, 2, 7]], encoding in ("string", "split"), metric in ("time", "distance_time_quantile")
            target = "/reachable?index=$(H3.API.h3ToString(origin))&departure_h=0&budget_h=0.03333333333333333&window_h=0.016944444444444446&step_h=0.008333333333333333&encoding=$encoding&metric=$metric&max_walk_h=0"
            response = handler(HTTP.Request("GET", target))
            @test response.status == 200
            @test response.body == expected_handler(HTTP.Request("GET", target)).body
            @test HTTP.header(response, "X-Router-Window-Strategy") == name
            @test HTTP.header(response, "X-Router-Backend") == (name in ("origin", "catchup") ? "reference" : name)
            @test occursin("X-Router-Window-Strategy", HTTP.header(response, "Access-Control-Expose-Headers"))
        end
    end
end
@testset "Parallel window HTTP output" begin
    rows = [(1, 2, i, 1, Float64(i + 1)) for i in 0:128]
    push!(rows, (2, 3, 150, 10, 10.0))
    graph = pack_graph(distance_table(rows))
    serial = make_handler(graph; window_route=(h, t, b, w, s) -> route_window_cached(graph, h, t, b, w; step_ms=s, chunk_size=8, workers=1))
    parallel = make_handler(graph; window_route=(h, t, b, w, s) -> route_window_cached(graph, h, t, b, w; step_ms=s, chunk_size=8))
    server = HTTP.serve!(parallel, "127.0.0.1", 0; listenany=true, verbose=-1)
    try
        for encoding in ("string", "split"), metric in ("time", "distance_time_quantile")
            target = "/reachable?index=$(H3.API.h3ToString(DEMO_CELLS[1]))&departure_h=0&budget_h=0.05555555555555555&window_h=0.035833333333333335&step_h=0.0002777777777777778&encoding=$encoding&metric=$metric&max_walk_h=0"
            response = HTTP.get("http://127.0.0.1:$(HTTP.port(server))$target")
            @test response.body == serial(HTTP.Request("GET", target)).body
            @test HTTP.header(response, "X-Router-Workers") == string(min(Threads.nthreads(:default), cld(129, 8)))
            @test occursin("X-Router-Workers", HTTP.header(response, "Access-Control-Expose-Headers"))
        end
    finally
        close(server)
    end
end
