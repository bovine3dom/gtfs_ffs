@testset "CPU window HTTP parity" begin
    graph = pack_graph(distance_table([(1, 2, i, 1, Float64(i + 1)) for i in 0:128]))
    handler = make_handler(graph)
    server = HTTP.serve!(handler, "127.0.0.1", 0; listenany=true, verbose=-1)
    try
        for origin in DEMO_CELLS[[1, 2, 7]], encoding in ("string", "split"), metric in ("time", "time_distance_quantile")
            target = "/reachable?index=$(H3.API.h3ToString(origin))&departure_h=0&budget_h=$(200/3600)&window_h=$(129/3600)&step_h=$(1/3600)&encoding=$encoding&metric=$metric&max_walk_h=0"
            response = HTTP.get("http://127.0.0.1:$(HTTP.port(server))$target")
            expected = route_window(graph, origin, 0, 200_000, 129_000; step_ms=1000)
            @test response.body == Reachability.window_arrow(graph, expected, origin, encoding; metric)
            @test HTTP.header(response, "X-Router-Window-Strategy") == "catchup"
            @test HTTP.header(response, "X-Router-Backend") == "reference"
            @test occursin("X-Router-Workers", HTTP.header(response, "Access-Control-Expose-Headers"))
        end
    finally
        close(server)
    end
end
