@testset "Request-local timing and learned cost" begin
    R = Reachability
    handler = make_handler(pack_graph(fixture_table()))
    path = "/reachable?index=$(string(DEMO_ORIGIN; base=16))&departure_h=8&budget_h=1&max_walk_h=0"
    request() = HTTP.Request("GET", path, ["X-Router-Timing" => "true"])
    first_response = handler(request())
    @test first_response.status == 200
    @test occursin("route;dur=", HTTP.header(first_response, "Server-Timing"))
    @test HTTP.header(first_response, "X-Router-Response-Cache") == "miss"
    cached = handler(request())
    @test cached.body == first_response.body
    @test HTTP.header(cached, "X-Router-Response-Cache") == "hit"
    @test !occursin("route;dur=", HTTP.header(cached, "Server-Timing"))
    @test isempty(HTTP.header(handler(HTTP.Request("GET", path)), "Server-Timing"))
    model = R.RouteCostModel()
    @test !R._short_route(model, :window, false)
    R._record_route_cost!(model, :window, 5_000_000, 24)
    @test R._short_route(model, :window, false)
    R._record_route_cost!(model, :window, 300_000_000, 1)
    @test !R._short_route(model, :window, true)
    for i in 1:1100
        R._record_route_cost!(model, i, 1, 1)
    end
    @test length(model.costs) == 1024
    if Threads.nthreads(:default) >= 3
        admission = RequestAdmission(; short_workers=1, memory_bytes=160_000, output_bytes=160_000)
        constrained = make_handler(pack_graph(fixture_table()); admission)
        response = constrained(request())
        @test response.status == 200
        @test HTTP.header(response, "X-Router-Lane") == "bulk"
    end
end

@testset "Off-network windows use bounded output work" begin
    R = Reachability
    graph = pack_graph(fixture_table())
    outside = DEMO_CELLS[7]
    @test !haskey(graph.node_id, outside)
    handler = make_handler(graph)
    for mode in (:mean_intersection, :min_union, :diff_union, :reachable_union)
        path = "/reachable?index=$(string(outside; base=16))&departure_h=0&budget_h=160" *
            "&max_walk_h=0&window_h=24&step_h=0.51&window_mode=$mode&distance_mode=straight_line&encoding=split"
        response = handler(HTTP.Request("GET", path, ["X-Router-Timing" => "true"]))
        expected = route_window_cached(graph, outside, 0, 576_000_000, 86_400_000;
            step_ms=1_836_000, window_mode=mode, distance_mode=:straight_line)
        @test response.status == 200
        @test HTTP.header(response, "X-Router-Lane") == "short"
        @test response.body == R.window_arrow(graph, expected, outside, "split"; window_mode=mode)
    end
end

@testset "Exact trip window sample reuse" begin
    R = Reachability
    graph = pack_graph(merge(fixture_table(), (trip_id=fill("trip", 6),)))
    cache = R.WindowSampleCache(; capacity=1024^2)
    for distance_mode in (:straight_line, :itinerary), window_mode in (:min_union, :mean_intersection, :diff_union)
        namespace = gensym()
        kwargs = (; step_ms=60_000, distance_mode, window_mode, workers=min(4, Threads.nthreads(:default)))
        first_result = route_window_cached(graph, DEMO_ORIGIN, START, 3_600_000, 600_000;
            kwargs..., sample_cache=(cache, namespace))
        @test first_result.full_searches == 10
        shifted = route_window_cached(graph, DEMO_ORIGIN, START + 60_000, 3_600_000, 600_000;
            kwargs..., sample_cache=(cache, namespace))
        reference = route_window_cached(graph, DEMO_ORIGIN, START + 60_000, 3_600_000, 600_000; kwargs...)
        @test shifted.full_searches == 1
        @test shifted.reused_samples == 9
        for field in (:elapsed_ms, :reachable_elapsed_ms, :distance_km, :reachable_samples)
            @test isequal(getproperty(shifted, field), getproperty(reference, field))
        end
        changed = route_window_cached(graph, DEMO_ORIGIN, START + 60_000, 3_599_999, 600_000;
            kwargs..., sample_cache=(cache, namespace))
        @test changed.full_searches == 10
    end
    small = R.WindowSampleCache(; capacity=1)
    compute() = (labels=UInt32[1], distances=nothing)
    @test last(R._cached_window_sample(compute, small, :key)) == false
    @test isempty(small.entries) && small.bytes == 0
    @test cache.bytes <= cache.capacity
    bounded = R.WindowSampleCache(; capacity=1024)
    R._cached_window_sample(compute, bounded, :first)
    R._cached_window_sample(compute, bounded, :second)
    @test collect(keys(bounded.entries)) == [:second]
    @test last(R._cached_window_sample(compute, bounded, :second))
    @test_throws ErrorException R._cached_window_sample(() -> error("injected"), bounded, :failure)
    @test !haskey(bounded.entries, :failure)
    @test bounded.bytes == sum(last, values(bounded.entries))
end

@testset "Parallel population tiles and sparse lane reset" begin
    R = Reachability
    graph = pack_graph(merge(fixture_table(), (trip_id=fill("trip", 6),)))
    origins = sort!(filter!(!iszero, H3.API.gridDisk(DEMO_ORIGIN, 3)))
    children = UInt64[first(H3.API.cellToChildren(cell, 8)) for cell in origins]
    population = R._population(children, Float64.(1:length(children)))
    index = WalkingIndex(graph)
    for mode in (:min_union, :mean_intersection, :reachable_union), walk in (0, 600_000), exclude in (false, true)
        kwargs = (; origins=copy(origins), walking_index=index, max_walk_ms=walk,
            window_ms=300_000, step_ms=60_000, window_mode=mode,
            origin_batch_size=3, exclude_origin_population=exclude)
        serial = R._route_population_reference(graph, population, DEMO_ORIGIN, START, 3_600_000;
            kwargs..., workers=1)
        parallel = R._route_population_reference(graph, population, DEMO_ORIGIN, START, 3_600_000;
            kwargs..., workers=min(4, Threads.nthreads(:default)))
        @test parallel.h3 == serial.h3
        @test parallel.value ≈ serial.value
    end
    workspace = R.TripWorkspace{UInt128}()
    for lanes in (64, 7, 32, 64)
        R._reset_trip_workspace!(workspace, 100, lanes)
        @test !any(workspace.dense_seen)
        R._trip_dense_node!(workspace, 42, lanes)
        slots = ((42 - 1) * lanes + 1):(42 * lanes)
        @test all(==(R.INF), workspace.best[slots])
        workspace.best[first(slots)] = 0
        R._trip_dense_node!(workspace, 42, lanes)
        @test workspace.best[first(slots)] == 0
    end
end
