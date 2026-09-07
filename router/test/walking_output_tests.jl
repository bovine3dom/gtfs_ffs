# Included in WalkingCatchupTests, sharing only raw fixture builders with the oracle.
using HTTP

@testset "Indexed walking output" begin
    for res in (7, 9), distances in (false, true)
        a, b, c, seconds = chain(res)
        ab = walk(a, b).ms
        cells = sort([a, b, c])
        rows = [(1, 2, 0, ab, 7.0), (2, 2, ab, 0, 3.0),
                (2, 3, ab, 0, 4.0), (1, 3, 0, ab, 9.0),
                (3, 1, ab, 0, 2.0), (1, 2, DAY - 1, 0, 1.0)]
        graph = pack_graph(raw_table(cells, rows; distances))
        bare = WalkingIndex(graph)
        index = prepare_walking(bare; max_walk_s=seconds)
        offgraph = first(setdiff(disk(a, 1), cells))
        remote = cell_at(40.0, -3.0, res)
        for origin in (a, offgraph, remote), limit in (0, seconds, seconds + 1),
            (ready, budget, window, step) in ((0, ab, 7, 1), (DAY - 2, 7DAY, 65, 7))
            expected = route_window_walking(graph, origin, ready, budget, window;
                step_ms=step, max_walk_s=limit, walking_index=bare)
            for workers in (1, 4, 8), chunk_size in (1, 2, 64)
                actual = route_window_walking_cached(graph, origin, ready, budget, window;
                    step_ms=step, max_walk_s=limit, walking_index=index, workers, chunk_size)
                for field in FIELDS
                    @test isequal(getproperty(actual, field), getproperty(expected, field))
                end
                @test actual.searches == actual.full_searches + actual.repair_searches == actual.sample_count
            end
            @test isequal(route_walking(graph, origin, ready, budget; max_walk_s=limit, walking_index=index),
                          route_walking(graph, origin, ready, budget; max_walk_s=limit, walking_index=bare))
        end
        output = index.prepared
        @test output.output_cells[1:length(graph.h3)] == graph.h3
        @test output.output_cells[output.output.targets] == output.geographic.targets
        @test length(unique(output.output_cells)) == length(output.output_cells)
        for origin in (a, offgraph, remote)
            plan = Reachability._walking_window_plan(graph, origin, 0, 7DAY, 65;
                step_ms=1, max_walk_s=seconds, walking_index=index)
            indexed = Reachability._walking_output_plan(plan, origin)
            @test !isnothing(indexed)
            @test indexed.cells[indexed.origin] == origin
            @test all(h -> h.cell in indexed.cells, walking_cells(bare, origin, 1000seconds))
        end
        if distances
            handler = make_handler(graph)
            reference = make_handler(graph; walking_window_route=(h, t, b, w, s, m, _) ->
                route_window_walking(graph, h, t, b, w; step_ms=s, max_walk_s=m, walking_index=bare))
            for metric in ("time", "distance_time_quantile"), origin in (a, offgraph, remote)
                query = "/reachable?index=$(string(origin; base=16))&departure=00:00:00&budget_s=604800&window_s=86400&step_s=900&metric=$metric"
                actual, expected = handler(HTTP.Request("GET", query)), reference(HTTP.Request("GET", query))
                @test actual.status == expected.status == 200
                @test actual.body == expected.body
                @test HTTP.header(actual, "X-Router-Window-Strategy") == "walking_catchup"
                @test HTTP.header(actual, "X-Router-Searches") == "96"
            end
        end
    end
    @testset "Prepared tentative overflow and recovery" begin
        a, b, c, seconds = chain(9)
        d = cell_at(51.7, 0.3, 9)
        huge = floatmax(Float64)
        rows = [(1, 2, 0, 1, huge), (1, 2, 1, 1, 0.0), (1, 3, 0, 2, 0.0),
                (1, 3, 1, 1, 0.0), (2, 4, 2, 8, huge), (3, 4, 2, 1, 0.0)]
        graph = pack_graph(raw_table([a, b, c, d], rows))
        index = prepare_walking(WalkingIndex(graph))
        for workers in (1, 4, 8), chunk_size in (1, 2, 64)
            @test_throws r"accumulated route distance" route_window_walking_cached(graph, a, 0, 20, 4;
                step_ms=1, walking_index=index, workers, chunk_size)
            actual = route_window_walking_cached(graph, a, 1, 20, 4;
                step_ms=1, walking_index=index, workers, chunk_size)
            expected = route_window_walking(graph, a, 1, 20, 4; step_ms=1)
            for field in FIELDS
                @test isequal(getproperty(actual, field), getproperty(expected, field))
            end
        end
    end
end
