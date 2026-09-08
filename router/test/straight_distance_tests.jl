# Included in WalkingCatchupTests. Expected times come from the independent
# itinerary search; expected distances use H3 directly, never routing km state.
using Arrow
od(origin, cells) = [h == origin ? 0.0 : H3.Lib.greatCircleDistanceKm(
    Ref(H3.API.cellToLatLng(origin)), Ref(H3.API.cellToLatLng(h))) for h in cells]

@testset "Straight-line distance" begin
    a, b, c, seconds = chain(7)
    ab = walk(a, b).ms
    cells = [a, b, c]
    rows = [(1, 2, 0, ab, 7.0), (2, 2, ab, 0, 3.0), (2, 3, ab, 0, 4.0),
            (1, 3, 0, ab, 9.0), (3, 1, ab, 0, 2.0), (1, 2, DAY - 1, 0, 1.0)]
    offgraph = first(setdiff(disk(a, 1), cells))
    remote = cell_at(40.0, -3.0, 7)
    for distances in (false, true)
        graph = pack_graph(raw_table(cells, rows; distances))
        bare = WalkingIndex(graph)
        prepared = prepare_walking(bare; max_walk_ms=1000seconds)
        for origin in (a, offgraph, remote), limit in (0, seconds, seconds + 1),
            (ready, budget, window, step) in ((0, 0, 3, 1), (0, ab, 7, 1), (DAY - 2, 7DAY, 65, 7))
            args = (graph, origin, ready, budget)
            kwargs = (; max_walk_ms=1000limit)
            expected_point = route_walking(args...; kwargs..., walking_index=bare)
            expected = route_window_walking(args..., window; kwargs..., step_ms=step, walking_index=bare)
            for index in (bare, prepared)
                point = route_walking(args...; kwargs..., walking_index=index, distance_mode="straight_line")
                @test point.h3 == expected_point.h3
                @test point.arrival == expected_point.arrival
                @test isequal(point.distance_km, od(origin, point.h3))
                reference = route_window_walking(args..., window; kwargs..., step_ms=step,
                    walking_index=index, distance_mode=:straight_line)
                for workers in (1, 4, 8)
                    actual = route_window_walking_cached(args..., window; kwargs..., step_ms=step,
                        walking_index=index, workers, chunk_size=2, distance_mode=:straight_line)
                    old = route_window_walking_cached(args..., window; kwargs..., step_ms=step,
                        walking_index=index, workers, chunk_size=2)
                    for field in FIELDS
                        field == :distance_km && continue
                        @test isequal(getproperty(actual, field), getproperty(expected, field))
                        @test isequal(getproperty(actual, field), getproperty(reference, field))
                    end
                    @test isequal(actual.distance_km, od(origin, actual.h3))
                    @test isequal(actual.distance_km, reference.distance_km)
                    for field in (:profile_lookups, :routing_expansions, :searches, :full_searches, :repair_searches)
                        @test getproperty(actual, field) == getproperty(old, field)
                    end
                end
            end
        end

        @testset "Absent replay and km buffers" for index in (bare, prepared), origin in (a, offgraph)
            plan = Reachability._walking_window_plan(graph, origin, 0, 7DAY, 7;
                step_ms=1, max_walk_ms=1000seconds, walking_index=index, distance_mode=:straight_line)
            output = Reachability._walking_output_plan(plan, origin)
            state = Reachability._walking_catchup_workspace(graph, plan, 7, nothing, output)
            @test all(isnothing(getproperty(state, f)) for f in (:connections, :seenA, :seenE, :kmA, :kmE))
            # Replay cannot accept these Nothing buffers; a successful chunk proves bypass.
            Reachability._walking_catchup_chunk!(state, graph, origin, plan, 1, 7)
            @test all(!hasproperty(p, :distance_km) for p in state.points)
            if !isnothing(output)
                @test isnothing(state.output.distance)
                @test eltype(state.points[1].ids) == Int32
                @test eltype(state.points[1].arrival) == UInt32
                @test isnothing(Reachability.WalkingOutputAccumulator(output.cells, 7, plan.budget, false).km)
            end
        end

        @testset "Transit-only arrival windows" for origin in (a, offgraph), ready in (0, DAY - 2)
            expected = route_window_cached(graph, origin, ready, 7DAY, 65; step_ms=7)
            actual = route_window_cached(graph, origin, ready, 7DAY, 65; step_ms=7, distance_mode=:straight_line)
            for field in (:elapsed_sum_ms, :elapsed_ms, :reachable_elapsed_ms, :reachable_samples,
                          :sample_count, :profile_lookups, :routing_expansions, :searches)
                @test isequal(getproperty(actual, field), getproperty(expected, field))
            end
            ids = findall(!iszero, actual.reachable_samples)
            @test actual.distance_km[ids] == od(origin, graph.h3[ids])
            state = Reachability._catchup_workspace(graph, 3, false)
            @test all(isnothing(getproperty(state, f)) for f in (:saved_distances, :seen, :connections))
        end

        @testset "HTTP mode and metric" begin
            handler = make_handler(graph)
            for origin in (a, offgraph), walking in (0, seconds), window in (0, 86400),
                metric in ("time", "distance_time_quantile"), encoding in ("string", "split")
                query = "/reachable?index=$(string(origin; base=16))&departure_h=0&budget_h=168&max_walk_h=$(walking / 3600)&metric=$metric&encoding=$encoding"
                window > 0 && (query *= "&window_h=$(window / 3600)&step_h=0.25")
                old = handler(HTTP.Request("GET", query))
                explicit = handler(HTTP.Request("GET", query * "&distance_mode=itinerary"))
                @test old.status == explicit.status
                @test old.body == explicit.body
                response = handler(HTTP.Request("GET", query * "&distance_mode=straight_line"))
                @test response.status == 200
                @test HTTP.header(response, "X-Router-Distance") == "origin-destination-great-circle-km"
                @test HTTP.header(response, "X-Router-Distance-Mode") == "straight_line"
                @test HTTP.header(response, "X-Router-Backend") == "reference"
                @test occursin("X-Router-Distance-Mode", HTTP.header(response, "Access-Control-Expose-Headers"))
                expected_body = if window > 0
                    result = walking > 0 ? route_window_walking(graph, origin, 0, 7DAY, DAY;
                        step_ms=900_000, max_walk_ms=1000walking, walking_index=bare) :
                        route_window(graph, origin, 0, 7DAY, DAY; step_ms=900_000)
                    h3 = hasproperty(result, :h3) ? result.h3 : graph.h3
                    Reachability.window_arrow(graph, merge(result, (distance_km=od(origin, h3),)), origin, encoding; metric)
                else
                    result = walking > 0 ? route_walking(graph, origin, 0, 7DAY; max_walk_ms=1000walking, walking_index=bare) :
                        route_details(graph, origin, 0, 7DAY)
                    h3 = hasproperty(result, :h3) ? result.h3 : graph.h3
                    Reachability.arrow_result(graph, result.arrival, origin, UInt32(0), encoding;
                        distance_km=od(origin, h3), h3, metric)
                end
                @test response.body == expected_body
                @test all(isfinite, Arrow.Table(response.body).distance_km)
                !distances && metric != "time" && @test old.status == 400
            end
            base = "/reachable?index=$(string(a; base=16))&departure_h=0&budget_h=0"
            for suffix in ("&distance_mode=fast", "&distance_mode=", "&distance_mode=straight_line&distance_mode=itinerary")
                @test handler(HTTP.Request("GET", base * suffix)).status == 400
            end
        end
        for mode in ("bad", :bad, nothing, 1, true, ["straight_line"])
            @test_throws ArgumentError route_walking(graph, UInt64(0), 0, 0; distance_mode=mode)
            for route in (route_window_walking, route_window_walking_cached, route_window_cached)
                @test_throws ArgumentError route(graph, UInt64(0), 0, 0, 1; distance_mode=mode)
            end
        end
    end

    @testset "Overflow is irrelevant to straight-line arrivals" begin
        d = cell_at(51.7, 0.3, 7)
        huge = floatmax(Float64)
        rows = [(1, 2, 0, 1, huge), (1, 3, 0, 2, 0.0), (2, 4, 2, 8, huge), (3, 4, 2, 1, 0.0)]
        graph = pack_graph(raw_table([a, b, c, d], rows))
        oracle = pack_graph(raw_table([a, b, c, d], rows; distances=false))
        for index in (WalkingIndex(graph), prepare_walking(WalkingIndex(graph))), origin in (a,)
            @test_throws r"accumulated route distance" route_walking(graph, origin, 0, 20; walking_index=index)
            point = route_walking(graph, origin, 0, 20; walking_index=index, distance_mode=:straight_line)
            @test point.arrival == route_walking(oracle, origin, 0, 20).arrival
            for route in (route_window_walking, route_window_walking_cached)
                @test_throws r"accumulated route distance" route(graph, origin, 0, 20, 4; step_ms=1, walking_index=index)
                result = route(graph, origin, 0, 20, 4; step_ms=1, walking_index=index, distance_mode=:straight_line)
                @test result.elapsed_sum_ms == route_window_walking(oracle, origin, 0, 20, 4; step_ms=1).elapsed_sum_ms
            end
        end
        @test_throws r"accumulated route distance" route_window_cached(graph, a, 0, 20, 4; step_ms=1)
        @test route_window_cached(graph, a, 0, 20, 4; step_ms=1, distance_mode=:straight_line).elapsed_sum_ms ==
              route_window_cached(oracle, a, 0, 20, 4; step_ms=1).elapsed_sum_ms
        handler = make_handler(graph)
        for window in ("", "&window_h=0.0011111111111111111&step_h=0.0002777777777777778"), walk in (0, 3600)
            query = "/reachable?index=$(string(a; base=16))&departure_h=0&budget_h=0.0002777777777777778&max_walk_h=$(walk / 3600)&distance_mode=straight_line$window"
            @test handler(HTTP.Request("GET", query)).status == 200
        end
    end

    @testset "No consecutive walks" begin
        graph = pack_graph(raw_table([a, c], [(1, 1, 0, 0, 0.0), (2, 2, 0, 0, 0.0)]))
        for route in (route_window_walking, route_window_walking_cached)
            result = route(graph, a, 0, 7DAY, 3; step_ms=1, max_walk_ms=1000seconds, distance_mode=:straight_line)
            @test !(c in result.h3)
            @test at(result, a, :distance_km) == 0.0
        end
    end
end
