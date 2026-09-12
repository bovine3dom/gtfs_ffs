module CoarsenessHTTPTests
using Test, HTTP, Arrow
import H3, JSON
using ..Reachability
const R = Reachability
const MODES = ("mean_intersection", "min_union", "max_intersection", "diff_union", "diff_intersection", "reachable_union")

@testset "Coarseness HTTP selection and validation" begin
    fine = pack_graph(R._warmup_table())
    population = R._population(fine.h3, ones(length(fine.h3)))
    for resolution in 5:8
        graph = resolution == 8 ? fine : coarsen_graph(fine, resolution)
        handler = make_handler(graph; population)
        origin = first(graph.h3)
        query(extra=""; cell=origin, walk="0.05", budget="0.05") = handler(HTTP.Request("GET",
            "/reachable?index=$(string(cell; base=16))&departure_h=0&budget_h=$budget&max_walk_h=$walk&encoding=string" * extra))
        normal = query()
        @test normal.status == 200
        @test normal.body == query("&coarseness=0").body
        @test HTTP.header(normal, "X-Router-Core-Resolution") == string(resolution)
        models = getfield(handler, :coarse_models)
        snapshot = copy(models)
        @test sort!(collect(keys(models))) == collect(5:(resolution - 1))
        for offset in ("0", "1", "2", "3", "9999999999999999999999999999999999999", "00002")
            effective = min(resolution - 5, offset == "0" ? 0 : offset == "1" ? 1 : offset in ("2", "00002") ? 2 : 3)
            response = query("&coarseness=$offset")
            @test response.status == 200
            @test HTTP.header(response, "X-Router-Coarseness") == string(effective)
            @test HTTP.header(response, "X-Router-Core-Resolution") == string(resolution - effective)
            @test HTTP.header(response, "X-Router-Backend") == (effective == 0 ? "reference" : "coarse-time")
            table = Arrow.Table(response.body)
            @test all(h -> H3.API.getResolution(parse(UInt64, h; base=16)) == resolution, table.index)
            @test table.elapsed_h[findfirst(==(string(origin; base=16)), table.index)] == 0
            @test eltype(table.distance_km) == Float64
            @test occursin("X-Router-Coarseness", HTTP.header(response, "Access-Control-Expose-Headers"))
        end
        for invalid in ("", "-1", "-0", "1.5", "true", "%2B1", "1e2", "1%0A")
            @test query("&coarseness=$invalid").status == (resolution == 5 ? 200 : 400)
            @test query("&coarseness=$invalid"; walk="0").status == 200
            @test query("&coarseness=$invalid"; budget="0").status == 200
            response = query("&coarseness=$invalid"; walk="2", budget="0.001")
            @test response.status == 200
            @test HTTP.header(response, "X-Router-Coarseness") == "0"
        end
        @test query("&coarseness=1&coarseness=2"; walk="0").status == 400
        @test query("&approximate=true").status == 400
        @test query("&coarseness=1&window_mode=invalid").status == 200
        @test query("&coarseness=1&window_h=0.01&window_mode=invalid").status == 400
        for offset in 0:(resolution - 5)
            url = "&coarseness=$offset&metric=accessible_population"
            for hits in (0, 1)
                response = query(url)
                @test response.status == 200
                @test HTTP.header(response, "X-Router-Cache-Hits") == string(hits)
                @test HTTP.header(response, "X-Router-Cache-Misses") == string(1 - hits)
                @test HTTP.header(response, "X-Router-Backend") == (offset == 0 ? "shared-population" : "coarse-population")
            end
        end
        @test HTTP.header(query("&coarseness=99999999999999999999&metric=accessible_population"), "X-Router-Cache-Hits") == "1"
        elsewhere = H3.API.latLngToCell(H3.API.LatLng(deg2rad(48.85), deg2rad(2.35)), resolution)
        @test query("&coarseness=2"; cell=elsewhere, budget="0.001").status == 200
        @test keys(models) == keys(snapshot)
        @test all(models[target] === snapshot[target] for target in keys(models))
    end
end

@testset "Coarse Arrow modes and optional distances" begin
    source = R._warmup_table()
    for distances in (true, false)
        graph = pack_graph(distances ? source : Base.structdiff(source, NamedTuple{(:distance_km,)}))
        handler = make_handler(graph; population=R._population(graph.h3, ones(length(graph.h3))))
        base = "/reachable?index=$(string(first(graph.h3); base=16))&departure_h=0&budget_h=0.05&max_walk_h=0.05&coarseness=2"
        for mode in MODES, distance in ("itinerary", "straight_line"), metric in ("time", "time_distance_quantile", "accessible_population")
            response = handler(HTTP.Request("GET", base * "&window_h=0.02&step_h=0.01&window_mode=$mode&distance_mode=$distance&metric=$metric"))
            invalid = metric == "time_distance_quantile" && (mode == "reachable_union" || (!distances && distance == "itinerary"))
            @test response.status == (invalid ? 400 : 200)
            invalid && continue
            table = Arrow.Table(response.body)
            @test eltype(table.value) == Float64
            if metric == "accessible_population"
                @test propertynames(table) == [:index_lower, :index_upper, :value]
            else
                @test all(==(2), table.sample_count)
                @test eltype(table.elapsed_h) == eltype(table.distance_km) == Float64
                @test metric != "time_distance_quantile" || :time_quantile in propertynames(table)
            end
        end
        absent = make_handler(graph)
        @test absent(HTTP.Request("GET", base * "&metric=accessible_population")).status == 400
    end
end

@testset "Reusable population cache callback" begin
    graph = pack_graph(R._warmup_table())
    population = R._population(graph.h3, ones(length(graph.h3)))
    walking = prepare_walking(WalkingIndex(graph))
    origin = first(graph.h3)
    for value in (0.0, 1.0, 2.0)
        cache = R.PopulationResultCache(graph, population, walking)
        calls = Ref(0)
        route_origins = origins -> begin
            calls[] += 1
            (; h3=origins, value=fill(value, length(origins)), shared_expansions=1, query_expansions=1, workers=1)
        end
        for hits in (0, 1)
            result = R._cached_route_population(cache, origin, 0, 100; route_origins)
            @test result.value == [value]
            @test result.cache_hits == hits
            @test calls[] == 1
        end
    end
end

@testset "Coarse HTTP and WebSocket parity" begin
    graph = pack_graph(R._warmup_table())
    handler = make_handler(graph; population=R._population(graph.h3, ones(length(graph.h3))))
    server = HTTP.serve!(make_stream_handler(handler), "127.0.0.1", 0;
        stream=true, listenany=true, verbose=-1)
    try
        port = HTTP.port(server)
        HTTP.WebSockets.open("ws://127.0.0.1:$port/query"; proxy=nothing) do ws
            HTTP.closewrite(ws.io)
            id = 0
            for metric in ("time", "time_distance_quantile", "accessible_population"), encoding in ("split", "string")
                url = "/reachable?index=$(string(first(graph.h3); base=16))&departure_h=0&budget_h=0.05&max_walk_h=0.05&coarseness=2&metric=$metric&encoding=$encoding"
                response = HTTP.get("http://127.0.0.1:$port$url"; proxy=nothing)
                HTTP.WebSockets.send(ws, JSON.json((type="query", id=(id += 1), url=url)))
                bytes = HTTP.WebSockets.receive(ws)
                @test bytes == vcat(UInt8[0, 0, 0, id], response.body)
            end
        end
    finally
        close(server)
    end
end
end
