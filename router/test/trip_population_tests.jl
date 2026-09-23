@testset "Trip-aware population admits sample-block workers" begin
    a, b, c = DEMO_CELLS[1:3]
    table = (from_h3=UInt64[a, b], to_h3=UInt64[b, c],
        departure_ms=UInt32[0, 300_999], duration_ms=Int64[1000, 1000],
        trip_id=["first", "second"], distance_km=[1.0, 1.0])
    mktempdir() do dir
        source = joinpath(dir, "trip_res5.arrow")
        Arrow.write(source, table)
        graph = pack_graph(source; trip_aware=false)
        shards = TripShardSet(source, graph)
        cells = UInt64[first(H3.API.cellToChildren(cell, 8)) for cell in (a, b, c)]
        population = Reachability._population(cells, fill(100.0, length(cells)))
        admission = RequestAdmission(; workers=Threads.nthreads(:default))
        handler = make_handler(graph; population, admission, trip_graph_set=shards)
        query = "/reachable?index=$(string(a; base=16))&departure_h=0&budget_h=1&max_walk_h=0" *
            "&metric=accessible_population&origin_radius=1&window_h=1.6&step_h=$(1 / 60)&trip_aware=true"
        response = handler(HTTP.Request("GET", query))
        @test response.status == 200
        @test HTTP.header(response, "X-Router-Trip-Aware") == "true"
        @test parse(Int, HTTP.header(response, "X-Router-Cache-Misses")) > 0
        @test parse(Int, HTTP.header(response, "X-Router-Workers")) == min(3, Threads.nthreads(:default))

        trip_graph = pack_graph(source)
        direct = make_handler(graph; population, trip_graph_loader=() -> trip_graph)
        response = direct(HTTP.Request("GET", query))
        @test response.status == 200
        @test parse(Int, HTTP.header(response, "X-Router-Workers")) == min(3, Threads.nthreads(:default))
    end
end
