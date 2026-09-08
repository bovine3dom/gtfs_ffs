# Fixture departure/duration units are seconds; the packed input is milliseconds.
function distance_table(rows; cells=DEMO_CELLS, distances=true)
    table = (from_h3=UInt64[cells[r[1]] for r in rows], to_h3=UInt64[cells[r[2]] for r in rows],
             departure_ms=UInt32[1000r[3] for r in rows], duration_ms=Int64[1000r[4] for r in rows])
    return distances ? merge(table, (distance_km=Float64[r[5] for r in rows],)) : table
end

@testset "Connection distance packing and routing" begin
    origin, target = DEMO_CELLS[1], DEMO_CELLS[3]
    rows = [(1, 2, 10, 100, 0.1), (1, 2, 20, 10, 8.0), (1, 2, 40, 5, 2.0), (2, 3, 50, 5, 3.0)]
    graph = pack_graph(distance_table(rows))
    for (start, km) in ((0, 11.0), (21_000, 5.0))
        result = route_details(graph, origin, start, 60_000)
        @test result.arrival == route_cpu(graph, origin, start, 60_000)
        @test result.arrival[graph.node_id[target]] == 55_000
        @test result.distance_km[graph.node_id[target]] == km
    end
    ties = [(1, 2, 10, 20, 0.5), (1, 2, 20, 10, 9.0), (1, 2, 20, 10, 4.0)]
    canonical = pack_graph(distance_table(ties))
    @test canonical.departure == UInt32[20_000, 86_420_000]
    @test canonical.arrival == UInt32[30_000, 86_430_000]
    @test canonical.distance_km == [4.0, 4.0] # Latest departure wins, then shortest exact tie.
    rng = MersenneTwister(7102)
    expected = pack_graph(distance_table([rows; ties]))
    for _ in 1:4
        shuffled = pack_graph(distance_table(shuffle(rng, [rows; ties])))
        @test all(getfield(shuffled, f) == getfield(expected, f) for f in fieldnames(Graph))
    end
    legacy = pack_graph(distance_table(rows; distances=false))
    @test legacy.distance_km === nothing
    @test all(getfield(legacy, f) == getfield(graph, f) for f in fieldnames(Graph)[1:9])
    for (connections, start, arrival, km) in (
        ([(1, 3, 0, 30, 9.0), (1, 2, 0, 10, 1.0), (2, 3, 10, 20, 1.0)], 0, 30_000, 9.0), # Not the 2 km tie.
        ([(1, 1, 0, 0, 9.0), (1, 2, 0, 0, 0.0), (2, 1, 0, 0, 8.0), (2, 3, 0, 0, 4.0)], 0, 0, 4.0),
        ([(1, 2, 86390, 20, 2.0), (2, 3, 20, 10, 3.0)], 86_380_000, 86_430_000, 5.0))
        g = pack_graph(distance_table(connections))
        result = route_details(g, origin, start, 60_000)
        @test result.arrival[g.node_id[target]] == arrival
        @test result.distance_km[g.node_id[target]] == km
        @test result.distance_km[g.node_id[origin]] == 0.0
    end
    table = distance_table([(1, 2, 0, 0, 0.0)])
    for km in ([NaN], [Inf], [-1.0], Float32[1], Union{Missing,Float64}[1], [missing], Float64[])
        @test_throws ArgumentError pack_graph(merge(table, (distance_km=km,)))
    end
    skipped = distance_table([(1, 2, 0, -1, NaN), (1, 2, 1, 0, 0.0)])
    retained = @test_logs (:warn, r"Skipping 1 of 2 connections") pack_graph(skipped; skip_invalid_durations=true)
    @test retained.distance_km == [0.0, 0.0]
    huge = pack_graph(distance_table([(1, 2, 0, 0, floatmax(Float64)), (2, 3, 0, 0, floatmax(Float64))]))
    @test all(iszero, route_cpu(huge, origin, 0, 0))
    @test_throws ArgumentError route_details(huge, origin, 0, 0)
    mktempdir() do dir
        path = joinpath(dir, "distance.arrow")
        Arrow.write(path, distance_table(rows); file=true, compress=nothing)
        restored = pack_graph(path)
        @test all(getfield(restored, f) == getfield(graph, f) for f in fieldnames(Graph))
    end
end

@testset "Distance/window HTTP and Arrow" begin
    rows = [(1, 2, 120, 0, 2.0), (2, 3, 150, 30, 3.0), (3, 4, 1000, 0, 7.0)]
    graph = pack_graph(distance_table(rows))
    handler = make_handler(graph)
    origin = DEMO_CELLS[1]
    index = "index=$(H3.API.h3ToString(origin))"
    words = "index_lower=$(origin % UInt32)&index_upper=$((origin >> 32) % UInt32)"
    times = "departure_h=0&budget_h=0.03333333333333333"
    window = "window_h=0.03361111111111111&step_h=0.008333333333333333"
    request(query) = handler(HTTP.Request("GET", "/reachable?$query&max_walk_h=0"))
    for encoding in ("string", "split")
        response = request("$index&$times&$window&encoding=$encoding")
        @test response.status == 200
        @test HTTP.header(response, "Content-Type") == "application/vnd.apache.arrow.file"
        for (name, value) in (("Backend", "reference"), ("Distance", "connection-sum-km"), ("Searches", "1"), ("Reused-Samples", "4"))
            @test HTTP.header(response, "X-Router-$name") == value
            @test "X-Router-$name" in strip.(split(HTTP.header(response, "Access-Control-Expose-Headers"), ','))
        end
        @test String(response.body[1:6]) == String(response.body[end-5:end]) == "ARROW1"
        table = Arrow.Table(response.body)
        stream = Arrow.Stream(response.body)
        collect(stream)
        @test stream.compression[] === nothing
        indices = encoding == "string" ? [:index] : [:index_lower, :index_upper]
        @test propertynames(table) == [indices; :value; :elapsed_h; :distance_km; :reachable_elapsed_h; :reachable_fraction; :reachable_samples; :sample_count]
        @test all(!(getproperty(table, f) isa Arrow.DictEncoded) for f in propertynames(table))
        @test all(eltype(getproperty(table, f)) == Float64 for f in (:value, :elapsed_h, :distance_km, :reachable_elapsed_h, :reachable_fraction))
        @test eltype(table.reachable_samples) == eltype(table.sample_count) == UInt32
        @test all(eltype(getproperty(table, f)) == (encoding == "string" ? String : UInt32) for f in indices)
        ids = encoding == "string" ? parse.(UInt64, table.index; base=16) : UInt64.(table.index_lower) .| (UInt64.(table.index_upper) .<< 32)
        @test ids == sort(DEMO_CELLS[1:2]) # Partial and never-reachable cells are excluded.
        at = [findfirst(==(h), ids) for h in DEMO_CELLS[1:2]]
        @test table.elapsed_h[at] == [0.0, 1/60]
        @test table.reachable_elapsed_h[at] == [0.0, 1/60]
        @test table.distance_km[at] == [0.0, 2.0]
        @test table.reachable_fraction[at] == [1.0, 1.0]
        @test table.reachable_samples[at] == UInt32[5, 5]
        @test table.sample_count == fill(UInt32(5), 2)
        @test table.value == table.elapsed_h
        split_input = Arrow.Table(request("$words&$times&$window&encoding=$encoding").body)
        @test all(isequal(getproperty(table, f), getproperty(split_input, f)) for f in propertynames(table))
        point = request("$index&$times&window_h=0&encoding=$encoding")
        point_table = Arrow.Table(point.body)
        @test HTTP.header(point, "X-Router-Backend") == "reference"
        @test propertynames(point_table) == [indices; :value; :elapsed_h; :distance_km]
        @test eltype(point_table.elapsed_h) == Float64
        @test sort(collect(point_table.distance_km)) == [0.0, 2.0]
        legacy = pack_graph(distance_table(rows; distances=false))
        old = make_handler(legacy)
        for suffix in ("", "&window_h=0")
            result = old(HTTP.Request("GET", "/reachable?$index&$times&encoding=$encoding$suffix&max_walk_h=0"))
            @test propertynames(Arrow.Table(result.body)) == [indices; :value; :elapsed_h]
            @test eltype(Arrow.Table(result.body).elapsed_h) == Float64
        end
        unknown = old(HTTP.Request("GET", "/reachable?$index&$times&$window&encoding=$encoding&max_walk_h=0"))
        @test HTTP.header(unknown, "X-Router-Distance") == "unavailable"
        @test count(isnan, Arrow.Table(unknown.body).distance_km) == 1
        @test count(iszero, Arrow.Table(unknown.body).distance_km) == 1
    end
    for suffix in ("window_h=-1", "window_h=1200", "window_h=NaN", "window_h=%ZZ", "window_h=0.0002777777777777778%0A",
                   "window_h=$(typemax(UInt64))", "window_h=0.0002777777777777778&window_h=0.0005555555555555556", "step_h=1e-999", "window_h=0&step_h=1e-10",
                   "window_h=1e-999&step_h=0", "window_h=0.0002777777777777778&step_h=-1", "window_h=0.0002777777777777778&step_h=1200",
                   "window_h=0.0002777777777777778&step_h=Inf", "window_h=0.0002777777777777778&step_h=0.0002777777777777778%0A", "window_h=0.0002777777777777778&step_h=0.0002777777777777778&step_h=0.0005555555555555556")
        @test request("$index&$times&$suffix").status == 400
    end
    absent = "index=$(H3.API.h3ToString(DEMO_CELLS[7]))"
    cases = [("$index&$times&$window", [0.0, 60_000.0], 5),
             ("$index&departure_h=0&budget_h=0&$window", [0.0], 5),
             ("index=$(H3.API.h3ToString(DEMO_CELLS[2]))&$times&$window", [0.0], 5),
             ("$index&$times&window_h=0.03361111111111111", [0.0, 60_000.0], 3),
             ("$index&$times&window_h=0.016944444444444446&step_h=0.008333333333333333", [0.0, 90_000.0], 3),
             ("$index&departure_h=23.999722222222225&budget_h=0.03333333333333333&$window", [0.0], 5),
             ("$absent&$times&window_h=0.03361111111111111", [0.0], 3),
             ("$absent&departure_h=23.999722222222225&budget_h=168&window_h=24&step_h=0.0002777777777777778", [0.0], 86400),
             ("$absent&departure_h=23.999722222222225&budget_h=0&window_h=0.0005555555555555556&step_h=24", [0.0], 1)]
    server = HTTP.serve!(handler, "127.0.0.1", 0; listenany=true, verbose=-1)
    try
        tasks = map(cases) do (query, elapsed, samples)
            @async HTTP.get("http://127.0.0.1:$(HTTP.port(server))/reachable?$query&encoding=string&max_walk_h=0")
        end
        for ((query, elapsed, samples), task) in zip(cases, tasks)
            response = fetch(task)
            @test response.status == 200
            table = Arrow.Table(response.body)
            @test all(==(samples), table.sample_count)
            @test sort(collect(table.elapsed_h)) == elapsed ./ 3_600_000
            if startswith(query, "$absent&")
                @test table.index == [H3.API.h3ToString(DEMO_CELLS[7])]
                @test table.distance_km == table.reachable_elapsed_h == [0.0]
                @test table.reachable_fraction == [1.0]
                @test table.reachable_samples == table.sample_count
            end
        end
    finally
        close(server)
    end
end

@testset "Inferred H3 resolutions" for resolution in (6, 7)
    cells = H3.API.cellToChildren(DEMO_ORIGIN, resolution)
    graph = pack_graph(distance_table([(1, 2, 0, 1, 2.0)]; cells))
    @test graph.resolution == resolution
    @test route_cpu(graph, cells[1], 0, 1000)[graph.node_id[cells[2]]] == 1000
    @test route_details(graph, cells[1], 0, 1000).distance_km[graph.node_id[cells[2]]] == 2.0
    @test route_window(graph, cells[1], 0, 1000, 1).reachable_samples == UInt32[1, 1]
    handler = make_handler(graph)
    for cell in (cells[1], cells[3], DEMO_ORIGIN, UInt64(0))
        response = handler(HTTP.Request("GET", "/reachable?index=$(H3.API.h3ToString(cell))&departure_h=0&budget_h=0.0002777777777777778&window_h=0.0002777777777777778&max_walk_h=0"))
        @test response.status == (cell in cells ? 200 : 400)
        if !(cell in cells)
            @test_throws ArgumentError route_cpu(graph, cell, 0, 1000)
            @test_throws ArgumentError route_details(graph, cell, 0, 1000)
            @test_throws ArgumentError route_window(graph, cell, 0, 1000, 1)
            @test_throws ArgumentError pack_graph(distance_table([(1, 2, 0, 1, 2.0)]; cells=[cells[1], cell]))
        end
    end
end
