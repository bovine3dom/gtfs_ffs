include("calibrate.jl")
using Test, Arrow

@testset "Isolated adapter matches in-process handler output" begin
    h = H3.API.latLngToCell(H3.API.LatLng(deg2rad(48.8566), deg2rad(2.3522)), 8)
    cells = sort!(filter(!iszero, H3.API.gridDisk(h, 1)))
    graph = R.pack_graph((from_h3=cells[1:6], to_h3=cells[2:7],
        departure_ms=UInt32.(28_800_000 .+ (0:5)*60_000), duration_ms=fill(Int64(60_000),6),
        distance_km=fill(0.5,6)))
    population = R._population(cells, Float64.(100:106))
    index = R.prepare_walking(R.WalkingIndex(graph))
    prepared = R._prepare_population(population, index)
    pool = R.PopulationWorkspacePool()
    handler = R.make_handler(graph; population)
    total = 0
    for metric in ("time", "time_distance_quantile", "accessible_population"),
        window in (0, 0.05), mode in MODES, walk in (0, 0.1), encoding in ("split", "string")
        metric == "time_distance_quantile" && window > 0 && mode == "reachable_union" && continue
        url = "/reachable?index=$(string(h; base=16))&departure_h=8&budget_h=0.25&metric=$metric&window_h=$window&window_mode=$mode&max_walk_h=$walk&encoding=$encoding&origin_radius=1"
        expected = handler(HTTP.Request("GET", url))
        @test expected.status == 200
        actual = compute(url, graph, index, population, prepared, pool; body_only=true)
        @test actual == expected.body
        total += 1
    end
    println("Byte-identical Arrow responses: $total")
    url = "/reachable?index=$(string(h; base=16))&departure_h=8&budget_h=0.25&max_walk_h=0"
    direct() = compute(url, graph, index, population, prepared, pool; body_only=true)
    http() = handler(HTTP.Request("GET", url))
    batch(f) = foreach(_->f(), 1:100)
    batch(direct); batch(http)
    open(joinpath(@__DIR__, "adapter-timings.csv"), "w") do io
        println(io, "variant,round,requests,cpu_ms_per_request,wall_ms_per_request,compile_ms")
        for round in 1:5, (variant, f) in (isodd(round) ? (("compute", direct), ("handler", http)) : (("handler", http), ("compute", direct)))
            before = cpu()
            t = @timed batch(f)
            ms = (cpu()-before)*10
            @test t.compile_time == 0
            println(io, join((variant, round, 100, ms, t.time*10, t.compile_time*1000), ','))
        end
    end
end
