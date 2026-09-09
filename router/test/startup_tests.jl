using Logging

@testset "Synthetic server warmup" begin
    logger = Test.TestLogger()
    count = with_logger(logger) do
        warmup_server()
    end
    @test count == 222
    record = only(filter(r -> r.message == "Synthetic routing warmup", logger.logs))
    @test record.kwargs[:queries] == count
    @test last(logger.logs).message == "Startup complete: Compiling routing and Arrow responses"
    @test !isdefined(Reachability, :KernelRouter)
    graph = pack_graph(Reachability._warmup_table())
    result = route_window_cached(graph, first(graph.h3), 0, 3_960_000, 129 * 60_000)
    @test result.searches == 129
    @test result.full_searches == 3
    @test result.workers == min(3, Threads.nthreads(:default))
end

@testset "Startup profile determinism" begin
    rng = MersenneTwister(912)
    for res in 5:7, distances in (false, true), shuttle in (false, true)
        cells = [H3.API.latLngToCell(H3.API.LatLng(deg2rad(51.5), deg2rad(lon)), res) for lon in (-0.1, 0.1, 0.3)]
        table = (from_h3=rand(rng, cells, 200), to_h3=rand(rng, cells, 200),
                 departure_ms=rand(rng, UInt32[0, 10, 20, P - 1], 200),
                 duration_ms=rand(rng, Int64[-1, 0, 10, 20, P, 7P, 7P + 1], 200))
        distances && (table = merge(table, (distance_km=rand(rng, [0.0, -0.0, 1.0, 10.0], 200),)))
        snapshot = deepcopy(table)
        mktempdir() do dir
            path = joinpath(dir, "chunks.arrow")
            Arrow.write(path, table; file=false)
            Arrow.append(path, table)
            doubled = map(vcat, table, table)
            graph = @test_logs (:warn, r"Skipping") min_level=Logging.Warn pack_graph(doubled;
                skip_invalid_durations=true, badajoz_shuttle=shuttle)
            restored = @test_logs (:warn, r"Skipping") min_level=Logging.Warn pack_graph(path;
                skip_invalid_durations=true, badajoz_shuttle=shuttle, progress=true)
            for field in fieldnames(Graph)
                @test isequal(getfield(graph, field), getfield(restored, field))
            end
            distances && @test reinterpret(UInt64, graph.distance_km) == reinterpret(UInt64, restored.distance_km)
            if shuttle
                extra = Reachability._badajoz_shuttle(res)
                doubled = map(vcat, doubled, NamedTuple{keys(doubled)}(extra))
            end
            pairs = sort!(unique([(doubled.from_h3[i], doubled.to_h3[i]) for i in eachindex(doubled.from_h3)
                                 if 0 <= doubled.duration_ms[i] <= Int64(INF) - 1 - P - doubled.departure_ms[i]]))
            @test collect(zip(graph.h3[graph.edge_from], graph.h3[graph.edge_to])) == pairs
            # Independent original two-day sort, including stable equal-key and signed-zero ties.
            for (edge, (from, to)) in enumerate(pairs)
                profile = Tuple{UInt32,UInt32,Int}[]
                for i in eachindex(doubled.from_h3)
                    (doubled.from_h3[i], doubled.to_h3[i]) == (from, to) || continue
                    0 <= doubled.duration_ms[i] <= Int64(INF) - 1 - P - doubled.departure_ms[i] || continue
                    d, a = doubled.departure_ms[i], doubled.departure_ms[i] + UInt32(doubled.duration_ms[i])
                    push!(profile, (d, a, i), (d + UInt32(P), a + UInt32(P), i))
                end
                sort!(profile; by=c -> (c[1], -Int64(c[2]), distances ? -doubled.distance_km[c[3]] : 0.0))
                retained = eltype(profile)[]
                best = INF
                for c in reverse(profile)
                    if c[2] < best
                        push!(retained, c)
                        best = c[2]
                    end
                end
                reverse!(retained)
                at = graph.schedule_ptr[edge]:(graph.schedule_ptr[edge + 1] - 1)
                @test graph.departure[at] == first.(retained)
                @test graph.arrival[at] == getindex.(retained, 2)
                if distances
                    expected = [doubled.distance_km[c[3]] for c in retained]
                    @test reinterpret(UInt64, graph.distance_km[at]) == reinterpret(UInt64, expected)
                end
            end
        end
        @test isequal(table, snapshot)
    end
end

@testset "Startup progress and workers" begin
    graph = @test_logs pack_graph(fixture_table())
    bare = WalkingIndex(graph)
    @test_logs prepare_walking(bare)
    logger = Test.TestLogger()
    prepared = with_logger(logger) do
        prepare_walking(bare; progress=true)
    end
    record = only(filter(r -> r.message == "Preparing resident walking adjacency", logger.logs))
    @test record.kwargs[:workers] == min(Threads.nthreads(:default), length(graph.h3))
    serial = prepare_walking(bare; workers=1)
    for field in (:geographic, :graph, :output), column in (:offsets, :targets, :durations, :distances)
        @test isequal(getfield(getfield(prepared.prepared, field), column), getfield(getfield(serial.prepared, field), column))
    end
    @test prepared.prepared.output_cells == serial.prepared.output_cells
    @test prepared.prepared.output_id == serial.prepared.output_id
    for n in (0, 12_800)
        with_logger(Test.TestLogger()) do
            Reachability._startup_stage(true, "Counted batches"; total=n) do meter
                @test isnothing(meter) == !(stderr isa Base.TTY)
                @sync for _ in 1:(n ÷ 64)
                    Threads.@spawn Reachability._startup_advance(meter, 64)
                end
                isnothing(meter) || @test meter.counter == n
            end
        end
    end
    logger = Test.TestLogger()
    with_logger(logger) do
        @test_throws ArgumentError Reachability._startup_stage(true, "Failing stage"; total=10) do _
            throw(ArgumentError("test failure"))
        end
    end
    @test [r.message for r in logger.logs] == ["Startup: Failing stage"]
end
