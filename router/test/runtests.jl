using Test, Random, Arrow, HTTP
import H3
import KernelAbstractions as KA
include("../src/Reachability.jl")
using .Reachability
include("../fixture.jl")

if "--backend=oneapi" in ARGS
    @eval import oneAPI
    oneAPI.functional() || error("GPU tests requested but oneAPI is unavailable")
    oneAPI.versioninfo()
end

const P = Int(Reachability.PERIOD)
const INF = Reachability.INF
const START = 28_800_000

@testset "Input validation and packing" begin
    table = fixture_table()
    graph = pack_graph(table)
    @test graph.h3 == sort!(unique([table.from_h3; table.to_h3]))
    @test DEMO_CELLS[6] in graph.h3 # destination-only node
    @test issorted(graph.out_ptr)
    @test graph.out_ptr[end] == length(graph.edge_to) + 1
    @test graph.schedule_ptr[end] == length(graph.departure) + 1
    for edge in eachindex(graph.edge_to)
        range = graph.schedule_ptr[edge]:(graph.schedule_ptr[edge + 1] - 1)
        @test all(>(0), diff(Int64.(graph.departure[range])))
        @test all(>(0), diff(Int64.(graph.arrival[range])))
    end
    for duration in (-1, Int(Reachability.MAX_BUDGET_MS) + 1)
        @test_throws ArgumentError pack_graph(merge(table, (duration_ms=fill(Int64(duration), 6),)))
    end
    @test_throws ArgumentError pack_graph(merge(table, (departure_ms=fill(UInt32(P), 6),)))
    @test_throws ArgumentError pack_graph(merge(table, (duration_ms=UInt32.(table.duration_ms),)))
    @test_throws ArgumentError pack_graph(merge(table, (from_h3=fill(UInt64(0), 6),)))
    @test_throws ArgumentError pack_graph(merge(table, (to_h3=UInt64[],)))
    @test_throws ArgumentError pack_graph((from_h3=table.from_h3,))
    @test_throws ArgumentError pack_graph(merge(table, (duration_ms=Union{Missing,Int64}[missing; table.duration_ms[2:end]],)))
    coarse = H3.API.cellToParent(DEMO_ORIGIN, 4)
    @test_throws ArgumentError pack_graph(merge(table, (from_h3=fill(coarse, 6),)))
    mktempdir() do dir
        for file in (true, false)
            path = joinpath(dir, "edges.arrow")
            Arrow.write(path, table; file)
            restored = pack_graph(path)
            @test restored.h3 == graph.h3
            @test restored.departure == graph.departure
            @test restored.arrival == graph.arrival
        end
    end
end

@testset "Invalid duration exclusion" begin
    valid = fixture_table()
    invalid = (from_h3=fill(DEMO_ORIGIN, 5), to_h3=fill(DEMO_CELLS[7], 5),
               departure_ms=zeros(UInt32, 5),
               duration_ms=Int64[-60_000, -120_000, typemin(Int64), 1_767_311_700_000, typemax(Int64)])
    table = map(vcat, valid, invalid)
    @test_throws r"3 negative, 2 above seven days" pack_graph(table)
    graph = @test_logs (:warn, r"Skipping 5 of 11 connections") pack_graph(table; skip_invalid_durations=true)
    reference = pack_graph(valid)
    @test graph.h3[graph.edge_from] == reference.h3[reference.edge_from]
    @test graph.h3[graph.edge_to] == reference.h3[reference.edge_to]
    @test graph.departure == reference.departure
    @test graph.arrival == reference.arrival
    labels = route_cpu(graph, DEMO_ORIGIN, START, 7P)
    expected = route_cpu(reference, DEMO_ORIGIN, START, 7P)
    @test labels[[graph.node_id[h] for h in reference.h3]] == expected
    @test labels[graph.node_id[DEMO_CELLS[7]]] == INF
    empty = @test_logs (:warn, r"Skipping 5 of 5 connections") pack_graph(invalid; skip_invalid_durations=true)
    @test isempty(empty.edge_to)
    @test empty.out_ptr == fill(Int32(1), length(empty.h3) + 1)
    @test route_cpu(empty, DEMO_ORIGIN, 0, 0)[empty.node_id[DEMO_ORIGIN]] == 0
    @test_throws ArgumentError pack_graph(merge(invalid, (departure_ms=fill(UInt32(P), 5),)); skip_invalid_durations=true)
    boundaries = (from_h3=fill(DEMO_ORIGIN, 2), to_h3=DEMO_CELLS[2:3],
                  departure_ms=zeros(UInt32, 2), duration_ms=Int64[0, 7P])
    boundary_graph = @test_logs pack_graph(boundaries; skip_invalid_durations=true)
    boundary_labels = route_cpu(boundary_graph, DEMO_ORIGIN, 0, 7P)
    @test boundary_labels[boundary_graph.node_id[DEMO_CELLS[2]]] == 0
    @test boundary_labels[boundary_graph.node_id[DEMO_CELLS[3]]] == 7P
    mktempdir() do dir
        path = joinpath(dir, "invalid-durations.arrow")
        Arrow.write(path, table)
        @test_throws ArgumentError pack_graph(path)
        restored = @test_logs (:warn, r"Skipping 5 of 11 connections") pack_graph(path; skip_invalid_durations=true)
        @test restored.h3 == graph.h3
        @test restored.departure == graph.departure
    end
end

@testset "Profiles versus raw connections" begin
    rng = MersenneTwister(573)
    for sample in 1:20
        # Include duplicates, overtaking and an overnight slow/fast alternative.
        departures = UInt32[0, 10, 10, 20, P - 10, rand(rng, 0:(P - 1), 20)...]
        durations = Int64[10, 100, 80, 5, 2P, rand(rng, 0:(3P), 20)...]
        table = (from_h3=fill(DEMO_CELLS[1], length(departures)),
                 to_h3=fill(DEMO_CELLS[2], length(departures)),
                 departure_ms=departures, duration_ms=durations)
        graph = pack_graph(table)
        @test all(>(0), diff(Int64.(graph.departure)))
        @test all(>(0), diff(Int64.(graph.arrival)))
        for ready in [0, 10, 11, 20, P - 20, P - 1, P, 7P, rand(rng, 0:(7P), 20)...]
            cutoff = ready + rand(rng, 0:(7P))
            expected = INF
            for (d, duration) in zip(departures, durations)
                departure = (ready ÷ P) * P + Int(d)
                departure < ready && (departure += P)
                arrival = departure + duration
                arrival <= cutoff && (expected = min(expected, UInt32(arrival)))
            end
            @test Reachability.next_arrival(graph.schedule_ptr, graph.departure,
                  graph.arrival, 1, UInt32(ready), UInt32(cutoff)) == expected
        end
    end
end

@testset "Reference routing" begin
    graph = pack_graph(fixture_table())
    labels = route_cpu(graph, DEMO_ORIGIN, START, 3_600_000)
    @test labels[graph.node_id[DEMO_CELLS[1]]] == START
    @test labels[graph.node_id[DEMO_CELLS[2]]] == START + 1_200_000
    @test labels[graph.node_id[DEMO_CELLS[3]]] == START + 2_400_000
    @test labels[graph.node_id[DEMO_CELLS[4]]] == START + 3_600_000
    @test labels[graph.node_id[DEMO_CELLS[5]]] == INF
    @test labels[graph.node_id[DEMO_CELLS[6]]] == INF
    short = route_cpu(graph, DEMO_ORIGIN, START, 3_599_999)
    @test short[graph.node_id[DEMO_CELLS[4]]] == INF
    @test all(==(INF), route_cpu(graph, DEMO_CELLS[7], START, 7P))
    @test_throws ArgumentError route_cpu(graph, DEMO_ORIGIN, P, 0)
end

include("window_tests.jl")
include("catchup_tests.jl")
include("window_gpu_tests.jl")
include("distance_http_tests.jl")
include("window_engine_http_tests.jl")
include("metric_tests.jl")
include("hours_tests.jl")
include("kernel_tests.jl")
include("shuttle_tests.jl")

@testset "HTTP and Arrow: $(typeof(backend))" for backend in kernel_backends
    graph = pack_graph(fixture_table())
    router = KernelRouter(graph, backend)
    handler = make_handler(graph; route=(h, t, b) -> route_kernel!(router, h, t, b))
    origin = "index=85075dd7fffffff"
    times = "departure_h=8&budget_h=1"
    lower, upper = DEMO_ORIGIN % UInt32, (DEMO_ORIGIN >> 32) % UInt32
    words = "index_lower=$lower&index_upper=$upper"
    request(query) = handler(HTTP.Request("GET", "/reachable?$query&max_walk_h=0"))
    high_cell = first(filter(h -> h % UInt32 > typemax(Int32), DEMO_CELLS))
    high_words = "index_lower=$(high_cell % UInt32)&index_upper=$((high_cell >> 32) % UInt32)"
    high_split = Arrow.Table(request("$high_words&$times&encoding=string").body)
    high_string = Arrow.Table(request("index=$(H3.API.h3ToString(high_cell))&$times&encoding=string").body)
    @test high_split.index == high_string.index
    @test high_split.elapsed_h == high_string.elapsed_h
    responses = Dict(encoding => request("$origin&$times&encoding=$encoding")
                     for encoding in ("string", "split"))
    for encoding in ("string", "split")
        response = responses[encoding]
        @test response.status == 200
        @test HTTP.header(response, "Content-Type") == "application/vnd.apache.arrow.file"
        @test HTTP.header(response, "Access-Control-Allow-Origin") == "*"
        @test String(response.body[1:6]) == "ARROW1"
        @test String(response.body[(end - 5):end]) == "ARROW1"
        table = Arrow.Table(response.body)
        split_input = Arrow.Table(request("$words&$times&encoding=$encoding").body)
        @test all(collect(getproperty(table, key)) == collect(getproperty(split_input, key))
                  for key in propertynames(table))
        @test eltype(table.elapsed_h) == Float64
        @test eltype(table.value) == Float64
        @test table.value == table.elapsed_h
        ids = if encoding == "string"
            @test propertynames(table) == [:index, :value, :elapsed_h]
            @test eltype(table.index) == String
            parse.(UInt64, table.index; base=16)
        else
            @test propertynames(table) == [:index_lower, :index_upper, :value, :elapsed_h]
            @test eltype(table.index_lower) == UInt32
            @test eltype(table.index_upper) == UInt32
            UInt64.(table.index_lower) .| (UInt64.(table.index_upper) .<< 32)
        end
        @test ids == sort(DEMO_CELLS[1:4])
        @test table.elapsed_h[findfirst(==(DEMO_ORIGIN), ids)] == 0
    end
    for query in (
        "", "$origin", "$times", "$words", "$origin&$words&$times",
        "index_lower=$lower&$times", "index_upper=$upper&$times",
        "index_lower=-1&index_upper=$upper&$times",
        "index_lower=4294967296&index_upper=$upper&$times",
        "index=123&$times", "index=000000000000000&$times",
        "index=84075ddffffffff&$times", "index=%ZZ&$times",
        "index=%&$times", "index=%0&$times", "$origin%0A&$times",
        "index_lower=$lower%0A&index_upper=$upper&$times",
        "$origin&departure_h=8%0A&budget_h=0.0002777777777777778", "$origin&departure_h=8&budget_h=0.0002777777777777778%0A",
        "$origin&departure_h=24&budget_h=0.0002777777777777778", "$origin&departure_h=8:00:00&budget_h=0.0002777777777777777778",
        "$origin&departure_h=8&budget_h=-1", "$origin&departure_h=8&budget_h=NaN",
        "$origin&departure_h=8&budget_h=168.00027777777777", "$origin&departure_h=8&budget_h=$(typemax(UInt64))",
        "$origin&$times&encoding=uint64", "$origin&$times&foo=1",
        "$origin&$times&$origin", "$origin&$times&budget_h=0.0002777777777777778")
        @test request(query).status == 400
    end
    @test handler(HTTP.Request("POST", "/reachable")).status == 405
    @test handler(HTTP.Request("OPTIONS", "/reachable")).status == 204
    @test handler(HTTP.Request("GET", "/missing")).status == 404
    outside = H3.API.h3ToString(DEMO_CELLS[7])
    isolated = Arrow.Table(request("index=$outside&$times&encoding=string").body)
    @test collect(isolated.index) == [outside]
    @test collect(isolated.elapsed_h) == [0]
    zero = Arrow.Table(request("$origin&departure_h=8&budget_h=0").body)
    @test collect(zero.elapsed_h) == [0]

    server = HTTP.serve!(handler, "127.0.0.1", 0; listenany=true, verbose=-1)
    try
        url = "http://127.0.0.1:$(HTTP.port(server))/reachable"
        tasks = map(1:8) do i
            query = isodd(i) ? "$origin&$times" : "index=$(H3.API.h3ToString(DEMO_CELLS[5]))&$times"
            @async HTTP.get("$url?$query&max_walk_h=0")
        end
        for (i, task) in enumerate(tasks)
            result = fetch(task)
            @test result.status == 200
            @test sort(collect(Arrow.Table(result.body).value)) == (isodd(i) ? [0, 1/3, 2/3, 1] : [0, 1/12])
        end
    finally
        close(server)
    end
    if haskey(ENV, "ROUTER_FRONTEND")
        mktempdir() do dir
            for (encoding, response) in responses
                write(joinpath(dir, "$encoding.arrow"), response.body)
            end
            @test success(`node $(joinpath(@__DIR__, "frontend.mjs")) $(ENV["ROUTER_FRONTEND"]) $dir`)
        end
    end
end

include("walking_geometry_tests.jl")
include("walking_tests.jl")
include("walking_catchup_tests.jl")
include("walking_http_tests.jl")
include("websocket_tests.jl")
