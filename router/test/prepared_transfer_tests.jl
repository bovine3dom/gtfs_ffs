using Test, Arrow
include("../src/Reachability.jl")
using .Reachability
include("../fixture.jl")

@testset "Resolution transfer delay and prepared walking" begin
    a, b, c = DEMO_CELLS[1:3]
    delay = Reachability.TRIP_CONNECTION_MS[6]
    @test delay == 7_094_946
    @test issorted(reverse(collect(Reachability.TRIP_CONNECTION_MS)))
    mktempdir() do dir
        source = joinpath(dir, "test.arrow")
        Arrow.write(source, merge(fixture_table(), (trip_id=fill("t", 6),)))
        graph = pack_graph(source; trip_aware=false, badajoz_shuttle=true)
        output = joinpath(dir, "prepared")
        prepare_trip_shard_set!(TripShardSet(source, graph), output)
        set = TripShardSet(source, graph; prepared_dir=output)
        rm(source)
        for component in Int32(1):set.component_count
            shard = Reachability._build_trip_shard(set, component)
            @test shard.walking_index.prepared !== nothing
            @test isfile(joinpath(output, "shard_$(component).bin.walking"))
        end
    end
    for gap in (delay - UInt32(1), delay), same in (false, true)
        graph = pack_graph((from_h3=UInt64[a,b], to_h3=UInt64[b,c],
            departure_ms=UInt32[0,1000+gap], duration_ms=Int64[1000,1000],
            trip_id=same ? ["a","a"] : ["a","b"], distance_km=[1.0,2.0]))
        labels = route_cpu(graph, a, 0, Int(delay)+2000)
        @test (labels[graph.node_id[c]] != Reachability.INF) == (same || gap == delay)
        population = Reachability._population(
            [first(H3.API.cellToChildren(c, 8))], [10.0])
        result = route_population(graph, population, a, 0, Int(delay)+2000; max_walk_ms=0)
        @test sum(result.value) == (same || gap == delay ? 10.0 : 0.0)
        mktempdir() do dir
            path = joinpath(dir, "shard.bin")
            Reachability.write_trip_shard(path, graph, 1)
            loaded = Reachability.read_trip_shard(path)
            @test loaded.distance_km == graph.distance_km
            @test route_cpu(loaded,a,0,Int(delay)+2000) == labels
            index = open(Reachability.deserialize, path * ".walking")
            @test index.prepared !== nothing
            @test index.cells == graph.h3
            @test route_walking(loaded,a,0,Int(delay)+2000;
                max_walk_ms=0, walking_index=index).arrival ==
                route_walking(graph,a,0,Int(delay)+2000;max_walk_ms=0).arrival
        end
    end
end
