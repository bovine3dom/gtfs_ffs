using Test, Random, Serialization
include("../src/Reachability.jl")
using .Reachability
import H3
const R = Reachability

@testset "Continuation index" begin
    rng = MersenneTwister(481)
    empty_graph = pack_graph((from_h3=UInt64[],to_h3=UInt64[],departure_ms=UInt32[],
        duration_ms=Int64[],trip_id=String[]))
    empty_index = R.prepare_continuation(empty_graph)
    @test empty_index.offsets == [1]
    @test isempty(empty_index.pairs)
    mktempdir() do dir
        path = joinpath(dir,"empty.bin")
        R.write_trip_shard(path,empty_graph,1)
        R.write_continuation(path)
        @test isempty(R.read_trip_shard(path).continuation.pairs)
    end
    for resolution in (5,6,8), trial in 1:10
        origin = H3.API.latLngToCell(H3.API.LatLng(deg2rad(51.5),deg2rad(-0.1)),resolution)
        cells = H3.API.gridDisk(origin,1)
        table = (from_h3=rand(rng,cells,80),to_h3=rand(rng,cells,80),
            departure_ms=rand(rng,UInt32(0):UInt32(86_399_999),80),
            duration_ms=rand(rng,Int64(0):Int64(3_600_000),80),
            trip_id=string.(rand(rng,1:12,80)),distance_km=rand(rng,80))
        graph = pack_graph(table)
        index = R.prepare_continuation(graph)
        indexed = R._with_continuation(graph,index)
        for node in eachindex(graph.h3), trip in UInt32(0):UInt32(13)
            expected = [e for e in graph.out_ptr[node]:graph.out_ptr[node+1]-1
                if trip in @view graph.trip_id[graph.schedule_ptr[e]:graph.schedule_ptr[e+1]-1]]
            @test collect(R._trip_edges(indexed,node,trip,false)) == expected
        end
        @test_throws ArgumentError R.prepare_continuation(graph; max_bytes=1)
        @test_throws ArgumentError R.prepare_continuation(graph; max_bytes=8length(index.offsets))
        ready = rand(rng,0:86_399_999)
        budget = rand(rng,1:172_800_000)
        @test route_cpu(indexed,origin,ready,budget) == route_cpu(graph,origin,ready,budget)
        population = R._population([first(H3.API.cellToChildren(c,8)) for c in cells],ones(length(cells)))
        for walk in (0,3_600_000)
            a = route_walking(indexed,origin,ready,budget;max_walk_ms=walk)
            b = route_walking(graph,origin,ready,budget;max_walk_ms=walk)
            @test a.h3 == b.h3
            @test a.arrival == b.arrival
            @test isequal(a.distance_km,b.distance_km)
            a = route_population(indexed,population,origin,ready,budget;
                max_walk_ms=walk,origin_radius=1,window_ms=900_000,step_ms=300_000)
            b = route_population(graph,population,origin,ready,budget;
                max_walk_ms=walk,origin_radius=1,window_ms=900_000,step_ms=300_000)
            @test a.h3 == b.h3
            @test a.value ≈ b.value
        end
        trial == 1 || continue
        mktempdir() do dir
            path = joinpath(dir,"shard.bin")
            R.write_trip_shard(path,graph,1)
            @test R.read_trip_shard(path).continuation === nothing
            sidecar = R.write_continuation(path)
            restored = R.read_trip_shard(path)
            @test restored.continuation.pairs == index.pairs
            @test route_cpu(restored,origin,ready,budget) == route_cpu(graph,origin,ready,budget)
            @test R.read_trip_shard(path;continuation=false).continuation === nothing
            @test_throws ArgumentError R.write_continuation(path;max_bytes=1)
            @test R.read_trip_shard(path).continuation.pairs == index.pairs
            open(sidecar,"a") do io
                write(io,UInt8(0))
            end
            @test_throws ArgumentError R.read_trip_shard(path)
            R.write_continuation(path)
            open(path,"a") do io
                write(io,UInt8(0))
            end
            @test_throws ArgumentError R.read_trip_shard(path)
        end
    end
end
