using Test
include("../src/Reachability.jl")
using .Reachability
include("../fixture.jl")
const R = Reachability

@testset "Sparse state IDs and bounded workspace reuse" begin
    pool = R.TripWorkspacePool(;limit=1024^2)
    first_ws = Ref{Any}()
    R._with_trip_workspace(UInt64,3,1;pool) do ws
        first_ws[] = ws
        key = R._trip_state_key(Int32(3),UInt32(100_000_000))
        @test R._trip_id!(ws,key) == 1
        @test R._trip_id!(ws,key) == 1
        @test length(ws.keys) == length(ws.arrival) == 1
        ws.arrival[1] = 7; ws.distance[1] = 4.0
        push!(ws.queue,UInt32(7),UInt32(1))
    end
    @test 0 < pool.bytes <= pool.limit
    R._with_trip_workspace(UInt64,8,1;pool) do ws
        @test ws === first_ws[]
        @test isempty(ws.ids) && isempty(ws.keys) && isempty(ws.queue)
        @test length(ws.best) == 8
        @test all(==(R.INF),ws.best)
        @test ws.queue.last == 0
        R._with_trip_workspace(UInt64,2,1;pool) do other
            @test other !== ws
        end
    end
    @test_throws ErrorException R._with_trip_workspace(UInt128,3,7;pool) do ws
        id = R._trip_id!(ws,UInt128(999))
        ws.settled[id] = 1
        error("injected failure")
    end
    R._with_trip_workspace(UInt128,1,1;pool) do ws
        @test isempty(ws.ids) && isempty(ws.settled)
        @test length(ws.best) == 1
        @test isempty(ws.heads) && isempty(ws.event_state) && isempty(ws.free_events)
    end
    R._with_trip_workspace(UInt128,1,1;pool) do ws
        id = R._trip_id!(ws,UInt128(1))
        ws.heads[id] = 1
        append!(ws.event_time,UInt32.(1:40))
        append!(ws.event_next,[UInt32.(2:40); UInt32(0)])
        stats = TripRouteStats()
        @test R._pending_event(ws,id,UInt32(40),stats) == 40
        @test ws.indexed_pending[id]
        @test stats.pending_probe_peak == 32
        @test length(ws.pending_index) == 40
        @test R._pending_event(ws,id,UInt32(1),stats) == 1
        @test R._pending_event(ws,id,UInt32(100),stats) == 0
    end
    @test pool.bytes == sum(last,pool.idle)
    @test pool.bytes <= pool.limit
    for limit in (0,4096)
        small = R.TripWorkspacePool(;limit)
        R._with_trip_workspace(UInt64,1000,1;pool=small) do ws
            for key in UInt64(1):UInt64(1000)
                R._trip_id!(ws,key)
            end
        end
        @test isempty(small.idle)
        @test small.bytes == 0
    end
end

@testset "Concurrent leases and owned route results" begin
    pool = R.TripWorkspacePool(;limit=1024^2)
    entered = Channel{Any}(4)
    release = Channel{Nothing}(4)
    tasks = [Threads.@spawn R._with_trip_workspace(UInt64,10,1;pool) do ws
        put!(entered,ws)
        take!(release)
    end for _ in 1:4]
    active = [take!(entered) for _ in 1:4]
    @test length(unique(objectid.(active))) == 4
    @test pool.bytes == 0 && isempty(pool.idle)
    foreach(_ -> put!(release,nothing),1:4)
    foreach(fetch,tasks)
    @test pool.bytes == sum(last,pool.idle) <= pool.limit
    graph = pack_graph(merge(fixture_table(),(trip_id=fill("trip",6),)))
    expected = route_cpu(graph,DEMO_ORIGIN,28_800_000,10_800_000)
    tasks = [Threads.@spawn route_cpu(graph,DEMO_ORIGIN,28_800_000,10_800_000) for _ in 1:12]
    @test all(result -> result == expected, fetch.(tasks))
    owned = copy(expected)
    route_cpu(graph,DEMO_ORIGIN,0,0)
    @test expected == owned
    a,b = DEMO_CELLS[1:2]
    tied = pack_graph((from_h3=UInt64[a,a],to_h3=UInt64[b,b],
        departure_ms=UInt32[0,0],duration_ms=Int64[1000,1000],
        trip_id=["short","long"],distance_km=[1.0,9.0]))
    @test route_details(tied,a,0,1000).distance_km[tied.node_id[b]] == 1.0
    R._with_trip_workspace(UInt64,1,1) do ws
        for key in UInt64(1):UInt64(10_000)
            R._trip_id!(ws,key)
        end
    end
    @test route_details(tied,a,0,1000).distance_km[tied.node_id[b]] == 1.0
end
