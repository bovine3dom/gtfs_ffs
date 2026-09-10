module PopulationQueueTests
using Test
import H3

module Probe
include("../src/Reachability.jl")
end
const R = Probe.Reachability
const claimed = Int[]
const owners = IdDict{Task,Any}()
const guard = ReentrantLock()
const active = Threads.Atomic{Int}(0)
const completed = Threads.Atomic{Int}(0)
const fail = Ref(false)
const passed_wave = Threads.Atomic{Bool}(false)

function R._population_tile!(w, graph::R.Graph, network, population, sources, ids,
                             ready, budget, step, samples, limit, mode, own_ids)
    lock(guard) do
        get!(owners, current_task(), w) === w || error("worker changed workspace")
        append!(claimed, ids)
    end
    Threads.atomic_add!(active, 1)
    try
        if fail[] && first(ids) == 1
            error("worker test failure")
        elseif !fail[] && first(ids) == 1 && Threads.nthreads(:default) > 1
            timedwait(() -> passed_wave[], 10) == :ok || error("tile wave did not advance")
        else
            first(ids) > 16Threads.nthreads(:default) && (passed_wave[] = true)
            sleep(0.001)
        end
        Threads.atomic_add!(completed, 1)
        return (; value=Float64.(ids), shared=length(ids), separate=sum(ids))
    finally
        Threads.atomic_add!(active, -1)
    end
end

@testset "Whole-tile queue ownership and joined failures" begin
    origin = H3.API.latLngToCell(H3.API.LatLng(0.5, 0.1), 7)::UInt64
    cells = sort!(H3.API.gridDisk(origin, 10))
    graph = R.pack_graph((from_h3=cells, to_h3=cells,
        departure_ms=zeros(UInt32, length(cells)), duration_ms=zeros(Int64, length(cells))))
    population = R._population([first(H3.API.cellToChildren(origin, 8))], [1.0])
    index = R.prepare_walking(R.WalkingIndex(graph); max_walk_ms=0)
    query() = R.route_population(graph, population, origin, 0, 100;
        walking_index=index, max_walk_ms=0, origin_radius=10, window_ms=96, step_ms=1,
        origin_batch_size=16)
    result = query()
    @test result.value == Float64.(1:331)
    @test result.shared_expansions == 331
    @test result.query_expansions == sum(1:331)
    @test sort(claimed) == collect(1:331)
    @test length(owners) == result.workers
    @test length(Set(objectid(w) for w in values(owners))) == result.workers
    @test active[] == 0
    @test Threads.nthreads(:default) == 1 || passed_wave[]
    empty!(claimed)
    empty!(owners)
    fail[] = true
    @test_throws CompositeException query()
    @test active[] == 0
    snapshot = (copy(claimed), completed[])
    sleep(0.02)
    @test (claimed, completed[]) == snapshot
    @test length(claimed) < 331
    fail[] = false
    @test query().value == result.value
end
end
