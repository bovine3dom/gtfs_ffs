module PopulationWorkspaceTests
using Test
import H3
using ..Reachability
const R = Reachability
workspaces(pool) = isempty(pool.idle) ? Any[] : last(pool.idle).workspaces

function fixture()
    origin = H3.API.latLngToCell(H3.API.LatLng(0.5, 0.1), 8)::UInt64
    cells = sort!(H3.API.gridDisk(origin, 6))
    graph = pack_graph((from_h3=[cells; cells[1]], to_h3=[cells; cells[2]],
        departure_ms=zeros(UInt32, length(cells) + 1), duration_ms=zeros(Int64, length(cells) + 1)))
    outputs = sort!(H3.API.gridDisk(origin, 9))
    population = R._population(outputs, [i % 7 == 0 ? 2e-30 : (i % 11) / 4 for i in eachindex(outputs)])
    index = prepare_walking(WalkingIndex(graph); max_walk_ms=0)
    return (; graph, population, origin, index)
end

@testset "Workspace budget arithmetic" begin
    @test population_workspace_estimate(10, 20, 0, 3) == 3 * (20*8 + 40*8 + 10*sizeof(Int))
    @test population_workspace_estimate(10, 20, 16, 3) == 3 * (20*8 + 40*8 + 10*sizeof(Int) + 20*12 + 16*20*4)
    @test population_workspace_estimate(big(10), UInt(20), 64, 0) == 0
    for count in (-1, big(-1))
        @test_throws ArgumentError population_workspace_estimate(count, 0, 0, 1)
    end
    for count in (typemax(Int), big(2)^200, typemax(UInt128))
        @test_throws PopulationMemoryError population_workspace_estimate(count, count, count, 2)
    end
    for budget in (0, -1, big(typemax(Int)) + 1, typemax(UInt128))
        @test_throws ArgumentError PopulationWorkspacePool(; max_bytes=budget)
    end
    pool = PopulationWorkspacePool()
    @test population_workspace_stats(pool) == (; estimated_bytes=0, retained_bytes=0,
        reused_workers=0, workers=0, max_bytes=8*1024^3)
end

@testset "Request reuse, layout replacement, and independent results" begin
    (; graph, population, origin, index) = fixture()
    pool = PopulationWorkspacePool()
    query(; pool=pool, graph=graph, index=index, population=population, origin=origin, radius=7, kwargs...) =
        route_population(graph, population, origin, 0, 100; walking_index=index,
            max_walk_ms=0, origin_radius=radius, workspace_pool=pool, kwargs...)
    old = query()
    snapshot = deepcopy(old)
    first_worker = first(workspaces(pool))
    again = query()
    @test first(workspaces(pool)) === first_worker
    @test again.workspace_reused_workers == again.workers
    @test again.workspace_estimated_bytes <= pool.max_bytes
    @test again.workspace_retained_bytes == Base.summarysize(workspaces(pool))
    @test !hasproperty(query(pool=nothing), :workspace_estimated_bytes)
    for batch in (16, 64, 16), mode in (:mean_intersection, :max_intersection,
            :diff_intersection, :min_union, :diff_union, :reachable_union), exclude in (false, true)
        options = (; origin_batch_size=batch, window_ms=96, step_ms=1,
            window_mode=mode, exclude_origin_population=exclude)
        fresh = query(; pool=nothing, options...)
        actual = query(; options...)
        @test actual.h3 == fresh.h3
        @test actual.value == fresh.value
        @test iszero.(actual.value) == iszero.(fresh.value)
        @test (actual.shared_expansions, actual.query_expansions) ==
            (fresh.shared_expansions, fresh.query_expansions)
        @test size(first(workspaces(pool)).arrivals, 1) == batch
        for w in workspaces(pool)
            @test isempty(w.queue) && isempty(w.pending)
            @test all(iszero, w.heads) && all(iszero, w.range_pending)
            @test all(==(R.INF), w.range_queued)
        end
    end
    @test old == snapshot
    @test old.value !== again.value && old.h3 !== again.h3
    range_worker = first(workspaces(pool))
    query()
    @test first(workspaces(pool)) !== range_worker
    @test isempty(first(workspaces(pool)).arrivals)
    held = first(workspaces(pool))
    reached, coverage, arrivals = held.reached, held.coverage, held.arrivals
    @test query(radius=8).value == query(pool=nothing, radius=8).value
    @test held.reached === reached && held.coverage === coverage && held.arrivals === arrivals
    moved = first(filter(!=(origin), H3.API.gridDisk(origin, 1)))
    @test query(origin=moved).value == query(pool=nothing, origin=moved).value
    @test first(workspaces(pool)) === held
    capacity = length(held.reached)
    query()
    @test length(held.reached) == capacity
    @test query(radius=6).value == query(pool=nothing, radius=6).value
    @test length(held.reached) == capacity
    replacement = deepcopy(graph)
    replacement.arrival .= 200
    changed = query(graph=replacement)
    @test first(workspaces(pool)) !== held
    @test last(pool.idle).graph === replacement
    @test changed.value == query(pool=nothing, graph=replacement).value
    @test changed.value != old.value
    held = first(workspaces(pool))
    other_index = prepare_walking(WalkingIndex(replacement); max_walk_ms=0)
    query(graph=replacement, index=other_index)
    @test first(workspaces(pool)) !== held
    held = first(workspaces(pool))
    empty!(population.schedule_hints)
    query(graph=replacement, index=other_index)
    @test first(workspaces(pool)) !== held
    empty!(pool)
    @test isempty(pool.idle) && isempty(pool.active)
    @test population_workspace_stats(pool).retained_bytes == 0
end

@testset "Budget reduction, cache hits, fallback, and errors" begin
    (; graph, population, origin, index) = fixture()
    pool = PopulationWorkspacePool()
    options = (; walking_index=index, max_walk_ms=0, origin_radius=6, origin_batch_size=16,
        window_ms=96, step_ms=1)
    query(pool; kwargs...) = route_population(graph, population, origin, 0, 100;
        merge(options, (; kwargs...))..., workspace_pool=pool)
    baseline = query(nothing)
    prepared = R._prepare_population(population, index)
    per_worker = population_workspace_estimate(prepared.node_count, length(prepared.weights), 16, 1)
    small = PopulationWorkspacePool(max_bytes=per_worker)
    result = query(small)
    @test result.workers == 1
    @test result.value == baseline.value
    @test result.workspace_estimated_bytes == per_worker
    @test result.workspace_retained_bytes == 0 # Dynamic capacity exceeds the fixed-only budget.
    @test isempty(workspaces(small))
    for mode in (:mean_intersection, :max_intersection, :diff_intersection,
            :min_union, :diff_union, :reachable_union), exclude in (false, true)
        flags = (; window_mode=mode, exclude_origin_population=exclude)
        reduced, fresh = query(small; flags...), query(nothing; flags...)
        @test reduced.workers == 1
        @test reduced.value == fresh.value
        @test (reduced.shared_expansions, reduced.query_expansions) ==
            (fresh.shared_expansions, fresh.query_expansions)
    end
    @test_throws PopulationMemoryError query(PopulationWorkspacePool(max_bytes=per_worker - 1))
    query(pool)
    selected = graph.h3[1:16]
    direct = R._route_population_origins(graph, index, population, prepared,
        R._population_rollup(population, 8), selected, UInt32(0), 100, 1, 96, UInt32(0),
        :mean_intersection, 16, false; workspace_pool=pool)
    @test direct.workers == direct.workspace_reused_workers == length(workspaces(pool)) == 1
    @test direct.workspace_estimated_bytes == per_worker
    @test direct.value == baseline.value[1:16]
    query(pool)
    held = first(workspaces(pool))
    @test_throws PopulationMemoryError query(pool; origin_radius=1_000_000)
    oversized = try
        query(pool; origin_radius=typemax(Cint))
        nothing
    catch error
        error
    end
    @test oversized isa PopulationMemoryError
    r = UInt128(typemax(Cint))
    @test oversized.estimated_bytes == 16 * (3r * (r + 1) + 1)
    @test first(workspaces(pool)) === held
    cache = R.PopulationResultCache(graph, population, index)
    cached() = R._cached_route_population(cache, origin, 0, 100; workspace_pool=pool,
        max_walk_ms=0, origin_radius=6, origin_batch_size=16, window_ms=96, step_ms=1)
    cached()
    held = first(workspaces(pool))
    retained = population_workspace_stats(pool).retained_bytes
    hit = cached()
    @test hit.workers == hit.workspace_estimated_bytes == hit.workspace_reused_workers == 0
    @test hit.workspace_retained_bytes == retained
    @test first(workspaces(pool)) === held
    @test population_workspace_stats(pool).workers == 0
    zero = R._population(UInt64[], Float64[])
    static = route_population(graph, zero, origin, 0, 0; walking_index=index, workspace_pool=pool)
    @test static.workspace_estimated_bytes == static.workspace_reused_workers == static.workers == 0
    @test first(workspaces(pool)) === held
    query(pool; max_walk_ms=1)
    @test first(workspaces(pool)) === held # Fallback must not discard other layouts.
    query(pool)
    query(pool; walking_index=WalkingIndex(graph))
    @test !isempty(workspaces(pool))
    query(pool)
    huge = R._population(population.h3, fill(floatmax(Float64), length(population.h3)))
    @test_throws Exception route_population(graph, huge, origin, 0, 100;
        merge(options, (; window_ms=0))..., workspace_pool=pool)
    @test !isempty(workspaces(pool)) # An error discards only its own lease.
    @test query(pool).value == baseline.value
    results = fetch.([Threads.@spawn query(pool) for _ in 1:4])
    @test all(r -> r.value == baseline.value, results)
    @test all(r -> 0 <= r.workspace_reused_workers <= r.workers, results)
    @test population_workspace_stats(pool).retained_bytes <= pool.max_bytes
end

module OwnershipProbe
include("../src/Reachability.jl")
end
const P = OwnershipProbe.Reachability
const probe_pool = P.PopulationWorkspacePool()
const guard = ReentrantLock()
const active = Ref(0)
const buffers = IdDict{Any,Nothing}()
const peak = Ref(0)
const fail = Ref(false)
const throwing = Channel{Nothing}(1)
const release = Channel{Nothing}(1000)

function P._population_tile!(w, graph::P.Graph, network, population, sources, ids,
        ready, budget, step, samples, limit, mode, own_ids)
    failing = fail[]
    lock(guard) do
        haskey(buffers, w) && error("overlapping workspace ownership")
        buffers[w] = nothing
        active[] += 1
        peak[] = max(peak[], active[])
    end
    try
        if failing && first(ids) == 1
            timedwait(() -> lock(() -> active[] >= min(Threads.nthreads(:default), 8), guard), 10) == :ok ||
                error("workers did not enter")
            put!(throwing, nothing)
            error("worker failure")
        elseif failing
            take!(release)
        else
            sleep(0.01)
        end
        return (; value=Float64.(ids), shared=length(ids), separate=length(ids))
    finally
        lock(guard) do
            active[] -= 1
            delete!(buffers, w)
        end
    end
end

@testset "Explicit pool ownership and joined worker errors" begin
    (; graph, population, origin) = fixture()
    g = P.pack_graph((from_h3=graph.h3, to_h3=graph.h3,
        departure_ms=zeros(UInt32, length(graph.h3)), duration_ms=zeros(Int64, length(graph.h3))))
    p = P._population(population.h3, population.weights)
    index = P.prepare_walking(P.WalkingIndex(g); max_walk_ms=0)
    query() = P.route_population(g, p, origin, 0, 100; walking_index=index,
        workspace_pool=probe_pool, max_walk_ms=0, origin_radius=6, origin_batch_size=16)
    results = fetch.([Threads.@spawn query() for _ in 1:3])
    @test all(r -> r.value == Float64.(1:127), results)
    @test active[] == 0
    @test peak[] > 1
    fail[] = true
    first_request = Threads.@spawn try query() catch e; e end
    take!(throwing)
    fail[] = false
    next_request = Threads.@spawn try query() catch e; e end
    if Threads.nthreads(:default) > 1
        @test timedwait(() -> istaskdone(first_request), 0.03) == :timed_out
        @test timedwait(() -> istaskdone(next_request), 10) == :ok
    end
    fail[] = false
    for _ in 2:results[1].workers
        put!(release, nothing)
    end
    @test fetch(first_request) isa CompositeException
    next_result = fetch(next_request)
    @test next_result.value == results[1].value
    @test next_result.workspace_reused_workers <= next_result.workers
    @test active[] == 0
    @test isempty(probe_pool.active) && isempty(buffers)
    @test query().value == results[1].value
end
end
