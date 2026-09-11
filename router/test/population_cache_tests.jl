module PopulationCacheTests
using Test, HTTP, Arrow
import H3
using ..Reachability
using ..PopulationPackedTests: fixture, MODES
const R = Reachability

@testset "Bounded population origin cache" begin
    (; graph, population, origin, index, limit) = fixture(8)
    moved = first(filter(!=(origin), H3.API.gridDisk(origin, 1)))
    for selected in (index, WalkingIndex(graph)), walk in (0, limit + 1),
            mode in MODES, exclude in (false, true)
        cache = R.PopulationResultCache(graph, population, selected)
        seen = Set{UInt64}()
        options = (; window_ms=101, step_ms=25, window_mode=mode, max_walk_ms=walk,
            exclude_origin_population=exclude)
        for (centre, radius) in ((origin, 2), (origin, 2), (moved, 2), (moved, 3), (origin, 1))
            actual = R._cached_route_population(cache, centre, 0, limit + 100;
                origin_radius=radius, options...)
            fresh = route_population(graph, population, centre, 0, limit + 100;
                walking_index=selected, origin_radius=radius, options...)
            @test actual.h3 == fresh.h3
            @test actual.value ≈ fresh.value
            @test iszero.(actual.value) == iszero.(fresh.value)
            @test actual.cache_hits == length(intersect(seen, actual.h3))
            @test actual.cache_misses == length(setdiff(actual.h3, seen))
            if actual.cache_misses == 0
                @test actual.workers == actual.shared_expansions == actual.query_expansions == 0
            elseif isempty(seen)
                @test actual.shared_expansions == fresh.shared_expansions
            end
            union!(seen, actual.h3)
        end
    end

    cache = R.PopulationResultCache(graph, population, index)
    base = (; window_ms=101, step_ms=25, max_walk_ms=100, window_mode=:mean_intersection)
    query(; kwargs...) = R._cached_route_population(cache, origin, 0, 100; merge(base, (; kwargs...))...)
    @test query().cache_misses == 1
    @test query(window_ms=124, max_walk_ms=200, window_mode="diff_intersection").cache_hits == 1
    for change in ((window_mode=:min_union,), (window_mode=:reachable_union,),
            (exclude_origin_population=true,), (step_ms=24,), (window_ms=126,), (max_walk_ms=0,))
        @test query(; change...).cache_misses == 1
    end
    @test R._cached_route_population(cache, origin, 1, 100; base...).cache_misses == 1
    @test R._cached_route_population(cache, origin, 0, 101; base...).cache_misses == 1
    @test query(window_ms=0, step_ms=0).cache_misses == 1
    @test query(window_ms=1, step_ms=500, window_mode=:min_union).cache_hits == 1
    for invalid in ((origin_radius=-1,), (step_ms=-1,), (window_ms=-1,),
            (window_mode=:invalid,), (max_walk_ms=-1,), (origin_batch_size=65,),
            (prepared_population=0,), (window_ms=Int(R.INF),))
        @test_throws ArgumentError query(; invalid...)
    end
    @test_throws ArgumentError R._cached_route_population(cache, UInt64(0), 0, 100; base...)
    @test_throws ArgumentError R._cached_route_population(cache, origin, -1, 100; base...)
    @test_throws ArgumentError R._cached_route_population(cache, origin, 0, -1; base...)

    for (g, p, w) in ((deepcopy(graph), population, index), (graph, population, WalkingIndex(graph)),
            (graph, R._population(population.h3, population.weights .* 2), index))
        isolated = R.PopulationResultCache(g, p, w)
        actual = R._cached_route_population(isolated, origin, 0, 100; base...)
        @test actual.cache_misses == 1
        @test actual.value ≈ route_population(g, p, origin, 0, 100; walking_index=w, base...).value
    end
    zero = R._population(UInt64[], Float64[])
    tiny = R.PopulationResultCache(graph, zero, index; capacity=2)
    cells = H3.API.gridDisk(origin, 1)[1:3]
    for cell in cells
        @test R._cached_route_population(tiny, cell, 0, 0).cache_misses == 1
    end
    @test length(tiny.totals) == length(tiny.order) == 2
    @test R._cached_route_population(tiny, cells[2], 0, 0).cache_hits == 1
    @test R._cached_route_population(tiny, cells[1], 0, 0).cache_misses == 1
    @test R._cached_route_population(tiny, cells[2], 0, 0).cache_misses == 1 # hits do not refresh FIFO
    @test all(iszero, values(tiny.totals))
    huge = R._population(population.h3, fill(floatmax(Float64), length(population.h3)))
    failed = R.PopulationResultCache(graph, huge, index)
    @test_throws Exception R._cached_route_population(failed, origin, 0, limit;
        origin_radius=2, max_walk_ms=limit)
    @test isempty(failed.totals) && isempty(failed.order)
    for _ in 1:2
        result = R._cached_route_population(tiny, origin, 0, 0; origin_radius=2)
        @test all(iszero, result.value)
        @test length(tiny.totals) == length(tiny.order) == 2
    end

    handler = make_handler(graph; population=zero)
    path = "/reachable?index=$(string(origin; base=16))&departure_h=0&budget_h=0&metric=accessible_population&origin_radius=2"
    for hits in (0, 19)
        response = handler(HTTP.Request("GET", path))
        @test response.status == 200
        @test HTTP.header(response, "X-Router-Cache-Hits") == string(hits)
        @test HTTP.header(response, "X-Router-Cache-Misses") == string(19 - hits)
        @test HTTP.header(response, "X-Router-Origin-Count") == "19"
        @test occursin("X-Router-Cache-Hits", HTTP.header(response, "Access-Control-Expose-Headers"))
        @test isempty(Arrow.Table(response.body).value)
    end
    @test handler(HTTP.Request("GET", path * "&exclude_origin_population=invalid")).status == 400
    @test isempty(HTTP.header(handler(HTTP.Request("GET", replace(path,
        "metric=accessible_population" => "metric=time"))), "X-Router-Cache-Hits"))
end
end
