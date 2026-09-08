module WalkingCatchupTests

using Test, Random, H3
include("../src/Reachability.jl")
using .Reachability

const DAY = 86_400_000
const FIELDS = (:h3, :elapsed_sum_ms, :elapsed_ms, :reachable_elapsed_ms,
                :reachable_samples, :distance_km, :sample_count)
cell_at(lat, lon, res) = H3.API.latLngToCell(H3.API.LatLng(deg2rad(lat), deg2rad(lon)), res)::UInt64
disk(h, k) = sort!(filter(!iszero, H3.API.gridDisk(h, k)))
at(result, cell, field) = getproperty(result, field)[findfirst(==(cell), result.h3)]

function walk(a, b)
    p, q = H3.API.cellToLatLng.((min(a, b), max(a, b)))
    km = H3.Lib.greatCircleDistanceKm(Ref(p), Ref(q))
    return (ms=ceil(Int64, 720_000km), km=km)
end

function chain(res)
    a = cell_at(51.5, -0.1, res)
    for b in disk(a, 1), c in disk(b, 1)
        (a == b || b == c || a == c) && continue
        seconds = cld(max(walk(a, b).ms, walk(b, c).ms), 1000)
        walk(a, c).ms > 1000seconds && return a, b, c, seconds
    end
    error("could not construct a two-hop walking chain")
end

# Keep schedules raw so packing, overtaking and duplicate ties are exercised too.
function raw_table(cells, rows; distances=true)
    table = (from_h3=UInt64[cells[r[1]] for r in rows], to_h3=UInt64[cells[r[2]] for r in rows],
             departure_ms=UInt32[r[3] for r in rows], duration_ms=Int64[r[4] for r in rows])
    return distances ? merge(table, (distance_km=Float64[r[5] for r in rows],)) : table
end

function check_window(graph, origin, ready, budget, window; kwargs...)
    expected = route_window_walking(graph, origin, ready, budget, window; kwargs...)
    @testset "chunk=$chunk_size workers=$workers" for chunk_size in (1, 2, 8, 64), workers in (1, 4)
        actual = route_window_walking_cached(graph, origin, ready, budget, window;
                                             chunk_size, workers, kwargs...)
        for field in FIELDS
            @test isequal(getproperty(actual, field), getproperty(expected, field))
        end
        @test actual.backend == "walking_catchup"
        @test actual.searches == actual.sample_count == actual.full_searches + actual.repair_searches
        @test actual.reused_samples == 0
        @test 1 <= actual.workers <= min(workers, Threads.nthreads(:default), actual.full_searches)
        @test actual.profile_lookups >= 0 && actual.routing_expansions >= 0
        chunk_size == 1 && @test actual.repair_searches == 0
    end
    return expected
end

@testset "Walking catch-up" begin
    @testset "Random raw schedules res$res trial=$trial" for res in (7, 9), trial in 1:12
        rng = MersenneTwister(91300 + 100res + trial)
        a, b, c, seconds = chain(res)
        remote = cell_at(51.7, 0.3, res)
        cells = [a, b, c, shuffle(rng, disk(remote, 1))[1:3]...]
        span = 1000seconds
        ready = rand(rng, (0, DAY - span, DAY - 5))
        rows = [(i, mod1(i + 1, 6), mod(ready + rand(rng, 0:3span), DAY),
                 rand(rng, (0, 1, span, DAY + 1)), rand(rng) * 10) for i in 1:6]
        append!(rows, [(rand(rng, 1:6), rand(rng, 1:6), mod(ready + rand(rng, 0:3span), DAY),
                       rand(rng, 0:3span), rand(rng) * 10) for _ in 1:18])
        append!(rows, [(1, 2, ready, 3span, 9.0), (1, 2, mod(ready + 1, DAY), 0, 2.0),
                       (1, 2, mod(ready + 1, DAY), 0, 1.0), (2, 1, mod(ready + 1, DAY), 0, 3.0),
                       (2, 2, mod(ready + 1, DAY), 0, 4.0)])
        graph = pack_graph(raw_table(cells, shuffle(rng, rows); distances=isodd(trial)))
        origin = trial % 3 == 0 ? first(setdiff(disk(a, 1), cells)) : a
        step = max(1, span ÷ 5)
        check_window(graph, origin, ready, rand(rng, (0, span ÷ 2, 3span, DAY + span, 7DAY)),
                     8step + 1; step_ms=step, max_walk_ms=1000seconds, walking_index=WalkingIndex(graph))
    end

    @testset "Earlier walk, later eligibility and equal-time km res$res" for res in (7, 9)
        a, b, c, seconds = chain(res)
        ab, bc = walk(a, b), walk(b, c)
        for delay in (0, 10)
            graph = pack_graph(raw_table([a, b, c], [(1, 2, 2, ab.ms + delay, 7.0),
                                                    (3, 3, DAY ÷ 2, 0, 0.0)]))
            budget = ab.ms + 2 + delay + bc.ms
            result = check_window(graph, a, 0, budget, 3; step_ms=1, max_walk_ms=1000seconds)
            @test at(result, c, :reachable_samples) == 3
            @test at(result, c, :distance_km) == 7.0 + bc.km
            @test at(result, c, :elapsed_sum_ms) == 3budget - 3
            last = route_walking(graph, a, 2, budget; max_walk_ms=1000seconds)
            @test at(last, b, :arrival) == ab.ms + 2
            @test at(last, b, :distance_km) == (delay == 0 ? 7.0 : ab.km)
        end
        # A self-transit resets eligibility even when its arrival only ties the walk.
        graph = pack_graph(raw_table([a, b, c], [(2, 2, ab.ms + 2, 0, 3.0),
                                                (3, 3, DAY ÷ 2, 0, 0.0)]))
        result = check_window(graph, a, 0, ab.ms + 2 + bc.ms, 4; step_ms=1, max_walk_ms=1000seconds)
        @test !(a in graph.h3)
        @test at(result, c, :reachable_samples) == 3
        @test at(result, c, :distance_km) == ab.km + 3.0 + bc.km
    end

    @testset "Off-graph access, geographic egress and shrinking midnight cutoff res$res" for res in (7, 9)
        a, b, _, seconds = chain(res)
        remote = cell_at(51.7, 0.3, res)
        egress = argmin(h -> walk(remote, h).ms, setdiff(disk(remote, 1), [remote]))
        ab, de = walk(a, b), walk(remote, egress)
        seconds = max(seconds, cld(de.ms, 1000))
        graph = pack_graph(raw_table([b, remote], [(1, 2, 0, 17, 4.0)]))
        budget = ab.ms + 17 + de.ms
        step = 1 + ab.ms ÷ 8
        result = check_window(graph, a, DAY - ab.ms - 1, budget + 2step, ab.ms + 3;
                              step_ms=step, max_walk_ms=1000seconds)
        @test !(a in graph.h3) && !(egress in graph.h3)
        @test 0 < at(result, egress, :reachable_samples) < result.sample_count
        @test at(result, egress, :reachable_elapsed_ms) < at(result, egress, :elapsed_ms)
        @test at(result, egress, :distance_km) == ab.km + 4.0 + de.km
        # The first sample misses by exactly 1 ms; repair must mask the later label.
        boundary = check_window(graph, a, DAY - ab.ms - 1, budget, 2; step_ms=1, max_walk_ms=1000seconds)
        @test at(boundary, egress, :reachable_samples) == 1
        @test at(boundary, egress, :elapsed_sum_ms) == 2budget
        @test at(boundary, egress, :reachable_elapsed_ms) == budget
        check_window(graph, a, DAY - 1, 2DAY, 13; step_ms=2, max_walk_ms=1000seconds)
    end

    @testset "Zero cycles, disabled walking, empty graphs and NaN means" begin
        a, b, c, seconds = chain(9)
        cells = sort([a, b, c])
        rows = [(3, 3, 2, 0, 9.0), (3, 2, 2, 0, 2.0), (2, 1, 2, 0, 4.0),
                (1, 2, 2, 0, 8.0), (2, 3, 2, 0, 7.0), (1, 1, 2, 0, 5.0)]
        for distances in (false, true), max_walk_s in (0, seconds)
            graph = pack_graph(raw_table(cells, rows; distances))
            result = check_window(graph, cells[3], 0, 2, 4; step_ms=1, max_walk_ms=1000max_walk_s)
            @test at(result, cells[1], :reachable_samples) == 3
            @test isequal(at(result, cells[1], :distance_km), distances ? 6.0 : NaN)
        end
        empty = pack_graph(raw_table(UInt64[], []; distances=false))
        origin = cell_at(51.5, -0.1, empty.resolution)
        result = check_window(empty, origin, DAY - 1, 7DAY, 65; step_ms=1, max_walk_ms=0)
        @test result.h3 == [origin] && result.distance_km == [0.0]
        check_window(empty, origin, 0, 0, 1; step_ms=DAY, max_walk_ms=3600000)
        neighbor = first(setdiff(disk(origin, 1), [origin]))
        radius = cld(walk(origin, neighbor).ms, 1000)
        geographic = check_window(empty, origin, 0, 1000radius, 3; step_ms=1, max_walk_ms=1000radius)
        @test neighbor in geographic.h3
        for departure in (0, 1)
            duration = departure == 0 ? 0 : walk(a, b).ms
            graph = pack_graph(raw_table([a, b], [(1, 2, departure, duration, 0.0)]; distances=false))
            result = check_window(graph, a, 0, walk(a, b).ms, 3; step_ms=1, max_walk_ms=1000seconds)
            @test at(result, b, :reachable_samples) == 3
            @test isnan(at(result, b, :distance_km))
            @test at(result, a, :distance_km) == 0.0
            first = route_walking(graph, a, 0, walk(a, b).ms; max_walk_ms=1000seconds)
            @test isnan(at(first, b, :distance_km)) == (departure == 0)
            @test isfinite(at(route_walking(graph, a, 2, walk(a, b).ms; max_walk_ms=1000seconds), b, :distance_km))
        end
    end

    @testset "Reused downstream connections still replay changed prefix km" begin
        a, b, _, seconds = chain(9)
        ab = walk(a, b)
        cells = [b; [cell_at(52.0 + i, 0.3, 9) for i in 1:6]]
        rows = [(1, 2, ab.ms + i, 10, 1.0 + i / 7) for i in 0:64]
        append!(rows, [(i, i + 1, ab.ms + 100 + 20i, 10, i / 3) for i in 2:6])
        graph = pack_graph(raw_table(cells, rows))
        index = WalkingIndex(graph)
        budget = ab.ms + 300 + 1000seconds
        check_window(graph, a, 0, budget, 65; step_ms=1, max_walk_ms=1000seconds, walking_index=index)
        early = route_walking(graph, a, 0, budget; max_walk_ms=1000seconds)
        late = route_walking(graph, a, 64, budget; max_walk_ms=1000seconds)
        @test !(a in graph.h3)
        @test [at(early, h, :arrival) for h in cells[3:end]] == [at(late, h, :arrival) for h in cells[3:end]]
        @test all(h -> at(early, h, :distance_km) != at(late, h, :distance_km), cells[3:end])
        for workers in (1, 4)
            independent = route_window_walking_cached(graph, a, 0, budget, 65;
                step_ms=1, max_walk_ms=1000seconds, walking_index=index, chunk_size=1, workers)
            cached = route_window_walking_cached(graph, a, 0, budget, 65;
                step_ms=1, max_walk_ms=1000seconds, walking_index=index, chunk_size=64, workers)
            @test cached.profile_lookups * 2 < independent.profile_lookups
            @test cached.routing_expansions < independent.routing_expansions
            @test cached.full_searches > 1 && cached.repair_searches > 0
        end
    end

    @testset "Tentative and eligibility-only overflow; recovery after a worker error" begin
        a, b, c, seconds = chain(9)
        d = cell_at(51.7, 0.3, 9)
        huge = floatmax(Float64)
        # b first proposes an overflowing route to d, which c would later beat.
        rows = [(1, 2, 0, 1, huge), (1, 2, 1, 1, 0.0), (1, 3, 0, 2, 0.0),
                (1, 3, 1, 1, 0.0), (2, 4, 2, 8, huge), (3, 4, 2, 1, 0.0)]
        transient = pack_graph(raw_table([a, b, c, d], rows))
        # c has an earlier A label from walking, but its first E label overflows.
        ac = walk(a, c).ms
        eligible = pack_graph(raw_table([a, b, c], [(1, 2, 0, 1, huge), (2, 3, 2, ac, huge)]))
        for (graph, budget, limit) in ((transient, 20, seconds), (eligible, ac + 2, cld(ac, 1000)))
            @test_throws r"accumulated route distance" route_window_walking(graph, a, 0, budget, 4;
                                                                          step_ms=1, max_walk_ms=1000limit)
            for chunk_size in (1, 2, 8, 64), workers in (1, 4)
                @test_throws r"accumulated route distance" route_window_walking_cached(graph, a, 0, budget, 4;
                    step_ms=1, max_walk_ms=1000limit, chunk_size, workers)
            end
        end
        # Keep both graph and index resident across a failing wave and later requests.
        function recover(graph)
            index, snapshot = WalkingIndex(graph), deepcopy(graph)
            expected = route_window_walking_cached(graph, a, 1, 20, 4;
                step_ms=1, max_walk_ms=1000seconds, walking_index=index, chunk_size=1, workers=4)
            for _ in 1:3
                @test_throws ArgumentError route_window_walking_cached(graph, a, 0, 20, 4;
                    step_ms=1, max_walk_ms=1000seconds, walking_index=index, chunk_size=1, workers=4)
                actual = route_window_walking_cached(graph, a, 1, 20, 4;
                    step_ms=1, max_walk_ms=1000seconds, walking_index=index, chunk_size=1, workers=4)
                @test isequal(actual, expected)
            end
            @test all(f -> isequal(getfield(graph, f), getfield(snapshot, f)), fieldnames(Graph))
            check_window(graph, a, 1, 20, 4; step_ms=1, max_walk_ms=1000seconds, walking_index=index)
        end
        recover(transient)
    end

    @testset "Shared geometry snapshots and recovery" begin
        a, b, c, seconds = chain(9)
        graph = pack_graph(raw_table([a, b, c], [(1, 2, 0, 0, 1.0), (2, 3, 0, 0, 1.0)]))
        index, shared = WalkingIndex(graph), Reachability.WalkingGeometryCache()
        topologies = [Reachability.WalkingTopology(index, 1000seconds, shared) for _ in 1:8]
        for geographic in (false, true)
            tasks = [Threads.@spawn Reachability._walking_hops(t, a; geographic) for t in topologies]
            results = fetch.(tasks)
            @test all(hops -> hops === results[1], results)
            expected = geographic ? walking_cells(index, a, 1000seconds) : walking_neighbors(index, a, 1000seconds)
            @test results[1] == expected
            @test shared.entries[(geographic, a)][2][] == (UInt32(1000seconds), expected)
        end
        partial = Reachability._walking_hops(topologies[1], b; geographic=true, limit=500seconds)
        snapshot = copy(partial)
        larger = Reachability._walking_hops(topologies[2], b; geographic=true)
        @test partial == snapshot && partial !== larger
        @test Reachability._walking_hops(topologies[1], b; geographic=true, limit=250seconds) === partial
        @test Reachability._walking_hops(topologies[1], b; geographic=true) === larger
        @test_throws ArgumentError Reachability._walking_hops(topologies[1], b; geographic=true,
                                                             limit=Int(Reachability.MAX_BUDGET_MS) + 1)
        @test Reachability._walking_hops(topologies[3], b; geographic=true) === larger
        @test_throws ArgumentError Reachability._walking_hops(topologies[1], UInt64(0); geographic=true)
        @test Reachability._walking_hops(topologies[1], c; geographic=true) == walking_cells(index, c, 1000seconds)
        @test length(shared.entries) == 5
    end

    @testset "Invalid arguments" begin
        a, b = chain(9)
        graph = pack_graph(raw_table([a, b], [(1, 2, 0, 0, 1.0)]))
        for kwargs in ((chunk_size=0,), (chunk_size=-1,), (workers=0,), (workers=-1,),
                       (max_walk_ms=-1,), (max_walk_ms=604801000,), (max_walk_ms=typemax(UInt64),),
                       (step_ms=0,), (step_ms=-1,))
            @test_throws ArgumentError route_window_walking_cached(graph, a, 0, 0, 1; kwargs...)
        end
        for (ready, budget, window) in ((-1, 0, 1), (DAY, 0, 1), (0, -1, 1),
                                        (0, 7DAY + 1, 1), (0, typemax(UInt64), 1),
                                        (0, 0, 0), (0, 0, DAY + 1), (0, 0, 86_401))
            @test_throws ArgumentError route_window_walking_cached(graph, a, ready, budget, window; step_ms=1)
        end
        for origin in (UInt64(0), H3.API.cellToParent(a, 7))
            @test_throws ArgumentError route_window_walking_cached(graph, origin, 0, 0, 1)
        end
        other = WalkingIndex(pack_graph(raw_table([a], [(1, 1, 0, 0, 0.0)])))
        @test_throws ArgumentError route_window_walking(graph, a, 0, 0, 1; walking_index=other)
        @test_throws ArgumentError route_window_walking_cached(graph, a, 0, 0, 1; walking_index=other)
    end
end

include("walking_output_tests.jl")
include("straight_distance_tests.jl")

end # module
