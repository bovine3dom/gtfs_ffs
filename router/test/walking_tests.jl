module WalkingTests

using Test, Random, H3
include("../src/Reachability.jl")
using .Reachability

const DAY = 86_400_000
cell_at(lat, lon, res) = H3.API.latLngToCell(H3.API.LatLng(deg2rad(lat), deg2rad(lon)), res)::UInt64
disk(h, k) = filter(!iszero, H3.API.gridDisk(h, k))

# Fixture times are raw milliseconds, not packed/pruned schedule entries.
function raw_table(cells, rows; distances=true)
    table = (from_h3=UInt64[cells[r[1]] for r in rows], to_h3=UInt64[cells[r[2]] for r in rows],
             departure_ms=UInt32[r[3] for r in rows], duration_ms=Int64[r[4] for r in rows])
    return distances ? merge(table, (distance_km=Float64[r[5] for r in rows],)) : table
end

function walk(a, b)
    p, q = H3.API.cellToLatLng.((min(a, b), max(a, b)))
    km = H3.Lib.greatCircleDistanceKm(Ref(p), Ref(q))
    return (ms=ceil(Int64, 720_000km), km=km)
end

function chain(res)
    a = cell_at(51.5, -0.1, res)
    for b in sort(disk(a, 1)), c in sort(disk(b, 1))
        (a == b || b == c || a == c) && continue
        seconds = cld(max(walk(a, b).ms, walk(b, c).ms), 1000)
        walk(a, c).ms > 1000seconds && return a, b, c, seconds
    end
    error("could not construct a two-hop walking chain")
end

"""Bellman-Ford over (cell, last-edge-was-transit), using only raw rows and H3 centres."""
function brute_route(table, origin, ready, budget, seconds)
    sources = sort!(unique([table.from_h3; table.to_h3; origin]))
    limit, cutoff = min(1000seconds, budget), ready + budget
    # Generous local disks are an independent geographic candidate enumeration.
    # Check their outer rings so fixture changes cannot silently clip the oracle.
    candidates = UInt64[]
    for h in sources
        outer = disk(h, 8)
        @test all(c -> walk(h, c).ms > limit, setdiff(outer, disk(h, 7)))
        append!(candidates, outer)
    end
    cells = sort!(unique(candidates))
    hops = [(a, b, walk(a, b).ms) for a in sources for b in cells
            if a != b && 0 < walk(a, b).ms <= limit]
    never = typemax(Int64)
    labels = Dict((h, state) => never for h in cells for state in (false, true))
    labels[(origin, true)] = ready
    for iteration in 1:(2length(cells) + 1)
        changed = false
        for i in eachindex(table.from_h3), state in (false, true)
            t = labels[(table.from_h3[i], state)]
            t == never && continue
            departure = Int64(table.departure_ms[i])
            departure += max(0, cld(t - departure, DAY)) * DAY
            arrival = departure + table.duration_ms[i]
            key = (table.to_h3[i], true)
            if arrival <= cutoff && arrival < labels[key]
                labels[key] = arrival
                changed = true
            end
        end
        for (a, b, duration) in hops
            t = labels[(a, true)]
            t == never && continue
            if t + duration <= cutoff && t + duration < labels[(b, false)]
                labels[(b, false)] = t + duration
                changed = true
            end
        end
        if !changed
            reached = filter(h -> min(labels[(h, false)], labels[(h, true)]) != never, cells)
            return (h3=reached, arrival=[min(labels[(h, false)], labels[(h, true)]) for h in reached])
        end
    end
    error("two-state oracle did not converge")
end

at(result, cell) = findfirst(==(cell), result.h3)
arrival_at(result, cell) = isnothing(at(result, cell)) ? typemax(UInt32) : result.arrival[at(result, cell)]
km_at(result, cell) = result.distance_km[at(result, cell)]

function check_oracle(table, origin, ready, budget, seconds)
    expected = brute_route(table, origin, ready, budget, seconds)
    actual = route_walking(pack_graph(table), origin, ready, budget; max_walk_s=seconds)
    @test actual.h3 == expected.h3
    @test actual.arrival == expected.arrival
    @test issorted(actual.h3) && allunique(actual.h3)
    @test arrival_at(actual, origin) == ready
    @test km_at(actual, origin) == 0.0
    return actual
end

@testset "Walking routing" begin
    @testset "Raw-schedule two-state oracle res$res" for res in (7, 9)
        rng = MersenneTwister(7300 + res)
        a, b, c, seconds = chain(res)
        remote = cell_at(51.7, 0.3, res)
        pool = unique([a; b; c; disk(a, 2); disk(remote, 1)])
        for trial in 1:16
            cells = [a; b; c; shuffle(rng, setdiff(pool, [a, b, c]))[1:3]]
            budget = 3seconds * 1000
            ready = isodd(trial) ? 0 : DAY - seconds * 1000
            rows = [(i, i == 6 ? 1 : i + 1, mod(ready + rand(rng, 0:budget), DAY),
                     rand(rng, 0:budget), rand(rng) * 10) for i in 1:6]
            append!(rows, [(rand(rng, 1:6), rand(rng, 1:6),
                           mod(ready + rand(rng, 0:budget), DAY), rand(rng, 0:budget), rand(rng) * 10)
                          for _ in 1:14])
            # Raw overtaking, duplicate profiles and a zero-duration cycle.
            append!(rows, [(1, 2, mod(ready, DAY), budget, 9.0),
                           (1, 2, mod(ready + 1, DAY), 0, 2.0),
                           (1, 2, mod(ready + 1, DAY), 0, 1.0),
                           (2, 1, mod(ready + 1, DAY), 0, 3.0)])
            origin = trial % 3 == 0 ? first(setdiff(disk(a, 1), cells)) : a
            table = raw_table(cells, rows; distances=isodd(trial))
            actual = check_oracle(table, origin, ready, budget, seconds)
            permuted = raw_table(cells, shuffle(rng, rows); distances=isodd(trial))
            @test isequal(actual, route_walking(pack_graph(permuted), origin, ready, budget; max_walk_s=seconds))
            graph = pack_graph(table)
            zero = route_walking(graph, origin, ready, budget; max_walk_s=0)
            transit = route_details(graph, origin, ready, budget)
            reached = findall(!=(typemax(UInt32)), transit.arrival)
            @test zero.h3 == sort!(unique([graph.h3[reached]; origin]))
            @test [arrival_at(zero, h) for h in graph.h3] == transit.arrival
            @test isequal([km_at(zero, graph.h3[i]) for i in reached], transit.distance_km[reached])
        end
    end

    @testset "Access, terminal geography, eligibility and exact cutoff res$res" for res in (7, 9)
        a, b, c, seconds = chain(res)
        limit = 1000seconds
        remote = cell_at(51.7, 0.3, res)
        egress = first(setdiff(disk(remote, 1), [remote]))
        ab, bc, de = walk(a, b), walk(b, c), walk(remote, egress)
        seconds = max(seconds, cld(de.ms, 1000))
        # Keep the forbidden two-walk endpoint outside the per-hop limit.
        @test walk(a, c).ms > 1000seconds
        cells = [a, b, c, remote]
        dormant = [(1, 2, div(DAY, 2), 0, 0.0), (2, 3, div(DAY, 2), 0, 0.0)]
        blocked = check_oracle(raw_table(cells, dormant), a, 0, ab.ms + bc.ms, seconds)
        @test arrival_at(blocked, b) == ab.ms
        @test km_at(blocked, b) == ab.km
        @test !(c in blocked.h3) # No walk->walk through graph vertex b.
        offgraph = raw_table(cells, [(2, 3, div(DAY, 2), 0, 0.0)])
        geographic = check_oracle(offgraph, a, 0, ab.ms + bc.ms, seconds)
        @test !(a in pack_graph(offgraph).h3)
        @test !(c in geographic.h3)
        only_remote = raw_table(cells, [(4, 4, div(DAY, 2), 0, 0.0)])
        terminal = check_oracle(only_remote, a, 0, ab.ms + bc.ms, seconds)
        @test b in terminal.h3 && !(b in pack_graph(only_remote).h3)
        @test !(c in terminal.h3) # Nor through geographic-only b.

        access = raw_table(cells, [(2, 4, ab.ms, 17, 4.0)])
        cutoff = ab.ms + 17 + de.ms
        result = check_oracle(access, a, 0, cutoff, seconds)
        @test arrival_at(result, remote) == ab.ms + 17
        @test arrival_at(result, egress) == cutoff
        @test isapprox(km_at(result, egress), ab.km + 4 + de.km)
        @test !(egress in pack_graph(access).h3) # Egress from a destination-only graph vertex.
        @test !(egress in route_walking(pack_graph(access), a, 0, cutoff - 1; max_walk_s=seconds).h3)
        for budget in (ab.ms - 1, ab.ms)
            direct = route_walking(pack_graph(only_remote), a, 0, budget; max_walk_s=seconds)
            @test (b in direct.h3) == (budget == ab.ms)
        end
        @test !(b in route_walking(pack_graph(only_remote), a, 0, limit * 2; max_walk_s=fld(ab.ms - 1, 1000)).h3)

        for delay in (0, 10)
            later = raw_table(cells, [(1, 2, 0, ab.ms + delay, 7.0), (2, 3, div(DAY, 2), 0, 0.0)])
            result = check_oracle(later, a, 0, ab.ms + delay + bc.ms, seconds)
            @test arrival_at(result, b) == ab.ms
            @test km_at(result, b) == (delay == 0 ? 7.0 : ab.km)
            @test arrival_at(result, c) == ab.ms + delay + bc.ms
            @test isapprox(km_at(result, c), 7 + bc.km) # Use the transit label's km, not the earlier walk's.
        end
        reset = raw_table(cells, [dormant; (2, 2, ab.ms, 0, 3.0)])
        result = check_oracle(reset, a, 0, ab.ms + bc.ms, seconds)
        @test arrival_at(result, c) == ab.ms + bc.ms
        @test isapprox(km_at(result, c), ab.km + 3 + bc.km)

        midnight = raw_table(cells, [(2, 4, 0, 17, 4.0)])
        result = check_oracle(midnight, a, DAY - ab.ms, cutoff, seconds)
        @test arrival_at(result, remote) == DAY + 17
        @test arrival_at(result, egress) == DAY + 17 + de.ms
        missing = check_oracle(raw_table(cells, [(2, 4, ab.ms, 17, 0.0)]; distances=false), a, 0, cutoff, seconds)
        @test isfinite(km_at(missing, b)) && km_at(missing, b) == ab.km
        @test isnan(km_at(missing, remote)) && isnan(km_at(missing, egress))
    end

    @testset "Transit ties, zero cycles, validation and overflow" begin
        a, b, c, seconds = chain(9)
        cells = [a, b, c]
        rows = [(1, 2, 10, 20, 0.5), (1, 2, 20, 10, 9.0), (1, 2, 20, 10, 4.0),
                (1, 3, 0, 40, 9.0), (2, 3, 30, 10, 1.0)]
        rng = MersenneTwister(7341)
        for _ in 1:6
            result = check_oracle(raw_table(cells, shuffle(rng, rows)), a, 0, 40, seconds)
            @test km_at(result, b) == 4.0 # Latest departure, then shortest identical connection.
            @test km_at(result, c) == 9.0 # Chosen itinerary, not distance-optimal arrival ties.
        end
        cycle = raw_table(cells, [(1, 1, 0, 0, 9.0), (1, 2, 0, 0, 2.0),
                                   (2, 1, 0, 0, 8.0), (2, 3, 0, 0, 4.0), (3, 2, 0, 0, 7.0)])
        result = check_oracle(cycle, a, 0, 0, seconds)
        @test all(iszero, result.arrival)
        @test km_at(result, c) == 6.0
        huge = pack_graph(raw_table(cells, [(1, 2, 0, 0, floatmax(Float64)), (2, 3, 0, 0, floatmax(Float64))]))
        @test_throws r"accumulated route distance" route_walking(huge, a, 0, 0; max_walk_s=seconds)
        @test_throws r"accumulated route distance" route_window_walking(huge, a, 0, 0, 1; max_walk_s=seconds)
        graph = pack_graph(cycle)
        for bad in (-1, 604801, typemax(UInt64))
            @test_throws ArgumentError route_walking(graph, a, 0, 0; max_walk_s=bad)
        end
        for (ready, budget) in ((-1, 0), (DAY, 0), (0, -1), (0, typemax(UInt64)))
            @test_throws ArgumentError route_walking(graph, a, ready, budget; max_walk_s=0)
        end
        @test_throws ArgumentError route_walking(graph, UInt64(0), 0, 0)
        @test_throws ArgumentError route_walking(graph, H3.API.cellToParent(a, 7), 0, 0)
        @test_throws ArgumentError route_walking(graph, a, 0, 0; walking_index=WalkingIndex(pack_graph(raw_table([a], [(1, 1, 0, 0, 0.0)]))))
        @test_throws Reachability.WalkingLimitError route_walking(graph, a, 0, 0; max_cells=2)
        @test length(route_walking(graph, a, 0, 0; max_cells=3).h3) == 3
        # A valid maximum budget must not wrap; impossible long connections stay absent.
        long = pack_graph(raw_table(cells, [(1, 2, DAY - 1, 7DAY, 1.0), (2, 3, DAY - 1, 7DAY, 1.0)]))
        result = route_walking(long, a, DAY - 1, 7DAY; max_walk_s=0)
        @test arrival_at(result, b) == 8DAY - 1
        @test !(c in result.h3)
    end

    @testset "Geometry cache and work limits" begin
        a, b, c, seconds = chain(9)
        graph = pack_graph(raw_table([a, b, c], [(1, 2, 0, 0, 1.0), (2, 3, 0, 0, 1.0)]))
        index = WalkingIndex(graph)
        topology = Reachability.WalkingTopology(index, 1000seconds)
        full = Reachability._walking_route_at(graph, topology, a, UInt32(0), UInt32(2000seconds))
        for budget in (0, 1000seconds, 2000seconds)
            cached = Reachability._walking_route_at(graph, topology, a, UInt32(0), UInt32(budget))
            @test isequal(cached, route_walking(graph, a, 0, budget; max_walk_s=seconds))
        end
        saturated = Reachability.WalkingTopology(index, 1000seconds)
        saturated.cached_hops = Reachability.WALK_MAX_CANDIDATES
        @test isequal(full, Reachability._walking_route_at(graph, saturated, a, UInt32(0), UInt32(2000seconds)))
        @test isempty(saturated.neighbors) && isempty(saturated.coverage)
        topology.remaining_work = 0
        @test_throws Reachability.WalkingLimitError Reachability._walking_route_at(graph, topology, a, UInt32(0), UInt32(0))
        @test_throws Reachability.WalkingLimitError Reachability._walking_hops(topology, a; geographic=true)
        @test_throws Reachability.WalkingLimitError walking_neighbors(index, a; work=topology)
        @test_throws Reachability.WalkingLimitError walking_cells(index, a; work=topology)
    end

    @testset "Window union and independent chronological means" begin
        for res in (7, 9), distances in (false, true), ready in (0, DAY - 30_000)
            a, b, c, seconds = chain(res)
            remote = cell_at(51.7, 0.3, res)
            cells = [a, b, c, remote]
            ab = walk(a, b).ms
            budget, window, step = ab + 150_000, 181_000, 60_000
            rows = [(2, 4, mod(ready + ab + 60_000, DAY), 0, 2.0),
                    (2, 4, mod(ready + ab + 300_000, DAY), 0, 8.0)]
            graph = pack_graph(raw_table(cells, rows; distances))
            samples = collect(ready:step:(ready + window - 1))
            points = [route_walking(graph, a, mod(t, DAY), budget; max_walk_s=seconds) for t in samples]
            result = route_window_walking(graph, a, ready, budget, window; step_ms=step, max_walk_s=seconds)
            union_cells = sort!(unique(reduce(vcat, getproperty.(points, :h3))))
            @test result.h3 == union_cells
            @test result.sample_count == length(samples) == result.searches
            @test result.reused_samples == 0 # Walking changes boarding availability within an origin profile.
            @test result.backend == "walking_reference" && result.workers == 1
            for (i, h) in enumerate(union_cells)
                reached = [j for j in eachindex(points) if h in points[j].h3]
                elapsed = [Int64(arrival_at(points[j], h)) - mod(samples[j], DAY) for j in reached]
                total = sum(elapsed) + (length(samples) - length(reached)) * budget
                kms = [km_at(points[j], h) for j in reached]
                @test result.elapsed_sum_ms[i] == total
                @test result.reachable_samples[i] == length(reached)
                @test result.elapsed_ms[i] == total / length(samples)
                @test result.reachable_elapsed_ms[i] == sum(elapsed) / length(reached)
                @test isnan(sum(kms)) ? isnan(result.distance_km[i]) : isapprox(result.distance_km[i], sum(kms) / length(kms))
            end
            @test any(<(result.sample_count), result.reachable_samples)
            @test result.elapsed_ms[at(result, a)] == result.reachable_elapsed_ms[at(result, a)] == 0
            @test result.distance_km[at(result, a)] == 0
            @test result.reachable_samples[at(result, a)] == result.sample_count
            @test result.reachable_samples[at(result, remote)] == 3
            @test isnan(result.distance_km[at(result, remote)]) == !distances
            # One sample when step exceeds the window; zero walking retains graph-only parity.
            single = route_window_walking(graph, a, ready, budget, 1; step_ms=DAY, max_walk_s=seconds)
            @test single.h3 == points[1].h3
            @test single.elapsed_ms == points[1].arrival .- ready
            @test isequal(single.distance_km, points[1].distance_km)
            zero = route_window_walking(graph, b, ready, budget, window; step_ms=step, max_walk_s=0)
            transit = route_window(graph, b, ready, budget, window; step_ms=step)
            reached = findall(>(0), transit.reachable_samples)
            @test zero.h3 == graph.h3[reached]
            for field in (:elapsed_ms, :reachable_elapsed_ms, :distance_km, :reachable_samples, :elapsed_sum_ms)
                @test isequal(getproperty(zero, field), getproperty(transit, field)[reached])
            end
        end
        a, b, c, seconds = chain(9)
        graph = pack_graph(raw_table([a, b], [(1, 2, 0, 0, 0.0)]; distances=false))
        budget = walk(a, b).ms
        early = route_walking(graph, a, 0, budget; max_walk_s=seconds)
        late = route_walking(graph, a, 1, budget; max_walk_s=seconds)
        @test isnan(km_at(early, b)) && isfinite(km_at(late, b))
        mixed = route_window_walking(graph, a, 0, budget, 2; step_ms=1, max_walk_s=seconds)
        @test mixed.reachable_samples[at(mixed, b)] == 2
        @test mixed.elapsed_ms[at(mixed, b)] == mixed.reachable_elapsed_ms[at(mixed, b)] == budget / 2
        @test isnan(mixed.distance_km[at(mixed, b)]) # Never average just the known walking km.
        for (window, step) in ((0, 1), (DAY + 1, 1), (1, 0), (86_401, 1))
            @test_throws ArgumentError route_window_walking(graph, a, 0, budget, window; step_ms=step)
        end
    end
end

end # module
