using Test, Random, H3
include("../../router/src/Reachability.jl")
include("Hierarchy.jl")
using .HierarchyResearch
const R = Reachability
const HR = HierarchyResearch
const P = Int(R.PERIOD)
const MODES = (:mean_intersection, :max_intersection, :diff_intersection,
    :min_union, :diff_union, :reachable_union)
cell(lat, lon) = H3.API.latLngToCell(H3.API.LatLng(deg2rad(lat), deg2rad(lon)), 8)::UInt64
table(from, to, d, t) = (; from_h3=from, to_h3=to, departure_ms=UInt32.(d), duration_ms=Int64.(t))

function fine_coverage(g, p, wi, o, d, b, walk)
    point = R._walking_route_at(g, R.WalkingTopology(wi, min(walk, b)), o, UInt32(d), UInt32(d + b), false)
    weights = R._population_rollup(p, 8)
    return Set(h for h in point.h3 if get(weights, h, 0.0) > 0)
end

@testset "Zero weights and exact fine-origin exclusion" begin
    a = cell(48.1855, 16.3768)
    b = first(filter(!=(a), H3.API.gridDisk(a, 1)))
    g = R.pack_graph(table([a], [a], [div(P, 2)], [0]))
    wi = R.prepare_walking(R.WalkingIndex(g))
    for weights in ([0.0, 0.0], [1e200, 2e-30]), res in (8, 7, 6)
        pop = R._population([a, b], weights)
        h = prepare_hierarchy(g, pop, [a]; core_resolution=res, walking_index=wi)
        for mode in MODES
            value = only(route_hierarchy(h, [a], 0, 3_600_000; window_mode=mode,
                window_ms=96, step_ms=1, exclude_origin_population=true).value)
            @test isapprox(value, weights[2]; rtol=1e-12, atol=0)
        end
    end
end

function packed_coverage(h, o, d, b)
    sources = R._population_sources(h, h.destinations, nothing, [o], UInt32(min(h.max_walk_ms, b)))
    w = R.PopulationWorkspace(length(h.graph.h3), length(sources.weights))
    R._population_sample_packed!(w, h.graph, h.prepared.graph, h.destinations, sources,
        1:1, UInt32[d], UInt32[d + b], UInt32(min(h.max_walk_ms, b)))
    return Set(h.destinations.cells[i] for i in w.reached_ids if w.reached[i] != 0)
end

# Independent label correction with linear profile scans, not the production queue.
function model_coverage(h, origin, departure, budget; labels=false)
    g, p, walk = h.graph, h.destinations, h.prepared.graph
    cutoff = Int64(departure) + budget
    a, e = fill(Int64(R.INF), length(g.h3)), fill(Int64(R.INF), length(g.h3))
    source = h.core_nodes + h.origins[origin]
    a[source] = departure
    changed = true
    while changed
        changed = false
        for edge in eachindex(g.edge_to)
            u, v = g.edge_from[edge], g.edge_to[edge]
            a[u] <= cutoff || continue
            base = fld(a[u], P) * P
            for j in g.schedule_ptr[edge]:(g.schedule_ptr[edge + 1] - 1)
                g.departure[j] >= mod(a[u], P) || continue
                t = base + g.arrival[j]
                if t <= cutoff
                    changed |= t < a[v] || t < e[v]
                    a[v], e[v] = min(a[v], t), min(e[v], t)
                end
            end
        end
        for u in eachindex(e), j in walk.offsets[u]:(walk.offsets[u + 1] - 1)
            t, v = e[u] + walk.durations[j], walk.targets[j]
            if t <= cutoff && t < a[v]
                changed = true
                a[v] = t
            end
        end
    end
    coverage = Set{UInt64}()
    i = h.origins[origin]
    for j in h.direct.offsets[i]:(h.direct.offsets[i + 1] - 1)
        h.direct.durations[j] <= budget && push!(coverage, p.cells[h.direct.targets[j]])
    end
    for u in eachindex(e), j in p.offsets[u]:(p.offsets[u + 1] - 1)
        e[u] + p.durations[j] <= cutoff && push!(coverage, p.cells[p.targets[j]])
    end
    for u in eachindex(e)
        e[u] <= cutoff && p.walk_min[u] <= min(h.max_walk_ms, cutoff - e[u]) || (e[u] = R.INF)
    end
    return labels ? (; coverage, a, e) : coverage
end

@testset "Fine access, midnight, self legs, and optimistic transfers" begin
    o = cell(48.1855, 16.3768)
    siblings = H3.API.cellToChildren(H3.API.cellToParent(o, 7), 8)
    a, b = siblings[1:2]
    c = cell(47.813, 13.046)
    c2 = first(filter(!=(c), H3.API.cellToChildren(H3.API.cellToParent(c, 7), 8)))
    z = cell(47.0, 15.0)
    distance = only(R.walking_neighbors(R.WalkingIndex(R.pack_graph(table([a], [a], [0], [0]))), b, 3_600_000)).duration_ms
    # b cannot reach a's first bus. A coarse source teleport would incorrectly catch it.
    g = R.pack_graph(table([a, c2, b, a, a], [c, z, b, c, c],
        [100, 100, 1000, 0, P - 1], [0, 0, 0, 2P, 2P]))
    pop = R._population([a, b, c, c2, z], [0.1, 2e-30, 3.25, 0.0, 10.5])
    wi = R.prepare_walking(R.WalkingIndex(g))
    snapshot = deepcopy((g.departure, g.arrival, wi.prepared.graph.targets, pop.weights))
    for res in (8, 7, 6)
        h = prepare_hierarchy(g, pop, [a, b]; core_resolution=res, walking_index=wi)
        @test !haskey(pop.schedule_hints, h.graph)
        @test haskey(h.population.schedule_hints, h.graph)
        @test h.population.h3 === pop.h3 && h.population.weights === pop.weights
        result = route_hierarchy(h, [b, a, a], 0, 200)
        @test result.h3 == sort([a, b])
        @test result.value[findfirst(==(a), result.h3)] > result.value[findfirst(==(b), result.h3)]
        @test !(z in fine_coverage(g, pop, wi, a, 0, 200, 3_600_000))
        @test (z in packed_coverage(h, a, 0, 200)) == (res < 8)
        for origin in (a, b), (d, budget) in ((0, 200), (P - 1000, 3P), (0, 7P), (P - 1, Int(R.MAX_TIME_MS) - P))
            fine = fine_coverage(g, pop, wi, origin, d, budget, 3_600_000)
            actual = packed_coverage(h, origin, d, budget)
            @test issubset(fine, actual)
            @test actual == model_coverage(h, origin, d, budget)
            res == 8 && @test actual == fine
        end
        for mode in MODES, exclude in (false, true), samples in (1, 4, 96), budget in (0, 200, 7P)
            opts = (; window_ms=samples == 1 ? 0 : samples * 1000, step_ms=1000,
                window_mode=mode, exclude_origin_population=exclude)
            actual = route_hierarchy(h, [a, b], P - 1000, budget; opts...)
            expected = R._route_population_reference(g, pop, a, P - 1000, budget;
                opts..., walking_index=wi, origins=sort([a, b]))
            @test all(actual.value .>= expected.value .- 1e-10)
            (res == 8 || budget == 0) && @test isapprox(actual.value, expected.value; atol=1e-10, rtol=1e-12)
        end
        @test route_hierarchy(h, [a], 0, 100; max_walk_ms=0).backend == :fine_fallback
        for opts in ((; max_walk_ms=-1), (; max_walk_ms=1), (; window_ms=-1),
                (; step_ms=-1), (; window_mode=:bad), (; origin_batch_size=65),
                (; window_ms=R.MAX_TIME_MS, step_ms=1))
            @test_throws ArgumentError route_hierarchy(h, [a], 0, 200; opts...)
        end
        @test_throws ArgumentError route_hierarchy(h, [z], 0, 200)
        @test_throws ArgumentError route_hierarchy(h, [a], -1, 200)
        @test_throws ArgumentError route_hierarchy(h, [a], 0, -1)
        @test_throws ArgumentError route_hierarchy(h, [a], 1, R.MAX_TIME_MS)
        @test_throws ArgumentError route_hierarchy(h, UInt64[], 0, 1)
        @test all(iszero, h.destinations.weights[1:length(h.graph.h3)])
        @test all(u -> h.destinations.walk_min[u] == R.INF, (h.core_nodes + 1):length(h.graph.h3))
    end
    @test snapshot == (g.departure, g.arrival, wi.prepared.graph.targets, pop.weights)
    # The first boarding threshold crosses the preceding midnight.
    midnight = R.pack_graph(table([a], [c], [100], [0]))
    wi2 = R.prepare_walking(R.WalkingIndex(midnight))
    h = prepare_hierarchy(midnight, pop, [b]; core_resolution=8, walking_index=wi2)
    ready = P - Int(distance) + 100
    @test c in packed_coverage(h, b, ready, Int(distance))
    @test !(c in packed_coverage(h, b, ready + 1, Int(distance)))
    @test packed_coverage(h, b, ready, Int(distance)) == fine_coverage(midnight, pop, wi2, b, ready, Int(distance), 3_600_000)
    extreme = R.pack_graph(table([a], [c], [0], [Int(R.MAX_TIME_MS) - P]))
    @test_throws ArgumentError prepare_hierarchy(extreme, pop, [b]; core_resolution=7)
    @test_throws ArgumentError prepare_hierarchy(g, pop, [a]; core_resolution=9)
    @test_throws ArgumentError prepare_hierarchy(g, pop, [a]; core_resolution=7, max_walk_ms=0)
end

@testset "200 random graphs: fine equivalence and coarse coverage" begin
    rng = MersenneTwister(20260912)
    local_cells = sort(H3.API.gridDisk(cell(48.1855, 16.3768), 2))
    remote = sort(H3.API.gridDisk(cell(47.813, 13.046), 1))
    nodes = [local_cells[1:3:end]; remote[1:2:end]]
    for trial in 1:200
        from, to = rand(rng, nodes, 24), rand(rng, nodes, 24)
        d = rand(rng, [0, 100, 900_000, P - 1000], 24)
        g = R.pack_graph(table([nodes; from], [nodes; to], [fill(div(P, 2), length(nodes)); d],
            [zeros(Int, length(nodes)); rand(rng, [0, 100, 900_000, 2P], 24)]))
        pop = R._population([local_cells; remote], rand(rng, [0.0, 0.125, 2e-30, 1234.56789], 26))
        walk = trial % 2 == 0 ? 3_600_000 : 800_000
        wi = R.prepare_walking(R.WalkingIndex(g); max_walk_ms=walk)
        origins = sort(local_cells[[1, 2, 8]])
        departure, budget = rand(rng, [0, P - 500, 28_800_000]), rand(rng, [200, 3_600_000, 7P])
        for res in (8, trial % 2 == 0 ? 7 : 6)
            h = prepare_hierarchy(g, pop, origins; core_resolution=res, max_walk_ms=walk, walking_index=wi)
            sources = R._population_sources(h, h.destinations, nothing, origins, UInt32(min(walk, budget)))
            workspace = R.PopulationWorkspace(length(h.graph.h3), length(sources.weights), 1)
            for (i, o) in enumerate(origins)
                fine = fine_coverage(g, pop, wi, o, departure, budget, walk)
                actual = packed_coverage(h, o, departure, budget)
                model = model_coverage(h, o, departure, budget; labels=true)
                @test issubset(fine, actual)
                @test actual == model.coverage
                res == 8 && @test actual == fine
                fill!(workspace.arrivals, R.INF)
                fill!(workspace.settled, 0)
                empty!(workspace.settled_ids)
                R._population_sample_range!(workspace, h.graph, h.prepared.graph, h.destinations,
                    sources, i:i, UInt32[departure], UInt32[departure + budget],
                    UInt32(min(walk, budget)), workspace.arrivals)
                @test workspace.arrivals[1, 1:2:(2h.core_nodes)] == model.a[1:h.core_nodes]
                @test workspace.arrivals[1, 2:2:(2h.core_nodes)] == model.e[1:h.core_nodes]
            end
            mode, samples, exclude = MODES[mod1(trial, 6)], trial % 3 == 0 ? 96 : 4, isodd(trial)
            opts = (; max_walk_ms=walk, window_ms=samples * 900_000, step_ms=900_000,
                window_mode=mode, exclude_origin_population=exclude)
            actual = route_hierarchy(h, origins, departure, budget; opts..., origin_batch_size=2)
            other = route_hierarchy(h, origins, departure, budget; opts..., origin_batch_size=64)
            expected = R._route_population_reference(g, pop, first(origins), departure, budget;
                opts..., walking_index=wi, origins=origins)
            @test isapprox(actual.value, other.value; atol=1e-8, rtol=1e-12)
            @test all(actual.value .>= expected.value .- 1e-8)
            res == 8 && @test isapprox(actual.value, expected.value; atol=1e-8, rtol=1e-12)
        end
    end
end
