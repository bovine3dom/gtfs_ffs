include("test.jl")
haskey(ENV, "BOARDING_SCALAR_REFERENCE") && include(ENV["BOARDING_SCALAR_REFERENCE"])

function boarding_coverage(h, g, origin, departure, budget; correction=true)
    g = HR.BoardingGraph(g.graph, g.boarding, g.arrival_child, g.gaps, g.walk_child, g.hints, :independent, correction)
    credit = R.PopulationWorkspace(length(g.h3), length(h.destinations.weights))
    w = HR.BoardingWorkspace(credit, 1)
    i = h.origins[origin]
    HR.boarding_sample!(w, g, h, h.core_nodes + i, UInt32(departure),
        UInt32(departure + budget), UInt32(min(h.max_walk_ms, budget)))
    for j in h.direct.offsets[i]:(h.direct.offsets[i + 1] - 1)
        h.direct.durations[j] <= min(h.max_walk_ms, budget) &&
            R._population_credit!(credit, h.direct.targets[j], UInt64(1))
    end
    return Set(h.destinations.cells[id] for id in credit.reached_ids)
end

@testset "Later closer arrivals and valid descending history" begin
    a = cell(48.1855, 16.3768)
    b, c = sort(H3.API.cellToChildren(H3.API.cellToParent(cell(47.813, 13.046), 7), 8))[1:2]
    z = cell(47.0, 15.0)
    f = R.pack_graph(table([a, a, c], [b, c, z], [0, 10, 201], [100, 190, 0]))
    pop = R._population([z], [10.0])
    wi = R.prepare_walking(R.WalkingIndex(f))
    h = prepare_hierarchy(f, pop, [a]; core_resolution=7, walking_index=wi)
    g = prepare_boarding(h)
    @test z in fine_coverage(f, pop, wi, a, 0, 300, 3_600_000)
    @test !(z in boarding_coverage(h, g, a, 0, 300))
    @test z in boarding_coverage(h, g, a, 10, 300)
    @test only(route_boarding(h, g, [a], 0, 300; window_ms=20, step_ms=10, history=:independent).value) == 0
    @test only(route_boarding(h, g, [a], 0, 300; window_ms=20, step_ms=10, history=:descending).value) == 10
    # The retained journey starts at 10 and is still feasible from ready time 0.
    @test only(route_boarding(h, g, [a], 0, 300; window_ms=20, step_ms=10, correction=false).value) == 10
    @test_throws ArgumentError route_boarding(h, g, [a], 0, 300; history=:unknown)
    @test route_boarding(h, g, [a], 0, 300; max_walk_ms=0).backend == :fine_fallback
    @test route_boarding(h, g, [a], 0, 0).backend == :fine_fallback
end

# Keep every (parent state, child, walk-used) label and scan schedules linearly.
# This is an oracle for the retained profiles and optimistic projected walks,
# not for the fine model. Single-winner coverage must be a subset of this oracle.
function expanded_coverage(h, g, origin, departure, budget)
    State = Tuple{Int,UInt8,UInt32}
    labels = Dict{State,UInt32}()
    pending = R.BinaryMinHeap{Tuple{UInt32,State}}()
    cutoff, limit = UInt32(departure + budget), UInt32(min(h.max_walk_ms, budget))
    function enqueue(t, s)
        t <= cutoff && t < get(labels, s, R.INF) || return
        labels[s] = t
        push!(pending, (t, s))
    end
    enqueue(UInt32(departure), (2Int(h.core_nodes + h.origins[origin]) - 1, UInt8(0), UInt32(0)))
    while !isempty(pending)
        time, s = pop!(pending)
        labels[s] == time || continue
        state, child, used = s
        u = (state + 1) >> 1
        if iseven(state)
            net = h.prepared.graph
            for j in net.offsets[u]:(net.offsets[u + 1] - 1)
                d = net.durations[j]
                d <= limit && enqueue(time + d, (2Int(net.targets[j]) - 1, g.walk_child[j], d))
            end
        else
            for e in g.out_ptr[u]:(g.out_ptr[u + 1] - 1)
                gap = iszero(g.boarding[e]) ? UInt32(0) : g.gaps[u][child, g.boarding[e]]
                gap <= min(limit - used, cutoff - time) || continue
                clock = time + gap
                base = div(clock, R.PERIOD) * R.PERIOD
                for j in g.schedule_ptr[e]:(g.schedule_ptr[e + 1] - 1)
                    g.departure[j] >= clock % R.PERIOD && g.arrival[j] <= cutoff - base || continue
                    for state in (2Int(g.edge_to[e]) - 1, 2Int(g.edge_to[e]))
                        enqueue(base + g.arrival[j], (state, g.arrival_child[j], UInt32(0)))
                    end
                end
            end
        end
    end
    p, i = h.destinations, h.origins[origin]
    result = Set(p.cells[h.direct.targets[j]] for j in h.direct.offsets[i]:(h.direct.offsets[i + 1] - 1)
        if h.direct.durations[j] <= limit)
    for ((state, _, _), time) in labels
        iseven(state) || continue
        u = state >> 1
        for j in p.offsets[u]:(p.offsets[u + 1] - 1)
            p.durations[j] <= min(limit, cutoff - time) && push!(result, p.cells[p.targets[j]])
        end
    end
    return result
end

@testset "Child gaps, through connections, and shortcuts" begin
    a = cell(48.1855, 16.3768)
    b, c = sort(H3.API.cellToChildren(H3.API.cellToParent(cell(47.813, 13.046), 7), 8))[1:2]
    z = cell(47.0, 15.0)
    for same in (false, true), shortcut in (false, true), walk in (1, 3_600_000)
        from, to, dep, dur = [a, same ? b : c], [b, z], [100, 101], [0, 0]
        shortcut && (push!(from, a); push!(to, z); push!(dep, 100); push!(dur, 1))
        f = R.pack_graph(table(from, to, dep, dur))
        pop = R._population([a, b, c, z], ones(4))
        wi = R.prepare_walking(R.WalkingIndex(f); max_walk_ms=walk)
        h = prepare_hierarchy(f, pop, [a]; core_resolution=7, max_walk_ms=walk, walking_index=wi)
        g = prepare_boarding(h)
        @test (z in boarding_coverage(h, g, a, 0, 200)) == (same || shortcut)
        @test z in boarding_coverage(h, g, a, 0, 200; correction=false)
        @test all(m[i, i] == 0 for m in g.gaps for i in axes(m, 1))
        @test eltype(g.arrival_child) == UInt8
        @test eltype(first(g.gaps)) == UInt32
        if !same
            parent = h.graph.node_id[H3.API.cellToParent(b, 7)]
            @test g.gaps[parent][1, 2] == (walk == 1 ? R.INF :
                only(R.walking_neighbors(R.WalkingIndex(f), b, walk)).duration_ms)
        end
    end
end

@testset "Exact millisecond threshold and remaining walking budget" begin
    a = cell(48.1855, 16.3768)
    b, c = sort(H3.API.cellToChildren(H3.API.cellToParent(cell(47.813, 13.046), 7), 8))[1:2]
    z = cell(47.0, 15.0)
    stub = R.pack_graph(table([b], [c], [0], [0]))
    gap = Int(only(R.walking_neighbors(R.WalkingIndex(stub), b, 3_600_000)).duration_ms)
    for margin in (-1, 0)
        f = R.pack_graph(table([a, c], [b, z], [0, 100 + gap + margin], [100, 0]))
        pop = R._population([z], [1.0])
        wi = R.prepare_walking(R.WalkingIndex(f))
        h = prepare_hierarchy(f, pop, [a]; core_resolution=7, walking_index=wi)
        g = prepare_boarding(h)
        @test (z in boarding_coverage(h, g, a, 0, 100 + gap)) == (margin == 0)
    end
    f = R.pack_graph(table([a, b, c], [a, b, z], [P ÷ 2, P ÷ 2, 100 + gap], [0, 0, 0]))
    pop = R._population([z], [1.0])
    wi = R.prepare_walking(R.WalkingIndex(f))
    h = prepare_hierarchy(f, pop, [a]; core_resolution=7, walking_index=wi)
    g = prepare_boarding(h)
    parent = findfirst(==(H3.API.cellToParent(b, 7)), h.graph.h3)
    for spent in (3_600_000 - gap, 3_600_001 - gap)
        credit = R.PopulationWorkspace(length(g.h3), length(h.destinations.weights))
        w = HR.BoardingWorkspace(credit, 1)
        HR.boarding_enqueue!(w, UInt32(100), 2parent - 1, UInt8(1), UInt32(spent), UInt32(100 + gap))
        HR.boarding_sample!(w, g, h, h.core_nodes + h.origins[a], UInt32(0), UInt32(100 + gap), UInt32(3_600_000))
        @test any(id -> h.destinations.cells[id] == z, credit.reached_ids) == (spent + gap == 3_600_000)
    end
end

@testset "Same-clock self-edge metadata snapshot" begin
    a, z = cell(48.1855, 16.3768), cell(-30.0, 0.0)
    siblings = sort(H3.API.cellToChildren(H3.API.cellToParent(cell(47.813, 13.046), 7), 8))
    stub = R.pack_graph(table(siblings, siblings, zeros(Int, 7), zeros(Int, 7)))
    wi = R.WalkingIndex(stub)
    distance(x, y) = only(filter(hop -> hop.cell == y, R.walking_neighbors(wi, x, 3_600_000))).duration_ms
    d = siblings[1]
    ordered = sort(siblings[2:end]; by=x -> distance(x, d))
    c, b = first(ordered), last(ordered)
    gap, time = distance(c, d), UInt32(3_600_100)
    @test gap < distance(b, d)
    f = R.pack_graph(table([a, c, d], [a, b, z], [P ÷ 2, time, time + gap], [0, 0, 0]))
    pop = R._population([z], [1.0])
    walking = R.prepare_walking(R.WalkingIndex(f))
    h = prepare_hierarchy(f, pop, [a]; core_resolution=7, walking_index=walking)
    g = prepare_boarding(h)
    parent = findfirst(==(H3.API.cellToParent(c, 7)), g.h3)
    target = findfirst(==(H3.API.cellToParent(z, 7)), g.h3)
    @test parent < target # The self-edge changes the label before the target edge.
    children = sort([b, c, d])
    credit = R.PopulationWorkspace(length(g.h3), length(h.destinations.weights))
    w = HR.BoardingWorkspace(credit, 2)
    HR.boarding_enqueue!(w, time, 2parent - 1, UInt8(findfirst(==(c), children)), UInt32(3_600_000), time + gap, UInt64(1))
    HR.boarding_enqueue!(w, time, 2parent - 1, UInt8(findfirst(==(d), children)), UInt32(0), time + gap, UInt64(2))
    source = h.core_nodes + h.origins[a]
    sources = (; sources=[source, source], direct=[Int32[], Int32[]])
    HR.boarding_samples!(w, g, h.prepared.graph, h.destinations, sources, 1:2,
        UInt32[time, time], UInt32[time + gap, time + gap], UInt32(3_600_000))
    @test credit.reached[h.prepared.output_id[z]] == UInt64(2)
end

@testset "200 random graphs: zero control, res8, and fixed-history tiles" begin
    rng = MersenneTwister(261009)
    local_cells = sort(H3.API.gridDisk(cell(48.1855, 16.3768), 2))
    remote = sort(H3.API.gridDisk(cell(47.813, 13.046), 1))
    nodes = [local_cells[1:3:end]; remote[1:2:end]]
    cohort = sort(H3.API.gridDisk(cell(48.1855, 16.3768), 5))[1:65]
    for trial in 1:200
        f = R.pack_graph(table([nodes; rand(rng, nodes, 24)], [nodes; rand(rng, nodes, 24)],
            [fill(div(P, 2), length(nodes)); rand(rng, [0, 100, 900_000, P - 1000], 24)],
            [zeros(Int, length(nodes)); rand(rng, [0, 100, 900_000, 2P], 24)]))
        pop = R._population([local_cells; remote], rand(rng, [0.0, 0.125, 2e-30, 1234.56789], 26))
        walk = iseven(trial) ? 3_600_000 : 800_000
        wi = R.prepare_walking(R.WalkingIndex(f); max_walk_ms=walk)
        origins = sort!(unique!([cohort; local_cells[[1, 2, 8]]]))
        departure, budget = rand(rng, [0, P - 500, 28_800_000]), rand(rng, [200, 3_600_000, 7P])
        for res in (8, iseven(trial) ? 7 : 6)
            h = prepare_hierarchy(f, pop, origins; core_resolution=res, max_walk_ms=walk, walking_index=wi)
            snapshot = copy(f.arrival), copy(f.departure)
            g = prepare_boarding(h)
            @test snapshot == (f.arrival, f.departure)
            for o in local_cells[[1, 2, 8]]
                @test boarding_coverage(h, g, o, departure, budget; correction=false) == packed_coverage(h, o, departure, budget)
                @test issubset(boarding_coverage(h, g, o, departure, budget), expanded_coverage(h, g, o, departure, budget))
                res == 8 && @test boarding_coverage(h, g, o, departure, budget) == fine_coverage(f, pop, wi, o, departure, budget, walk)
            end
            samples = (1, 4, 96)[mod1(trial, 3)]
            options = (; window_ms=samples * 900_000, step_ms=900_000,
                window_mode=MODES[mod1(trial, 6)], exclude_origin_population=isodd(trial))
            baseline = route_hierarchy(h, origins, departure, budget; options...)
            zero = route_boarding(h, g, origins, departure, budget; options..., correction=false)
            @test isapprox(zero.value, baseline.value; atol=1e-8, rtol=1e-12)
            for history in (:descending, :independent)
                small = route_boarding(h, g, origins, departure, budget; options..., history, origin_batch_size=1)
                medium = route_boarding(h, g, origins, departure, budget; options..., history, origin_batch_size=16)
                large = route_boarding(h, g, origins, departure, budget; options..., history, origin_batch_size=64)
                @test isapprox(small.value, large.value; atol=1e-8, rtol=1e-12)
                @test isapprox(medium.value, large.value; atol=1e-8, rtol=1e-12)
                if isdefined(@__MODULE__, :ScalarBoarding)
                    scalar = ScalarBoarding.BoardingGraph(g.graph, g.boarding, g.arrival_child, g.gaps, g.walk_child, history, true)
                    expected = ScalarBoarding.route_boarding(h, scalar, origins, departure, budget; options..., history)
                    @test isapprox(large.value, expected.value; atol=1e-8, rtol=1e-12)
                end
                res == 8 && @test isapprox(small.value, baseline.value; atol=1e-8, rtol=1e-12)
            end
        end
    end
end
