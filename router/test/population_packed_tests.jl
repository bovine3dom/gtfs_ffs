module PopulationPackedTests

using Test, Random, Arrow, HTTP
import H3
using ..Reachability
const R = Reachability
const MODES = (:mean_intersection, :max_intersection, :diff_intersection,
               :min_union, :diff_union, :reachable_union)

function fixture(res)
    origin = H3.API.latLngToCell(H3.API.LatLng(deg2rad(51.5), deg2rad(-0.1)), res)::UInt64
    cells = sort!(filter(!iszero, H3.API.gridDisk(origin, 2)))
    rng = MersenneTwister(9100 + res)
    nodes = cells[1:2:end]
    graph = pack_graph((from_h3=[nodes; rand(rng, nodes, 50)],
        to_h3=[nodes; rand(rng, nodes, 50)],
        departure_ms=UInt32[zeros(Int, length(nodes)); rand(rng, [0, 95, 100, 1_800_000, 86_399_000], 50)],
        duration_ms=Int64[zeros(Int, length(nodes)); rand(rng, [0, 1000, 60_000], 50)]))
    cells = sort!(filter(!iszero, H3.API.gridDisk(origin, 6)))
    weights = Float64.(mod.(1:length(cells), 13)) .+ 0.25
    weights[findfirst(==(first(nodes)), cells)] = 0.0
    population = R._population(UInt64[first(H3.API.cellToChildren(h, 8)) for h in cells], weights)
    limit = res == 6 ? 7_200_000 : 3_600_000
    index = prepare_walking(WalkingIndex(graph); max_walk_ms=limit)
    return (; graph, population, origin, index, limit)
end

function oracle(graph, population, index, origins, departure, budget, step, samples, walk, mode;
                exclude_origin_population=false)
    weights = R._population_rollup(population, graph.resolution)
    topology = R.WalkingTopology(index, min(walk, budget))
    return map(origins) do origin
        counts = Dict{UInt64,Int}()
        for sample in 0:(samples - 1)
            ready = UInt32(departure + sample * step)
            result = R._walking_route_at(graph, topology, origin, ready, ready + UInt32(budget), false)
            for cell in result.h3
                exclude_origin_population && cell == origin && continue
                counts[cell] = get(counts, cell, 0) + 1
            end
        end
        sum(get(weights, cell, 0.0) * (mode == :reachable_union ? count / samples :
            mode in (:min_union, :diff_union) ? 1 : count == samples) for (cell, count) in counts; init=0.0)
    end
end

@testset "Per-origin population exclusion" begin
    (; graph, population, origin, index, limit) = fixture(6)
    for selected in (index, WalkingIndex(graph)), walk in (0, limit, limit + 1),
            window in (0, 96), mode in MODES
        options = (; walking_index=selected, max_walk_ms=walk, window_ms=window,
                    step_ms=1, window_mode=mode, origin_radius=2)
        baseline = route_population(graph, population, origin, 0, limit + 100; options...)
        @test route_population(graph, population, origin, 0, limit + 100;
            options..., exclude_origin_population=false) == baseline
        actual = route_population(graph, population, origin, 0, limit + 100;
            options..., exclude_origin_population=true)
        expected = oracle(graph, population, selected, actual.h3, 0, limit + 100,
            1, max(1, window), walk, mode; exclude_origin_population=true)
        @test isapprox(actual.value, expected)
        @test actual.h3 == baseline.h3
        @test actual.shared_expansions == baseline.shared_expansions
        @test actual.query_expansions == baseline.query_expansions
    end
end

@testset "Exact own-only exclusion and tiny other populations" begin
    cell = H3.API.latLngToCell(H3.API.LatLng(0.5, 0.1), 6)::UInt64
    neighbor = first(filter(!=(cell), H3.API.gridDisk(cell, 1)))
    children = H3.API.cellToChildren(cell, 8)[1:2]
    graph = pack_graph((from_h3=[cell, neighbor], to_h3=[neighbor, cell],
        departure_ms=UInt32[95, 95], duration_ms=Int64[0, 0]))
    index = prepare_walking(WalkingIndex(graph); max_walk_ms=0)
    for other in (0.0, 2e-30)
        population = R._population([children; first(H3.API.cellToChildren(neighbor, 8))],
            [0.04, 0.06, other])
        for selected in (index, WalkingIndex(graph)), window in (0, 96), mode in MODES
            result = route_population(graph, population, cell, 0, 96;
                walking_index=selected, max_walk_ms=0, origin_radius=2,
                window_ms=window, step_ms=1, window_mode=mode, exclude_origin_population=true)
            @test isapprox(result.value[findfirst(==(cell), result.h3)], other)
            @test isapprox(result.value[findfirst(==(neighbor), result.h3)], 0.1)
            @test count(>(0), result.value) == (iszero(other) ? 1 : 2)
        end
    end
    own_only = R._population(children, [0.04, 0.06])
    for node in (cell, neighbor)
        isolated = pack_graph((from_h3=[node], to_h3=[node], departure_ms=UInt32[0], duration_ms=Int64[0]))
        selected = prepare_walking(WalkingIndex(isolated); max_walk_ms=0)
        for mode in MODES
            result = route_population(isolated, own_only, cell, 0, 0; walking_index=selected,
                max_walk_ms=0, origin_radius=2, window_ms=96, step_ms=1,
                window_mode=mode, exclude_origin_population=true)
            @test all(iszero, result.value)
        end
    end
    graph = pack_graph((from_h3=[cell], to_h3=[cell], departure_ms=UInt32[0], duration_ms=Int64[0]))
    handler = make_handler(graph; population=own_only)
    base = "/reachable?index=$(string(cell; base=16))&departure_h=0&budget_h=0&max_walk_h=0"
    for window in ("", "&window_h=$(96 / 3_600_000)&step_h=$(1 / 3_600_000)"), mode in MODES,
            encoding in ("string", "split"), distance in ("itinerary", "straight_line")
        path = base * "&metric=accessible_population&origin_radius=2&encoding=$encoding&distance_mode=$distance&window_mode=$mode$window"
        baseline = handler(HTTP.Request("GET", path))
        for flag in ("false", "0")
            @test handler(HTTP.Request("GET", path * "&exclude_origin_population=$flag")).body == baseline.body
        end
        for flag in ("true", "1")
            response = handler(HTTP.Request("GET", path * "&exclude_origin_population=$flag"))
            @test response.status == 200
            table = Arrow.Table(response.body)
            @test isempty(table.value)
            @test eltype(table.value) == Float64
            @test Set(propertynames(table)) == Set(encoding == "string" ? (:index, :value) : (:index_lower, :index_upper, :value))
        end
    end
    for metric in ("time", "time_distance_quantile"), window in ("", "&window_h=0.01")
        path = base * "&metric=$metric&distance_mode=straight_line$window"
        baseline = handler(HTTP.Request("GET", path))
        for flag in ("true", "false", "1", "0", "", "nonsense")
            response = handler(HTTP.Request("GET", path * "&exclude_origin_population=$flag"))
            @test response.status == 200
            @test response.body == baseline.body
        end
    end
end

@testset "Packed population identity and immutable inputs" begin
    (; graph, population, origin, index) = fixture(7)
    snapshot = deepcopy((index.cells, index.prepared, population.h3, population.weights))
    view = R._prepare_population(population, index)
    @test view === R._prepare_population(population, index)
    tasks = [Threads.@spawn R._prepare_population(population, index) for _ in 1:8]
    @test all(task -> fetch(task) === view, tasks)
    @test view.cells === index.prepared.output_cells
    @test view.node_count == length(graph.h3)
    @test view.weights == [get(R._population_rollup(population, 7), h, 0.0) for h in view.cells]
    @test all(>(0), view.weights[view.targets])
    @test all(u -> issorted(view.durations[view.offsets[u]:(view.offsets[u + 1] - 1)]), 1:view.node_count)
    other_index = prepare_walking(WalkingIndex(graph))
    @test R._prepare_population(population, other_index) !== view
    other_pop = R._population(population.h3, population.weights .* 2)
    @test R._prepare_population(other_pop, index).weights == 2 .* view.weights
    @test R._prepare_population(population, WalkingIndex(graph)) === nothing
    @test_throws ArgumentError route_population(graph, other_pop, origin, 0, 100;
        walking_index=index, prepared_population=view)
    @test_throws ArgumentError route_population(graph, population, origin, 0, 100;
        walking_index=other_index, prepared_population=view)
    other_graph = pack_graph((from_h3=[origin], to_h3=[origin], departure_ms=UInt32[0], duration_ms=Int64[0]))
    @test_throws ArgumentError route_population(other_graph, population, origin, 0, 100; walking_index=index)
    route_population(graph, population, origin, 0, 7_200_000; origin_radius=5, walking_index=index)
    @test index.cells == snapshot[1]
    for name in (:graph, :output, :geographic), field in (:offsets, :targets, :durations, :distances)
        @test getproperty(getproperty(index.prepared, name), field) == getproperty(getproperty(snapshot[2], name), field)
    end
    @test population.h3 == snapshot[3]
    @test population.weights == snapshot[4]
    @test length(population.prepared) == 2
end

@testset "Packed population blocks and real walking res$res" for res in (6, 7)
    (; graph, population, origin, index, limit) = fixture(res)
    @test !isempty(index.prepared.graph.targets)
    @test !isempty(R._prepare_population(population, index).targets)
    for radius in (0, 1, 5), mode in MODES, walk in (0, limit)
        options = (; origin_radius=radius, window_ms=96, step_ms=1, max_walk_ms=walk,
                    window_mode=mode, walking_index=index)
        expected = R._route_population_reference(graph, population, origin, 0, limit + 100; options...)
        for batch in (8, 16, 32, 64)
            actual = route_population(graph, population, origin, 0, limit + 100; options..., origin_batch_size=batch)
            @test actual.h3 == expected.h3
            @test isapprox(actual.value, expected.value; rtol=1e-12, atol=1e-6)
        end
    end
    for mode in MODES, (departure, budget, walk) in ((86_399_000, limit, limit), (0, 100, 0))
        result = route_population(graph, population, origin, departure, budget; origin_radius=1,
            window_ms=3001, step_ms=1000, max_walk_ms=walk, window_mode=mode, walking_index=index)
        @test isapprox(result.value, oracle(graph, population, index, result.h3, departure,
            budget, 1000, 4, walk, mode); rtol=1e-12, atol=1e-6)
    end
    for walk in (0, limit, limit + 1), selected in (index, WalkingIndex(graph))
        options = (; origin_radius=5, walking_index=selected, max_walk_ms=walk)
        result = route_population(graph, population, origin, 0, limit + 1; options...)
        expected = R._route_population_reference(graph, population, origin, 0, limit + 1; options...)
        @test length(result.h3) > 64
        @test result.h3 == expected.h3
        @test result.value == expected.value
    end
    zero = first(graph.h3)
    @test get(R._population_rollup(population, res), zero, 0.0) == 0.0
    result = route_population(graph, population, zero, 0, 7 * Int(R.PERIOD); max_walk_ms=0, walking_index=index)
    labels = route_cpu(graph, zero, 0, 7 * Int(R.PERIOD))
    weights = R._population_rollup(population, res)
    @test only(result.value) == sum(get(weights, graph.h3[i], 0.0) for i in eachindex(labels) if labels[i] != R.INF)
    @test only(result.value) > 0
end

@testset "Packed population deadlines, empty walks, and bounds" begin
    (; graph, population, origin, index, limit) = fixture(6)
    options = (; walking_index=index, max_walk_ms=1)
    actual = route_population(graph, population, first(graph.h3), 0, 7 * Int(R.PERIOD); options...)
    expected = R._route_population_reference(graph, population, first(graph.h3), 0, 7 * Int(R.PERIOD); options...)
    @test actual.value == expected.value
    @test 2actual.shared_expansions == expected.shared_expansions
    @test 2actual.query_expansions == expected.query_expansions
    for mode in MODES
        options = (; walking_index=index, max_walk_ms=0, window_ms=30 * Int(R.PERIOD),
                    step_ms=Int(R.PERIOD), window_mode=mode, origin_radius=1)
        result = route_population(graph, population, origin, 0, Int(R.MAX_TIME_MS) - 29 * Int(R.PERIOD); options...)
        expected = R._route_population_reference(graph, population, origin, 0, Int(R.MAX_TIME_MS) - 29 * Int(R.PERIOD); options...)
        @test isapprox(result.value, expected.value; rtol=1e-12, atol=1e-6)
        options = (; walking_index=index, max_walk_ms=limit, window_ms=Int(R.MAX_TIME_MS) - limit + 1,
                    step_ms=Int(R.MAX_TIME_MS) - limit, window_mode=mode, origin_radius=1)
        result = route_population(graph, population, origin, 0, limit; options...)
        expected = R._route_population_reference(graph, population, origin, 0, limit; options...)
        @test isapprox(result.value, expected.value; rtol=1e-12, atol=1e-6)
    end
    for batch in (0, 65, 1.5)
        @test_throws ArgumentError route_population(graph, population, origin, 0, 0; origin_batch_size=batch)
    end
    @test_throws ArgumentError route_population(graph, population, origin, 0, R.MAX_TIME_MS;
        window_ms=2, step_ms=1, walking_index=index)
    source = first(graph.h3)
    hop = first(walking_cells(index, source, limit))
    isolated = pack_graph((from_h3=[source], to_h3=[source], departure_ms=UInt32[R.PERIOD - 1], duration_ms=Int64[0]))
    index = prepare_walking(WalkingIndex(isolated); max_walk_ms=limit)
    pair = R._population(UInt64[first(H3.API.cellToChildren(h, 8)) for h in (source, hop.cell)], [2.0, 3.0])
    for delta in (-1, 0), origin in (source, hop.cell)
        result = route_population(isolated, pair, origin, 0, hop.duration_ms;
            max_walk_ms=Int(hop.duration_ms) + delta, walking_index=index)
        @test only(result.value) == (delta == 0 ? 5.0 : origin == source ? 2.0 : 3.0)
    end
end

@testset "Packed population zero schema and subnormal weights" begin
    cell = H3.API.latLngToCell(H3.API.LatLng(0.5, 0.1), 8)::UInt64
    graph = pack_graph((from_h3=[cell], to_h3=[cell], departure_ms=UInt32[0], duration_ms=Int64[0]))
    index = prepare_walking(WalkingIndex(graph); max_walk_ms=0)
    population = R._population([cell], [nextfloat(0.0)])
    result = route_population(graph, population, cell, 0, 0; window_ms=2, step_ms=1,
        max_walk_ms=0, window_mode=:reachable_union, walking_index=index)
    @test only(result.value) == nextfloat(0.0)
    empty = R._population([cell], [0.0])
    handler = make_handler(graph; population=empty)
    base = "/reachable?index=$(string(cell; base=16))&departure_h=0&budget_h=0&max_walk_h=0"
    for encoding in ("string", "split")
        response = handler(HTTP.Request("GET", base * "&metric=accessible_population&encoding=$encoding"))
        @test response.status == 200
        table = Arrow.Table(response.body)
        @test isempty(table.value)
        @test eltype(table.value) == Float64
        @test Set(propertynames(table)) == Set(encoding == "string" ? (:index, :value) : (:index_lower, :index_upper, :value))
    end
    @test handler(HTTP.Request("GET", base * "&origin_radius=ignored")).body == handler(HTTP.Request("GET", base)).body
    neighbor = first(filter(!=(cell), H3.API.gridDisk(cell, 1)))
    graph = pack_graph((from_h3=[cell], to_h3=[neighbor], departure_ms=UInt32[1], duration_ms=Int64[0]))
    huge = R._population([cell, neighbor], fill(floatmax(Float64), 2))
    index = prepare_walking(WalkingIndex(graph); max_walk_ms=0)
    for mode in MODES
        @test_throws ArgumentError route_population(graph, huge, cell, 0, 100; walking_index=index,
            max_walk_ms=0, window_ms=2, step_ms=1, window_mode=mode)
    end
end

@testset "Packed population first-arrival lane masks" begin
    (; graph, population, index) = fixture(6)
    origin, hub, target = graph.h3[1:3]
    graph = pack_graph((from_h3=[origin, hub, target], to_h3=[hub, target, hub],
        departure_ms=UInt32[95, 100, 100], duration_ms=Int64[0, 0, 0]))
    index = prepare_walking(WalkingIndex(graph); max_walk_ms=0)
    population = R._population(UInt64[first(H3.API.cellToChildren(h, 8)) for h in graph.h3], ones(3))
    prepared = R._prepare_population(population, index)
    sources = R._population_sources(index, prepared, R._population_rollup(population, 6), [origin], UInt32(0))
    w = R.PopulationWorkspace(3, length(sources.weights))
    ready = UInt32.(0:63)
    counts = @inferred R._population_sample_packed!(w, graph, index.prepared.graph, prepared,
        sources, 1:1, ready, ready .+ UInt32(96), UInt32(0))
    @test w.reached[graph.node_id[origin]] == typemax(UInt64)
    @test w.reached[graph.node_id[hub]] == typemax(UInt64)
    @test w.reached[graph.node_id[target]] == typemax(UInt64) << 4
    @test counts == (66, 188)
    @test isempty(w.pending) && isempty(w.queue) && isempty(w.times)
    @test all(iszero, w.heads)
    result = @inferred R._population_tile!(w, graph, index.prepared.graph, prepared,
        sources, 1:1, UInt32(0), UInt32(96), Int64(1), 96, UInt32(0), :reachable_union)
    @test isapprox(only(result.value), 2 + 92 / 96; rtol=1e-12, atol=1e-6)
    for mode in (:mean_intersection, :min_union)
        result = R._population_tile!(w, graph, index.prepared.graph, prepared,
            sources, 1:1, UInt32(0), UInt32(96), Int64(1), 96, UInt32(0), mode)
        @test only(result.value) == (mode == :min_union ? 3 : 2)
    end
end

@testset "Packed population walk states and transit resets" begin
    origin = H3.API.latLngToCell(H3.API.LatLng(deg2rad(51.5), deg2rad(-0.1)), 7)::UInt64
    distance(a, b) = ceil(Int, H3.Lib.greatCircleDistanceKm(
        Ref(H3.API.cellToLatLng(min(a, b))), Ref(H3.API.cellToLatLng(max(a, b)))) * R.WALK_MS_PER_KM)
    chain = first((a, b, c) for a in [origin] for b in H3.API.gridDisk(a, 1)
        for c in H3.API.gridDisk(b, 1) if !iszero(b) && !iszero(c) && length(Set((a, b, c))) == 3 &&
        distance(a, c) > max(distance(a, b), distance(b, c)))
    a, b, c = chain
    ab, bc = distance(a, b), distance(b, c)
    limit = max(ab, bc)
    population = R._population(UInt64[first(H3.API.cellToChildren(h, 8)) for h in chain], [0.0, 2.0, 3.0])
    for reset in (false, true), delay in (0, 10)
        graph = pack_graph((from_h3=[a, b, c], to_h3=[b, b, c],
            departure_ms=UInt32[reset ? ab + delay : R.PERIOD ÷ 2, ab + delay, R.PERIOD ÷ 2],
            duration_ms=Int64[0, 0, 0]))
        index = prepare_walking(WalkingIndex(graph); max_walk_ms=limit)
        budget = ab + bc + delay
        for source in (a, first(filter(h -> h != a && !haskey(graph.node_id, h), H3.API.gridDisk(a, 1))))
            result = route_population(graph, population, source, 0, budget; max_walk_ms=limit, walking_index=index)
            @test result.value == oracle(graph, population, index, [source], 0, budget, 1, 1, limit, :min_union)
        end
        @test only(route_population(graph, population, a, 0, budget;
            max_walk_ms=limit, walking_index=index).value) == 5.0
        blocked = pack_graph((from_h3=[a, b, c], to_h3=[b, b, c],
            departure_ms=fill(UInt32(R.PERIOD ÷ 2), 3), duration_ms=Int64[0, 0, 0]))
        @test only(route_population(blocked, population, a, 0, budget;
            max_walk_ms=limit, walking_index=index).value) == 2.0
    end
end

@testset "Population batch defaults and deadline parity" begin
    (; graph, population, origin, index, limit) = fixture(6)
    for window_ms in (0, 96), walking_index in (index, WalkingIndex(graph))
        options = (; walking_index, window_ms, step_ms=1, max_walk_ms=limit, origin_radius=5)
        expected = route_population(graph, population, origin, 0, limit; options...,
            origin_batch_size=window_ms == 0 ? 64 : 16)
        @test route_population(graph, population, origin, 0, limit; options...) == expected
    end
    for batch in (1, 3, 63), mode in MODES
        options = (; walking_index=index, window_ms=96, step_ms=1, max_walk_ms=0,
                    origin_radius=5, origin_batch_size=batch, window_mode=mode)
        expected = R._route_population_reference(graph, population, origin, 0, 100; options...)
        actual = route_population(graph, population, origin, 0, 100; options...)
        @test isapprox(actual.value, expected.value; rtol=1e-12, atol=1e-6)
    end
    a, b = graph.h3[1:2]
    graph = pack_graph((from_h3=[a], to_h3=[b], departure_ms=UInt32[100], duration_ms=Int64[50]))
    index = prepare_walking(WalkingIndex(graph); max_walk_ms=0)
    population = R._population(UInt64[first(H3.API.cellToChildren(h, 8)) for h in (a, b)], [1.0, 5.0])
    for mode in MODES
        actual = route_population(graph, population, a, 0, 149; walking_index=index,
            max_walk_ms=0, window_ms=64, step_ms=1, window_mode=mode)
        expected = mode == :reachable_union ? 1 + 5 * 63 / 64 : mode in (:min_union, :diff_union) ? 6 : 1
        @test only(actual.value) == expected
    end
end

end
