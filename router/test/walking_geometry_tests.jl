module WalkingGeometryTests

using Test, Random, H3
include("../src/Reachability.jl")
using .Reachability

cell_at(lat, lon, resolution) = H3.API.latLngToCell(H3.API.LatLng(deg2rad(lat), deg2rad(lon)), resolution)::UInt64
disk(origin, k) = filter(!iszero, H3.API.gridDisk(origin, k))

function graph_of(cells)
    return pack_graph((from_h3=copy(cells), to_h3=copy(cells),
                       departure_ms=zeros(UInt32, length(cells)), duration_ms=zeros(Int64, length(cells))))
end

function brute_walks(cells, origin, limit)
    result = Reachability.WalkingNeighbor[]
    iszero(limit) && return result
    for cell in sort!(unique(cells))
        cell == origin && continue
        a, b = H3.API.cellToLatLng.((min(origin, cell), max(origin, cell)))
        km = H3.Lib.greatCircleDistanceKm(Ref(a), Ref(b))
        # Int64 accommodates even antipodal distances, unlike UInt32.
        isfinite(km) || continue
        duration = ceil(Int64, km * 720_000)
        duration <= limit && push!(result, (cell=cell, duration_ms=UInt32(duration), distance_km=km))
    end
    return result
end

polygon_walks(index, origin, limit) = iszero(limit) ? Reachability.WalkingNeighbor[] :
    Reachability._walking_polygon_cells(index, origin, H3.API.cellToLatLng(origin), limit)
same_walks(a, b) = length(a) == length(b) && all(x === y for (x, y) in zip(a, b))

@testset "Walking graph index versus brute force" begin
    rng = MersenneTwister(7301)
    for resolution in (7, 9)
        origins = [cell_at(lat, lon, resolution) for (lat, lon) in
                   ((0, 0), (51.5, -0.1), (0, 179.999), (0, -179.999), (90, 0), (-90, 0))]
        append!(origins, [cell_at(rad2deg(asin(2rand(rng) - 1)), 360rand(rng) - 180, resolution) for _ in 1:12])
        cells = UInt64[]
        for origin in origins
            append!(cells, shuffle(rng, disk(origin, 12))[1:150])
        end
        append!(cells, [cell_at(rad2deg(asin(2rand(rng) - 1)), 360rand(rng) - 180, resolution) for _ in 1:500])
        graph = graph_of(cells)
        index = WalkingIndex(graph)
        @test index.cells == graph.h3
        @test index.cells !== graph.h3
        @test sum(length, values(index.bins)) == length(graph.h3)
        @test Reachability.WALK_BIN_WIDTH == 2sin(5 / (2 * 6371.007180918475))
        @test any(origin -> !(origin in graph.h3), origins)
        for origin in [origins; graph.h3[1:20]], limit in (0, 1, 600_000, 3_600_000, 9_000_000, 604_800_000)
            actual = walking_neighbors(index, origin, limit)
            @test actual == brute_walks(graph.h3, origin, limit)
            @test eltype(actual) == @NamedTuple{cell::UInt64, duration_ms::UInt32, distance_km::Float64}
        end
        for origin in origins
            hop = first(walking_neighbors(index, origin, 9_000_000))
            for limit in (Int(hop.duration_ms) - 1, hop.duration_ms)
                @test walking_neighbors(index, origin, limit) == brute_walks(graph.h3, origin, limit)
            end
        end
        @test walking_neighbors(index, origins[1]) == walking_neighbors(index, origins[1], 3_600_000)
        @test walking_neighbors(index, origins[1], big(3_600_000)) == walking_neighbors(index, origins[1])
    end
end

@testset "Walking equality, symmetry, and validation" begin
    origin = cell_at(0, 179.999, 9)
    cells = disk(origin, 2)
    index = WalkingIndex(graph_of(cells))
    for hop in walking_neighbors(index, origin)
        @test hop in walking_neighbors(index, origin, hop.duration_ms)
        @test !(hop.cell in getproperty.(walking_neighbors(index, origin, Int(hop.duration_ms) - 1), :cell))
        reverse = only(filter(h -> h.cell == origin, walking_neighbors(index, hop.cell)))
        @test reverse.distance_km === hop.distance_km
        @test reverse.duration_ms === hop.duration_ms
    end
    empty_index = WalkingIndex(graph_of(UInt64[]))
    @test isempty(walking_neighbors(empty_index, cell_at(0, 0, 5)))
    for query in (walking_neighbors, walking_cells)
        @test isempty(query(index, origin, 0))
        @test_throws ArgumentError query(index, UInt64(0), 0)
        @test_throws ArgumentError query(index, H3.API.cellToParent(origin, 8), 0)
        for limit in (-1, big(Reachability.INF), typemax(UInt64))
            @test_throws ArgumentError query(index, origin, limit)
        end
    end
    graph = graph_of(cells)
    graph.h3[1] = UInt64(0)
    @test_throws ArgumentError WalkingIndex(graph)
    graph.h3[1] = H3.API.cellToParent(origin, 8)
    @test_throws ArgumentError WalkingIndex(graph)

    destination = first(filter(!=(origin), cells))
    graph = pack_graph((from_h3=[origin], to_h3=[destination], departure_ms=UInt32[0], duration_ms=Int64[1]))
    @test only(walking_neighbors(WalkingIndex(graph), origin)).cell == destination
end

@testset "Geographic walking versus generous grid disks" begin
    rng = MersenneTwister(7302)
    for resolution in (7, 9)
        pentagons = zeros(UInt64, 12)
        @test iszero(H3.Lib.getPentagons(resolution, pentagons))
        origins = [cell_at(lat, lon, resolution) for (lat, lon) in
                   ((0, 0), (51.5, -0.1), (0, 179.999), (0, -179.999),
                    (90, 0), (-90, 0), (89.96, 179.99), (-89.96, -179.99))]
        append!(origins, pentagons)
        append!(origins, [cell_at(178rand(rng) - 89, 360rand(rng) - 180, resolution) for _ in 1:5])
        # Most origins and destinations are off-network.
        index = WalkingIndex(graph_of([first(origins)]))
        for origin in origins
            reference_cells = disk(origin, resolution == 7 ? 30 : 90)
            outer_cells = setdiff(reference_cells, disk(origin, resolution == 7 ? 29 : 89))
            @test isempty(brute_walks(outer_cells, origin, 7_200_000))
            for limit in (1, 600_000, 3_600_000, 7_200_000)
                actual = walking_cells(index, origin, limit)
                @test actual == brute_walks(reference_cells, origin, limit)
                @test same_walks(actual, polygon_walks(index, origin, limit))
                @test length(unique(h.cell for h in actual)) == length(actual)
                @test all(h -> h.cell != origin && h.duration_ms <= limit, actual)
            end
            hops = walking_cells(index, origin)
            @test !isempty(hops)
            boundary = first(hops)
            @test boundary in walking_cells(index, origin, boundary.duration_ms)
            @test !(boundary.cell in getproperty.(walking_cells(index, origin, Int(boundary.duration_ms) - 1), :cell))
        end
    end
end

@testset "Geographic walking versus the entire coarse globe" begin
    rng = MersenneTwister(7303)
    for resolution in 0:2
        cells = reduce(vcat, [H3.API.cellToChildren(c, resolution) for c in H3.API.getRes0Cells()])
        index = WalkingIndex(graph_of(cells))
        origins = [rand(rng, cells, 30); cell_at(90, 0, resolution);
                   cell_at(-90, 0, resolution); cell_at(0, 180, resolution)]
        for origin in origins, limit in (3_600_000, 36_000_000, Reachability.MAX_TIME_MS)
            actual = walking_cells(index, origin, limit)
            @test same_walks(actual, brute_walks(cells, origin, limit))
            @test same_walks(actual, polygon_walks(index, origin, limit))
        end
    end
end

@testset "Certified disks versus polygon fill and random cutoffs" begin
    rng = MersenneTwister(7304)
    for resolution in (5, 6, 7)
        origins = [cell_at(45 + 10rand(rng), -5 + 20rand(rng), resolution) for _ in 1:300]
        append!(origins, [cell_at(rad2deg(asin(2rand(rng) - 1)), 360rand(rng) - 180, resolution) for _ in 1:300])
        pentagons = zeros(UInt64, 12)
        @test iszero(H3.Lib.getPentagons(resolution, pentagons))
        for pentagon in pentagons
            append!(origins, disk(pentagon, 1))
        end
        append!(origins, [cell_at(lat, lon, resolution) for lat in (-90, -89.96, 0, 89.96, 90)
                         for lon in (-179.999, -90, 0, 90, 179.999)])
        index = WalkingIndex(graph_of([first(origins)]))
        for origin in unique(origins)
            # An independent destination selects cutoffs that exercise inclusion
            # even when the default walk has no neighbors at coarse resolutions.
            boundary = rand(rng, brute_walks(disk(origin, 2), origin, 604_800_000))
            for limit in (0, 1, 3_600_000, 7_200_000, rand(rng, 1:7_200_000),
                          Int(boundary.duration_ms) - 1, Int(boundary.duration_ms))
                actual = walking_cells(index, origin, limit)
                @test same_walks(actual, polygon_walks(index, origin, limit))
                @test (boundary.cell in getproperty.(actual, :cell)) == (boundary.duration_ms <= limit)
            end
        end
    end
end

@testset "Certified disk selection and unrestricted fallback" begin
    for resolution in (5, 6, 7, 9)
        origin = cell_at(51.5, -0.1, resolution)
        centre = H3.API.cellToLatLng(origin)
        index = WalkingIndex(graph_of([origin]))
        candidates = Reachability._walking_disk(origin, centre, 3_600_000)
        if resolution == 9
            @test isnothing(candidates)
            actual = walking_cells(index, origin)
            @test any(h -> !(h.cell in disk(origin, 8)), actual)
            @test same_walks(actual, polygon_walks(index, origin, 3_600_000))
        else
            @test !isnothing(candidates)
            @test same_walks(brute_walks(filter(!iszero, candidates), origin, 3_600_000),
                             polygon_walks(index, origin, 3_600_000))
        end
        @test walking_cells(index, origin, big(3_600_000)) == walking_cells(index, origin)
    end
end

@testset "Geographic API validation" begin
    origin = cell_at(0, 0, 9)
    index = WalkingIndex(graph_of([origin]))
    @test isempty(walking_cells(index, origin, 0))
    @test isempty(walking_cells(index, origin, 1))
    @test_throws MethodError walking_cells(index, origin; max_cells=0)
    @test_throws MethodError walking_cells(index, origin; work=nothing)
    @test_throws MethodError walking_neighbors(index, origin; work=nothing)
    for bad in (-1, Int(Reachability.INF), typemax(UInt64))
        @test_throws ArgumentError walking_cells(index, origin, bad)
    end
    origin = cell_at(0, 0, 5)
    @test isempty(walking_cells(WalkingIndex(graph_of(UInt64[])), origin))
end

end # module
