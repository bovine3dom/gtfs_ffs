const DEFAULT_MAX_WALK_MS = UInt32(3_600_000)
const WALK_MS_PER_KM = 720_000.0 # 5 km/h
const WALK_EARTH_RADIUS_KM = 6371.007180918475 # H3's WGS84 authalic radius
const WALK_BIN_WIDTH = 2sin(5 / (2WALK_EARTH_RADIUS_KM))
const WalkingNeighbor = @NamedTuple{cell::UInt64, duration_ms::UInt32, distance_km::Float64}

struct PackedWalking{T}
    offsets::Vector{Int}
    targets::Vector{T}
    durations::Vector{UInt32}
    distances::Vector{Float64}
end

struct WalkingAdjacency
    limit::UInt32
    node_id::Dict{UInt64,Int32}
    geographic::PackedWalking{UInt64}
    graph::PackedWalking{Int32}
    output_cells::Vector{UInt64}
    output_id::Dict{UInt64,Int32}
    output::PackedWalking{Int32}
end

# Borrowed, read-only ranges: no neighbor-vector copies on a prepared hit.
struct WalkingRange{T} <: AbstractVector{NamedTuple{(:cell, :duration_ms, :distance_km),Tuple{T,UInt32,Float64}}}
    packed::PackedWalking{T}
    first::Int
    count::Int
end
Base.size(hops::WalkingRange) = (hops.count,)
Base.IndexStyle(::Type{<:WalkingRange}) = IndexLinear()
Base.iterate(hops::WalkingRange, i::Int=1) = i > length(hops) ? nothing : (hops[i], i + 1)
function Base.getindex(hops::WalkingRange, i::Int)
    @boundscheck checkbounds(hops, i)
    j = hops.first + i - 1
    p = hops.packed
    @inbounds return (cell=p.targets[j], duration_ms=p.durations[j], distance_km=p.distances[j])
end

"""
    WalkingIndex(graph::Graph)

Static snapshot of all graph vertices, hashed by their unit-sphere centres.
Bins have the chord width of a one-hour walk at 5 km/h. Treat the index as
read-only; rebuild it after changing graph vertices or resolution.
"""
struct WalkingIndex
    cells::Vector{UInt64}
    centres::Vector{H3.API.LatLng}
    bins::Dict{NTuple{3,Int},Vector{Int}}
    resolution::Int
    prepared::Union{Nothing,WalkingAdjacency}
end

function _walking_xyz(p::H3.API.LatLng)
    return (cos(p.lat) * cos(p.lng), cos(p.lat) * sin(p.lng), sin(p.lat))
end

function WalkingIndex(graph::Graph)
    0 <= graph.resolution <= 15 || throw(ArgumentError("invalid graph H3 resolution"))
    foreach(h -> validate_cell(h, graph.resolution), graph.h3)
    cells = copy(graph.h3)
    centres = H3.API.LatLng[H3.API.cellToLatLng(h) for h in cells]
    bins = Dict{NTuple{3,Int},Vector{Int}}()
    for (i, centre) in enumerate(centres)
        bin = map(x -> floor(Int, x / WALK_BIN_WIDTH), _walking_xyz(centre))
        push!(get!(Vector{Int}, bins, bin), i)
    end
    return WalkingIndex(cells, centres, bins, graph.resolution, nothing)
end

"""
    prepare_walking(index::WalkingIndex; max_walk_s=3600, workers=min(4, Threads.nthreads(:default)))

Return a read-only index with resident packed geographic and graph adjacency.
Enumerate exact geometry once per vertex, preserving canonical H3 order. Preparation
does not mutate the input; larger radii and off-graph origins use request-local fallback.
Memory and startup work scale with the complete geographic adjacency, without caps.
"""
function prepare_walking(index::WalkingIndex; max_walk_s::Integer=3600,
                         workers::Integer=min(4, Threads.nthreads(:default)))
    limit = _walking_limit(max_walk_s)
    workers > 0 || throw(ArgumentError("workers must be positive"))
    bare = WalkingIndex(index.cells, index.centres, index.bins, index.resolution, nothing)
    n = length(index.cells)
    slots = Vector{Vector{WalkingNeighbor}}(undef, n)
    count = Int(min(workers, Threads.nthreads(:default), max(1, n)))
    @sync for slot in 1:count
        Threads.@spawn for u in slot:count:n
            slots[u] = walking_cells(bare, index.cells[u], limit)
        end
    end
    node_id = Dict(h => Int32(i) for (i, h) in enumerate(index.cells))
    geographic = PackedWalking([1], UInt64[], UInt32[], Float64[])
    network = PackedWalking([1], Int32[], UInt32[], Float64[])
    for hops in slots
        for hop in hops
            push!(geographic.targets, hop.cell)
            push!(geographic.durations, hop.duration_ms)
            push!(geographic.distances, hop.distance_km)
            v = get(node_id, hop.cell, Int32(0))
            if v != 0
                push!(network.targets, v)
                push!(network.durations, hop.duration_ms)
                push!(network.distances, hop.distance_km)
            end
        end
        push!(geographic.offsets, length(geographic.targets) + 1)
        push!(network.offsets, length(network.targets) + 1)
    end
    # Graph IDs stay a prefix; geographic IDs follow first canonical discovery.
    output_cells, output_id = copy(index.cells), copy(node_id)
    targets = Int32[]
    sizehint!(targets, length(geographic.targets))
    for cell in geographic.targets
        id = get!(output_id, cell) do
            push!(output_cells, cell)
            Int32(length(output_cells))
        end
        push!(targets, id)
    end
    output = PackedWalking(geographic.offsets, targets, geographic.durations, geographic.distances)
    return WalkingIndex(index.cells, index.centres, index.bins, index.resolution,
                        WalkingAdjacency(limit, node_id, geographic, network, output_cells, output_id, output))
end

function _walking_validate(index, origin, max_walk_ms)
    validate_cell(origin, index.resolution)
    0 <= max_walk_ms <= MAX_BUDGET_MS ||
        throw(ArgumentError("max_walk_ms must be between zero and seven days"))
end

function _walking_neighbor(origin, cell, a, b, max_walk_ms)
    cell == origin && return nothing
    # Canonical argument order makes both directions bitwise identical.
    a, b = origin < cell ? (a, b) : (b, a)
    km = H3.Lib.greatCircleDistanceKm(Ref(a), Ref(b))
    ms = km * WALK_MS_PER_KM
    # Filter before converting: far-away cells can exceed UInt32 milliseconds.
    ms <= max_walk_ms || return nothing
    return (cell=cell, duration_ms=ceil(UInt32, ms), distance_km=km)
end

"""
    walking_neighbors(index, origin::UInt64, max_walk_ms::Integer=DEFAULT_MAX_WALK_MS)

Return graph destinations sorted by cell as `(cell, duration_ms, distance_km)`.
The origin may be off-graph but must be valid at the index resolution. Distances
are symmetric great-circle centre distances; durations are rounded up to integer
milliseconds, with an inclusive cutoff in `0:MAX_BUDGET_MS`. Zero disables walks
and self is always excluded. Large radii scan vertices instead of a huge bin cube.
"""
function walking_neighbors(index::WalkingIndex, origin::UInt64,
                           max_walk_ms::Integer=DEFAULT_MAX_WALK_MS)
    _walking_validate(index, origin, max_walk_ms)
    result = WalkingNeighbor[]
    (iszero(max_walk_ms) || isempty(index.cells)) && return result
    centre = H3.API.cellToLatLng(origin)::H3.API.LatLng
    xyz = _walking_xyz(centre)
    # Padding affects candidate discovery only, never the exact time cutoff.
    chord = 2sin(Float64(max_walk_ms) / WALK_MS_PER_KM / (2WALK_EARTH_RADIUS_KM)) + 1e-12
    ranges = map(x -> floor(Int, (x - chord) / WALK_BIN_WIDTH):
                      floor(Int, (x + chord) / WALK_BIN_WIDTH), xyz)
    candidates = if prod(length, ranges) > min(4096, length(index.cells))
        eachindex(index.cells)
    else
        Iterators.flatten(get(index.bins, bin, ()) for bin in Iterators.product(ranges...))
    end
    for i in candidates
        hop = _walking_neighbor(origin, index.cells[i], centre, index.centres[i], max_walk_ms)
        isnothing(hop) || push!(result, hop)
    end
    return sort!(result; by=hop -> hop.cell)
end

function _walking_rectangles(centre, max_walk_ms)
    # A spherical cap's latitude bounds are +/- angular radius; away from a
    # pole its longitude half-width is asin(sin(radius) / cos(latitude)).
    pad = 1e-12
    radius = Float64(max_walk_ms) / WALK_MS_PER_KM / WALK_EARTH_RADIUS_KM + pad
    south, north = max(-pi / 2, centre.lat - radius), min(pi / 2, centre.lat + radius)
    west, east = if abs(centre.lat) + radius >= pi / 2
        (-pi, pi)
    else
        halfwidth = asin(clamp(sin(radius) / cos(centre.lat), 0, 1)) + pad
        (centre.lng - halfwidth, centre.lng + halfwidth)
    end
    rectangles = Vector{H3.API.LatLng}[]
    # Intersect an unwrapped interval with shifted longitude quadrants. These
    # planar lat/lng rectangles cover polar caps too, without a pole-winding
    # polygon or an edge spanning >90 degrees (including at the antimeridian).
    for shift in (-2pi, 0.0, 2pi), quadrant in -2:1
        lo = max(west - shift, quadrant * (pi / 2))
        hi = min(east - shift, (quadrant + 1) * (pi / 2))
        lo < hi || continue
        push!(rectangles, [H3.API.LatLng(south, lo), H3.API.LatLng(south, hi),
                           H3.API.LatLng(north, hi), H3.API.LatLng(north, lo)])
    end
    return rectangles
end

function _walking_check_h3(code)
    iszero(code) || error("H3 walking enumeration failed: $(H3.API.describeH3Error(code))")
end

function _walking_disk(origin, centre, max_walk_ms)
    radius = Float64(max_walk_ms) / WALK_MS_PER_KM + 1e-6 # 1 mm discovery-only guard
    boundary = Ref{H3.Lib.CellBoundary}()
    # These attempts bound fast-path work, not output; failure uses polygon fill.
    for k in (1, 2, 4, 8)
        size = H3.API.maxGridDiskSize(k)::Int64
        cells, distances = zeros(UInt64, size), zeros(Cint, size)
        _walking_check_h3(H3.Lib.gridDiskDistances(origin, k, cells, distances))
        certified = true
        for i in eachindex(cells)
            (iszero(cells[i]) || distances[i] != k) && continue
            c = H3.API.cellToLatLng(cells[i])::H3.API.LatLng
            d = H3.Lib.greatCircleDistanceKm(Ref(centre), Ref(c))
            certified = d > radius
            certified || break
            _walking_check_h3(H3.Lib.cellToBoundary(cells[i], boundary))
            circumradius = 0.0
            for j in 1:boundary[].numVerts
                circumradius = max(circumradius,
                    H3.Lib.greatCircleDistanceKm(Ref(c), Ref(boundary[].verts[j])))
            end
            # H3 includes face-crossing vertices, so every boundary segment is a
            # short great-circle arc. A vertex-enclosing cap below a hemisphere
            # is geodesically convex and contains the whole cell polygon.
            certified = circumradius < pi / 2 * WALK_EARTH_RADIUS_KM && d > radius + circumradius
            certified || break
            cells[i] = 0 # The certified outer cell cannot be a destination.
        end
        # A cap disjoint from the outer ring cannot cross it to reach outside
        # this disk. An empty outer ring means the connected globe is covered.
        certified && return cells
    end
    return nothing
end

"""
    walking_cells(index, origin::UInt64, max_walk_ms::Integer=DEFAULT_MAX_WALK_MS)

Return all geographic destinations, including nonnetwork cells, in the same
format and with the same exact cutoff as `walking_neighbors`. Self is excluded.
A small grid disk is used only when its outer cell polygons provably miss the
walking cap; otherwise conservative spherical-cap rectangles are filled by H3.
Both paths use the same exact distance filter.
"""
function walking_cells(index::WalkingIndex, origin::UInt64,
                        max_walk_ms::Integer=DEFAULT_MAX_WALK_MS)
    _walking_validate(index, origin, max_walk_ms)
    result = WalkingNeighbor[]
    iszero(max_walk_ms) && return result
    centre = H3.API.cellToLatLng(origin)::H3.API.LatLng
    candidates = _walking_disk(origin, centre, max_walk_ms)
    isnothing(candidates) && return _walking_polygon_cells(index, origin, centre, max_walk_ms)
    for cell in candidates
        (iszero(cell) || cell == origin) && continue
        hop = _walking_neighbor(origin, cell, centre, H3.API.cellToLatLng(cell)::H3.API.LatLng, max_walk_ms)
        isnothing(hop) || push!(result, hop)
    end
    return sort!(result; by=hop -> hop.cell)
end

function _walking_polygon_cells(index, origin, centre, max_walk_ms)
    result = WalkingNeighbor[]
    rectangles = _walking_rectangles(centre, max_walk_ms)
    seen = Set{UInt64}()
    for vertices in rectangles
        size = Ref{Int64}(0)
        GC.@preserve vertices begin
            polygon = Ref(H3.Lib.GeoPolygon(H3.Lib.GeoLoop(length(vertices), pointer(vertices)), 0, C_NULL))
            _walking_check_h3(H3.Lib.maxPolygonToCellsSize(polygon, index.resolution, UInt32(0), size))
        end
        candidates = zeros(UInt64, size[])
        GC.@preserve vertices begin
            polygon = Ref(H3.Lib.GeoPolygon(H3.Lib.GeoLoop(length(vertices), pointer(vertices)), 0, C_NULL))
            _walking_check_h3(H3.Lib.polygonToCells(polygon, index.resolution, UInt32(0), candidates))
        end
        for cell in candidates
            (iszero(cell) || cell == origin || cell in seen) && continue
            hop = _walking_neighbor(origin, cell, centre, H3.API.cellToLatLng(cell)::H3.API.LatLng, max_walk_ms)
            isnothing(hop) && continue
            push!(seen, cell)
            push!(result, hop)
        end
    end
    return sort!(result; by=hop -> hop.cell)
end
