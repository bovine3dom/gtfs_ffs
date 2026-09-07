const DEFAULT_MAX_WALK_MS = UInt32(3_600_000)
const WALK_MS_PER_KM = 720_000.0 # 5 km/h
const WALK_EARTH_RADIUS_KM = 6371.007180918475 # H3's WGS84 authalic radius
const WALK_BIN_WIDTH = 2sin(5 / (2WALK_EARTH_RADIUS_KM))
const WALK_MAX_CANDIDATES = 2_000_000
const WalkingNeighbor = @NamedTuple{cell::UInt64, duration_ms::UInt32, distance_km::Float64}

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
    return WalkingIndex(cells, centres, bins, graph.resolution)
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
                           max_walk_ms::Integer=DEFAULT_MAX_WALK_MS; work=nothing)
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
        isnothing(work) || _walking_work!(work, 1)
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

"""
    walking_cells(index, origin::UInt64, max_walk_ms::Integer=DEFAULT_MAX_WALK_MS;
                        max_cells::Integer=250_000)

Return all geographic destinations, including nonnetwork cells, in the same
format and with the same exact cutoff as `walking_neighbors`. Self is excluded.
Conservative spherical-cap rectangles are filled by H3, then distance-filtered.
Reject rather than truncate if output exceeds `max_cells` or cumulative H3
candidate capacity exceeds 2,000,000 slots. The latter is checked before any fill
allocation and can reject a request whose exact output would fit `max_cells`.
"""
function walking_cells(index::WalkingIndex, origin::UInt64,
                        max_walk_ms::Integer=DEFAULT_MAX_WALK_MS;
                        max_cells::Integer=250_000, work=nothing)
    _walking_validate(index, origin, max_walk_ms)
    0 <= max_cells <= typemax(Int) || throw(ArgumentError("invalid max_cells"))
    result = WalkingNeighbor[]
    iszero(max_walk_ms) && return result
    centre = H3.API.cellToLatLng(origin)::H3.API.LatLng
    rectangles = _walking_rectangles(centre, max_walk_ms)
    sizes = Int[]
    total = 0
    for vertices in rectangles
        GC.@preserve vertices begin
            polygon = Ref(H3.Lib.GeoPolygon(H3.Lib.GeoLoop(length(vertices), pointer(vertices)), 0, C_NULL))
            size = Ref{Int64}(0)
            _walking_check_h3(H3.Lib.maxPolygonToCellsSize(polygon, index.resolution, UInt32(0), size))
            0 <= size[] <= WALK_MAX_CANDIDATES - total ||
                throw(WalkingLimitError("walking geographic candidate capacity exceeds $WALK_MAX_CANDIDATES"))
            push!(sizes, size[])
            total += size[]
        end
    end
    isnothing(work) || _walking_work!(work, total)
    seen = Set{UInt64}()
    for (vertices, size) in zip(rectangles, sizes)
        candidates = zeros(UInt64, size)
        GC.@preserve vertices begin
            polygon = Ref(H3.Lib.GeoPolygon(H3.Lib.GeoLoop(length(vertices), pointer(vertices)), 0, C_NULL))
            _walking_check_h3(H3.Lib.polygonToCells(polygon, index.resolution, UInt32(0), candidates))
        end
        for cell in candidates
            (iszero(cell) || cell == origin || cell in seen) && continue
            hop = _walking_neighbor(origin, cell, centre, H3.API.cellToLatLng(cell)::H3.API.LatLng, max_walk_ms)
            isnothing(hop) && continue
            length(result) < max_cells || throw(WalkingLimitError("walking geographic output exceeds max_cells=$max_cells"))
            push!(seen, cell)
            push!(result, hop)
        end
    end
    return sort!(result; by=hop -> hop.cell)
end
