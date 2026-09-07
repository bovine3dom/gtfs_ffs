# Read-only concatenation keeps the large input columns resident without copying them.
struct _PatchedColumn{T,A<:AbstractVector{T}} <: AbstractVector{T}
    original::A
    extra::Vector{T}
end
Base.size(c::_PatchedColumn) = (length(c.original) + length(c.extra),)
Base.IndexStyle(::Type{<:_PatchedColumn}) = IndexLinear()
@inline function Base.getindex(c::_PatchedColumn, i::Int)
    @boundscheck checkbounds(c, i)
    n = length(c.original)
    @inbounds return i <= n ? c.original[i] : c.extra[i - n]
end

"""Original fantasy rail shuttle; OSM station provenance is recorded in README.md."""
function _badajoz_shuttle(resolution)
    elvas, badajoz = map(((38.8955418, -7.1422766), (38.8907326, -6.9816158))) do (lat, lon)
        # Match the export's res11-parent convention, not direct coarse geocoding.
        h = H3.API.latLngToCell(H3.API.LatLng(deg2rad(lat), deg2rad(lon)), max(11, resolution))
        H3.API.cellToParent(h, resolution)
    end
    departures = collect(UInt32(14_400_000):UInt32(60_000):UInt32(84_600_000))
    n = length(departures)
    return (from_h3=[fill(elvas, n); fill(badajoz, n)],
            to_h3=[fill(badajoz, n); fill(elvas, n)],
            departure_ms=[departures; departures], duration_ms=fill(Int64(900_000), 2n),
            distance_km=fill(13.88, 2n))
end
