# Experimental process only. Do not include this file in the server.
using Test
include("../../router/src/Reachability.jl")
using .Reachability
import H3
const R = Reachability
function index_trips(g)
    offsets = Int[1]
    pairs = UInt64[]
    for node in eachindex(g.h3)
        start = length(pairs)+1
        for edge in g.out_ptr[node]:g.out_ptr[node+1]-1
            last = UInt32(0)
            for row in g.schedule_ptr[edge]:g.schedule_ptr[edge+1]-1
                trip = g.trip_id[row]
                trip == last || push!(pairs, (UInt64(trip)<<32) | UInt64(edge))
                last = trip
            end
        end
        sort!(@view pairs[start:end])
        push!(offsets,length(pairs)+1)
    end
    offsets,pairs
end
path=only(ARGS)
g=R.read_trip_shard(path)
build=@timed index_trips(g)
println("index_seconds=",build.time," index_bytes=",Base.summarysize(build.value)," peak_rss=",Sys.maxrss())
origins=[H3.API.latLngToCell(H3.API.LatLng(deg2rad(lat),deg2rad(lon)),g.resolution)
    for (lat,lon) in ((51.5074,-0.1278),(48.8566,2.3522),(52.52,13.405))]
filter!(h->haskey(g.node_id,h),origins)
expected=[route_cpu(g,h,28_800_000,10_800_000) for h in origins]
before=[minimum((@elapsed route_cpu(g,h,28_800_000,10_800_000)) for _ in 1:3) for h in origins]
@eval R begin
    const PROBE_INDEX = $(build.value)
    function probe_edges(node,trip)
        offsets,pairs=PROBE_INDEX
        entries=@view pairs[offsets[node]:offsets[node+1]-1]
        lo=searchsortedfirst(entries,UInt64(trip)<<32)
        hi=searchsortedlast(entries,(UInt64(trip)<<32)|UInt64(0xffffffff))
        (Int32(x & UInt64(0xffffffff)) for x in @view entries[lo:hi])
    end
end
source=read(joinpath(@__DIR__,"../../router/src/Reachability.jl"),String)
body=split(split(source,"function _route_trip_at";limit=2)[2],"include(\"graph_resolution.jl\")";limit=2)[1]
body=replace("function _route_trip_at" * body,
    "for edge in graph.out_ptr[u]:(graph.out_ptr[u + 1] - Int32(1))" =>
    "for edge in (scan_transfers ? (graph.out_ptr[u]:(graph.out_ptr[u + 1] - Int32(1))) : probe_edges(u,current_trip))")
Base.include_string(R,body,"continuation-probe")
for (i,h) in enumerate(origins)
    @test route_cpu(g,h,28_800_000,10_800_000) == expected[i]
    after=minimum((@elapsed route_cpu(g,h,28_800_000,10_800_000)) for _ in 1:3)
    println("origin=",string(h;base=16)," before=",before[i]," after=",after," speedup=",before[i]/after)
end
