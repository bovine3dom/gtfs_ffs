using Test
include("../../router/src/Reachability.jl")
using .Reachability
import H3
const R = Reachability

path = only(ARGS)
graph = R.read_trip_shard(path;continuation=false)
build = @timed R.prepare_continuation(graph)
indexed = R._with_continuation(graph,build.value)
println("index_seconds=",build.time," index_bytes=",Base.summarysize(build.value),
    " peak_rss=",Sys.maxrss())
for (lat,lon) in ((51.5074,-0.1278),(48.8566,2.3522),(52.52,13.405))
    origin = H3.API.latLngToCell(H3.API.LatLng(deg2rad(lat),deg2rad(lon)),graph.resolution)
    haskey(graph.node_id,origin) || continue
    expected = route_cpu(graph,origin,28_800_000,10_800_000)
    @test route_cpu(indexed,origin,28_800_000,10_800_000) == expected
    before = minimum((@elapsed route_cpu(graph,origin,28_800_000,10_800_000)) for _ in 1:3)
    after = minimum((@elapsed route_cpu(indexed,origin,28_800_000,10_800_000)) for _ in 1:3)
    println("origin=",string(origin;base=16)," before=",before," after=",after,
        " speedup=",before/after)
end
