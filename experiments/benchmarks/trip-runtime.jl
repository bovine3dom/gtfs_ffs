using Profile
include("../../router/src/Reachability.jl")
using .Reachability
import H3

length(ARGS) >= 1 || error("usage: trip-runtime.jl SHARD [OUTPUT] [BUDGET_H]")
path = ARGS[1]
output = length(ARGS) >= 2 ? ARGS[2] : "/tmp/trip-runtime"
budget = round(Int, (length(ARGS) >= 3 ? parse(Float64, ARGS[3]) : 3.0) * 3_600_000)
graph = Reachability.read_trip_shard(path)
println("nodes=", length(graph.h3), " edges=", length(graph.edge_to), " events=", length(graph.departure))
# Select named real origins; do not choose a tiny disconnected test graph.
origins = [("London",51.5074,-0.1278), ("Paris",48.8566,2.3522), ("Berlin",52.52,13.405)]
open(output * ".csv", "w") do io
    println(io,"city,budget_ms,seconds,allocated_bytes,gc_seconds,reached,",join(string.(fieldnames(TripRouteStats)),","))
    for (city, lat, lon) in origins
        centre = H3.API.LatLng(deg2rad(lat),deg2rad(lon))
        origin = H3.API.latLngToCell(centre,graph.resolution)
        haskey(graph.node_id,origin) || continue
        ready = 8*3_600_000
        route_cpu(graph,origin,ready,min(budget,60_000);stats=TripRouteStats()) # Compile separately.
        stats = TripRouteStats()
        route_cpu(graph,origin,ready,budget) # Warm pages and the uninstrumented path.
        result = @timed route_cpu(graph,origin,ready,budget;stats)
        println(io,join((city,budget,result.time,result.bytes,result.gctime,
            count(!=(Reachability.INF),result.value),
            (getfield(stats,f) for f in fieldnames(TripRouteStats))...),","))
        flush(io)
        println(city," seconds=",result.time," allocated=",result.bytes," scans=",stats.event_rows_scanned," states=",stats.state_pops)
        Profile.clear()
        @profile route_cpu(graph,origin,ready,budget)
        open(output * "-" * city * ".profile", "w") do p
            Profile.print(p;format=:flat,sortedby=:count,mincount=5)
        end
    end
end
