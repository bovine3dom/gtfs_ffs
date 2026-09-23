using Test, Serialization, Profile
include("../../router/src/Reachability.jl")
using .Reachability
import H3
module Before
    include(joinpath(ENV["TRIP_BASELINE_SRC"], "Reachability.jl"))
end
const B = Before.Reachability
path, output = ARGS
budget = round(Int, parse(Float64, get(ENV, "TRIP_BUDGET_H", "3")) * 3_600_000)
radius = parse(Int, get(ENV, "TRIP_ORIGIN_RADIUS", "1"))
new = Reachability.read_trip_shard(path)
old = B.read_trip_shard(path)
ni = open(deserialize, path * ".walking")
bi = B.WalkingIndex(ni.cells, ni.centres, ni.bins, ni.resolution,
    B.prepare_walking(B.WalkingIndex(old)).prepared)
population_path = get(ENV,"TRIP_POPULATION","")
np, bp = if isempty(population_path)
    # Identical sparse weights on a real full-size transit graph.
    children = [first(H3.API.cellToChildren(h,8)) for h in new.h3]
    (Reachability._population(children, ones(length(children))),
        B._population(children, ones(length(children))))
else
    (Reachability.load_population(population_path), B.load_population(population_path))
end
open(output,"w") do io
    println(io,"city,mode,before_s,after_s,speedup,before_bytes,after_bytes")
    for (city,lat,lon) in (("London",51.5074,-0.1278),("Paris",48.8566,2.3522),("Berlin",52.52,13.405))
        h=H3.API.latLngToCell(H3.API.LatLng(deg2rad(lat),deg2rad(lon)),new.resolution)
        haskey(new.node_id,h) || continue
        for mode in (:point,:walking,:population,:window)
            function query(m,g,index,pop;stats=nothing)
                mode == :point && return m.route_cpu(g,h,28_800_000,budget;stats)
                mode == :walking && return m.route_walking(g,h,28_800_000,budget;walking_index=index,max_walk_ms=3_600_000,stats)
                m.route_population(g,pop,h,28_800_000,budget;walking_index=index,max_walk_ms=3_600_000,origin_radius=radius,
                    window_ms=mode == :window ? 900_000 : 0, step_ms=300_000,stats)
            end
            a=query(B,old,bi,bp); b=query(Reachability,new,ni,np)
            if mode == :point
                @test a == b
            elseif mode == :walking
                @test a.h3 == b.h3
                @test a.arrival == b.arrival
                @test isequal(a.distance_km,b.distance_km)
            else
                @test a.h3 == b.h3
                @test a.value ≈ b.value
            end
            before=[@timed query(B,old,bi,bp) for _ in 1:3]
            after=[@timed query(Reachability,new,ni,np) for _ in 1:3]
            x=minimum(t.time for t in before); y=minimum(t.time for t in after)
            println(io,join((city,mode,x,y,x/y,minimum(t.bytes for t in before),minimum(t.bytes for t in after)),","));flush(io)
            println(city," ",mode," ",round(x/y;digits=2),"x peak_rss_bytes=",Sys.maxrss())
            if get(ENV,"TRIP_COUNTERS","") == "1"
                counters = TripRouteStats()
                query(Reachability,new,ni,np;stats=counters)
                println("counters ",city," ",mode," ",
                    NamedTuple{fieldnames(TripRouteStats)}(Tuple(getfield(counters,f) for f in fieldnames(TripRouteStats))))
            end
            if get(ENV,"TRIP_PROFILE","") == "1"
                Profile.clear()
                @profile query(Reachability,new,ni,np)
                open(output * "-$(city)-$(mode).profile","w") do log
                    Profile.print(log;format=:flat,sortedby=:count,mincount=3)
                end
            end
        end
    end
end
