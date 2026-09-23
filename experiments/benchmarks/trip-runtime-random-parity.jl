using Test, Random
include("../../router/src/Reachability.jl")
using .Reachability
import H3
module Before
    include(joinpath(ENV["TRIP_BASELINE_SRC"], "Reachability.jl"))
end
const B = Before.Reachability
rng = MersenneTwister(42)
@testset "Trip runtime random parity" begin
    for resolution in (5,6,8), trial in 1:12
        h = H3.API.latLngToCell(H3.API.LatLng(deg2rad(51.5),deg2rad(-0.1)),resolution)
        cells = H3.API.gridDisk(h,1)
        table = (from_h3=rand(rng,cells,80),to_h3=rand(rng,cells,80),
            departure_ms=rand(rng,UInt32(0):UInt32(86_399_999),80),
            duration_ms=rand(rng,Int64(0):Int64(3_600_000),80),
            trip_id=string.(rand(rng,1:12,80)),distance_km=rand(rng,80))
        new=pack_graph(table); old=B.pack_graph(table)
        children=[first(H3.API.cellToChildren(c,8)) for c in cells]
        np=Reachability._population(children,ones(length(cells)))
        bp=B._population(children,ones(length(cells)))
        ready=rand(rng,0:86_399_999); budget=rand(rng,1:172_800_000)
        @test route_cpu(new,h,ready,budget) == B.route_cpu(old,h,ready,budget)
        for walk in (0,3_600_000)
            a=route_walking(new,h,ready,budget;max_walk_ms=walk)
            b=B.route_walking(old,h,ready,budget;max_walk_ms=walk)
            @test a.h3 == b.h3
            @test a.arrival == b.arrival
            @test isequal(a.distance_km,b.distance_km)
            a=route_population(new,np,h,ready,budget;max_walk_ms=walk,origin_radius=1,window_ms=900_000,step_ms=300_000)
            b=B.route_population(old,bp,h,ready,budget;max_walk_ms=walk,origin_radius=1,window_ms=900_000,step_ms=300_000)
            @test a.h3 == b.h3
            @test a.value ≈ b.value
        end
    end
end
