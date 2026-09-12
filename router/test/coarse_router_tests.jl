if !isdefined(@__MODULE__, :Reachability)
    include("../src/Reachability.jl")
end
isdefined(Reachability, :CoarseRouter) || Base.include(Reachability, joinpath(@__DIR__, "../src/coarse.jl"))

module CoarseRouterTests
using Test, H3, Random
import ..Reachability as R
cell(lat, lon, res=8) = H3.API.latLngToCell(H3.API.LatLng(deg2rad(lat), deg2rad(lon)), res)::UInt64
const MODES = (:mean_intersection, :max_intersection, :diff_intersection, :min_union, :diff_union, :reachable_union)
const W = 3_600_000
table(a, b, d, t; km=fill(10.0, length(a))) = (; from_h3=a, to_h3=b, departure_ms=UInt32.(d), duration_ms=Int64.(t), distance_km=km)
pop(cells) = R._population(sort!(unique(cells)), fill(10.0, length(unique(cells))))

@testset "Global models, all resolution pairs, and unseen origins" begin
    for resolution in 6:8
        a, b, c = cell(48.85, 2.35, resolution), cell(51.5, -0.12, resolution), cell(47.8, 13.0, resolution)
        f = R.pack_graph(table([a,b], [b,c], [1000,2000], [100,100]))
        wi = R.prepare_walking(R.WalkingIndex(f))
        p = pop([cell(48.85,2.35), cell(51.5,-0.12), cell(47.8,13.0)])
        for target in 5:(resolution-1)
            g = R.prepare_coarse_router(f, wi, target; population=p)
            refs = g.refs
            @test g.fine === f && g.walking === wi && g.core_resolution == target
            @test length(g.h3) == length(unique(H3.API.cellToParent.(f.h3, target)))
            @test all(1 .<= g.refs .<= length(f.arrival))
            for o in (a, b, c, cell(-33.86,151.2,resolution)), walk in (0, 37*60_000, W), mode in MODES
                options = (; window_ms=4000, step_ms=1000, max_walk_ms=walk, window_mode=mode)
                result = R.route_coarse_population(g, [o,o], 0, 10_000; options...)
                fine = R.route_population(f, p, o, 0, 10_000; walking_index=wi, options...)
                @test result.h3 == [o]
                @test isapprox(result.value, fine.value)
                excluded = R.route_coarse_population(g, [o], 0, 10_000; options..., exclude_origin_population=true)
                @test isapprox(excluded.value[1], result.value[1] - get(R._population_rollup(p,resolution),o,0.0))
            end
            @test g.refs === refs
            point = R.route_coarse_time(g,a,0,10_000,0,60_000,W,:itinerary,:mean_intersection)
            exact = R.route_walking(f,a,0,10_000;walking_index=wi)
            @test point.h3 == exact.h3 && point.arrival == exact.arrival
            @test isequal(point.distance_km,exact.distance_km)
            @test_throws ArgumentError R.route_coarse_population(g, [a], 0, 2W; max_walk_ms=W+1)
            @test_throws ArgumentError R.route_coarse_population(g, [a], 0, W; origin_batch_size=65)
            @test_throws ArgumentError R.route_coarse_population(g, [a], 0, W; window_ms=-1)
            @test_throws ArgumentError R.route_coarse_population(g, UInt64[], 0, W)
            @test R.route_coarse_population(g, [a], 0, 0).value == [get(R._population_rollup(p,resolution),a,0.0)]
        end
        @test_throws ArgumentError R.prepare_coarse_router(f, wi, resolution; population=p)
        @test_throws ArgumentError R.prepare_coarse_router(f, wi, 4; population=p)
    end
end

@testset "Child tags above 255 and real first boarding" begin
    parent = cell(48.85,2.35,5)
    children = sort!(H3.API.cellToChildren(parent,8))
    @test length(children) == 343
    a, z = cell(51.5,-0.12), cell(47.8,13.0)
    b = children[300]
    f = R.pack_graph(table(vcat(children,[a,b]), vcat(children,[b,z]),
        vcat(fill(40_000_000,343),[1000,1100]), vcat(zeros(Int,343),[100,100])))
    wi = R.prepare_walking(R.WalkingIndex(f))
    g = R.prepare_coarse_router(f, wi, 5; population=pop([a,z]))
    @test g.tags[f.node_id[b]] == 300
    @test maximum(g.boarding) == 343
    @test R.route_coarse_population(g,[a],0,2000).value == [20.0]
    point = R.route_coarse_time(g,a,0,2000,0,60_000,W,:itinerary,:mean_intersection)
    @test point.arrival[findfirst(==(z),point.h3)] == 1200
    @test point.distance_km[findfirst(==(z),point.h3)] == 20.0
    # Without a fine initial connection, a same-parent source must not teleport.
    far = first(children)
    @test R.route_coarse_population(g,[far],0,2000).value == [0.0]
    origins = vcat([a], children[270:332])
    for batch in (1,16,64), mode in MODES
        x = R.route_coarse_population(g,origins,0,2000; window_ms=960,step_ms=10,window_mode=mode,origin_batch_size=batch)
        y = R.route_coarse_population(g,origins,0,2000; window_ms=960,step_ms=10,window_mode=mode,origin_batch_size=64)
        @test x.h3 == y.h3 && isapprox(x.value, y.value)
    end
end

@testset "Time outputs, distances, midnight, and window reducers" begin
    a,b,c = cell(48.85,2.35),cell(51.5,-0.12),cell(47.8,13.0)
    P = Int(R.PERIOD)
    f = R.pack_graph(table([a,a,b],[b,b,c],[P-1000,1000,1200],[500,100,100]; km=[500.0,600.0,700.0]))
    wi = R.prepare_walking(R.WalkingIndex(f))
    g = R.prepare_coarse_router(f,wi,6)
    @test_throws ArgumentError R.route_coarse_population(g,[a],0,W)
    for o in (a,b,cell(-33.86,151.2)), ready in (0,P-1500), mode in MODES, distance in (:itinerary,:straight_line)
        point = R.route_coarse_time(g,o,ready,10_000,0,1000,W,distance,mode)
        reference = R.route_walking(f,o,ready,10_000;max_walk_ms=W,walking_index=wi,distance_mode=distance)
        @test point.h3 == reference.h3
        @test point.arrival == reference.arrival
        @test isequal(point.distance_km,reference.distance_km)
        result = R.route_coarse_time(g,o,ready,10_000,4000,1000,W,distance,mode)
        acc = R._walking_window_accumulator(distance == :itinerary, mode)
        for sample in reverse(0:3)
            clock = UInt32(ready+sample*1000)
            p = R._walking_route_at(f,R.WalkingTopology(wi,10_000),o,clock,clock+UInt32(10_000),distance == :itinerary)
            distance == :straight_line && (p = merge(p,(distance_km=R._od_distances(o,p.h3),)))
            R._accumulate_walking!(acc,p,clock,UInt32(10_000),4)
        end
        expected = R._finish_walking_window(acc,4;budget=UInt32(10_000),origin=o)
        for field in (:h3,:elapsed_ms,:reachable_elapsed_ms,:reachable_samples,:distance_km,:sample_count)
            @test isequal(getproperty(result,field),getproperty(expected,field))
        end
        @test all(h -> H3.API.getResolution(h)==8,result.h3)
    end
    @test_throws ArgumentError R.route_coarse_time(g,a,0,W,1000,0,W,:itinerary,:mean_intersection)
    @test_throws ArgumentError R.route_coarse_time(g,a,0,2W,0,1000,W+1,:itinerary,:mean_intersection)
end
@testset "Equal window extrema use the earlier departure distance" begin
    a,b = cell(48.85,2.35),cell(51.5,-0.12)
    f = R.pack_graph(table([a,a],[b,b],[100,110],[100,100];km=[10.0,20.0]))
    wi = R.prepare_walking(R.WalkingIndex(f))
    g = R.prepare_coarse_router(f,wi,6)
    for mode in (:min_union,:max_intersection,:diff_union,:diff_intersection)
        result = R.route_coarse_time(g,a,100,1000,20,10,W,:itinerary,mode)
        @test result.distance_km[findfirst(==(b),result.h3)] == 10.0
        @test result.profile_lookups > 0
    end
end

@testset "64 lanes with distinct large child tags" begin
    children = sort!(H3.API.cellToChildren(cell(48.85,2.35,5),8))
    z = cell(51.5,-0.12)
    f = R.pack_graph(table(vcat(children,[children[300]]),vcat(children,[z]),
        vcat(zeros(Int,343),[100]),vcat(zeros(Int,343),[100])))
    wi = R.prepare_walking(R.WalkingIndex(f))
    g = R.prepare_coarse_router(f,wi,5;population=pop([z]))
    origins = children[270:333]
    for batch in (1,16,64), mode in MODES
        result = R.route_coarse_population(g,origins,0,2000;window_ms=96,step_ms=1,window_mode=mode,origin_batch_size=batch)
        @test isapprox(result.value, [h == children[300] ? 10.0 : 0.0 for h in origins])
    end
end

@testset "Exact boarding gap and variable walking limit" begin
    siblings = sort!(H3.API.cellToChildren(cell(48.85,2.35,7),8))
    a,b,c,z = cell(51.5,-0.12),siblings[1],siblings[2],cell(47.8,13.0)
    km = H3.Lib.greatCircleDistanceKm(Ref(H3.API.cellToLatLng(b)),Ref(H3.API.cellToLatLng(c)))
    gap = ceil(Int,km*R.WALK_MS_PER_KM)
    f = R.pack_graph(table([a,c],[b,z],[0,100+gap],[100,1];km=[500.0,700.0]))
    wi = R.prepare_walking(R.WalkingIndex(f))
    g = R.prepare_coarse_router(f,wi,7;population=pop([z]))
    @test R.route_coarse_population(g,[a],0,2W;max_walk_ms=gap).value == [10.0]
    @test R.route_coarse_population(g,[a],0,2W;max_walk_ms=gap-1).value == [0.0]
    point = R.route_coarse_time(g,a,0,2W,0,60_000,gap,:itinerary,:mean_intersection)
    @test point.arrival[findfirst(==(z),point.h3)] == 101+gap
    @test isapprox(point.distance_km[findfirst(==(z),point.h3)], 1200+km)
end

@testset "Population and time use the same approximate coverage" begin
    rng = MersenneTwister(6028)
    cells = vcat(sort!(H3.API.cellToChildren(cell(48.85,2.35,7),8)),
        [cell(51.5,-0.12),cell(47.8,13.0)])
    population = pop(cells)
    for trial in 1:20
        f = R.pack_graph(table(rand(rng,cells,35),rand(rng,cells,35),rand(rng,0:60_000:W,35),rand(rng,0:60_000:W,35)))
        wi = R.prepare_walking(R.WalkingIndex(f))
        g = R.prepare_coarse_router(f,wi,6;population)
        origin = rand(rng,cells)
        for mode in MODES
            point = R.route_coarse_time(g,origin,0,W,240_000,60_000,W,:straight_line,mode)
            actual = R.route_coarse_population(g,[origin],0,W;window_ms=240_000,step_ms=60_000,window_mode=mode)
            expected = 0.0
            for (i,h) in enumerate(point.h3)
                h in cells || continue
                hits = point.reachable_samples[i]
                factor = mode == :reachable_union ? hits/point.sample_count :
                    mode in (:min_union,:diff_union) ? 1 : hits == point.sample_count
                expected += 10factor
            end
            @test isapprox(actual.value, [expected])
        end
    end
end

@testset "Off-network access and population outside global geometry" begin
    a,b = cell(48.85,2.35),cell(51.5,-0.12)
    near = first(filter(!=(a),H3.API.gridDisk(a,1)))
    remote = cell(-33.86,151.2)
    f = R.pack_graph(table([a],[b],[1_000_000],[60_000]))
    wi = R.prepare_walking(R.WalkingIndex(f))
    p = pop([near,remote,b])
    g = R.prepare_coarse_router(f,wi,6;population=p)
    @test !haskey(g.prepared.output_id,remote)
    @test R.route_coarse_population(g,[remote],0,W).value == [10.0]
    @test R.route_coarse_population(g,[remote],0,W;exclude_origin_population=true).value == [0.0]
    for mode in MODES
        options = (;window_ms=96*60_000,step_ms=60_000,window_mode=mode)
        coarse = R.route_coarse_population(g,[near],0,W;options...)
        fine = R.route_population(f,p,near,0,W;walking_index=wi,options...)
        @test isapprox(coarse.value, fine.value)
    end
end
end
