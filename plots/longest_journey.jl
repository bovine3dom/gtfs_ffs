struct RouteStateW
    time::DateTime
    parent_h3::UInt64
    parent_was_walk::Bool
    distance::Float64
    action::String
end

# - fix departure time
# - find the fastest route between every pair of stations, store the sum of geodesic distances
# - find the longest sum of geodesic distances
# 
# 
# done:
# -> group by smallish h3, say 11
# -> allow walks by looking at gdfs in a ring around it
# -> walking time by measuring distance between h3s and adjusting horizon?
# -> allow overnight with modulo maths but +n?
# -> prevent chaining walks
# -> keep track of distance travelled and route taken
# -> make list of candidate stations to start at?
#
# not done:
# -> decide on allowable start times and intervals between them?
# -> decide whether to make a real timetable rather than the fantasy one?
# -> fix elvas/badajoz connection in transitous because currently we have to go north from portugal
#    which is unrealistic
# 
# i guess in router.jl earliest arrivals needs to become (time, route)[] pairs?
using JSON, CSV, DataFrames, Dates, Arrow
import H3

include("lib.jl")

# h3_11 originally. oddly it's reasonably invariant to lower resolutions. 10 -> 55k unique; 8 -> 50k unique
edgelist = select_df(con(), """
    select distinct toTime(departure_time) departure_time, h3ToParent(h3, 10) h3, stop_name, h3ToParent(next_h3,10) next_h3, travel_time,
    geoDistance(stop_lon, stop_lat, next_lon, next_lat)/1000 distance
    from transitous_everything_20260218_edgelist_fahrtle2
    where route_type = 2 or route_type between 100 and 117 -- only trains
    order by h3, next_h3, departure_time
""")
edgelist.departure_time = Time.(edgelist.departure_time)

# add the bovine3dom sonderfahrt shuttle
elvas = first(edgelist[edgelist.stop_name .== "Elvas", :h3])
badajoz = first(edgelist[edgelist.stop_name .== "Badajoz", :h3])
distance = 13.88 # i really looked this up
travel_time = 15 # thank u scotty
departures = Time(04, 00):Minute(1):Time(23, 30) |> collect # it may be less frequent than this in reality
outbound = eachrow(DataFrame(h3=elvas, next_h3=badajoz, departure_time=departures, travel_time=travel_time, distance=distance, stop_name="Elvas"))
inbound = eachrow(DataFrame(h3=badajoz, next_h3=elvas, departure_time=departures, travel_time=travel_time, distance=distance, stop_name="Badajoz"))
push!.(Ref(edgelist), outbound);
push!.(Ref(edgelist), inbound);

gdf = groupby(edgelist, [:h3, :next_h3])

adj_list = Dict{UInt64, Vector{UInt64}}()
for (h3, next_h3) in keys(gdf)
    push!(get!(adj_list, h3, UInt64[]), next_h3)
end

friendly_keys = sort(unique(map(x->x.h3, keys(gdf))))

function get_walkable_neighbours(h3_pos::UInt64, friendly_keys::Vector{UInt64}, max_dist=4)
    (ring, distances) = H3.API.gridDiskDistances(h3_pos, max_dist)
    area_km2 = H3.API.cellAreaKm2(h3_pos)
    dist_multiplier = (sqrt(area_km2 / 3) * 2) / 5
    walks = Tuple{UInt64, Dates.Millisecond}[]
    for i in eachindex(ring)
        r = ring[i]
        d = distances[i]
        if d > 0 && insorted(r, friendly_keys)
            hours_walk = d * dist_multiplier
            millis = Dates.Millisecond(round(Int64, hours_walk * 3.6e6))
            push!(walks, (r, millis))
        end
    end
    return walks
end

# julia> gdf
# GroupedDataFrame with 163192 groups based on keys: h3, next_h3
# First Group (1 row): h3 = 0x08a08021a2a07fff, next_h3 = 0x08a080239b20ffff
#  Row │ departure_time  distance  h3                  next_h3             stop_name          travel_time 
#      │ Time            Float64   UInt64              UInt64              String             Int64       
# ─────┼──────────────────────────────────────────────────────────────────────────────────────────────────
#    1 │ 17:07:00         46.9677  621637630527832063  621637638991970303  Steinkjer stasjon           52
# ⋮
# Last Group (180 rows): h3 = 0x08ac9934b56cffff, next_h3 = 0x08ac9934a30e7fff
#  Row │ departure_time  distance  h3                  next_h3             stop_name               travel_time 
#      │ Time            Float64   UInt64              UInt64              String                  Int64       
# ─────┼───────────────────────────────────────────────────────────────────────────────────────────────────────
#    1 │ 00:09:00         3.99428  625042899958824959  625042899650641919  Kwinana Stn Platform 2            3
#    2 │ 00:14:00         3.99428  625042899958824959  625042899650641919  Kwinana Stn Platform 2            3
#    3 │ 00:24:00         3.99428  625042899958824959  625042899650641919  Kwinana Stn Platform 2            3
#    4 │ 00:32:00         3.99428  625042899958824959  625042899650641919  Kwinana Stn Platform 2            3
#    5 │ 00:44:00         3.99428  625042899958824959  625042899650641919  Kwinana Stn Platform 2            3

function find_earliest_arrivals(edge_gdf::GroupedDataFrame, adj_list::Dict, start_uuid::UInt64, initial_arrival_time::DateTime, cutoff_time::DateTime, friendly_keys)
    earliest_arrivals = Dict{Tuple{UInt64, Bool}, RouteStateW}()
    arrivals_lock = Base.Threads.SpinLock()
    processing_queue = Tuple{UInt64, Bool, RouteStateW}[]
    initial_state = RouteStateW(initial_arrival_time, start_uuid, false, 0.0, "Start")
    earliest_arrivals[(start_uuid, false)] = initial_state
    push!(processing_queue, (start_uuid, false, initial_state))

    while !isempty(processing_queue)
        newly_discovered_channel = Channel{Tuple{UInt64, Bool, RouteStateW}}(Inf)
        current_batch = copy(processing_queue)
        empty!(processing_queue)

        Threads.@threads for (current_h3, current_was_walk, current_state) in current_batch
            current_arrival = current_state.time
            current_dist = current_state.distance
            
            # trains
            if haskey(adj_list, current_h3)
                for next_h3 in adj_list[current_h3]
                    t = edge_gdf[(h3=current_h3, next_h3=next_h3)]
                    current_clock = Dates.Time(current_arrival)
                    
                    idx = searchsortedfirst(t.departure_time, current_clock)
                    wrapped = idx > nrow(t)
                    row = t[wrapped ? 1 : idx, :]
                    
                    dep_dt = DateTime(Dates.Date(current_arrival), row.departure_time)
                    if wrapped; dep_dt += Dates.Day(1); end
                    arrival_at_next = dep_dt + Minute(row.travel_time)

                    if arrival_at_next <= cutoff_time
                        new_dist = current_dist + row.distance
                        new_state = RouteStateW(arrival_at_next, current_h3, current_was_walk, new_dist, row.stop_name)
                        next_key = (next_h3, false)

                        lock(arrivals_lock)
                        try
                            if !haskey(earliest_arrivals, next_key) || arrival_at_next < earliest_arrivals[next_key].time
                                earliest_arrivals[next_key] = new_state
                                put!(newly_discovered_channel, (next_h3, false, new_state))
                            end
                        finally
                            unlock(arrivals_lock)
                        end
                    end
                end
            end

            # walking but disallow chaining walks
            if !current_was_walk
                (ring, distances) = H3.API.gridDiskDistances(current_h3, 4)
                area_factor = sqrt(H3.API.cellAreaKm2(current_h3) / 3) * 2
                
                for i in 2:length(ring) # skip self
                    neighbor_h3 = ring[i]
                    !insorted(neighbor_h3, friendly_keys) && continue
                    
                    walk_dist_km = distances[i] * area_factor
                    walk_millis = Dates.Millisecond(round(Int, (walk_dist_km / 5) * 3.6e6))
                    arrival_via_walk = current_arrival + walk_millis
                    
                    if arrival_via_walk <= cutoff_time
                        new_dist = current_dist + walk_dist_km
                        new_state = RouteStateW(arrival_via_walk, current_h3, current_was_walk, new_dist, "Walk")
                        next_key = (neighbor_h3, true)
                        
                        lock(arrivals_lock)
                        try
                            transit_key = (neighbor_h3, false)
                            transit_beat_walk = haskey(earliest_arrivals, transit_key) && earliest_arrivals[transit_key].time <= arrival_via_walk
                            
                            if !transit_beat_walk
                                if !haskey(earliest_arrivals, next_key) || arrival_via_walk < earliest_arrivals[next_key].time
                                    earliest_arrivals[next_key] = new_state
                                    put!(newly_discovered_channel, (neighbor_h3, true, new_state))
                                end
                            end
                        finally
                            unlock(arrivals_lock)
                        end
                    end
                end
            end
        end

        close(newly_discovered_channel)
        
        temp_best = Dict{Tuple{UInt64, Bool}, RouteStateW}()
        for (id, was_walk, state) in newly_discovered_channel
            key = (id, was_walk)
            if !haskey(temp_best, key) || state.time < temp_best[key].time
                temp_best[key] = state
            end
        end
        for (key, state) in temp_best
            push!(processing_queue, (key[1], key[2], state))
        end
    end
    
    return earliest_arrivals
end

# we have to keep a split list of walk/not walk, this merges them
function tidy_routes(earliest_arrivals)
    res = Dict()
    for (k, v) in earliest_arrivals
        current = get(res, k[1], RouteStateW(DateTime(2126,01,01,09,00), k[1], false, 0.0, "Start"))
        if (v.time < current.time)
            res[k[1]] = v
        end
    end
    return res
end
deux(a) = a[2] # surely this already exists

# edgelist[edgelist.stop_name .== "Nice-Ville", :] # find a stop you want
# edgelist[edgelist.stop_name .== "Nuits sous Ravières", :] # find a stop you want
# 622506761884139519
start_pos = UInt64(621646712112906239) # Järna
start_pos = UInt64(622506761884139519) # Nice-Ville
start_pos = UInt64(622054452367032319) # Nuits sous Ravières
initial_arrival_time = DateTime(2026,01,01,09,00)
# initial_arrival_time = DateTime(2026,01,01,04,00) # Jon gets up early
cutoff_time = DateTime(2026,01,01,12,00)
@time results = find_earliest_arrivals(gdf, adj_list, start_pos, initial_arrival_time, cutoff_time + Day(7), friendly_keys)

res = tidy_routes(results)


df = DataFrame(child=collect(keys(res)), distance=round.(getfield.(values(res), :distance)), time=getfield.(round.(getfield.(values(res), :time) .- initial_arrival_time, Minute), :value)./(60*24))
addquantiles!(df, :distance)
addquantiles!(df, :time)
df.value = df.distance_quantile .- df.time_quantile
# df = DataFrame(child=collect(keys(results)), value=Float64.(getfield.(round.(collect(values(results)) .- initial_arrival_time, Minute), :value)./(60*24)))
df.index = H3.API.cellToParent.(df.child, 4)
bdf = combine(groupby(df, :index), group -> begin
     i = argmin(abs.(group.value))
     (value=group.value[i], time=group.time[i], distance=group.distance[i])
 end
)
bdf.index = string.(bdf.index, base=16)
# sshfs the_server:projects/H3-MON/www/data data
Arrow.write("data/scratch/2026-04-26-4.arrow", bdf)
impressum = Dict(
   "t" => "Distance - time quantile, from Nuits sous Ravières at 4am",
   "c" => "Transitous et al.",
)
write("data/scratch/2026-04-26-4.json", JSON.json(impressum))


getbest(rs) = begin
    rrs = collect(pairs(rs))
    return rrs[findmax(x -> x[2].distance, rrs)[2]]
end
# ok. right. we wanted to find the biggest distance pair innit. so let's do that.
# get manageable number of stations
edgelist.parent_h3 = H3.API.cellToParent.(edgelist.h3, 2)
starts = combine(groupby(edgelist, :parent_h3,), :h3 => first => :h3).h3
# i need to keep the results so i can retrace my steps if i want
bests = Dict()
using ProgressMeter
@showprogress for start_pos in starts
    results = find_earliest_arrivals(gdf, adj_list, start_pos, initial_arrival_time, initial_arrival_time + Day(7), friendly_keys)
    tidy_routes(results)
    bests[start_pos] = (best=getbest(tidy_routes(results)), results=results)
    # getbest(tidy_routes(results))
end
getfield.(deux.(getfield.(values(bests), :best)), :distance)
df = DataFrame(child=collect(keys(bests)), distance=round.(getfield.(deux.(getfield.(values(bests), :best)), :distance)), time=getfield.(round.(getfield.(deux.(getfield.(values(bests), :best)), :time) .- initial_arrival_time, Minute), :value)./(60*24))
df.index = string.(H3.API.cellToParent.(df.child, 2), base=16)
df.value = df.distance

# i guess then take like the top ~20, do h3 below, repeat?
Arrow.write("data/scratch/2026-04-26-5.arrow", df)
impressum = Dict(
   "t" => "Furthest distance travelled along fastest route to everywhere from point in km",
   "c" => "Transitous et al.",
)
write("data/scratch/2026-04-26-5.json", JSON.json(impressum))


# only check portugal, finland
using Missings
# julia> edgelist[coalesce.(edgelist.ISO_A2 .== "PT",false), :]
countries = copy(Arrow.Table("ne_50m_admin_0_countries.asc.arrow") |> DataFrame)
dropmissing!(countries)
compact = combine(groupby(countries, :ISO_A2,), :h3 => collect∘Ref => :h3)
transform!(compact, :h3 => ByRow(x -> H3.API.uncompactCells(collect(x), 5)) => :h3)
uncompact = flatten(compact, :h3)
lookup = Dict(v => k for (k,v) in enumerate(unique(countries.ISO_A2)))



edgelist.parent_h3 = H3.API.cellToParent.(edgelist.h3, 5)
leftjoin!(edgelist, uncompact[!, [:h3, :ISO_A2]], on=:parent_h3 => :h3)

starts = edgelist[coalesce.(edgelist.ISO_A2 .== "PT",false) .|| coalesce.(edgelist.ISO_A2 .== "FI",false), :h3] |> unique # 700. doable.
#starts_raw = edgelist[coalesce.(edgelist.ISO_A2 .== "PT",false) .|| coalesce.(edgelist.ISO_A2 .== "FI",false), :]#h3] |> unique # 700. doable.
#H3_RES = 6
#starts_raw.parent_h3 = H3.API.cellToParent.(starts_raw.h3, H3_RES)
#starts = combine(groupby(starts_raw, :parent_h3,), :h3 => first => :h3).h3

getbest(rs) = begin
    rrs = collect(pairs(rs))
    return rrs[findmax(x -> x[2].distance, rrs)[2]]
end
bests = Dict()
using ProgressMeter
@showprogress for start_pos in starts
    results = find_earliest_arrivals(gdf, adj_list, start_pos, initial_arrival_time, initial_arrival_time + Day(7), friendly_keys)
    tidy_routes(results)
    bests[start_pos] = (best=getbest(tidy_routes(results)), results=results)
    # getbest(tidy_routes(results))
end
df = DataFrame(child=collect(keys(bests)), distance=round.(getfield.(deux.(getfield.(values(bests), :best)), :distance)), time=getfield.(round.(getfield.(deux.(getfield.(values(bests), :best)), :time) .- initial_arrival_time, Minute), :value)./(60*24))
df.index = string.(H3.API.cellToParent.(df.child, 6), base=16)
#df.index = string.(df.child, base=16)
df.value = df.distance
Arrow.write("data/scratch/scratch.arrow", df[!, [:index, :value, :time]])

# i guess then take like the top ~20, do h3 below, repeat?
# Arrow.write("data/scratch/2026-04-26-5.arrow", df)
# impressum = Dict(
#    "t" => "Furthest distance travelled along fastest route to everywhere from point in km",
#    "c" => "Transitous et al.",
# )
# write("data/scratch/2026-04-26-5.json", JSON.json(impressum))

# i _think_ when we're updating routes, we need to check if the distance is less and update the route that takes the lesser distance 

# anyway let's see what routes we got
function reconstruct_route(earliest_arrivals::Dict{Tuple{UInt64, Bool}, RouteStateW}, start_uuid::UInt64, target_uuid::UInt64)
    state_transit = get(earliest_arrivals, (target_uuid, false), nothing)
    state_walk    = get(earliest_arrivals, (target_uuid, true), nothing)
    
    if state_transit === nothing && state_walk === nothing
        println("Target H3 index was never reached.")
        return nothing
    end
    
    if state_transit !== nothing && state_walk !== nothing
        curr_was_walk = state_walk.time < state_transit.time
    elseif state_transit !== nothing
        curr_was_walk = false
    else
        curr_was_walk = true
    end
    
    route =[]
    curr_h3 = target_uuid
    
    while true
        state = earliest_arrivals[(curr_h3, curr_was_walk)]
        pushfirst!(route, (
            h3_index = curr_h3,
            arrival_time = state.time, 
            accumulated_distance_km = round(state.distance, digits=2), 
            action = state.action
        ))
        if curr_h3 == start_uuid
            break
        end
        curr_h3 = state.parent_h3
        curr_was_walk = state.parent_was_walk
    end
    return DataFrame(route)
end

using Serialization: serialize, deserialize
serialize("bests.jls", bests)
# todo: if we don't use this soon, delete it

paths = []
distances = []
names = []
for b in pairs(bests)
    path = map(p->rad2deg.([p.lng + rand()/1000, p.lat + rand()/1000]), H3.API.cellToLatLng.(reconstruct_route(b.second.results, b.first, b.second.best.first).h3_index))
    start_station = first(edgelist[edgelist.h3 .== b.first, :stop_name])
    end_station = first(edgelist[edgelist.h3 .== b.second.best.first, :stop_name])
    push!(names, "$(start_station) -> $(end_station)")
    push!(paths, path)
    distance = b.second.best.second.distance
    push!(distances, distance)
end
#top10 = sort!(collect(zip(paths, distances)), by=x->x[2], rev=true)#[1:20]
top10 = Iterators.filter(pd -> pd[2] > 6300, zip(paths, distances, names))

features = []
for (path, distance, name) in top10
    push!(features, Dict(
        "type" => "Feature",
        "geometry" => Dict(
            "type" => "LineString",
            "coordinates" => path,
        ),
        "properties" => Dict(
            "value" => distance,
            "route" => name,
        )
    ))
end
json = Dict("type" => "FeatureCollection", "features" => features)
write("data/scratch/longest_routes.geojson", JSON.json(json))
impressum = Dict(
   "t" => "Routes and their total distances in km",
   "c" => "Transitous et al.",
)
write("data/scratch/longest_routes.json", JSON.json(impressum))
