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
#
# not done:
# -> prevent chaining walks
# -> keep track of distance travelled and route taken
# -> make list of candidate stations to start at?
# -> decide on allowable start times and intervals between them?
# -> decide whether to make a real timetable rather than the fantasy one?
# 
# i guess in router.jl earliest arrivals needs to become (time, route)[] pairs?
using JSON, CSV, DataFrames, Dates, Arrow
import H3

include("lib.jl")

# h3_11 originally. oddly it's reasonably invariant to lower resolutions. 10 -> 55k unique; 8 -> 50k unique
edgelist = select_df(con(), """
    select distinct toTime(departure_time) departure_time, h3ToParent(h3, 10) h3, stop_name, h3ToParent(next_h3,10) next_h3, travel_time from transitous_everything_20260218_edgelist_fahrtle2
    where route_type = 2 or route_type between 100 and 117 -- only trains
    order by h3, next_h3, departure_time
""")
edgelist.departure_time = Time.(edgelist.departure_time)

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

# julia> gdf[[(r,) for r in ring if insorted(r, friendly_keys)]]
# GroupedDataFrame with 1 group based on key: h3
# First Group (199 rows): h3 = 0x08a196928806ffff
#  Row │ departure_time       h3                  next_arrival         next_h3             stop_name 
#      │ DateTime             UInt64              DateTime             UInt64              String    
# ─────┼─────────────────────────────────────────────────────────────────────────────────────────────
#    1 │ 2026-01-01T06:09:00  621943779645390847  2026-01-01T06:16:00  621943777779286015  Rosmalen
#    2 │ 2026-01-01T06:20:00  621943779645390847  2026-01-01T06:24:00  622053425420795903  Rosmalen
#    3 │ 2026-01-01T06:22:00  621943779645390847  2026-01-01T06:26:00  622053425420795903  Rosmalen
#    4 │ 2026-01-01T06:39:00  621943779645390847  2026-01-01T06:46:00  621943777779286015  Rosmalen
#    5 │ 2026-01-01T06:50:00  621943779645390847  2026-01-01T06:54:00  622053425420795903  Rosmalen
#    6 │ 2026-01-01T07:09:00  621943779645390847  2026-01-01T07:16:00  621943777779286015  Rosmalen

get_walkable_neighbours(start_pos, friendly_keys)
gdf[(start_pos,)]
(ring, distances) = H3.API.gridDiskDistances(start_pos, 4) # approx 5 min walk
walking_times = (distances .* (sqrt(H3.API.cellAreaKm2(start_pos) /3) * 2)) / 5 # 5 km/h, in hours
gdf[[(r,) for r in ring if insorted(r, friendly_keys)]]

start_pos = UInt64(621646712112906239)
initial_arrival_time = DateTime(2026,01,01,09,00)
cutoff_time = DateTime(2026,01,01,12,00)

function find_earliest_arrivals(edge_gdf::GroupedDataFrame, adj_list::Dict, start_uuid::UInt64, initial_arrival_time::DateTime, cutoff_time::DateTime, friendly_keys)
    earliest_arrivals = Dict{UInt64, DateTime}()
    arrivals_lock = Base.Threads.SpinLock()
    processing_queue = Tuple{UInt64, DateTime}[]

    earliest_arrivals[start_uuid] = initial_arrival_time
    push!(processing_queue, (start_uuid, initial_arrival_time))

    while !isempty(processing_queue)
        newly_discovered_channel = Channel{Tuple{UInt64, DateTime}}(Inf)
        current_batch = copy(processing_queue)
        empty!(processing_queue)

        Threads.@threads for (current_h3, current_arrival) in current_batch
            
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
                        lock(arrivals_lock)
                        try
                            if !haskey(earliest_arrivals, next_h3) || arrival_at_next < earliest_arrivals[next_h3]
                                earliest_arrivals[next_h3] = arrival_at_next
                                put!(newly_discovered_channel, (next_h3, arrival_at_next))
                            end
                        finally
                            unlock(arrivals_lock)
                        end
                    end
                end
            end

            # walking
            (ring, distances) = H3.API.gridDiskDistances(current_h3, 4)
            area_factor = sqrt(H3.API.cellAreaKm2(current_h3) / 3) * 2
            
            # i guess in theory you're allowed to walk forever?. TODO: limit it to one consecutive walk
            for i in 2:length(ring) # skip self
                neighbor_h3 = ring[i]
                !insorted(neighbor_h3, friendly_keys) && continue
                
                walk_millis = Dates.Millisecond(round(Int, (distances[i] * area_factor / 5) * 3.6e6))
                arrival_via_walk = current_arrival + walk_millis
                
                if arrival_via_walk <= cutoff_time
                    lock(arrivals_lock)
                    try
                        if !haskey(earliest_arrivals, neighbor_h3) || arrival_via_walk < earliest_arrivals[neighbor_h3]
                            earliest_arrivals[neighbor_h3] = arrival_via_walk
                            put!(newly_discovered_channel, (neighbor_h3, arrival_via_walk))
                        end
                    finally
                        unlock(arrivals_lock)
                    end
                end
            end
        end

        close(newly_discovered_channel)
        
        temp_best = Dict{UInt64, DateTime}()
        for (id, time) in newly_discovered_channel
            if !haskey(temp_best, id) || time < temp_best[id]
                temp_best[id] = time
            end
        end
        for (id, time) in temp_best
            push!(processing_queue, (id, time))
        end
    end
    return earliest_arrivals
end
start_pos = UInt64(621646712112906239)
initial_arrival_time = DateTime(2026,01,01,09,00)
cutoff_time = DateTime(2026,01,01,12,00)
@time results = find_earliest_arrivals(gdf, adj_list, start_pos, initial_arrival_time, cutoff_time + Day(7), friendly_keys)

df = DataFrame(child=collect(keys(results)), value=Float64.(getfield.(round.(collect(values(results)) .- initial_arrival_time, Minute), :value)./(60*24)))
df.index = H3.API.cellToParent.(df.child, 4)
bdf = combine(groupby(df, :index), :value => minimum => :value)
bdf.index = string.(bdf.index, base=16)
# sshfs the_server:projects/H3-MON/www/data data
Arrow.write("data/scratch/2026-04-25.arrow", bdf)
impressum = Dict(
   "t" => "Fastest travel time in days from Järna, SE, heavy rail + foot at 9am",
   "flip" => true,
   "c" => "Transitous et al.",
)
write("data/scratch/2026-04-25.json", JSON.json(impressum))
