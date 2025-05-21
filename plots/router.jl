#!/bin/julia
import Arrow
using DataFrames, Dates, JSON, CSV
function find_earliest_arrivals(df::GroupedDataFrame, start_uuid::Int64, initial_arrival_time::DateTime, cutoff_time::DateTime)
    earliest_arrivals = Dict{Int64, DateTime}()
    # nb: SpinLock much quicker than ReentrantLock but cannot be acquired twice, will hang forever
    arrivals_lock = Base.Threads.SpinLock()
    processing_queue = Tuple{Int64, DateTime}[]

    earliest_arrivals[start_uuid] = initial_arrival_time
    push!(processing_queue, (start_uuid, initial_arrival_time))

    while !isempty(processing_queue)
        # channels are just thread-safe FIFO queues
        newly_discovered_channel = Channel{Tuple{Int64, DateTime}}(Inf)
        current_batch_to_process = copy(processing_queue)
        empty!(processing_queue)

        Threads.@threads for (current_uuid, current_arrival_at_stop) in current_batch_to_process
            !haskey(gdf, (current_uuid,)) && continue
            t = gdf[(current_uuid,)]
            potential_departures = @view t[(cutoff_time .>= t.next_arrival) .&& (t.departure_time .>= current_arrival_at_stop), :]

            for depart in eachrow(potential_departures)
                next_stop_id = depart.next_stop
                arrival_at_next_stop = depart.next_arrival
                added_to_channel_for_next_round = false
                lock(arrivals_lock)
                try
                    if !haskey(earliest_arrivals, next_stop_id) || arrival_at_next_stop < earliest_arrivals[next_stop_id]
                        earliest_arrivals[next_stop_id] = arrival_at_next_stop
                        added_to_channel_for_next_round = true
                    end
                finally
                    unlock(arrivals_lock)
                end

                if added_to_channel_for_next_round
                    put!(newly_discovered_channel, (next_stop_id, arrival_at_next_stop))
                end

            end
        end

        close(newly_discovered_channel)
        for item in newly_discovered_channel
            push!(processing_queue, item)
        end
    end
    return earliest_arrivals
end

# left as exercise for the reader
# big_df = Arrow.Table("../data/edgelist.arrow") |> DataFrame
# big_df = Arrow.Table("../data/edgelist_onlytrains_gb.arrow") |> DataFrame

### example usage

# add a check for max journey time
#gdf = groupby(big_df, :stop_uuid)

## start_uuid = 217175# df[1, :stop_uuid] # morbio
## start_uuid = 358964# nice
## start_uuid = 277445 # shanklin
#start_uuid = 27679 # waterloo
##start_uuid = 385545 # meggen
##initial_arrival_time = DateTime(2025,01,01,04,00)
#initial_arrival_time = DateTime(2025,01,01,08,00)
#for t in initial_arrival_time:Minute(5):initial_arrival_time + Hour(22)
#    t = initial_arrival_time
#    @time stops = find_earliest_arrivals(gdf, start_uuid, t, t + Hour(12));
#    stops_df = flatten(DataFrame(stop_uuid=keys(stops),arrival_time=values(stops)), [:stop_uuid, :arrival_time])
#    leftjoin!(stops_df, combine(gdf, :h3 => first => :h3), on=:stop_uuid)
#    dropmissing!(stops_df)
#    stops_df.journey_time = map(x->x.value, round.(stops_df.arrival_time .- t, Minute))
#    agg = combine(groupby(stops_df, :h3), :journey_time => minimum => :journey_time)
#    agg.index = string.(agg.h3, base=16)
#    agg.value = agg.journey_time
#    tday = Dates.today()
#    mkpath("$(homedir())/projects/H3-MON/www/data/debug")
#    write("""$(homedir())/projects/H3-MON/www/data/debug/$tday.json""",
#        JSON.json(Dict(
#        "t" => "Travel time to destination, minutes",
#        "flip" => true,
#        # "raw" => true,
#        "c" => "Transitous et al.",
#        # "scale" => Dict(zip(0.0:0.2:1.0, 0:12:60)),
#    )))
#    CSV.write("$(homedir())/projects/H3-MON/www/data/debug/$tday.csv", agg[!, [:index, :value]])
#    print(t); print('\r')
#    sleep(0.2)
#end



## Shanklin to Bank journey durations
#initial_arrival_time = DateTime(2025,01,01,04,00)
#dist = Dict()
#for t in initial_arrival_time:Minute(5):initial_arrival_time + Hour(22)
#    @time stops = find_earliest_arrivals(gdf, start_uuid, t, t + Hour(4)); # ~4x quicker. nice
#    dist[t] = get(stops,41019,missing) - t
#end
#df = DataFrame(t=collect(keys(dist)), duration=collect(values(dist)))
#sort!(df, :t)
#using Plots

## the nan stuff with Plots is always a nightmare
#df.duration_mins = map(x -> x isa TimePeriod ? round(x, Minute).value : NaN, df.duration)
#plot(df.t, df.duration_mins, xrot=45, margin=15Plots.mm, legend=:none, ylabel="Minutes from Shanklin to Bank")


# todo:
# - consider caching "where can i get to at time t from station x" in a function
# - switch departure_time to findfirsts?
# - walking connections
# - api for draggable map
