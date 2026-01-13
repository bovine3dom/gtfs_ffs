#!/bin/julia

using Arrow, DataFrames, StatsBase, DataFrames, Dates, JSON, CSV
import H3.API: LatLng, latLngToCell, h3ToString

big_df = Arrow.Table("../data/edgelist.arrow") |> DataFrame |> copy # copy to disable memory mapping
gdf = groupby(big_df, :stop_uuid)

# get biggest stop_uuid per h3
mytmp = combine(groupby(sort(combine(groupby(big_df, [:h3, :stop_uuid]), nrow), :nrow, rev=true), :h3), :stop_uuid => first => :stop_uuid)
h3tostop = Dict(t[1] => t[2] for t in zip(mytmp.h3, mytmp.stop_uuid))
# stoptoh3 = Dict(t[2] => h3ToString(t[1]) for t in zip(mytmp.h3, mytmp.stop_uuid))
stoptoh3 = Dict(t[2] => t[1] for t in zip(mytmp.h3, mytmp.stop_uuid))


# different goal from router.jl: we don't care about arrival time, just any reachable stop
function walk_graph_no_constraints(gdf::GroupedDataFrame, start_uuid::Int64)
    visited_stops = Set{Int64}()
    
    visited_lock = Base.Threads.SpinLock()
    
    processing_queue = Int64[]
    push!(visited_stops, start_uuid)
    push!(processing_queue, start_uuid)

    while !isempty(processing_queue)
        newly_discovered_channel = Channel{Int64}(Inf)
        
        current_batch_to_process = copy(processing_queue)
        empty!(processing_queue)

        Threads.@threads for current_uuid in current_batch_to_process
            if !haskey(gdf, (current_uuid,))
                continue
            end

            t = gdf[(current_uuid,)]
            potential_next_stops = unique(t.next_stop)

            for next_stop_id in potential_next_stops
                added_to_channel = false
                
                # Atomic Check-and-Set
                lock(visited_lock)
                try
                    if !(next_stop_id in visited_stops)
                        push!(visited_stops, next_stop_id)
                        added_to_channel = true
                    end
                finally
                    unlock(visited_lock)
                end

                if added_to_channel
                    put!(newly_discovered_channel, next_stop_id)
                end
            end
        end

        close(newly_discovered_channel)
        
        for item in newly_discovered_channel
            push!(processing_queue, item)
        end
    end
    
    return visited_stops
end

function latlon2anisochrone(x, y)
    h3 = latLngToCell(LatLng(deg2rad(y), deg2rad(x)), 7) # i forget to convert to radians every time
    !haskey(h3tostop, h3) && return DataFrame(index = h3ToString(h3), value = 1.0, actual_value = 0)
    stop_uuid = h3tostop[h3]
    res = walk_graph_no_constraints(gdf, stop_uuid)
    df = DataFrame(stop_uuid=collect(res))
    df.h3 = get.(Ref(stoptoh3), df.stop_uuid, missing) # i don't understand how any of these can be missing mais bon
    dropmissing!(df)
    return df
end

# import H3.API: compactCells
# London St Pancras
x = -.125
y = 51.53
df = latlon2anisochrone(x, y)

# Belfast because we get stuck at the ferry it seems
x = -5.92
y = 54.59
df2 = latlon2anisochrone(x, y)
df = vcat(df, df2)

# filter(>(0),compactCells(df.h3))
df.index = h3ToString.(df.h3)
df.dummy .= true

CSV.write("accessible.csv", df[!, [:index, :dummy]])
