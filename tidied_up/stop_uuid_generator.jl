#!/bin/julia

using CSV, DataFrames
include("lib.jl") # make sure you have the right env variables and you're tunnelled if necessary

struct DisjointSet
    parents::Dict{UInt64, UInt64}
    ranks::Dict{UInt64, Int}
end

DisjointSet() = DisjointSet(Dict{UInt64, UInt64}(), Dict{UInt64, Int}())

function find_root!(ds::DisjointSet, i::UInt64)
    if !haskey(ds.parents, i)
        ds.parents[i] = i
        ds.ranks[i] = 0
        return i
    end
    
    if ds.parents[i] != i
        ds.parents[i] = find_root!(ds, ds.parents[i])
    end
    return ds.parents[i]
end

function union_sets!(ds::DisjointSet, i::UInt64, j::UInt64)
    root_i = find_root!(ds, i)
    root_j = find_root!(ds, j)
    
    if root_i != root_j
        if ds.ranks[root_i] < ds.ranks[root_j]
            ds.parents[root_i] = root_j
        elseif ds.ranks[root_i] > ds.ranks[root_j]
            ds.parents[root_j] = root_i
        else
            ds.parents[root_j] = root_i
            ds.ranks[root_i] += 1
        end
        return true
    end
    return false
end

function generate_uuids(con, stops_table_name, output_table_name)
    select_df(con, "select count() c from $stops_table_name")
    df = select_df(con, """
    select groupArray(h3) overlaps from (
        -- 2 is too aggressive... 1 works kinda
        select distinct geoToH3(stop_lat, stop_lon, 11) h3, arrayJoin(h3kRing(h3, 1)) h3_ring from $stops_table_name
    )
    group by h3_ring
    """)

    # using ProgressMeter

    # union-find algorithm
    ds = DisjointSet()
    # p = Progress(size(df, 1); desc="Unionizing Overlaps: ", dt=1.0)

    for row in df.overlaps
        if length(row) > 1
            first_item = row[1]
            find_root!(ds, first_item) 
            
            for i in 2:length(row)
                union_sets!(ds, first_item, row[i])
            end
        elseif length(row) == 1
            find_root!(ds, row[1])
        end
        # next!(p)
    end
    h3_to_cluster_id = Dict{UInt64, Int}()

    root_to_id = Dict{UInt64, Int}()
    next_id = 0

    for h3 in keys(ds.parents)
        root = find_root!(ds, h3)
        if !haskey(root_to_id, root)
            next_id += 1
            root_to_id[root] = next_id
        end
        h3_to_cluster_id[h3] = root_to_id[root]
    end

    agg = DataFrame(h3 = collect(keys(h3_to_cluster_id)), stop_uuid = collect(values(h3_to_cluster_id)))

    execute(con, "drop table if exists $output_table_name")
    execute(con, """
    CREATE TABLE $output_table_name
    (
        `h3` UInt64,
        `stop_uuid` Int64
    )
    ENGINE = MergeTree
    ORDER BY (h3, stop_uuid)
    SETTINGS allow_nullable_key = 1, index_granularity = 8192
    """)
    insert(con, output_table_name, [df2dict(agg)])
end
