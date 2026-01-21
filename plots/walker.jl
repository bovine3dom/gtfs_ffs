#!/bin/julia

using Arrow, DataFrames, StatsBase, DataFrames, Dates, JSON, CSV
import H3.API: LatLng, latLngToCell, h3ToString, cellToLatLng
import Random: random

include("lib.jl")

big_df = Arrow.Table("../data/edgelist.arrow") |> DataFrame |> copy # copy to disable memory mapping
gdf = groupby(big_df, :stop_uuid)

big_df = select_df(con(), "select distinct h3ToParent(h3, 6) h3, h3ToParent(next_h3, 6) next_h3 from transitous_everything_20260117_edgelist_fahrtle")
df = big_df

## borrowed from uuid generator
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

# --- New Processing Logic ---

ds = DisjointSet()

for (u, v) in zip(df.h3, df.next_h3)
    union_sets!(ds, u, v)
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

agg = DataFrame(
    h3 = collect(keys(h3_to_cluster_id)), 
    cluster_id = collect(values(h3_to_cluster_id))
)
cluster_stats = combine(groupby(agg, :cluster_id), nrow)
sort!(cluster_stats, :nrow, rev=true) # ok so the top 5 account for most of it
only_big = agg[agg.cluster_id .∈ Ref(cluster_stats.cluster_id[1:5]), :]

function cellToPos(h3)
    ll = cellToLatLng(h3)
    return (ll.lat, ll.lng)
end

dist(l, r) = begin
    (lat_l, lng_l) = cellToPos(l)
    (lat_r, lng_r) = cellToPos(r)
    dist_rad = (lat_l - lat_r)^2 + (lng_l - lng_r)^2
    return 6371 * acos(dist_rad)
end

row = rand(eachrow(only_big))
find_destination(row) = begin
    not_found = true
    while not_found
        candidate = rand(eachrow(only_big[only_big.cluster_id .∈ Ref(row.cluster_id), :]))
        dist(row.h3, candidate.h3) > 100 && return candidate
        not_found = false # lol
    end
end


n = 365*10 # we can repeat after a decade
days = Array{@NamedTuple{start::String, finish::String}, 1}()
for i in 1:n
    row = rand(eachrow(only_big))
    push!(days, (start=h3ToString(row.h3), finish=h3ToString(find_destination(row).h3)))
end
days
write("races.json", JSON.json(days))
