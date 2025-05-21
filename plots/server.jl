#!/bin/julia

include("router.jl")

# server
API_VERSION = "0.0.1"
PORT = get(ENV, "JULIA_API_PORT", 50075)
IN_PRODUCTION = !isinteractive()

using Oxygen, Arrow, DataFrames, StatsBase
import HTTP
import H3.API: LatLng, latLngToCell, h3ToString

big_df = Arrow.Table("../data/edgelist.arrow") |> DataFrame
gdf = groupby(big_df, :stop_uuid)

# get biggest stop_uuid per h3
mytmp = combine(groupby(sort(combine(groupby(big_df, [:h3, :stop_uuid]), nrow), :nrow, rev=true), :h3), :stop_uuid => first => :stop_uuid)
h3tostop = Dict(t[1] => t[2] for t in zip(mytmp.h3, mytmp.stop_uuid))
stoptoh3 = Dict(t[2] => h3ToString(t[1]) for t in zip(mytmp.h3, mytmp.stop_uuid))


function df2arrowresp(df)
    io = IOBuffer()
    Arrow.write(io, df)
    body = take!(io)
    resp = HTTP.Response(200, [], body)
    HTTP.setheader(resp, "Content-Type" => "application/x-arrow")
    HTTP.setheader(resp, "Content-Length" => string(sizeof(body)))
    return resp
end
function latlon2isochrone(x, y, t)
    start_time = DateTime(2025,01,01,17,00)
    cutoff = start_time + Hour(t)
    h3 = latLngToCell(LatLng(deg2rad(y), deg2rad(x)), 7) # i forget to convert to radians every time
    !haskey(h3tostop, h3) && return df2arrowresp(DataFrame(index = h3ToString(h3), value = 1.0, actual_value = 0))
    stop_uuid = h3tostop[h3]
    res = find_earliest_arrivals(gdf, stop_uuid, start_time, cutoff)
    df = DataFrame(stop_uuid=collect(keys(res)), arrival_time=collect(values(res)))
    df.h3 = get.(Ref(stoptoh3), df.stop_uuid, missing) # i don't understand how any of these can be missing mais bon
    dropmissing!(df)
    agg = combine(groupby(df, :h3), :arrival_time => minimum => :arrival_time)
    agg.actual_value = map(x -> round(x - start_time, Minute).value, agg.arrival_time)
    agg.value = 1 .- agg.actual_value ./ (60*t)
    if (length(agg.value) > 1)
        agg.value = quantilerank.(Ref(agg.value), agg.value)
    end
    rename!(agg, :h3 => :index)
    sort!(agg, :value)
    return df2arrowresp(agg)
end

@get "/isochrone" function(req::HTTP.Request, x::Float64, y::Float64, t::Int = 2)
    return latlon2isochrone(x,y,t)
end

# from https://github.com/OxygenFramework/Oxygen.jl/issues/149#issuecomment-1914998878 what a nightmare
allowed_origins = [ "Access-Control-Allow-Origin" => "*" ]

cors_headers = [
    allowed_origins...,
    "Access-Control-Allow-Headers" => "*",
    "Access-Control-Allow-Methods" => "GET, POST"
]

function CorsHandler(handle)
    return function (req::HTTP.Request)
        # return headers on OPTIONS request
        if HTTP.method(req) == "OPTIONS"
            return HTTP.Response(200, cors_headers)
        else
            r = handle(req)
            append!(r.headers, allowed_origins)
            return r
        end
    end
end

serve(;middleware=[CorsHandler], host="0.0.0.0", port=PORT)
