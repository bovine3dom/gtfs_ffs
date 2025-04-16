#!/bin/julia
using Plots, CSV, DataFrames, Dates

df = CSV.read("utilisation_over_day.csv", DataFrame)
transform!(groupby(df, :company), :c => (x-> x./maximum(x)) => :utilisation)
df.probe = Time.(df.probe, "yyyy-mm-dd HH:MM:SS.SSS")
plot(df.probe, df.utilisation, group=df.company, xticks=Time(0):Minute(120):Time(23,59), xrot=45, xlims=(Time(0), Time(23,59)), ylabel="# of services running as fraction of peak")
