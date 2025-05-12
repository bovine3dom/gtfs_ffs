#!/bin/julia
using Plots
using JSON, CSV, DataFrames, Dates

include("lib.jl")

# utilisation over 24 hours
df = CSV.read("utilisation_over_day.csv", DataFrame)

df = select_df(con(), """
-- number of trains* active in 15 minute intervals throughout the day
-- ignoring ones that started the day before :)
-- * in switzerland we use a broad definition of trains which includes, buses, ferries and apparently even taxis?
-- todo: daft switcherooo here: best effort parse datetime parses to 2025-01-01, toTime parses to 1970-01-2
with probes as (select addMinutes('1970-01-02', number*5) probe from numbers(24*12)) -- +1)) if you want to include the next day
select sum(probe between yes.dt and yes.at) c, probe, company from probes pr
left outer join (
select toTime(min(departure_time)) dt, toTime(max(if(arrival_time >= '2025-01-02', parseDateTimeBestEffort('2025-01-01 23:59:59'), arrival_time))) at, trip_id, company from gtfs_stop_times st
-- inner join gtfs_trips tr on st.trip_id = tr.trip_id and st.company = tr.company
--inner join gtfs_calendar ca on tr.service_id = ca.service_id and tr.company = ca.company -- france doesn't support
--inner join gtfs_routes ro on tr.route_id = ro.route_id and tr.company = ro.company -- france doesn't support
--outer join (select
where true
--and '2025-04-01' between ca.start_date and ca.end_date -- should probably pick a random date -- looks like only switzerland supports this :(
--and ca.tuesday -- first of april was a tuesday
--and route_type between 0 and 199 -- only trains -- france doesn't support this
group by trip_id, company
union all
-- this segment here to support trains that run overnight
select toTime(parseDateTimeBestEffort('2025-01-01 00:00:00')) dt, toTime(max(arrival_time)) at, trip_id, company from gtfs_stop_times st
group by trip_id, company
having toDate(min(departure_time)) < toDate(max(arrival_time))
) yes on true -- clickhouse doesn't support < or > on joins so we do an outer join :(
group by probe, company
order by probe
""")

transform!(groupby(df, :company), :c => (x-> x./maximum(x)) => :utilisation)
# df.probe = Time.(df.probe, "yyyy-mm-dd HH:MM:SS.SSS") # not needed if not using the CSV
df.probe = Time.(df.probe) # not needed if not using the CSV
plot(df.probe, df.utilisation, group=df.company, xticks=Time(0):Minute(120):Time(23,59), xrot=45, xlims=(Time(0), Time(23,59)), ylabel="# of services running as fraction of peak")




# headways
df = select_df(con(), """
select company, headway, count(*) n from (select 
company, stop_id, route_id, departure_time,
dateDiff('minute', lagInFrame(departure_time, 1, departure_time) over (
--dateDiff(last_value(departure_time), first_value(departure_time)) over (
    partition by company, stop_id, route_id
    order by departure_time asc
    rows between 1 preceding and current row
), departure_time) headway
from gtfs_stop_times st
left join gtfs_trips tr on st.trip_id = tr.trip_id and st.company = tr.company
where company = 'ter'
) where headway between 1 and 120
group by all
order by company, headway
""")

plot(df.headway, df.n) # looks UK data is messed up, maybe they're not using real route IDs

# correcting for services that are actually running
df = select_df(con(), """
select company, headway, count(*) n from (select 
company, stop_id, route_id, departure_time,
dateDiff('minute', lagInFrame(departure_time, 1, departure_time) over (
    partition by company, stop_id, route_id, tr.direction_id, if(tr.company in ('sncb', 'swiss', 'se', 'ee', 'ns_nl', 'dk', 'obb'), tr.trip_headsign, null) -- only sncb uses headsign?
    order by departure_time asc
    rows between 1 preceding and current row
), departure_time) headway
from gtfs_stop_times st
inner join 
(
select distinct tr.company, trip_id, tr.route_id route_id, direction_id, trip_headsign from gtfs_trips tr -- not necessarily distinct because join with calendar can mess things up
left join gtfs_calendar ca on tr.service_id = ca.service_id and tr.company = ca.company
left join gtfs_calendar_dates cd on tr.service_id = cd.service_id and tr.company = cd.company
left join gtfs_routes gr on tr.route_id = gr.route_id and tr.company = gr.company
where true
and tr.company != 'uk' -- uk data messed up / ages in the past
--and tr.company in ('ter', 'sncb', 'tgv') -- swiss data too messy?
and (tr.company not in ('swiss', 'no', 'se', 'dk', 'ns_nl') or gr.route_type between 0 and 199)
and ((cd.date = '2025-05-13' and cd.exception_type = 1) or (ca.start_date <= '2025-05-13' and ca.end_date >= '2025-05-13' and tuesday and not (cd.date = '2025-05-13' and cd.exception_type = 2)))
)
tr 
on st.trip_id = tr.trip_id and st.company = tr.company
) why 
where headway between 1 and 60*5
group by all
order by company, headway
""")
transform!(groupby(df, :company), :n => (x-> x./maximum(x)) => :n_norm)
takts = [10, 15, 30, 60, 120, 240]
ticks = takts

# estonia looks mega dodge
# de_local too
df2 = df[.!in.(df.company, Ref(["ee", "de_local"])), :]
plot(df2.headway, df2.n_norm, group=df2.company, xscale=:log10, xlims=(10, 60*5), xticks=(ticks, string.(ticks))) # looks UK data is messed up, maybe they're not using real route IDs :(


# there's still some noise in the data
# e.g.
#=
-- one offender
 1. │ tgv     │ Paris Montparnasse Hall 1 - 2 │ StopPoint:OCETGV INOUI-87391003 │ Paris - Quimper TGV                            │ 2025-01-01 08:45:00 │ 2025-01-01 08:43:00 │       2 │ OCESN8707F3995320:2025-04-14T23:19:14Z │ OCESN8790F3852769:2025-02-07T11:15:13Z │ 1            │

select * from gtfs_trips tr
left join gtfs_calendar_dates cd on tr.service_id = cd.service_id and tr.company = cd.company
--left join gtfs_calendar ca on tr.service_id = ca.service_id and tr.company = ca.company
where true
and trip_id in ('OCESN8707F3995320:2025-04-14T23:19:14Z', 'OCESN8790F3852769:2025-02-07T11:15:13Z')
and ((cd.date = '2025-05-13' and cd.exception_type = 1))-- or (ca.start_date <= '2025-05-13' and ca.end_date >= '2025-05-13' and tuesday and not (cd.date = '2025-05-13' and cd.exception_type = 2)))
=#
# (only the 08:43 service is actually running that day)


# getting somewhere i think
function istakt(x; rtol=0.01, takts=[10, 15, 30, 60, 120, 240])
    any(isapprox.(x, takts; rtol=rtol))
end

agg_t = combine(groupby(df[df.headway .> 5, :], :company), 
    [:headway, :n] => ((t, n) -> mean(istakt.(t, rtol=0.01, takts=[10, 15, 30, 60, 120]), weights(n))) => :istakt,
    [:headway, :n] => ((t, n) -> mean(istakt.(t, rtol=0.1, takts=[10, 15, 30, 60, 120]), weights(n))) => :istaktish,
)
agg = stack(agg_t, [:istakt, :istaktish])

using StatsPlots
groupedbar(agg.company, agg.value, group=agg.variable, bar_position=:dodge, legend=:outertopright, ylims=(0,1))


# this is getting cool. now aggregate by stop -> h3 instead
df = select_df(con(), """
select geoToH3(stop_lon, stop_lat, 4) h3, headway, count(*) n from (select 
company, stop_id, route_id, departure_time,
dateDiff('minute', lagInFrame(departure_time, 1, departure_time) over (
    partition by company, stop_id, route_id, tr.direction_id, if(tr.company in ('sncb', 'swiss', 'se', 'ee', 'ns_nl', 'dk', 'obb'), tr.trip_headsign, null) -- only sncb uses headsign?
    order by departure_time asc
    rows between 1 preceding and current row
), departure_time) headway
from gtfs_stop_times st
inner join 
(
select distinct tr.company, trip_id, tr.route_id route_id, direction_id, trip_headsign from gtfs_trips tr -- not necessarily distinct because join with calendar can mess things up
left join gtfs_calendar ca on tr.service_id = ca.service_id and tr.company = ca.company
left join gtfs_calendar_dates cd on tr.service_id = cd.service_id and tr.company = cd.company
left join gtfs_routes gr on tr.route_id = gr.route_id and tr.company = gr.company
where true
and tr.company not in ('uk', 'ee', 'de_local') -- uk data messed up / ages in the past, ee and de_local too messy to be believable
--and tr.company in ('ter', 'sncb', 'tgv') -- swiss data too messy?
and (tr.company not in ('swiss', 'no', 'se', 'dk', 'ns_nl') or gr.route_type between 0 and 199)
and ((cd.date = '2025-05-13' and cd.exception_type = 1) or (ca.start_date <= '2025-05-13' and ca.end_date >= '2025-05-13' and tuesday and not (cd.date = '2025-05-13' and cd.exception_type = 2)))
)
tr 
on st.trip_id = tr.trip_id and st.company = tr.company
) why 
left join gtfs_stops s on s.stop_id = why.stop_id
where headway between 1 and 60*5
group by h3, headway
""")
# could be interesting to look at e.g. an entire week rather than just a single day to see how much stability there is

function istakt(x; rtol=0.01, takts=[10, 15, 30, 60, 120, 240])
    any(isapprox.(x, takts; rtol=rtol))
end

agg = combine(groupby(df[df.headway .> 5, :], :h3), 
    [:headway, :n] => ((t, n) -> mean(istakt.(t, rtol=0.01, takts=[10, 15, 30, 60, 120]), weights(n))) => :istakt,
    [:headway, :n] => ((t, n) -> mean(istakt.(t, rtol=0.1, takts=[10, 15, 30, 60, 120]), weights(n))) => :istaktish,
)

using CSV
agg.index = string.(agg.h3, base=16)
agg.value = agg.istakt
#agg.value = agg.istaktish
CSV.write("$(homedir())/projects/H3-MON/www/data/h3_data.csv", agg[!, [:index, :value]])

# should maybe group by at the route level to get taktfulness by route/stop since an e.g. 15 minute then 30 minute headway would look fine on this
# maybe something along the lines of max(taktfulness(takt)) for takt in 15, 30, 60 etc

select_df(con(), "select distinct route_type, company from gtfs_routes") 
select_df(con(), "select count(distinct(route_type)) c, company from gtfs_routes group by company order by c desc") # no, swiss, se, dk, ns_nl
select_df(con(), "select count(distinct(trip_headsign)), company from gtfs_trips group by company") # se, ee, ns_nl, dk, tgv, sncb
select_df(con(), "select count(distinct(direction_id)), company from gtfs_trips group by company") # no, ter, intercites, ns_nl, dk, tgv
select_df(con(), "select distinct company from gtfs_trips") 



# trip speeds
df = select_df(con(), "
select company, round(speed, -1) speed, count(*) c from (
select st.company company, st.stop_id, st.prev_stop, st.trip_id, geoDistance(s.stop_lon, s.stop_lat, s2.stop_lon, s2.stop_lat)/1000 dist, dateDiff('minute', prev_departure, departure_time)/60 t, dist/t speed from (select *,
    lagInFrame(departure_time, 1, departure_time) over (
    --dateDiff(last_value(departure_time), first_value(departure_time)) over (
        partition by st.company, st.trip_id, stop_headsign
        order by departure_time asc
        rows between 1 preceding and current row
    ) prev_departure,
    lagInFrame(stop_id, 1, stop_id) over (
    --dateDiff(last_value(departure_time), first_value(departure_time)) over (
        partition by st.company, st.trip_id, stop_headsign
        order by departure_time asc
        rows between 1 preceding and current row
    ) prev_stop
    from gtfs_stop_times st
    left join gtfs_trips tr on tr.trip_id = st.trip_id and tr.company = st.company
    left join gtfs_routes gr on tr.route_id = gr.route_id and tr.company = gr.company
    left join gtfs_calendar ca on tr.service_id = ca.service_id and tr.company = ca.company
    left join gtfs_calendar_dates cd on tr.service_id = cd.service_id and tr.company = cd.company
    where true
    and tr.company not in ('uk', 'ee', 'de_local') -- uk data messed up / ages in the past, ee and de_local too messy to be believable
    --and tr.company in ('ter', 'sncb', 'tgv') -- swiss data too messy?
    and (tr.company not in ('swiss', 'no', 'se', 'dk', 'ns_nl') or gr.route_type between 0 and 199)
    and ((cd.date = '2025-05-13' and cd.exception_type = 1) or (ca.start_date <= '2025-05-13' and ca.end_date >= '2025-05-13' and tuesday and not (cd.date = '2025-05-13' and cd.exception_type = 2)))
) st
left join gtfs_stops s on st.prev_stop = s.stop_id and st.company = s.company
left join gtfs_stops s2 on st.stop_id = s2.stop_id and st.company = s2.company
) group by all
having speed between 0 and 400
order by speed
")
df2 = df[in.(df.company, Ref(["tgv", "de_long"])), :]
df2 = df
transform!(groupby(df2, :company), :c => (x-> x./sum(x)) => :c_norm)
plot(df2.speed, df2.c_norm, group=df2.company)

# if we want to know how long each train spends at each speed we need to multiply by distance? i think



# dwell times
df = select_df(con(), "
select st.company company, dwell_time, count(*) c from (select *,
    dateDiff('minute', arrival_time, departure_time) dwell_time
    from gtfs_stop_times st
    left join gtfs_trips tr on tr.trip_id = st.trip_id and tr.company = st.company
    left join gtfs_routes gr on tr.route_id = gr.route_id and tr.company = gr.company
    left join gtfs_calendar ca on tr.service_id = ca.service_id and tr.company = ca.company
    left join gtfs_calendar_dates cd on tr.service_id = cd.service_id and tr.company = cd.company
    where true
    and tr.company not in ('uk', 'ee', 'de_local') -- uk data messed up / ages in the past, ee and de_local too messy to be believable
    --and tr.company in ('ter', 'sncb', 'tgv') -- swiss data too messy?
    and (tr.company not in ('swiss', 'no', 'se', 'dk', 'ns_nl') or gr.route_type between 0 and 199)
    and ((cd.date = '2025-05-13' and cd.exception_type = 1) or (ca.start_date <= '2025-05-13' and ca.end_date >= '2025-05-13' and tuesday and not (cd.date = '2025-05-13' and cd.exception_type = 2)))
) 
where dwell_time > 0 -- surely a bug
group by all
order by dwell_time
")
df2 = df
df2 = df[(in.(df.company, Ref(["tgv", "de_long", "intercites"]))) .&& (df.dwell_time .< 20), :] # exclude obvious bugs
transform!(groupby(df2, :company), :c => (x-> x./sum(x)) => :c_norm)
# plot(df2.dwell_time, df2.c_norm, group=df2.company, xlims=(0,20), xlabel="Dwell time, minutes", ylabel="Probability density")
agg = combine(groupby(df2, :company), [:dwell_time, :c_norm] => ((d, n) -> quantile(d, weights(d.*n), 0:0.01:1)) => :dwell_time, :c => (n -> collect(0:0.01:1)) => :q) # there has to be a better way of doing this
plot(agg.dwell_time, agg.q, group=agg.company, xlims=(0,15), xlabel="Dwell time, minutes", ylabel="Dwell time weighted quantile", yticks=0:0.1:1, xticks=0:2:15)

# dwell times as percentage of trip time
df = select_df(con(), "
select company, round(total_dwell/journey_length, 2) dwell_percent, count(*) c from
(select st.company company, sum(dwell_time) total_dwell, tr.trip_id, dateDiff('minute', min(departure_time), max(arrival_time)) journey_length from (select *,
    dateDiff('minute', arrival_time, departure_time) dwell_time
    from gtfs_stop_times st
    left join gtfs_trips tr on tr.trip_id = st.trip_id and tr.company = st.company
    left join gtfs_routes gr on tr.route_id = gr.route_id and tr.company = gr.company
    left join gtfs_calendar ca on tr.service_id = ca.service_id and tr.company = ca.company
    left join gtfs_calendar_dates cd on tr.service_id = cd.service_id and tr.company = cd.company
    where true
    and tr.company not in ('uk', 'ee', 'de_local') -- uk data messed up / ages in the past, ee and de_local too messy to be believable
    --and tr.company in ('ter', 'sncb', 'tgv') -- swiss data too messy?
    and (tr.company not in ('swiss', 'no', 'se', 'dk', 'ns_nl') or gr.route_type between 0 and 199)
    and ((cd.date = '2025-05-13' and cd.exception_type = 1) or (ca.start_date <= '2025-05-13' and ca.end_date >= '2025-05-13' and tuesday and not (cd.date = '2025-05-13' and cd.exception_type = 2)))
    and dwell_time < 20
) 
group by all)
group by all
order by dwell_percent
")
df2 = df
df2 = df[in.(df.company, Ref(["tgv", "de_long", "intercites", "ter"])), :]
transform!(groupby(df2, :company), :c => (x-> x./sum(x)) => :c_norm)
plot(df2.dwell_percent, df2.c_norm, group=df2.company, xlims=(0,0.2)) # noisy, not really credible
agg = combine(groupby(df2, :company), [:dwell_percent, :c_norm] => ((d, n) -> quantile(d, weights(n), 0:0.01:1)) => :dwell_percent, :c => (n -> collect(0:0.01:1)) => :q) # there has to be a better way of doing this
plot(agg.dwell_percent, agg.q, group=agg.company, xlims=(0,1), xlabel="Dwell percentage", ylabel="Quantile", yticks=0:0.1:1) # still looks implausible, IC/ICEs seem to spend to much of their journeys at stops. maybe they just have many more stops?




# trip distances in distance and time. i wonder how many trips there will be. hehe. only 240k that's fine
df = select_df(con(), "
select distinct on (company, total_dist, total_t) company, st.trip_id, min(departure_time) dt, max(arrival_time) at, dateDiff('minute', dt, at) total_t, argMin(s2.stop_name, departure_time) start_stop, argMax(s2.stop_name, arrival_time) end_stop, sum(dist) total_dist from (
select st.company company, st.stop_id, s2.stop_name, st.prev_stop, st.trip_id, geoDistance(s.stop_lon, s.stop_lat, s2.stop_lon, s2.stop_lat)/1000 dist, dateDiff('minute', prev_departure, departure_time)/60 t, dist/t speed, departure_time, arrival_time from (select *,
    lagInFrame(departure_time, 1, departure_time) over (
    --dateDiff(last_value(departure_time), first_value(departure_time)) over (
        partition by st.company, st.trip_id, stop_headsign
        order by departure_time asc
        rows between 1 preceding and current row
    ) prev_departure,
    lagInFrame(stop_id, 1, stop_id) over (
    --dateDiff(last_value(departure_time), first_value(departure_time)) over (
        partition by st.company, st.trip_id, stop_headsign
        order by departure_time asc
        rows between 1 preceding and current row
    ) prev_stop
    from gtfs_stop_times st
    left join gtfs_trips tr on tr.trip_id = st.trip_id and tr.company = st.company
    left join gtfs_routes gr on tr.route_id = gr.route_id and tr.company = gr.company
    left join gtfs_calendar ca on tr.service_id = ca.service_id and tr.company = ca.company
    left join gtfs_calendar_dates cd on tr.service_id = cd.service_id and tr.company = cd.company
    where true
    --and tr.company not in ('uk', 'ee', 'de_local') -- uk data messed up / ages in the past, ee and de_local too messy to be believable
    --and tr.company in ('ter', 'sncb', 'tgv') -- swiss data too messy?
    and (tr.company not in ('swiss', 'no', 'se', 'dk', 'ns_nl') or gr.route_type between 0 and 199)
    --and ((cd.date = '2025-05-13' and cd.exception_type = 1) or (ca.start_date <= '2025-05-13' and ca.end_date >= '2025-05-13' and tuesday and not (cd.date = '2025-05-13' and cd.exception_type = 2))) -- do we actually want this? probably don't care?
) st
left join gtfs_stops s on st.prev_stop = s.stop_id and st.company = s.company
left join gtfs_stops s2 on st.stop_id = s2.stop_id and st.company = s2.company
) group by all
order by total_dist desc
")


df.speed = df.total_dist./(df.total_t./60)
sort!(df, :speed, rev=true)
sort!(df, :total_dist, rev=true)
df2 = df[in.(df.company, Ref(["tgv", "de_long"])), :]
atfront(df[(df.speed .< 400) .&& (df.company .== "tgv") .&& (df.end_stop .== "Marseille Saint-Charles"), :], [:speed]) # some ns ones are ultra dodge
# are ouigos not included or is it really not far to lille? i guess the tgvs all go on to brussels and are therefore quicker?
# nice ville shows up in a lot of these but the data isn't credible. so that's nice (e.g. nice -> nancy i only measure 1000km but it comes out as 1300km)
atfront(df[(df.speed .< 400) .&& (df.company .== "tgv"), :], [:speed]) # some ns ones are ultra dodge
df2 = df
# transform!(groupby(df2, :company), :c => (x-> x./sum(x)) => :c_norm)
# plot(df2.speed, df2.c_norm, group=df2.company)

atfront(df[(df.total_dist .< 1500) .&& (df.speed .< 400), :][1:50, :], [:speed, :total_dist])

df2 = df[(df.total_t .< 60*24) .&& (10 .< df.total_dist .< 2000) .&& (10 .< df.speed .< 400) .&& .!(in.(df.company, Ref(["de_local", "ns_nl", "ee", "dk", "no"]))), :] # the data quality of this stuff is horrendous
transform!(groupby(df2, :company), nrow)
sort!(df2, :nrow, rev=true)
scatter(; legend=:outerright, markersize=1, markerstrokewidth=0.01, xlabel="Total station-to-station straight line distance, km", ylabel="Speed, km/h") # oh god we need to aggregate this
for c in unique(df2.company)
    scatter!(df2[df2.company .== c, :total_dist], df2[df2.company .== c, :speed], label=c, legend=:outerright, markersize=1, markerstrokewidth=0.5, marker=:auto, markeropacity=0.5)
end
scatter!()

df2[(df2.company .== "uk") .&& (df2.total_dist .> 200) .&& (df2.speed .< 50) .&& (df2.total_t .< 60*24), :]

atfront(sort(df2, :total_dist, rev=true), [:total_dist])


# fastest TERs, fastest TGVs. quantiles thereof? probably should weight quantiles by duration? not-belgian dude wanted to know marseille -> paris

sort!(df2, :speed, rev=true)
atfront(df2[(df2.company .== "tgv") .&& (df2.end_stop .== "Marseille Saint-Charles"), :], [:speed])

# take best case speed. make distance/time up
agg = combine(groupby(df2, [:company, :end_stop, :start_stop]), :speed => maximum => :speed, :total_dist => median => :total_dist, :total_t => minimum => :total_t)
sort!(agg, :speed, rev=true)
atfront(agg[(agg.company .== "tgv") .&& (agg.end_stop .== "Marseille Saint-Charles"), :], [:speed])
agg2 = combine(groupby(agg, :company), g -> addquantiles!(g, :speed), :start_stop, :end_stop, :speed, :total_dist, :total_t) # not sure why transform! doesn't work
rename!(agg2, :x1 => :speed_quantile)
atfront(agg2[(agg2.company .== "tgv") .&& (agg2.end_stop .== "Marseille Saint-Charles"), :], [:speed])


scatter(; legend=:outerright, markersize=1, markerstrokewidth=0.01, xlabel="Total station-to-station straight line distance, km", ylabel="Speed, km/h") # aggregated :)
for c in unique(agg2.company)
    scatter!(agg2[agg2.company .== c, :total_dist], agg2[agg2.company .== c, :speed], label=c, legend=:outerright, markersize=1, markerstrokewidth=0.5, marker=:auto, markeropacity=0.5)
end
scatter!()


# so, quickest TGVs
sort!(agg2, :speed, rev=true)
atfront(agg2[(agg2.company .== "tgv"), :], [:speed])
atfront(agg2[(agg2.company .== "intercites"), :][1:20, :], [:speed])
atfront(agg2[(agg2.company .== "de_long"), :][1:20, :], [:speed])

sort!(agg2, :total_dist, rev=true)
atfront(agg2[(agg2.company .== "ter"), :][1:20, :], [:total_dist])
atfront(agg2[(agg2.company .== "intercites"), :][1:20, :], [:total_dist])

# slowest
atfront(agg2[(agg2.company .== "tgv"), :][end:-1:end-19, :], [:speed])
# df2[(df2.company .== "tgv") .&& (df2.start_stop .== "Saint-Gervais-les-Bains-Le Fayet"), :] # doesn't seem to actually exist :(

plot(agg2.speed, agg2.speed_quantile, group=agg2.company, legend=:outerright)


agg3 = agg[(agg.company .!= "swiss") .|| ((agg.speed .< 180) .&& (agg.total_dist .< 200)), :] # exclude TGVs from Swiss data
sort!(agg3, :speed)
transform!(groupby(agg3, :company), [:speed, :total_t] => ((s, t) -> cumsum(s.*t)./sum(s.*t)) => :speed_wq)

plot(agg3.speed, agg3.speed_wq, group=agg3.company, legend=:outerright,
series_annotations=map(c -> text(rand() < 0.003 ? c : "", :bottom, 3), agg3.company), # this is ugly but it helps a bit i guess
xlabel="Station-to-station straight line speed, km/h", ylabel="Journey duration weighted quantile",
) # weighted by how long the route is


####
#
# transitous instead
#
####

df = select_df(con(), """
    select geoToH3(stop_lon, stop_lat, 1) h3, count(*) value from transitous_stops
    group by h3
""")
df.index = string.(df.h3, base=16)
mkpath("$(homedir())/projects/H3-MON/www/data/$(today())")
CSV.write("""$(homedir())/projects/H3-MON/www/data/$(today())/railway_stations.csv""", df[!, [:index, :value]])

select_df(con(), """
    select route_type, count(*) c from transitous_routes
    group by all
    order by c
""")

df = select_df(con(), """
    select geoToH3(stop_lon, stop_lat, 2) h3, anyHeavy(route_type) value from transitous_stops ts
    inner join (select route_type, trip_id, source from transitous_routes tr
    inner join (select arrayJoin(topK(1000)(trip_id)) trip_id, route_id, source from transitous_trips
    group by route_id, source
    ) tt on tt.route_id = tr.route_id and tt.source = tr.source) tr on ts.stop_id = tr.trip_id and ts.source = tr.source
    group by all
""")
df.index = string.(df.h3, base=16)
mkpath("$(homedir())/projects/H3-MON/www/data/$(today())")
CSV.write("""$(homedir())/projects/H3-MON/www/data/$(today())/railway_stations.csv""", df[!, [:index, :value]])

t = select_df(con(), """
    select tst.* from transitous_stops ts
    left join transitous_stop_times tst on ts.stop_id = tst.stop_id
    where ts.stop_name ilike '%cuneo f.s.%'
""")

select_df(con(), """
    select tr.* from transitous_trips tt
    inner join transitous_routes tr on tr.route_id = tt.route_id
    where trip_id in $(julia2clickhouse(t.trip_id))
""")
select_df(con(), """
    select arrayJoin(topK(2)(trip_id)) trip_id, route_id from transitous_trips
    group by route_id
    limit 10
""")
    
# maybe i should have made source the first index after all
t = select_df(con(), """
    select tst.* from transitous_stops ts
    left join transitous_stop_times tst on ts.stop_id = tst.stop_id and ts.source = tst.source
    where geoToH3(stop_lon, stop_lat, 6) = reinterpretAsUInt64(reverse(unhex('861f9a50fffffff')))
""")

df = select_df(con(), """
    select geoToH3(stop_lon, stop_lat, 6) h3, anyHeavy(route_type) value from transitous_stop_times tst
    inner join transitous_trips tt on tst.trip_id = tt.trip_id and tst.source = tt.source
    inner join transitous_routes tr on tt.route_id = tr.route_id and tt.source = tr.source
    inner join transitous_stops ts on ts.stop_id = tt.trip_id and ts.source = tt.source
    group by all
""") # why are they all in romania wtf?
df.index = string.(df.h3, base=16)
mkpath("$(homedir())/projects/H3-MON/www/data/$(today())")
CSV.write("""$(homedir())/projects/H3-MON/www/data/$(today())/railway_stations.csv""", df[!, [:index, :value]])


df = select_df(con(), """
    select distinct * from transitous_stop_times st -- why do we need distinct :(
    left join transitous_trips tr on tr.trip_id = st.trip_id and tr.source = st.source
    left join transitous_routes ro on tr.route_id = ro.route_id and tr.source = ro.source
    left join transitous_calendar ca on tr.service_id = ca.service_id and tr.source = ca.source
    left join transitous_calendar_dates cd on cd.service_id = tr.service_id and cd.source = tr.source
    where true
    and ((cd.date = '2025-05-13' and cd.exception_type = 1) or (ca.start_date <= '2025-05-13' and ca.end_date >= '2025-05-13' and tuesday and not (cd.date = '2025-05-13' and cd.exception_type = 2)))
    limit 10
""")



df = select_df(con(), "
select distinct on (company, total_dist, total_t) company, st.trip_id, min(departure_time) dt, max(arrival_time) at, dateDiff('minute', dt, at) total_t, argMin(s2.stop_name, departure_time) start_stop, argMax(s2.stop_name, arrival_time) end_stop, sum(dist) total_dist from (
select st.company company, st.stop_id, s2.stop_name, st.prev_stop, st.trip_id, geoDistance(s.stop_lon, s.stop_lat, s2.stop_lon, s2.stop_lat)/1000 dist, dateDiff('minute', prev_departure, departure_time)/60 t, dist/t speed, departure_time, arrival_time from (select *,
    lagInFrame(departure_time, 1, departure_time) over (
    --dateDiff(last_value(departure_time), first_value(departure_time)) over (
        partition by st.company, st.trip_id, stop_headsign
        order by departure_time asc
        rows between 1 preceding and current row
    ) prev_departure,
    lagInFrame(stop_id, 1, stop_id) over (
    --dateDiff(last_value(departure_time), first_value(departure_time)) over (
        partition by st.company, st.trip_id, stop_headsign
        order by departure_time asc
        rows between 1 preceding and current row
    ) prev_stop
    from gtfs_stop_times st
    left join gtfs_trips tr on tr.trip_id = st.trip_id and tr.company = st.company
    left join gtfs_routes gr on tr.route_id = gr.route_id and tr.company = gr.company
    left join gtfs_calendar ca on tr.service_id = ca.service_id and tr.company = ca.company
    left join gtfs_calendar_dates cd on tr.service_id = cd.service_id and tr.company = cd.company
    where true
    --and tr.company not in ('uk', 'ee', 'de_local') -- uk data messed up / ages in the past, ee and de_local too messy to be believable
    --and tr.company in ('ter', 'sncb', 'tgv') -- swiss data too messy?
    and (tr.company not in ('swiss', 'no', 'se', 'dk', 'ns_nl') or gr.route_type between 0 and 199)
    --and ((cd.date = '2025-05-13' and cd.exception_type = 1) or (ca.start_date <= '2025-05-13' and ca.end_date >= '2025-05-13' and tuesday and not (cd.date = '2025-05-13' and cd.exception_type = 2))) -- do we actually want this? probably don't care?
) st
left join gtfs_stops s on st.prev_stop = s.stop_id and st.company = s.company
left join gtfs_stops s2 on st.stop_id = s2.stop_id and st.company = s2.company
) group by all
order by total_dist desc
")


df = select_df(con(), """
    select geoToH3(stop_lon, stop_lat, 4) h3, count(*) value from transitous_stop_times_one_day
    group by all
""")
df.index = string.(df.h3, base=16)
mkpath("$(homedir())/projects/H3-MON/www/data/$(today())")
CSV.write("""$(homedir())/projects/H3-MON/www/data/$(today())/station_calls_per_day.csv""", df[!, [:index, :value]])

df = select_df(con(), """
    select * from transitous_stop_times_one_day
    where stop_name = 'Nice-Ville'
    order by arrival_time
    limit 10
""")


# these trips are all listed as running at the same time but they can't
julia> atfront(df, [:trip_id])[5:8, :trip_id]
4-element Vector{String}:
 "OCESA466775R3997023:2025-04-26T22:42:03Z"
 "OCESA466777R3997021:2025-04-26T22:42:03Z"
 "OCESA86002F3839666:2025-03-19T23:52:12Z"
 "OCESA881351F3994630:2025-04-26T22:42:03Z"

select_df(con(), """
     select * from transitous_trips
 where trip_id = 'OCESA466777R3997021:2025-04-26T22:42:03Z'
     limit 1
""")

df = select_df(con(), """
    select * from transitous_stop_times_one_day
    where stop_name = 'Nice-Ville'
    order by arrival_time
    limit 10
""")
# :( 
# julia> df[5:8, [:stop_id, :direction_id]] # it is bus and train but they're listed as the same direction so god knows what's going on

select_df(con(), """
    select * from transitous_calendar_dates
    where true
    and service_id in ('000015' , '003318')
    and source = 'fr_horaires-des-lignes-ter-sncf.gtfs'
    and date = '2025-05-13'
""") # they're both listed as running
select_df(con(), """
    select * from transitous_routes tr
    left semi join (select route_id, source from transitous_trips where trip_id in $(julia2clickhouse(df[5:8, [:stop_id, :direction_id, :service_id, :trip_id]].trip_id))
    and source = 'fr_horaires-des-lignes-ter-sncf.gtfs') tt on tt.route_id = tr.route_id and tt.source = tr.source
""") # .. fine, they're really real


# debugging, sanity check whether a station has plausible headways
df = select_df(con(), """
select * from (
select 
source, direction_id, sane_route_id, departure_time, trip_id, trip_headsign, stop_lon, stop_lat, stop_name,
dateDiff('minute', lagInFrame(departure_time, 1, departure_time) over (
    partition by source, stop_id, sane_route_id
    order by departure_time asc
    rows between 1 preceding and current row
), departure_time) headway
from transitous_stop_times_one_day_sane st
where true
and source ilike 'fr_r%'
--and stop_name = 'Robinson'
and stop_name ilike '%Bourg%Reine%'
and stop_name != trip_headsign
order by departure_time
)
where headway > 9
""")

using UnicodePlots
df = select_df(con(), """
select avg((mod(60, headway) == 0) or (headway = 120)) value, geoToH3(stop_lon, stop_lat, 8) h3 from (
select 
source, stop_id, sane_route_id, departure_time, trip_headsign, stop_lon, stop_lat,
dateDiff('minute', lagInFrame(departure_time, 1, departure_time) over (
    partition by source, stop_id, sane_route_id
    order by departure_time asc
    rows between 1 preceding and current row
), departure_time) headway
from transitous_stop_times_one_day_sane st
where true
and ((trip_headsign = '') or (trip_headsign != stop_name))
)
where headway between 10 and 60*5 -- exclude sub-10 minute headway because we're not following a timetable at that point
group by all
""")
df.index = string.(df.h3, base=16)
mkpath("$(homedir())/projects/H3-MON/www/data/$(today())")
write("""$(homedir())/projects/H3-MON/www/data/$(today())/taktness_hires.json""", JSON.json(Dict(
    "t" => "Fraction of departures following a clockface schedule",
    "raw" => true,
    "c" => "Transitous et al.",
)))
CSV.write("""$(homedir())/projects/H3-MON/www/data/$(today())/taktness_hires.csv""", df[!, [:index, :value]])
# mkpath("$(homedir())/projects/H3-MON/www/data/2025-05-05")
# write("""$(homedir())/projects/H3-MON/www/data/2025-05-05/taktness.json""", JSON.json(Dict(
#     "t" => "Fraction of departures following a clockface schedule",
#     "raw" => true,
#     "c" => "Transitous et al.",
# )))
# CSV.write("""$(homedir())/projects/H3-MON/www/data/2025-05-05/taktness.csv""", df[!, [:index, :value]])

####
#
# Lenient taktishness
#
####
df = select_df(con(), """
select avg(least(mod(60, headway), headway - mod(60, headway)) <= 5) value, geoToH3(stop_lon, stop_lat, 5) h3 from (
select 
source, stop_id, route_id, departure_time, trip_headsign, stop_lon, stop_lat,
dateDiff('minute', lagInFrame(departure_time, 1, departure_time) over (
    partition by source, stop_id, route_id, direction_id, if(direction_id = 0 OR direction_id is null, trip_headsign, null) -- fall back to headsign only if direction_id is not 1
    order by departure_time asc
    rows between 1 preceding and current row
), departure_time) headway
from transitous_stop_times_one_day st
where true
and ((trip_headsign = '') or (trip_headsign != stop_name))
)
where headway between 10 and 60*5 -- exclude sub-10 minute headway because we're not following a timetable at that point
group by all
""")
df.index = string.(df.h3, base=16)
mkpath("$(homedir())/projects/H3-MON/www/data/$(today())")
write("""$(homedir())/projects/H3-MON/www/data/$(today())/taktishness.json""", JSON.json(Dict(
    "t" => "Fraction of departures almost following a clockface schedule",
    "raw" => true,
    "c" => "Transitous et al.",
)))
CSV.write("""$(homedir())/projects/H3-MON/www/data/$(today())/taktishness.csv""", df[!, [:index, :value]])


####
#
# train bed time
#
####
# utilisation over 24 hours

###
#
# by 'trip'
#
###
df = select_df(con(), """
-- number of trains* active in 15 minute intervals throughout the day
-- ignoring ones that started the day before :)
-- * in switzerland we use a broad definition of trains which includes, buses, ferries and apparently even taxis?
-- todo: daft switcherooo here: best effort parse datetime parses to 2025-01-01, toTime parses to 1970-01-2
with probes as (select addMinutes('1970-01-02', number*5) probe from numbers(24*12)) -- +1)) if you want to include the next day
select sum(probe between yes.dt and yes.at) c, probe, source from probes pr
left outer join (
select toTime(min(departure_time)) dt, toTime(max(if(arrival_time >= '2025-01-02', parseDateTimeBestEffort('2025-01-01 23:59:59'), arrival_time))) at, trip_id, source from transitous_stop_times_one_day st
where true
group by trip_id, source
union all
-- this segment here to support trains that run overnight
select toTime(parseDateTimeBestEffort('2025-01-01 00:00:00')) dt, toTime(max(arrival_time)) at, trip_id, source from transitous_stop_times_one_day st
group by trip_id, source
having toDate(min(departure_time)) < toDate(max(arrival_time))
) yes on true -- clickhouse doesn't support < or > on joins so we do an outer join :(
group by probe, source
order by probe
""")

transform!(groupby(df, :company), :c => (x-> x./maximum(x)) => :utilisation)
# df.probe = Time.(df.probe, "yyyy-mm-dd HH:MM:SS.SSS") # not needed if not using the CSV
df.probe = Time.(df.probe) # not needed if not using the CSV
plot(df.probe, df.utilisation, group=df.source, xticks=Time(0):Minute(120):Time(23,59), xrot=45, xlims=(Time(0), Time(23,59)), ylabel="# of services running as fraction of peak")
# JSON.json(Dict("scale" => Dict(zip([0, 0.2, 0.4, 0.6, 0.8, 1], round.(f.([0.0001, 0.2, 0.4, 0.6, 0.8, 0.9999]), sigdigits=2)))))
# scale: data must be 0-1. lhs gives you values of the data as presented in the CSV, rhs gives you labels for those values (i.e. the 'real' values)

###
#
# by 'station'
#
###
df = select_df(con(), """
-- todo: daft switcherooo here: best effort parse datetime parses to 2025-01-01, toTime parses to 1970-01-2
with 
60 as delta,
'2025-01-01' as start,
probes as (select addMinutes(start, number*delta) probe from numbers(24*2*(60/delta))) -- * 2 = 2 days
select any(probe) bedtime, h3 from (
--select argMin(probe, abs(p - 0.1)) bedtime, h3 from (
select sum(yes.dt between probe and addMinutes(probe, delta)) c, c/(max(c) over wndw) p, probe, geoToH3(stop_lon, stop_lat, 5) h3 from probes pr
left outer join (
select departure_time dt, stop_lon, stop_lat from transitous_stop_times_one_day st
where true
--and source ilike 'ch_%'
) yes on true -- clickhouse doesn't support < or > on joins so we do an outer join :(
group by all
window wndw as (
    partition by h3
    rows between unbounded preceding and unbounded following
)
order by probe
)
where probe >= '2025-01-01 17:00:00' -- todo: use 'start' + 5pm
-- where probe >= '1970-01-02 12:00:00' -- lol, i had to increase this because french lunchtimes kept setting it off
and c > 0
and p <= 0.5
group by all
""")
# looks like bedtime past midnight doesn't work :(
df.index = string.(df.h3, base=16)
df.bedtime_int = map(x-> x.instant.periods.value, df.bedtime)
df.t_bedtime = Time.(df.bedtime)
#df = df[df.bedtime_int .> 0, :]
# scaled between zero and one
lower = quantile(df.bedtime_int, 0.01)
upper = quantile(df.bedtime_int, 0.99)
df.value = (df.bedtime_int .- lower) ./ (upper - lower)
sort!(df, :value)
probes = 0:0.2:1.0 # looks like this always needs to be 0:0.2:1.0
probe_times = Time.(df[map(p -> findfirst(>=(p), df.value), probes), :bedtime])
mkpath("$(homedir())/projects/H3-MON/www/data/$(today())")
write("""$(homedir())/projects/H3-MON/www/data/$(today())/bedtime.json""", 
    JSON.json(Dict(
    "t" => "The first time after 5pm at which fewer than 50% of peak services are running",
    "raw" => true,
    "c" => "Transitous et al.",
    "scale" => Dict(zip(probes, probe_times)),
)))
CSV.write("""$(homedir())/projects/H3-MON/www/data/$(today())/bedtime.csv""", df[!, [:index, :value, :t_bedtime]])

# reuse the 'start' thing in the probe narrowing
# fix nap time at 3pm for france, which then goes up to peak (somewhat fixed by starting at 5pm)
# fix places that never drop below 50% (somewhat fixed by going over midnight)


####
#
# Lunch breaks


# this is quicker than doing the 'true' join with probes we were doing above
df = select_df(con(), """
    with 
    60 as delta,
    '2025-01-01' as start,
    probes as (select true dummy, addMinutes(start, number*delta) probe from numbers(24*1*(60/delta))) -- * 2 = 2 days
    select h3, value/mvalue value from (
        select geoToH3(stop_lon, stop_lat, 8) h3, count(*) value, pr.probe, max(value) over wndw mvalue from (
        select *, true dummy from transitous_stop_times_one_day
        --where source ilike 'be_%'
        ) tst
        left join probes pr on tst.dummy = pr.dummy and pr.probe between tst.arrival_time and addMinutes(tst.arrival_time, delta - 1)
        group by all
        window wndw as (
            partition by h3
            rows between unbounded preceding and unbounded following
        )
    )
    where probe = '2025-01-01 12:00:00'
    settings allow_experimental_join_condition = 1
""")
df.index = string.(df.h3, base=16)
mkpath("$(homedir())/projects/H3-MON/www/data/$(today())")
write("""$(homedir())/projects/H3-MON/www/data/$(today())/lunchtime.json""", 
    JSON.json(Dict(
    "t" => "Fraction of peak hourly departures between 12pm and 1pm",
    # "raw" => true,
    "c" => "Transitous et al.",
)))
CSV.write("""$(homedir())/projects/H3-MON/www/data/$(today())/lunchtime.csv""", df[!, [:index, :value]])


####
#
# super cool map of transport accessibility
#
####

df = select_df(con(), """
    with
    10 as res,
    30 as max_dist,
    -- assume that people walk in "straight lines" at half the speed they walk around buildings, roads etc.
    80/2 as walk_speed_per_min
    select pls h3, min(total_wait) time_to_transport from (
    -- value is number of transport per day, t.1 is distance
    -- average wait time is (18*60)/(number of transport per day * 2) + walking time if we say we're awake for 18 hours and we on average only wait half time
        select h3, (18*60)/(departs_per_day*2) wait_time_at_stop, ((2*(t.1)+1)*h3EdgeLengthM(res))/walk_speed_per_min walk_to_stop, walk_to_stop + wait_time_at_stop total_wait, arrayJoin(t.2) pls from (
            select geoToH3(stop_lon, stop_lat, res) h3, arrayJoin(arrayMap(x->(x, h3HexRing(h3, x)), arrayMap(x->toUInt16(x), range(0,max_dist+1)))) t, max(value) departs_per_day from (
                select stop_id, stop_lat, stop_lon, count(*) value from transitous_everything_stop_times_one_day
                where source like 'gb_%'
                group by all
            )
        group by all
        )
    ) st
    -- optional if not in UK / you don't have this table
    left semi join (select geoToH3(lon, lat, res) h3 from uprn_os) uo on h3 = uo.h3 -- exclude places where zero people live
    group by pls
    having time_to_transport <= 60
""")
dropmissing!(df)
df.index = string.(df.h3, base=16)
today = Dates.today()
df.value = df.time_to_transport ./ 60
mkpath("$(homedir())/projects/H3-MON/www/data/transport_stops")
write("""$(homedir())/projects/H3-MON/www/data/transport_stops/$today.json""", 
    JSON.json(Dict(
    "t" => "Approximate time to walk and board nearest public transport, minutes",
    "flip" => true,
    "raw" => true,
    "c" => "Transitous et al.",
    "scale" => Dict(zip(0.0:0.2:1.0, 0:12:60)),
)))
#CSV.write("$(homedir())/projects/H3-MON/www/data/transport_stops/$today.csv", df[df.value .<= 0.45, [:index, :value]])#, :time_to_transport]])
CSV.write("$(homedir())/projects/H3-MON/www/data/transport_stops/$today.csv", df[!, [:index, :value, :time_to_transport]])



####
#
# debugging stops
#
####

df = select_df(con(), """
    select geoToH3(stop_lon, stop_lat, 10) h3, count(*) value from transitous_everything_stop_times_one_day
    where source like 'fr_%'
    group by all
""")
dropmissing!(df)
df.index = string.(df.h3, base=16)
today = Dates.today()
mkpath("$(homedir())/projects/H3-MON/www/data/debug")
CSV.write("$(homedir())/projects/H3-MON/www/data/debug/$today.csv", df[!, [:index, :value]])

df = select_df(con(), """
    with
    10 as res,
    30 as max_dist,
    -- assume that people walk in "straight lines" at half the speed they walk around buildings, roads etc.
    80/2 as walk_speed_per_min
    select pls h3, min(total_wait) time_to_transport from (
    -- value is number of transport per day, t.1 is distance
    -- average wait time is (18*60)/(number of transport per day * 2) + walking time if we say we're awake for 18 hours and we on average only wait half time
        select h3, (18*60)/(departs_per_day*2) wait_time_at_stop, ((2*(t.1)+1)*h3EdgeLengthM(res))/walk_speed_per_min walk_to_stop, walk_to_stop + wait_time_at_stop total_wait, arrayJoin(t.2) pls from (
            select geoToH3(stop_lon, stop_lat, res) h3, arrayJoin(arrayMap(x->(x, h3HexRing(h3, x)), arrayMap(x->toUInt16(x), range(0,max_dist+1)))) t, max(value) departs_per_day from (
                select stop_id, stop_lat, stop_lon, count(*) value from transitous_everything_stop_times_one_day
                where source like 'gb_%'
                group by all
            )
        group by all
        )
    ) st
    -- optional if not in UK / you don't have this table
    left semi join (select geoToH3(lon, lat, res) h3 from uprn_os) uo on h3 = uo.h3 -- exclude places where zero people live
    group by pls
    having time_to_transport <= 60
""")
dropmissing!(df)
df.index = string.(df.h3, base=16)
today = Dates.today()
df.value = df.time_to_transport ./ 60
mkpath("$(homedir())/projects/H3-MON/www/data/debug")
write("""$(homedir())/projects/H3-MON/www/data/debug/$today.json""", 
    JSON.json(Dict(
    "t" => "Approximate time to walk and board nearest public transport, minutes",
    "flip" => true,
    "raw" => true,
    "c" => "Transitous et al.",
    "scale" => Dict(zip(0.0:0.2:1.0, 0:12:60)),
)))
#CSV.write("$(homedir())/projects/H3-MON/www/data/transport_stops/$today.csv", df[df.value .<= 0.45, [:index, :value]])#, :time_to_transport]])
CSV.write("$(homedir())/projects/H3-MON/www/data/debug/$today.csv", df[!, [:index, :value, :time_to_transport]])


# cool that seems fixed. what a palava
select_df(con(), """
select * from transitous_everything_stop_times_one_day
where true
--and source = 'fr_export-quotidien-au-format-gtfs-du-reseau-de-transport-lignes-d-azur.gtfs'
--and stop_id = '282'
and geoToH3(stop_lon, stop_lat, 10) in h3kRing(reinterpretAsUInt64(reverse(unhex('8a3969a08c8ffff'))), 2)
order by departure_time
""")
||||||| parent of 9140a5b (Add more on utilisation rates)
CSV.write("""$(homedir())/projects/H3-MON/www/data/$(today())/taktness.csv""", df[!, [:index, :value]])


####
#
# utilisation over 24 hours but bigger
#
####
df = select_df(con(), """
-- number of trains* active in 15 minute intervals throughout the day
-- ignoring ones that started the day before :)
with probes as (select addMinutes('1970-01-02', number*5) probe from numbers(24*12)) -- +1)) if you want to include the next day
select sum(probe between yes.dt and yes.at) c, probe, source from probes pr
left outer join (
select toTime(min(departure_time)) dt, toTime(max(if(arrival_time >= '2025-01-02', parseDateTimeBestEffort('2025-01-01 23:59:59'), arrival_time))) at, trip_id, source from transitous_everything_stop_times_one_day st
where true
and (false
    or source like 'gb_%'
    or source like 'us_%'
    or source like 'ca_%'
    or source like 'fr_%'
    or source like 'es_%'
    or source like 'ch_%'
)
group by trip_id, source
union all
-- this segment here to support trains that run overnight
select toTime(parseDateTimeBestEffort('2025-01-01 00:00:00')) dt, toTime(max(arrival_time)) at, trip_id, source from transitous_everything_stop_times_one_day st
where true
and (false
    or source like 'gb_%'
    or source like 'us_%'
    or source like 'ca_%'
    or source like 'fr_%'
    or source like 'es_%'
    or source like 'ch_%'
)
group by trip_id, source
having toDate(min(departure_time)) < toDate(max(arrival_time))
) yes on true -- clickhouse doesn't support < or > on joins so we do an outer join :(
group by probe, source
order by probe
""")
df.probe = Time.(df.probe) # not needed if not using the CSV

df.country = map(x->string(x)[1:2], df.source)
agg = combine(groupby(df, [:country, :probe]), :c => sum => :c)
transform!(groupby(agg, :country), :c => (x-> x./maximum(x)) => :utilisation)
# df.probe = Time.(df.probe, "yyyy-mm-dd HH:MM:SS.SSS") # not needed if not using the CSV
plot(agg.probe, agg.utilisation, group=agg.country, xticks=Time(0):Minute(120):Time(23,59), xrot=45, xlims=(Time(0), Time(23,59)), ylabel="# of services running as fraction of peak")

####
#
# utilisation over 24 hours but bigger and only trains
#
####
df = select_df(con(), """
-- number of trains* active in 15 minute intervals throughout the day
-- ignoring ones that started the day before :)
with probes as (select addMinutes('1970-01-02', number*5) probe from numbers(24*12)) -- +1)) if you want to include the next day
select sum(probe between yes.dt and yes.at) c, probe, source from probes pr
left outer join (
select toTime(min(departure_time)) dt, toTime(max(if(arrival_time >= '2025-01-02', parseDateTimeBestEffort('2025-01-01 23:59:59'), arrival_time))) at, trip_id, source from transitous_everything_stop_times_one_day st
where true
and ((route_type = 2) or route_type between 100 and 199)
and (false
    or source like 'gb_%'
    or source like 'us_%'
    or source like 'ca_%'
    or source like 'fr_%'
    or source like 'es_%'
    or source like 'ch_%'
)
group by trip_id, source
union all
-- this segment here to support trains that run overnight
select toTime(parseDateTimeBestEffort('2025-01-01 00:00:00')) dt, toTime(max(arrival_time)) at, trip_id, source from transitous_everything_stop_times_one_day st
where true
and ((route_type = 2) or route_type between 100 and 199)
and (false
    or source like 'gb_%'
    or source like 'us_%'
    or source like 'ca_%'
    or source like 'fr_%'
    or source like 'es_%'
    or source like 'ch_%'
)
group by trip_id, source
having toDate(min(departure_time)) < toDate(max(arrival_time))
) yes on true -- clickhouse doesn't support < or > on joins so we do an outer join :(
group by probe, source
order by probe
""")
df.probe = Time.(df.probe) # not needed if not using the CSV

df.country = map(x->string(x)[1:2], df.source)
agg = combine(groupby(df, [:country, :probe]), :c => sum => :c)
transform!(groupby(agg, :country), :c => (x-> x./maximum(x)) => :utilisation)
# df.probe = Time.(df.probe, "yyyy-mm-dd HH:MM:SS.SSS") # not needed if not using the CSV
plot(agg.probe, agg.utilisation, group=agg.country, xticks=Time(0):Minute(120):Time(23,59), xrot=45, xlims=(Time(0), Time(23,59)), ylabel="# of services running as fraction of peak")
