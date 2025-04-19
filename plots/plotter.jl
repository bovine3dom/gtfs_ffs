#!/bin/julia
using Plots, CSV, DataFrames, Dates

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
order by total_dist desc -- desc desc desc oops
limit 1000
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
