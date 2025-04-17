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
    partition by company, stop_id, route_id, tr.direction_id, if(tr.company in ('sncb', 'swiss'), tr.trip_headsign, null) -- only sncb uses headsign?
    order by departure_time asc
    rows between 1 preceding and current row
), departure_time) headway
from gtfs_stop_times st
inner join 
(
select distinct tr.company, trip_id, route_id, direction_id, trip_headsign from gtfs_trips tr -- not necessarily distinct because join with calendar can mess things up
left join gtfs_calendar ca on tr.service_id = ca.service_id and tr.company = ca.company
left join gtfs_calendar_dates cd on tr.service_id = cd.service_id and tr.company = cd.company
where true
and tr.company != 'uk' -- uk data messed up / ages in the past
--and tr.company in ('ter', 'sncb', 'tgv') -- swiss data too messy?
and ((cd.date = '2025-05-13' and cd.exception_type = 1) or (ca.start_date <= '2025-05-13' and ca.end_date >= '2025-05-13' and tuesday and not (cd.date = '2025-05-13' and cd.exception_type = 2)))
)
tr 
on st.trip_id = tr.trip_id and st.company = tr.company
) where headway between 1 and 60*5
group by all
order by company, headway
""")
transform!(groupby(df, :company), :n => (x-> x./maximum(x)) => :n_norm)
ticks = [10, 15, 30, 60, 120, 240]
plot(df.headway, df.n_norm, group=df.company, xscale=:log10, xlims=(10, 60*5), xticks=(ticks, string.(ticks))) # looks UK data is messed up, maybe they're not using real route IDs :(
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
