-- these queries could run fine on files but i got bored of them taking too long so stuck them on a real server with ingest.sql

-- stock utilisation quartiles by hour (nb: for now switzerland probably includes boats etc.)
-- trains that have no stops in a given hour are not included which probably matters for TGVs and intercites
-- periods with zero stops (e.g. overnight) are excluded from the calculation

with activity as (select company, count(*) c, toTime(tumbleStart(departure_time, interval 1 hour)) t from gtfs_stop_times group by t, company)
select company, arrayMap(q -> round(q, 2), quantiles(0.25, 0.5, 0.75)(c/peak)) q from activity act
inner join (select max(c) peak, company from activity group by company) pk on act.company = pk.company
group by company
order by q[2]; -- only ten times faster than doing it on the files :(


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
format CSVWithNames -- can just copy paste it u lazy thing


select company, headway, count(*) n from (select 
company, stop_id, route_id, departure_time,
dateDiff('minute', lagInFrame(departure_time, 1, departure_time) over (
--dateDiff(last_value(departure_time), first_value(departure_time)) over (
    partition by company, stop_id, route_id
    order by departure_time asc
    rows between 1 preceding and current row
), departure_time) headway
from gtfs_stop_times st
inner join 
(
select distinct tr.company, trip_id, route_id from gtfs_trips tr -- not necessarily distinct because join with calendar can mess things up
left join gtfs_calendar ca on tr.service_id = ca.service_id and tr.company = ca.company
left join gtfs_calendar_dates cd on tr.service_id = cd.service_id and tr.company = cd.company
where true
and tr.company != 'uk' -- uk data messed up / ages in the past
and tr.company = 'ter'
and ((cd.date = '2025-05-13' and cd.exception_type = 1) or (ca.start_date <= '2025-05-13' and ca.end_date >= '2025-05-13' and tuesday and not (cd.date = '2025-05-13' and cd.exception_type = 2)))
)
tr 
on st.trip_id = tr.trip_id and st.company = tr.company
) where headway between 1 and 120
group by all
order by company, headway
select * from gtfs_routes where company = 'uk' limit 10;
-- headways: partition with dateDiff(first, second)

select distinct tr.company, trip_id, route_id from gtfs_trips tr -- not necessarily distinct because join with calendar can mess things up
left join gtfs_calendar ca on tr.service_id = ca.service_id and tr.company = ca.company
left join gtfs_calendar_dates cd on tr.service_id = cd.service_id and tr.company = cd.company
where true
and tr.company != 'uk' -- uk data messed up / ages in the past
and tr.company = 'ter'
and ((cd.date = '2025-05-13' and cd.exception_type = 1) or (ca.start_date <= '2025-05-13' and ca.end_date >= '2025-05-13' and tuesday and not (cd.date = '2025-05-13' and cd.exception_type = 2)))
limit 10



--- TGV has extremely hard to believe headways for some stuff
select company, stop_name, stop_id, route_long_name, departure_time, prev_departure, headway, prev_trip, trip_id, direction_id from (select 
tr.company company, stop_id, tr.route_id, st.trip_id trip_id, route_long_name, departure_time, direction_id,
lagInFrame(departure_time, 1, departure_time) over (
--dateDiff(last_value(departure_time), first_value(departure_time)) over (
    partition by st.company, st.stop_id, ro.route_id, direction_id, trip_headsign
    order by departure_time asc
    rows between 1 preceding and current row
) prev_departure,
dateDiff('minute', prev_departure, departure_time) headway,
lagInFrame(st.trip_id, 1, st.trip_id) over (
--dateDiff(last_value(departure_time), first_value(departure_time)) over (
    partition by st.company, st.stop_id, ro.route_id, direction_id, trip_headsign
    order by departure_time asc
    rows between 1 preceding and current row
) prev_trip
from gtfs_stop_times st
inner join 
(
select distinct tr.company, trip_id, route_id, direction_id, trip_headsign from gtfs_trips tr -- not necessarily distinct because join with calendar can mess things up
left join gtfs_calendar ca on tr.service_id = ca.service_id and tr.company = ca.company
left join gtfs_calendar_dates cd on tr.service_id = cd.service_id and tr.company = cd.company
where true
and tr.company != 'uk' -- uk data messed up / ages in the past
and tr.company = 'sncb'
and ((cd.date = '2025-05-13' and cd.exception_type = 1) or (ca.start_date <= '2025-05-13' and ca.end_date >= '2025-05-13' and tuesday and not (cd.date = '2025-05-13' and cd.exception_type = 2)))
)
tr 
on st.trip_id = tr.trip_id and st.company = tr.company
left join gtfs_routes ro on tr.route_id = ro.route_id and tr.company = ro.company
) pls 
left join gtfs_stops stp on pls.stop_id = stp.stop_id and stp.company = pls.company
where headway between 1 and 30
order by company, headway
limit 10



-- some offenders

-- 1. │ tgv     │ Paris Montparnasse Hall 1 - 2 │ StopPoint:OCETGV INOUI-87391003 │ Paris - Quimper TGV                            │ 2025-01-01 08:45:00 │ 2025-01-01 08:43:00 │       2 │ OCESN8707F3995320:2025-04-14T23:19:14Z │ OCESN8790F3852769:2025-02-07T11:15:13Z │ 1            │
-- ^ only the 08:43 TGV is real

-- 1. │ sncb    │ Braine-l'Alleud │ 8814258 │ Anvers-Central -- Charleroi-Central │ 2025-01-01 18:01:00 │ 2025-01-01 18:00:00 │       1 │ 88____:007::8821006:8872009:38:1835:20251212 │ 88____:007::8872009:8821006:38:1906:20251212 │              │
-- ^ only the 18:00 SNCB is real. fixed, it was trip_headsign

-- 1. │ sncb    │ La Louvière-Sud  │ 8882206 │ Namur -- Tournai        │ 2025-01-01 16:32:00 │ 2025-01-01 16:31:00 │       1 │ 88____:007::8885001:8863008:38:1721:20250516   │ 88____:007::8885001:8863008:38:1722:20250613:1 │              │
-- ^ only the 16:31 is real

select * from gtfs_trips tr
left join gtfs_calendar_dates cd on tr.service_id = cd.service_id and tr.company = cd.company
left join gtfs_calendar ca on tr.service_id = ca.service_id and tr.company = ca.company
where true
--and trip_id in ('OCESN8707F3995320:2025-04-14T23:19:14Z', 'OCESN8790F3852769:2025-02-07T11:15:13Z')
and trip_id in ('88____:007::8885001:8863008:38:1721:20250516', '88____:007::8885001:8863008:38:1722:20250613:1')
and ((cd.date = '2025-05-13' and cd.exception_type = 1) or (ca.start_date <= '2025-05-13' and ca.end_date >= '2025-05-13' and tuesday and not (cd.date = '2025-05-13' and cd.exception_type = 2)))
