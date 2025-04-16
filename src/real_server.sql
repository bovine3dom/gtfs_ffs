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

