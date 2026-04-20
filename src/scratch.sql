-- clickhouse-local v25+
-- select min(YYYYMMDDToDate(toUInt32(date))) from file('../data/*/calendar_dates.txt', 'CSVWithNames') -- oops these are just exceptions. who cares

select sparkbar(24)(t, c), company from (select company, count(*) c, toTime(parseDateTimeBestEffortOrNull(departure_time)) t from file('../data/company=*/stop_times.txt', 'CSVWithNames') group by t, company) group by company;



-- stock utilisation quartiles by hour (nb: for now switzerland probably includes boats etc.)
-- trains that have no stops in a given hour are not included which probably matters for TGVs and intercites
-- periods with zero stops (e.g. overnight) are excluded from the calculation
with activity as (select company, count(*) c, toTime(tumbleStart(parseDateTimeBestEffortOrNull(departure_time), interval 1 hour)) t from file('../data/company=*/stop_times.txt', 'CSVWithNames') group by t, company)
select company, arrayMap(q -> round(q, 2), quantiles(0.25, 0.5, 0.75)(c/peak)) q from activity act
inner join (select max(c) peak, company from activity group by company) pk on act.company = pk.company
group by company
order by q[2];


-- latest arriving trains at least one calendar day after departure (i.e. interrail hacks)
select distinct * from ( select
any(initial_name) departure, 
min(departure_time) departure_t,
any(final_name) arrival, 
max(arrival_time) arrival_t
from transitous_everything_20260218_edgelist_fahrtle2
where ( route_type between 100 and 199 or route_type = 2 )
and source not like 'ca_%'
and source not like 'in_%'
and source not like 'us_%'
group by trip_id, source
having toDayOfYear(min(departure_time)) < toDayOfYear(max(arrival_time))
order by max(arrival_time) desc
limit 100);
