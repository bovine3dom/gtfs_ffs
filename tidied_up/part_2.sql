create table transitous_everything_stop_times_one_day_even_saner
engine = MergeTree
order by (source, stop_uuid, sane_route_id, stop_lat, stop_lon, trip_id, arrival_time, departure_time)
settings allow_nullable_key = 1
as
select *, geoToH3(stop_lon, stop_lat, 11) h3 from transitous_everything_stop_times_one_day_sane st
left join transitous_everything_stop_uuids tu on tu.h3 = h3


drop table if exists transitous_everything_edgelist_sane;
create table transitous_everything_edgelist_sane
ENGINE MergeTree
ORDER BY (stop_uuid, source, trip_id, next_stop, stop_lat, stop_lon, departure_time, next_pop, pop_per_minute)
settings allow_nullable_key = 1
AS
select source, trip_id, stop_uuid, any(stop_lat) stop_lat, any(stop_lon) stop_lon, any(departure_time) departure_time, any(next_stop) next_stop, any(next_arrival) next_arrival, travel_time, sum(population) next_pop, any(route_type) route_type, next_pop/travel_time pop_per_minute from (
select source, trip_id, stop_uuid, stop_lat, stop_lon, departure_time, next_stop, next_arrival, dateDiff('minute', departure_time, next_arrival) travel_time, arrayJoin(arrayFilter(x -> not has(arrayIntersect(ring, next_ring), x), ring)) newh3, route_type, population from (
select 
lagInFrame(stop_uuid, 1, stop_uuid) over (
    partition by source, trip_id
    order by arrival_time desc
    rows between 1 preceding and current row
) next_stop,
lagInFrame(arrival_time, 1, arrival_time) over (
    partition by source, trip_id
    order by arrival_time desc
    rows between 1 preceding and current row
) next_arrival,
lagInFrame(ring, 1, ring) over (
    partition by source, trip_id
    order by arrival_time desc
    rows between 1 preceding and current row
) next_ring,
source, trip_id, stop_uuid, arrival_time, departure_time, stop_lat, stop_lon, route_type,
h3kRing(geoToH3(stop_lon, stop_lat, 9), 4) ring
from transitous_everything_stop_times_one_day_even_saner
-- no point including NA until we do GHS pop
where source not like 'us%'
and source not like 'ca%'
) st
left join population_h3 pop on newh3 = pop.h3
where next_stop != stop_uuid
and res = 9
)
where travel_time > 0
group by all;
-- takes aaaaages but it does seem to work (4200 seconds)



-- -- export for router
-- -- 7.9 seconds cyclops, 3 seconds vauban
-- select stop_uuid, next_stop, next_arrival, departure_time from transitous_everything_edgelist_sane ts
-- left join transitous_everything_trips t on ts.trip_id = t.trip_id
-- left join transitous_everything_routes r on t.route_id = r.route_id
-- where true
-- and ((route_type = 2) or (route_type between 100 and 199))
-- and ts.source like 'gb%'
-- -- limit 10
-- into outfile 'edgelist_onlytrains_gb.arrow' -- eugh, wrong format for times for Julia


-- make statistics table for stops
-- select distinct lower(hex(geoToH3(stop_lat, stop_lon, 11))) h3 from (
-- get stops on routes that go at least 100km

drop table if exists transitous_everything_stop_statistics;
create table if not exists transitous_everything_stop_statistics
engine = MergeTree
order by (stop_lat, stop_lon, stop_uuid)
as
select * from (
    select max(crow_km) crow_km, stop_uuid from (
        select sane_route_id, crow_km, arrayJoin(stops) stop_uuid from (
            select sane_route_id, geoDistance(max(stop_lat), max(stop_lon), min(stop_lat), min(stop_lon))/1000 crow_km, groupArray(stop_uuid) stops from transitous_everything_stop_times_one_day_even_saner group by sane_route_id
        )
    )
    group by stop_uuid
) bs left join (
    select stop_uuid, any(stop_lat) stop_lat, any(stop_lon) stop_lon, anyHeavy(stop_name) stop_name,
    arraySort(x -> multiIf(
        -- 1. Air (Highest Priority)
        x = 1100, 1,

        -- 2. Ferry (Standard + Extended)
        x IN (4, 1000, 1200), 2,

        -- 3. Rail / Train (Standard 2 + Monorail 12 + Extended 100s)
        x IN (2, 12) OR (x >= 100 AND x <= 117), 3,

        -- 4. Subway / Metro (Standard 1 + Extended 400s)
        x = 1 OR (x >= 400 AND x <= 405), 4,

        -- 5. Tram / Cable Tram (Standard 0, 5 + Extended 900s)
        x IN (0, 5) OR (x >= 900 AND x <= 906), 5,

        -- 6. Aerial / Funicular (Standard 6, 7 + Extended 1300s, 1400)
        x IN (6, 7, 1400) OR (x >= 1300 AND x <= 1307), 6,

        -- 7. Coach / Intercity Bus (Extended 200s - distinct from local bus)
        (x >= 200 AND x <= 209), 7,

        -- 8. Trolleybus (Standard 11 + Extended 800)
        x = 11 OR x = 800, 8,

        -- 9. Bus (Standard 3 + Extended 700s)
        x = 3 OR (x >= 700 AND x <= 716), 9,

        -- 10. Taxi / Misc / Horse (Lowest Priority)
        999 
    ), groupArray(route_type))[1] AS route_type
    from transitous_everything_stop_times_one_day_even_saner group by stop_uuid
) st on st.stop_uuid = bs.stop_uuid;

-- include all stops for higher zoom levels
drop table if exists transitous_everything_stop_statistics_unmerged;
create table if not exists transitous_everything_stop_statistics_unmerged
engine = MergeTree
order by (stop_lat, stop_lon, stop_uuid)
as
select * from (
    select max(crow_km) crow_km, stop_uuid from (
        select sane_route_id, crow_km, arrayJoin(stops) stop_uuid from (
            select sane_route_id, geoDistance(max(stop_lat), max(stop_lon), min(stop_lat), min(stop_lon))/1000 crow_km, groupArray(stop_uuid) stops from transitous_everything_stop_times_one_day_even_saner group by sane_route_id
        )
    )
    group by stop_uuid
) bs left join (
    select stop_id, any(stop_uuid) stop_uuid, any(stop_lat) stop_lat, any(stop_lon) stop_lon, any(stop_name) stop_name,
    arraySort(x -> multiIf(
        -- 1. Air (Highest Priority)
        x = 1100, 1,

        -- 2. Ferry (Standard + Extended)
        x IN (4, 1000, 1200), 2,

        -- 3. Rail / Train (Standard 2 + Monorail 12 + Extended 100s)
        x IN (2, 12) OR (x >= 100 AND x <= 117), 3,

        -- 4. Subway / Metro (Standard 1 + Extended 400s)
        x = 1 OR (x >= 400 AND x <= 405), 4,

        -- 5. Tram / Cable Tram (Standard 0, 5 + Extended 900s)
        x IN (0, 5) OR (x >= 900 AND x <= 906), 5,

        -- 6. Aerial / Funicular (Standard 6, 7 + Extended 1300s, 1400)
        x IN (6, 7, 1400) OR (x >= 1300 AND x <= 1307), 6,

        -- 7. Coach / Intercity Bus (Extended 200s - distinct from local bus)
        (x >= 200 AND x <= 209), 7,

        -- 8. Trolleybus (Standard 11 + Extended 800)
        x = 11 OR x = 800, 8,

        -- 9. Bus (Standard 3 + Extended 700s)
        x = 3 OR (x >= 700 AND x <= 716), 9,

        -- 10. Taxi / Misc / Horse (Lowest Priority)
        999 
    ), groupArray(route_type))[1] AS route_type
    from transitous_everything_stop_times_one_day_even_saner group by (stop_id, ru.source)
) st on st.stop_uuid = bs.stop_uuid;



-- table for fahrtle
drop table if exists transitous_everything_edgelist_fahrtle;
create table transitous_everything_edgelist_fahrtle
ENGINE MergeTree
ORDER BY (h3, source, stop_uuid, sane_route_id, stop_lat, stop_lon, trip_id, arrival_time, departure_time)
--(stop_uuid, source, sane_route_id, trip_id, next_stop, stop_lat, stop_lon, arrival_time, departure_time)
settings allow_nullable_key = 1
AS
select 
    source, 
    trip_id, 
    stop_uuid, 
    any(stop_lat) as stop_lat, 
    any(arrival_time) as arrival_time,  
    any(stop_lon) as stop_lon, 
    geoToH3(stop_lat, stop_lon, 11) as h3, -- NB: untested, and different order to what is in the rest of this file
    any(departure_time) as departure_time, 
    any(next_stop) as next_stop, 
    any(next_arrival) as next_arrival, 
	any(next_lat) as next_lat,
	any(next_lon) as next_lon,
    travel_time, 
    any(route_type) as route_type, 
    any(stop_name) as stop_name,
    any(route_short_name) as route_short_name,
    any(route_long_name) as route_long_name,
    any(trip_headsign) as trip_headsign,
    any(sane_route_id) as sane_route_id,
    any(route_color) as route_color,
    any(route_text_color) as route_text_color
from (
    select 
        source, trip_id, stop_uuid, stop_lat, stop_lon, departure_time, next_stop, next_arrival, next_lat, next_lon, arrival_time,
        dateDiff('minute', departure_time, next_arrival) as travel_time,
        stop_name, route_short_name, route_long_name, trip_headsign, sane_route_id, route_color, route_text_color,
        route_type, 
    from (
        select
            lagInFrame(stop_uuid, 1, stop_uuid) over (partition by source, trip_id order by arrival_time desc rows between 1 preceding and current row) as next_stop,
            lagInFrame(arrival_time, 1, arrival_time) over (partition by source, trip_id order by arrival_time desc rows between 1 preceding and current row) as next_arrival,
			lagInFrame(stop_lat, 1, stop_lat) over (
				partition by source, trip_id
				order by arrival_time desc
				rows between 1 preceding and current row
			) next_lat,
			lagInFrame(stop_lon, 1, stop_lon) over (
				partition by source, trip_id
				order by arrival_time desc
				rows between 1 preceding and current row
			) next_lon,
            source, trip_id, stop_uuid, arrival_time, departure_time, stop_lat, stop_lon, route_type,
            stop_name,
            route_short_name,
            route_long_name,
            trip_headsign,
            sane_route_id,
            route_color,
            route_text_color,
        from transitous_everything_stop_times_one_day_even_saner
    ) st
    where next_stop != stop_uuid
)
where travel_time > 0
group by all
settings receive_timeout = 10000;
-- receive_timeout needs increasing https://clickhouse.com/docs/operations/settings/settings#receive_timeout
