-- setup for long queries
SET receive_timeout = 40000;
SET send_timeout = 40000;
SET max_threads = 5;
SET max_execution_time = 0;
SET max_result_rows = 0;
SET max_result_bytes = 0;
SET receive_timeout = 40000;
SET send_timeout = 40000;
SET connect_timeout_with_failover_ms = 40000000;
SET http_connection_timeout = 40000;
SET http_send_timeout = 40000;
SET http_receive_timeout = 40000;

 -- avoid oom killer
SET max_bytes_before_external_group_by = '90G';
SET max_memory_usage = '100G';

-- isn't this just an extra column? why aren't we using alter
drop table if exists transitous_everything_20260213_stop_times_one_day_even_saner2;
create table transitous_everything_20260213_stop_times_one_day_even_saner2
engine = MergeTree
order by (source, sane_route_id, trip_id, departure_time) -- order optimised for fahrtle
settings allow_nullable_key = 1
as
select *, geoToH3(stop_lat, stop_lon, 11) h3 from transitous_everything_20260213_stop_times_one_day_sane st
left join transitous_everything_20260213_stop_uuids tu on tu.h3 = h3


drop table if exists transitous_everything_20260213_edgelist_sane;
create table transitous_everything_20260213_edgelist_sane
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
h3kRing(geoToH3(stop_lat, stop_lon, 9), 4) ring
from transitous_everything_20260213_stop_times_one_day_even_saner2
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

DROP TABLE IF EXISTS transitous_everything_20260213_stop_statistics;
CREATE TABLE IF NOT EXISTS transitous_everything_20260213_stop_statistics
ENGINE = MergeTree
ORDER BY (stop_lat, stop_lon, stop_uuid)
AS
SELECT 
    bs.crow_km as crow_km,
    st.stop_uuid as stop_uuid,
    st.stop_lat as stop_lat,
    st.stop_lon as stop_lon,
    st.stop_name as stop_name,
    st.route_type as route_type
FROM (
    SELECT 
        max(crow_km) AS crow_km, 
        stop_uuid 
    FROM (
        SELECT 
            sane_route_id, 
            geoDistance(max(stop_lat), max(stop_lon), min(stop_lat), min(stop_lon))/1000 AS crow_km, 
            groupArray(stop_uuid) AS stops 
        FROM transitous_everything_20260213_stop_times_one_day_even_saner2 
        GROUP BY sane_route_id
    )
    ARRAY JOIN stops AS stop_uuid
    GROUP BY stop_uuid
) bs 
LEFT JOIN (
    SELECT 
        stop_uuid,
        argMin(stop_lat, (route_priority_rank, -departure_count)) AS stop_lat,
        argMin(stop_lon, (route_priority_rank, -departure_count)) AS stop_lon,
        argMin(stop_name, (route_priority_rank, -departure_count)) AS stop_name,
        argMin(route_type, (route_priority_rank, -departure_count)) AS route_type
    FROM (
        SELECT 
            stop_uuid,
            stop_lat, 
            stop_lon, 
            stop_name, 
            route_type,
            count() as departure_count,
            multiIf(
                -- 1. Air
                route_type = 1100, 1,
                -- 2. Ferry
                route_type IN (4, 1000, 1200), 2,
                -- 3. Rail / Train
                route_type IN (2, 12) OR (route_type >= 100 AND route_type <= 117), 3,
                -- 4. Subway / Metro
                route_type = 1 OR (route_type >= 400 AND route_type <= 405), 4,
                -- 5. Tram
                route_type IN (0, 5) OR (route_type >= 900 AND route_type <= 906), 5,
                -- 6. Aerial
                route_type IN (6, 7, 1400) OR (route_type >= 1300 AND route_type <= 1307), 6,
                -- 7. Coach
                (route_type >= 200 AND route_type <= 209), 7,
                -- 8. Trolleybus
                route_type = 11 OR route_type = 800, 8,
                -- 9. Bus
                route_type = 3 OR (route_type >= 700 AND route_type <= 716), 9,
                -- 10. Default
                999 
            ) AS route_priority_rank
        FROM transitous_everything_20260213_stop_times_one_day_even_saner2
        GROUP BY stop_uuid, stop_lat, stop_lon, stop_name, route_type
    )
    GROUP BY stop_uuid
) st ON st.stop_uuid = bs.stop_uuid;

-- include all stops for higher zoom levels
drop table if exists transitous_everything_20260213_stop_statistics_unmerged2;
create table if not exists transitous_everything_20260213_stop_statistics_unmerged2
engine = MergeTree
order by (stop_lat, stop_lon, crow_km, stop_uuid)
as
select * from (
    select max(crow_km) crow_km, stop_id_tuple.1 stop_id, stop_id_tuple.2 source, stop_id_tuple.3 stop_uuid from (
        select sane_route_id, crow_km, arrayJoin(stops) stop_id_tuple from (
            select sane_route_id, geoDistance(max(stop_lat), max(stop_lon), min(stop_lat), min(stop_lon))/1000 crow_km, groupArray((stop_id, source, stop_uuid)) stops from transitous_everything_20260213_stop_times_one_day_even_saner2 group by sane_route_id
        )
    )
    group by stop_id_tuple
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
    from transitous_everything_20260213_stop_times_one_day_even_saner2 group by (stop_id, ru.source)
) st on st.stop_uuid = bs.stop_uuid and st.stop_id = bs.stop_id;

-- same as unmerged2 but calculates crow_km based on remaining stops
drop table if exists transitous_everything_20260213_stop_statistics_unmerged3;
create table if not exists transitous_everything_20260213_stop_statistics_unmerged3
engine = MergeTree
order by (stop_lat, stop_lon, crow_km, stop_uuid)
as
select * from (
    select 
        max(dist_to_end_km) as crow_km, 
        stop_id, 
        source, 
        stop_uuid 
    from (
        select 
            sane_route_id,
            s_tuple.1 as stop_id,
            s_tuple.2 as source,
            s_tuple.3 as stop_uuid,
            geoDistance(s_tuple.4, s_tuple.5, term_lat, term_lon)/1000 as dist_to_end_km
        from (
            select 
                sane_route_id,
                arraySort(t -> t.6, groupArray((stop_id, source, stop_uuid, stop_lat, stop_lon, arrival_time))) as stops_sorted,
                stops_sorted[-1].4 as term_lat,
                stops_sorted[-1].5 as term_lon
            from transitous_everything_20260213_stop_times_one_day_even_saner2 
            group by sane_route_id
        )
        ARRAY JOIN stops_sorted as s_tuple
    )
    group by stop_id, source, stop_uuid
) bs left join (
    -- This section (The Stop Types) remains exactly the same as your original query
    select stop_id, any(stop_uuid) stop_uuid, any(stop_lat) stop_lat, any(stop_lon) stop_lon, any(stop_name) stop_name,
    arraySort(x -> multiIf(
        x = 1100, 1,
        x IN (4, 1000, 1200), 2,
        x IN (2, 12) OR (x >= 100 AND x <= 117), 3,
        x = 1 OR (x >= 400 AND x <= 405), 4,
        x IN (0, 5) OR (x >= 900 AND x <= 906), 5,
        x IN (6, 7, 1400) OR (x >= 1300 AND x <= 1307), 6,
        (x >= 200 AND x <= 209), 7,
        x = 11 OR x = 800, 8,
        x = 3 OR (x >= 700 AND x <= 716), 9,
        999 
    ), groupArray(route_type))[1] AS route_type
    from transitous_everything_20260213_stop_times_one_day_even_saner2 group by (stop_id, source)
) st on st.stop_uuid = bs.stop_uuid and st.stop_id = bs.stop_id;



-- table for fahrtle
drop table if exists transitous_everything_20260213_edgelist_fahrtle;
create table transitous_everything_20260213_edgelist_fahrtle
ENGINE MergeTree
ORDER BY (h3, source, stop_uuid, sane_route_id, stop_lat, stop_lon, trip_id, arrival_time, departure_time)
settings allow_nullable_key = 1
AS
select 
    source, 
    trip_id, 
    stop_uuid, 
    any(stop_lat) as stop_lat, 
    any(arrival_time) as arrival_time,  
    any(stop_lon) as stop_lon, 
    geoToH3(stop_lat, stop_lon, 11) as h3,
    geoToH3(next_lat, next_lon, 11) as next_h3,
    any(departure_time) as departure_time, 
    any(next_stop) as next_stop, 
    any(next_arrival) as next_arrival, 
	any(next_lat) as next_lat,
	any(next_lon) as next_lon,
    any(final_stop) as final_stop,
    any(final_arrival) as final_arrival,
    any(final_lat) as final_lat,
    any(final_lon) as final_lon,
    any(final_name) as final_name,
    any(initial_stop) as initial_stop,
    any(initial_arrival) as initial_arrival,
    any(initial_lat) as initial_lat,
    any(initial_lon) as initial_lon,
    any(initial_name) as initial_name,
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
        final_stop, final_arrival, final_lat, final_lon, final_name, initial_stop, initial_arrival, initial_lat, initial_lon, initial_name,
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
            first_value(stop_uuid) over (partition by source, trip_id order by arrival_time desc rows between unbounded preceding and current row) as final_stop,
            first_value(arrival_time) over (partition by source, trip_id order by arrival_time desc rows between unbounded preceding and current row) as final_arrival,
            first_value(stop_lat) over (partition by source, trip_id order by arrival_time desc rows between unbounded preceding and current row) as final_lat,
            first_value(stop_lon) over (partition by source, trip_id order by arrival_time desc rows between unbounded preceding and current row) as final_lon,
            first_value(stop_name) over (partition by source, trip_id order by arrival_time desc rows between unbounded preceding and current row) as final_name,
            last_value(stop_uuid) over (partition by source, trip_id order by arrival_time desc rows between current row and unbounded following) as initial_stop,
            last_value(arrival_time) over (partition by source, trip_id order by arrival_time desc rows between current row and unbounded following) as initial_arrival,
            last_value(stop_lat) over (partition by source, trip_id order by arrival_time desc rows between current row and unbounded following) as initial_lat,
            last_value(stop_lon) over (partition by source, trip_id order by arrival_time desc rows between current row and unbounded following) as initial_lon,
            last_value(stop_name) over (partition by source, trip_id order by arrival_time desc rows between current row and unbounded following) as initial_name,
            source, trip_id, stop_uuid, arrival_time, departure_time, stop_lat, stop_lon, route_type,
            stop_name,
            route_short_name,
            route_long_name,
            trip_headsign,
            sane_route_id,
            route_color,
            route_text_color,
        from transitous_everything_20260213_stop_times_one_day_even_saner2
    ) st
    where next_stop != stop_uuid
)
where travel_time > 0 -- hmm, does this mean we remove all arrivals?
group by all
settings receive_timeout = 10000;
-- receive_timeout needs increasing https://clickhouse.com/docs/operations/settings/settings#receive_timeout

-- TODO: test this optimisation which avoids window functions
-- yeah it is a bazillion times faster. maybe. it sits chilling at 99% for a while
-- 850 seconds? i think that's slower
DROP TABLE IF EXISTS transitous_everything_20260213_edgelist_fahrtle2;
CREATE TABLE transitous_everything_20260213_edgelist_fahrtle2
ENGINE = MergeTree
-- Optimized for H3 Geospatial lookups
ORDER BY (h3, departure_time, arrival_time)
SETTINGS allow_nullable_key = 1
AS
WITH trip_data AS (
    SELECT
        source,
        trip_id,
        sane_route_id,
        
        -- Static Trip info (Optimization: take any(), these don't change per trip)
        any(route_short_name) as route_short_name,
        any(route_long_name) as route_long_name,
        any(route_type) as route_type,
        any(route_color) as route_color,
        any(route_text_color) as route_text_color,
        any(trip_headsign) as trip_headsign,

        -- THE MAGIC: Create the array, but sort by TIME, not SEQUENCE.
        arraySort(x -> x.1, groupArray(
            (
                -- 1. SORT KEY (The "Safe" Time)
                -- If Arrival is set (not 1970/0), use it. Otherwise use Departure.
                -- This handles Start/End stops correctly.
                if(toUInt32(arrival_time) > 0, arrival_time, departure_time),
                
                -- 2. TIE BREAKER
                -- If times are identical (common in some feeds), use sequence to keep stable order.
                stop_sequence,

                -- 3. The actual data we need later
                stop_id,       -- mapped to stop_uuid
                stop_lat,      
                stop_lon,      
                arrival_time,  
                departure_time,
                stop_name      
            )
        )) AS stops
    FROM transitous_everything_20260213_stop_times_one_day_sane
    GROUP BY source, trip_id, sane_route_id
)
SELECT
    source,
    trip_id,
    sane_route_id,
    
    -- Geospatial Index
    geoToH3(curr_stop.4, curr_stop.5, 11) AS h3,
    geoToH3(next_stop_arr.4, next_stop_arr.5, 11) AS next_h3,

    -- Current Stop
    curr_stop.3 AS stop_uuid,
    curr_stop.4 AS stop_lat,
    curr_stop.5 AS stop_lon,
    curr_stop.6 AS arrival_time,
    curr_stop.7 AS departure_time,
    curr_stop.8 AS stop_name,

    -- Next Stop
    next_stop_arr.3 AS next_stop,
    next_stop_arr.4 AS next_lat,
    next_stop_arr.5 AS next_lon,
    next_stop_arr.6 AS next_arrival,

    -- Trip Metadata
    route_short_name,
    route_long_name,
    route_type,
    route_color,
    route_text_color,
    trip_headsign,

    -- Travel Time (Minutes)
    -- We use Next Arrival - Current Departure
    dateDiff('minute', curr_stop.7, next_stop_arr.6) AS travel_time,

    -- First Stop (Index 1 of the time-sorted array)
    stops[1].3 AS initial_stop,
    stops[1].4 AS initial_lat,
    stops[1].5 AS initial_lon,
    stops[1].6 AS initial_arrival,
    stops[1].8 AS initial_name,

    -- Final Stop (Index -1 of the time-sorted array)
    stops[-1].3 AS final_stop,
    stops[-1].4 AS final_lat,
    stops[-1].5 AS final_lon,
    stops[-1].6 AS final_arrival,
    stops[-1].8 AS final_name

FROM trip_data
-- Slicing logic:
-- curr_stop = items 1 to N-1
-- next_stop = items 2 to N
ARRAY JOIN 
    arraySlice(stops, 1, length(stops) - 1) AS curr_stop,
    arraySlice(stops, 2) AS next_stop_arr;
-- Ensure we don't have negative travel times (e.g. broken feeds where next arrives before current departs)
-- WHERE travel_time >= 0; -- not sure we need this

-- ALTER TABLE transitous_everything_edgelist_fahrtle ADD COLUMN next_h3 UInt64 DEFAULT geoToH3(next_lat, next_lon, 11);
-- OPTIMIZE TABLE transitous_everything_edgelist_fahrtle;

-- let's see if we need it first
-- ALTER TABLE transitous_everything_20260213_edgelist_fahrtle
--     ASS PROJECTION prj_arrivals (
--         SELECT *
--         ORDER BY (next_h3, arrival_time) -- do we need any more indices?
--     );
