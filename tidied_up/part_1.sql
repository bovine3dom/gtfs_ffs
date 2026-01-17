DROP TABLE IF EXISTS transitous_everything_routes;
DROP TABLE IF EXISTS transitous_everything_trips;
DROP TABLE IF EXISTS transitous_everything_stop_times;
DROP TABLE IF EXISTS transitous_everything_calendar;
DROP TABLE IF EXISTS transitous_everything_calendar_dates;
DROP TABLE IF EXISTS transitous_everything_stops;
DROP TABLE IF EXISTS transitous_everything_agency;

CREATE TABLE transitous_everything_routes
ENGINE MergeTree
ORDER BY (source, route_id, route_type)
AS
SELECT
    toLowCardinality(assumeNotNull(source)) AS source, -- if <100k use LowCardinality
    route_id,
    toLowCardinality(assumeNotNull(agency_id)) AS agency_id,
    route_short_name,
    route_long_name,
    route_desc,
    toUInt16(route_type) AS route_type, -- Route type can be extended, use UInt16
    route_url,
    route_color,
    route_text_color,
    toUInt32OrNull(route_sort_order) AS route_sort_order,
    toUInt8OrNull(continuous_pickup) AS continuous_pickup,
    toUInt8OrNull(continuous_drop_off) AS continuous_drop_off
FROM file('transitous/source=*/routes.txt', 'CSVWithNames', '
    source String,
    route_id String,
    agency_id String,
    route_short_name String,
    route_long_name String,
    route_desc String,
    route_type String,
    route_url String,
    route_color String,
    route_text_color String,
    route_sort_order String,
    continuous_pickup String,
    continuous_drop_off String
')
SETTINGS use_hive_partitioning = 1;

CREATE TABLE transitous_everything_trips
ENGINE MergeTree
ORDER BY (source, route_id, service_id, trip_id)
AS
SELECT
    toLowCardinality(assumeNotNull(tt.source)) AS source,
    tt.route_id route_id,
    service_id,
    trip_id,
    trip_headsign,
    trip_short_name,
    toUInt8OrNull(direction_id) AS direction_id,
    block_id,
    shape_id,
    toUInt8OrNull(wheelchair_accessible) AS wheelchair_accessible,
    toUInt8OrNull(bikes_allowed) AS bikes_allowed
FROM file('transitous/source=*/trips.txt', 'CSVWithNames', '
    source String,
    route_id String,
    service_id String,
    trip_id String,
    trip_headsign String,
    trip_short_name String,
    direction_id String,
    block_id String,
    shape_id String,
    wheelchair_accessible String,
    bikes_allowed String
'
) tt
SETTINGS use_hive_partitioning = 1;

-- maybe it would have been better to go via the calendar first so that we only had trips that were running on x date
CREATE TABLE transitous_everything_stop_times
ENGINE MergeTree
ORDER BY (source, trip_id, stop_id, arrival_time, departure_time)
SETTINGS allow_nullable_key = 1
AS
SELECT
    toLowCardinality(assumeNotNull(st.source)) AS source,
    st.trip_id trip_id,
    parseDateTimeBestEffortOrNull(arrival_time) arrival_time, -- uses 1 jan of current year as base. which is annoying
    parseDateTimeBestEffortOrNull(departure_time) departure_time,
    stop_id,
    toUInt32OrNull(stop_sequence) AS stop_sequence,
    stop_headsign,
    toUInt8OrNull(pickup_type) AS pickup_type,
    toUInt8OrNull(drop_off_type) AS drop_off_type,
    toUInt8OrNull(continuous_pickup) AS continuous_pickup,
    toUInt8OrNull(continuous_drop_off) AS continuous_drop_off,
    toFloat32OrNull(shape_dist_traveled) AS shape_dist_traveled,
    toUInt8OrNull(timepoint) AS timepoint,
    local_zone_id
FROM file('transitous/source=*/stop_times.txt', 'CSVWithNames', '
    source String,
    trip_id String,
    arrival_time String,
    departure_time String,
    stop_id String,
    stop_sequence String,
    stop_headsign String,
    pickup_type String,
    drop_off_type String,
    continuous_pickup String,
    continuous_drop_off String,
    shape_dist_traveled String,
    timepoint String,
    local_zone_id String
') st
SETTINGS use_hive_partitioning = 1;

CREATE TABLE transitous_everything_stops
ENGINE MergeTree
ORDER BY (source, stop_id, stop_lat, stop_lon)
AS
SELECT
    toLowCardinality(assumeNotNull(ts.source)) AS source,
    ts.stop_id stop_id,
    stop_code,
    stop_name,
    stop_desc,
    toFloat64OrZero(stop_lat) AS stop_lat,
    toFloat64OrZero(stop_lon) AS stop_lon,
    zone_id,
    stop_url,
    toUInt8OrNull(location_type) AS location_type,
    parent_station,
    stop_timezone,
    toUInt8OrNull(wheelchair_boarding) AS wheelchair_boarding,
    level_id,
    platform_code
FROM file('transitous/source=*/stops.txt', 'CSVWithNames', '
    source String,
    stop_id String,
    stop_code String,
    stop_name String,
    stop_desc String,
    stop_lat String,
    stop_lon String,
    zone_id String,
    stop_url String,
    location_type String,
    parent_station String,
    stop_timezone String,
    wheelchair_boarding String,
    level_id String,
    platform_code String
') ts
SETTINGS use_hive_partitioning = 1;

CREATE TABLE transitous_everything_calendar
ENGINE MergeTree
ORDER BY (source, service_id, start_date, end_date)
AS
SELECT
    toLowCardinality(assumeNotNull(tc.source)) AS source,
    tc.service_id service_id,
    toUInt8OrZero(monday) AS monday,
    toUInt8OrZero(tuesday) AS tuesday,
    toUInt8OrZero(wednesday) AS wednesday,
    toUInt8OrZero(thursday) AS thursday,
    toUInt8OrZero(friday) AS friday,
    toUInt8OrZero(saturday) AS saturday,
    toUInt8OrZero(sunday) AS sunday,
    toDateOrZero(start_date) AS start_date, -- Assumes YYYYMMDD format
    toDateOrZero(end_date) AS end_date     -- Assumes YYYYMMDD format
FROM file('transitous/source=*/calendar.txt', 'CSVWithNames', '
    source String,
    service_id String,
    monday String,
    tuesday String,
    wednesday String,
    thursday String,
    friday String,
    saturday String,
    sunday String,
    start_date String,
    end_date String
') tc
SETTINGS use_hive_partitioning = 1;

CREATE TABLE transitous_everything_calendar_dates
ENGINE MergeTree
ORDER BY (source, service_id, date, exception_type)
AS
SELECT
    toLowCardinality(assumeNotNull(tcd.source)) AS source,
    tcd.service_id service_id,
    toDateOrZero(date) AS date, -- Assumes YYYYMMDD format
    toUInt8OrZero(exception_type) AS exception_type
FROM file('transitous/source=*/calendar_dates.txt', 'CSVWithNames', '
    source String,
    service_id String,
    date String,
    exception_type String
') tcd
SETTINGS use_hive_partitioning = 1;

CREATE TABLE transitous_everything_agency
ENGINE MergeTree
ORDER BY (source, agency_id)
AS
SELECT
    toLowCardinality(assumeNotNull(ta.source)) AS source,
    ta.agency_id agency_id,
    agency_name,
    agency_url,
    agency_timezone,
    agency_email,
    agency_fare_url,
    agency_lang,
    agency_phone
FROM file('transitous/source=*/agency.txt', 'CSVWithNames', '
    source String,
    agency_id String,
    agency_name String,
    agency_url String,
    agency_timezone String,
    agency_email String,
    agency_fare_url String,
    agency_lang String,
    agency_phone String
') ta
SETTINGS use_hive_partitioning = 1;


-- one big table for convenience
DROP TABLE IF EXISTS transitous_everything_stop_times_one_day;
CREATE TABLE transitous_everything_stop_times_one_day -- 250 seconds 😎
ENGINE MergeTree
order by (source, stop_id, stop_lat, stop_lon, route_type, trip_id, arrival_time, departure_time)
settings allow_nullable_key = 1
AS
WITH
date_bounds AS (
   
    SELECT
        parseDateTimeBestEffort('2025-03-01') AS min_overall_date,
        parseDateTimeBestEffort('2026-01-01') AS max_overall_date
-- if you want to to it programmatically but tbh who cares
--        min(event_date) AS min_overall_date,
--        max(event_date) AS max_overall_date
--    FROM (
--        SELECT start_date AS event_date FROM transitous_calendar WHERE start_date IS NOT NULL
--        UNION ALL
--        SELECT end_date AS event_date FROM transitous_calendar WHERE end_date IS NOT NULL
--        UNION ALL
--        SELECT date AS event_date FROM transitous_calendar_dates WHERE date IS NOT NULL
--    )
--    WHERE event_date IS NOT NULL
),
all_dates_in_range AS (
    SELECT
        (SELECT min_overall_date FROM date_bounds) + toIntervalDay(number) AS check_date, true dummy
    FROM numbers(
            assumeNotNull(1 + toUInt32(dateDiff('day',
                (SELECT min_overall_date FROM date_bounds),
                (SELECT max_overall_date FROM date_bounds)
            )
        ))
    )
    WHERE check_date <= (SELECT max_overall_date FROM date_bounds)
),
calendar_potential_services AS (
    SELECT
        ad.check_date,
        ca.service_id,
        ca.source
    FROM all_dates_in_range AS ad
    JOIN (select *, true dummy from transitous_everything_calendar) AS ca
        ON ad.check_date >= ca.start_date
        AND ad.dummy = ca.dummy
        AND ad.check_date <= ca.end_date
        AND (
            (toDayOfWeek(ad.check_date) = 1 AND ca.monday) OR
            (toDayOfWeek(ad.check_date) = 2 AND ca.tuesday) OR
            (toDayOfWeek(ad.check_date) = 3 AND ca.wednesday) OR
            (toDayOfWeek(ad.check_date) = 4 AND ca.thursday) OR
            (toDayOfWeek(ad.check_date) = 5 AND ca.friday) OR
            (toDayOfWeek(ad.check_date) = 6 AND ca.saturday) OR
            (toDayOfWeek(ad.check_date) = 7 AND ca.sunday)
        )
),
calendar_date_exceptions AS (
    SELECT
        cd.date AS check_date,
        cd.service_id,
        cd.source,
        cd.exception_type
    FROM transitous_everything_calendar_dates AS cd
),
active_services_on_date AS (
    SELECT
        cps.check_date,
        cps.service_id,
        cps.source
    FROM calendar_potential_services AS cps
    LEFT ANTI JOIN calendar_date_exceptions AS cde_remove
        ON cps.check_date = cde_remove.check_date
        AND cps.service_id = cde_remove.service_id
        AND cps.source = cde_remove.source
        AND cde_remove.exception_type = 2

    UNION DISTINCT
    SELECT
        cde_add.check_date,
        cde_add.service_id,
        cde_add.source
    FROM calendar_date_exceptions AS cde_add
    WHERE cde_add.exception_type = 1
      AND cde_add.check_date IN (SELECT check_date FROM all_dates_in_range)
),
services_per_source_per_day AS (
   
    SELECT
        source,
        check_date,
        count() AS num_active_services
                                       
    FROM active_services_on_date
    GROUP BY source, check_date
),
ranked_days_per_source AS (
    SELECT
        source,
        check_date,
        num_active_services,
        ROW_NUMBER() OVER (PARTITION BY source ORDER BY num_active_services DESC, check_date ASC) as rn
    FROM services_per_source_per_day
),
best_days_per_source as (
    SELECT
        source,
        check_date as best_date_for_this_source,
        num_active_services
    FROM ranked_days_per_source
    WHERE rn = 1
    ORDER BY source
),
active_services as (
    SELECT
        ca.service_id service_id,
        ca.source source
    FROM transitous_everything_calendar AS ca
    JOIN best_days_per_source AS bds ON ca.source = bds.source
    LEFT ANTI JOIN transitous_everything_calendar_dates AS cd_remove
        ON ca.service_id = cd_remove.service_id
        AND ca.source = cd_remove.source
        AND cd_remove.date = bds.best_date_for_this_source
        AND cd_remove.exception_type = 2
    WHERE
        ca.start_date <= bds.best_date_for_this_source
        AND ca.end_date >= bds.best_date_for_this_source
        AND (
            (toDayOfWeek(bds.best_date_for_this_source) = 1 AND ca.monday) OR
            (toDayOfWeek(bds.best_date_for_this_source) = 2 AND ca.tuesday) OR
            (toDayOfWeek(bds.best_date_for_this_source) = 3 AND ca.wednesday) OR
            (toDayOfWeek(bds.best_date_for_this_source) = 4 AND ca.thursday) OR
            (toDayOfWeek(bds.best_date_for_this_source) = 5 AND ca.friday) OR
            (toDayOfWeek(bds.best_date_for_this_source) = 6 AND ca.saturday) OR
            (toDayOfWeek(bds.best_date_for_this_source) = 7 AND ca.sunday)
        )
    UNION DISTINCT
   
    SELECT
        cd.service_id,
        cd.source
    FROM transitous_everything_calendar_dates AS cd
    JOIN best_days_per_source AS bds ON cd.source = bds.source
    WHERE
        cd.date = bds.best_date_for_this_source
        AND cd.exception_type = 1
)
select
arrival_time,
bikes_allowed,
block_id,
tst.service_id service_id,
tst.source source,
departure_time,
direction_id,
drop_off_type,
level_id,
local_zone_id,
location_type,
parent_station,
pickup_type,
platform_code,
st.stop_id stop_id,
tst.trip_id trip_id,
ro.continuous_drop_off continuous_drop_off,
ro.continuous_pickup continuous_pickup,
ro.route_id route_id,
route_color,
route_desc,
route_long_name,
route_short_name,
route_sort_order,
route_text_color,
route_type,
route_url,
stop_code,
stop_desc,
stop_headsign,
stop_lat,
stop_lon,
stop_name,
stop_sequence,
stop_timezone,
stop_url,
timepoint,
trip_headsign,
trip_short_name,
wheelchair_accessible,
wheelchair_boarding,
zone_id
from transitous_everything_trips tst
inner join active_services on active_services.service_id = tst.service_id and active_services.source = tst.source
inner join transitous_everything_stop_times st on tst.trip_id = st.trip_id and tst.source = st.source
inner join transitous_everything_stops ts on st.stop_id = ts.stop_id and ts.source = st.source -- lol, st.source not ts.source
--inner join transitous_everything_trips tr on tst.trip_id = tr.trip_id and tst.source = tr.source
inner join transitous_everything_routes ro on tst.route_id = ro.route_id and tst.source = ro.source;

DROP TABLE IF EXISTS transitous_everything_stop_times_one_day_sane;
CREATE TABLE transitous_everything_stop_times_one_day_sane -- ignore route ids, only care about stops
ENGINE MergeTree
order by (source, stop_id, sane_route_id, stop_lat, stop_lon, trip_id, arrival_time, departure_time)
settings allow_nullable_key = 1
AS
with route_uuids as (
    select arrayJoin(trip_id) trip_id, generateUUIDv7() sane_route_id, source from (
        select source, stop_id, groupArray(trip_id) trip_id from (
            select source, groupArray(stop_id) stop_id, trip_id from (
                select source, stop_id, departure_time, trip_id from transitous_everything_stop_times_one_day st 
                order by departure_time asc
            )
            group by all
        )
        group by all
    )
)
select * from transitous_everything_stop_times_one_day st
inner join route_uuids ru on ru.trip_id = st.trip_id and ru.source = st.source;

drop table if exists transitous_everything_edgelist2;
create table transitous_everything_edgelist2
ENGINE MergeTree
ORDER BY (source, stop_id, trip_id, next_stop, stop_lat, stop_lon, departure_time, next_pop, pop_per_minute)
settings allow_nullable_key = 1
AS
select source, trip_id, stop_id, any(stop_lat) stop_lat, any(stop_lon) stop_lon, any(departure_time) departure_time, any(next_stop) next_stop, any(next_arrival) next_arrival, travel_time, sum(population) next_pop, next_pop/travel_time pop_per_minute from (
select source, trip_id, stop_id, stop_lat, stop_lon, departure_time, next_stop, next_arrival, dateDiff('minute', departure_time, next_arrival) travel_time, arrayJoin(arrayFilter(x -> not has(arrayIntersect(ring, next_ring), x), ring)) newh3, population from (
select 
lagInFrame(stop_id, 1, stop_id) over (
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
source, trip_id, stop_id, arrival_time, departure_time, stop_lat, stop_lon,
h3kRing(geoToH3(stop_lon, stop_lat, 9), 4) ring
from transitous_everything_stop_times_one_day_sane
-- no point including NA until we do GHS pop
where source not like 'us%'
and source not like 'ca%'
) st
left join population_h3 pop on newh3 = pop.h3
where next_stop != stop_id
and res = 9
)
where travel_time > 0
group by all;
-- takes aaaaages but it does seem to work (4200 seconds)
exchange tables transitous_everything_edgelist and transitous_everything_edgelist2;


drop table if exists transitous_pop_within_60_2;
create table transitous_pop_within_60_2
engine = MergeTree
order by (lon, lat, h3, pop_in_60)
as
with
10 as res_trans,
9 as res_pop,
2 as res_final,
30 as max_dist,
-- assume that people walk in "straight lines" at half the speed they walk around buildings, roads etc.
80/2 as walk_speed_per_min
-- NB: THIS LON/LAT ORDER SWAPS IN CLICKHOUSE 25+
select assumeNotNull(h3ToGeo(h3).1) lon, assumeNotNull(h3ToGeo(h3).2) lat, assumeNotNull(pop_in_60) pop_in_60, assumeNotNull(h3) h3 from (
    select pls h3, max(pop_in_60) pop_in_60 from (
    -- value is number of transport per day, t.1 is distance
    -- average wait time is (18*60)/(number of transport per day * 2) + walking time if we say we're awake for 18 hours and we on average only wait half time
        select h3, ((2*(t.1)+1)*h3EdgeLengthM(res_trans))/walk_speed_per_min walk_to_stop, walk_to_stop + wait_time_t total_wait, (60 - total_wait - 5) * pop_per_minute_t pop_in_60, arrayJoin(t.2) pls from (
            select geoToH3(stop_lon, stop_lat, res_trans) h3, arrayJoin(arrayMap(x->(x, h3HexRing(h3, x)), arrayMap(x->toUInt16(x), range(0,max_dist+1)))) t, argMax(pop_per_minute, greatest(0, (60 - wait_time - 5) * pop_per_minute)) pop_per_minute_t, argMax(wait_time, greatest(0, (60 - wait_time - 5) * pop_per_minute)) wait_time_t from (
                select source, stop_id, stop_lat, stop_lon, avg(value) pop_per_minute, count(*) departs_per_day, (18*60)/(departs_per_day*2) wait_time from transitous_everything_edgelist te
                left join (
                select source, trip_id, sum(next_pop)/(sum(travel_time) + count()*5) value from transitous_everything_edgelist -- add on 5 min walk
                group by all
                ) tee on te.source = tee.source and te.trip_id = tee.trip_id
                --where source like 'lu_%'
                group by all
                having wait_time <= 55 -- since we always add on a 5 minute walk at the end
            )
        group by all
        )
    ) st
    -- optional if not in UK / you don't have this table
    --left anti join (select geoToH3(lon, lat, res_trans) h3 from uprn_os) uo on h3 = uo.h3 -- exclude places where zero people live
    group by pls
    having pop_in_60 > 0
);
exchange tables transitous_pop_within_60 and transitous_pop_within_60_2;

-- baked table for interaction
create table if not exists transitous_pop_within_60_baked 
engine = MergeTree
order by (res, lon, lat, h3)
as 
select lon, lat, pop_in_60, h3, h3GetResolution(h3) res from transitous_pop_within_60;

-- initial res is 10
insert into transitous_pop_within_60_baked
select 
avg(lon) lon, avg(lat) lat,
median(pop_in_60) pop_in_60, 
h3ToParent(h3, res) h3,
res from transitous_pop_within_60 t,
(select arrayJoin([4, 5, 6, 7, 8, 9]) res) resolutions
group by all;
