-- setup for long queries
-- run with
-- clickhouse-client --receive_timeout=300000 --send_timeout=300000 --max_execution_time=0 --max_result_rows=0 --max_result_bytes=0 
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

DROP TABLE IF EXISTS transitous_everything_20260218_routes;
DROP TABLE IF EXISTS transitous_everything_20260218_trips;
DROP TABLE IF EXISTS transitous_everything_20260218_stop_times;
DROP TABLE IF EXISTS transitous_everything_20260218_calendar;
DROP TABLE IF EXISTS transitous_everything_20260218_calendar_dates;
DROP TABLE IF EXISTS transitous_everything_20260218_stops;
DROP TABLE IF EXISTS transitous_everything_20260218_agency;

CREATE TABLE transitous_everything_20260218_routes
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
FROM file('chungus/transitous/2026-02-13/source=*/routes.txt', 'CSVWithNames', '
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

CREATE TABLE transitous_everything_20260218_trips
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
FROM file('chungus/transitous/2026-02-13/source=*/trips.txt', 'CSVWithNames', '
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

CREATE TABLE transitous_everything_20260218_stops
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
FROM file('chungus/transitous/2026-02-13/source=*/stops.txt', 'CSVWithNames', '
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

CREATE TABLE transitous_everything_20260218_calendar
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
FROM file('chungus/transitous/2026-02-13/source=*/calendar.txt', 'CSVWithNames', '
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

CREATE TABLE transitous_everything_20260218_calendar_dates
ENGINE MergeTree
ORDER BY (source, service_id, date, exception_type)
AS
SELECT
    toLowCardinality(assumeNotNull(tcd.source)) AS source,
    tcd.service_id service_id,
    toDateOrZero(date) AS date, -- Assumes YYYYMMDD format
    toUInt8OrZero(exception_type) AS exception_type
FROM file('chungus/transitous/2026-02-13/source=*/calendar_dates.txt', 'CSVWithNames', '
    source String,
    service_id String,
    date String,
    exception_type String
') tcd
SETTINGS use_hive_partitioning = 1;

CREATE TABLE transitous_everything_20260218_agency
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
FROM file('chungus/transitous/2026-02-13/source=*/agency.txt', 'CSVWithNames', '
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

DROP TABLE IF EXISTS transitous_everything_20260218_stop_times;
CREATE TABLE transitous_everything_20260218_stop_times
(
    source LowCardinality(String),
    trip_id String,
    arrival_time DateTime,
    departure_time DateTime,
    stop_id String,
    stop_sequence Nullable(UInt32),
    stop_headsign String,
    pickup_type Nullable(UInt8),
    drop_off_type Nullable(UInt8),
    continuous_pickup Nullable(UInt8),
    continuous_drop_off Nullable(UInt8),
    shape_dist_traveled Nullable(Float32),
    timepoint Nullable(UInt8),
    local_zone_id String
)
ENGINE = MergeTree
ORDER BY (source, trip_id, stop_id, arrival_time, departure_time)
SETTINGS allow_nullable_key = 1;

INSERT INTO transitous_everything_20260218_stop_times
SELECT
    toLowCardinality(assumeNotNull(st.source)) AS source,
    st.trip_id,
    parseDateTimeBestEffortOrNull(arrival_time) AS arrival_time,
    parseDateTimeBestEffortOrNull(departure_time) AS departure_time,
    stop_id,
    toUInt32OrNull(stop_sequence),
    stop_headsign,
    toUInt8OrNull(pickup_type),
    toUInt8OrNull(drop_off_type),
    toUInt8OrNull(continuous_pickup),
    toUInt8OrNull(continuous_drop_off),
    toFloat32OrNull(shape_dist_traveled),
    toUInt8OrNull(timepoint),
    local_zone_id
FROM file('chungus/transitous/2026-02-13/source=*/stop_times.txt', 'CSVWithNames', '
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

-- Now generate route_uuids based on the stop sequence for every trip
DROP TABLE IF EXISTS transitous_everything_20260218_trip_route_uuids;
CREATE TABLE transitous_everything_20260218_trip_route_uuids
ENGINE = MergeTree
ORDER BY (source, trip_id)
AS 
SELECT
    source,
    trip_id,
    -- maybe we should include source in the hash? we get a couple of collisions
    reinterpretAsUUID(sipHash128Reference(
        -- inner sort here stops ch sorting the _entire_ table at once
        arrayMap(x -> x.2, arraySort(
            groupArray(
                -- hash stop to shrink
                (departure_time, xxh3(stop_id))
            )
        )
    ))) as sane_route_id
FROM transitous_everything_20260218_stop_times
GROUP BY source, trip_id;

-- Now calculate the best day per sane_route_id
DROP TABLE IF EXISTS transitous_everything_20260218_valid_trips_lookup;
CREATE TABLE transitous_everything_20260218_valid_trips_lookup
ENGINE = MergeTree()
ORDER BY (source, trip_id)
AS
WITH
    -- 1. Define bounds once
    toDate('2020-12-01') AS min_date, -- todo: find these automatically?
    toDate('2030-01-01') AS max_date,

    -- 2. Pre-calculate Trip Counts per Service/Route
    -- This collapses 25M rows into ~500k rows before we even look at dates.
    route_service_counts AS (
        SELECT
            t.source,
            t.service_id,
            tru.sane_route_id,
            count() AS trip_count,
            groupArray(t.trip_id) as trip_ids -- Keep trip IDs packed for later
        FROM transitous_everything_20260218_trips t
        JOIN transitous_everything_20260218_trip_route_uuids tru
            ON t.source = tru.source AND t.trip_id = tru.trip_id
        GROUP BY t.source, t.service_id, tru.sane_route_id
    ),

    -- 3. Efficiently Expand Calendar to Valid Dates
    -- Instead of joining a date table, we generate dates on the fly for each service.
    valid_service_dates AS (
        SELECT
            source,
            service_id,
            check_date
        FROM
        (
            -- 3a. Expand Regular Schedule (Calendar)
            SELECT
                source,
                service_id,
                arrayJoin(
                    arrayMap(x -> toDate(x), 
                        range(toUInt32(greatest(start_date, min_date)), toUInt32(least(end_date, max_date)) + 1)
                    )
                ) AS check_date,
                monday, tuesday, wednesday, thursday, friday, saturday, sunday
            FROM transitous_everything_20260218_calendar
            WHERE end_date >= min_date AND start_date <= max_date
        )
        WHERE 
            -- Quick Bitmask Check for Day of Week
            CASE toDayOfWeek(check_date)
                WHEN 1 THEN monday WHEN 2 THEN tuesday WHEN 3 THEN wednesday
                WHEN 4 THEN thursday WHEN 5 THEN friday WHEN 6 THEN saturday
                WHEN 7 THEN sunday ELSE 0
            END = 1
        
        UNION DISTINCT
        
        -- 3b. Additions (Calendar Dates Type 1)
        SELECT source, service_id, date as check_date
        FROM transitous_everything_20260218_calendar_dates
        WHERE exception_type = 1 AND date BETWEEN min_date AND max_date

        EXCEPT
        
        -- 3c. Removals (Calendar Dates Type 2)
        SELECT source, service_id, date as check_date
        FROM transitous_everything_20260218_calendar_dates
        WHERE exception_type = 2 AND date BETWEEN min_date AND max_date
    ),

    -- 4. Find the "Best Date" per Route using argMax (No Sorting!)
    best_date_per_route AS (
        SELECT
            source,
            sane_route_id,
            -- Find the date that had the highest sum of trips
            argMax(check_date, total_trips_on_day) AS best_date
        FROM
        (
            SELECT
                r.source,
                r.sane_route_id,
                v.check_date,
                sum(r.trip_count) AS total_trips_on_day
            FROM route_service_counts r
            JOIN valid_service_dates v 
                ON r.source = v.source AND r.service_id = v.service_id
            GROUP BY r.source, r.sane_route_id, v.check_date
        )
        GROUP BY source, sane_route_id
    )
-- 5. Final Assembly
-- We have the best date for every route. Now grab the trips that are active on that specific date.
SELECT DISTINCT
    r.source source,
    arrayJoin(r.trip_ids) trip_id
FROM route_service_counts r
JOIN best_date_per_route b
    ON r.source = b.source AND r.sane_route_id = b.sane_route_id
JOIN valid_service_dates v
    ON r.source = v.source 
    AND r.service_id = v.service_id 
    AND v.check_date = b.best_date;

DROP TABLE IF EXISTS transitous_everything_20260218_stop_times_one_day_sane;
CREATE TABLE transitous_everything_20260218_stop_times_one_day_sane
ENGINE = MergeTree
ORDER BY (source, stop_lat, stop_lon, sane_route_id, trip_id, stop_sequence)
SETTINGS allow_nullable_key = 1
AS
SELECT
    -- 1. Identifiers
    st.source AS source,
    tru.sane_route_id AS sane_route_id, -- UUID
    st.trip_id AS trip_id,
    st.stop_id AS stop_id,
    
    -- 2. Sequencing & Time
    st.stop_sequence AS stop_sequence,
    st.arrival_time AS arrival_time,
    st.departure_time AS departure_time,

    -- 3. Geometry (Float64 from Stops table)
    ts.stop_lat AS stop_lat,
    ts.stop_lon AS stop_lon,

    -- 4. Trip Attributes
    cast(tst.service_id as LowCardinality(String)) AS service_id,
    tst.trip_headsign AS trip_headsign,
    tst.trip_short_name AS trip_short_name,
    tst.direction_id AS direction_id, -- Nullable(UInt8)
    tst.block_id AS block_id,
    tst.shape_id AS shape_id,
    tst.wheelchair_accessible AS wheelchair_accessible, -- Nullable(UInt8)
    tst.bikes_allowed AS bikes_allowed, -- Nullable(UInt8)

    -- 5. Route Attributes
    ro.route_id AS route_id,
    ro.route_short_name AS route_short_name,
    ro.route_long_name AS route_long_name,
    ro.route_desc AS route_desc,
    ro.route_type AS route_type, 
    ro.route_url AS route_url,
    cast(ro.route_color as LowCardinality(String)) AS route_color,
    cast(ro.route_text_color as LowCardinality(String)) AS route_text_color,
    ro.route_sort_order AS route_sort_order,
    ro.continuous_pickup AS route_continuous_pickup,
    ro.continuous_drop_off AS route_continuous_drop_off,

    -- 6. Stop Attributes
    st.pickup_type AS pickup_type,
    st.drop_off_type AS drop_off_type,
    st.stop_headsign AS stop_headsign,
    st.continuous_pickup AS continuous_pickup,
    st.continuous_drop_off AS continuous_drop_off,
    st.shape_dist_traveled AS shape_dist_traveled,
    st.timepoint AS timepoint,
    st.local_zone_id AS local_zone_id,
    
    ts.stop_code AS stop_code,
    ts.stop_name AS stop_name,
    ts.stop_desc AS stop_desc,
    ts.zone_id AS zone_id,
    ts.stop_url AS stop_url,
    ts.location_type AS location_type,
    ts.parent_station AS parent_station,
    ts.stop_timezone AS stop_timezone,
    ts.wheelchair_boarding AS wheelchair_boarding,
    ts.level_id AS level_id,
    ts.platform_code AS platform_code

FROM transitous_everything_20260218_stop_times st

-- JOIN 1: Filter to only the best day's trips
INNER JOIN transitous_everything_20260218_valid_trips_lookup vt
    ON st.source = vt.source 
    AND st.trip_id = vt.trip_id

-- JOIN 2: Attach the UUIDs
INNER JOIN transitous_everything_20260218_trip_route_uuids tru 
    ON st.source = tru.source 
    AND st.trip_id = tru.trip_id

-- JOIN 3: Attach Trip Info
INNER JOIN transitous_everything_20260218_trips tst 
    ON st.source = tst.source 
    AND st.trip_id = tst.trip_id

-- JOIN 4: Attach Route Info
INNER JOIN transitous_everything_20260218_routes ro 
    ON tst.source = ro.source 
    AND tst.route_id = ro.route_id

-- JOIN 5: Attach Stop Info (Lat/Lon)
INNER JOIN transitous_everything_20260218_stops ts 
    ON st.source = ts.source 
    AND st.stop_id = ts.stop_id;


-- i don't think we need anything below here any more

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
h3kRing(geoToH3(stop_lat, stop_lon, 9), 4) ring
from transitous_everything_20260218_stop_times_one_day_sane
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
            select geoToH3(stop_lat, stop_lon, res_trans) h3, arrayJoin(arrayMap(x->(x, h3HexRing(h3, x)), arrayMap(x->toUInt16(x), range(0,max_dist+1)))) t, argMax(pop_per_minute, greatest(0, (60 - wait_time - 5) * pop_per_minute)) pop_per_minute_t, argMax(wait_time, greatest(0, (60 - wait_time - 5) * pop_per_minute)) wait_time_t from (
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
    --left anti join (select geoToH3(lat, lon, res_trans) h3 from uprn_os) uo on h3 = uo.h3 -- exclude places where zero people live
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
