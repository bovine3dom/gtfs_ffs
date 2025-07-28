--- making actual tables
select *, company from file('gtfs_hive/company=*/agency.txt', 'CSVWithNames') limit 1000 settings use_hive_partitioning = 1;
DESCRIBE TABLE file('gtfs_hive/company=*/agency.txt', 'CSVWithNames') settings use_hive_partitioning = 1;

DROP TABLE IF EXISTS gtfs_agency;
CREATE TABLE gtfs_agency
ENGINE = MergeTree
ORDER BY (company, agency_id, agency_name)
AS SELECT company, assumeNotNull(agency_id) agency_id, assumeNotNull(agency_name) agency_name, agency_url, agency_timezone, lower(agency_lang) agency_lang
FROM file('gtfs_hive/company=*/agency.txt', 'CSVWithNames')
SETTINGS use_hive_partitioning = 1;

DESCRIBE TABLE file('gtfs_hive/company=*/stops.txt', 'CSVWithNames') settings use_hive_partitioning = 1;

DROP TABLE IF EXISTS gtfs_stops;
CREATE TABLE gtfs_stops
ENGINE = MergeTree
ORDER BY (company, stop_lat, stop_lon, stop_id)
AS SELECT company, assumeNotNull(stop_lat) stop_lat, assumeNotNull(stop_lon) stop_lon, assumeNotNull(stop_id) stop_id, stop_name, stop_desc, zone_id, stop_url, location_type, parent_station
FROM file('gtfs_hive/company=*/stops.txt', 'CSVWithNames')
SETTINGS use_hive_partitioning = 1;



DESCRIBE TABLE file('gtfs_hive/company=*/routes.txt', 'CSVWithNames') settings use_hive_partitioning = 1;

-- route type docs here: https://developers.google.com/transit/gtfs/reference/extended-route-types ... basically < 200 is a train, 400 <= x < 500 is a metro, 900 <= 1000 is a tram, 1400 is a funicular
DROP TABLE IF EXISTS gtfs_routes;
CREATE TABLE gtfs_routes
ENGINE = MergeTree
ORDER BY (company, route_type, agency_id, route_id)
AS SELECT company, assumeNotNull(agency_id) agency_id, assumeNotNull(route_type) route_type, assumeNotNull(route_id) route_id, route_short_name, route_long_name, route_desc, route_url, route_color, route_text_color
FROM file('gtfs_hive/company=*/routes.txt', 'CSVWithNames')
SETTINGS use_hive_partitioning = 1;




DESCRIBE TABLE file('gtfs_hive/company=*/trips.txt', 'CSVWithNames') settings use_hive_partitioning = 1;
DROP TABLE IF EXISTS gtfs_trips;
CREATE TABLE gtfs_trips
ENGINE = MergeTree
ORDER BY (company, route_id, service_id, trip_id)
AS SELECT company, assumeNotNull(route_id) route_id, assumeNotNull(service_id) service_id, assumeNotNull(trip_id) trip_id, trip_headsign, direction_id, block_id, shape_id
FROM file('gtfs_hive/company=*/trips.txt', 'CSVWithNames', 'route_id String, service_id String, trip_id String, trip_headsign String, direction_id String, block_id String, shape_id String')
SETTINGS use_hive_partitioning = 1; -- fails to sniff swiss column types :(
DESCRIBE TABLE file('gtfs_hive/company=*/routes.txt', 'CSVWithNames') settings use_hive_partitioning = 1;


DESCRIBE TABLE file('gtfs_hive/company=*/stop_times.txt', 'CSVWithNames') settings use_hive_partitioning = 1;
DROP TABLE IF EXISTS gtfs_stop_times;
CREATE TABLE gtfs_stop_times
ENGINE = MergeTree
ORDER BY (company, trip_id, stop_id, departure_time, arrival_time)
AS SELECT company, assumeNotNull(trip_id) trip_id, assumeNotNull(stop_id) stop_id, assumeNotNull(parseDateTimeBestEffortOrNull(departure_time)) departure_time, assumeNotNull(parseDateTimeBestEffortOrNull(arrival_time)) arrival_time, stop_sequence, stop_headsign, pickup_type, drop_off_type, shape_dist_traveled
FROM file('gtfs_hive/company=*/stop_times.txt', 'CSVWithNames')
SETTINGS use_hive_partitioning = 1;

DESCRIBE TABLE file('gtfs_hive/company=*/calendar.txt', 'CSVWithNames', 'service_id String, monday String, tuesday String, wednesday String, thursday String, friday String, saturday String, sunday String, start_date String, end_date String') settings use_hive_partitioning = 1;
DROP TABLE IF EXISTS gtfs_calendar;
CREATE TABLE gtfs_calendar
ENGINE = MergeTree
ORDER BY (company, service_id, start_date, end_date, monday, tuesday, wednesday, thursday, friday, saturday, sunday)
AS SELECT company, service_id, assumeNotNull(parseDateTimeBestEffortOrNull(start_date)) start_date, assumeNotNull(parseDateTimeBestEffortOrNull(end_date)) end_date, toBool(monday) monday, toBool(tuesday) tuesday, toBool(wednesday) wednesday, toBool(thursday) thursday, toBool(friday) friday, toBool(saturday) saturday, toBool(sunday) sunday
FROM file('gtfs_hive/company=*/calendar.txt', 'CSVWithNames', 'service_id String, monday String, tuesday String, wednesday String, thursday String, friday String, saturday String, sunday String, start_date String, end_date String')
SETTINGS use_hive_partitioning = 1;



DESCRIBE TABLE file('gtfs_hive/company=*/calendar_dates.txt', 'CSVWithNames', 'service_id String, date String, exception_type UInt8') settings use_hive_partitioning = 1;
DROP TABLE IF EXISTS gtfs_calendar_dates;
CREATE TABLE gtfs_calendar_dates
ENGINE = MergeTree
ORDER BY (company, service_id, date, exception_type)
AS SELECT company, service_id, assumeNotNull(parseDateTimeBestEffortOrNull(date)) date, exception_type
FROM file('gtfs_hive/company=*/calendar_dates.txt', 'CSVWithNames', 'service_id String, date String, exception_type UInt8')
SETTINGS use_hive_partitioning = 1;


-- todo: import transfers etc following https://developers.google.com/transit/gtfs/examples/gtfs-feed and https://gtfs.org/documentation/schedule/reference/#calendar_datestxt

-- transitious
select *, source from file('transitous/source=it_*/agency.txt', 'CSVWithNames') limit 1000 settings use_hive_partitioning = 1;
DESCRIBE TABLE file('gtfs_hive/company=*/agency.txt', 'CSVWithNames') settings use_hive_partitioning = 1;

-- need to be more explicit about the types :(
SELECT source, assumeNotNull(agency_id) agency_id, toInt16OrNull(route_type) route_type, assumeNotNull(route_id) route_id, route_short_name, route_long_name
FROM file('transitous/source=it_*/routes.txt', 'CSVWithNames', 'route_long_name String, route_short_name String ,agency_id String ,route_type String,route_id String')
WHERE true 
and route_type < 200
--and (route_type between 1100 and 1199) -- airlines but only sardinian
and route_long_name ilike '%afragola%' -- high speed naples station. only get local trains
LIMIT 100
SETTINGS use_hive_partitioning = 1;


-- actually making the timetable
DROP TABLE IF EXISTS transitous_everything_routes;
DROP TABLE IF EXISTS transitous_everything_trips;
DROP TABLE IF EXISTS transitous_everything_stop_times;
DROP TABLE IF EXISTS transitous_everything_calendar;
DROP TABLE IF EXISTS transitous_everything_calendar_dates;
DROP TABLE IF EXISTS transitous_everything_stops;
DROP TABLE IF EXISTS transitous_everything_agency;
-- oh it looks like there's also un-extended route type
-- https://ipeagit.github.io/gtfstools/reference/filter_by_route_type.html
-- 0 = tram, 1 = metro, 2 = rail, 3 = bus, 4 = ferry, 5 = cable car, 6 = gondola, 7 = funicular, 11 = trolleybus, 12 = monorail
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
WHERE true
--and (route_type between 1100 and 1199) -- airlines but only sardinian
--and (route_type between 100 and 199) -- 'proper' trains
--and (route_type between 400 and 499) -- 'urban' trains
--and ((route_type between 900 and 999)) -- trams
--and ((route_type = 2) or (route_type between 100 and 199))
--and (route_type between 1400 and 1499) -- funiculars
--and source ilike 'fr%'
--and route_desc ilike '%%'
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
--left semi join transitous_everything_routes tr on tt.route_id = tr.route_id and tt.source = tr.source
WHERE true
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
--left semi join transitous_everything_trips tt on st.trip_id = tt.trip_id and st.source = tt.source
WHERE true
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
--left semi join transitous_everything_stop_times st on ts.stop_id = st.stop_id and ts.source = st.source
WHERE true
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
--left semi join transitous_everything_trips tt on tc.service_id = tt.service_id and tc.source = tt.source
WHERE true
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
    service_id String,
    date String,
    exception_type String
') tcd
--left semi join transitous_everything_trips tt on tcd.service_id = tt.service_id and tcd.source = tt.source
WHERE true
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
    agency_id String,
    agency_name String,
    agency_url String,
    agency_timezone String,
    agency_email String,
    agency_fare_url String,
    agency_lang String,
    agency_phone String
') ta
--left semi join transitous_everything_routes tr on tr.agency_id = ta.agency_id and ta.source = tr.source
WHERE true
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


-- debugging

-- nice has too many buses
-- i don't believe that these buses actually stop here
-- did we mess up a join somewhere? yes, lol, bigly
select departure_time, stop_id, stop_name, stop_lon, stop_lat, source, trip_id, route_id, trip_headsign from transitous_everything_stop_times_one_day
where true
and stop_name = 'Congrès / Promenade'
and source like 'fr_%'
--and geoToH3(stop_lon, stop_lat, 10) in h3kRing(reinterpretAsUInt64(reverse(unhex('8a3969a08c8ffff'))), 10)
and departure_time > '2025-01-01 09:00:00'
order by departure_time
limit 20

-- eugh somehow the join has just joined on stop_id and not (stop_id AND source_id)
select st.source, ts.stop_id, departure_time, stop_name from transitous_everything_stop_times_one_day st
left join transitous_everything_stops ts on st.stop_id = ts.stop_id and st.source = ts.source
where trip_id = '1582485189'
and source = 'fr_offre-de-transport-de-montpellier-mediterranee-metropole-tam-gtfs.gtfs'
order by departure_time

-- next job: check for REAL duplicates lol
select stop_id, stop_name, source from transitous_everything_stops
where true
--and stop_name = 'Congrès / Promenade'
and source like 'fr_%'
and geoToH3(stop_lon, stop_lat, 10) in h3kRing(reinterpretAsUInt64(reverse(unhex('8a3969a08c8ffff'))), 3)
limit 20

select * from transitous_everything_stop_times
where true
and source = 'fr_export-quotidien-au-format-gtfs-du-reseau-de-transport-lignes-d-azur.gtfs'
and stop_id = '282'
limit 10

select * from transitous_everything_trips
where true
and source = 'fr_export-quotidien-au-format-gtfs-du-reseau-de-transport-lignes-d-azur.gtfs'
and trip_id = '6001229-08_R_99_0801_07:28-PVS2024-25-08-Semaine-31'
limit 10

select * from transitous_everything_calendar
where service_id = 'JANV2025-15-Semaine-39-15'
and source = 'fr_export-quotidien-au-format-gtfs-du-reseau-de-transport-lignes-d-azur.gtfs'

--- argh some have stupidly short horizons
--- need to find day when most services run
SETTINGS allow_experimental_join_condition = 1;


    select ca.service_id service_id, ca.source source from transitous_everything_calendar as ca
    left anti join transitous_everything_calendar_dates as cd_remove on
    ca.service_id = cd_remove.service_id and ca.source = cd_remove.source
    and cd_remove.date = '2025-05-13' and cd_remove.exception_type = 2
    where true
    and ca.start_date <= '2025-05-13' and ca.end_date >= '2025-05-13'
    and ca.tuesday
    union distinct
    SELECT cd.service_id service_id, cd.source source
    FROM transitous_everything_calendar_dates AS cd
    WHERE cd.date = '2025-05-13'
    AND cd.exception_type = 1
--     ┌──────departure_time─┬─stop_name───────────┬─stop_lon─┬──stop_lat─┬─source─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─trip_id─────────────────────────────────┬─route_id─┬─trip_headsign─────────────────┐
--  1. │ 2025-01-01 09:02:00 │ Congrès / Promenade │ 7.262923 │ 43.694717 │ fr_arrets-horaires-et-parcours-theoriques-du-reseau-stan-gtfs.gtfs                                                             │ S1-3400567-25H04-_PS_BUS-Semaine-01     │ 2        │ LAXOU SAPINIERE               │
--  2. │ 2025-01-01 09:02:00 │ Congrès / Promenade │ 7.262923 │ 43.694717 │ fr_transport-du-reseau-urbain-agglobus-et-de-transports-scolaires-gtfs.gtfs                                                    │ 926                                     │ 20       │ AGGLOBUS                      │
--  3. │ 2025-01-01 09:02:00 │ Congrès / Promenade │ 7.262923 │ 43.694717 │ fr_horaires-theoriques-du-reseau-stas.gtfs                                                                                     │ 795                                     │ 01       │ Bellevue                      │
--  4. │ 2025-01-01 09:04:00 │ Congrès / Promenade │ 7.262923 │ 43.694717 │ fr_offre-de-transport-de-montpellier-mediterranee-metropole-tam-gtfs.gtfs                                                      │ 1582485219                              │ 11       │ MONTPELLIER - Tournezy        │
--  5. │ 2025-01-01 09:06:00 │ Congrès / Promenade │ 7.264292 │  43.69498 │ fr_offres-de-services-bus-tram-et-scolaire-au-format-gtfs-gtfs-rt-siri-lite.gtfs                                               │ 44911831-2025_HIVER-M37_A00-L-Ma-J-V-17 │ 37       │ BORDEAUX Parc des Expositions │
--  6. │ 2025-01-01 09:06:16 │ Congrès / Promenade │ 7.262923 │ 43.694717 │ fr_offres-de-services-bus-tram-et-scolaire-au-format-gtfs-gtfs-rt-siri-lite.gtfs                                               │ 44219823-2025_HIVER-M74_A00-Semaine-08  │ 74       │ GRADIGNAN Stade Ornon         │
--  7. │ 2025-01-01 09:07:00 │ Congrès / Promenade │ 7.262923 │ 43.694717 │ fr_arrets-horaires-et-parcours-theoriques-du-reseau-stan-gtfs.gtfs                                                             │ S1-3400568-25H04-_PS_BUS-Semaine-01     │ 2        │ LAXOU SAPINIERE               │
--  8. │ 2025-01-01 09:09:00 │ Congrès / Promenade │ 7.262923 │ 43.694717 │ fr_fr-200052264-t0014-0000-1.gtfs                                                                                              │ 5804127-S_2024-internet-Semaine-11      │ 15       │ SAINTE-BARBE                  │
--  9. │ 2025-01-01 09:09:56 │ Congrès / Promenade │ 7.262923 │ 43.694717 │ fr_offre-de-transport-solea-et-tram-train-en-format-gtfs-1.gtfs                                                                │ 5804127-S_2024-internet-Semaine-11      │ 15-868   │ STE BARBE                     │
-- 10. │ 2025-01-01 09:10:00 │ Congrès / Promenade │ 7.262923 │ 43.694717 │ fr_reseau-urbain-surf.gtfs                                                                                                     │ 1-712376334                             │ 42       │ Lécousse Pilais               │
-- 11. │ 2025-01-01 09:11:00 │ Congrès / Promenade │ 7.262923 │ 43.694717 │ fr_horaires-theoriques-et-temps-reels-du-reseau-de-transports-lagglo-en-bus-communaute-dagglomeration-gap-tallard-durance.gtfs │ TRIP_5_ttbl_2_1_16                      │ 2        │                               │
-- 12. │ 2025-01-01 09:12:00 │ Congrès / Promenade │ 7.262923 │ 43.694717 │ fr_donnees-gtfs-du-reseau-de-transport-public-cara-bus.gtfs                                                                    │ 556                                     │ 261      │ ST PALAIS SUR MER -  Vallet   │
-- 13. │ 2025-01-01 09:12:00 │ Congrès / Promenade │ 7.262923 │ 43.694717 │ fr_fr-200052264-t0040-0000-1.gtfs                                                                                              │ 4449164-24-25-SCO_NS17-L-Ma-J-V-01      │ S22      │ ESPLANADE (FRESQUE)           │
-- 14. │ 2025-01-01 09:12:00 │ Congrès / Promenade │ 7.262923 │ 43.694717 │ fr_horaires-theoriques-du-reseau-stas.gtfs                                                                                     │ 796                                     │ 01       │ Bellevue                      │
-- 15. │ 2025-01-01 09:14:00 │ Congrès / Promenade │ 7.262923 │ 43.694717 │ fr_arrets-horaires-et-parcours-theoriques-du-reseau-stan-gtfs.gtfs                                                             │ S1-3400569-25H04-_PS_BUS-Semaine-01     │ 2        │ LAXOU SAPINIERE               │
-- 16. │ 2025-01-01 09:15:00 │ Congrès / Promenade │ 7.262923 │ 43.694717 │ fr_horaires-theoriques-du-reseau-tag.gtfs                                                                                      │ 29178756                                │ 42       │ Meylan, La Détourbe           │
-- 17. │ 2025-01-01 09:15:59 │ Congrès / Promenade │ 7.262923 │ 43.694717 │ fr_ametis.gtfs                                                                                                                 │ 15-L-7-B-085200                         │ L        │ CHU A. PICARDIE               │
-- 18. │ 2025-01-01 09:17:00 │ Congrès / Promenade │ 7.262923 │ 43.694717 │ fr_transport-du-reseau-urbain-agglobus-et-de-transports-scolaires-gtfs.gtfs                                                    │ 912                                     │ 3        │ AGGLOBUS                      │
-- 19. │ 2025-01-01 09:18:00 │ Congrès / Promenade │ 7.262923 │ 43.694717 │ fr_transport-du-reseau-urbain-agglobus-et-de-transports-scolaires-gtfs.gtfs                                                    │ 746                                     │ 2        │                               │
-- 20. │ 2025-01-01 09:19:00 │ Congrès / Promenade │ 7.262923 │ 43.694717 │ fr_offre-de-transport-de-montpellier-mediterranee-metropole-tam-gtfs.gtfs                                                      │ 1582485189                              │ 11       │ MONTPELLIER - Tournezy        │
--     └─────────────────────┴─────────────────────┴──────────┴───────────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴─────────────────────────────────────────┴──────────┴───────────────────────────────┘

-- is it lite if it's bigger than all the others
CREATE TABLE transitous_stops_lite
ENGINE MergeTree
ORDER BY (source, h3_15, stop_name)
AS
SELECT
    toLowCardinality(assumeNotNull(ts.source)) AS source,
    ts.stop_id stop_id,
    --stop_code,
    stop_name,
    stop_desc,
    toFloat64OrZero(stop_lat) AS stop_lat,
    toFloat64OrZero(stop_lon) AS stop_lon,
    geoToH3(stop_lon, stop_lat, 15) AS h3_15
    --zone_id,
    --stop_url,
    --toUInt8OrNull(location_type) AS location_type,
    --parent_station,
    --stop_timezone,
    --toUInt8OrNull(wheelchair_boarding) AS wheelchair_boarding,
    --level_id,
    --platform_code
FROM file('transitous/source=*/stops.txt', 'CSVWithNames', '
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
WHERE true
SETTINGS use_hive_partitioning = 1;


-- router innit
-- current limitation: can only work with one source at a time
-- need source/stop unification :(

-- to get walk time at end: at resolution 10, at 2.5km/h straight line speed walk 0.6 tiles per minute
-- eugh it looks like swiss segregates stop ids for e.g. bus vs train
-- so try meggen instead

select * from transitous_everything_stops where source like 'ch_%' and stop_name ilike 'meggen%zentrum'
select count() from transitous_everything_stop_times_one_day where source like 'ch_%' and stop_id = '8505018:0:1'
select distinct stop_name, stop_uuid from transitous_everything_stop_times_one_day_even_saner where source like 'lu_%' and stop_name ilike 'luxembourg%gare%' limit 10


-- isochrone sketch
WITH RECURSIVE
    --'8575785' AS start_node, -- morbio
    '8505018:0:1' as start_node, -- meggen zentrum
    4 as max_travel_time,
    20 as max_walk_time,
    parseDateTimeBestEffort('2025-01-01 09:00:00') AS journey_start_time,
    journey_start_time + INTERVAL max_travel_time HOUR AS journey_end_limit_time,
    'ch_opentransportdataswiss25.gtfs' as target_source,
    reachable_destinations AS (
        -- Anchor Member: First hops from the starting source_id
        -- These are direct connections from the input_source_id
        SELECT
            e.next_stop current_node_id,
            e.next_arrival current_arrival_time,
            [start_node, e.next_stop] path_nodes,
            1 hop_count
        FROM transitous_everything_edgelist e
        WHERE e.stop_id = start_node
          AND e.departure_time >= journey_start_time
          AND e.next_arrival <= journey_end_limit_time
          AND source like 'ch_%'

        UNION ALL

        SELECT
            e.next_stop,
            e.next_arrival,
            arrayPushBack(rd.path_nodes, e.next_stop) AS path_nodes,
            rd.hop_count + 1
        FROM reachable_destinations rd
        JOIN transitous_everything_edgelist e ON rd.current_node_id = e.stop_id
        -- To access journey_end_limit_time
        WHERE e.departure_time >= rd.current_arrival_time
          AND e.next_arrival <= journey_end_limit_time
          AND NOT has(rd.path_nodes, e.next_stop)
          AND rd.hop_count < 1000
          AND source like 'ch_%'
    )
select arrayJoin(h3) h3, min(travel_time + (h3Distance(h3, stop_h3) / 0.6)) value from (
SELECT current_node_id, dateDiff('minute', journey_start_time, min(current_arrival_time)) travel_time, max_travel_time*60 - travel_time remaining_travel_time,
toUInt16(floor(least(remaining_travel_time, max_walk_time) * 0.6)) res_10_radius, any(st.stop_lat) lat, any(st.stop_lon) lon, assumeNotNull(geoToH3(lon, lat, 10)) stop_h3,
--[geoToH3(lon, lat, 9)] h3
h3kRing(stop_h3, assumeNotNull(res_10_radius)) h3
--SELECT current_node_id, min(current_arrival_time)--, any(st.stop_lat), any(st.stop_lon)
FROM reachable_destinations r
-- Optionally, you can filter out the start_node itself if it becomes reachable via a loop
LEFT ANY JOIN transitous_everything_stops st on st.stop_id = r.current_node_id
WHERE current_node_id != start_node
AND source = target_source
GROUP BY current_node_id
ORDER BY current_node_id
)
group by h3
-- ok it's the source= that's the slow bit
-- todo:
-- stop unification, even within sources often stops count as different stops esp e.g. bus stop at a railway station, merge stops within 5 minute radius
-- if trip id changes add minimum connection time
-- backwards option to work in reverse
-- think about how to aggregate over a day to sum accessible population
-- add a maximum walk time of e.g. 20 minutes



--- trying to construct an edgelist
--- this is done but now what
--- i guess we could add fake walking departures for less than 30 minute walks to walk between stop_ids for different sources?
--- or maybe better to precisely merge any stops with exactly the same lat/lon
-- SAD, symmetricDifference is too new
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
-- where source like 'be_%'
-- or source like 'ch_%'
-- or source like 'lu_%'
-- or source like 'fr_%'
-- or source like 'gb_%'
-- or source like 'it_%'
-- or source like 'de_%'
-- or source like 'nl_%'
) st
left join population_h3 pop on newh3 = pop.h3
where next_stop != stop_id
and res = 9
)
where travel_time > 0
group by all;
-- takes aaaaages but it does seem to work (4200 seconds)
exchange tables transitous_everything_edgelist and transitous_everything_edgelist2;


-- first go at goodness
select geoToH3(stop_lon, stop_lat, 9) h3, sum(pop_per_minute) from transitous_everything_edgelist group by h3
limit 10

-- second go: trip goodness, then median trip goodness from stop

-- third go: some crazy network stuff with edges weighted by pop per minute

-- 4 ring at res 9 is approx a 10 minute walk
-- can i speed query up by swapping fake grouping keys for any()?
-- real keys are only source, trip_id, stop_id
-- all the top are always metros
-- but maybe that's fine?
-- 192 seconds time to beat. barely any change

--- dumb left join on population was very slow
--- maybe better to do an arrayJoin and then regroup?

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

-- drop table if exists transitous_pop_within_60_2;
-- drop table if exists transitous_everything_edgelist2;


--- finding longest trip distances, 3,700 seconds

select 
distinct
max(geoDistance(l.stop_lon, l.stop_lat, r.stop_lon, r.stop_lat)) dist,
argMax(l.stop_name, geoDistance(l.stop_lon, l.stop_lat, r.stop_lon, r.stop_lat)) l_id,
argMax(r.stop_name, geoDistance(l.stop_lon, l.stop_lat, r.stop_lon, r.stop_lat)) r_id,
any(l.source)
from transitous_everything_stop_times_one_day_sane l inner join transitous_everything_stop_times_one_day_sane r on l.sane_route_id = r.sane_route_id
where true
-- and ((route_type = 2) or (route_type between 100 and 199)) -- trains
-- and ((route_type = 0) or (route_type between 900 and 999)) -- trams. eugh, looks like lots of fake zeroes have crept in to the data?
--and ((route_type = 7) or (route_type between 1400 and 1499)) -- trams. eugh, looks like lots of fake zeroes have crept in to the data?
and ((route_type = 11) or (route_type between 800 and 899)) -- trolley buses
--and source like 'fr%'
--and source like 'lu%'
group by sane_route_id order by dist desc limit 500

-- mostly buses :(
--       ┌───────────────dist─┬─l_id───────────────────────────────────────┬─r_id─────────────────────────────────────────┬─any(source)──────────────────────────────┐
--    1. │ 3357888.1923631025 │ Toronto                                    │ Vancouver                                    │ ca_Viarail.gtfs                          │
--    2. │  2972639.332541529 │ Chicago Union Station                      │ Richmond                                     │ us_Amtrak.gtfs                           │
--    3. │ 2822698.5204869467 │ Chicago Union Station                      │ Portland                                     │ us_Amtrak.gtfs                           │
--    4. │ 2802405.9537520306 │ Chicago Union Station                      │ Los Angeles                                  │ us_Amtrak.gtfs                           │
--    5. │ 2792176.3736140956 │ Chicago Union Station                      │ Edmonds                                      │ us_Amtrak.gtfs                           │
--    6. │  2685566.452083582 │ Los Angeles                                │ New Orleans Union Passenger Terminal         │ us_Amtrak.gtfs                           │
--    7. │ 2550990.2195867402 │ St Louis Bus Station                       │ Los Angeles Union Station                    │ us_flixbus.gtfs                          │
--    8. │  2212418.650000143 │ Houston Bus Station (Greyhound)            │ Los Angeles Union Station                    │ us_flixbus.gtfs                          │
--    9. │ 2153987.8349294025 │ Rennes (Central bus station)               │ Bucharest (bus station Militari)             │ eu_flixbus.gtfs                          │
--   10. │ 2121371.7821912547 │ Denver Union Station Bus Concourse         │ Pittsburgh Intermodal Station                │ us_flixbus.gtfs                          │
--   11. │  2092791.709453438 │ Bordeaux (Saint-Jean - Paludate)           │ Bucharest (bus station Militari)             │ eu_flixbus.gtfs                          │
--   12. │ 2061499.8416898178 │ Aeroporto di Olbia (OLB)                   │ Aeroporto di Riga (RIX)                      │ it_Sardegna-Planes-Olbia-Airport.gtfs    │
--   13. │ 1990290.0395629392 │ Dallas Bus Station                         │ Los Angeles Union Station                    │ us_flixbus.gtfs                          │
--   14. │ 1925930.6339935088 │ Aeroporto di Edinburgh Airport             │ Aeroporto di Olbia (OLB)                     │ it_Sardegna-Planes-Olbia-Airport.gtfs    │
--   15. │ 1904595.7713257372 │ Chicago Union Station                      │ Miami Amtrak Station                         │ us_Amtrak.gtfs                           │
--   16. │ 1884317.3620316128 │ New Orleans Union Passenger Terminal       │ Ny Moynihan Train Hall At Penn Station       │ us_Amtrak.gtfs                           │
--   17. │ 1836104.4234587962 │ Brussels-North train station               │ Kyiv (Central Bus Station)                   │ eu_flixbus.gtfs                          │
--   18. │  1824656.754048666 │ Aeroporto di Dublino/Dublin (DUB)          │ Aeroporto di Olbia (OLB)                     │ it_Sardegna-Planes-Olbia-Airport.gtfs    │
--   19. │ 1752528.8703451534 │ Miami Amtrak Station                       │ Ny Moynihan Train Hall At Penn Station       │ us_Amtrak.gtfs                           │
--   20. │  1709979.005716243 │ Lisbon - Sete Rios                         │ Brussels - Midi Train Station                │ eu_blablacar-bus.gtfs                    │
--   21. │ 1695416.7861805079 │ Chicago Union Station                      │ San Antonio Amtrak Station                   │ us_Amtrak.gtfs                           │
--   22. │  1695040.782509659 │ Paris (Bercy Seine)                        │ Vilnius, Bus Station                         │ eu_flixbus.gtfs                          │
--   23. │ 1691761.2048011709 │ Chernihiv (Chernihiv-1 Bus Station)        │ Düsseldorf central bus station               │ eu_flixbus.gtfs                          │
--   24. │ 1673546.4050588047 │ Lisbon (Oriente)                           │ Milan (Lampugnano bus station)               │ eu_flixbus.gtfs                          │
--   25. │ 1648055.9244573077 │ Aeroporto di Manchester (MAN)              │ Aeroporto di Olbia (OLB)                     │ it_Sardegna-Planes-Olbia-Airport.gtfs    │
--   26. │ 1599638.0878984702 │ Aeroporto di Cagliari (CAG) (ELMAS)        │ Aeroporto di Amburgo/Hamburg (HAM)           │ it_Sardegna-Planes-Cagliari-Airport.gtfs │
--   27. │ 1585191.7414017746 │ Etretat                                    │ Lublin, Bus Station Lublin                   │ eu_flixbus.gtfs                          │
--   28. │ 1582948.4952748974 │ Aeroporto di Cagliari (CAG) (ELMAS)        │ Aeroporto di Poznan-Lawica (POZ)             │ it_Sardegna-Planes-Cagliari-Airport.gtfs │
--   29. │ 1575248.7626265197 │ Kyiv (Central Bus Station)                 │ Mannheim central bus station                 │ eu_flixbus.gtfs                          │
--   30. │ 1558653.2647846458 │ Marseille (Saint-Antoine)                  │ Lublin, Bus Station Lublin                   │ eu_flixbus.gtfs                          │
--   31. │ 1554301.2628097192 │ Vinnytsia (Central Bus Station)            │ Duisburg central train station               │ eu_flixbus.gtfs                          │
--   32. │  1542986.630649843 │ Los Angeles                                │ Seattle                                      │ us_Amtrak.gtfs                           │
--   33. │ 1542227.4048825768 │ Miami Intermodal Center                    │ Houston Bus Station (Greyhound)              │ us_flixbus.gtfs                          │
--   34. │ 1518003.3035098545 │ Kyiv (Central Bus Station)                 │ Bremen Fernbusterminal                       │ eu_flixbus.gtfs                          │
--   35. │ 1511608.4450830135 │ New York City Chinatown (28 Allen St)      │ Orlando Bus Station                          │ us_flixbus.gtfs                          │
--   36. │ 1500108.6798641842 │ Aeroporto di Bristol (BRS)                 │ Aeroporto di Olbia (OLB)                     │ it_Sardegna-Planes-Olbia-Airport.gtfs    │
--   37. │ 1494445.2041441952 │ Chelm, Armii Krajowej Street/KFC 05 and 06 │ Lyon (Perrache Bus Station)                  │ eu_flixbus.gtfs                          │
--   38. │ 1488925.8431345234 │ Aeroporto di Amsterdam (AMS)               │ Aeroporto di Cagliari (CAG) (ELMAS)          │ it_Sardegna-Planes-Cagliari-Airport.gtfs │
--   39. │  1473083.308376557 │ Aeroporto di Cagliari (CAG) (ELMAS)        │ Aeroporto di Cracovia/Krakow (KRK)           │ it_Sardegna-Planes-Cagliari-Airport.gtfs │
--   40. │ 1456907.5284599792 │ Gdansk, Bus Station                        │ Rome Tiburtina Bus station                   │ eu_flixbus.gtfs                          │
--   41. │  1456877.058327583 │ Lisbon - Oriente                           │ Champigny-sur-Marne                          │ eu_blablacar-bus.gtfs                    │
--   42. │  1456622.169973206 │ Paris (Bercy Seine)                        │ Lisbon Sete Rios (Jardim Zoológico) (IC)     │ eu_flixbus.gtfs                          │
--   43. │ 1453094.1569803595 │ Kyiv (Central Bus Station)                 │ Hanover central bus station                  │ eu_flixbus.gtfs                          │
--   44. │ 1450884.5268676933 │ Lisbon - Oriente                           │ Paris - Bercy-Seine Bus Station              │ eu_blablacar-bus.gtfs                    │
--   45. │ 1450824.0193368862 │ Lisbon (Oriente)                           │ Paris (Bercy Seine)                          │ eu_flixbus.gtfs                          │
--   46. │  1450216.173599012 │ Frankfurt - Main Bus Station               │ Madrid - South Station                       │ eu_blablacar-bus.gtfs                    │
--   47. │ 1446980.5480276537 │ London Victoria Coach Station              │ Warsaw, Bus Station West                     │ eu_flixbus.gtfs                          │
--   48. │ 1446979.4333483735 │ Victoria Coach Station                     │ Warsaw Bus Station West                      │ gb_bus-dft.gtfs                          │
--   49. │ 1437533.9680038283 │ Aeroporto di Londra/London Luton (LTN)     │ Aeroporto di Olbia (OLB)                     │ it_Sardegna-Planes-Olbia-Airport.gtfs    │
--   50. │ 1437290.4561789865 │ Niš AS                                     │ Hamburg central bus station                  │ eu_flixbus.gtfs                          │
--   51. │ 1435078.5120327997 │ Poltava (Bus Station-1)                    │ Prague (Central Bus Station Florenc)         │ eu_flixbus.gtfs                          │
--   52. │ 1420392.7891409232 │ Rzeszow, Bus Station                       │ Paris (Bercy Seine)                          │ eu_flixbus.gtfs                          │
--   53. │ 1416292.3357907054 │ Krakow, MDA Bus Station                    │ Montpellier (Sabines)                        │ eu_flixbus.gtfs                          │
--   54. │ 1414055.5750815948 │ Aeroporto di Olbia (OLB)                   │ Aeroporto di Londra/London Stansted (STN)    │ it_Sardegna-Planes-Olbia-Airport.gtfs    │
--   55. │  1409276.228086753 │ Bergen Nøstet kystrutekai                  │ Hammerfest kystrutekai                       │ no_Entur.gtfs                            │
--   56. │  1409276.228086753 │ Hammerfest kystrutekai                     │ Bergen Nøstet kystrutekai                    │ no_Entur.gtfs                            │
--   57. │ 1405292.2089089751 │ St Louis Bus Station                       │ New York Port Authority                      │ us_flixbus.gtfs                          │
--   58. │ 1404622.3041376083 │ Aeroporto di Londra/London Heathrow (LHR)  │ Aeroporto di Olbia (OLB)                     │ it_Sardegna-Planes-Olbia-Airport.gtfs    │
--   59. │ 1399444.5721952983 │ Lille                                      │ Bialystok, Bus Station                       │ eu_flixbus.gtfs                          │
--   60. │ 1399383.5875824073 │ Medyka, Border Crossing                    │ Bruges (Station Brugge)                      │ eu_flixbus.gtfs                          │
--   61. │ 1396435.1516181207 │ Crotone                                    │ Frankfurt central train station              │ eu_flixbus.gtfs                          │
--   62. │ 1395037.9265416376 │ Kyiv (Central Bus Station)                 │ Munich central bus station                   │ eu_flixbus.gtfs                          │
--   63. │  1380013.746783772 │ Ljubljana bus station                      │ Daugavpils, Bus Station                      │ eu_flixbus.gtfs                          │
--   64. │  1375174.421858024 │ Warsaw, Bus Station West                   │ Lyon (Perrache Bus Station)                  │ eu_flixbus.gtfs                          │
--   65. │ 1367844.9193254227 │ Vienna Erdberg (Busterminal VIB)           │ Tallinn, Harbour Terminal D                  │ eu_flixbus.gtfs                          │
--   66. │ 1366757.9379953076 │ Boston                                     │ Chicago Union Station                        │ us_Amtrak.gtfs                           │
--   67. │ 1364322.4734750662 │ Aeroporto di Londra/London Gatwick (LGW)   │ Aeroporto di Olbia (OLB)                     │ it_Sardegna-Planes-Olbia-Airport.gtfs    │
--   68. │ 1361974.0179066102 │ Warsaw, Bus Station West                   │ Paris (Bercy Seine)                          │ eu_flixbus.gtfs                          │
--   69. │ 1357914.2182439824 │ Barcelona (Bus Terminal Nord)              │ Prague (Central Bus Station Florenc)         │ eu_flixbus.gtfs                          │
--   70. │ 1349613.8328104268 │ Aeroporto di Cagliari (CAG) (ELMAS)        │ Aeroporto di Duesseldorf (DUS)               │ it_Sardegna-Planes-Cagliari-Airport.gtfs │
--   71. │  1342153.769323527 │ Chicago Union Station                      │ New Orleans Union Passenger Terminal         │ us_Amtrak.gtfs                           │
--   72. │ 1324746.3498119332 │ Medyka, Border Crossing                    │ The Hague (Central Station)                  │ eu_flixbus.gtfs                          │
--   73. │ 1320144.5227463816 │ Bialystok, Bus Station                     │ Milan (Lampugnano bus station)               │ eu_flixbus.gtfs                          │
--   74. │ 1314126.9513575654 │ Przemysl, Bus Station                      │ The Hague (Central Station)                  │ eu_flixbus.gtfs                          │
--   75. │   1312920.47041077 │ Lisbon - Oriente                           │ Marseille - Saint-Charles Bus Station        │ eu_blablacar-bus.gtfs                    │
--   76. │ 1298289.2939956407 │ Riga, Bus Station                          │ Munich central bus station                   │ eu_flixbus.gtfs                          │
--   77. │  1292961.975522992 │ Naples                                     │ Paris - Bercy-Seine Bus Station              │ eu_blablacar-bus.gtfs                    │
--   78. │  1292854.905567993 │ Naples (FS Park Stazione Centrale)         │ Paris (Bercy Seine)                          │ eu_flixbus.gtfs                          │
--   79. │ 1292716.9667014976 │ Medyka, Border Crossing                    │ Amsterdam Sloterdijk                         │ eu_flixbus.gtfs                          │
--   80. │ 1274710.2830505313 │ Cheyenne (Sinclair)                        │ Sparks Transit Center                        │ us_flixbus.gtfs                          │
--   81. │  1271856.746418714 │ The Hague (Central Station)                │ Bialystok, Bus Station                       │ eu_flixbus.gtfs                          │
--   82. │ 1261545.8246241258 │ Aeroporto di Parigi/Paris Beauvais (BVA)   │ Aeroporto di Cagliari (CAG) (ELMAS)          │ it_Sardegna-Planes-Cagliari-Airport.gtfs │
--   83. │ 1257094.2979840692 │ Antwerp Rooseveltplaats (B5)               │ Županja bus station                          │ eu_flixbus.gtfs                          │
--   84. │  1256866.837042146 │ Brno (hotell Grand)                        │ Tallinna bussijaam                           │ ee_peatus.gtfs                           │
--   85. │ 1246819.3089760828 │ Budapest - Népliget                        │ Paris - Bercy-Seine Bus Station              │ eu_blablacar-bus.gtfs                    │
--   86. │  1246773.736887602 │ Paris (Bercy Seine)                        │ Budapest Népliget bus station                │ eu_flixbus.gtfs                          │
--   87. │  1246773.736887602 │ Budapest Népliget bus station              │ Paris (Bercy Seine)                          │ eu_flixbus.gtfs                          │
--   88. │ 1238905.3287423013 │ Amsterdam City Center - Sloterdijk         │ Barcelona - North Bus Station                │ eu_blablacar-bus.gtfs                    │
--   89. │  1238714.134792903 │ Paris (Bercy Seine)                        │ Murcia (Bus Station)                         │ eu_flixbus.gtfs                          │
--   90. │  1238618.108680929 │ Murcia                                     │ Paris - Bercy-Seine Bus Station              │ eu_blablacar-bus.gtfs                    │
--   91. │ 1228980.3550122108 │ Plovdiv Train station                      │ Munich central bus station                   │ eu_flixbus.gtfs                          │
--   92. │ 1226022.9667506535 │ Marseille (Saint-Charles)                  │ Zielona Gora, Bus Station                    │ eu_flixbus.gtfs                          │
--   93. │ 1225248.8056442244 │ Paris (Bercy Seine)                        │ Torun, Bus Station                           │ eu_flixbus.gtfs                          │
--   94. │  1217709.115922247 │ Warsaw, Chopin Airport                     │ Vaasa, Bus Station                           │ eu_flixbus.gtfs                          │
--   95. │ 1215111.6608562202 │ Porto - TIC Campanhã                       │ Paris - Bercy-Seine Bus Station              │ eu_blablacar-bus.gtfs                    │
--   96. │ 1215059.6950162204 │ Paris (Bercy Seine)                        │ Porto (TIC - Campanhã)                       │ eu_flixbus.gtfs                          │
--   97. │ 1207198.8661342275 │ Atlanta Bus Station                        │ New York Port Authority                      │ us_flixbus.gtfs                          │
--   98. │ 1206431.1250904046 │ Atlanta Bus Station                        │ New York Midtown (31st St & 8th Ave)         │ us_flixbus.gtfs                          │
--   99. │ 1204340.7388247948 │ Atlanta Bus Station                        │ New York City Chinatown (28 Allen St)        │ us_flixbus.gtfs                          │
--  100. │ 1202995.4935102344 │ Aeroporto di Cagliari (CAG) (ELMAS)        │ Aeroporto di Parigi/Paris C. de Gaulle (CDG) │ it_Sardegna-Planes-Cagliari-Airport.gtfs │
--       └───────────────dist─┴─l_id───────────────────────────────────────┴─r_id─────────────────────────────────────────┴─any(source)──────────────────────────────┘


--- only trains
--      ┌───────────────dist─┬─l_id──────────────────────────────────────────────────┬─r_id───────────────────────────────────┬─any(source)──────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
--   1. │ 3357888.1923631025 │ Toronto                                               │ Vancouver                              │ ca_Viarail.gtfs                                                                                                              │
--   2. │  2972639.332541529 │ Chicago Union Station                                 │ Richmond                               │ us_Amtrak.gtfs                                                                                                               │
--   3. │ 2822698.5204869467 │ Chicago Union Station                                 │ Portland                               │ us_Amtrak.gtfs                                                                                                               │
--   4. │ 2802405.9537520306 │ Chicago Union Station                                 │ Los Angeles                            │ us_Amtrak.gtfs                                                                                                               │
--   5. │ 2792176.3736140956 │ Chicago Union Station                                 │ Edmonds                                │ us_Amtrak.gtfs                                                                                                               │
--   6. │  2685566.452083582 │ Los Angeles                                           │ New Orleans Union Passenger Terminal   │ us_Amtrak.gtfs                                                                                                               │
--   7. │ 1904595.7713257372 │ Chicago Union Station                                 │ Miami Amtrak Station                   │ us_Amtrak.gtfs                                                                                                               │
--   8. │ 1884317.3620316128 │ New Orleans Union Passenger Terminal                  │ Ny Moynihan Train Hall At Penn Station │ us_Amtrak.gtfs                                                                                                               │
--   9. │ 1752528.8703451534 │ Miami Amtrak Station                                  │ Ny Moynihan Train Hall At Penn Station │ us_Amtrak.gtfs                                                                                                               │
--  10. │ 1695416.7861805079 │ Chicago Union Station                                 │ San Antonio Amtrak Station             │ us_Amtrak.gtfs                                                                                                               │
--  11. │  1542986.630649843 │ Los Angeles                                           │ Seattle                                │ us_Amtrak.gtfs                                                                                                               │
--  12. │ 1366757.9379953076 │ Boston                                                │ Chicago Union Station                  │ us_Amtrak.gtfs                                                                                                               │
--  13. │  1342153.769323527 │ Chicago Union Station                                 │ New Orleans Union Passenger Terminal   │ us_Amtrak.gtfs                                                                                                               │
--  14. │ 1160964.6074167688 │ Lorton Amtrak Auto Train Station                      │ Sanford Amtrak Auto Train Station      │ us_Amtrak.gtfs                                                                                                               │
--  15. │ 1156100.8841759819 │ Ny Moynihan Train Hall At Penn Station                │ Savannah                               │ us_Amtrak.gtfs                                                                                                               │
--  16. │ 1146399.8984789378 │ Chicago Union Station                                 │ Croton-Harmon Amtrak Station           │ us_Amtrak.gtfs                                                                                                               │
--  17. │ 1145253.0576368182 │ Chicago Union Station                                 │ Ny Moynihan Train Hall At Penn Station │ us_Amtrak.gtfs                                                                                                               │
--  18. │ 1016212.9516013731 │ Narvik stasjon                                        │ Stockholm Centralstation               │ no_Entur.gtfs                                                                                                                │
--  19. │ 1016194.3862462809 │ Stockholm Centralstation                              │ Narvik station                         │ se_Trafiklab.gtfs                                                                                                            │
--  20. │ 1008128.2659274775 │ Hegyeshalom pályaudvar                                │ Kyjiw-Passaschyrskyj                   │ at_Railway-Current-Reference-Data-2025.gtfs                                                                                  │
--  21. │  963190.6689383725 │ Stockholm Centralstation                              │ Dresden Hbf                            │ se_Trafiklab.gtfs                                                                                                            │
--  22. │   947125.675031288 │ Boston                                                │ Roanoke                                │ us_Amtrak.gtfs                                                                                                               │
--  23. │  941774.9763711928 │ Amsterdam Centraal                                    │ Wien Hauptbahnhof                      │ de_DELFI.gtfs                                                                                                                │
--  24. │  935773.6929755118 │ Sevilla-Santa Justa                                   │ Figueres Vilafant                      │ es_RENFE---Media%2C-Larga-Distancia-y-AVE.gtfs                                                                               │
--  25. │  932728.0417723072 │ Budapest-Nyugati pu                                   │ Hamburg-Altona                         │ sk_zssk.gtfs                                                                                                                 │
--  26. │    911694.73676328 │ Perpignan                                             │ Bruxelles Midi                         │ fr_horaires-des-tgv.gtfs                                                                                                     │
--  27. │  907611.7171257255 │ Cadiz                                                 │ Barcelona-Sants                        │ es_RENFE---Media%2C-Larga-Distancia-y-AVE.gtfs                                                                               │
--  28. │   907113.619041244 │ Vigo Guixar                                           │ Barcelona-Sants                        │ es_RENFE---Media%2C-Larga-Distancia-y-AVE.gtfs                                                                               │
--  29. │   898975.453395443 │ Santiago de Compostela                                │ Barcelona-Sants                        │ es_RENFE---Media%2C-Larga-Distancia-y-AVE.gtfs                                                                               │
--  30. │  880912.0802953154 │ Berlin Ostbahnhof                                     │ Paris Est                              │ fr_horaires-des-tgv.gtfs                                                                                                     │
--  31. │  858395.4103065612 │ Deventer                                              │ Wien Hauptbahnhof                      │ de_DELFI.gtfs                                                                                                                │
--  32. │  856528.2686079794 │ Charlotte Amtrak Station                              │ Ny Moynihan Train Hall At Penn Station │ us_Amtrak.gtfs                                                                                                               │
--  33. │  841243.0476705956 │ Marseille Saint-Charles                               │ Bruxelles Midi                         │ fr_horaires-des-tgv.gtfs                                                                                                     │
--  34. │  838362.6371090382 │ Ourense                                               │ Barcelona-Sants                        │ es_RENFE---Media%2C-Larga-Distancia-y-AVE.gtfs                                                                               │
--  35. │  834267.0406105234 │ Lille Europe                                          │ Marseille Saint-Charles                │ fr_horaires-des-tgv.gtfs                                                                                                     │
--  36. │  834143.9131785679 │ Marseille-Saint-Charles                               │ Lille-Europe                           │ ch_opentransportdataswiss25.gtfs                                                                                             │
--  37. │  834054.8612443403 │ Lille Flandres                                        │ Marseille Saint-Charles                │ fr_horaires-des-tgv.gtfs                                                                                                     │
--  38. │  829921.6692308197 │ Barcelone-Sants                                       │ Paris Gare de Lyon Hall 1 - 2          │ fr_horaires-des-tgv.gtfs                                                                                                     │
--  39. │  828021.5070683161 │ Jasper                                                │ Prince Rupert                          │ ca_Viarail.gtfs                                                                                                              │
--  40. │   827834.651816587 │ Roanoke                                               │ Springfield                            │ us_Amtrak.gtfs                                                                                                               │
--  41. │  827568.7455931219 │ Sevilla-Santa Justa                                   │ Barcelona-Sants                        │ es_RENFE---Media%2C-Larga-Distancia-y-AVE.gtfs                                                                               │
--  42. │   820315.317123104 │ Aix en Provence                                       │ Madrid Pta.Atocha - Almudena Grandes   │ es_RENFE---Media%2C-Larga-Distancia-y-AVE.gtfs                                                                               │
--  43. │   820315.317123104 │ Estación de tren Madrid Pta.Atocha - Almudena Grandes │ Estación de tren Aix en Provence       │ fr_horaires-ave-espagne-france.gtfs                                                                                          │
--  44. │  813794.5312335533 │ Penzance                                              │ Aberdeen                               │ gb_great-britain.gtfs                                                                                                        │
--  45. │  812673.2929768749 │ Stockholm Centralstation                              │ Hamburg Hbf                            │ se_Trafiklab.gtfs                                                                                                            │
--  46. │    805604.06797811 │ Montpellier Sud de France                             │ Bruxelles Midi                         │ fr_horaires-des-tgv.gtfs                                                                                                     │
--  47. │  805416.8005327992 │ Truro                                                 │ Montréal                               │ ca_Viarail.gtfs                                                                                                              │
--  48. │  804673.2807636582 │ Montpellier Saint-Roch                                │ Bruxelles Midi                         │ fr_horaires-des-tgv.gtfs                                                                                                     │
--  49. │  801722.2428380807 │ Helsinki                                              │ Kolari                                 │ fi_fintraffic.gtfs                                                                                                           │
--  50. │    798012.97060962 │ Le Havre                                              │ Marseille Saint-Charles                │ fr_horaires-des-tgv.gtfs                                                                                                     │
--  51. │    798012.97060962 │ Marseille Saint-Charles                               │ Le Havre                               │ fr_base-de-donnees-multimodale-des-reseaux-de-transport-public-normands.gtfs                                                 │
--  52. │  797939.7262240078 │ Marseille-Saint-Charles                               │ Le Havre                               │ ch_opentransportdataswiss25.gtfs                                                                                             │
--  53. │   796988.384789758 │ Francfort sur le Main                                 │ Marseille Saint-Charles                │ fr_horaires-des-tgv.gtfs                                                                                                     │
--  54. │  793814.9440878055 │ Tourcoing                                             │ Montpellier Sud de France              │ fr_horaires-des-tgv.gtfs                                                                                                     │
--  55. │  789611.3296669425 │ Hegyeshalom pályaudvar                                │ București Nord Gara A                  │ at_Railway-Current-Reference-Data-2025.gtfs                                                                                  │
--  56. │   787378.046447792 │ Boston                                                │ Petersburg Amtrak Station              │ us_Amtrak.gtfs                                                                                                               │
--  57. │  785770.4209581058 │ Lille Europe                                          │ Montpellier Sud de France              │ fr_horaires-des-tgv.gtfs                                                                                                     │
--  58. │  785756.7545236354 │ Lille-Europe                                          │ Montpellier Sud de France              │ ch_opentransportdataswiss25.gtfs                                                                                             │
--  59. │  785494.2124492873 │ Lille Flandres                                        │ Montpellier Sud de France              │ fr_horaires-des-tgv.gtfs                                                                                                     │
--  60. │  784452.6573498236 │ Lille-Europe                                          │ Montpellier Saint-Roch                 │ ch_opentransportdataswiss25.gtfs                                                                                             │
--  61. │  784445.1338616798 │ Lille Europe                                          │ Montpellier Saint-Roch                 │ fr_horaires-des-tgv.gtfs                                                                                                     │
--  62. │  784283.7886400525 │ Lille-Flandres                                        │ Montpellier Saint-Roch                 │ ch_opentransportdataswiss25.gtfs                                                                                             │
--  63. │  784167.0025486536 │ Lille Flandres                                        │ Montpellier Saint-Roch                 │ fr_horaires-des-tgv.gtfs                                                                                                     │
--  64. │  773802.1179708687 │ Kiel Hbf                                              │ Basel SBB                              │ de_DELFI.gtfs                                                                                                                │
--  65. │   771346.589795415 │ Kiel Hbf                                              │ Basel Bad Bf                           │ de_DELFI.gtfs                                                                                                                │
--  66. │  769577.4009603547 │ Malaga Maria Zambrano                                 │ Barcelona-Sants                        │ es_RENFE---Media%2C-Larga-Distancia-y-AVE.gtfs                                                                               │
--  67. │   768616.222978436 │ Ourense                                               │ Alicante/alacant                       │ es_RENFE---Media%2C-Larga-Distancia-y-AVE.gtfs                                                                               │
--  68. │  765793.1485965232 │ Rennes                                                │ Marseille Saint-Charles                │ fr_arrets-horaires-et-circuits-des-lignes-de-transports-en-commun-en-pays-de-la-loire-gtfs-destineo-reseaux-aom-aleop-1.gtfs │
--  69. │  765793.1485965232 │ Rennes                                                │ Marseille Saint-Charles                │ fr_horaires-des-tgv.gtfs                                                                                                     │
--  70. │  765739.0772533672 │ Marseille-Saint-Charles                               │ Rennes                                 │ ch_opentransportdataswiss25.gtfs                                                                                             │
--  71. │  763525.5834442435 │ Boston                                                │ Richmond Main Street Amtrak Station    │ us_Amtrak.gtfs                                                                                                               │
--  72. │  759356.2684840038 │ Strasbourg                                            │ Bordeaux Saint-Jean                    │ fr_horaires-des-tgv.gtfs                                                                                                     │
--  73. │   759346.418099664 │ Strasbourg                                            │ Bordeaux-St-Jean                       │ ch_opentransportdataswiss25.gtfs                                                                                             │
--  74. │  753574.9028343817 │ Antequera AV                                          │ Barcelona-Sants                        │ es_RENFE---Media%2C-Larga-Distancia-y-AVE.gtfs                                                                               │
--  75. │  749836.2633334766 │ Wien Hauptbahnhof                                     │ Hamburg-Altona                         │ de_DELFI.gtfs                                                                                                                │
--  76. │  749819.0916601219 │ Wien Hauptbahnhof                                     │ Hamburg-Altona                         │ de_DELFI.gtfs                                                                                                                │
--  77. │  748247.6562550408 │ Boden Centralstation                                  │ Stockholm Centralstation               │ no_Entur.gtfs                                                                                                                │
--  78. │  748227.2520808022 │ Amsterdam Centraal                                    │ Wörgl Hbf                              │ de_DELFI.gtfs                                                                                                                │
--  79. │   748208.501737712 │ Boden C/Resecentrum                                   │ Stockholm Centralstation               │ se_Trafiklab.gtfs                                                                                                            │
--  80. │  748130.3657958974 │ Amsterdam Centraal                                    │ Wörgl Hbf                              │ nl_ovapi.gtfs                                                                                                                │
--  81. │  744532.2862857749 │ Świnoujście                                           │ Przemyśl Główny                        │ pl_PKP-Intercity.gtfs                                                                                                        │
--  82. │  742456.8674775768 │ Praha hl.n.                                           │ Den Haag HS                            │ nl_ovapi.gtfs                                                                                                                │
--  83. │  740728.1957765864 │ Helsinki                                              │ Kemijärvi                              │ fi_fintraffic.gtfs                                                                                                           │
--  84. │   740643.935397662 │ Kemijärvi                                             │ Helsinki                               │ fi_digitraffic.gtfs                                                                                                          │
--  85. │  740510.1034516718 │ Helsinki                                              │ Kemijärvi                              │ fi_fintraffic.gtfs                                                                                                           │
--  86. │  737159.5531007934 │ Lleida                                                │ Sevilla-Santa Justa                    │ es_RENFE---Media%2C-Larga-Distancia-y-AVE.gtfs                                                                               │
--  87. │  733242.1137625299 │ St. Albans Amtrak Station                             │ Washington Union Station               │ us_Amtrak.gtfs                                                                                                               │
--  88. │  727720.9378693356 │ Hamm (Westf.)                                         │ Wien Hbf                               │ nl_ovapi.gtfs                                                                                                                │
--  89. │  724075.7218279395 │ Alicante/alacant                                      │ Gijon                                  │ es_RENFE---Media%2C-Larga-Distancia-y-AVE.gtfs                                                                               │
--  90. │  714094.7759397656 │ Paris Austerlitz                                      │ Cerbère                                │ fr_horaires-des-lignes-intercites-sncf.gtfs                                                                                  │
--  91. │  713526.0213835966 │ Inverness                                             │ London Euston                          │ gb_great-britain.gtfs                                                                                                        │
--  92. │  713504.4268593577 │ Inverness                                             │ London Kings Cross                     │ gb_great-britain.gtfs                                                                                                        │
--  93. │   712011.070031751 │ Tourcoing                                             │ Bordeaux Saint-Jean                    │ fr_horaires-des-tgv.gtfs                                                                                                     │
--  94. │  711442.5259069377 │ München Hbf                                           │ Ostseebad Binz                         │ de_DELFI.gtfs                                                                                                                │
--  95. │  711429.7105598713 │ München Hbf                                           │ Ostseebad Binz                         │ de_DELFI.gtfs                                                                                                                │
--  96. │   710370.485750598 │ Paris Austerlitz                                      │ Latour-de-Carol - Enveitg              │ fr_horaires-des-lignes-intercites-sncf.gtfs                                                                                  │
--  97. │  710339.0877031066 │ Huesca                                                │ Sevilla-Santa Justa                    │ es_RENFE---Media%2C-Larga-Distancia-y-AVE.gtfs                                                                               │
--  98. │  710097.0954881496 │ Strasbourg                                            │ Nantes                                 │ ch_opentransportdataswiss25.gtfs                                                                                             │
--  99. │  710037.2399140501 │ Strasbourg                                            │ Nantes                                 │ fr_horaires-des-tgv.gtfs                                                                                                     │
-- 100. │  710037.2399140501 │ Strasbourg                                            │ Nantes                                 │ fr_arrets-horaires-et-circuits-des-lignes-de-transports-en-commun-en-pays-de-la-loire-gtfs-destineo-reseaux-aom-aleop-1.gtfs │


-- NB: needs stop UUID clusters to have been generated
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
-- where source like 'be_%'
-- or source like 'ch_%'
-- and source like 'lu_%'
-- or source like 'fr_%'
-- or source like 'gb_%'
-- or source like 'it_%'
-- or source like 'de_%'
-- or source like 'nl_%'
) st
left join population_h3 pop on newh3 = pop.h3
where next_stop != stop_uuid
and res = 9
)
where travel_time > 0
group by all;
-- takes aaaaages but it does seem to work (4200 seconds)
-- exchange tables transitous_everything_edgelist and transitous_everything_edgelist2;
-- isochrone sketch
select distinct stop_name, stop_uuid from transitous_everything_stop_times_one_day_even_saner where source like 'ch_%' and stop_name ilike 'meggen%zentrum%' limit 10
select distinct stop_name, stop_uuid from transitous_everything_stop_times_one_day_even_saner where source like 'fr_%' and stop_name ilike 'nice%ville%' limit 10
select distinct stop_name, stop_uuid from transitous_everything_stop_times_one_day_even_saner where source like 'gb_%' and stop_name ilike 'shanklin' limit 10
select stop_name, stop_uuid, count() c from transitous_everything_stop_times_one_day_even_saner where source like 'gb_%' and stop_name ilike '%Waterloo%' group by all order by c desc limit 10

-- why is this so slow once minutes > 30ish :(
WITH RECURSIVE
    --'8575785' AS start_node, -- morbio
    1158217 as start_node, -- meggen zentrum
    --469887 as start_node, -- meggen zentrum
    30 as max_travel_time,
    20 as max_walk_time,
    parseDateTimeBestEffort('2025-01-01 09:00:00') AS journey_start_time,
    journey_start_time + INTERVAL max_travel_time MINUTE AS journey_end_limit_time,
    reachable_destinations AS (
        -- Anchor Member: First hops from the starting source_id
        -- These are direct connections from the input_source_id
        SELECT
            e.next_stop current_node_id,
            e.next_arrival current_arrival_time,
            [start_node, e.next_stop] path_nodes,
            1 hop_count
        FROM transitous_everything_edgelist_sane e
        WHERE e.stop_uuid = start_node
          AND e.departure_time >= journey_start_time
          AND e.next_arrival <= journey_end_limit_time
          --AND source like 'ch_%'

        UNION ALL

        SELECT
            e.next_stop,
            e.next_arrival,
            arrayPushBack(rd.path_nodes, e.next_stop) AS path_nodes,
            rd.hop_count + 1
        FROM reachable_destinations rd
        JOIN transitous_everything_edgelist_sane e ON rd.current_node_id = e.stop_uuid
        -- To access journey_end_limit_time
        WHERE e.departure_time >= rd.current_arrival_time
          AND e.next_arrival <= journey_end_limit_time
          AND NOT has(rd.path_nodes, e.next_stop)
          AND rd.hop_count < 1000
          --AND source like 'ch_%'
    )
select arrayJoin(h3) h3, min(travel_time + (h3Distance(h3, stop_h3) / 0.6)) value from (
SELECT current_node_id, dateDiff('minute', journey_start_time, min(current_arrival_time)) travel_time, max_travel_time - travel_time remaining_travel_time,
toUInt16(floor(least(remaining_travel_time, max_walk_time) * 0.6)) res_10_radius, any(st.stop_lat) lat, any(st.stop_lon) lon, assumeNotNull(geoToH3(lon, lat, 10)) stop_h3,
--[geoToH3(lon, lat, 9)] h3
h3kRing(stop_h3, assumeNotNull(res_10_radius)) h3
--SELECT current_node_id, min(current_arrival_time)--, any(st.stop_lat), any(st.stop_lon)
FROM reachable_destinations r
-- Optionally, you can filter out the start_node itself if it becomes reachable via a loop
LEFT ANY JOIN transitous_everything_stop_times_one_day_even_saner st on st.stop_uuid = r.current_node_id
WHERE current_node_id != start_node
GROUP BY current_node_id
ORDER BY current_node_id
)
group by h3

select h3EdgeLengthKm(7)

-- export for router
select stop_uuid, next_stop, next_arrival, departure_time from transitous_everything_edgelist_sane ts
left join transitous_everything_trips t on ts.trip_id = t.trip_id
left join transitous_everything_routes r on t.route_id = r.route_id
where true
and ((route_type = 2) or (route_type between 100 and 199))
and ts.source like 'gb%'
into outfile 'edgelist_onlytrains_gb.arrow' -- eugh, wrong format for times for Julia
