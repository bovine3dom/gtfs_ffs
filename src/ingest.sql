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
