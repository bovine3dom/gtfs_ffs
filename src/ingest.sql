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
DROP TABLE IF EXISTS transitous_routes;
DROP TABLE IF EXISTS transitous_trips;
DROP TABLE IF EXISTS transitous_stop_times;
DROP TABLE IF EXISTS transitous_calendar;
DROP TABLE IF EXISTS transitous_calendar_dates;
DROP TABLE IF EXISTS transitous_stops;
DROP TABLE IF EXISTS transitous_agency;
-- oh it looks like there's also un-extended route type
-- https://ipeagit.github.io/gtfstools/reference/filter_by_route_type.html
-- 0 = tram, 1 = metro, 2 = rail, 3 = bus, 4 = ferry, 5 = cable car, 6 = gondola, 7 = funicular, 11 = trolleybus, 12 = monorail
CREATE TABLE transitous_routes
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
and ((route_type = 2) or (route_type between 100 and 199))
--and (route_type between 1400 and 1499) -- funiculars
--and source ilike 'fr%'
--and route_desc ilike '%%'
SETTINGS use_hive_partitioning = 1;

CREATE TABLE transitous_trips
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
left semi join transitous_routes tr on tt.route_id = tr.route_id and tt.source = tr.source
WHERE true
SETTINGS use_hive_partitioning = 1;

-- maybe it would have been better to go via the calendar first so that we only had trips that were running on x date
CREATE TABLE transitous_stop_times
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
left semi join transitous_trips tt on st.trip_id = tt.trip_id and st.source = tt.source
WHERE true
SETTINGS use_hive_partitioning = 1;

CREATE TABLE transitous_stops
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
left semi join transitous_stop_times st on ts.stop_id = st.stop_id and ts.source = st.source
WHERE true
SETTINGS use_hive_partitioning = 1;

CREATE TABLE transitous_calendar
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
left semi join transitous_trips tt on tc.service_id = tt.service_id and tc.source = tt.source
WHERE true
SETTINGS use_hive_partitioning = 1;

CREATE TABLE transitous_calendar_dates
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
left semi join transitous_trips tt on tcd.service_id = tt.service_id and tcd.source = tt.source
WHERE true
SETTINGS use_hive_partitioning = 1;

CREATE TABLE transitous_agency
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
left semi join transitous_routes tr on tr.agency_id = ta.agency_id and ta.source = tr.source
WHERE true
SETTINGS use_hive_partitioning = 1;


-- one big table for convenience
DROP TABLE IF EXISTS transitous_stop_times_one_day;
CREATE TABLE transitous_stop_times_one_day -- 250 seconds 😎
ENGINE MergeTree
order by (source, stop_id, trip_id, arrival_time, departure_time)
settings allow_nullable_key = 1
AS
with active_services as (
    select ca.service_id service_id, ca.source source from transitous_calendar as ca
    left anti join transitous_calendar_dates as cd_remove on
    ca.service_id = cd_remove.service_id and ca.source = cd_remove.source
    and cd_remove.date = '2025-05-13' and cd_remove.exception_type = 2
    where true
    and ca.start_date <= '2025-05-13' and ca.end_date >= '2025-05-13'
    and ca.tuesday
    union distinct
    SELECT cd.service_id service_id, cd.source source
    FROM transitous_calendar_dates AS cd
    WHERE cd.date = '2025-05-13'
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
from transitous_trips tst
inner join active_services on active_services.service_id = tst.service_id and active_services.source = tst.source
inner join transitous_stop_times st on tst.trip_id = st.trip_id and tst.source = st.source
inner join transitous_stops ts on st.stop_id = ts.stop_id and st.source = st.source -- they don't seem to be unique? 10 rows -> 6700
inner join transitous_trips tr on tst.trip_id = tr.trip_id and tst.source = tr.source
inner join transitous_routes ro on tr.route_id = ro.route_id and tr.source = ro.source

DROP TABLE IF EXISTS transitous_stop_times_one_day_sane;
CREATE TABLE transitous_stop_times_one_day_sane -- ignore route ids, only care about stops
ENGINE MergeTree
order by (source, stop_id, sane_route_id, stop_lat, stop_lon, trip_id, arrival_time, departure_time)
settings allow_nullable_key = 1
AS
with route_uuids as (
    select arrayJoin(trip_id) trip_id, generateUUIDv7() sane_route_id, source from (
        select source, stop_id, groupArray(trip_id) trip_id from (
            select source, groupArray(stop_id) stop_id, trip_id from (
                select source, stop_id, departure_time, trip_id from transitous_stop_times_one_day st 
                order by departure_time asc
            )
            group by all
        )
        group by all
    )
)
select * from transitous_stop_times_one_day st
inner join route_uuids ru on ru.trip_id = st.trip_id and ru.source = st.source
