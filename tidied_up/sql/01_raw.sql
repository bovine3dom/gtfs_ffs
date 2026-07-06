-- Rendered by tidied_up/orchestrate.jl.
-- Raw GTFS tables are shared by both timetable products.

SET receive_timeout = 40000;
SET send_timeout = 40000;
SET max_threads = 5;
SET max_execution_time = 0;
SET max_result_rows = 0;
SET max_result_bytes = 0;
SET connect_timeout_with_failover_ms = 40000000;
SET http_connection_timeout = 40000;
SET http_send_timeout = 40000;
SET http_receive_timeout = 40000;
SET input_format_skip_unknown_fields = 1;
SET input_format_defaults_for_omitted_fields = 1;
SET input_format_csv_empty_as_default = 1;
SET max_bytes_before_external_group_by = '90G';
SET max_memory_usage = '100G';

DROP TABLE IF EXISTS {{RAW_PREFIX}}routes;
DROP TABLE IF EXISTS {{RAW_PREFIX}}trips;
DROP TABLE IF EXISTS {{RAW_PREFIX}}stop_times;
DROP TABLE IF EXISTS {{RAW_PREFIX}}calendar;
DROP TABLE IF EXISTS {{RAW_PREFIX}}calendar_dates;
DROP TABLE IF EXISTS {{RAW_PREFIX}}stops;
DROP TABLE IF EXISTS {{RAW_PREFIX}}agency;
DROP TABLE IF EXISTS {{RAW_PREFIX}}shapes;
DROP TABLE IF EXISTS {{RAW_PREFIX}}trip_route_uuids;

CREATE TABLE {{RAW_PREFIX}}routes
ENGINE MergeTree
ORDER BY (source, route_id, route_type)
AS
SELECT
    toLowCardinality(assumeNotNull(source)) AS source,
    route_id,
    toLowCardinality(assumeNotNull(agency_id)) AS agency_id,
    route_short_name,
    route_long_name,
    route_desc,
    toUInt16OrZero(route_type) AS route_type,
    route_url,
    route_color,
    route_text_color,
    toUInt32OrNull(route_sort_order) AS route_sort_order,
    toUInt8OrNull(continuous_pickup) AS continuous_pickup,
    toUInt8OrNull(continuous_drop_off) AS continuous_drop_off
FROM file('{{INPUT_GLOB}}/routes.txt', 'CSVWithNames', '
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

CREATE TABLE {{RAW_PREFIX}}trips
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
FROM file('{{INPUT_GLOB}}/trips.txt', 'CSVWithNames', '
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
') tt
SETTINGS use_hive_partitioning = 1;

CREATE TABLE {{RAW_PREFIX}}shapes
ENGINE = MergeTree
ORDER BY (source, shape_id, shape_pt_sequence)
AS
SELECT
    toLowCardinality(assumeNotNull(source)) source,
    assumeNotNull(shape_id) shape_id,
    toFloat64OrNull(shape_pt_lat) shape_pt_lat,
    toFloat64OrNull(shape_pt_lon) shape_pt_lon,
    toUInt32OrZero(shape_pt_sequence) shape_pt_sequence,
    toFloat64OrNull(shape_dist_traveled) shape_dist_traveled
FROM file('{{INPUT_GLOB}}/shapes.txt', 'CSVWithNames', '
    source String,
    shape_id String,
    shape_pt_lat String,
    shape_pt_lon String,
    shape_pt_sequence String,
    shape_dist_traveled String
')
SETTINGS use_hive_partitioning = 1;

CREATE TABLE {{RAW_PREFIX}}stops
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
FROM file('{{INPUT_GLOB}}/stops.txt', 'CSVWithNames', '
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

CREATE TABLE {{RAW_PREFIX}}calendar
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
    toDateOrZero(start_date) AS start_date,
    toDateOrZero(end_date) AS end_date
FROM file('{{INPUT_GLOB}}/calendar.txt', 'CSVWithNames', '
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

CREATE TABLE {{RAW_PREFIX}}calendar_dates
ENGINE MergeTree
ORDER BY (source, service_id, date, exception_type)
AS
SELECT
    toLowCardinality(assumeNotNull(tcd.source)) AS source,
    tcd.service_id service_id,
    toDateOrZero(date) AS date,
    toUInt8OrZero(exception_type) AS exception_type
FROM file('{{INPUT_GLOB}}/calendar_dates.txt', 'CSVWithNames', '
    source String,
    service_id String,
    date String,
    exception_type String
') tcd
SETTINGS use_hive_partitioning = 1;

CREATE TABLE {{RAW_PREFIX}}agency
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
FROM file('{{INPUT_GLOB}}/agency.txt', 'CSVWithNames', '
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

CREATE TABLE {{RAW_PREFIX}}stop_times
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

INSERT INTO {{RAW_PREFIX}}stop_times
SELECT
    toLowCardinality(assumeNotNull(st.source)) AS source,
    st.trip_id,
    assumeNotNull(parseDateTimeBestEffortOrNull(arrival_time)) AS arrival_time,
    assumeNotNull(parseDateTimeBestEffortOrNull(departure_time)) AS departure_time,
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
FROM file('{{INPUT_GLOB}}/stop_times.txt', 'CSVWithNames', '
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

CREATE TABLE {{RAW_PREFIX}}trip_route_uuids
ENGINE = MergeTree
ORDER BY (source, trip_id)
AS
SELECT
    source,
    trip_id,
    reinterpretAsUUID(sipHash128Reference(
        arrayMap(x -> x.2, arraySort(
            groupArray((departure_time, xxh3(stop_id)))
        ))
    )) AS sane_route_id
FROM {{RAW_PREFIX}}stop_times
GROUP BY source, trip_id;
