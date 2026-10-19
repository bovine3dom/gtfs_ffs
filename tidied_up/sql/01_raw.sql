-- Rendered by tidied_up/orchestrate.jl.
-- Raw GTFS tables are shared by both timetable products.

SET receive_timeout = {{TIMEOUT_SECONDS}};
SET send_timeout = {{TIMEOUT_SECONDS}};
SET max_threads = {{THREADS}};
SET max_insert_threads = 1;
SET max_partitions_per_insert_block = {{TRIP_BUCKETS}};
SET max_execution_time = 0;
SET max_result_rows = 0;
SET max_result_bytes = 0;
SET connect_timeout_with_failover_ms = {{TIMEOUT_MS}};
SET http_connection_timeout = {{TIMEOUT_SECONDS}};
SET http_send_timeout = {{TIMEOUT_SECONDS}};
SET http_receive_timeout = {{TIMEOUT_SECONDS}};
SET input_format_skip_unknown_fields = 1;
SET input_format_defaults_for_omitted_fields = 1;
SET input_format_csv_empty_as_default = 1;
SET max_bytes_before_external_group_by = {{SPILL_BYTES}};
SET max_bytes_before_external_sort = {{SPILL_BYTES}};
SET max_memory_usage = {{MEMORY_BYTES}};
SET join_algorithm = 'auto';

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

-- Ordinary attribution rows are credits, not source groups.
DROP TABLE IF EXISTS {{RAW_PREFIX}}attributions;
CREATE TABLE {{RAW_PREFIX}}attributions
ENGINE = MergeTree
ORDER BY (source, attribution_id, route_id)
AS
SELECT DISTINCT *
FROM file('{{INPUT_GLOB}}/attributions.txt', 'CSVWithNames', '
    source String, attribution_id String, agency_id String, route_id String,
    trip_id String, organization_name String, is_producer String,
    is_operator String, is_authority String, attribution_url String,
    attribution_email String, attribution_phone String
')
SETTINGS use_hive_partitioning = 1, engine_file_empty_if_not_exists = 1;

SELECT throwIf(count() > 0, 'Invalid Nigiri source attribution')
FROM {{RAW_PREFIX}}attributions
WHERE startsWith(attribution_id, 'nigiri:') AND (
    NOT match(attribution_id, '^nigiri:[0-9]+:[0-9]+:[0-9]+$')
    OR splitByChar(':', attribution_id)[3] != route_id
    OR organization_name = '' OR is_producer != '1'
    OR agency_id != '' OR trip_id != ''
);

SELECT throwIf(count() > 0, 'Conflicting Nigiri attribution IDs')
FROM (
    SELECT source, attribution_id
    FROM {{RAW_PREFIX}}attributions
    WHERE startsWith(attribution_id, 'nigiri:')
    GROUP BY source, attribution_id
    HAVING count() != 1
);

-- Keep support for older exports with two CSV files.
DROP TABLE IF EXISTS {{RAW_PREFIX}}route_sources;
CREATE TABLE {{RAW_PREFIX}}route_sources
ENGINE = MergeTree
ORDER BY (source, route_id)
AS
SELECT DISTINCT source, route_id, origin_source FROM (
    SELECT source, route_id, origin_source
    FROM file('{{INPUT_GLOB}}/route_sources.csv', 'CSVWithNames',
              'source String, route_id String, origin_source String')
    UNION ALL
    SELECT source, route_id, splitByChar(':', attribution_id)[2] AS origin_source
    FROM {{RAW_PREFIX}}attributions
    WHERE startsWith(attribution_id, 'nigiri:')
)
SETTINGS use_hive_partitioning = 1, engine_file_empty_if_not_exists = 1;

DROP TABLE IF EXISTS {{RAW_PREFIX}}sources;
CREATE TABLE {{RAW_PREFIX}}sources
ENGINE = MergeTree
ORDER BY (source, origin_source, source_file)
AS
SELECT DISTINCT source, origin_source, source_file FROM (
    SELECT source, origin_source, source_file
    FROM file('{{INPUT_GLOB}}/sources.csv', 'CSVWithNames',
              'source String, origin_source String, source_file String')
    UNION ALL
    SELECT source, splitByChar(':', attribution_id)[2] AS origin_source,
           organization_name AS source_file
    FROM {{RAW_PREFIX}}attributions
    WHERE startsWith(attribution_id, 'nigiri:')
)
SETTINGS use_hive_partitioning = 1, engine_file_empty_if_not_exists = 1;

SELECT throwIf(count() > 0, 'Empty or conflicting route source metadata')
FROM (
    SELECT source, route_id
    FROM {{RAW_PREFIX}}route_sources
    GROUP BY source, route_id
    HAVING count() != 1 OR route_id = '' OR countIf(origin_source = '') > 0
);

CREATE TABLE {{RAW_PREFIX}}trips
ENGINE MergeTree
PARTITION BY sipHash64(source, trip_id) % {{TRIP_BUCKETS}}
ORDER BY (source, route_id, service_id, trip_id)
AS
SELECT
    toLowCardinality(assumeNotNull(tt.source)) AS source,
    tt.route_id route_id,
    ifNull(rs.origin_source, '') AS origin_source,
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
LEFT JOIN {{RAW_PREFIX}}route_sources rs
    ON tt.source = rs.source AND tt.route_id = rs.route_id
SETTINGS use_hive_partitioning = 1;

ALTER TABLE {{RAW_PREFIX}}trips
ADD PROJECTION trip_shape_lookup
(
    SELECT source, trip_id, shape_id
    ORDER BY (source, trip_id)
);

ALTER TABLE {{RAW_PREFIX}}trips
MATERIALIZE PROJECTION trip_shape_lookup
SETTINGS mutations_sync = 1;

-- Do not treat an incomplete combined feed as a single original source.
SELECT throwIf(count() > 0, 'Missing route source metadata for trips')
FROM {{RAW_PREFIX}}trips
WHERE origin_source = '' AND source IN (
    SELECT source FROM {{RAW_PREFIX}}route_sources
    UNION DISTINCT SELECT source FROM {{RAW_PREFIX}}sources
);

SELECT throwIf(count() > 0, 'Missing source file metadata for trips')
FROM {{RAW_PREFIX}}trips
WHERE origin_source != '' AND (source, origin_source) NOT IN (
    SELECT source, origin_source FROM {{RAW_PREFIX}}sources
);

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
PARTITION BY sipHash64(source, trip_id) % {{TRIP_BUCKETS}}
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
(
    source LowCardinality(String),
    trip_id String,
    sane_route_id UUID
)
ENGINE = MergeTree
PARTITION BY sipHash64(source, trip_id) % {{TRIP_BUCKETS}}
ORDER BY (source, trip_id);
