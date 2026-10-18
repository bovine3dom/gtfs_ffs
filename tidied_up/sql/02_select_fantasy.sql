-- Rendered by tidied_up/orchestrate.jl.
-- Fantasy timetable: best day per route pattern and original source.
-- This mode is intentionally silent in table names: {{OUTPUT_PREFIX}}*.

SET receive_timeout = {{TIMEOUT_SECONDS}};
SET send_timeout = {{TIMEOUT_SECONDS}};
SET max_threads = {{THREADS}};
SET max_insert_threads = 1;
SET max_execution_time = 0;
SET max_result_rows = 0;
SET max_result_bytes = 0;
SET max_bytes_before_external_group_by = {{SPILL_BYTES}};
SET max_bytes_before_external_sort = {{SPILL_BYTES}};
SET max_memory_usage = {{MEMORY_BYTES}};
SET join_algorithm = 'auto';

DROP TABLE IF EXISTS {{OUTPUT_PREFIX}}selected_days;
CREATE TABLE {{OUTPUT_PREFIX}}selected_days
ENGINE = MergeTree
ORDER BY (source, origin_source, sane_route_id)
AS
WITH
    service_trip_counts AS (
        SELECT t.source, t.origin_source, tru.sane_route_id, t.service_id, count() AS trip_count
        FROM {{RAW_PREFIX}}trips t
    INNER JOIN {{RAW_PREFIX}}trip_route_uuids tru
        ON t.source = tru.source AND t.trip_id = tru.trip_id
        GROUP BY t.source, t.origin_source, tru.sane_route_id, t.service_id
    ),
    day_counts AS (
        SELECT c.source, c.origin_source, c.sane_route_id, v.check_date, sum(c.trip_count) AS active_trip_count
        FROM service_trip_counts c
        INNER JOIN {{OUTPUT_PREFIX}}valid_service_dates v
            ON c.source = v.source AND c.service_id = v.service_id
        GROUP BY c.source, c.origin_source, c.sane_route_id, v.check_date
    ),
    best_days AS (
        SELECT
            source, origin_source, sane_route_id,
            argMin(toNullable(d.check_date),
                   tuple(-toInt64(d.active_trip_count), d.check_date)) AS selected_date,
            max(d.active_trip_count) AS active_trip_count,
            toNullable(min(d.check_date)) AS first_active_date,
            toNullable(max(d.check_date)) AS last_active_date
        FROM day_counts d
        GROUP BY source, origin_source, sane_route_id
    )
SELECT
    g.source AS source,
    g.origin_source AS origin_source,
    g.sane_route_id AS sane_route_id,
    b.selected_date AS selected_date,
    ifNull(b.active_trip_count, 0) AS active_trip_count,
    b.first_active_date AS first_active_date,
    b.last_active_date AS last_active_date,
    toDate('{{MIN_DATE}}') AS search_start_date,
    toDate('{{MAX_DATE}}') AS search_end_date,
    if(ifNull(b.active_trip_count, 0) = 0, 'no_active_trips', 'selected') AS status
FROM (SELECT DISTINCT source, origin_source, sane_route_id FROM service_trip_counts) g
LEFT JOIN best_days b ON g.source = b.source AND g.origin_source = b.origin_source AND g.sane_route_id = b.sane_route_id;

DROP TABLE IF EXISTS {{OUTPUT_PREFIX}}valid_trips_lookup;
CREATE TABLE {{OUTPUT_PREFIX}}valid_trips_lookup
ENGINE = MergeTree
ORDER BY (source, trip_id)
AS
SELECT DISTINCT
    t.source AS source,
    t.origin_source AS origin_source,
    t.trip_id AS trip_id,
    b.selected_date AS selected_date
FROM {{RAW_PREFIX}}trips t
    INNER JOIN {{RAW_PREFIX}}trip_route_uuids tru
        ON t.source = tru.source AND t.trip_id = tru.trip_id
INNER JOIN {{OUTPUT_PREFIX}}selected_days b
    ON t.source = b.source AND t.origin_source = b.origin_source AND tru.sane_route_id = b.sane_route_id
INNER JOIN {{OUTPUT_PREFIX}}valid_service_dates v
    ON t.source = v.source AND t.service_id = v.service_id
    AND v.check_date = b.selected_date;

DROP TABLE IF EXISTS {{OUTPUT_PREFIX}}stop_times_one_day_sane;
CREATE TABLE {{OUTPUT_PREFIX}}stop_times_one_day_sane
ENGINE = MergeTree
ORDER BY (source, stop_lat, stop_lon, sane_route_id, trip_id, stop_sequence)
SETTINGS allow_nullable_key = 1
AS
SELECT
    st.source AS source,
    vt.origin_source AS origin_source,
    vt.selected_date AS selected_date,
    tru.sane_route_id AS sane_route_id,
    st.trip_id AS trip_id,
    st.stop_id AS stop_id,
    st.stop_sequence AS stop_sequence,
    st.arrival_time AS arrival_time,
    st.departure_time AS departure_time,
    ts.stop_lat AS stop_lat,
    ts.stop_lon AS stop_lon,
    cast(tst.service_id AS LowCardinality(String)) AS service_id,
    tst.trip_headsign AS trip_headsign,
    tst.trip_short_name AS trip_short_name,
    tst.direction_id AS direction_id,
    tst.block_id AS block_id,
    tst.shape_id AS shape_id,
    tst.wheelchair_accessible AS wheelchair_accessible,
    tst.bikes_allowed AS bikes_allowed,
    ro.route_id AS route_id,
    ro.route_short_name AS route_short_name,
    ro.route_long_name AS route_long_name,
    ro.route_desc AS route_desc,
    ro.route_type AS route_type,
    ro.route_url AS route_url,
    cast(ro.route_color AS LowCardinality(String)) AS route_color,
    cast(ro.route_text_color AS LowCardinality(String)) AS route_text_color,
    ro.route_sort_order AS route_sort_order,
    ro.continuous_pickup AS route_continuous_pickup,
    ro.continuous_drop_off AS route_continuous_drop_off,
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
FROM {{RAW_PREFIX}}stop_times st
INNER JOIN {{OUTPUT_PREFIX}}valid_trips_lookup vt
    ON st.source = vt.source AND st.trip_id = vt.trip_id
INNER JOIN {{RAW_PREFIX}}trip_route_uuids tru
    ON st.source = tru.source AND st.trip_id = tru.trip_id
INNER JOIN {{RAW_PREFIX}}trips tst
    ON st.source = tst.source AND st.trip_id = tst.trip_id
INNER JOIN {{RAW_PREFIX}}routes ro
    ON tst.source = ro.source AND tst.route_id = ro.route_id
INNER JOIN {{RAW_PREFIX}}stops ts
    ON st.source = ts.source AND st.stop_id = ts.stop_id;
