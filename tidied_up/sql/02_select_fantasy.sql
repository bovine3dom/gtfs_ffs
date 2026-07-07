-- Rendered by tidied_up/orchestrate.jl.
-- Fantasy timetable: best service day per unique route within each source.
-- This mode is intentionally silent in table names: {{OUTPUT_PREFIX}}*.

SET receive_timeout = {{TIMEOUT_SECONDS}};
SET send_timeout = {{TIMEOUT_SECONDS}};
SET max_threads = 5;
SET max_execution_time = 0;
SET max_result_rows = 0;
SET max_result_bytes = 0;
SET max_bytes_before_external_group_by = '90G';
SET max_memory_usage = '100G';

DROP TABLE IF EXISTS {{OUTPUT_PREFIX}}valid_trips_lookup;
CREATE TABLE {{OUTPUT_PREFIX}}valid_trips_lookup
ENGINE = MergeTree()
ORDER BY (source, trip_id)
AS
WITH
    toDate('{{MIN_DATE}}') AS min_date,
    toDate('{{MAX_DATE}}') AS max_date,
    route_service_counts AS (
        SELECT
            t.source,
            t.service_id,
            tru.sane_route_id,
            count() AS trip_count,
            groupArray(t.trip_id) AS trip_ids
        FROM {{RAW_PREFIX}}trips t
        JOIN {{RAW_PREFIX}}trip_route_uuids tru
            ON t.source = tru.source AND t.trip_id = tru.trip_id
        GROUP BY t.source, t.service_id, tru.sane_route_id
    ),
    valid_service_dates AS (
        SELECT
            source,
            service_id,
            check_date
        FROM
        (
            SELECT
                source,
                service_id,
                arrayJoin(
                    arrayMap(x -> toDate(x),
                        range(toUInt32(greatest(start_date, min_date)), toUInt32(least(end_date, max_date)) + 1)
                    )
                ) AS check_date,
                monday, tuesday, wednesday, thursday, friday, saturday, sunday
            FROM {{RAW_PREFIX}}calendar
            WHERE end_date >= min_date AND start_date <= max_date
        )
        WHERE
            CASE toDayOfWeek(check_date)
                WHEN 1 THEN monday WHEN 2 THEN tuesday WHEN 3 THEN wednesday
                WHEN 4 THEN thursday WHEN 5 THEN friday WHEN 6 THEN saturday
                WHEN 7 THEN sunday ELSE 0
            END = 1

        UNION DISTINCT

        SELECT source, service_id, date AS check_date
        FROM {{RAW_PREFIX}}calendar_dates
        WHERE exception_type = 1 AND date BETWEEN min_date AND max_date

        EXCEPT

        SELECT source, service_id, date AS check_date
        FROM {{RAW_PREFIX}}calendar_dates
        WHERE exception_type = 2 AND date BETWEEN min_date AND max_date
    ),
    best_date_per_route AS (
        SELECT
            source,
            sane_route_id,
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
SELECT DISTINCT
    r.source AS source,
    arrayJoin(r.trip_ids) AS trip_id
FROM route_service_counts r
JOIN best_date_per_route b
    ON r.source = b.source AND r.sane_route_id = b.sane_route_id
JOIN valid_service_dates v
    ON r.source = v.source
    AND r.service_id = v.service_id
    AND v.check_date = b.best_date;

DROP TABLE IF EXISTS {{OUTPUT_PREFIX}}stop_times_one_day_sane;
CREATE TABLE {{OUTPUT_PREFIX}}stop_times_one_day_sane
ENGINE = MergeTree
ORDER BY (source, stop_lat, stop_lon, sane_route_id, trip_id, stop_sequence)
SETTINGS allow_nullable_key = 1
AS
SELECT
    st.source AS source,
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
