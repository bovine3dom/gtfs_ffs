-- Rendered by tidied_up/orchestrate.jl.
-- Runs once per timetable mode. {{OUTPUT_PREFIX}} is silent for fantasy and *_real_* for real.

SET receive_timeout = {{TIMEOUT_SECONDS}};
SET send_timeout = {{TIMEOUT_SECONDS}};
SET max_threads = 5;
SET max_execution_time = 0;
SET max_result_rows = 0;
SET max_result_bytes = 0;
SET max_bytes_before_external_group_by = '90G';
SET max_memory_usage = '100G';

DROP TABLE IF EXISTS {{OUTPUT_PREFIX}}stop_times_one_day_even_saner2;
CREATE TABLE {{OUTPUT_PREFIX}}stop_times_one_day_even_saner2
ENGINE = MergeTree
ORDER BY (source, sane_route_id, trip_id, departure_time)
SETTINGS allow_nullable_key = 1
AS
SELECT
    st.*,
    geoToH3(st.stop_lat, st.stop_lon, 11) AS h3,
    tu.stop_uuid AS stop_uuid
FROM {{OUTPUT_PREFIX}}stop_times_one_day_sane st
LEFT JOIN {{STOP_UUIDS_TABLE}} tu
    ON tu.h3 = geoToH3(st.stop_lat, st.stop_lon, 11);

DROP TABLE IF EXISTS {{OUTPUT_PREFIX}}edgelist_sane;
CREATE TABLE {{OUTPUT_PREFIX}}edgelist_sane
ENGINE MergeTree
ORDER BY (stop_uuid, source, trip_id, next_stop, stop_lat, stop_lon, departure_time, next_pop, pop_per_minute)
SETTINGS allow_nullable_key = 1
AS
SELECT
    source,
    trip_id,
    stop_uuid,
    any(stop_lat) AS stop_lat,
    any(stop_lon) AS stop_lon,
    any(departure_time) AS departure_time,
    any(next_stop) AS next_stop,
    any(next_arrival) AS next_arrival,
    travel_time,
    sum(population) AS next_pop,
    any(route_type) AS route_type,
    next_pop / travel_time AS pop_per_minute
FROM
(
    SELECT
        source,
        trip_id,
        stop_uuid,
        stop_lat,
        stop_lon,
        departure_time,
        next_stop,
        next_arrival,
        dateDiff('minute', departure_time, next_arrival) AS travel_time,
        newh3,
        route_type,
        population
    FROM
    (
        SELECT
            lagInFrame(stop_uuid, 1, stop_uuid) OVER (PARTITION BY source, trip_id ORDER BY arrival_time DESC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS next_stop,
            lagInFrame(arrival_time, 1, arrival_time) OVER (PARTITION BY source, trip_id ORDER BY arrival_time DESC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS next_arrival,
            lagInFrame(h3_9, 1, h3_9) OVER (PARTITION BY source, trip_id ORDER BY arrival_time DESC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS next_h3_9,
            source,
            trip_id,
            stop_uuid,
            arrival_time,
            departure_time,
            stop_lat,
            stop_lon,
            route_type,
            geoToH3(stop_lat, stop_lon, 9) AS h3_9
        FROM {{OUTPUT_PREFIX}}stop_times_one_day_even_saner2
        WHERE source NOT LIKE 'us%'
          AND source NOT LIKE 'ca%'
    ) st
    ARRAY JOIN h3kRing(h3_9, 4) AS newh3
    LEFT JOIN population_h3 pop ON newh3 = pop.h3
    WHERE next_stop != stop_uuid
      AND NOT has(h3kRing(next_h3_9, 4), newh3)
      AND res = 9
)
WHERE travel_time > 0
GROUP BY ALL;

DROP TABLE IF EXISTS {{OUTPUT_PREFIX}}stop_statistics;
CREATE TABLE {{OUTPUT_PREFIX}}stop_statistics
ENGINE = MergeTree
ORDER BY (stop_lat, stop_lon, stop_uuid)
SETTINGS allow_nullable_key = 1
AS
SELECT
    bs.crow_km AS crow_km,
    st.stop_uuid AS stop_uuid,
    st.stop_lat AS stop_lat,
    st.stop_lon AS stop_lon,
    st.stop_name AS stop_name,
    st.route_type AS route_type
FROM
(
    SELECT
        max(crow_km) AS crow_km,
        stop_uuid
    FROM
    (
        SELECT
            source,
            sane_route_id,
            geoDistance(max(stop_lat), max(stop_lon), min(stop_lat), min(stop_lon)) / 1000 AS crow_km,
            groupArray(stop_uuid) AS stops
        FROM {{OUTPUT_PREFIX}}stop_times_one_day_even_saner2
        GROUP BY source, sane_route_id
    )
    ARRAY JOIN stops AS stop_uuid
    GROUP BY stop_uuid
) bs
LEFT JOIN
(
    SELECT
        stop_uuid,
        argMin(stop_lat, (route_priority_rank, -departure_count)) AS stop_lat,
        argMin(stop_lon, (route_priority_rank, -departure_count)) AS stop_lon,
        argMin(stop_name, (route_priority_rank, -departure_count)) AS stop_name,
        argMin(route_type, (route_priority_rank, -departure_count)) AS route_type
    FROM
    (
        SELECT
            stop_uuid,
            stop_lat,
            stop_lon,
            stop_name,
            route_type,
            count() AS departure_count,
            multiIf(
                route_type = 1100, 1,
                route_type IN (4, 1000, 1200), 2,
                route_type IN (2, 12) OR (route_type >= 100 AND route_type <= 117), 3,
                route_type = 1 OR (route_type >= 400 AND route_type <= 405), 4,
                route_type IN (0, 5) OR (route_type >= 900 AND route_type <= 906), 5,
                route_type IN (6, 7, 1400) OR (route_type >= 1300 AND route_type <= 1307), 6,
                route_type >= 200 AND route_type <= 209, 7,
                route_type = 11 OR route_type = 800, 8,
                route_type = 3 OR (route_type >= 700 AND route_type <= 716), 9,
                999
            ) AS route_priority_rank
        FROM {{OUTPUT_PREFIX}}stop_times_one_day_even_saner2
        GROUP BY stop_uuid, stop_lat, stop_lon, stop_name, route_type
    )
    GROUP BY stop_uuid
) st ON st.stop_uuid = bs.stop_uuid;

DROP TABLE IF EXISTS {{OUTPUT_PREFIX}}stop_statistics_unmerged2;
CREATE TABLE {{OUTPUT_PREFIX}}stop_statistics_unmerged2
ENGINE = MergeTree
ORDER BY (stop_lat, stop_lon, crow_km, stop_uuid)
SETTINGS allow_nullable_key = 1
AS
SELECT
    bs.crow_km AS crow_km,
    bs.stop_id AS stop_id,
    bs.source AS source,
    bs.stop_uuid AS stop_uuid,
    st.stop_lat AS stop_lat,
    st.stop_lon AS stop_lon,
    st.stop_name AS stop_name,
    st.route_type AS route_type
FROM
(
    SELECT
        max(crow_km) AS crow_km,
        stop_id_tuple.1 AS stop_id,
        stop_id_tuple.2 AS source,
        stop_id_tuple.3 AS stop_uuid
    FROM
    (
        SELECT
            sane_route_id,
            crow_km,
            arrayJoin(stops) AS stop_id_tuple
        FROM
        (
            SELECT
                source,
                sane_route_id,
                geoDistance(max(stop_lat), max(stop_lon), min(stop_lat), min(stop_lon)) / 1000 AS crow_km,
                groupArray((stop_id, source, stop_uuid)) AS stops
            FROM {{OUTPUT_PREFIX}}stop_times_one_day_even_saner2
            GROUP BY source, sane_route_id
        )
    )
    GROUP BY stop_id_tuple
) bs
LEFT JOIN
(
    SELECT
        source,
        stop_id,
        stop_uuid,
        any(stop_lat) AS stop_lat,
        any(stop_lon) AS stop_lon,
        any(stop_name) AS stop_name,
        argMin(route_type, multiIf(
            route_type = 1100, 1,
            route_type IN (4, 1000, 1200), 2,
            route_type IN (2, 12) OR (route_type >= 100 AND route_type <= 117), 3,
            route_type = 1 OR (route_type >= 400 AND route_type <= 405), 4,
            route_type IN (0, 5) OR (route_type >= 900 AND route_type <= 906), 5,
            route_type IN (6, 7, 1400) OR (route_type >= 1300 AND route_type <= 1307), 6,
            route_type >= 200 AND route_type <= 209, 7,
            route_type = 11 OR route_type = 800, 8,
            route_type = 3 OR (route_type >= 700 AND route_type <= 716), 9,
            999
        )) AS route_type
    FROM {{OUTPUT_PREFIX}}stop_times_one_day_even_saner2
    GROUP BY source, stop_id, stop_uuid
) st ON st.source = bs.source AND st.stop_id = bs.stop_id AND st.stop_uuid = bs.stop_uuid;

DROP TABLE IF EXISTS {{OUTPUT_PREFIX}}stop_statistics_unmerged3;
CREATE TABLE {{OUTPUT_PREFIX}}stop_statistics_unmerged3
ENGINE = MergeTree
ORDER BY (stop_lat, stop_lon, crow_km, stop_uuid)
SETTINGS allow_nullable_key = 1
AS
SELECT
    bs.crow_km AS crow_km,
    bs.stop_id AS stop_id,
    bs.source AS source,
    bs.stop_uuid AS stop_uuid,
    st.stop_lat AS stop_lat,
    st.stop_lon AS stop_lon,
    st.stop_name AS stop_name,
    st.route_type AS route_type
FROM
(
    SELECT
        max(dist_to_end_km) AS crow_km,
        stop_id,
        source,
        stop_uuid
    FROM
    (
        SELECT
            sane_route_id,
            s_tuple.1 AS stop_id,
            s_tuple.2 AS source,
            s_tuple.3 AS stop_uuid,
            geoDistance(s_tuple.4, s_tuple.5, term_lat, term_lon) / 1000 AS dist_to_end_km
        FROM
        (
            SELECT
                source,
                sane_route_id,
                arraySort(t -> t.6, groupArray((stop_id, source, stop_uuid, stop_lat, stop_lon, arrival_time))) AS stops_sorted,
                stops_sorted[-1].4 AS term_lat,
                stops_sorted[-1].5 AS term_lon
            FROM {{OUTPUT_PREFIX}}stop_times_one_day_even_saner2
            GROUP BY source, sane_route_id
        )
        ARRAY JOIN stops_sorted AS s_tuple
    )
    GROUP BY stop_id, source, stop_uuid
) bs
LEFT JOIN
(
    SELECT
        source,
        stop_id,
        stop_uuid,
        any(stop_lat) AS stop_lat,
        any(stop_lon) AS stop_lon,
        any(stop_name) AS stop_name,
        argMin(route_type, multiIf(
            route_type = 1100, 1,
            route_type IN (4, 1000, 1200), 2,
            route_type IN (2, 12) OR (route_type >= 100 AND route_type <= 117), 3,
            route_type = 1 OR (route_type >= 400 AND route_type <= 405), 4,
            route_type IN (0, 5) OR (route_type >= 900 AND route_type <= 906), 5,
            route_type IN (6, 7, 1400) OR (route_type >= 1300 AND route_type <= 1307), 6,
            route_type >= 200 AND route_type <= 209, 7,
            route_type = 11 OR route_type = 800, 8,
            route_type = 3 OR (route_type >= 700 AND route_type <= 716), 9,
            999
        )) AS route_type
    FROM {{OUTPUT_PREFIX}}stop_times_one_day_even_saner2
    GROUP BY source, stop_id, stop_uuid
) st ON st.source = bs.source AND st.stop_id = bs.stop_id AND st.stop_uuid = bs.stop_uuid;

DROP TABLE IF EXISTS {{OUTPUT_PREFIX}}edgelist_fahrtle;
CREATE TABLE {{OUTPUT_PREFIX}}edgelist_fahrtle
ENGINE MergeTree
ORDER BY (h3, source, stop_uuid, sane_route_id, stop_lat, stop_lon, trip_id, arrival_time, departure_time)
SETTINGS allow_nullable_key = 1
AS
SELECT
    source,
    trip_id,
    stop_uuid,
    any(stop_lat) AS stop_lat,
    any(arrival_time) AS arrival_time,
    any(stop_lon) AS stop_lon,
    geoToH3(stop_lat, stop_lon, 11) AS h3,
    geoToH3(next_lat, next_lon, 11) AS next_h3,
    any(departure_time) AS departure_time,
    any(next_stop) AS next_stop,
    any(next_arrival) AS next_arrival,
    any(next_lat) AS next_lat,
    any(next_lon) AS next_lon,
    any(final_stop) AS final_stop,
    any(final_arrival) AS final_arrival,
    any(final_lat) AS final_lat,
    any(final_lon) AS final_lon,
    any(final_name) AS final_name,
    any(initial_stop) AS initial_stop,
    any(initial_arrival) AS initial_arrival,
    any(initial_lat) AS initial_lat,
    any(initial_lon) AS initial_lon,
    any(initial_name) AS initial_name,
    travel_time,
    any(route_type) AS route_type,
    any(stop_name) AS stop_name,
    any(route_short_name) AS route_short_name,
    any(route_long_name) AS route_long_name,
    any(trip_headsign) AS trip_headsign,
    any(sane_route_id) AS sane_route_id,
    any(route_color) AS route_color,
    any(route_text_color) AS route_text_color
FROM
(
    SELECT
        source,
        trip_id,
        stop_uuid,
        stop_lat,
        stop_lon,
        departure_time,
        next_stop,
        next_arrival,
        next_lat,
        next_lon,
        arrival_time,
        final_stop,
        final_arrival,
        final_lat,
        final_lon,
        final_name,
        initial_stop,
        initial_arrival,
        initial_lat,
        initial_lon,
        initial_name,
        dateDiff('minute', departure_time, next_arrival) AS travel_time,
        stop_name,
        route_short_name,
        route_long_name,
        trip_headsign,
        sane_route_id,
        route_color,
        route_text_color,
        route_type
    FROM
    (
        SELECT
            lagInFrame(stop_uuid, 1, stop_uuid) OVER (PARTITION BY source, trip_id ORDER BY arrival_time DESC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS next_stop,
            lagInFrame(arrival_time, 1, arrival_time) OVER (PARTITION BY source, trip_id ORDER BY arrival_time DESC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS next_arrival,
            lagInFrame(stop_lat, 1, stop_lat) OVER (PARTITION BY source, trip_id ORDER BY arrival_time DESC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS next_lat,
            lagInFrame(stop_lon, 1, stop_lon) OVER (PARTITION BY source, trip_id ORDER BY arrival_time DESC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS next_lon,
            first_value(stop_uuid) OVER (PARTITION BY source, trip_id ORDER BY arrival_time DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS final_stop,
            first_value(arrival_time) OVER (PARTITION BY source, trip_id ORDER BY arrival_time DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS final_arrival,
            first_value(stop_lat) OVER (PARTITION BY source, trip_id ORDER BY arrival_time DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS final_lat,
            first_value(stop_lon) OVER (PARTITION BY source, trip_id ORDER BY arrival_time DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS final_lon,
            first_value(stop_name) OVER (PARTITION BY source, trip_id ORDER BY arrival_time DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS final_name,
            last_value(stop_uuid) OVER (PARTITION BY source, trip_id ORDER BY arrival_time DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS initial_stop,
            last_value(arrival_time) OVER (PARTITION BY source, trip_id ORDER BY arrival_time DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS initial_arrival,
            last_value(stop_lat) OVER (PARTITION BY source, trip_id ORDER BY arrival_time DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS initial_lat,
            last_value(stop_lon) OVER (PARTITION BY source, trip_id ORDER BY arrival_time DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS initial_lon,
            last_value(stop_name) OVER (PARTITION BY source, trip_id ORDER BY arrival_time DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS initial_name,
            source,
            trip_id,
            stop_uuid,
            arrival_time,
            departure_time,
            stop_lat,
            stop_lon,
            route_type,
            stop_name,
            route_short_name,
            route_long_name,
            trip_headsign,
            sane_route_id,
            route_color,
            route_text_color
        FROM {{OUTPUT_PREFIX}}stop_times_one_day_even_saner2
    ) st
    WHERE next_stop != stop_uuid
)
WHERE travel_time > 0
GROUP BY ALL
SETTINGS receive_timeout = {{TIMEOUT_SECONDS}};

DROP TABLE IF EXISTS {{OUTPUT_PREFIX}}edgelist_fahrtle2;
CREATE TABLE {{OUTPUT_PREFIX}}edgelist_fahrtle2
ENGINE = MergeTree
ORDER BY (h3, departure_time, arrival_time)
SETTINGS allow_nullable_key = 1
AS
WITH trip_data AS (
    SELECT
        source,
        trip_id,
        sane_route_id,
        any(route_short_name) AS route_short_name,
        any(route_long_name) AS route_long_name,
        any(route_type) AS route_type,
        any(route_color) AS route_color,
        any(route_text_color) AS route_text_color,
        any(trip_headsign) AS trip_headsign,
        arraySort(x -> x.1, groupArray(
            (
                if(toUInt32(arrival_time) > 0, arrival_time, departure_time),
                stop_sequence,
                stop_uuid,
                stop_lat,
                stop_lon,
                arrival_time,
                departure_time,
                stop_name
            )
        )) AS stops
    FROM {{OUTPUT_PREFIX}}stop_times_one_day_even_saner2
    GROUP BY source, trip_id, sane_route_id
)
SELECT
    source,
    trip_id,
    sane_route_id,
    geoToH3(curr_stop.4, curr_stop.5, 11) AS h3,
    geoToH3(next_stop_arr.4, next_stop_arr.5, 11) AS next_h3,
    curr_stop.3 AS stop_uuid,
    curr_stop.4 AS stop_lat,
    curr_stop.5 AS stop_lon,
    curr_stop.6 AS arrival_time,
    curr_stop.7 AS departure_time,
    curr_stop.8 AS stop_name,
    next_stop_arr.3 AS next_stop,
    next_stop_arr.4 AS next_lat,
    next_stop_arr.5 AS next_lon,
    next_stop_arr.6 AS next_arrival,
    route_short_name,
    route_long_name,
    route_type,
    route_color,
    route_text_color,
    trip_headsign,
    dateDiff('minute', curr_stop.7, next_stop_arr.6) AS travel_time,
    stops[1].3 AS initial_stop,
    stops[1].4 AS initial_lat,
    stops[1].5 AS initial_lon,
    stops[1].6 AS initial_arrival,
    stops[1].8 AS initial_name,
    stops[-1].3 AS final_stop,
    stops[-1].4 AS final_lat,
    stops[-1].5 AS final_lon,
    stops[-1].6 AS final_arrival,
    stops[-1].8 AS final_name
FROM trip_data
ARRAY JOIN
    arraySlice(stops, 1, length(stops) - 1) AS curr_stop,
    arraySlice(stops, 2) AS next_stop_arr;
