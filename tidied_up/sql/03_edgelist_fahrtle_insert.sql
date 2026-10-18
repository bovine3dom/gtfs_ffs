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

INSERT INTO {{OUTPUT_PREFIX}}edgelist_fahrtle
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
            lagInFrame(stop_uuid, 1, stop_uuid) OVER (PARTITION BY source, trip_id ORDER BY stop_sequence DESC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS next_stop,
            lagInFrame(arrival_time, 1, arrival_time) OVER (PARTITION BY source, trip_id ORDER BY stop_sequence DESC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS next_arrival,
            lagInFrame(stop_lat, 1, stop_lat) OVER (PARTITION BY source, trip_id ORDER BY stop_sequence DESC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS next_lat,
            lagInFrame(stop_lon, 1, stop_lon) OVER (PARTITION BY source, trip_id ORDER BY stop_sequence DESC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS next_lon,
            first_value(stop_uuid) OVER (PARTITION BY source, trip_id ORDER BY stop_sequence DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS final_stop,
            first_value(arrival_time) OVER (PARTITION BY source, trip_id ORDER BY stop_sequence DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS final_arrival,
            first_value(stop_lat) OVER (PARTITION BY source, trip_id ORDER BY stop_sequence DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS final_lat,
            first_value(stop_lon) OVER (PARTITION BY source, trip_id ORDER BY stop_sequence DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS final_lon,
            first_value(stop_name) OVER (PARTITION BY source, trip_id ORDER BY stop_sequence DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS final_name,
            last_value(stop_uuid) OVER (PARTITION BY source, trip_id ORDER BY stop_sequence DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS initial_stop,
            last_value(arrival_time) OVER (PARTITION BY source, trip_id ORDER BY stop_sequence DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS initial_arrival,
            last_value(stop_lat) OVER (PARTITION BY source, trip_id ORDER BY stop_sequence DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS initial_lat,
            last_value(stop_lon) OVER (PARTITION BY source, trip_id ORDER BY stop_sequence DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS initial_lon,
            last_value(stop_name) OVER (PARTITION BY source, trip_id ORDER BY stop_sequence DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS initial_name,
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
        WHERE source = {{SOURCE_LITERAL}}
          AND sipHash64(source, trip_id) % {{TRIP_BUCKETS}} = {{BUCKET}}
    ) st
    WHERE next_stop != stop_uuid
)
WHERE travel_time > 0
GROUP BY ALL
SETTINGS receive_timeout = {{TIMEOUT_SECONDS}};
