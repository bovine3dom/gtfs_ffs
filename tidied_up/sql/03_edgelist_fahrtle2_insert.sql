SET receive_timeout = {{TIMEOUT_SECONDS}};
SET send_timeout = {{TIMEOUT_SECONDS}};
SET max_threads = 5;
SET max_execution_time = 0;
SET max_result_rows = 0;
SET max_result_bytes = 0;
SET max_bytes_before_external_group_by = '90G';
SET max_memory_usage = '100G';

INSERT INTO {{OUTPUT_PREFIX}}edgelist_fahrtle2
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
    WHERE source = {{SOURCE_LITERAL}}
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
