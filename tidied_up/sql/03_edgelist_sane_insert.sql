SET receive_timeout = {{TIMEOUT_SECONDS}};
SET send_timeout = {{TIMEOUT_SECONDS}};
SET max_threads = 5;
SET max_execution_time = 0;
SET max_result_rows = 0;
SET max_result_bytes = 0;
SET max_bytes_before_external_group_by = '90G';
SET max_memory_usage = '100G';

INSERT INTO {{OUTPUT_PREFIX}}edgelist_sane
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
        WHERE source = {{SOURCE_LITERAL}}
          AND source NOT LIKE 'us%'
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
