SET receive_timeout = {{TIMEOUT_SECONDS}};
SET send_timeout = {{TIMEOUT_SECONDS}};
SET max_threads = 5;
SET max_execution_time = 0;
SET max_result_rows = 0;
SET max_result_bytes = 0;
SET max_bytes_before_external_group_by = '90G';
SET max_memory_usage = '100G';

DROP TABLE IF EXISTS {{OUTPUT_PREFIX}}edgelist_sane;
CREATE TABLE {{OUTPUT_PREFIX}}edgelist_sane
(
    source LowCardinality(String),
    trip_id String,
    stop_uuid Nullable(Int64),
    stop_lat Float64,
    stop_lon Float64,
    departure_time DateTime,
    next_stop Nullable(Int64),
    next_arrival DateTime,
    travel_time Int64,
    next_pop Nullable(Float64),
    route_type UInt16,
    pop_per_minute Nullable(Float64)
)
ENGINE MergeTree
ORDER BY (stop_uuid, source, trip_id, next_stop, stop_lat, stop_lon, departure_time, next_pop, pop_per_minute)
SETTINGS allow_nullable_key = 1;
