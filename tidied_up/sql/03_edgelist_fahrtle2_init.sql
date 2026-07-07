SET receive_timeout = {{TIMEOUT_SECONDS}};
SET send_timeout = {{TIMEOUT_SECONDS}};
SET max_threads = 5;
SET max_execution_time = 0;
SET max_result_rows = 0;
SET max_result_bytes = 0;
SET max_bytes_before_external_group_by = '90G';
SET max_memory_usage = '100G';

DROP TABLE IF EXISTS {{OUTPUT_PREFIX}}edgelist_fahrtle2;
CREATE TABLE {{OUTPUT_PREFIX}}edgelist_fahrtle2
(
    source LowCardinality(String),
    trip_id String,
    sane_route_id UUID,
    h3 UInt64,
    next_h3 UInt64,
    stop_uuid Nullable(Int64),
    stop_lat Float64,
    stop_lon Float64,
    arrival_time DateTime,
    departure_time DateTime,
    stop_name String,
    next_stop Nullable(Int64),
    next_lat Float64,
    next_lon Float64,
    next_arrival DateTime,
    route_short_name String,
    route_long_name String,
    route_type UInt16,
    route_color LowCardinality(String),
    route_text_color LowCardinality(String),
    trip_headsign String,
    travel_time Int64,
    initial_stop Nullable(Int64),
    initial_lat Float64,
    initial_lon Float64,
    initial_arrival DateTime,
    initial_name String,
    final_stop Nullable(Int64),
    final_lat Float64,
    final_lon Float64,
    final_arrival DateTime,
    final_name String
)
ENGINE = MergeTree
ORDER BY (h3, departure_time, arrival_time)
SETTINGS allow_nullable_key = 1;
