SET receive_timeout = {{TIMEOUT_SECONDS}};
SET send_timeout = {{TIMEOUT_SECONDS}};
SET max_threads = {{THREADS}};
SET max_insert_threads = 1;
SET max_execution_time = 0;
SET max_bytes_before_external_group_by = {{SPILL_BYTES}};
SET max_bytes_before_external_sort = {{SPILL_BYTES}};
SET max_memory_usage = {{MEMORY_BYTES}};

DROP TABLE IF EXISTS {{OUTPUT_PREFIX}}edgelist_fahrtle;
CREATE TABLE {{OUTPUT_PREFIX}}edgelist_fahrtle
(
    source LowCardinality(String),
    trip_id String,
    stop_uuid Nullable(Int64),
    stop_lat Float64,
    arrival_time DateTime,
    stop_lon Float64,
    h3 UInt64,
    next_h3 UInt64,
    departure_time DateTime,
    next_stop Nullable(Int64),
    next_arrival DateTime,
    next_lat Float64,
    next_lon Float64,
    final_stop Nullable(Int64),
    final_arrival DateTime,
    final_lat Float64,
    final_lon Float64,
    final_name String,
    initial_stop Nullable(Int64),
    initial_arrival DateTime,
    initial_lat Float64,
    initial_lon Float64,
    initial_name String,
    travel_time Int64,
    route_type UInt16,
    stop_name String,
    route_short_name String,
    route_long_name String,
    trip_headsign String,
    sane_route_id UUID,
    route_color LowCardinality(String),
    route_text_color LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (h3, source, stop_uuid, sane_route_id, stop_lat, stop_lon, trip_id, arrival_time, departure_time)
SETTINGS allow_nullable_key = 1;
