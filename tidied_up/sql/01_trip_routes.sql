SET receive_timeout = {{TIMEOUT_SECONDS}};
SET send_timeout = {{TIMEOUT_SECONDS}};
SET max_threads = {{THREADS}};
SET max_insert_threads = 1;
SET max_execution_time = 0;
SET max_bytes_before_external_group_by = {{SPILL_BYTES}};
SET max_bytes_before_external_sort = {{SPILL_BYTES}};
SET max_memory_usage = {{MEMORY_BYTES}};

INSERT INTO {{RAW_PREFIX}}trip_route_uuids
SELECT
    source,
    trip_id,
    reinterpretAsUUID(sipHash128Reference(
        arrayMap(x -> x.2, arraySort(
            groupArray((departure_time, xxh3(stop_id)))
        ))
    )) AS sane_route_id
FROM {{RAW_PREFIX}}stop_times
WHERE sipHash64(source, trip_id) % {{TRIP_BUCKETS}} = {{BUCKET}}
GROUP BY source, trip_id;
