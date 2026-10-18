-- Rendered by tidied_up/orchestrate.jl.
-- Active service dates within the inclusive search limits.

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

DROP TABLE IF EXISTS {{OUTPUT_PREFIX}}valid_service_dates;
CREATE TABLE {{OUTPUT_PREFIX}}valid_service_dates
ENGINE = MergeTree
ORDER BY (source, service_id, check_date)
AS
WITH
    toDate('{{MIN_DATE}}') AS min_date,
    toDate('{{MAX_DATE}}') AS max_date
SELECT DISTINCT source, service_id, check_date
FROM (
    SELECT source, service_id, check_date
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
)
WHERE (source, service_id, check_date) NOT IN (
    SELECT source, service_id, date
    FROM {{RAW_PREFIX}}calendar_dates
    WHERE exception_type = 2 AND date BETWEEN min_date AND max_date
);
