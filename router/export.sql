-- Replace only the source table below when exporting another compatible snapshot.
-- departure_time is DateTime: clock extraction uses its effective ClickHouse timezone.
-- travel_time is Int64 MINUTES. Keep self-edges and invalid durations for loader validation.
SELECT DISTINCT
    h3ToParent(e.h3, 5) AS from_h3,
    h3ToParent(e.next_h3, 5) AS to_h3,
    toUInt32(
        (toUInt32(toHour(e.departure_time)) * 3600
         + toUInt32(toMinute(e.departure_time)) * 60
         + toUInt32(toSecond(e.departure_time))) * 1000
    ) AS departure_ms,
    toInt64(e.travel_time) * 60000 AS duration_ms
FROM transitous_everything_20260218_edgelist_fahrtle2 AS e
WHERE e.route_type = 2 OR e.route_type BETWEEN 100 AND 117
ORDER BY from_h3, to_h3, departure_ms, duration_ms
SETTINGS output_format_arrow_compression_method = 'none'
FORMAT Arrow;
