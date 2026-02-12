-- clickhouse
select * from transitous_everything_20260117_edgelist_fahrtle where source = 'fr_horaires-sncf.gtfs' and trip_id like '%OUI%' limit 1
# departure_time, next_arrival, stop_{lat, lon}, next_{lat, lon}
# avg(speed) group by (stop_{lat, lon}, next_{lat, lon})

-- rounded to avoid noise in platforms
SELECT
    round(if((stop_lat, stop_lon) < (next_lat, next_lon), stop_lat, next_lat), 3) AS start_lat,
    round(if((stop_lat, stop_lon) < (next_lat, next_lon), stop_lon, next_lon), 3) AS start_lon,
    round(if((stop_lat, stop_lon) < (next_lat, next_lon), next_lat, stop_lat), 3) AS finish_lat,
    round(if((stop_lat, stop_lon) < (next_lat, next_lon), next_lon, stop_lon), 3) AS finish_lon,
    avg(
        geoDistance(stop_lon, stop_lat, next_lon, next_lat) / 
        NULLIF(dateDiff('second', toDateTime(departure_time), toDateTime(next_arrival)), 0)
    ) * 3.6 AS avg_speed_kmh,
    count() as count
FROM transitous_everything_20260117_edgelist_fahrtle
WHERE source = 'fr_horaires-sncf.gtfs'
  AND trip_id LIKE '%OUI%'
  AND next_arrival > departure_time 
GROUP BY 
    start_lat, start_lon, finish_lat, finish_lon
HAVING avg_speed_kmh < 120
ORDER BY avg_speed_kmh ASC
INTO OUTFILE 'trains_a_moindre_vitesse.csv' TRUNCATE FORMAT CSVWithNames;

SELECT
    round(if((stop_lat, stop_lon) < (next_lat, next_lon), stop_lat, next_lat), 3) AS start_lat,
    round(if((stop_lat, stop_lon) < (next_lat, next_lon), stop_lon, next_lon), 3) AS start_lon,
    round(if((stop_lat, stop_lon) < (next_lat, next_lon), next_lat, stop_lat), 3) AS finish_lat,
    round(if((stop_lat, stop_lon) < (next_lat, next_lon), next_lon, stop_lon), 3) AS finish_lon,
    quantile(0.99)( 
        geoDistance(stop_lon, stop_lat, next_lon, next_lat) / 
        NULLIF(dateDiff('second', toDateTime(departure_time), toDateTime(next_arrival)), 0)
    ) * 3.6 AS 99th_percentile_speed_kmh,
    count() as count
FROM transitous_everything_20260117_edgelist_fahrtle
WHERE TRUE -- source = 'fr_horaires-sncf.gtfs'
  -- AND trip_id LIKE '%OUI%'
  AND next_arrival > departure_time 
  AND ( route_type between 100 and 199 or route_type = 2 )
GROUP BY 
    start_lat, start_lon, finish_lat, finish_lon
ORDER BY 99th_percentile_speed_kmh ASC
INTO OUTFILE 'trains_a_vitesse.csv' TRUNCATE FORMAT CSVWithNames;
