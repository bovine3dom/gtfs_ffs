--- making actual tables
select *, company from file('gtfs_hive/company=*/agency.txt', 'CSVWithNames') limit 1000 settings use_hive_partitioning = 1;
DESCRIBE TABLE file('gtfs_hive/company=*/agency.txt', 'CSVWithNames') settings use_hive_partitioning = 1;

DROP TABLE IF EXISTS gtfs_agency;
CREATE TABLE gtfs_agency
ENGINE = MergeTree
ORDER BY (company, agency_id, agency_name)
AS SELECT company, assumeNotNull(agency_id) agency_id, assumeNotNull(agency_name) agency_name, agency_url, agency_timezone, lower(agency_lang) agency_lang
FROM file('gtfs_hive/company=*/agency.txt', 'CSVWithNames')
SETTINGS use_hive_partitioning = 1;

DESCRIBE TABLE file('gtfs_hive/company=*/stops.txt', 'CSVWithNames') settings use_hive_partitioning = 1;

DROP TABLE IF EXISTS gtfs_stops;
CREATE TABLE gtfs_stops
ENGINE = MergeTree
ORDER BY (company, stop_lat, stop_lon, stop_id)
AS SELECT company, assumeNotNull(stop_lat) stop_lat, assumeNotNull(stop_lon) stop_lon, assumeNotNull(stop_id) stop_id, stop_name, stop_desc, zone_id, stop_url, location_type, parent_station
FROM file('gtfs_hive/company=*/stops.txt', 'CSVWithNames')
SETTINGS use_hive_partitioning = 1;



DESCRIBE TABLE file('gtfs_hive/company=*/routes.txt', 'CSVWithNames') settings use_hive_partitioning = 1;

-- route type docs here: https://developers.google.com/transit/gtfs/reference/extended-route-types ... basically < 200 is a train, 400 <= x < 500 is a metro, 900 <= 1000 is a tram, 1400 is a funicular
DROP TABLE IF EXISTS gtfs_routes;
CREATE TABLE gtfs_routes
ENGINE = MergeTree
ORDER BY (company, route_type, agency_id, route_id)
AS SELECT company, assumeNotNull(agency_id) agency_id, assumeNotNull(route_type) route_type, assumeNotNull(route_id) route_id, route_short_name, route_long_name, route_desc, route_url, route_color, route_text_color
FROM file('gtfs_hive/company=*/routes.txt', 'CSVWithNames')
SETTINGS use_hive_partitioning = 1;




DESCRIBE TABLE file('gtfs_hive/company=*/trips.txt', 'CSVWithNames') settings use_hive_partitioning = 1;
DROP TABLE IF EXISTS gtfs_trips;
CREATE TABLE gtfs_trips
ENGINE = MergeTree
ORDER BY (company, route_id, service_id, trip_id)
AS SELECT company, assumeNotNull(route_id) route_id, assumeNotNull(service_id) service_id, assumeNotNull(trip_id) trip_id, trip_headsign, direction_id, block_id, shape_id
FROM file('gtfs_hive/company=*/trips.txt', 'CSVWithNames', 'route_id String, service_id String, trip_id String, trip_headsign String, direction_id String, block_id String, shape_id String')
SETTINGS use_hive_partitioning = 1; -- fails to sniff swiss column types :(
DESCRIBE TABLE file('gtfs_hive/company=*/routes.txt', 'CSVWithNames') settings use_hive_partitioning = 1;


DESCRIBE TABLE file('gtfs_hive/company=*/stop_times.txt', 'CSVWithNames') settings use_hive_partitioning = 1;
DROP TABLE IF EXISTS gtfs_stop_times;
CREATE TABLE gtfs_stop_times
ENGINE = MergeTree
ORDER BY (company, trip_id, stop_id, departure_time, arrival_time)
AS SELECT company, assumeNotNull(trip_id) trip_id, assumeNotNull(stop_id) stop_id, assumeNotNull(parseDateTimeBestEffortOrNull(departure_time)) departure_time, assumeNotNull(parseDateTimeBestEffortOrNull(arrival_time)) arrival_time, stop_sequence, stop_headsign, pickup_type, drop_off_type, shape_dist_traveled
FROM file('gtfs_hive/company=*/stop_times.txt', 'CSVWithNames')
SETTINGS use_hive_partitioning = 1;

DESCRIBE TABLE file('gtfs_hive/company=*/calendar.txt', 'CSVWithNames', 'service_id String, monday String, tuesday String, wednesday String, thursday String, friday String, saturday String, sunday String, start_date String, end_date String') settings use_hive_partitioning = 1;
DROP TABLE IF EXISTS gtfs_calendar;
CREATE TABLE gtfs_calendar
ENGINE = MergeTree
ORDER BY (company, service_id, start_date, end_date, monday, tuesday, wednesday, thursday, friday, saturday, sunday)
AS SELECT company, service_id, assumeNotNull(parseDateTimeBestEffortOrNull(start_date)) start_date, assumeNotNull(parseDateTimeBestEffortOrNull(end_date)) end_date, toBool(monday) monday, toBool(tuesday) tuesday, toBool(wednesday) wednesday, toBool(thursday) thursday, toBool(friday) friday, toBool(saturday) saturday, toBool(sunday) sunday
FROM file('gtfs_hive/company=*/calendar.txt', 'CSVWithNames', 'service_id String, monday String, tuesday String, wednesday String, thursday String, friday String, saturday String, sunday String, start_date String, end_date String')
SETTINGS use_hive_partitioning = 1;



DESCRIBE TABLE file('gtfs_hive/company=*/calendar_dates.txt', 'CSVWithNames', 'service_id String, date String, exception_type UInt8') settings use_hive_partitioning = 1;
DROP TABLE IF EXISTS gtfs_calendar_dates;
CREATE TABLE gtfs_calendar_dates
ENGINE = MergeTree
ORDER BY (company, service_id, date, exception_type)
AS SELECT company, service_id, assumeNotNull(parseDateTimeBestEffortOrNull(date)) date, exception_type
FROM file('gtfs_hive/company=*/calendar_dates.txt', 'CSVWithNames', 'service_id String, date String, exception_type UInt8')
SETTINGS use_hive_partitioning = 1;


-- todo: import transfers etc following https://developers.google.com/transit/gtfs/examples/gtfs-feed and https://gtfs.org/documentation/schedule/reference/#calendar_datestxt
