-- clickhouse-local v25+
-- select min(YYYYMMDDToDate(toUInt32(date))) from file('../data/*/calendar_dates.txt', 'CSVWithNames') -- oops these are just exceptions. who cares

select sparkbar(24)(t, c), company from (select company, count(*) c, toTime(parseDateTimeBestEffortOrNull(departure_time)) t from file('../data/company=*/stop_times.txt', 'CSVWithNames') group by t, company) group by company
