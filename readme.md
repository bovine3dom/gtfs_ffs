# messing with gtfs timetable data

named after the swiss railway company if anyone asks


1. grab data
```sh
cd data
wget https://eu.ftp.opendatasoft.com/sncf/plandata/export-ter-gtfs-last.zip
7za -ocompany=ter x export-ter-gtfs-last.zip
rm export-ter-gtfs-last.zip
wget https://eu.ftp.opendatasoft.com/sncf/plandata/export-intercites-gtfs-last.zip
7za -ocompany=intercites x export-intercites-gtfs-last.zip
rm export-intercites-gtfs-last.zip
wget https://eu.ftp.opendatasoft.com/sncf/plandata/export_gtfs_voyages.zip
7za -ocompany=tgv x export_gtfs_voyages.zip
rm export_gtfs_voyages.zip
wget https://data.opentransportdata.swiss/dataset/6cca1dfb-e53d-4da8-8d49-4797b3e768e3/resource/77171916-1fbf-4e1a-9f5c-a35bdb58ee9e/download/gtfs_fp2025_2025-04-14.zip
7za -ocompany=swiss x gtfs_fp2025_2025-04-14.zip
rm gtfs_fp2025_2025-04-14.zip
```

2. mess with it with e.g. clickhouse-local

```sql
SELECT
    sparkbar(24)(t, c),
    company
FROM
(
    SELECT
        company,
        count(*) AS c,
        toTime(parseDateTimeBestEffortOrNull(departure_time)) AS t
    FROM file('../data/company=*/stop_times.txt', 'CSVWithNames')
    GROUP BY
        t,
        company
)
GROUP BY company

   ┌─sparkbar(24)(t, c)───────┬─company────┐
1. │ ▁▁▁▁▁▂▄▇▆▅▄▄▅▅▄▄▅▇█▇▅▃▂▁ │ ter        │
2. │ ▁  ▁▂▃▄▅▆▅▄▅▅▄▅▄▅▄▅█▇▅▄▃ │ intercites │
3. │ ▃▁▁▁▁▃▅▆▆▆▇▇▇▇▇▇█▇▇▆▅▄▄▄ │ swiss      │
4. │ ▁▁▁ ▁▂▄▅▅▆▆▆▇▆▇▆▆▇█▇▇▅▄▂ │ tgv        │
   └──────────────────────────┴────────────┘
```
