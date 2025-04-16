# messing with gtfs timetable data

named after the swiss railway company if anyone asks


1. grab data
```fish
cd data
set GTFS_URL https://eu.ftp.opendatasoft.com/sncf/plandata/export-ter-gtfs-last.zip
set GTFS_COMPANY ter
wget $GTFS_URL -O gtfs.zip && 7za -ocompany=$GTFS_COMPANY x gtfs.zip && rm gtfs.zip

set GTFS_URL https://eu.ftp.opendatasoft.com/sncf/plandata/export-intercites-gtfs-last.zip
set GTFS_COMPANY intercites
wget $GTFS_URL -O gtfs.zip && 7za -ocompany=$GTFS_COMPANY x gtfs.zip && rm gtfs.zip

set GTFS_URL https://eu.ftp.opendatasoft.com/sncf/plandata/export_gtfs_voyages.zip
set GTFS_COMPANY tgv
wget $GTFS_URL -O gtfs.zip && 7za -ocompany=$GTFS_COMPANY x gtfs.zip && rm gtfs.zip

set GTFS_URL https://data.opentransportdata.swiss/dataset/6cca1dfb-e53d-4da8-8d49-4797b3e768e3/resource/77171916-1fbf-4e1a-9f5c-a35bdb58ee9e/download/gtfs_fp2025_2025-04-14.zip
set GTFS_COMPANY swiss
wget $GTFS_URL -O gtfs.zip && 7za -ocompany=$GTFS_COMPANY x gtfs.zip && rm gtfs.zip

set GTFS_URL 'https://gtfs.irail.be/nmbs/gtfs/latest.zip'
set GTFS_COMPANY sncb
wget $GTFS_URL -O gtfs.zip && 7za -ocompany=$GTFS_COMPANY x gtfs.zip && rm gtfs.zip

set GTFS_URL 'https://github.com/thomasforth/ATOCCIF2GTFS/raw/refs/heads/master/ttis389_gtfs.zip' # this is six years old but i can't be bothered to make a newer one
set GTFS_COMPANY uk
wget $GTFS_URL -O gtfs.zip && 7za -ocompany=$GTFS_COMPANY x gtfs.zip && rm gtfs.zip

# ideally i would have more railways included here
# but it looks like e.g. each trenitalia region has their own feed
# and i have run out of stamina
```

<!-- i can't work out how this tool is supposed to work so probably don't do this
install transitland
```
sudo env GOBIN=/bin/ go install github.com/interline-io/transitland-lib/cmd/transitland@latest
```
-->

finding more feeds
```
git submodule init
cd transitland-atlas/
cat gtfs.irail.be.dmfr.json | jaq '.feeds | map(select(.urls?.static_current != null) | { id: .id, name: .name, url: .urls?.static_current })'
# you can be clever about this if you want but i got bored
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
SETTINGS use_hive_partitioning = 1

Query id: e49980c8-f60c-4a47-ad20-a4d69a7efb97

   ┌─sparkbar(24)(t, c)───────┬─company────┐
1. │ ▁▁▁▁▁▂▄▇▆▅▄▄▅▅▄▄▅▇█▇▅▃▂▁ │ ter        │
2. │ ▁  ▁▂▃▄▅▆▅▄▅▅▄▅▄▅▄▅█▇▅▄▃ │ intercites │
3. │ ▃▂▁▁▁▂▄▆▇▇▇▇▇▇▇▇▇▇█▇▇▇▇▆ │ uk         │
4. │ ▃▁▁▁▁▃▅▆▆▆▇▇▇▇▇▇█▇▇▆▅▄▄▄ │ swiss      │
5. │ ▁▁▁ ▁▂▄▅▅▆▆▆▇▆▇▆▆▇█▇▇▅▄▂ │ tgv        │
6. │ ▂▁▁▁▁▂▅▆▇▇█▇▇▇▇▇▇▇▇▇▇▆▅▄ │ sncb       │
   └──────────────────────────┴────────────┘

6 rows in set. Elapsed: 2.313 sec. Processed 20.02 million rows, 1.85 GB (8.65 million rows/s., 801.06 MB/s.)
Peak memory usage: 17.21 MiB.


WITH activity AS
    (
        SELECT
            company,
            count(*) AS c,
            toTime(tumbleStart(parseDateTimeBestEffortOrNull(departure_time), toIntervalHour(1))) AS t
        FROM file('../data/company=*/stop_times.txt', 'CSVWithNames')
        GROUP BY
            t,
            company
    )
SELECT
    company,
    arrayMap(q -> round(q, 2), quantiles(0.25, 0.5, 0.75)(c / peak)) AS q
FROM activity AS act
INNER JOIN
(
    SELECT
        max(c) AS peak,
        company
    FROM activity
    GROUP BY company
) AS pk ON act.company = pk.company
GROUP BY company
ORDER BY q[2] ASC
SETTINGS use_hive_partitioning = 1

Query id: 26a030a0-6801-4f23-b967-564b0367f577

   ┌─company────┬─q────────────────┐
1. │ ter        │ [0.14,0.53,0.68] │
2. │ intercites │ [0.42,0.57,0.66] │
3. │ tgv        │ [0.36,0.73,0.86] │
4. │ swiss      │ [0.47,0.76,0.89] │
5. │ sncb       │ [0.38,0.87,0.96] │
6. │ uk         │ [0.47,0.93,0.96] │
   └────────────┴──────────────────┘

6 rows in set. Elapsed: 4.741 sec. Processed 39.99 million rows, 2.50 GB (8.43 million rows/s., 527.27 MB/s.)
Peak memory usage: 37.37 MiB.
```
