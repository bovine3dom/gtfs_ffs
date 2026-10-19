# tidied ingestion

The orchestrator now renders SQL from `tidied_up/sql/` into `tidied_up/temp/YYYYMMDD/`.
It does not download Transitous unless `--download-transitous` is passed explicitly.

## Dry run with the tiny fixture

```bash
julia --project=tidied_up tidied_up/orchestrate.jl 20260105 --fixture fantasy --replace-sources
```

This stages a tiny GTFS feed in `data/2026-01-05/source=fixture_fantasy/` and renders SQL only.

## Execute against ClickHouse

```bash
julia --project=tidied_up tidied_up/orchestrate.jl 20260105 --fixture fantasy --replace-sources --execute
```

The ClickHouse `file()` paths must be visible to the ClickHouse server. For remote servers, stage the files wherever ClickHouse can read them and pass that root with `--data-root`, or pass an existing dated root with `--input-root`.

Paths under `/mnt/chungus/clickhouse_files` are rendered for ClickHouse as `chungus/...`, because ClickHouse resolves `file('chungus/...')` inside its `user_files` sandbox. Override this mapping with `CLICKHOUSE_FILE_ROOT` and `CLICKHOUSE_FILE_PREFIX` if needed.

## Set the staging root

```bash
julia --project=tidied_up tidied_up/orchestrate.jl 20260706 \
  --data-root /mnt/chungus/clickhouse_files/transitous \
  --fixture fantasy \
  --replace-sources
```

This stages into `/mnt/chungus/clickhouse_files/transitous/2026-07-06/source=fixture_fantasy/` and renders SQL that reads `/mnt/chungus/clickhouse_files/transitous/2026-07-06/source=*`.
For ClickHouse, that path is rendered as `chungus/transitous/2026-07-06/source=*`.

## Add local non-Transitous sources

```bash
julia --project=tidied_up tidied_up/orchestrate.jl 20260105 \
  --source sncf=/path/to/sncf_gtfs.zip \
  --source local_bus=/path/to/extracted/gtfs \
  --replace-sources
```

Each source is copied into `DATA_ROOT/YYYY-MM-DD/source=name/`. Source names may contain letters, digits, `_`, `-`, and `.`. Source paths can be absolute, or relative to `--data-root`.

For already-staged sources under the same data root, this shorthand works:

```bash
julia --project=tidied_up tidied_up/orchestrate.jl 20260706 \
  --data-root /mnt/chungus/clickhouse_files/transitous \
  --source dfds_gtfs=2026-02-13/dfds_gtfs \
  --replace-sources
```

If `DATA_ROOT/2026-02-13/dfds_gtfs` does not exist, the orchestrator also tries `DATA_ROOT/2026-02-13/source=dfds_gtfs`.

For example, to download Transitous into the dated `/mnt/chungus` root and copy an older local DFDS source into the same new run:

```bash
julia --project=tidied_up tidied_up/orchestrate.jl 20260706 \
  --data-root /mnt/chungus/clickhouse_files/transitous \
  --download-transitous \
  --source dfds_gtfs=2026-02-13/dfds_gtfs \
  --replace-sources \
  --execute
```

That reads from `/mnt/chungus/clickhouse_files/transitous/2026-07-06/source=*` after staging/downloading.

The Transitous downloader only follows `.gtfs.zip` links. Before downloading a zip body, it checks `--data-root` for a same-named unchanged zip from any dated run and hardlinks/copies that into the new dated directory when possible. Unchanged means matching `Content-Length`, or matching saved `ETag`/`Last-Modified` metadata from a previous downloader run.

Very long Transitous names are shortened with a stable hash suffix before writing the local `.gtfs.zip` and `source=` directory. This avoids filesystem component length errors from long percent-encoded names.

The downloader writes a per-run status file at `DATA_ROOT/YYYY-MM-DD/.grabber-status.tsv` and prints a summary of `reuse`, `download_attempt`, `downloaded`, `failed`, and `skipped` rows. To debug or force a fresh network download, prefix the orchestrator command with `GTFS_FORCE_DOWNLOAD=1`.

If the zips are already present in the dated directory, skip all Transitous listing/download/reuse checks and only extract them:

```bash
julia --project=tidied_up tidied_up/orchestrate.jl 20260706 \
  --data-root /mnt/chungus/clickhouse_files/transitous \
  --extract-only \
  --execute
```

This reads existing `DATA_ROOT/YYYY-MM-DD/*.gtfs.zip`, extracts them to `source=*`, then continues the normal SQL pipeline.

## Trip-to-shape lookup

Raw ingestion adds and materializes the `trip_shape_lookup` projection on the
trips table. It stores `source`, `trip_id`, and `shape_id`, ordered by
`(source, trip_id)`. The ingestion process waits for materialization to finish.
ClickHouse can use this projection for covered trip-to-shape queries. Queries
that need other trip columns may still read the base table.

Existing tables are not changed by a code update. To add the projection without
repeating raw ingestion, run these statements in the intended database:

```sql
ALTER TABLE transitous_everything_20261018_trips
ADD PROJECTION IF NOT EXISTS trip_shape_lookup
(
    SELECT source, trip_id, shape_id
    ORDER BY (source, trip_id)
);

ALTER TABLE transitous_everything_20261018_trips
MATERIALIZE PROJECTION trip_shape_lookup
SETTINGS mutations_sync = 1;
```

Materialization uses background mutation resources and additional disk space.
Do not run it at the same time as a raw-table rebuild.

## Query memory

Queries use an 8 GiB memory limit and two threads. Aggregation and sorting can use temporary disk files above 2 GiB. Set `--memory-limit-gib`, `--spill-gib`, and `--threads` to change these limits. The spill threshold must be less than the query limit. Keep space available for temporary files and memory available for the server's background tasks.

Raw trips, stop times, and route-pattern IDs use 64 disk partitions, keyed by a hash of `(source, trip_id)`. Route-pattern calculation processes one partition at a time. Use `--trip-buckets N` to set 1–256 partitions when rebuilding raw tables.

All three edge builders also use `--trip-buckets`. Each insert selects one
hash bucket within one staging source before window functions, trip arrays,
or population expansion. A trip is never split between batches. Batches run
in sequence. One large source therefore no longer creates one large insert.

New `stop_times_one_day_even_saner2` tables have the same hash partitions.
Existing unpartitioned tables still work, but each batch can scan the table.
A different bucket count also works for edges, but can reduce partition pruning.
Do not change the raw bucket count in the middle of raw ingestion.

### Other memory costs

- Raw file loading, selectors, service-date expansion, and stop joins are not
  manually batched. They still use the query limits and spill settings.
- Stop statistics must combine results across trips. Do not append independent
  trip-bucket results to these tables. Two statistics queries now keep unique
  route stops, not one stop entry per departure. The terminal-distance query
  uses distinct route stops and an aggregate instead of a sorted stop-time array.
  Equal terminal times use stop sequence and trip ID as tie-breakers.
- The population join can still read the full population table in each batch.
  Other joins and distinct sets also need memory. Spilling is not a guarantee
  that a query will fit its memory limit.
- Stop clustering stores a union-find structure in Julia. The ClickHouse query
  limit does not limit Julia memory.
- Existing `us%` and `ca%` exclusions use the staging source name. They do not
  identify those original feeds inside a combined `source=nigiri` bundle.

This is not a full-scale memory benchmark. Monitor memory and temporary disk
space during a full import.

Run the tests with:

```sh
julia tidied_up/test/runtests.jl
# Use an isolated database on a local test server:
julia tidied_up/test/buckets.jl /path/to/clickhouse/user_files
```

The database test uses `CLICKHOUSE_*` connection settings and requires permission to create a temporary database.

## Resume or retry stages

The stage order is:

```text
raw -> stop-uuids -> fantasy-select -> fantasy-even-saner -> fantasy-edgelist-sane -> fantasy-stop-statistics -> fantasy-stop-statistics-unmerged2 -> fantasy-stop-statistics-unmerged3 -> fantasy-edgelist-fahrtle -> fantasy-edgelist-fahrtle2 -> real-select -> real-even-saner -> real-edgelist-sane -> real-stop-statistics -> real-stop-statistics-unmerged2 -> real-stop-statistics-unmerged3 -> real-edgelist-fahrtle -> real-edgelist-fahrtle2
```

`fantasy-postprocess` and `real-postprocess` are aliases for all postprocess table stages for that mode.

To resume after raw ingest and stop UUID generation have already succeeded:

```bash
julia --project=tidied_up tidied_up/orchestrate.jl 20260706 \
  --data-root /mnt/chungus/clickhouse_files/transitous \
  --start-at fantasy-select \
  --execute
```

To retry only the stage that failed in the fantasy postprocess:

```bash
julia --project=tidied_up tidied_up/orchestrate.jl 20260706 \
  --data-root /mnt/chungus/clickhouse_files/transitous \
  --only-stage fantasy-edgelist-sane \
  --execute
```

Each `edgelist-sane`, `edgelist-fahrtle`, and `edgelist-fahrtle2` stage first
recreates its output table. It then inserts each source and trip-hash bucket.
This applies to both timetable modes. Log filenames include the bucket number.
On failure, retry the whole stage. Do not run an individual failed insert again:
it can have written partial rows. A stage retry removes that partial output.
Previously completed stages remain unchanged. Do not run two imports with the
same database and table date at the same time.

If you only need the timetable/router output and not the population-weighted `edgelist_sane` table, skip it:

```bash
julia --project=tidied_up tidied_up/orchestrate.jl 20260706 \
  --data-root /mnt/chungus/clickhouse_files/transitous \
  --start-at fantasy-even-saner \
  --skip-edgelist-sane \
  --execute
```

When `raw` is skipped, input staging/download/extraction is skipped too. This means it is safe to keep old `--source` or `--download-transitous` flags in shell history while resuming from a later stage; they will not run unless `raw` is selected.

## Use an existing staged root

```bash
julia --project=tidied_up tidied_up/orchestrate.jl 20260105 \
  --input-root /clickhouse/user_files/chungus/transitous/2026-01-05/source=*
```

If `--input-root` does not include `source=*`, the orchestrator appends it.

## Timetable products

By default both products are rendered/built:

- fantasy: the day with the most active trips for each route pattern and original source. The table names do not change, for example `transitous_everything_20260105_edgelist_fahrtle2`.
- real: the day with the most active trips for each original source. The table names include `real`, for example `transitous_everything_20260105_real_edgelist_fahrtle2`.

Use `--mode fantasy`, `--mode real`, or `--mode both` to choose.

## Original sources and selected dates

Directory and ZIP staging keep `attributions.txt` from the Nigiri export.
Source rows use the ID `nigiri:<source>:<route>:<file-index>`. The `route_id`
column links each row to a route. `organization_name` contains the recorded
filename or path. This is a project convention, not a standard GTFS meaning
for organization names. Missing filenames use `unknown-source-<source>`.

The raw `attributions` table keeps credits, including the dataset-wide OSM row.
Only IDs with the `nigiri:` prefix define source groups. Ordinary organization
credits and the `osm` row do not define groups. Invalid or conflicting Nigiri
IDs cause an error. One source can have several file rows; these rows do not
increase the trip count.

The consumer also reads the old `route_sources.csv` and `sources.csv` files.
Supply both old files, or neither file. Raw tables with these names contain the
combined source mappings. New exports need only `attributions.txt`. If old and
new metadata disagree about a route's source, ingestion stops.

`source` remains the name of the staging directory. `origin_source` is the
original source ID from the export. Use both columns as the source key. For a
feed without source metadata, `origin_source` is an empty string. Source ID `0`
is not an empty string. SQL checks reject conflicting route mappings, missing
mappings for trips in a feed with metadata, and missing source file metadata.
Empty source metadata is equivalent to no metadata. Ordinary credits alone do
not identify original feeds. Do not remove source metadata from a combined export.

Each selection stage first builds `valid_service_dates`. It applies calendar
weekdays and date exceptions within `--min-date` and `--max-date`, inclusive.
The selector counts active trips, not active service IDs. If two dates have the
same count, it selects the earlier date. Fantasy mode makes a separate choice
for each route pattern within each original source.

Each output prefix has a `selected_days` table. It records:

- `source` and `origin_source`;
- `sane_route_id` in fantasy mode;
- `selected_date` and `active_trip_count`;
- `first_active_date` and `last_active_date` within the search limits;
- `search_start_date` and `search_end_date`;
- `status`: `selected` or `no_active_trips`.

Groups with trips but no active date have null dates and a zero count. The date
range is not the full feed coverage if the search limits exclude active dates.
`valid_trips_lookup` and `stop_times_one_day_sane` also keep `origin_source` and
`selected_date`. Later edge tables do not yet keep these columns.

Existing raw trip tables do not have `origin_source`. Rebuild raw tables in an
approved test database before you test these selectors. Do not resume selection
against the old raw schema.

The offline tests check metadata staging, ZIP staging, date limits, and SQL rendering.
They do not execute SQL. Database tests for trip counts, date exceptions, ties,
missing dates, and combined feeds still require an approved isolated database.
No database access is needed for:

```sh
julia --startup-file=no tidied_up/test/runtests.jl
```
