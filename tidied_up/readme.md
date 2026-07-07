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

The heavy edge stages `fantasy-edgelist-sane`, `fantasy-edgelist-fahrtle2`, `real-edgelist-sane`, and `real-edgelist-fahrtle2` are executed as one table-initialisation query plus one insert query per source. That keeps each query smaller and makes it clearer which source is slow or failing.

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

- fantasy: best day per unique route within source. This keeps the historical silent names like `transitous_everything_20260105_edgelist_fahrtle2`.
- real: best coherent calendar day per source. These tables are explicit, for example `transitous_everything_20260105_real_edgelist_fahrtle2`.

Use `--mode fantasy`, `--mode real`, or `--mode both` to choose.
