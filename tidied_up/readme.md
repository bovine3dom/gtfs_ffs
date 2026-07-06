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

## Set the staging root

```bash
julia --project=tidied_up tidied_up/orchestrate.jl 20260706 \
  --data-root /mnt/chungus/clickhouse_files/transitous \
  --fixture fantasy \
  --replace-sources
```

This stages into `/mnt/chungus/clickhouse_files/transitous/2026-07-06/source=fixture_fantasy/` and renders SQL that reads `/mnt/chungus/clickhouse_files/transitous/2026-07-06/source=*`.

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
