#!/bin/bash
set -euo pipefail

# Requires BASE_URL. Optionally accepts DATA_ROOT, which should be the directory
# containing dated Transitous snapshots, e.g. /mnt/chungus/clickhouse_files/transitous.
NUM_PARALLEL="${NUM_PARALLEL:-3}"
EXTRACT_ONLY="${GTFS_EXTRACT_ONLY:-0}"

if [ "$EXTRACT_ONLY" != "1" ] && [ -z "${BASE_URL:-}" ]; then
  echo "BASE_URL is required" >&2
  exit 1
fi

if [ -z "${1:-}" ]; then
  DEST="$(pwd)"
else
  DEST="$1"
fi

mkdir -p "$DEST"
DEST="$(cd "$DEST" && pwd)"

if [ -z "${DATA_ROOT:-}" ]; then
  DATA_ROOT="$(dirname "$DEST")"
fi
mkdir -p "$DATA_ROOT"
DATA_ROOT="$(cd "$DATA_ROOT" && pwd)"

INDEX_FILE="$(mktemp)"
LINKS_FILE="$(mktemp)"
trap 'rm -f "$INDEX_FILE" "$LINKS_FILE"' EXIT
STATUS_FILE="$DEST/.grabber-status.tsv"
rm -f "$STATUS_FILE"
touch "$STATUS_FILE"

if [ "$EXTRACT_ONLY" != "1" ]; then
  curl -fsSL "$BASE_URL" > "$INDEX_FILE"

  python3 - "$BASE_URL" "$INDEX_FILE" > "$LINKS_FILE" <<'PY'
import re
import sys
from urllib.parse import urljoin, urlparse

base_url, index_file = sys.argv[1], sys.argv[2]
html = open(index_file, encoding="utf-8", errors="replace").read()
urls = set()

for href in re.findall(r'''href\s*=\s*["']([^"']+)["']''', html, flags=re.I):
    href = href.strip()
    url = urljoin(base_url, href)
    if urlparse(url).path.endswith(".gtfs.zip"):
        urls.add(url)

for raw_url in re.findall(r'''https?://[^\s"'<>]+''', html, flags=re.I):
    raw_url = raw_url.strip()
    if urlparse(raw_url).path.endswith(".gtfs.zip"):
        urls.add(raw_url)

for url in sorted(urls):
    print(url)
PY

  if [ ! -s "$LINKS_FILE" ]; then
    echo "No .gtfs.zip links found at $BASE_URL" >&2
    exit 1
  fi

  echo "Found $(wc -l < "$LINKS_FILE") .gtfs.zip links at $BASE_URL"
  echo "Destination: $DEST"
  echo "Reuse search root: $DATA_ROOT"
  if [ "${GTFS_FORCE_DOWNLOAD:-0}" = "1" ]; then
    echo "GTFS_FORCE_DOWNLOAD=1: reuse checks disabled"
  fi
else
  echo "GTFS_EXTRACT_ONLY=1: skipping Transitous listing, reuse checks, and downloads"
  echo "Destination: $DEST"
fi

stat_size() {
  stat -c '%s' "$1" 2>/dev/null || stat -f '%z' "$1"
}

header_value() {
  local name="$1"
  awk -v name="$name" '
    BEGIN { IGNORECASE = 1 }
    index($0, name ":") == 1 {
      sub(/^[^:]+:[[:space:]]*/, "")
      sub(/\r$/, "")
      value = $0
    }
    END { print value }
  '
}

meta_value() {
  local file="$1"
  local key="$2"
  [ -f "$file" ] || return 0
  awk -F= -v key="$key" '$1 == key { sub(/^[^=]+=/, ""); print; exit }' "$file"
}

trim_url() {
  local value="$1"
  value="${value//$'\r'/}"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

safe_component() {
  local raw="$1"
  local suffix="${2:-}"
  local max_bytes="${3:-180}"
  python3 - "$raw" "$suffix" "$max_bytes" <<'PY'
import hashlib
import re
import sys

raw, suffix, max_bytes_s = sys.argv[1], sys.argv[2], sys.argv[3]
max_bytes = int(max_bytes_s)
safe = re.sub(r"[^A-Za-z0-9._%+=@-]", "_", raw.strip())
safe = safe or "feed"

if len(safe.encode("utf-8")) <= max_bytes:
    print(safe)
    raise SystemExit

digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
if suffix and safe.endswith(suffix):
    stem = safe[:-len(suffix)]
else:
    stem = safe
    suffix = ""

hash_suffix = f"_{digest}{suffix}"
budget = max(1, max_bytes - len(hash_suffix.encode("utf-8")))
prefix = stem.encode("utf-8")[:budget].decode("utf-8", "ignore").rstrip("._-%+=@-")
print((prefix or "feed") + hash_suffix)
PY
}

record_status() {
  local status="$1"
  local filename="$2"
  local detail="${3:-}"
  printf '%s\t%s\t%s\n' "$status" "$filename" "$detail" >> "$STATUS_FILE"
}

find_unchanged_zip() {
  local filename="$1"
  local content_length="$2"
  local etag="$3"
  local last_modified="$4"
  local candidate
  local size
  local meta
  local old_etag
  local old_last_modified

  while IFS= read -r -d '' candidate; do
    [ -f "$candidate" ] || continue

    if [ -n "$content_length" ]; then
      size="$(stat_size "$candidate")"
      if [ "$size" = "$content_length" ]; then
        printf '%s\n' "$candidate"
        return 0
      fi
    fi

    meta="$candidate.meta"
    if [ -f "$meta" ]; then
      old_etag="$(meta_value "$meta" etag)"
      old_last_modified="$(meta_value "$meta" last_modified)"
      if [ -n "$etag" ] && [ "$old_etag" = "$etag" ]; then
        printf '%s\n' "$candidate"
        return 0
      fi
      if [ -n "$last_modified" ] && [ "$old_last_modified" = "$last_modified" ]; then
        printf '%s\n' "$candidate"
        return 0
      fi
    fi
  done < <(find "$DATA_ROOT" -type f -name "$filename" -print0)

  return 1
}

reuse_zip() {
  local source="$1"
  local target="$2"
  if [ "$source" = "$target" ]; then
    return 0
  fi
  rm -f "$target"
  ln "$source" "$target" 2>/dev/null || cp -p "$source" "$target"
  if [ -f "$source.meta" ]; then
    cp -p "$source.meta" "$target.meta"
  fi
}

write_meta() {
  local target="$1"
  local url="$2"
  local content_length="$3"
  local etag="$4"
  local last_modified="$5"
  {
    printf 'url=%s\n' "$url"
    printf 'content_length=%s\n' "$content_length"
    printf 'etag=%s\n' "$etag"
    printf 'last_modified=%s\n' "$last_modified"
  } > "$target.meta"
}

download_one() {
  local url
  local raw_filename
  local filename
  local target
  local headers
  local content_length
  local etag
  local last_modified
  local candidate
  local tmp

  url="$(trim_url "${1:-}")"
  [ -n "$url" ] || return 0

  raw_filename="$(basename "${url%%\?*}")"
  if [[ "$raw_filename" != *.gtfs.zip ]]; then
    echo "Skipping non-GTFS zip URL: $url" >&2
    record_status skipped "$raw_filename" "$url"
    return 0
  fi

  filename="$(safe_component "$raw_filename" ".gtfs.zip" 180)"

  target="$DEST/$filename"
  headers="$(curl -fsSLI --max-redirs 5 "$url" 2>/dev/null || true)"
  content_length="$(printf '%s\n' "$headers" | header_value 'content-length')"
  etag="$(printf '%s\n' "$headers" | header_value 'etag')"
  last_modified="$(printf '%s\n' "$headers" | header_value 'last-modified')"

  if [ "${GTFS_FORCE_DOWNLOAD:-0}" != "1" ]; then
    if candidate="$(find_unchanged_zip "$filename" "$content_length" "$etag" "$last_modified")"; then
      record_status reuse "$filename" "$candidate"
      reuse_zip "$candidate" "$target"
      write_meta "$target" "$url" "$content_length" "$etag" "$last_modified"
      return 0
    fi
  fi

  record_status download_attempt "$filename" "$url"
  tmp="$target.tmp.$$"
  rm -f "$tmp"
  if ! curl -fL "$url" -o "$tmp"; then
    record_status failed "$filename" "$url"
    rm -f "$tmp"
    return 1
  fi
  mv "$tmp" "$target"
  write_meta "$target" "$url" "$content_length" "$etag" "$last_modified"
  record_status downloaded "$filename" "$url"
}

extract_one() {
  local zip="$1"
  local filename
  local source_name
  local target
  local temp_target
  local logs_dir
  local safe_name
  local log_file
  local rc
  filename="$(basename "$zip")"
  source_name="$(safe_component "${filename%.zip}" "" 180)"
  target="$DEST/source=$source_name"
  temp_target="$DEST/.extracting-source=$source_name.$$"
  logs_dir="$DEST/.grabber-extract-logs"
  safe_name="$(printf '%s' "$source_name" | tr -c 'A-Za-z0-9_.=-' '_')"
  log_file="$logs_dir/$safe_name.log"

  mkdir -p "$logs_dir"
  rm -rf "$temp_target"
  record_status extract_attempt "$filename" "$zip"

  if 7za x -y "$zip" "-o$temp_target" > "$log_file" 2>&1; then
    rc=0
  else
    rc=$?
  fi

  if [ "$rc" = "0" ] || [ "$rc" = "1" ]; then
    rm -rf "$target"
    mv "$temp_target" "$target"
    if [ "$rc" = "1" ]; then
      record_status extracted_warning "$filename" "7za exit 1; log=$log_file"
    else
      record_status extracted "$filename" "$target"
      rm -f "$log_file"
    fi
    return 0
  fi

  rm -rf "$temp_target"
  record_status extract_failed "$filename" "7za exit $rc; log=$log_file"
  return 0
}

export DATA_ROOT DEST
export STATUS_FILE
export -f stat_size header_value meta_value trim_url safe_component record_status find_unchanged_zip reuse_zip write_meta download_one extract_one
export SHELL="${BASH:-$(command -v bash)}"

if [ "$EXTRACT_ONLY" != "1" ]; then
  parallel --bar -j "$NUM_PARALLEL" download_one :::: "$LINKS_FILE"
  echo "Download/reuse summary:"
  awk -F '\t' '{ counts[$1]++ } END { for (status in counts) print "  " status ": " counts[status] }' "$STATUS_FILE" | sort
  echo "Detailed status: $STATUS_FILE"
else
  echo "Detailed status: $STATUS_FILE"
fi
find "$DEST" -maxdepth 1 -type f -name '*.gtfs.zip' -print0 | parallel -0 -j4 --bar extract_one
echo "Final grabber summary:"
awk -F '\t' '{ counts[$1]++ } END { for (status in counts) print "  " status ": " counts[status] }' "$STATUS_FILE" | sort

zip_count="$(find "$DEST" -maxdepth 1 -type f -name '*.gtfs.zip' | wc -l)"
extract_ok_count="$(awk -F '\t' '$1 == "extracted" || $1 == "extracted_warning" { count++ } END { print count + 0 }' "$STATUS_FILE")"
extract_failed_count="$(awk -F '\t' '$1 == "extract_failed" { count++ } END { print count + 0 }' "$STATUS_FILE")"

if [ "$extract_failed_count" -gt 0 ]; then
  echo "WARNING: $extract_failed_count GTFS zip(s) failed extraction. See $DEST/.grabber-extract-logs and $STATUS_FILE" >&2
fi

if [ "$EXTRACT_ONLY" = "1" ] && [ "$zip_count" -eq 0 ]; then
  echo "ERROR: --extract-only found no *.gtfs.zip files in $DEST" >&2
  exit 1
fi

if [ "$zip_count" -gt 0 ] && [ "$extract_ok_count" -eq 0 ]; then
  echo "ERROR: no GTFS zips extracted successfully. See $DEST/.grabber-extract-logs and $STATUS_FILE" >&2
  exit 1
fi
