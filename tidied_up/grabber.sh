#!/bin/bash
set -euo pipefail

# Requires BASE_URL. Optionally accepts DATA_ROOT, which should be the directory
# containing dated Transitous snapshots, e.g. /mnt/chungus/clickhouse_files/transitous.
NUM_PARALLEL="${NUM_PARALLEL:-3}"

if [ -z "${BASE_URL:-}" ]; then
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

curl -fsSL "$BASE_URL" > "$INDEX_FILE"

python3 - "$BASE_URL" "$INDEX_FILE" > "$LINKS_FILE" <<'PY'
import re
import sys
from urllib.parse import urljoin, urlparse

base_url, index_file = sys.argv[1], sys.argv[2]
html = open(index_file, encoding="utf-8", errors="replace").read()
urls = set()

for href in re.findall(r'''href\s*=\s*["']([^"']+)["']''', html, flags=re.I):
    url = urljoin(base_url, href)
    if urlparse(url).path.endswith(".gtfs.zip"):
        urls.add(url)

for raw_url in re.findall(r'''https?://[^\s"'<>]+''', html, flags=re.I):
    if urlparse(raw_url).path.endswith(".gtfs.zip"):
        urls.add(raw_url)

for url in sorted(urls):
    print(url)
PY

if [ ! -s "$LINKS_FILE" ]; then
  echo "No .gtfs.zip links found at $BASE_URL" >&2
  exit 1
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
  local url="$1"
  local filename
  local target
  local headers
  local content_length
  local etag
  local last_modified
  local candidate
  local tmp

  filename="$(basename "${url%%\?*}")"
  if [[ "$filename" != *.gtfs.zip ]]; then
    echo "Skipping non-GTFS zip URL: $url" >&2
    return 0
  fi

  target="$DEST/$filename"
  headers="$(curl -fsSLI --max-redirs 5 "$url" 2>/dev/null || true)"
  content_length="$(printf '%s\n' "$headers" | header_value 'content-length')"
  etag="$(printf '%s\n' "$headers" | header_value 'etag')"
  last_modified="$(printf '%s\n' "$headers" | header_value 'last-modified')"

  if candidate="$(find_unchanged_zip "$filename" "$content_length" "$etag" "$last_modified")"; then
    echo "Reusing $filename from $candidate"
    reuse_zip "$candidate" "$target"
    write_meta "$target" "$url" "$content_length" "$etag" "$last_modified"
    return 0
  fi

  echo "Downloading $filename"
  tmp="$target.tmp.$$"
  rm -f "$tmp"
  curl -fL "$url" -o "$tmp"
  mv "$tmp" "$target"
  write_meta "$target" "$url" "$content_length" "$etag" "$last_modified"
}

extract_one() {
  local zip="$1"
  local filename
  local source_name
  filename="$(basename "$zip")"
  source_name="${filename%.zip}"
  mkdir -p "$DEST/source=$source_name"
  7za x -y "$zip" "-o$DEST/source=$source_name"
}

export DATA_ROOT DEST
export -f stat_size header_value meta_value find_unchanged_zip reuse_zip write_meta download_one extract_one

parallel --bar -j "$NUM_PARALLEL" bash -c 'download_one "$1"' _ :::: "$LINKS_FILE"
find "$DEST" -maxdepth 1 -type f -name '*.gtfs.zip' -print0 | parallel -0 -j4 --bar bash -c 'extract_one "$1"' _
