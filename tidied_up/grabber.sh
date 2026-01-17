#!/bin/bash
# requires super-secret BASE_URL
NUM_PARALLEL=3 # 

# checks if a directory argument is provided
if [ -z "$1" ]; then
  echo "No directory provided. Using current directory."
else
  echo "Downloading to $1"
  mkdir -p "$1"
  cd "$1"
fi

curl -s "${BASE_URL}" | \
  awk -F'"' '/href="[^"]*\.gtfs\.zip"/{print $2}' | \
  sed "s|^|${BASE_URL}|" | \
  parallel --bar -j ${NUM_PARALLEL} curl -O -L {}

ls *.zip | parallel -j4 --bar 'mkdir -p {/.} && 7za x {} -osource={/.}'
# todo: skip land polygons
