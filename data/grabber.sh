#!/bin/bash
BASE_URL="https://api.transitous.org/gtfs/"
NUM_PARALLEL=3 # 

curl -s "${BASE_URL}" | \
  awk -F'"' '/href="[^"]*\.zip"/{print $2}' | \
  sed "s|^|${BASE_URL}|" | \
  parallel --bar -j ${NUM_PARALLEL} curl -O -L {}

ls *.zip | parallel -j4 --bar 'mkdir -p {/.} && 7za x {} -osource={/.}'
# todo: skip land polygons
