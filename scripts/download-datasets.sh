#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${SCRIPT_DIR}/../datasets"
mkdir -p "$DATA_DIR"

FILES=(
  "UNSW-NB15_features.csv|https://www.dropbox.com/s/c8qrzd99z5s9ub6/UNSW-NB15_features.csv?dl=1"
  "UNSW-NB15_LIST_EVENTS.csv|https://www.dropbox.com/s/x2x0b6hazukbzx9/UNSW-NB15_LIST_EVENTS.csv?dl=1"
  "UNSW-NB15.csv|https://www.dropbox.com/s/4xqg32ih9xoh5jq/UNSW-NB15.csv?dl=1"
)

for entry in "${FILES[@]}"; do
  name="${entry%%|*}"
  url="${entry#*|}"
  dest="${DATA_DIR}/${name}"

  if [[ -s "$dest" ]]; then
    echo "skip: $name already exists"
    continue
  fi

  echo "downloading: $name"
  curl -fL --retry 3 --retry-delay 2 -o "${dest}.part" "$url"
  mv "${dest}.part" "$dest"
done

ls -lh "$DATA_DIR"