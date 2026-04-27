#!/usr/bin/env bash
# Weekly scrape of all configured sites into one combined Excel workbook.

set -euo pipefail

cd "$(dirname "$0")"

if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

WEEK_TAG="$(date +%G-W%V_%Y%m%d_%H%M%S)"
ARCHIVE_DIR="output/archive/${WEEK_TAG}"
OUTPUT_FILE="output/all_sites_${WEEK_TAG}.xlsx"

mkdir -p "$ARCHIVE_DIR"

python main.py \
  --site all \
  --output "${OUTPUT_FILE}" \
  --format excel \
  --resume \
  --incremental \
  --max-pages 3 \
  --site-concurrency 11 \
  ${LLM_FLAG:+--llm}

find output -maxdepth 1 -name "all_sites_*.xlsx" -mtime +7 -exec mv {} "${ARCHIVE_DIR}/" \;

if [ -n "${SLACK_WEBHOOK_URL:-}" ]; then
  curl -X POST -H "Content-type: application/json" \
    --data "{\"text\":\"Weekly scrape completed. Workbook: ${OUTPUT_FILE}. Archives: ${ARCHIVE_DIR}\"}" \
    "${SLACK_WEBHOOK_URL}"
fi
