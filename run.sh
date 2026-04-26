#!/bin/bash
# Wrong Sense Scraper - Run Script
# Usage: ./run.sh [scrape|embed|import-db|all]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

LOG_FILE="$SCRIPT_DIR/run.log"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

log "=========================================="
log "Wrong Sense Scraper - Starting"
log "=========================================="

MODE="${1:-all}"

log "Running in mode: $MODE"

case "$MODE" in
    scrape)
        log "Step 1/3: Scraping products..."
        python main.py --scrape
        ;;
    embed)
        log "Step 2/3: Generating embeddings..."
        python main.py --embed
        ;;
    import-db)
        log "Step 3/3: Importing to Supabase..."
        python main.py --import-db
        ;;
    all|"")
        log "Running full pipeline..."
        python main.py --all
        ;;
    *)
        log "Unknown mode: $MODE"
        log "Usage: ./run.sh [scrape|embed|import-db|all]"
        exit 1
        ;;
esac

log "=========================================="
log "Wrong Sense Scraper - Complete"
log "=========================================="