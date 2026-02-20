#!/bin/bash
set -e

log() { echo "[start.sh] $*" >&2; }

log "Checking dependencies..."

# --- ffmpeg ---
if ! command -v ffmpeg &>/dev/null || ! command -v ffprobe &>/dev/null; then
    log "ffmpeg not found, installing..."
    apt-get update -qq && apt-get install -y ffmpeg -qq
    log "ffmpeg installed: $(ffmpeg -version 2>&1 | head -1)"
else
    log "ffmpeg OK: $(ffmpeg -version 2>&1 | head -1)"
fi

# --- Python dependencies ---
PYTHON=/root/.venv/bin/python
PIP=/root/.venv/bin/pip
REQ=/app/video_tentacles/requirements.txt

log "Checking Python packages..."
if ! $PYTHON -c "import aiofiles, aiogram, yt_dlp, aiohttp, httpx, bs4, dotenv" 2>/dev/null; then
    log "Missing packages, installing from requirements.txt..."
    $PIP install -r $REQ -q
    log "Packages installed."
else
    log "Python packages OK."
fi

# --- Start bot ---
log "Starting bot..."
exec $PYTHON /app/video_tentacles/bot.py
