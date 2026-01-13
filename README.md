# Live Roadworks Matching Demo

Streams roadworks observations, matches them to Overture road segments on-the-fly, and visualizes the matching process in a deck.gl map.

## Prerequisites

- Python 3.10+
- Node.js 18+
- Mapbox account (for basemap tiles)

## Quick Start

### 1. Set up Python environment

```bash
cd /Users/ishay.rosenthal/Documents/Source/overture_demo
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Prepare data files

Place your events parquet file at `data/events.parquet`. Required columns:
- `lon`, `lat` (coordinates)
- `event_type` (e.g., `"roadworks"`, `"enforcement"`)

Optional columns: `timestamp`, `heading`, `event_id`

### 3. Extract Overture roads for AOI

```bash
python scripts/01_extract_overture_roads.py \
  --events data/events.parquet \
  --output data/overture_roads.parquet \
  --buffer_m 2000 \
  --release 2024-11-13.0
```

### 4. Start the server

```bash
cd server
uvicorn app:app --reload --port 8000
```

Health check: `curl http://localhost:8000/health`

### 5. Set up and run the frontend

```bash
cd web

# Fix npm cache if needed (one-time)
sudo chown -R $(whoami) ~/.npm

# Install dependencies
npm install

# Set your Mapbox token
cp .env.example .env
# Edit .env and add your token: VITE_MAPBOX_TOKEN=pk.xxx

# Start dev server
npm run dev
```

Open http://localhost:5173

## Demo Controls

| Control | Description |
|---------|-------------|
| **Basemap Toggle** | Turn basemap on/off (segments remain visible) |
| **Speed Slider** | Adjust streaming speed (1x - 10x) |
| **Pause/Resume** | Pause or resume the event stream |
| **Restart** | Reset stream to beginning |

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/enforcement.geojson` | GET | Static enforcement events |
| `/stream/roadworks` | WS | Streamed matched roadworks |

## Configuration

Environment variables for the server:
- `EVENTS_PATH` - Path to events parquet (default: `data/events.parquet`)
- `ROADS_PATH` - Path to roads parquet (default: `data/overture_roads.parquet`)
- `MATCH_RADIUS_M` - Match radius in meters (default: `50`)
- `BATCH_SIZE` - Events per WebSocket batch (default: `50`)

## Tech Stack

**Backend**: FastAPI, DuckDB, Shapely (STRtree), PyArrow  
**Frontend**: React, Vite, TypeScript, deck.gl, MapLibre GL JS
