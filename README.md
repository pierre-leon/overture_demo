# Live Roadworks Matching Demo

Streams roadworks observations, matches them to Overture road segments on-the-fly, and visualizes the matching process in a deck.gl map.

## Why Overture Maps?

This demo uses [Overture Maps](https://overturemaps.org/) road segments rather than OpenStreetMap (OSM) for several key reasons:

- **Stable IDs**: Overture provides consistent, stable segment identifiers that persist across releases, making it easier to track and reference specific road segments over time
- **Meaningful Segments**: Overture segments represent logical road sections (e.g., full street blocks) rather than short stubs or fragments that are common in OSM
- **Cleaner Data**: The matching algorithm filters out non-drivable infrastructure (cycle paths, footways, pedestrian paths, bridleways, service roads) to focus on vehicle-relevant road segments, reducing memory usage and improving match accuracy

## Prerequisites

- Python 3.10+
- Node.js 18+

## Quick Start

### 1. Set up Python environment

```bash
cd /Users/ishay.rosenthal/Documents/Source/overture_demo
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Prepare data files

The demo loads events via file upload in the web UI. Your parquet file should contain:

**Required columns:**
- `lon`, `lat` - Event coordinates (WGS84)
- `event_type` - Set to `"Roadworks"` for roadworks events

**Optional columns:**
- `timestamp` - For chronological ordering
- `heading` - Vehicle heading in degrees (improves directional matching)
- `event_id` - Unique identifier for each event

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

# Install dependencies
npm install

# Configure API endpoints (optional for local dev)
cp .env.example .env
# Edit .env if needed: VITE_API_HTTP_URL and VITE_API_WS_URL

# Start dev server
npm run dev
```

Open http://localhost:5173

**Note:** The demo uses free Carto Dark Matter basemap tiles - no API key required.

## Demo Usage

1. **Upload Events**: Click "Upload Events" and select your `.parquet` file
2. **Watch Streaming**: Events are matched to road segments and streamed to the map in real-time
3. **Control Playback**:
   - **Basemap Toggle**: Show/hide the base map
   - **Speed Slider**: Adjust streaming speed (1x - 10x)
   - **Pause/Resume**: Pause or resume the event stream
   - **Restart**: Reset and replay from the beginning

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Root endpoint |
| `/health` | GET | Health check (returns roads loaded count) |
| `/upload` | POST | Upload events parquet file (multipart/form-data) |
| `/stream/roadworks` | WebSocket | Streamed matched roadworks with real-time controls |

### WebSocket Controls

Send JSON messages to `/stream/roadworks` to control playback:
- `{"action": "pause"}` - Pause streaming
- `{"action": "resume"}` - Resume streaming
- `{"action": "restart"}` - Restart from beginning
- `{"action": "set_speed", "batch_size": 100, "tick_ms": 30}` - Adjust speed

## Configuration

### Server Environment Variables

- `ROADS_PATH` - Path to Overture roads parquet (default: `data/overture_roads.parquet`)
- `MATCH_RADIUS_M` - Maximum match distance in meters (default: `50`)
- `BATCH_SIZE` - Events per WebSocket batch (default: `50`)
- `TICK_INTERVAL_MS` - Milliseconds between batches (default: `50`)
- `PORT` - Server port (default: `8000`, auto-set by Railway)

### Frontend Environment Variables

- `VITE_API_HTTP_URL` - Backend HTTP endpoint (default: `http://localhost:8000`)
- `VITE_API_WS_URL` - Backend WebSocket endpoint (default: `ws://localhost:8000`)

## Tech Stack

**Backend**: FastAPI, PyArrow (streaming parquet), Shapely (spatial indexing with STRtree)
**Frontend**: React, TypeScript, Vite, deck.gl, MapLibre GL JS
**Map Data**: Overture Maps (road segments), Carto (basemap tiles)

## Deployment

- **Backend**: Railway (1GB memory, 2 vCPU)
- **Frontend**: Vercel
- **Architecture**: Split-cloud with CORS-enabled REST + WebSocket communication
