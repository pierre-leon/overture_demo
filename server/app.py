"""FastAPI server for streaming roadworks matching demo."""

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional
import json

from io import BytesIO
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, UploadFile, File, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
import pyarrow.parquet as pq
import orjson

from config import ServerConfig
from matching import RoadMatcher


# Global state
matcher: Optional[RoadMatcher] = None
roadworks_events: list[dict] = []
enforcement_events: list[dict] = []
config: ServerConfig = ServerConfig.from_env()


def load_events(source, columns: dict) -> tuple[list[dict], list[dict]]:
    """Load events from parquet source (path or buffer) and partition."""
    print(f"[LOAD] Opening parquet file...", flush=True)

    # Use streaming reader to avoid loading entire file into memory at once
    parquet_file = pq.ParquetFile(source)

    print(f"[LOAD] Parquet metadata: {parquet_file.metadata.num_rows:,} rows, {parquet_file.num_row_groups} row groups", flush=True)

    roadworks = []
    enforcement = []

    lon_col = columns.get("lon", "lon")
    lat_col = columns.get("lat", "lat")
    event_type_col = columns.get("event_type", "event_type")
    timestamp_col = columns.get("timestamp", "timestamp")
    heading_col = columns.get("heading", "heading")
    event_id_col = columns.get("event_id", "event_id")

    total_processed = 0

    # Process parquet file in row groups (streaming) to avoid memory spikes
    print(f"[LOAD] Processing row groups...", flush=True)
    for row_group_idx in range(parquet_file.num_row_groups):
        print(f"[LOAD] Reading row group {row_group_idx + 1}/{parquet_file.num_row_groups}...", flush=True)

        # Read one row group at a time
        table = parquet_file.read_row_group(row_group_idx)
        col_names = table.column_names

        # Convert columns to Python lists for this batch
        lons = table[lon_col].to_pylist() if lon_col in col_names else [None] * len(table)
        lats = table[lat_col].to_pylist() if lat_col in col_names else [None] * len(table)
        event_types = table[event_type_col].to_pylist() if event_type_col in col_names else ["roadworks"] * len(table)
        timestamps = table[timestamp_col].to_pylist() if timestamp_col in col_names else [None] * len(table)
        headings = table[heading_col].to_pylist() if heading_col in col_names else [None] * len(table)
        event_ids = table[event_id_col].to_pylist() if event_id_col in col_names else None

        # Process rows in this batch
        for i in range(len(table)):
            # Skip if missing coordinates
            if lons[i] is None or lats[i] is None:
                continue

            # Filter to show ONLY Roadworks events as requested
            if event_types[i] != "Roadworks":
                continue

            event = {
                "lon": lons[i],
                "lat": lats[i],
                "event_type": event_types[i],
                "event_id": event_ids[i] if event_ids else f"evt_{total_processed + i}",
            }

            # Add optional fields
            if timestamps[i] is not None:
                event["timestamp"] = str(timestamps[i])

            if headings[i] is not None:
                event["heading"] = headings[i]

            # Partition by event type
            if event["event_type"] == config.enforcement_type:
                enforcement.append(event)
            else:
                roadworks.append(event)

        total_processed += len(table)
        print(f"[LOAD] Processed {total_processed:,} total events so far, {len(roadworks)} roadworks", flush=True)

        # Clear the batch table to free memory
        del table, lons, lats, event_types, timestamps, headings, event_ids

    print(f"[LOAD] Finished: {len(roadworks)} roadworks, {len(enforcement)} enforcement", flush=True)

    # Sort roadworks by timestamp if available
    if roadworks and "timestamp" in roadworks[0]:
        print(f"[LOAD] Sorting by timestamp...", flush=True)
        roadworks.sort(key=lambda e: e.get("timestamp") or "")

    return roadworks, enforcement


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Load data on startup."""
    global matcher, roadworks_events, enforcement_events
    
    roads_path = Path(config.roads_path)
    
    if not roads_path.exists():
        print(f"Warning: Roads file not found: {roads_path}")
        print("Run scripts/01_extract_overture_roads.py first")
    else:
        matcher = RoadMatcher(str(roads_path), radius_m=config.matching.radius_m)
    
    # We NO LONGER load events on startup!
    # User must upload them via POST /upload
    print(f"Ready. {len(roadworks_events)} events loaded. Waiting for uploads...")
    
    yield


app = FastAPI(
    title="Roadworks Matching Demo",
    lifespan=lifespan,
)

# Custom middleware to log all requests
class LoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        print(f"[REQUEST] {request.method} {request.url.path}", flush=True)
        try:
            response = await call_next(request)
            print(f"[RESPONSE] {request.method} {request.url.path} -> {response.status_code}", flush=True)
            return response
        except Exception as e:
            print(f"[ERROR] {request.method} {request.url.path} -> {type(e).__name__}: {e}", flush=True)
            raise

app.add_middleware(LoggingMiddleware)

# CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/upload")
async def upload_events(file: UploadFile = File(...)):
    """Upload new events file."""
    global roadworks_events, enforcement_events

    print(f"[UPLOAD] Received upload request for file: {file.filename}", flush=True)

    try:
        # Write to temp file to avoid memory issues
        import tempfile
        import os

        print(f"[UPLOAD] Writing to temp file...", flush=True)

        # Create temp file
        with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.parquet') as tmp:
            temp_path = tmp.name
            bytes_read = 0

            # Stream chunks directly to disk
            while chunk := await file.read(1024 * 1024):  # 1MB chunks
                tmp.write(chunk)
                bytes_read += len(chunk)

        file_size_mb = bytes_read / (1024 * 1024)
        print(f"[UPLOAD] File size: {file_size_mb:.2f} MB (saved to temp)", flush=True)

        # Check file size limit
        if file_size_mb > 80:
            os.unlink(temp_path)
            print(f"[UPLOAD] File too large: {file_size_mb:.2f} MB", flush=True)
            return JSONResponse(
                status_code=413,
                content={"error": f"File too large ({file_size_mb:.1f}MB). Maximum size is 80MB."}
            )

        columns = {
            "lon": config.columns.lon,
            "lat": config.columns.lat,
            "event_type": config.columns.event_type,
            "timestamp": config.columns.timestamp,
            "heading": config.columns.heading,
            "event_id": config.columns.event_id,
        }

        print(f"[UPLOAD] Loading events from parquet file...", flush=True)
        new_roadworks, new_enforcement = load_events(temp_path, columns)

        # Clean up temp file
        os.unlink(temp_path)
        print(f"[UPLOAD] Temp file cleaned up", flush=True)

        # Replace existing events
        roadworks_events = new_roadworks
        enforcement_events = new_enforcement

        print(f"Successfully loaded {len(roadworks_events)} roadworks events")

        return {
            "status": "ok",
            "message": f"Loaded {len(roadworks_events)} roadworks events",
            "roadworks_count": len(roadworks_events),
            "enforcement_count": len(enforcement_events),
        }
    except Exception as e:
        print(f"Upload error: {type(e).__name__}: {str(e)}")
        import traceback
        traceback.print_exc()
        return JSONResponse(status_code=400, content={"error": str(e)})


@app.get("/")
async def root():
    """Root endpoint for Railway health checks."""
    return {"status": "ok", "service": "roadworks-matching-api"}


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {
        "status": "ok",
        "roads_loaded": matcher is not None,
        "roadworks_count": len(roadworks_events),
        "enforcement_count": len(enforcement_events),
    }


@app.get("/enforcement.geojson")
async def get_enforcement():
    """Return enforcement events as GeoJSON."""
    features = []
    
    for event in enforcement_events:
        feature = {
            "type": "Feature",
            "properties": {k: v for k, v in event.items() if k not in ["lon", "lat"]},
            "geometry": {
                "type": "Point",
                "coordinates": [event["lon"], event["lat"]],
            },
        }
        features.append(feature)
    
    geojson = {
        "type": "FeatureCollection",
        "features": features,
    }
    
    return JSONResponse(
        content=geojson,
        media_type="application/geo+json",
    )


@app.websocket("/stream/roadworks")
async def stream_roadworks(
    websocket: WebSocket,
    batch_size: int = Query(default=50, ge=1, le=500),
    tick_ms: int = Query(default=50, ge=10, le=1000),
):
    """Stream matched roadworks events via WebSocket."""
    await websocket.accept()
    
    if matcher is None:
        await websocket.send_json({"type": "error", "message": "Roads not loaded"})
        await websocket.close()
        return
    
    sent_segments: set[str] = set()
    event_idx = 0
    paused = False
    current_batch_size = batch_size
    current_tick_ms = tick_ms
    
    try:
        # Start streaming task
        async def stream_events():
            nonlocal event_idx, paused, current_batch_size, current_tick_ms

            # Handle empty events case
            if len(roadworks_events) == 0:
                await websocket.send_bytes(orjson.dumps({
                    "type": "complete",
                    "total_events": 0,
                    "total_segments": 0,
                    "message": "No events loaded. Please upload a file."
                }))
                return

            while event_idx < len(roadworks_events):
                if paused:
                    await asyncio.sleep(0.1)
                    continue
                
                batch_end = min(event_idx + current_batch_size, len(roadworks_events))
                batch_messages = []
                
                for i in range(event_idx, batch_end):
                    event = roadworks_events[i]
                    
                    # Match event to road
                    result = matcher.match(
                        lon=event["lon"],
                        lat=event["lat"],
                        heading=event.get("heading"),
                    )
                    
                    # Send segment geometry if new
                    if result.matched and result.segment_id not in sent_segments:
                        seg_geojson = matcher.get_segment_geojson(result.segment_id)
                        if seg_geojson:
                            batch_messages.append({
                                "type": "segment",
                                "segment_id": result.segment_id,
                                "display_segment_key": result.display_segment_key,
                                "properties": seg_geojson["properties"],
                                "geometry": seg_geojson["geometry"],
                            })
                            sent_segments.add(result.segment_id)
                    
                    # Send event message
                    event_msg = {
                        "type": "event",
                        "event": {
                            "event_id": event["event_id"],
                            "lon": event["lon"],
                            "lat": event["lat"],
                            "event_type": event.get("event_type", "roadworks"),
                            "matched": result.matched,
                            "matched_segment_id": result.segment_id,
                            "display_segment_key": result.display_segment_key,
                            "match_distance_m": result.match_distance_m,
                            "snapped_lon": result.snapped_lon,
                            "snapped_lat": result.snapped_lat,
                        },
                    }
                    
                    # Include additional event properties
                    for key in ["timestamp", "severity", "category", "description"]:
                        if key in event:
                            event_msg["event"][key] = event[key]
                    
                    batch_messages.append(event_msg)
                
                # Send batch
                for msg in batch_messages:
                    await websocket.send_bytes(orjson.dumps(msg))
                
                event_idx = batch_end
                
                # Send progress
                await websocket.send_bytes(orjson.dumps({
                    "type": "progress",
                    "streamed": event_idx,
                    "total": len(roadworks_events),
                    "segments": len(sent_segments),
                }))
                
                await asyncio.sleep(current_tick_ms / 1000)
            
            # Stream complete
            await websocket.send_bytes(orjson.dumps({
                "type": "complete",
                "total_events": len(roadworks_events),
                "total_segments": len(sent_segments),
            }))
        
        # Handle control messages while streaming
        stream_task = asyncio.create_task(stream_events())
        
        try:
            while True:
                data = await websocket.receive_text()
                msg = json.loads(data)
                
                if msg.get("action") == "pause":
                    paused = True
                elif msg.get("action") == "resume":
                    paused = False
                elif msg.get("action") == "set_speed":
                    current_batch_size = msg.get("batch_size", current_batch_size)
                    current_tick_ms = msg.get("tick_ms", current_tick_ms)
                elif msg.get("action") == "restart":
                    # Cancel current stream and restart from beginning
                    stream_task.cancel()
                    event_idx = 0
                    sent_segments.clear()
                    # Start new stream task
                    stream_task = asyncio.create_task(stream_events())
        except WebSocketDisconnect:
            stream_task.cancel()
            
    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"WebSocket error: {e}")
        try:
            await websocket.send_json({"type": "error", "message": str(e)})
        except:
            pass


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
