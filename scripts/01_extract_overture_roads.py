#!/usr/bin/env python3
"""
Extract Overture transportation road segments for the AOI derived from events.parquet.

Usage:
    python scripts/01_extract_overture_roads.py \
        --events data/events.parquet \
        --output data/overture_roads.parquet \
        --buffer_m 2000 \
        --release 2024-11-13.0
"""

import argparse
import sys
from pathlib import Path

import duckdb
import pyarrow.parquet as pq


def compute_bbox_from_events(events_path: Path, lon_col: str = "lon", lat_col: str = "lat") -> tuple[float, float, float, float]:
    """Read events parquet and compute bounding box (min_lon, min_lat, max_lon, max_lat)."""
    table = pq.read_table(events_path, columns=[lon_col, lat_col])
    lons = table[lon_col].to_pylist()
    lats = table[lat_col].to_pylist()
    
    # Filter out nulls
    valid = [(lon, lat) for lon, lat in zip(lons, lats) if lon is not None and lat is not None]
    if not valid:
        raise ValueError("No valid coordinates found in events file")
    
    lons, lats = zip(*valid)
    return (min(lons), min(lats), max(lons), max(lats))


def buffer_bbox(bbox: tuple[float, float, float, float], buffer_m: float) -> tuple[float, float, float, float]:
    """Expand bbox by buffer in meters (approximate conversion at mid-latitude)."""
    min_lon, min_lat, max_lon, max_lat = bbox
    mid_lat = (min_lat + max_lat) / 2
    
    # Approximate degrees per meter at this latitude
    lat_deg_per_m = 1 / 111320
    lon_deg_per_m = 1 / (111320 * abs(__import__('math').cos(__import__('math').radians(mid_lat))))
    
    buffer_lat = buffer_m * lat_deg_per_m
    buffer_lon = buffer_m * lon_deg_per_m
    
    return (
        min_lon - buffer_lon,
        min_lat - buffer_lat,
        max_lon + buffer_lon,
        max_lat + buffer_lat
    )


def extract_overture_roads(
    bbox: tuple[float, float, float, float],
    output_path: Path,
    release: str = "2024-11-13.0"
) -> int:
    """
    Query Overture transportation segments within bbox and save to parquet.
    Returns count of segments extracted.
    """
    min_lon, min_lat, max_lon, max_lat = bbox
    
    # Overture S3 path for transportation segments
    overture_base = f"s3://overturemaps-us-west-2/release/{release}/theme=transportation/type=segment/*"
    
    con = duckdb.connect()
    
    # Install and load required extensions
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL spatial; LOAD spatial;")
    
    # Configure S3 for anonymous access
    con.execute("SET s3_region = 'us-west-2';")
    con.execute("SET s3_access_key_id = '';")
    con.execute("SET s3_secret_access_key = '';")
    
    print(f"Querying Overture release {release} for bbox: {bbox}")
    print("This may take a few minutes...")
    
    # Query with bbox filter using Overture's bbox columns for partition pruning
    query = f"""
    COPY (
        SELECT
            id as segment_id,
            names.primary as name,
            class,
            subclass,
            geometry,
            ST_AsWKB(geometry) as geometry_wkb
        FROM read_parquet('{overture_base}', filename=true, hive_partitioning=true)
        WHERE bbox.xmin <= {max_lon}
          AND bbox.xmax >= {min_lon}
          AND bbox.ymin <= {max_lat}
          AND bbox.ymax >= {min_lat}
          AND subtype = 'road'
    ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """
    
    con.execute(query)
    
    # Get count
    count_result = con.execute(f"SELECT COUNT(*) FROM read_parquet('{output_path}')").fetchone()
    count = count_result[0] if count_result else 0
    
    con.close()
    return count


def main():
    parser = argparse.ArgumentParser(description="Extract Overture roads for events AOI")
    parser.add_argument("--events", type=Path, required=True, help="Path to events.parquet")
    parser.add_argument("--output", type=Path, default=Path("data/overture_roads.parquet"), help="Output path")
    parser.add_argument("--buffer_m", type=float, default=2000, help="Buffer around AOI in meters")
    parser.add_argument("--release", type=str, default="2024-11-13.0", help="Overture release version")
    parser.add_argument("--lon_col", type=str, default="lon", help="Longitude column name")
    parser.add_argument("--lat_col", type=str, default="lat", help="Latitude column name")
    
    args = parser.parse_args()
    
    if not args.events.exists():
        print(f"Error: Events file not found: {args.events}", file=sys.stderr)
        sys.exit(1)
    
    # Ensure output directory exists
    args.output.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"Reading events from: {args.events}")
    bbox = compute_bbox_from_events(args.events, args.lon_col, args.lat_col)
    print(f"Events bbox: {bbox}")
    
    buffered_bbox = buffer_bbox(bbox, args.buffer_m)
    print(f"Buffered bbox (+{args.buffer_m}m): {buffered_bbox}")
    
    count = extract_overture_roads(buffered_bbox, args.output, args.release)
    print(f"Extracted {count} road segments to: {args.output}")


if __name__ == "__main__":
    main()
