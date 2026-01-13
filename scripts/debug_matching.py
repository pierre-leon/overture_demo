
import pyarrow.parquet as pq
from shapely import wkb
from shapely.geometry import Point
import sys
import os

# Add server to path
sys.path.append(os.getcwd())
try:
    from server.matching import RoadMatcher
except ImportError:
    print("Run from project root!")
    sys.exit(1)

def main():
    events_path = "data/events_all.parquet"
    roads_path = "data/overture_roads.parquet"
    
    print(f"Loading events from {events_path}...")
    events = pq.read_table(events_path)
    lons = events["lon"].to_pylist()
    lats = events["lat"].to_pylist()
    
    min_lon, max_lon = min(lons), max(lons)
    min_lat, max_lat = min(lats), max(lats)
    print(f"Events BBox: [{min_lon}, {min_lat}, {max_lon}, {max_lat}]")
    
    print(f"Loading roads from {roads_path}...")
    
    # Use DuckDB to get true BBox of roads
    import duckdb
    conn = duckdb.connect()
    # We need to install spatial extension to parse WKB? 
    # Or just parse min/max from geometry if possible?
    # Overture geometry is binary. We can't trivially get bbox without ST_Envelope or parsing.
    # But we can iterate in python fairly fast for 250k rows if we just want one point.
    
    # Let's just try to find the NEAREST road for the first event across ALL roads
    # to see how far it is.
    
    roads = pq.read_table(roads_path)
    print(f"Roads count: {len(roads)}")
    
    full_geoms = []
    wkbs = roads["geometry_wkb"]
    
    # Parse all (might take 5s)
    from shapely import wkb
    count = len(wkbs)
    print("Parsing all geometries...")
    # Just parse bounding boxes?
    
    min_x, min_y, max_x, max_y = 180, 90, -180, -90
    
    for i in range(0, count, 1000): # Sample 1% for speed or just do all?
        # Let's do all to be sure
        b = wkbs[i].as_py()
        g = wkb.loads(b)
        bounds = g.bounds
        min_x = min(min_x, bounds[0])
        min_y = min(min_y, bounds[1])
        max_x = max(max_x, bounds[2])
        max_y = max(max_y, bounds[3])
        
    print(f"Roads Extent (Sampled 1/1000): [{min_x}, {min_y}, {max_x}, {max_y}]")
    print(f"Events Extent: [{min_lon}, {min_lat}, {max_lon}, {max_lat}]")
    
    # Check intersection
    intersect = not (max_x < min_lon or min_x > max_lon or max_y < min_lat or min_y > max_lat)
    print(f"Bounding Boxes Intersect: {intersect}")

    if not intersect:
        print("CRITICAL: Roads do not cover events area!")
    
    # Initialize matcher
    print("Initializing Matcher...")
    matcher = RoadMatcher(roads_path, radius_m=2000) # Use huge radius to see if ANYTHING is close
    
    # Try matching first 5 events
    print("\nTesting matches:")
    for i in range(5):
        lon = lons[i]
        lat = lats[i]
        
        # Heading might be needed?
        heading = None
        if "ego_heading" in events.column_names:
             heading = events["ego_heading"][i].as_py()
             
        result = matcher.match(lon, lat, heading)
        print(f"Event {i}: ({lon}, {lat}) -> Matched: {result.matched}, Dist: {result.match_distance_m}m")
        if result.matched:
            print(f"  Segment: {result.segment_id}")

if __name__ == "__main__":
    main()
