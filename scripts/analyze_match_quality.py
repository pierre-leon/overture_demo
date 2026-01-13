import sys
import os
import statistics
import pyarrow.parquet as pq

# Add parent directory to path to import server modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from server.matching import RoadMatcher


def analyze():
    print("Initializing Matcher...")
    # Use config defaults or hardcode paths
    roads_path = "data/overture_roads.parquet"
    events_path = "data/events_all.parquet"
    
    matcher = RoadMatcher(roads_path)
    
    print(f"Loading events from {events_path}...")
    table = pq.read_table(events_path)
    
    # Columns
    lon_col = "lon"
    lat_col = "lat"
    heading_col = "ego_heading"
    type_col = "event_type"
    
    distances = []
    matches = 0
    total = 0
    
    print("Processing events (Roadworks only)...")
    
    # Get columns directly from Arrow (avoid pandas conversion issues)
    try:
        lons = table[lon_col].to_pylist()
        lats = table[lat_col].to_pylist()
        headings = table[heading_col].to_pylist() if heading_col in table.column_names else [None] * len(lons)
        types = table[type_col].to_pylist()
    except Exception as e:
        print(f"Error reading columns: {e}")
        return

    count = len(lons)
    total_roadworks = 0
    
    for i in range(count):
        # Filter for Roadworks first
        if types[i] != "Roadworks":
            continue
        
        total_roadworks += 1
        
        lon = lons[i]
        lat = lats[i]
        heading = headings[i]
        
        result = matcher.match(lon, lat, heading)
        
        if result.matched:
            matches += 1
            distances.append(result.match_distance_m)
            
    if not distances:
        print("No matches found.")
        return

    avg_dist = statistics.mean(distances)
    median_dist = statistics.median(distances)
    max_dist = max(distances)
    min_dist = min(distances)
    
    print("-" * 30)
    print(f"Total Roadworks Events: {total_roadworks}")
    print(f"Successfully Matched:   {matches} ({matches/total_roadworks*100:.1f}%)")
    print("-" * 30)
    print(f"Average Distance: {avg_dist:.2f} meters")
    print(f"Median Distance:  {median_dist:.2f} meters")
    print(f"Min Distance:     {min_dist:.2f} meters")
    print(f"Max Distance:     {max_dist:.2f} meters")
    print("-" * 30)

if __name__ == "__main__":
    analyze()
