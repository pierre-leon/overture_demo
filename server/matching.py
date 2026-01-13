"""Road segment matching using Shapely STRtree."""

from dataclasses import dataclass
from typing import Optional
import math

import pyarrow.parquet as pq
from shapely import wkb
from shapely.geometry import Point, LineString
from shapely.strtree import STRtree
from pyproj import Transformer


@dataclass
class MatchResult:
    """Result of matching a point to a road segment."""
    matched: bool
    segment_id: Optional[str] = None
    display_segment_key: Optional[str] = None
    directed_id: Optional[str] = None
    match_distance_m: Optional[float] = None
    snapped_lon: Optional[float] = None
    snapped_lat: Optional[float] = None


@dataclass
class RoadSegment:
    """A road segment with its geometry and attributes."""
    segment_id: str
    geometry: LineString
    name: Optional[str] = None
    road_class: Optional[str] = None
    subclass: Optional[str] = None
    geometry_wkb: Optional[bytes] = None


class RoadMatcher:
    """Matches points to road segments using spatial indexing."""
    
    def __init__(self, roads_path: str, radius_m: float = 50.0):
        self.radius_m = radius_m
        self.tree: Optional[STRtree] = None
        
        # Kept in memory but efficient (Arrow Table)
        self.table: Optional[pq.File] = None
        
        # Lists for STRtree index (geometry objects must be kept alive)
        self.geometries = []
        self.segment_ids = []
        
        # Map segment_id string -> integer index in self.geometries
        self.id_to_index = {}
        # Map valid geometry index -> original table row index
        self.valid_to_table_index = []
        
        # Transformer for WGS84 to metric (Web Mercator for distance call)
        self.to_metric = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
        self.to_wgs84 = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)
        
        self._load_roads(roads_path)
        self._build_index()
    
    def _load_roads(self, roads_path: str) -> None:
        """Load road segments from parquet file."""
        # Read the whole table into memory - PyArrow is RAM efficient
        print(f"Loading roads from {roads_path}...")
        self.table = pq.read_table(roads_path)
        
        # Pre-process columns to avoid overhead during loop
        try:
            ids = self.table["segment_id"].to_pylist()
            wkbs = self.table["geometry_wkb"].to_pylist()
            # Get classes for filtering
            classes = self.table["class"].to_pylist() if "class" in self.table.column_names else ["unknown"] * len(ids)
        except Exception:
            # Handle potential column name mismatch? (Schema assumed correct from debug_matching success)
            ids = self.table["id"].to_pylist() if "id" in self.table.column_names else []
            wkbs = self.table["geometry"].to_pylist() if "geometry" in self.table.column_names else []
            classes = ["unknown"] * len(ids)

        valid_geoms = []
        valid_ids = []
        
        # Filter out non-drivable roads to save memory (critical for Railway Starter tier)
        # Excludes: footway, steps, cycleway, path, pedestrian, track, service
        EXCLUDED_CLASSES = {
            "footway", "steps", "cycleway", "path", "pedestrian", "track", "service", "bridleway"
        }

        # Parse geometries (SLOW but necessary for Index)
        # We can't avoid Shapely objects in memory for STRtree
        count = len(ids)
        for i in range(count):
            try:
                # Check class before expensive WKB load
                if classes[i] in EXCLUDED_CLASSES:
                    continue
                    
                g = wkb.loads(wkbs[i])
                if isinstance(g, LineString):
                    valid_geoms.append(g)
                    valid_ids.append(str(ids[i]))
                    self.id_to_index[str(ids[i])] = len(valid_geoms) - 1
                    self.valid_to_table_index.append(i)
            except Exception:
                continue

        self.geometries = valid_geoms
        self.segment_ids = valid_ids
        print(f"Parsed {len(self.geometries)} valid road segments")
    
    def _build_index(self) -> None:
        """Build STRtree spatial index over road geometries."""
        if not self.geometries:
            print("Warning: No geometries to index!")
            return
            
        self.tree = STRtree(self.geometries)
        print(f"Built spatial index with {len(self.geometries)} road segments")
    
    def _compute_bearing(self, line: LineString, point: Point) -> float:
        """Compute approximate bearing of line segment near a point."""
        # Find the segment of the line closest to the point
        projected = line.project(point, normalized=True)
        
        # Get points slightly before and after on the line
        delta = 0.01
        p1_norm = max(0, projected - delta)
        p2_norm = min(1, projected + delta)
        
        p1 = line.interpolate(p1_norm, normalized=True)
        p2 = line.interpolate(p2_norm, normalized=True)
        
        # Calculate bearing
        dx = p2.x - p1.x
        dy = p2.y - p1.y
        bearing = math.degrees(math.atan2(dx, dy)) % 360
        
        return bearing
    
    def _choose_direction(self, line: LineString, point: Point, heading: Optional[float]) -> str:
        """Choose fwd or rev direction based on heading alignment."""
        if heading is None:
            return "fwd"
        
        line_bearing = self._compute_bearing(line, point)
        
        # Compare to heading: if within 90 degrees, use fwd; otherwise rev
        diff = abs(heading - line_bearing)
        if diff > 180:
            diff = 360 - diff
        
        return "fwd" if diff <= 90 else "rev"

    def _create_segment_object(self, index: int) -> RoadSegment:
        """Lazy load attributes from Arrow table only when needed."""
        seg_id = self.segment_ids[index]
        geometry = self.geometries[index] # Already in memory
        
        # Fetch attributes from arrow table row (fast lookup)
        # We need to map the 'index' from valid_ids back to original table index?
        # WAIT. We filtered items.
        # Simple solution: self.segments was handling this.
        # Now we need to query the TABLE. But indices mismatch if we skipped invalid geoms.
        
        # To strictly save memory, we shouldn't store a mapping of "valid_idx -> table_idx" (another int array).
        # Actually that costs very little (250k ints = 1MB).
        # OR:
        # Since 99.9% are valid, let's assume valid_idx maps fairly well?
        # No, that's risky.
        
        # ALTERNATIVE: Just store the attributes in simple python lists (tuples) instead of heavy objects.
        # 250k string tuples is cheaper than 250k objects.
        # BUT self.table is already in memory! Why duplicate strings?
        
        # Let's optimize: We assume filtering is rare. 
        # But we need to look up by ID for 'get_segment'.
        
        # Okay, let's just search the table by ID? No, slow.
        # Let's simple caching: Store `original_index` in `id_to_index`?
        # `id_to_index` maps ID -> valid_geometry_index. 
        
        # We need `valid_geometry_index` -> `table_row_index`.
        # Let's just create that list during load.
        return RoadSegment(
            segment_id=seg_id,
            geometry=geometry,
            name=str(self.table["name"][self.valid_to_table_index[index]].as_py()) if "name" in self.table.column_names else None,
            road_class=str(self.table["class"][self.valid_to_table_index[index]].as_py()) if "class" in self.table.column_names else None,
            subclass=str(self.table["subclass"][self.valid_to_table_index[index]].as_py()) if "subclass" in self.table.column_names else None
        )

    def match(self, lon: float, lat: float, heading: Optional[float] = None) -> MatchResult:
        """Match a point to the nearest road segment within radius."""
        if self.tree is None:
            return MatchResult(matched=False)
        
        point = Point(lon, lat)
        
        # Convert radius to approximate degrees for query
        # At equator, 1 degree ≈ 111km, so radius_m / 111000 gives rough buffer
        buffer_deg = self.radius_m / 111000 * 1.5  # Add some margin
        
        # Query candidates within buffer - returns INDICES in Shapely 2.0
        candidate_indices = self.tree.query(point.buffer(buffer_deg))
        
        if len(candidate_indices) == 0:
            return MatchResult(matched=False)
        
        # Collect all valid candidates
        possible_matches = []
        
        for idx in candidate_indices:
            seg_id = self.segment_ids[idx]
            geom = self.geometries[idx]
            
            # Compute snapped point
            snapped = geom.interpolate(geom.project(point))
            
            # Compute distance in meters using projection
            px, py = self.to_metric.transform(point.x, point.y)
            sx, sy = self.to_metric.transform(snapped.x, snapped.y)
            distance_m = math.sqrt((px - sx) ** 2 + (py - sy) ** 2)
            
            if distance_m > self.radius_m:
                continue
            
            # Check alignment if heading is available
            is_aligned = True
            if heading is not None:
                bearing = self._compute_bearing(geom, snapped)
                diff = abs(heading - bearing) % 180
                if diff > 90:
                    diff = 180 - diff
                
                # Consider aligned if within 60 degrees (parallel-ish)
                # If perpendicular (> 60 deg deviation), mark as not aligned
                if diff > 60:
                    is_aligned = False
            
            possible_matches.append({
                "segment_id": seg_id,
                "distance": distance_m,
                "snapped": snapped,
                "aligned": is_aligned,
                "index": idx
            })
        
        if not possible_matches:
            return MatchResult(matched=False)
            
        # Sort by:
        # 1. Alignment (True < False? No, in Python True=1, False=0. We want True first.)
        #    So key should be (not matched["aligned"], matched["distance"])
        possible_matches.sort(key=lambda x: (not x["aligned"], x["distance"]))
        
        best = possible_matches[0]
        
        best_distance_m = best["distance"]
        best_segment_id = best["segment_id"]
        best_snapped = best["snapped"]
        best_idx = best["index"]
        
        # We need the geometry for direction calculation
        geom = self.geometries[best_idx]
        direction = self._choose_direction(geom, point, heading)
        
        return MatchResult(
            matched=True,
            segment_id=best_segment_id,
            display_segment_key=best_segment_id,  # Direction-agnostic for roadworks
            directed_id=f"{best_segment_id}:{direction}",
            match_distance_m=round(best_distance_m, 2),
            snapped_lon=round(best_snapped.x, 6),
            snapped_lat=round(best_snapped.y, 6),
        )
    
    def get_segment(self, segment_id: str) -> Optional[RoadSegment]:
        """Get a segment by ID."""
        idx = self.id_to_index.get(segment_id)
        if idx is None:
            return None
        return self._create_segment_object(idx)
    
    def get_segment_geojson(self, segment_id: str) -> Optional[dict]:
        """Get segment as GeoJSON feature."""
        segment = self.get_segment(segment_id)
        if segment is None:
            return None
        
        coords = list(segment.geometry.coords)
        return {
            "type": "Feature",
            "properties": {
                "segment_id": segment.segment_id,
                "display_segment_key": segment.segment_id,
                "name": segment.name,
                "class": segment.road_class,
                "subclass": segment.subclass,
            },
            "geometry": {
                "type": "LineString",
                "coordinates": [[round(c[0], 6), round(c[1], 6)] for c in coords],
            },
        }
