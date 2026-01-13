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
        self.segments: dict[str, RoadSegment] = {}
        self.tree: Optional[STRtree] = None
        self.geom_to_id: dict[int, str] = {}
        
        # Transformer for WGS84 to metric (Web Mercator for distance calc)
        self.to_metric = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
        self.to_wgs84 = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)
        
        self._load_roads(roads_path)
        self._build_index()
    
    def _load_roads(self, roads_path: str) -> None:
        """Load road segments from parquet file."""
        table = pq.read_table(roads_path)
        
        for i in range(len(table)):
            segment_id = str(table["segment_id"][i].as_py())
            geometry_wkb_bytes = table["geometry_wkb"][i].as_py()
            
            try:
                geom = wkb.loads(geometry_wkb_bytes)
                if not isinstance(geom, LineString):
                    continue
            except Exception:
                continue
            
            self.segments[segment_id] = RoadSegment(
                segment_id=segment_id,
                geometry=geom,
                name=table["name"][i].as_py() if "name" in table.column_names else None,
                road_class=table["class"][i].as_py() if "class" in table.column_names else None,
                subclass=table["subclass"][i].as_py() if "subclass" in table.column_names else None,
                geometry_wkb=geometry_wkb_bytes,
            )
    
    def _build_index(self) -> None:
        """Build STRtree spatial index over road geometries."""
        # Keep lists in sync for index lookup
        self.geometries = []
        self.segment_ids = []
        
        for seg_id, segment in self.segments.items():
            self.geometries.append(segment.geometry)
            self.segment_ids.append(seg_id)
        
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
                "aligned": is_aligned
            })
        
        if not possible_matches:
            return MatchResult(matched=False)
            
        # Sort by:
        # 1. Alignment (True < False? No, in Python True=1, False=0. We want True first.)
        #    So key should be (not matched["aligned"], matched["distance"])
        #    False (0) comes before True (1)
        possible_matches.sort(key=lambda x: (not x["aligned"], x["distance"]))
        
        best = possible_matches[0]
        
        best_distance_m = best["distance"]
        best_segment_id = best["segment_id"]
        best_snapped = best["snapped"]
        
        # Check if within radius
        if best_distance_m > self.radius_m or best_segment_id is None:
            return MatchResult(matched=False)
        
        segment = self.segments[best_segment_id]
        direction = self._choose_direction(segment.geometry, point, heading)
        
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
        return self.segments.get(segment_id)
    
    def get_segment_geojson(self, segment_id: str) -> Optional[dict]:
        """Get segment as GeoJSON feature."""
        segment = self.segments.get(segment_id)
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
