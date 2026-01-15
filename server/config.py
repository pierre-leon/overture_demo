from dataclasses import dataclass, field
from typing import Optional
import os
from pathlib import Path

@dataclass
class ColumnMapping:
    """Mapping of expected column names to actual column names in events.parquet."""
    lon: str = "lon"
    lat: str = "lat"
    event_type: str = "event_type"
    timestamp: Optional[str] = "captured_at"
    heading: Optional[str] = "ego_heading"
    event_id: Optional[str] = "event_id"


@dataclass
class MatchingConfig:
    """Configuration for road segment matching."""
    radius_m: float = 50.0  # Max distance for matching
    batch_size: int = 50  # Events per WebSocket message
    tick_interval_ms: int = 50  # Milliseconds between batches


@dataclass
class ServerConfig:
    """Main server configuration."""
    events_path: str = "data/events_all.parquet"
    roads_path: str = "data/overture_roads.parquet"
    columns: ColumnMapping = field(default_factory=ColumnMapping)
    matching: MatchingConfig = field(default_factory=MatchingConfig)

    @classmethod
    def from_env(cls) -> "ServerConfig":
        """Load config from environment variables with defaults."""
        # Resolve paths relative to project root if running from server/ dir
        cwd = Path.cwd()
        root = cwd.parent if cwd.name == "server" else cwd
        
        default_events = str(root / "data" / "events_all.parquet")
        default_roads = str(root / "data" / "overture_roads.parquet")

        return cls(
            events_path=os.getenv("EVENTS_PATH", default_events),
            roads_path=os.getenv("ROADS_PATH", default_roads),
            columns=ColumnMapping(
                lon=os.getenv("COL_LON", "lon"),
                lat=os.getenv("COL_LAT", "lat"),
                event_type=os.getenv("COL_EVENT_TYPE", "event_type"),
                timestamp=os.getenv("COL_TIMESTAMP", "captured_at"),
                heading=os.getenv("COL_HEADING", "ego_heading"),
                event_id=os.getenv("COL_EVENT_ID", "event_id"),
            ),
            matching=MatchingConfig(
                radius_m=float(os.getenv("MATCH_RADIUS_M", "50")),
                batch_size=int(os.getenv("BATCH_SIZE", "50")),
                tick_interval_ms=int(os.getenv("TICK_INTERVAL_MS", "50")),
            ),
        )
