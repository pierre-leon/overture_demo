import { useState, useMemo, useCallback, Component, ErrorInfo, ReactNode } from 'react';
import Map from 'react-map-gl/maplibre';
import maplibregl from 'maplibre-gl';
import { DeckGL } from '@deck.gl/react';
import type { MapViewState } from '@deck.gl/core';
import 'maplibre-gl/dist/maplibre-gl.css';

import { Controls } from './components/Controls';
import { useRoadworksStream } from './hooks/useRoadworksStream';
import { useEnforcement } from './hooks/useEnforcement';
import { createSegmentsLayer } from './layers/segmentsLayer';
import { createRoadworksLayer } from './layers/roadworksLayer';
import { createEnforcementLayer } from './layers/enforcementLayer';

import './App.css';

// Error Boundary Component
class ErrorBoundary extends Component<{ children: ReactNode }, { hasError: boolean; error: Error | null }> {
  constructor(props: { children: ReactNode }) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error: Error) {
    return { hasError: true, error };
  }

  componentDidCatch(error: Error, errorInfo: ErrorInfo) {
    console.error("Uncaught error:", error, errorInfo);
  }

  render() {
    if (this.state.hasError) {
      return (
        <div style={{ color: 'white', padding: 20 }}>
          <h1>Something went wrong.</h1>
          <pre>{this.state.error?.toString()}</pre>
        </div>
      );
    }

    return this.props.children;
  }
}

// Carto Dark Matter style - Free, no token required
const MAP_STYLE = 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json';

// Blank style for "basemap off" mode
const BLANK_STYLE = {
  version: 8 as const,
  sources: {},
  layers: [],
};

// Initial view - will be updated when data streams in
const INITIAL_VIEW_STATE: MapViewState = {
  longitude: -0.118,
  latitude: 51.509,
  zoom: 12,
  pitch: 0,
  bearing: 0,
};

function AppContent() {
  const [viewState, setViewState] = useState<MapViewState>(INITIAL_VIEW_STATE);
  const [showBasemap, setShowBasemap] = useState(!!MAP_STYLE);
  const [speed, setSpeed] = useState(5);

  // WebSocket streaming
  const {
    events,
    segments,
    progress,
    isConnected,
    isPaused,
    isComplete,
    pause,
    resume,
    restart,
    setSpeed: setStreamSpeed,
    error: streamError
  } = useRoadworksStream({
    serverUrl: 'ws://localhost:8000',
    batchSize: 50,
    tickMs: 50,
    autoStart: true,
  });

  // Static enforcement data
  const { data: enforcementData, error: enforcementError } = useEnforcement({
    serverUrl: 'http://localhost:8000',
  });

  // Calculate unmatched count
  const unmatchedCount = useMemo(
    () => events.filter((e) => !e.matched).length,
    [events]
  );

  // Speed change handler - maps 1-10 slider to batch/tick params
  const handleSpeedChange = useCallback(
    (newSpeed: number) => {
      setSpeed(newSpeed);
      // Higher speed = larger batches, shorter ticks
      const batchSize = Math.round(20 + newSpeed * 20);
      const tickMs = Math.round(100 - newSpeed * 8);
      setStreamSpeed(batchSize, Math.max(10, tickMs));
    },
    [setStreamSpeed]
  );

  // Toggle pause/resume
  const handleTogglePause = useCallback(() => {
    if (isPaused) {
      resume();
    } else {
      pause();
    }
  }, [isPaused, pause, resume]);

  // Build deck.gl layers
  const layers = useMemo(
    () => [
      createSegmentsLayer(segments, { visible: true }),
      createRoadworksLayer(events, { visible: true }),
      createEnforcementLayer(enforcementData, { visible: true }),
    ],
    [segments, events, enforcementData]
  );

  // Auto-fit view to first segment when data arrives
  useMemo(() => {
    if (segments.size === 1 && events.length > 0) {
      const firstEvent = events[0];
      setViewState((prev) => ({
        ...prev,
        longitude: firstEvent.lon,
        latitude: firstEvent.lat,
        zoom: 13,
        transitionDuration: 1000
      }));
    }
  }, [segments.size, events.length]);

  return (
    <div className="app">
      {(!MAP_STYLE && showBasemap) && (
        <div style={{
          position: 'absolute',
          top: 10,
          right: 10,
          background: 'rgba(255,0,0,0.7)',
          color: 'white',
          padding: 10,
          borderRadius: 4,
          zIndex: 9999
        }}>
          Warning: VITE_MAPBOX_TOKEN not set. Basemap will be blank.
        </div>
      )}

      {(streamError || enforcementError) && (
        <div style={{
          position: 'absolute',
          bottom: 10,
          right: 10,
          background: 'rgba(255,0,0,0.7)',
          color: 'white',
          padding: 10,
          borderRadius: 4,
          zIndex: 9999
        }}>
          Error: {streamError || enforcementError}
        </div>
      )}

      <DeckGL
        viewState={viewState}
        onViewStateChange={(e) => setViewState(e.viewState as MapViewState)}
        controller={true}
        layers={layers}
        getTooltip={({ object }) => {
          if (!object) return null;

          // Segment tooltip
          if (object.properties?.segment_id) {
            return {
              html: `
                <div class="tooltip">
                  <strong>Segment</strong><br/>
                  ID: ${object.properties.segment_id}<br/>
                  Name: ${object.properties.name || 'Unknown'}<br/>
                  Class: ${object.properties.class || 'Unknown'}
                </div>
              `,
              style: { background: 'rgba(20, 20, 30, 0.9)', color: '#fff', padding: '8px', borderRadius: '4px' },
            };
          }

          // Event tooltip
          if (object.event_id) {
            return {
              html: `
                <div class="tooltip">
                  <strong>${object.event_type || 'Event'}</strong><br/>
                  ID: ${object.event_id}<br/>
                  Matched: ${object.matched ? 'Yes' : 'No'}<br/>
                  ${object.match_distance_m ? `Distance: ${object.match_distance_m}m` : ''}
                </div>
              `,
              style: { background: 'rgba(20, 20, 30, 0.9)', color: '#fff', padding: '8px', borderRadius: '4px' },
            };
          }

          return null;
        }}
      >
        <Map
          mapLib={maplibregl}
          mapStyle={(showBasemap && MAP_STYLE) ? MAP_STYLE : BLANK_STYLE}
          attributionControl={false}
        />
      </DeckGL>

      <Controls
        showBasemap={showBasemap}
        onToggleBasemap={() => setShowBasemap((v) => !v)}
        isPaused={isPaused}
        onTogglePause={handleTogglePause}
        onRestart={restart}
        speed={speed}
        onSpeedChange={handleSpeedChange}
        progress={progress}
        isConnected={isConnected}
        isComplete={isComplete}
        unmatchedCount={unmatchedCount}
      />
    </div>
  );
}

export default function App() {
  return (
    <ErrorBoundary>
      <AppContent />
    </ErrorBoundary>
  );
}
