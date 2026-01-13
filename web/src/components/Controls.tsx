import './Controls.css';

interface ControlsProps {
    showBasemap: boolean;
    onToggleBasemap: () => void;
    isPaused: boolean;
    onTogglePause: () => void;
    onRestart: () => void;
    speed: number;
    onSpeedChange: (speed: number) => void;
    progress: {
        streamed: number;
        total: number;
        segments: number;
    };
    isConnected: boolean;
    isComplete: boolean;
    unmatchedCount: number;
}

export function Controls({
    showBasemap,
    onToggleBasemap,
    isPaused,
    onTogglePause,
    onRestart,
    speed,
    onSpeedChange,
    progress,
    isConnected,
    isComplete,
    unmatchedCount,
}: ControlsProps) {
    const percent = progress.total > 0
        ? Math.round((progress.streamed / progress.total) * 100)
        : 0;

    return (
        <div className="controls-panel">
            <div className="controls-header">
                <h3>Live Roadworks Matching</h3>
                <div className={`connection-status ${isConnected ? 'connected' : 'disconnected'}`}>
                    {isConnected ? '● Connected' : '○ Disconnected'}
                </div>
            </div>

            <div className="controls-section">
                <label className="toggle-label">
                    <input
                        type="checkbox"
                        checked={showBasemap}
                        onChange={onToggleBasemap}
                    />
                    <span>Show Basemap</span>
                </label>
            </div>

            <div className="controls-section">
                <div className="speed-control">
                    <label>Speed</label>
                    <input
                        type="range"
                        min="1"
                        max="10"
                        value={speed}
                        onChange={(e) => onSpeedChange(Number(e.target.value))}
                    />
                    <span>{speed}x</span>
                </div>
            </div>

            <div className="controls-section playback-buttons">
                <button
                    className={`btn ${isPaused ? 'btn-primary' : 'btn-secondary'}`}
                    onClick={onTogglePause}
                    disabled={!isConnected || isComplete}
                >
                    {isPaused ? '▶ Resume' : '⏸ Pause'}
                </button>
                <button className="btn btn-secondary" onClick={onRestart} disabled={!isConnected}>
                    ↺ Restart
                </button>
            </div>

            <div className="controls-section stats">
                <div className="progress-bar">
                    <div className="progress-fill" style={{ width: `${percent}%` }} />
                </div>
                <div className="stats-grid">
                    <div className="stat">
                        <span className="stat-value">{progress.streamed.toLocaleString()}</span>
                        <span className="stat-label">Events</span>
                    </div>
                    <div className="stat">
                        <span className="stat-value">{progress.segments.toLocaleString()}</span>
                        <span className="stat-label">Segments</span>
                    </div>
                    <div className="stat">
                        <span className="stat-value">{unmatchedCount.toLocaleString()}</span>
                        <span className="stat-label">Unmatched</span>
                    </div>
                </div>
            </div>

            {isComplete && (
                <div className="controls-section complete-banner">
                    ✓ Stream Complete
                </div>
            )}
        </div>
    );
}
