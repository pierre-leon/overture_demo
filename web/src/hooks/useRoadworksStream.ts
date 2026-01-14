import { useState, useEffect, useCallback, useRef } from 'react';

export interface StreamedEvent {
    event_id: string;
    lon: number;
    lat: number;
    event_type: string;
    matched: boolean;
    matched_segment_id: string | null;
    display_segment_key: string | null;
    match_distance_m: number | null;
    snapped_lon: number | null;
    snapped_lat: number | null;
    timestamp?: string;
}

export interface StreamedSegment {
    segment_id: string;
    display_segment_key: string;
    properties: {
        name?: string;
        class?: string;
        subclass?: string;
    };
    geometry: {
        type: 'LineString';
        coordinates: [number, number][];
    };
}

export interface StreamProgress {
    streamed: number;
    total: number;
    segments: number;
}

interface UseRoadworksStreamOptions {
    serverUrl?: string;
    batchSize?: number;
    tickMs?: number;
    autoStart?: boolean;
}

export function useRoadworksStream(options: UseRoadworksStreamOptions = {}) {
    const {
        serverUrl = import.meta.env.VITE_API_WS_URL || 'ws://localhost:8000',
        batchSize = 50,
        tickMs = 50,
        autoStart = true,
    } = options;

    const [events, setEvents] = useState<StreamedEvent[]>([]);
    const [segments, setSegments] = useState<Map<string, StreamedSegment>>(new Map());
    const [progress, setProgress] = useState<StreamProgress>({ streamed: 0, total: 0, segments: 0 });
    const [isConnected, setIsConnected] = useState(false);
    const [isPaused, setIsPaused] = useState(false);
    const [isComplete, setIsComplete] = useState(false);
    const [error, setError] = useState<string | null>(null);

    const wsRef = useRef<WebSocket | null>(null);

    const connect = useCallback(() => {
        if (wsRef.current?.readyState === WebSocket.OPEN) return;

        const url = `${serverUrl}/stream/roadworks?batch_size=${batchSize}&tick_ms=${tickMs}`;
        const ws = new WebSocket(url);
        wsRef.current = ws;

        ws.onopen = () => {
            setIsConnected(true);
            setError(null);
            setIsComplete(false);
        };

        ws.onmessage = async (event) => {
            try {
                let msgText: string;
                if (event.data instanceof Blob) {
                    msgText = await event.data.text();
                } else {
                    msgText = event.data;
                }
                const msg = JSON.parse(msgText);

                switch (msg.type) {
                    case 'segment':
                        setSegments((prev) => {
                            if (prev.has(msg.display_segment_key)) return prev;
                            const next = new Map(prev);
                            next.set(msg.display_segment_key, {
                                segment_id: msg.segment_id,
                                display_segment_key: msg.display_segment_key,
                                properties: msg.properties,
                                geometry: msg.geometry,
                            });
                            return next;
                        });
                        break;

                    case 'event':
                        setEvents((prev) => [...prev, msg.event]);
                        break;

                    case 'progress':
                        setProgress({
                            streamed: msg.streamed,
                            total: msg.total,
                            segments: msg.segments,
                        });
                        if (msg.streamed >= msg.total) {
                            setIsComplete(true);
                        }
                        break;

                    case 'complete':
                        setIsComplete(true);
                        break;

                    case 'error':
                        setError(msg.message);
                        break;
                }
            } catch (e) {
                console.error('Failed to parse message:', e);
            }
        };

        ws.onerror = () => {
            setError('WebSocket error');
        };

        ws.onclose = () => {
            setIsConnected(false);
        };
    }, [serverUrl, batchSize, tickMs]);

    const disconnect = useCallback(() => {
        wsRef.current?.close();
        wsRef.current = null;
    }, []);

    const pause = useCallback(() => {
        if (wsRef.current?.readyState === WebSocket.OPEN) {
            wsRef.current.send(JSON.stringify({ action: 'pause' }));
            setIsPaused(true);
        }
    }, []);

    const resume = useCallback(() => {
        if (wsRef.current?.readyState === WebSocket.OPEN) {
            wsRef.current.send(JSON.stringify({ action: 'resume' }));
            setIsPaused(false);
        }
    }, []);

    const restart = useCallback(() => {
        setEvents([]);
        setSegments(new Map());
        setProgress({ streamed: 0, total: 0, segments: 0 });
        setIsComplete(false);
        if (wsRef.current?.readyState === WebSocket.OPEN) {
            wsRef.current.send(JSON.stringify({ action: 'restart' }));
        }
    }, []);

    const setSpeed = useCallback((newBatchSize: number, newTickMs: number) => {
        if (wsRef.current?.readyState === WebSocket.OPEN) {
            wsRef.current.send(JSON.stringify({
                action: 'set_speed',
                batch_size: newBatchSize,
                tick_ms: newTickMs,
            }));
        }
    }, []);

    useEffect(() => {
        if (autoStart) {
            connect();
        }
        return () => disconnect();
    }, [autoStart, connect, disconnect]);

    return {
        events,
        segments,
        progress,
        isConnected,
        isPaused,
        isComplete,
        error,
        connect,
        disconnect,
        pause,
        resume,
        restart,
        setSpeed,
    };
}
