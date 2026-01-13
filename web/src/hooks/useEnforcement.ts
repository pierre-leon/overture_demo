import { useState, useEffect } from 'react';

export interface EnforcementEvent {
    type: 'Feature';
    properties: {
        event_id: string;
        event_type: string;
        [key: string]: unknown;
    };
    geometry: {
        type: 'Point';
        coordinates: [number, number];
    };
}

export interface EnforcementData {
    type: 'FeatureCollection';
    features: EnforcementEvent[];
}

interface UseEnforcementOptions {
    serverUrl?: string;
    autoLoad?: boolean;
}

export function useEnforcement(options: UseEnforcementOptions = {}) {
    const apiUrl = import.meta.env.VITE_API_URL || 'ws://localhost:8000';
    // Convert WebSocket URL to HTTP for REST endpoint
    const httpUrl = apiUrl.replace(/^wss?:\/\//, 'https://').replace(':8000', '');
    const { serverUrl = httpUrl, autoLoad = true } = options;

    const [data, setData] = useState<EnforcementData | null>(null);
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);

    const load = async () => {
        setIsLoading(true);
        setError(null);

        try {
            const response = await fetch(`${serverUrl}/enforcement.geojson`);
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }
            const geojson = await response.json();
            setData(geojson);
        } catch (e) {
            setError(e instanceof Error ? e.message : 'Failed to load enforcement data');
        } finally {
            setIsLoading(false);
        }
    };

    useEffect(() => {
        if (autoLoad) {
            load();
        }
    }, [autoLoad]);

    return { data, isLoading, error, load };
}
