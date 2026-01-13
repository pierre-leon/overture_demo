import { GeoJsonLayer } from '@deck.gl/layers';
import { segmentKeyToColor } from '../utils/colorHash';
import type { StreamedSegment } from '../hooks/useRoadworksStream';

export function createSegmentsLayer(
    segments: Map<string, StreamedSegment>,
    options: { visible?: boolean; pickable?: boolean } = {}
) {
    const { visible = true, pickable = true } = options;

    // Convert segments map to GeoJSON FeatureCollection
    const features = Array.from(segments.values()).map((seg) => ({
        type: 'Feature' as const,
        properties: {
            segment_id: seg.segment_id,
            display_segment_key: seg.display_segment_key,
            ...seg.properties,
        },
        geometry: seg.geometry,
    }));

    const geojson = {
        type: 'FeatureCollection' as const,
        features,
    };

    return new GeoJsonLayer({
        id: 'segments-layer',
        data: geojson,
        visible,
        pickable,
        stroked: true,
        filled: false,
        lineWidthMinPixels: 3,
        lineWidthMaxPixels: 6,
        getLineColor: (f: { properties: { display_segment_key?: string } }) =>
            segmentKeyToColor(f.properties.display_segment_key),
        getLineWidth: 4,
        updateTriggers: {
            getLineColor: [segments.size],
        },
    });
}
