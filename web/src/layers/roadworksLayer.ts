import { ScatterplotLayer } from '@deck.gl/layers';
import { segmentKeyToColor } from '../utils/colorHash';
import type { StreamedEvent } from '../hooks/useRoadworksStream';

export function createRoadworksLayer(
    events: StreamedEvent[],
    options: { visible?: boolean; pickable?: boolean; showSnapped?: boolean } = {}
) {
    const { visible = true, pickable = true, showSnapped = false } = options;

    return new ScatterplotLayer<StreamedEvent>({
        id: 'roadworks-layer',
        data: events,
        visible,
        pickable,
        opacity: 0.9,
        stroked: true,
        filled: true,
        radiusScale: 1,
        radiusMinPixels: 4,
        radiusMaxPixels: 10,
        lineWidthMinPixels: 1,
        getPosition: (d) => {
            if (showSnapped && d.snapped_lon != null && d.snapped_lat != null) {
                return [d.snapped_lon, d.snapped_lat];
            }
            return [d.lon, d.lat];
        },
        getRadius: 6,
        getFillColor: (d) => segmentKeyToColor(d.display_segment_key),
        getLineColor: [255, 255, 255, 150],
        getLineWidth: 1,
        updateTriggers: {
            getPosition: [events.length, showSnapped],
            getFillColor: [events.length],
        },
    });
}
