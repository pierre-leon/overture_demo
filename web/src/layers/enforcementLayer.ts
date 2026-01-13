import { ScatterplotLayer } from '@deck.gl/layers';
import { enforcementTypeToColor } from '../utils/colorHash';
import type { EnforcementData } from '../hooks/useEnforcement';

export function createEnforcementLayer(
    data: EnforcementData | null,
    options: { visible?: boolean; pickable?: boolean } = {}
) {
    const { visible = true, pickable = true } = options;

    if (!data) {
        return new ScatterplotLayer({
            id: 'enforcement-layer',
            data: [],
            visible: false,
        });
    }

    return new ScatterplotLayer({
        id: 'enforcement-layer',
        data: data.features,
        visible,
        pickable,
        opacity: 0.85,
        stroked: true,
        filled: true,
        radiusScale: 1,
        radiusMinPixels: 5,
        radiusMaxPixels: 12,
        lineWidthMinPixels: 2,
        getPosition: (d) => d.geometry.coordinates,
        getRadius: 7,
        getFillColor: (d) => enforcementTypeToColor(d.properties.event_type),
        getLineColor: [255, 255, 255, 200],
        getLineWidth: 2,
    });
}
