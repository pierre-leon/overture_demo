/**
 * Deterministic hash of a string to an HSL color.
 * Used to assign consistent colors to segment IDs.
 */
export function segmentKeyToColor(key: string | null | undefined): [number, number, number, number] {
    if (!key) {
        return [128, 128, 128, 200]; // Gray for unmatched
    }

    // Simple hash
    let hash = 0;
    for (let i = 0; i < key.length; i++) {
        const char = key.charCodeAt(i);
        hash = ((hash << 5) - hash) + char;
        hash = hash & hash; // Convert to 32bit integer
    }

    // Convert hash to HSL, then to RGB
    const hue = Math.abs(hash) % 360;
    const saturation = 70;
    const lightness = 50;

    return hslToRgba(hue, saturation, lightness, 220);
}

function hslToRgba(h: number, s: number, l: number, a: number): [number, number, number, number] {
    s /= 100;
    l /= 100;

    const c = (1 - Math.abs(2 * l - 1)) * s;
    const x = c * (1 - Math.abs((h / 60) % 2 - 1));
    const m = l - c / 2;

    let r = 0, g = 0, b = 0;

    if (h >= 0 && h < 60) {
        r = c; g = x; b = 0;
    } else if (h >= 60 && h < 120) {
        r = x; g = c; b = 0;
    } else if (h >= 120 && h < 180) {
        r = 0; g = c; b = x;
    } else if (h >= 180 && h < 240) {
        r = 0; g = x; b = c;
    } else if (h >= 240 && h < 300) {
        r = x; g = 0; b = c;
    } else {
        r = c; g = 0; b = x;
    }

    return [
        Math.round((r + m) * 255),
        Math.round((g + m) * 255),
        Math.round((b + m) * 255),
        a,
    ];
}

/**
 * Color for enforcement events by event type.
 */
export function enforcementTypeToColor(eventType: string | null | undefined): [number, number, number, number] {
    const colors: Record<string, [number, number, number, number]> = {
        speed: [255, 87, 34, 220],    // Deep orange
        parking: [156, 39, 176, 220], // Purple
        redlight: [244, 67, 54, 220], // Red
        default: [96, 125, 139, 220], // Blue gray
    };

    const type = (eventType || 'default').toLowerCase();
    return colors[type] || colors.default;
}
