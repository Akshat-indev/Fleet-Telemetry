"""Pure rule functions over device-time-ordered windows (newest first).

No database access: deterministic and unit-testable. The engine fetches
windows and reconciles the results against active alerts.
"""

from decimal import Decimal

OVERHEAT_TRIGGER_C = Decimal("55")
OVERHEAT_RESOLVE_C = Decimal("50")
OVERHEAT_CONSECUTIVE = 3


def overheat_state(temps_newest_first: list[Decimal]) -> tuple[bool, bool]:
    """Returns (trigger, resolve). Readings between 50 and 55 break both
    runs, so a single normal reading never resets an active alert."""
    hot_run = 0
    for temp in temps_newest_first:
        if temp > OVERHEAT_TRIGGER_C:
            hot_run += 1
        else:
            break
    cool_run = 0
    for temp in temps_newest_first:
        if temp <= OVERHEAT_RESOLVE_C:
            cool_run += 1
        else:
            break
    return hot_run >= OVERHEAT_CONSECUTIVE, cool_run >= OVERHEAT_CONSECUTIVE


def discharge_drop(
    readings_newest_first: list, window_seconds: int
) -> Decimal:
    """Max SOC drop within the window ending at the newest reading.

    Each reading needs .ts, .soc_pct, .charging. The interval is broken by
    any charging reading, so charging can never count toward a discharge.
    """
    newest = readings_newest_first[0]
    if newest.charging:
        return Decimal("0")
    max_soc = newest.soc_pct
    for reading in readings_newest_first[1:]:
        if (newest.ts - reading.ts).total_seconds() > window_seconds:
            break
        if reading.charging:
            break
        max_soc = max(max_soc, reading.soc_pct)
    return max_soc - newest.soc_pct


def _on_segment(lat: float, lng: float, a: list[float], b: list[float]) -> bool:
    cross = (lng - a[1]) * (b[0] - a[0]) - (lat - a[0]) * (b[1] - a[1])
    if abs(cross) > 1e-9:
        return False
    return (
        min(a[0], b[0]) - 1e-9 <= lat <= max(a[0], b[0]) + 1e-9
        and min(a[1], b[1]) - 1e-9 <= lng <= max(a[1], b[1]) + 1e-9
    )


def point_in_polygon(lat: float, lng: float, polygon: list[list[float]]) -> bool:
    """Ray casting; points on an edge or vertex count as inside."""
    inside = False
    n = len(polygon)
    for i in range(n):
        a, b = polygon[i], polygon[(i + 1) % n]
        if _on_segment(lat, lng, a, b):
            return True
        if (a[1] > lng) != (b[1] > lng):
            lat_cross = a[0] + (lng - a[1]) * (b[0] - a[0]) / (b[1] - a[1])
            if lat < lat_cross:
                inside = not inside
    return inside