"""Per-vehicle state machines and invalid-telemetry generators.

Each vehicle owns its seq, device clock, SOC, temperature, position, odometer,
charging state and fault codes, and advances them realistically on every tick.
Operational profiles script abnormal-but-valid conditions (overheat, rapid
discharge, geofence breach, low battery, charging, offline/reconnect) so the
alert engine is exercised. Invalid-reading mutators deliberately violate
documented schema rules so strict backend validation is verified end-to-end.
"""

import math
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

FAULT_POOL = ["BMS_WARN", "MOTOR_TEMP", "BRAKE_WEAR", "GPS_DRIFT", "CELL_IMBALANCE"]

PROFILES = (
    "standard",
    "overheat",
    "discharge",
    "breacher",
    "charger",
    "low_battery",
    "offline_prone",
)


class VehicleState:
    def __init__(
        self,
        vehicle_id: str,
        profile: str,
        rng: random.Random,
        home: tuple[float, float],
        clock_drift_seconds: float,
    ):
        self.vehicle_id = vehicle_id
        self.profile = profile
        self.seq = 0
        self.soc = rng.uniform(35.0, 95.0)
        if profile == "low_battery":
            self.soc = rng.uniform(5.0, 16.0)
        self.temp = rng.uniform(26.0, 34.0)
        self.speed = 0.0
        self.lat = home[0] + rng.uniform(-0.15, 0.15)
        self.lng = home[1] + rng.uniform(-0.15, 0.15)
        if profile == "breacher":
            # Start just inside the operating-area edge so a short drive
            # crosses the boundary and back within one profile cycle.
            self.lat = home[0] + 0.24
            self.lng = home[1] + 0.24
        self.home = home
        self.odometer = rng.uniform(500.0, 25000.0)
        self.charging = False
        self.fault_codes: list[str] = []
        self.heading = rng.uniform(0.0, 2.0 * math.pi)
        self.clock_offset = timedelta(seconds=rng.uniform(-clock_drift_seconds, clock_drift_seconds))
        self.phase = 0
        self.ooo_pending: Any = None

    def _move(self, interval_seconds: float, rng: random.Random) -> None:
        distance_km = self.speed * interval_seconds / 3600.0
        self.odometer += distance_km
        self.heading += rng.uniform(-0.3, 0.3)
        self.lat = max(-89.9, min(89.9, self.lat + distance_km * math.cos(self.heading) / 111.0))
        cos_lat = max(0.01, math.cos(math.radians(self.lat)))
        lng = self.lng + distance_km * math.sin(self.heading) / (111.0 * cos_lat)
        self.lng = max(-180.0, min(180.0, lng))

    def _standard(self, interval_seconds: float, rng: random.Random) -> None:
        self.charging = False
        self.speed = max(0.0, self.speed + rng.uniform(-12.0, 12.0))
        self.speed = min(self.speed, 65.0)
        if self.speed > 0:
            self.soc = max(2.0, self.soc - rng.uniform(0.03, 0.12))
        self.temp += (30.0 + self.speed * 0.18 - self.temp) * 0.2 + rng.uniform(-0.4, 0.4)
        self._move(interval_seconds, rng)
        self.fault_codes = [rng.choice(FAULT_POOL)] if rng.random() < 0.01 else []

    def _overheat(self, interval_seconds: float, rng: random.Random) -> None:
        # 8 normal ticks, 8 hot ticks (>55C triggers), 8 cool ticks (<=50C
        # resolves), repeat. Still well inside valid sensor ceiling (150C).
        step = self.phase % 24
        self._standard(interval_seconds, rng)
        if 8 <= step < 16:
            self.temp = rng.uniform(57.0, 64.0)
        elif step >= 16:
            self.temp = rng.uniform(38.0, 46.0)

    def _discharge(self, interval_seconds: float, rng: random.Random) -> None:
        # 10 normal ticks then a 6-tick hard drain (~2%/tick -> >8% inside
        # the 300s rule window), then recovery.
        step = self.phase % 22
        self._standard(interval_seconds, rng)
        if 10 <= step < 16:
            self.charging = False
            self.speed = rng.uniform(40.0, 65.0)
            self.soc = max(2.0, self.soc - rng.uniform(1.8, 2.4))

    def _breacher(self, interval_seconds: float, rng: random.Random) -> None:
        # Drive out of the operating area (~12 ticks to cross), linger
        # outside, then drive back in — trigger AND resolve the geofence.
        step = self.phase % 50
        self.charging = False
        self.speed = 65.0
        if step < 20:
            target = (self.home[0] + 0.6, self.home[1] + 0.6)
        elif step < 28:
            target = (self.lat, self.lng)  # linger outside
            self.speed = 5.0
        else:
            target = self.home
        bearing = math.atan2(target[1] - self.lng, target[0] - self.lat)
        self.heading = bearing
        self._move(interval_seconds, rng)
        self.soc = max(2.0, self.soc - 0.08)
        self.temp += (42.0 - self.temp) * 0.2
        self.fault_codes = []

    def _charger(self, interval_seconds: float, rng: random.Random) -> None:
        # Alternating 10-tick drive / 10-tick charge blocks.
        step = self.phase % 20
        if step < 10:
            self._standard(interval_seconds, rng)
        else:
            self.charging = True
            self.speed = 0.0
            self.soc = min(100.0, self.soc + rng.uniform(0.8, 1.2))
            self.temp += (36.0 - self.temp) * 0.2 + rng.uniform(-0.3, 0.3)
            self.fault_codes = []

    def _low_battery(self, interval_seconds: float, rng: random.Random) -> None:
        # Operational low-battery profile: drains into 0-15% (valid telemetry)
        # before plugging in to recover.
        step = self.phase % 20
        if step < 12:
            self.charging = False
            self.speed = rng.uniform(15.0, 35.0)
            self.soc = max(0.0, self.soc - rng.uniform(0.4, 1.0))
            self.temp += (33.0 - self.temp) * 0.2 + rng.uniform(-0.3, 0.3)
            self._move(interval_seconds, rng)
            self.fault_codes = ["BMS_WARN"] if self.soc < 10.0 else []
        else:
            self.charging = True
            self.speed = 0.0
            self.soc = min(20.0, self.soc + rng.uniform(1.0, 1.8))
            self.temp += (35.0 - self.temp) * 0.2 + rng.uniform(-0.3, 0.3)
            self.fault_codes = []

    def tick(self, now: datetime, interval_seconds: float, rng: random.Random) -> dict:
        self.phase += 1
        behavior = {
            "standard": self._standard,
            "overheat": self._overheat,
            "discharge": self._discharge,
            "breacher": self._breacher,
            "charger": self._charger,
            "low_battery": self._low_battery,
            "offline_prone": self._standard,
        }[self.profile]
        behavior(interval_seconds, rng)
        self.seq += 1
        device_ts = now + self.clock_offset
        return {
            "vehicle_id": self.vehicle_id,
            "seq": self.seq,
            "ts": device_ts.astimezone(timezone.utc).isoformat(),
            "soc_pct": round(max(0.0, min(100.0, self.soc)), 2),
            "battery_temp_c": round(max(-50.0, min(150.0, self.temp)), 2),
            "speed_kmh": round(max(0.0, self.speed), 2),
            "lat": round(max(-90.0, min(90.0, self.lat)), 6),
            "lng": round(max(-180.0, min(180.0, self.lng)), 6),
            "odometer_km": round(max(0.0, self.odometer), 2),
            "charging": self.charging,
            "fault_codes": list(self.fault_codes),
        }


def _del_field(field: str) -> Callable[[dict, random.Random], None]:
    def _mutate(bad: dict, _rng: random.Random) -> None:
        bad.pop(field, None)
    return _mutate


INVALID_SCENARIOS: list[tuple[str, Callable[[dict, random.Random], None]]] = [
    ("soc_above_100", lambda b, r: b.__setitem__("soc_pct", r.choice([101, 101.5, 125.0, 150.0]))),
    ("soc_negative", lambda b, r: b.__setitem__("soc_pct", r.choice([-1, -0.5, -15.0]))),
    ("soc_null", lambda b, _r: b.__setitem__("soc_pct", None)),
    ("soc_string", lambda b, r: b.__setitem__("soc_pct", r.choice(["75", "101", "NaN", "Infinity"]))),
    ("soc_boolean", lambda b, r: b.__setitem__("soc_pct", r.choice([True, False]))),
    ("lat_out_of_range", lambda b, r: b.__setitem__("lat", r.choice([91.0, 95.0, -90.5, -120.0]))),
    ("lng_out_of_range", lambda b, r: b.__setitem__("lng", r.choice([181.0, 200.0, -180.5, -210.0]))),
    ("missing_coordinates", lambda b, r: b.pop(r.choice(["lat", "lng"]), None)),
    ("speed_negative", lambda b, r: b.__setitem__("speed_kmh", r.choice([-0.1, -5.0, -25.0]))),
    ("speed_invalid_type", lambda b, r: b.__setitem__("speed_kmh", r.choice(["40", True, None]))),
    ("odometer_negative", lambda b, r: b.__setitem__("odometer_km", r.choice([-1.0, -50.0]))),
    ("odometer_invalid_type", lambda b, r: b.__setitem__("odometer_km", r.choice(["1200", False, None]))),
    ("seq_nonpositive", lambda b, r: b.__setitem__("seq", r.choice([0, -1, -42]))),
    ("seq_invalid_type", lambda b, r: b.__setitem__("seq", r.choice(["1", True, 1.5, None]))),
    ("missing_required_field", lambda b, r: b.pop(
        r.choice(["vehicle_id", "seq", "ts", "soc_pct", "battery_temp_c", "speed_kmh", "odometer_km", "charging"]),
        None,
    )),
    ("malformed_timestamp", lambda b, r: b.__setitem__(
        "ts", r.choice(["not-a-timestamp", "2026-11-20T10:00:00", 1700000000, "", None])
    )),
    ("battery_temp_out_of_range", lambda b, r: b.__setitem__(
        "battery_temp_c", r.choice([151.0, 200.0, -51.0, -80.0])
    )),
    ("charging_invalid_type", lambda b, r: b.__setitem__(
        "charging", r.choice(["true", "yes", 1, 0, None])
    )),
    ("fault_codes_malformed", lambda b, r: b.__setitem__(
        "fault_codes", r.choice(["P0A80", [123], [True], [""], None])
    )),
    ("vehicle_id_invalid", lambda b, r: b.__setitem__(
        "vehicle_id", r.choice(["", "   ", 12345, None])
    )),
]


def corrupt_with_reason(reading: dict, rng: random.Random) -> tuple[dict, str]:
    """Produce a deterministically chosen invalid variant and its scenario name."""
    bad = dict(reading)
    name, mutator = INVALID_SCENARIOS[rng.randrange(len(INVALID_SCENARIOS))]
    mutator(bad, rng)
    return bad, name


def corrupt(reading: dict, rng: random.Random) -> dict:
    """Produce a malformed variant: one invalid field per reading, so the
    API's per-reading validation and partial-batch success are exercised."""
    bad, _ = corrupt_with_reason(reading, rng)
    return bad
