"""Simulator unit tests: structure, determinism, scale, data-quality
distribution (60% valid / 40% invalid default), and each profile's behavior.
Generation is pure and seeded — no network needed."""

import random
from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from app.alerts.rules import point_in_polygon
from app.schemas import TelemetryReadingIn
from simulator.__main__ import classify_reading
from simulator.config import SimConfig, build_arg_parser, config_from_args
from simulator.fleet import build_fleet
from simulator.report import Metrics
from simulator.sender import EmittedReading
from simulator.vehicle import INVALID_SCENARIOS, VehicleState, corrupt, corrupt_with_reason

NOW = datetime(2026, 11, 20, 12, 0, 0, tzinfo=timezone.utc)
CHENNAI = [[12.8, 79.9], [13.3, 79.9], [13.3, 80.5], [12.8, 80.5]]


def make_cfg(**kw):
    defaults = dict(vehicles=10, duration_seconds=10, interval_seconds=5, seed=123)
    defaults.update(kw)
    return SimConfig(**defaults)


def test_single_vehicle_reading_validates():
    fleet = build_fleet(make_cfg(vehicles=1))
    reading = fleet[0].tick(NOW, 5.0, random.Random(1))
    model = TelemetryReadingIn.model_validate(reading)
    assert model.vehicle_id == fleet[0].vehicle_id
    assert model.seq == 1


def test_2000_vehicle_generation_structure():
    cfg = make_cfg(vehicles=2000)
    fleet = build_fleet(cfg)
    assert len(fleet) == 2000
    assert len({v.vehicle_id for v in fleet}) == 2000
    rng = random.Random(cfg.seed)
    for vehicle in fleet:
        reading = vehicle.tick(NOW, 5.0, rng)
        TelemetryReadingIn.model_validate(reading)
        assert reading["seq"] == 1
        assert 0.0 <= reading["soc_pct"] <= 100.0
        assert -90.0 <= reading["lat"] <= 90.0
        assert -180.0 <= reading["lng"] <= 180.0


def test_small_fleet_multi_tick_invariants():
    cfg = make_cfg(vehicles=80, abnormal_fraction=0.5, offline_prob=0.2)
    fleet = build_fleet(cfg)
    profiles = {v.profile for v in fleet}
    assert profiles == {
        "standard",
        "overheat",
        "discharge",
        "breacher",
        "charger",
        "low_battery",
        "offline_prone",
    }
    rng = random.Random(cfg.seed)
    odometers = {v.vehicle_id: round(v.odometer, 2) for v in fleet}
    for tick in range(20):
        now = NOW + timedelta(seconds=5 * tick)
        for vehicle in fleet:
            reading = vehicle.tick(now, 5.0, rng)
            TelemetryReadingIn.model_validate(reading)
            assert reading["seq"] == tick + 1
            assert reading["odometer_km"] >= odometers[vehicle.vehicle_id]
            odometers[vehicle.vehicle_id] = reading["odometer_km"]
            assert 0.0 <= reading["soc_pct"] <= 100.0


def test_determinism_with_fixed_seed():
    def generate(seed):
        cfg = make_cfg(vehicles=25, seed=seed, abnormal_fraction=0.4)
        fleet = build_fleet(cfg)
        rng = random.Random(seed + 1)
        out = []
        for tick in range(10):
            now = NOW + timedelta(seconds=5 * tick)
            for v in fleet:
                emitted = classify_reading(v.tick(now, 5.0, rng), cfg, rng)
                out.append((emitted.expected_valid, emitted.invalid_reason, emitted.payload))
        return out

    assert generate(99) == generate(99)
    assert generate(99) != generate(100)


def test_clock_drift_within_bounds():
    cfg = make_cfg(vehicles=200, clock_drift_seconds=30)
    fleet = build_fleet(cfg)
    rng = random.Random(1)
    for vehicle in fleet:
        reading = vehicle.tick(NOW, 5.0, rng)
        ts = datetime.fromisoformat(reading["ts"])
        assert abs((ts - NOW).total_seconds()) <= 30.0


@pytest.mark.parametrize("choice", range(len(INVALID_SCENARIOS)))
def test_every_invalid_scenario_fails_validation(choice):
    fleet = build_fleet(make_cfg(vehicles=1))
    reading = fleet[0].tick(NOW, 5.0, random.Random(1))

    # Exercise several random seeds per scenario to cover all sub-choices.
    for sub_seed in range(10):
        class Fixed(random.Random):
            def randrange(self, n):
                return choice

        bad, reason = corrupt_with_reason(reading, Fixed(sub_seed))
        assert reason == INVALID_SCENARIOS[choice][0]
        with pytest.raises(ValidationError):
            TelemetryReadingIn.model_validate(bad)


def test_corrupt_helper_returns_invalid_reading():
    fleet = build_fleet(make_cfg(vehicles=1))
    reading = fleet[0].tick(NOW, 5.0, random.Random(1))
    for seed in range(25):
        bad = corrupt(reading, random.Random(seed))
        with pytest.raises(ValidationError):
            TelemetryReadingIn.model_validate(bad)


def test_default_60_40_valid_invalid_distribution():
    cfg = SimConfig()
    assert cfg.valid_pct == 60.0
    assert cfg.invalid_pct == 40.0
    cfg.validate()

    fleet = build_fleet(make_cfg(vehicles=100, abnormal_fraction=0.3, seed=42))
    rng = random.Random(43)
    valid_count = 0
    invalid_count = 0
    for tick in range(20):
        now = NOW + timedelta(seconds=5 * tick)
        for v in fleet:
            emitted = classify_reading(v.tick(now, 5.0, rng), cfg, rng)
            if emitted.expected_valid:
                valid_count += 1
                TelemetryReadingIn.model_validate(emitted.payload)
            else:
                invalid_count += 1
                with pytest.raises(ValidationError):
                    TelemetryReadingIn.model_validate(emitted.payload)

    total = valid_count + invalid_count
    assert total == 2000
    valid_pct = 100.0 * valid_count / total
    invalid_pct = 100.0 * invalid_count / total
    assert 56.0 <= valid_pct <= 64.0
    assert 36.0 <= invalid_pct <= 44.0


@pytest.mark.parametrize("target_valid,target_invalid", [(80.0, 20.0), (25.0, 75.0), (100.0, 0.0), (0.0, 100.0)])
def test_configurable_valid_invalid_distribution(target_valid, target_invalid):
    cfg = make_cfg(vehicles=50, valid_pct=target_valid, invalid_pct=target_invalid, seed=7)
    cfg.validate()
    fleet = build_fleet(cfg)
    rng = random.Random(8)

    valid_count = 0
    invalid_count = 0
    for tick in range(20):
        now = NOW + timedelta(seconds=5 * tick)
        for v in fleet:
            emitted = classify_reading(v.tick(now, 5.0, rng), cfg, rng)
            if emitted.expected_valid:
                valid_count += 1
                TelemetryReadingIn.model_validate(emitted.payload)
            else:
                invalid_count += 1
                with pytest.raises(ValidationError):
                    TelemetryReadingIn.model_validate(emitted.payload)

    total = valid_count + invalid_count
    actual_valid_pct = 100.0 * valid_count / total
    if target_valid in (0.0, 100.0):
        assert actual_valid_pct == target_valid
    else:
        assert abs(actual_valid_pct - target_valid) <= 5.0


def test_config_cli_distribution_flags_and_validation():
    parser = build_arg_parser()
    # Defaults: 60% valid / 40% invalid
    cfg_default = config_from_args(parser.parse_args([]))
    assert cfg_default.valid_pct == 60.0 and cfg_default.invalid_pct == 40.0

    # Single-flag derivation
    cfg_v = config_from_args(parser.parse_args(["--valid-pct", "75"]))
    assert cfg_v.valid_pct == 75.0 and cfg_v.invalid_pct == 25.0

    cfg_i = config_from_args(parser.parse_args(["--invalid-pct", "30"]))
    assert cfg_i.valid_pct == 70.0 and cfg_i.invalid_pct == 30.0

    # Mismatched sum rejected
    with pytest.raises(ValueError, match="must equal 100"):
        config_from_args(parser.parse_args(["--valid-pct", "70", "--invalid-pct", "20"]))

    # Legacy alias
    cfg_legacy = config_from_args(parser.parse_args(["--malformed-prob", "0.15"]))
    assert cfg_legacy.valid_pct == 85.0 and cfg_legacy.invalid_pct == 15.0


def test_metrics_flags_unexpectedly_accepted_invalid_reading():
    cfg = SimConfig()
    metrics = Metrics()
    metrics.readings_generated = 2
    metrics.valid_generated = 1
    metrics.invalid_generated = 1

    batch = [
        EmittedReading(payload={"vehicle_id": "V1", "seq": 1, "soc_pct": 50}, expected_valid=True),
        EmittedReading(
            payload={"vehicle_id": "V1", "seq": 2, "soc_pct": 101},
            expected_valid=False,
            invalid_reason="soc_above_100",
        ),
    ]
    # Simulate a broken backend silently accepting SOC 101.
    fake_response = {
        "accepted": 2,
        "duplicates": 0,
        "rejected": 0,
        "results": [
            {"index": 0, "vehicle_id": "V1", "seq": 1, "status": "accepted"},
            {"index": 1, "vehicle_id": "V1", "seq": 2, "status": "accepted"},
        ],
    }
    metrics.record_request(200, 0.01, fake_response, batch)

    assert metrics.valid_accepted == 1
    assert metrics.invalid_rejected == 0
    assert metrics.unexpectedly_accepted_invalid == 1
    assert metrics.is_successful is False
    assert len(metrics.flagged_invalid_acceptances) == 1
    assert metrics.flagged_invalid_acceptances[0]["reason"] == "soc_above_100"
    report = metrics.report(cfg)
    assert "FAIL (invalid telemetry accepted)" in report
    assert "Unexpectedly accepted invalid readings : 1" in report


def test_overheat_profile_triggers_and_cools():
    vehicle = VehicleState("V-HOT", "overheat", random.Random(5), (13.05, 80.25), 0.0)
    rng = random.Random(5)
    temps = [vehicle.tick(NOW + timedelta(seconds=5 * t), 5.0, rng)["battery_temp_c"]
             for t in range(24)]
    assert any(t > 55.0 for t in temps[7:15])
    assert all(t > 55.0 for t in temps[8:15])  # >=3 consecutive -> trigger
    assert all(t <= 50.0 for t in temps[15:23])  # >=3 consecutive -> resolve


def test_discharge_profile_exceeds_rule_threshold():
    vehicle = VehicleState("V-DRAIN", "discharge", random.Random(5), (13.05, 80.25), 0.0)
    rng = random.Random(5)
    socs = [vehicle.tick(NOW + timedelta(seconds=5 * t), 5.0, rng)["soc_pct"]
            for t in range(22)]
    window = socs[9:15]
    assert max(window) - min(window) > 8.0  # rule threshold inside 300s


def test_breacher_leaves_and_reenters_geofence():
    vehicle = VehicleState("V-BREACH", "breacher", random.Random(5), (13.05, 80.25), 0.0)
    rng = random.Random(5)
    inside = []
    for t in range(50):
        r = vehicle.tick(NOW + timedelta(seconds=5 * t), 5.0, rng)
        inside.append(point_in_polygon(r["lat"], r["lng"], CHENNAI))
    assert inside[0] is True
    assert any(flag is False for flag in inside)
    assert inside[-1] is True


def test_charger_profile_transitions():
    vehicle = VehicleState("V-CHARGE", "charger", random.Random(5), (13.05, 80.25), 0.0)
    rng = random.Random(5)
    states = [vehicle.tick(NOW + timedelta(seconds=5 * t), 5.0, rng)["charging"]
              for t in range(20)]
    assert any(states) and not all(states)
    assert states[0] is False and states[10] is True


def test_low_battery_profile_valid_and_reaches_low_soc():
    vehicle = VehicleState("V-LOW", "low_battery", random.Random(5), (13.05, 80.25), 0.0)
    rng = random.Random(5)
    readings = [vehicle.tick(NOW + timedelta(seconds=5 * t), 5.0, rng) for t in range(20)]
    for r in readings:
        TelemetryReadingIn.model_validate(r)
    assert min(r["soc_pct"] for r in readings) < 15.0
    assert any(r["charging"] for r in readings)
