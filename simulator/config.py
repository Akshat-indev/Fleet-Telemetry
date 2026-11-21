"""Simulator configuration: dataclass defaults + CLI/env resolution."""

import argparse
import os
from dataclasses import dataclass, fields


@dataclass
class SimConfig:
    api_base: str = "http://localhost:8000"
    api_key: str | None = None
    api_secret: str | None = None

    vehicles: int = 2000
    duration_seconds: float = 60.0
    interval_seconds: float = 5.0
    batch_size: int = 250
    concurrency: int = 8
    seed: int = 42
    vehicle_prefix: str = "SIM"

    # Configurable data quality distribution (defaults: 60% valid / 40% invalid).
    valid_pct: float = 60.0
    invalid_pct: float = 40.0
    # Legacy probability alias [0, 1]; when set, overrides valid_pct/invalid_pct.
    malformed_prob: float | None = None

    # Fault injection probabilities (per generated reading unless noted).
    duplicate_prob: float = 0.05
    out_of_order_prob: float = 0.10
    delayed_prob: float = 0.05
    max_delay_seconds: float = 60.0
    network_drop_prob: float = 0.01

    # Behavior shaping.
    clock_drift_seconds: float = 30.0
    offline_prob: float = 0.02          # per-vehicle chance of a silence window
    offline_silence_seconds: float = 120.0
    offline_trigger_prob: float = 0.05  # per-tick chance an offline-prone vehicle goes silent
    abnormal_fraction: float = 0.05     # share of fleet with abnormal profiles
    home_lat: float = 13.05
    home_lng: float = 80.25

    # Reconnection behavior.
    retry_max: int = 6
    retry_backoff_seconds: float = 0.5
    request_timeout_seconds: float = 15.0

    def __post_init__(self) -> None:
        if self.malformed_prob is not None:
            if not 0.0 <= self.malformed_prob <= 1.0:
                raise ValueError("malformed_prob must be within [0, 1]")
            self.invalid_pct = round(self.malformed_prob * 100.0, 6)
            self.valid_pct = round(100.0 - self.invalid_pct, 6)

    @property
    def invalid_rate(self) -> float:
        return self.invalid_pct / 100.0

    @property
    def valid_rate(self) -> float:
        return self.valid_pct / 100.0

    def validate(self) -> None:
        if not 1 <= self.batch_size <= 500:
            raise ValueError("batch_size must be between 1 and 500")
        if self.vehicles < 1:
            raise ValueError("vehicles must be >= 1")
        if self.interval_seconds <= 0 or self.duration_seconds <= 0:
            raise ValueError("interval and duration must be positive")
        if self.concurrency < 1:
            raise ValueError("concurrency must be >= 1")
        if not 0.0 <= self.valid_pct <= 100.0:
            raise ValueError("valid_pct must be within [0, 100]")
        if not 0.0 <= self.invalid_pct <= 100.0:
            raise ValueError("invalid_pct must be within [0, 100]")
        if abs((self.valid_pct + self.invalid_pct) - 100.0) > 1e-6:
            raise ValueError("valid_pct + invalid_pct must equal 100")
        for f in fields(self):
            val = getattr(self, f.name)
            if f.name.endswith("_prob") and val is not None and not 0.0 <= val <= 1.0:
                raise ValueError(f"{f.name} must be within [0, 1]")


def _env_float(name: str) -> float | None:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return None
    return float(raw)


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m simulator",
        description="Drive simulated fleet traffic through the real telemetry API.",
    )
    p.add_argument("--api-base", default=os.environ.get("SIM_API_BASE", SimConfig.api_base))
    p.add_argument("--api-key", default=os.environ.get("SIM_API_KEY"))
    p.add_argument("--api-secret", default=os.environ.get("SIM_API_SECRET"))
    p.add_argument("--vehicles", type=int, default=SimConfig.vehicles)
    p.add_argument("--duration", type=float, default=SimConfig.duration_seconds,
                   help="seconds of traffic generation")
    p.add_argument("--interval", type=float, default=SimConfig.interval_seconds,
                   help="seconds between readings per vehicle")
    p.add_argument("--batch-size", type=int, default=SimConfig.batch_size)
    p.add_argument("--concurrency", type=int, default=SimConfig.concurrency)
    p.add_argument("--seed", type=int, default=SimConfig.seed)
    p.add_argument("--vehicle-prefix", default=SimConfig.vehicle_prefix)
    p.add_argument("--valid-pct", type=float, default=_env_float("SIM_VALID_PCT"),
                   help="percentage of generated readings that are valid (default: 60)")
    p.add_argument("--invalid-pct", type=float, default=_env_float("SIM_INVALID_PCT"),
                   help="percentage of generated readings that are intentionally invalid (default: 40)")
    p.add_argument("--malformed-prob", type=float, default=None,
                   help="legacy alias for --invalid-pct expressed as a [0, 1] probability")
    p.add_argument("--duplicate-prob", type=float, default=SimConfig.duplicate_prob)
    p.add_argument("--out-of-order-prob", type=float, default=SimConfig.out_of_order_prob)
    p.add_argument("--delayed-prob", type=float, default=SimConfig.delayed_prob)
    p.add_argument("--max-delay", type=float, default=SimConfig.max_delay_seconds,
                   help="max seconds a delayed reading is buffered")
    p.add_argument("--network-drop-prob", type=float, default=SimConfig.network_drop_prob)
    p.add_argument("--clock-drift", type=float, default=SimConfig.clock_drift_seconds)
    p.add_argument("--offline-prob", type=float, default=SimConfig.offline_prob)
    p.add_argument("--offline-silence", type=float, default=SimConfig.offline_silence_seconds)
    p.add_argument("--offline-trigger-prob", type=float, default=SimConfig.offline_trigger_prob)
    p.add_argument("--abnormal-fraction", type=float, default=SimConfig.abnormal_fraction)
    p.add_argument("--home-lat", type=float, default=SimConfig.home_lat)
    p.add_argument("--home-lng", type=float, default=SimConfig.home_lng)
    p.add_argument("--retry-max", type=int, default=SimConfig.retry_max)
    p.add_argument("--retry-backoff", type=float, default=SimConfig.retry_backoff_seconds)
    p.add_argument("--request-timeout", type=float, default=SimConfig.request_timeout_seconds)
    return p


def config_from_args(args: argparse.Namespace) -> SimConfig:
    valid_pct = args.valid_pct
    invalid_pct = args.invalid_pct
    malformed_prob = args.malformed_prob

    if malformed_prob is not None and (valid_pct is not None or invalid_pct is not None):
        raise ValueError("specify either --malformed-prob or --valid-pct/--invalid-pct, not both")

    if malformed_prob is not None:
        valid_pct = 60.0
        invalid_pct = 40.0
    elif valid_pct is None and invalid_pct is None:
        valid_pct = SimConfig.valid_pct
        invalid_pct = SimConfig.invalid_pct
    elif valid_pct is not None and invalid_pct is None:
        invalid_pct = round(100.0 - valid_pct, 6)
    elif invalid_pct is not None and valid_pct is None:
        valid_pct = round(100.0 - invalid_pct, 6)

    cfg = SimConfig(
        api_base=args.api_base.rstrip("/"),
        api_key=args.api_key,
        api_secret=args.api_secret,
        vehicles=args.vehicles,
        duration_seconds=args.duration,
        interval_seconds=args.interval,
        batch_size=args.batch_size,
        concurrency=args.concurrency,
        seed=args.seed,
        vehicle_prefix=args.vehicle_prefix,
        valid_pct=valid_pct,
        invalid_pct=invalid_pct,
        malformed_prob=malformed_prob,
        duplicate_prob=args.duplicate_prob,
        out_of_order_prob=args.out_of_order_prob,
        delayed_prob=args.delayed_prob,
        max_delay_seconds=args.max_delay,
        network_drop_prob=args.network_drop_prob,
        clock_drift_seconds=args.clock_drift,
        offline_prob=args.offline_prob,
        offline_silence_seconds=args.offline_silence,
        offline_trigger_prob=args.offline_trigger_prob,
        abnormal_fraction=args.abnormal_fraction,
        home_lat=args.home_lat,
        home_lng=args.home_lng,
        retry_max=args.retry_max,
        retry_backoff_seconds=args.retry_backoff,
        request_timeout_seconds=args.request_timeout,
    )
    cfg.validate()
    return cfg
