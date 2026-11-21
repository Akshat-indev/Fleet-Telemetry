"""CLI entry point: python -m simulator [options].

Generates seeded fleet traffic for a fixed duration with a configurable
valid/invalid data quality distribution (default: 60% valid / 40% invalid),
injects configured transport faults, sends everything through the real signed
ingestion API, verifies server verdicts against expected validity, drains
pending work on exit (including SIGINT), and prints a measured report.
"""

import asyncio
import random
import signal
import sys
import time
from collections import Counter
from datetime import datetime, timezone

from simulator.config import SimConfig, build_arg_parser, config_from_args
from simulator.fleet import build_fleet
from simulator.report import Metrics
from simulator.sender import EmittedReading, Sender
from simulator.vehicle import corrupt_with_reason


def classify_reading(
    reading: dict, cfg: SimConfig, rng: random.Random
) -> EmittedReading:
    """Deterministically produce either a valid or intentionally invalid
    reading according to cfg.invalid_rate."""
    if rng.random() < cfg.invalid_rate:
        bad, reason = corrupt_with_reason(reading, rng)
        return EmittedReading(payload=bad, expected_valid=False, invalid_reason=reason)
    return EmittedReading(payload=reading, expected_valid=True, invalid_reason=None)


async def run(cfg: SimConfig) -> int:
    metrics = Metrics()
    fleet = build_fleet(cfg)
    sender = Sender(cfg, metrics)
    sender.start()
    rng = random.Random(cfg.seed + 1)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()

    def request_stop():
        loop.call_soon_threadsafe(stop.set)

    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, request_stop)
    except NotImplementedError:  # Windows
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, lambda *_: request_stop())

    profiles = Counter(v.profile for v in fleet)
    ticks = max(1, int(cfg.duration_seconds / cfg.interval_seconds))
    print(
        f"simulator: {cfg.vehicles} vehicles -> {cfg.api_base} "
        f"({ticks} ticks x {cfg.interval_seconds}s, seed={cfg.seed}, "
        f"target={cfg.valid_pct:.1f}% valid / {cfg.invalid_pct:.1f}% invalid)",
        flush=True,
    )
    print(f"simulator: profiles {dict(profiles)}", flush=True)

    silent_until: dict[str, int] = {}
    silence_ticks = max(1, int(cfg.offline_silence_seconds / cfg.interval_seconds))
    metrics.start()

    for tick in range(ticks):
        if stop.is_set():
            break
        tick_start = time.monotonic()
        now = datetime.now(timezone.utc)
        for vehicle in fleet:
            if stop.is_set():
                break
            until = silent_until.get(vehicle.vehicle_id)
            if until is not None:
                if tick < until:
                    continue
                del silent_until[vehicle.vehicle_id]
            if (
                vehicle.profile == "offline_prone"
                and vehicle.phase > 0
                and rng.random() < cfg.offline_trigger_prob
            ):
                silent_until[vehicle.vehicle_id] = tick + silence_ticks
                metrics.offline_windows += 1
                continue

            raw_reading = vehicle.tick(now, cfg.interval_seconds, rng)
            emitted = classify_reading(raw_reading, cfg, rng)
            metrics.readings_generated += 1
            if emitted.expected_valid:
                metrics.valid_generated += 1
            else:
                metrics.invalid_generated += 1
                metrics.malformed_generated += 1
                if emitted.invalid_reason:
                    metrics.invalid_scenario_counts[emitted.invalid_reason] += 1

            # Out-of-order: stash this reading, emit it AFTER the next one.
            if vehicle.ooo_pending is not None:
                emissions = [emitted, vehicle.ooo_pending]
                vehicle.ooo_pending = None
                metrics.out_of_order_generated += 1
            elif rng.random() < cfg.out_of_order_prob:
                vehicle.ooo_pending = emitted
                continue
            else:
                emissions = [emitted]

            for item in emissions:
                if rng.random() < cfg.delayed_prob:
                    sender.delay(
                        item, time.monotonic() + rng.uniform(0.5, cfg.max_delay_seconds)
                    )
                    metrics.delayed_generated += 1
                else:
                    await sender.enqueue(item)
                if rng.random() < cfg.duplicate_prob:
                    await sender.enqueue(item)
                    metrics.duplicates_generated += 1

        remaining = cfg.interval_seconds - (time.monotonic() - tick_start)
        if remaining > 0 and not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                pass

    # Readings stashed for out-of-order delivery on the last tick still
    # belong to the run; emit them before draining.
    for vehicle in fleet:
        if vehicle.ooo_pending is not None:
            await sender.enqueue(vehicle.ooo_pending)
            vehicle.ooo_pending = None

    print("simulator: generation complete, draining pending work...", flush=True)
    await sender.finish()
    metrics.stop()
    print(metrics.report(cfg), flush=True)
    return 0 if metrics.is_successful else 1


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()
    if not args.api_key or not args.api_secret:
        parser.error(
            "credentials required: --api-key/--api-secret or SIM_API_KEY/SIM_API_SECRET "
            "(provision a device group with: python -m app.manage create-group)"
        )
    try:
        cfg = config_from_args(args)
    except ValueError as exc:
        parser.error(str(exc))
    try:
        return asyncio.run(run(cfg))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
