"""Fleet construction: seeded profile assignment and initial conditions."""

import random

from simulator.config import SimConfig
from simulator.vehicle import VehicleState

ABNORMAL_PROFILES = ("overheat", "discharge", "breacher", "charger", "low_battery")


def build_fleet(cfg: SimConfig) -> list[VehicleState]:
    rng = random.Random(cfg.seed)
    home = (cfg.home_lat, cfg.home_lng)
    fleet = []
    for i in range(cfg.vehicles):
        roll = rng.random()
        if roll < cfg.offline_prob:
            profile = "offline_prone"
        elif roll < cfg.offline_prob + cfg.abnormal_fraction:
            profile = ABNORMAL_PROFILES[rng.randrange(len(ABNORMAL_PROFILES))]
        else:
            profile = "standard"
        fleet.append(
            VehicleState(
                vehicle_id=f"{cfg.vehicle_prefix}-{i:05d}",
                profile=profile,
                rng=rng,
                home=home,
                clock_drift_seconds=cfg.clock_drift_seconds,
            )
        )
    return fleet