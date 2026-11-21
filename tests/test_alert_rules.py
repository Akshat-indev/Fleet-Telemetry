"""Pure unit tests for rule functions — no database."""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

from app.alerts.rules import discharge_drop, overheat_state, point_in_polygon

T0 = datetime(2026, 11, 20, 12, 0, 0, tzinfo=timezone.utc)


def reading(seconds, soc, charging=False):
    return SimpleNamespace(
        ts=T0 + timedelta(seconds=seconds),
        soc_pct=Decimal(str(soc)),
        charging=charging,
    )


class TestOverheat:
    def test_two_hot_not_enough(self):
        assert overheat_state([Decimal(60), Decimal(60)]) == (False, False)

    def test_three_hot_triggers(self):
        assert overheat_state([Decimal(60)] * 3) == (True, False)

    def test_three_cool_resolves(self):
        assert overheat_state([Decimal(45)] * 3) == (False, True)

    def test_two_cool_not_enough(self):
        assert overheat_state([Decimal(45)] * 2) == (False, False)

    def test_neutral_reading_breaks_incomplete_run(self):
        # Chronological: 60, 60, 52, 60 — never reaches 3 consecutive hot.
        assert overheat_state(
            [Decimal(60), Decimal(52), Decimal(60), Decimal(60)]
        ) == (False, False)

    def test_neutral_after_three_hot_preserves_open_state(self):
        # Chronological: 60, 60, 60, 52 — most recent decisive run is trigger.
        assert overheat_state(
            [Decimal(52), Decimal(60), Decimal(60), Decimal(60)]
        ) == (True, False)

    def test_neutral_after_three_cool_preserves_resolved_state(self):
        # Chronological: 60, 60, 60, 45, 45, 45, 52 — cool run is more recent.
        assert overheat_state(
            [Decimal(52), Decimal(45), Decimal(45), Decimal(45),
             Decimal(60), Decimal(60), Decimal(60)]
        ) == (False, True)

    def test_two_cool_after_three_hot_keeps_open_state(self):
        assert overheat_state(
            [Decimal(45), Decimal(45), Decimal(60), Decimal(60), Decimal(60)]
        ) == (True, False)

    def test_exactly_55_does_not_trigger(self):
        assert overheat_state([Decimal(55)] * 3) == (False, False)

    def test_exactly_50_counts_as_cool(self):
        assert overheat_state([Decimal(50)] * 3) == (False, True)


class TestDischarge:
    def test_drop_computed_over_window(self):
        readings = [reading(240, 81.9), reading(0, 90)]
        assert discharge_drop(readings, 300) == Decimal("8.1")

    def test_charging_newest_gives_zero(self):
        readings = [reading(240, 80, charging=True), reading(0, 90)]
        assert discharge_drop(readings, 300) == Decimal("0")

    def test_charging_in_between_breaks_interval(self):
        readings = [
            reading(240, 80), reading(120, 85, charging=True), reading(0, 90),
        ]
        assert discharge_drop(readings, 300) == Decimal("0")

    def test_outside_window_excluded(self):
        readings = [reading(360, 80), reading(0, 90)]
        assert discharge_drop(readings, 300) == Decimal("0")

    def test_max_soc_in_window_used(self):
        readings = [reading(240, 80), reading(120, 95), reading(0, 90)]
        assert discharge_drop(readings, 300) == Decimal("15")


class TestGeofence:
    SQUARE = [[0.0, 0.0], [0.0, 10.0], [10.0, 10.0], [10.0, 0.0]]

    def test_inside(self):
        assert point_in_polygon(5, 5, self.SQUARE) is True

    def test_outside(self):
        assert point_in_polygon(15, 5, self.SQUARE) is False

    def test_on_edge_is_inside(self):
        assert point_in_polygon(0, 5, self.SQUARE) is True

    def test_on_vertex_is_inside(self):
        assert point_in_polygon(0, 0, self.SQUARE) is True

    def test_just_outside(self):
        assert point_in_polygon(-0.1, 5, self.SQUARE) is False