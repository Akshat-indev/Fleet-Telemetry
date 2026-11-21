"""Measured run metrics and the terminal report. Every number here is
counted or timed during the run — nothing is estimated or invented."""

import time
from collections import Counter
from typing import Any


def percentile(sorted_values: list[float], pct: float) -> float:
    if not sorted_values:
        return 0.0
    idx = min(len(sorted_values) - 1, int(round(pct / 100.0 * (len(sorted_values) - 1))))
    return sorted_values[idx]


class Metrics:
    def __init__(self):
        self.readings_generated = 0
        self.valid_generated = 0
        self.invalid_generated = 0
        self.duplicates_generated = 0
        self.out_of_order_generated = 0
        self.delayed_generated = 0
        self.malformed_generated = 0
        self.offline_windows = 0
        self.network_failures_simulated = 0
        self.connection_errors = 0
        self.retries = 0
        self.requests_sent = 0
        self.requests_failed = 0
        self.status_counts: Counter = Counter()
        self.latencies: list[float] = []

        # Server-side outcome totals from API responses.
        self.accepted = 0
        self.duplicates_seen = 0
        self.rejected = 0

        # Reconciled per-reading quality verification counts.
        self.valid_accepted = 0
        self.invalid_rejected = 0
        self.unexpectedly_accepted_invalid = 0
        self.unexpectedly_rejected_valid = 0
        self.flagged_invalid_acceptances: list[dict[str, Any]] = []
        self.flagged_valid_rejections: list[dict[str, Any]] = []
        self.invalid_scenario_counts: Counter = Counter()

        self.started_at: float | None = None
        self.ended_at: float | None = None

    def start(self):
        self.started_at = time.monotonic()

    def stop(self):
        self.ended_at = time.monotonic()

    @property
    def actual_valid_pct(self) -> float:
        if self.readings_generated == 0:
            return 0.0
        return round(100.0 * self.valid_generated / self.readings_generated, 2)

    @property
    def actual_invalid_pct(self) -> float:
        if self.readings_generated == 0:
            return 0.0
        return round(100.0 * self.invalid_generated / self.readings_generated, 2)

    @property
    def is_successful(self) -> bool:
        """A simulation fails if any intentionally invalid reading is accepted."""
        return self.unexpectedly_accepted_invalid == 0

    def record_request(
        self,
        status: int,
        latency: float,
        body: dict | None,
        batch: list[Any] | None = None,
    ):
        self.requests_sent += 1
        self.status_counts[status] += 1
        self.latencies.append(latency)
        if status == 200 and body is not None:
            self.accepted += body.get("accepted", 0)
            self.duplicates_seen += body.get("duplicates", 0)
            self.rejected += body.get("rejected", 0)
            if batch is not None:
                results = body.get("results", [])
                for res in results:
                    idx = res.get("index")
                    if not isinstance(idx, int) or not (0 <= idx < len(batch)):
                        continue
                    emitted = batch[idx]
                    expected_valid = getattr(emitted, "expected_valid", True)
                    reason = getattr(emitted, "invalid_reason", None)
                    payload = getattr(emitted, "payload", emitted)
                    verdict = res.get("status")

                    if expected_valid:
                        if verdict == "accepted":
                            self.valid_accepted += 1
                        elif verdict == "rejected":
                            self.unexpectedly_rejected_valid += 1
                            if len(self.flagged_valid_rejections) < 20:
                                self.flagged_valid_rejections.append(
                                    {
                                        "vehicle_id": res.get("vehicle_id"),
                                        "seq": res.get("seq"),
                                        "errors": res.get("errors"),
                                    }
                                )
                    else:
                        if verdict == "rejected":
                            self.invalid_rejected += 1
                        else:
                            # Both 'accepted' and 'duplicate' for an invalid
                            # payload mean backend validation failed to reject it.
                            self.unexpectedly_accepted_invalid += 1
                            flag = {
                                "reason": reason,
                                "verdict": verdict,
                                "vehicle_id": res.get("vehicle_id"),
                                "seq": res.get("seq"),
                                "payload": payload,
                            }
                            if len(self.flagged_invalid_acceptances) < 20:
                                self.flagged_invalid_acceptances.append(flag)
                            print(
                                f"simulator: VALIDATION FAILURE — invalid reading ({reason}) "
                                f"was {verdict} by backend: vehicle_id={flag['vehicle_id']} "
                                f"seq={flag['seq']}",
                                flush=True,
                            )

    def record_failure(self):
        self.requests_sent += 1
        self.requests_failed += 1

    @property
    def duration(self) -> float:
        if self.started_at is None:
            return 0.0
        end = self.ended_at if self.ended_at is not None else time.monotonic()
        return end - self.started_at

    def report(self, cfg) -> str:
        lat = sorted(self.latencies)
        duration = self.duration
        ok = self.status_counts.get(200, 0)
        lines = [
            "",
            "================ SIMULATION REPORT ================",
            f"API endpoint            : {cfg.api_base}/v1/telemetry/batch",
            f"Vehicles                : {cfg.vehicles}",
            f"Seed                    : {cfg.seed}",
            f"Duration (measured)     : {duration:.2f} s",
            "",
            "-- Data quality distribution --",
            f"Configured distribution : {cfg.valid_pct:.1f}% valid / {cfg.invalid_pct:.1f}% invalid",
            f"Actual distribution     : {self.actual_valid_pct:.2f}% valid / {self.actual_invalid_pct:.2f}% invalid",
            f"Actual valid %          : {self.actual_valid_pct:.2f}%",
            f"Actual invalid %        : {self.actual_invalid_pct:.2f}%",
            "",
            "-- Generated (client side) --",
            f"Readings generated      : {self.readings_generated}",
            f"Valid readings generated: {self.valid_generated}",
            f"Invalid readings generated: {self.invalid_generated}",
            f"Malformed injected      : {self.malformed_generated}",
            f"Duplicates injected     : {self.duplicates_generated}",
            f"Out-of-order injected   : {self.out_of_order_generated}",
            f"Delayed injected        : {self.delayed_generated}",
            f"Offline silence windows : {self.offline_windows}",
            "",
            "-- Requests --",
            f"Requests sent           : {self.requests_sent}",
            f"Successful (HTTP 200)   : {ok}",
            f"Failed (no response)    : {self.requests_failed}",
            f"Simulated network drops : {self.network_failures_simulated}",
            f"Connection errors       : {self.connection_errors}",
            f"Retries                 : {self.retries}",
            "HTTP status distribution:",
        ]
        for status in sorted(self.status_counts):
            lines.append(f"  {status}: {self.status_counts[status]}")
        lines += [
            "",
            "-- Latency (measured per request) --",
            f"avg : {sum(lat) / len(lat) * 1000:.1f} ms" if lat else "avg : n/a",
            f"p50 : {percentile(lat, 50) * 1000:.1f} ms" if lat else "p50 : n/a",
            f"p95 : {percentile(lat, 95) * 1000:.1f} ms" if lat else "p95 : n/a",
            f"p99 : {percentile(lat, 99) * 1000:.1f} ms" if lat else "p99 : n/a",
            "",
            "-- Server-side outcomes & validation audit --",
            f"Accepted readings       : {self.accepted}",
            f"Valid readings accepted : {self.valid_accepted}",
            f"Invalid readings rejected: {self.invalid_rejected}",
            f"Duplicate readings      : {self.duplicates_seen}",
            f"Rejected readings       : {self.rejected}",
            f"Unexpectedly accepted invalid readings : {self.unexpectedly_accepted_invalid}",
            f"Unexpectedly rejected valid readings   : {self.unexpectedly_rejected_valid}",
            f"Validation status       : {'PASS' if self.is_successful else 'FAIL (invalid telemetry accepted)'}",
        ]
        if self.flagged_invalid_acceptances:
            lines.append("Flagged invalid readings accepted by backend (first 20):")
            for item in self.flagged_invalid_acceptances:
                lines.append(
                    f"  - scenario={item['reason']} verdict={item['verdict']} "
                    f"vehicle_id={item['vehicle_id']} seq={item['seq']}"
                )
        if self.flagged_valid_rejections:
            lines.append("Flagged valid readings rejected by backend (first 20):")
            for item in self.flagged_valid_rejections:
                lines.append(
                    f"  - vehicle_id={item['vehicle_id']} seq={item['seq']} errors={item['errors']}"
                )
        lines += [
            "",
            "-- Throughput --",
            f"Generated rate          : {self.readings_generated / duration:.1f} readings/s"
            if duration > 0 else "Generated rate          : n/a",
            f"Effective accepted rate : {self.accepted / duration:.1f} readings/s"
            if duration > 0 else "Effective accepted rate : n/a",
            "===================================================",
            "",
        ]
        return "\n".join(lines)
