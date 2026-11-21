"""Async send pipeline: bounded queue -> batches (<=500) -> concurrent
HMAC-signed POSTs with retry/backoff, plus a delay buffer for late
arrivals. Retries re-sign with a fresh timestamp so replay protection
never mistakes a retry for a replay."""

import asyncio
import json
import random
import time
from dataclasses import dataclass

import httpx2 as httpx

from app.security import compute_signature
from simulator.config import SimConfig
from simulator.report import Metrics

QUEUE_MAX = 20_000
BATCH_FLUSH_SECONDS = 0.25


@dataclass(slots=True)
class EmittedReading:
    payload: dict
    expected_valid: bool = True
    invalid_reason: str | None = None


def _wrap(reading: EmittedReading | dict) -> EmittedReading:
    if isinstance(reading, EmittedReading):
        return reading
    return EmittedReading(payload=reading, expected_valid=True)


class Sender:
    def __init__(self, cfg: SimConfig, metrics: Metrics):
        self.cfg = cfg
        self.metrics = metrics
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAX)
        self.delay_buffer: list[tuple[float, EmittedReading]] = []
        self._sem = asyncio.Semaphore(cfg.concurrency)
        self._client = httpx.AsyncClient(timeout=cfg.request_timeout_seconds)
        self._tasks: set[asyncio.Task] = set()
        self._gen_done = asyncio.Event()
        self._stopping = False
        self._auth_error_logged = False
        self._worker_rngs = [
            random.Random(cfg.seed + 10_000 + i) for i in range(cfg.concurrency)
        ]

    async def enqueue(self, reading: EmittedReading | dict) -> None:
        await self.queue.put(_wrap(reading))

    def delay(self, reading: EmittedReading | dict, release_at: float) -> None:
        self.delay_buffer.append((release_at, _wrap(reading)))

    def _headers(self, body: bytes) -> dict:
        ts = str(int(time.time()))
        return {
            "X-API-Key": self.cfg.api_key,
            "X-Timestamp": ts,
            "X-Signature": compute_signature(self.cfg.api_secret, self.cfg.api_key, ts, body),
            "Content-Type": "application/json",
        }

    async def _send_batch(self, batch: list[EmittedReading], rng: random.Random) -> None:
        async with self._sem:
            body = json.dumps({"readings": [item.payload for item in batch]}).encode()
            url = self.cfg.api_base + "/v1/telemetry/batch"
            attempt = 0
            while True:
                if rng.random() < self.cfg.network_drop_prob:
                    self.metrics.network_failures_simulated += 1
                    status, latency, payload = None, 0.0, None
                else:
                    started = time.monotonic()
                    try:
                        resp = await self._client.post(
                            url, content=body, headers=self._headers(body)
                        )
                        latency = time.monotonic() - started
                        status = resp.status_code
                        payload = resp.json() if status == 200 else None
                        if status == 401 and not self._auth_error_logged:
                            self._auth_error_logged = True
                            print(
                                "simulator: authentication failed (401) — check "
                                "SIM_API_KEY / SIM_API_SECRET credentials",
                                flush=True,
                            )
                    except httpx.HTTPError:
                        latency = time.monotonic() - started
                        self.metrics.connection_errors += 1
                        status, payload = None, None

                if status == 200:
                    self.metrics.record_request(status, latency, payload, batch)
                    return
                if status in (401, 422):
                    # Credential or envelope errors will not heal on retry.
                    self.metrics.record_request(status, latency, None, batch)
                    return

                attempt += 1
                if attempt > self.cfg.retry_max:
                    if status is not None:
                        self.metrics.record_request(status, latency, None, batch)
                    else:
                        self.metrics.record_failure()
                    return
                self.metrics.retries += 1
                if status == 429:
                    wait = float(resp.headers.get("Retry-After", "1"))
                else:
                    wait = min(5.0, self.cfg.retry_backoff_seconds * (2 ** (attempt - 1)))
                await asyncio.sleep(wait)

    async def _batcher(self) -> None:
        batch: list[EmittedReading] = []
        worker = 0
        while True:
            try:
                reading = await asyncio.wait_for(self.queue.get(), BATCH_FLUSH_SECONDS)
            except asyncio.TimeoutError:
                if batch:
                    self._spawn(batch, worker)
                    batch, worker = [], worker + 1
                if self._gen_done.is_set() and self.queue.empty():
                    return
                continue
            batch.append(reading)
            if len(batch) >= self.cfg.batch_size:
                self._spawn(batch, worker)
                batch, worker = [], worker + 1

    def _spawn(self, batch: list[EmittedReading], worker: int) -> None:
        rng = self._worker_rngs[worker % len(self._worker_rngs)]
        task = asyncio.create_task(self._send_batch(batch, rng))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _delay_releaser(self) -> None:
        while not self._gen_done.is_set():
            self._release_due(time.monotonic())
            await asyncio.sleep(0.2)
        self._release_due(float("inf"))  # shutdown: everything goes out now

    def _release_due(self, now: float) -> None:
        if not self.delay_buffer:
            return
        due = [r for t, r in self.delay_buffer if t <= now]
        self.delay_buffer = [(t, r) for t, r in self.delay_buffer if t > now]
        for reading in due:
            self.queue.put_nowait(reading)

    async def finish(self) -> None:
        """Stop accepting new work and drain everything already generated."""
        self._gen_done.set()
        await self._delay_releaser_shutdown()
        await self._batcher_task
        if self._tasks:
            await asyncio.gather(*list(self._tasks))
        await self._client.aclose()

    async def _delay_releaser_shutdown(self) -> None:
        self._release_due(float("inf"))
        if self._releaser_task is not None:
            await self._releaser_task

    def start(self) -> None:
        self._batcher_task = asyncio.create_task(self._batcher())
        self._releaser_task = asyncio.create_task(self._delay_releaser())
