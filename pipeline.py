"""XAU Dynamics · TickForge - asynchronous high-throughput tick ingestion engine.

The engine is a single-writer producer/consumer pair joined by a bounded
``asyncio.Queue``, which is the backpressure boundary of the whole service:

    feed ──▶ producer ──▶ bounded queue ──▶ consumer ──▶ batched Cosmos writes
                                              │
                                              ├─▶ numpy batch context
                                              └─▶ pandas OHLCV bars

Design invariants
-----------------
* **One writer per instrument shard.** A second replica would open its own feed
  and persist every tick twice, corrupting the realised-volatility figures that
  RiskShield turns into a signed policy. Scale by sharding instruments across
  Container Apps *jobs*, never by raising ``--max-replicas``.
* **Nothing is lost on shutdown.** SIGTERM sets a stop event; the producer
  stops, the consumer drains the queue, the partial batch is flushed, and the
  aggregator emits its trailing bar - all inside the configured grace window.
* **Deterministic document ids.** ``id`` is derived from symbol, event time,
  and sequence, so a replayed batch upserts over itself instead of duplicating.
* **Timezone-aware throughout.** Every timestamp is an aware UTC ``datetime``;
  ``datetime.utcnow()`` (deprecated since 3.12) appears nowhere.

Target runtime: Python 3.10+.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import math
import random
import signal
import sys
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Final

import numpy as np

from config import (
    ConfigError,
    CosmosSettings,
    FeedSettings,
    Settings,
    configure_logging,
    get_settings,
)

logger = logging.getLogger("tickforge.pipeline")

#: Coroutine invoked by a source for each parsed tick.
TickHandler = Callable[["Tick"], Awaitable[None]]

SECONDS_PER_YEAR: Final[float] = 365.0 * 24.0 * 3_600.0
IDLE_POLL_SECONDS: Final[float] = 0.25
EXIT_OK: Final[int] = 0
EXIT_FAILURE: Final[int] = 1
EXIT_CONFIG: Final[int] = 78  # EX_CONFIG from sysexits.h
EXIT_INTERRUPT: Final[int] = 130


# --------------------------------------------------------------------------- #
# Time helpers
# --------------------------------------------------------------------------- #


def utc_now() -> datetime:
    """Timezone-aware UTC now. The only clock this service reads."""
    return datetime.now(timezone.utc)


def iso_z(moment: datetime) -> str:
    """Format as RFC 3339 with microseconds and a literal ``Z`` suffix."""
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return (
        moment.astimezone(timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def parse_event_time(value: Any) -> datetime | None:
    """Coerce a feed timestamp to aware UTC, or ``None`` if unparseable.

    Accepts epoch seconds, milliseconds, microseconds, or an ISO 8601 string.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        magnitude = abs(float(value))
        if magnitude >= 1e17:
            seconds = float(value) / 1e9  # nanoseconds
        elif magnitude >= 1e14:
            seconds = float(value) / 1e6  # microseconds
        elif magnitude >= 1e11:
            seconds = float(value) / 1e3  # milliseconds
        else:
            seconds = float(value)
        try:
            return datetime.fromtimestamp(seconds, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith(("Z", "z")):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


# --------------------------------------------------------------------------- #
# Domain model
# --------------------------------------------------------------------------- #

#: Characters Cosmos DB forbids in an item id or partition-key value.
_KEY_UNSAFE: Final[str] = "/\\?#"

#: Decimal places retained on derived prices. Five covers XAU (2-3) and the FX
#: majors (5) without reintroducing binary float noise into the audit trail.
PRICE_DECIMALS: Final[int] = 5


def safe_key(value: str) -> str:
    """Strip characters Cosmos DB rejects in ``id`` and partition-key values."""
    for character in _KEY_UNSAFE:
        value = value.replace(character, "_")
    return value.strip()


@dataclass(slots=True)
class Tick:
    """One normalised quote, with both feed time and local ingest time."""

    symbol: str
    bid: float
    ask: float
    volume: int
    event_time: datetime
    ingest_time: datetime
    sequence: int
    source: str

    @property
    def mid(self) -> float:
        return round((self.bid + self.ask) / 2.0, PRICE_DECIMALS)

    @property
    def spread(self) -> float:
        return round(self.ask - self.bid, PRICE_DECIMALS)

    @property
    def partition_key(self) -> str:
        """Symbol plus UTC hour bucket.

        Partitioning on ``symbol`` alone would funnel a single-instrument feed
        into one logical partition and hit the 20 GB ceiling; the hour bucket
        keeps partitions bounded while leaving single-partition range queries
        cheap for any intraday window.
        """
        return safe_key(f"{self.symbol}-{self.event_time:%Y%m%d%H}")

    @property
    def document_id(self) -> str:
        """Deterministic id, so a replayed batch upserts instead of duplicating."""
        micros = int(self.event_time.timestamp() * 1_000_000)
        return safe_key(f"{self.symbol}-{micros}-{self.sequence}")


    def to_document(self, *, shard_id: str, ttl_seconds: int) -> dict[str, Any]:
        """Render the Cosmos DB item. Key names match the documented schema."""
        document: dict[str, Any] = {
            "id": self.document_id,
            "pk": self.partition_key,
            "symbol": self.symbol,
            "bid": self.bid,
            "ask": self.ask,
            "mid": self.mid,
            "spread": self.spread,
            "volume": self.volume,
            "timestamp": iso_z(self.event_time),
            "ingested_at": iso_z(self.ingest_time),
            "latency_ms": round(
                (self.ingest_time - self.event_time).total_seconds() * 1_000.0, 3
            ),
            "sequence": self.sequence,
            "source": self.source,
            "shard": shard_id,
        }
        if ttl_seconds > 0:
            document["ttl"] = ttl_seconds
        return document


@dataclass(slots=True)
class Metrics:
    """Counters exported through the health endpoint and the metrics log line.

    Every received tick ends in exactly one terminal state, so the counters
    obey a conservation law that :meth:`unaccounted` checks at shutdown::

        ticks_received == ticks_written + ticks_failed + ticks_dropped + in_flight

    ``ticks_enqueued`` is deliberately *not* part of that sum. It counts
    admissions to the queue, and under ``drop_oldest`` a tick can be admitted
    and later evicted, landing in both ``ticks_enqueued`` and ``ticks_dropped``.
    Read it as "made it past the backpressure boundary", not as a balance.
    """

    started_at: str = field(default_factory=lambda: iso_z(utc_now()))
    ticks_received: int = 0
    ticks_enqueued: int = 0
    ticks_dropped: int = 0
    queue_full_events: int = 0
    malformed_messages: int = 0
    batches_flushed: int = 0
    ticks_written: int = 0
    #: Tick documents the sink rejected. Stream-scoped, unlike ``write_failures``.
    ticks_failed: int = 0
    write_failures: int = 0
    context_documents_written: int = 0
    bars_written: int = 0
    feed_reconnects: int = 0
    last_tick_at: str | None = None
    last_flush_at: str | None = None

    def unaccounted(self, in_flight: int = 0) -> int:
        """Ticks whose fate is unknown. Non-zero means silent data loss."""
        return self.ticks_received - (
            self.ticks_written + self.ticks_failed + self.ticks_dropped + in_flight
        )

    def snapshot(self) -> dict[str, Any]:
        return {
            "started_at": self.started_at,
            "ticks_received": self.ticks_received,
            "ticks_enqueued": self.ticks_enqueued,
            "ticks_dropped": self.ticks_dropped,
            "queue_full_events": self.queue_full_events,
            "malformed_messages": self.malformed_messages,
            "batches_flushed": self.batches_flushed,
            "ticks_written": self.ticks_written,
            "ticks_failed": self.ticks_failed,
            "write_failures": self.write_failures,
            "context_documents_written": self.context_documents_written,
            "bars_written": self.bars_written,
            "feed_reconnects": self.feed_reconnects,
            "last_tick_at": self.last_tick_at,
            "last_flush_at": self.last_flush_at,
        }


# --------------------------------------------------------------------------- #
# Tick sources
# --------------------------------------------------------------------------- #


async def sleep_unless_stopped(stop: asyncio.Event, seconds: float) -> bool:
    """Sleep for ``seconds``; return ``True`` if the stop event fired first."""
    if seconds <= 0:
        return stop.is_set()
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        return False
    return True


class TickSource:
    """A source pushes ticks into ``emit`` until ``stop`` is set."""

    name: str = "base"

    def __init__(self, feed: FeedSettings, metrics: Metrics) -> None:
        self._feed = feed
        self._metrics = metrics
        self._sequence = 0

    def _next_sequence(self) -> int:
        self._sequence += 1
        return self._sequence

    async def run(self, stop: asyncio.Event, emit: TickHandler) -> None:
        raise NotImplementedError

    async def aclose(self) -> None:
        return None


class SimulatedTickSource(TickSource):
    """Deterministic load generator: a bounded random walk at a fixed cadence.

    This is a load-testing harness, not a market feed. It exists so latency,
    batching, and shutdown behaviour can be exercised in CI without a broker
    session, and ``config.Settings.validate`` refuses to let it run against a
    production tenant.
    """

    name = "simulated"

    def __init__(self, feed: FeedSettings, metrics: Metrics) -> None:
        super().__init__(feed, metrics)
        self._random = random.Random(feed.sim_seed)
        self._price = feed.sim_base_price


    async def run(self, stop: asyncio.Event, emit: TickHandler) -> None:
        feed = self._feed
        interval = feed.sim_tick_interval_seconds
        logger.info(
            "simulated feed started",
            extra={
                "symbol": feed.symbol,
                "base_price": feed.sim_base_price,
                "tick_interval_seconds": interval,
                "seed": feed.sim_seed,
            },
        )
        # Pace against the monotonic clock rather than sleeping a fixed amount,
        # so per-iteration work does not accumulate into cadence drift.
        next_due = time.monotonic()
        while not stop.is_set():
            self._price = max(
                0.01,
                self._price
                + self._random.uniform(-feed.sim_price_step, feed.sim_price_step),
            )
            now = utc_now()
            await emit(
                Tick(
                    symbol=feed.symbol,
                    bid=round(self._price - feed.sim_half_spread, PRICE_DECIMALS),
                    ask=round(self._price + feed.sim_half_spread, PRICE_DECIMALS),
                    volume=self._random.randint(
                        feed.sim_min_volume, feed.sim_max_volume
                    ),
                    event_time=now,
                    ingest_time=now,
                    sequence=self._next_sequence(),
                    source=self.name,
                )
            )
            next_due += interval
            delay = next_due - time.monotonic()
            if delay <= 0:
                # Behind schedule: resynchronise instead of spinning on a
                # deficit that can never be repaid.
                next_due = time.monotonic()
                await asyncio.sleep(0)
                continue
            if await sleep_unless_stopped(stop, delay):
                break
        logger.info("simulated feed stopped", extra={"ticks": self._sequence})


def first_number(record: dict[str, Any], keys: Sequence[str]) -> float | None:
    """Return the first key that holds a finite number, tolerating aliases."""
    for key in keys:
        if key not in record:
            continue
        value = record[key]
        if isinstance(value, bool) or value is None:
            continue
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(number):
            return number
    return None


class WebSocketTickSource(TickSource):
    """Live feed client with exponential backoff, jitter, and idle detection."""

    name = "live"

    async def run(self, stop: asyncio.Event, emit: TickHandler) -> None:
        # Imported lazily so simulated and CI runs do not require the wheel.
        import websockets

        feed = self._feed
        headers = (
            {"Authorization": f"Bearer {feed.api_key}"} if feed.api_key else None
        )
        backoff = feed.reconnect_initial_seconds
        attempt = 0

        while not stop.is_set():
            attempt += 1
            try:
                socket = await websockets.connect(
                    feed.url,
                    open_timeout=feed.open_timeout_seconds,
                    ping_interval=feed.ping_interval_seconds,
                    ping_timeout=feed.ping_timeout_seconds,
                    close_timeout=feed.close_timeout_seconds,
                    max_size=feed.max_message_bytes,
                    extra_headers=headers,
                )
                try:
                    logger.info(
                        "feed connected",
                        extra={"url": feed.url, "attempt": attempt},
                    )
                    backoff = feed.reconnect_initial_seconds
                    if await self._consume_socket(socket, stop, emit):
                        break
                finally:
                    self._abort(socket)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - any fault must reconnect
                if stop.is_set():
                    break
                self._metrics.feed_reconnects += 1
                delay = self._jittered(backoff)
                logger.warning(
                    "feed connection failed; retrying",
                    extra={
                        "error": f"{type(exc).__name__}: {exc}",
                        "retry_in_seconds": round(delay, 3),
                        "attempt": attempt,
                    },
                )
                if await sleep_unless_stopped(stop, delay):
                    break
                backoff = min(backoff * 2.0, feed.reconnect_max_seconds)

        logger.info("live feed stopped", extra={"ticks": self._sequence})


    @staticmethod
    def _abort(socket: Any) -> None:
        """Tear the connection down without awaiting a closing handshake.

        ``websockets`` only guarantees ``close()`` inside ``4 *
        close_timeout``, and it shields its internal tasks, so the await is not
        reliably interruptible. A venue that keeps streaming while we shut down
        holds the handshake open for all of it: the client must read and discard
        every buffered data frame before it sees the peer's CLOSE echo.
        Measured cost of waiting politely under a 4,000 frame/s flood: the whole
        shutdown grace window, which risks the platform sending SIGKILL in the
        middle of a flush. Every tick is already persisted by this point and a
        market-data subscriber vanishing is a case every venue handles, so drop
        the socket instead.
        """
        transport = getattr(socket, "transport", None)
        if transport is not None:
            with contextlib.suppress(Exception):
                transport.abort()
                return
        with contextlib.suppress(Exception):  # pragma: no cover - defensive
            asyncio.get_running_loop().create_task(socket.close())

    def _jittered(self, backoff: float) -> float:
        """Spread reconnect attempts so shards never stampede a feed together."""
        jitter = self._feed.reconnect_jitter
        if jitter <= 0:
            return backoff
        return max(0.05, backoff * (1.0 + random.uniform(-jitter, jitter)))

    async def _consume_socket(
        self, socket: Any, stop: asyncio.Event, emit: TickHandler
    ) -> bool:
        """Drain one session. Returns ``True`` only when stop ended it.

        ``recv`` is *raced* against the stop event rather than polled between
        frames. Under a saturating feed a poll-only loop never reaches the
        flag check, so a shutdown stalls until the receive timeout expires -
        measured at 20 s against a 5,700 tick/s flood. Racing makes the exit
        immediate at any rate.
        """
        timeout = self._feed.recv_timeout_seconds
        stopper = asyncio.create_task(stop.wait(), name="feed-stop-watch")
        try:
            while not stop.is_set():
                receiver = asyncio.create_task(socket.recv(), name="feed-recv")
                done, _ = await asyncio.wait(
                    {receiver, stopper},
                    timeout=timeout,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if receiver in done:
                    # The frame is already off the wire; emit it even when
                    # stop fired in the same iteration rather than drop it.
                    for tick in self._parse(receiver.result()):
                        await emit(tick)
                    if stopper in done:
                        return True
                    continue

                receiver.cancel()
                with contextlib.suppress(BaseException):
                    await receiver
                if stopper in done:
                    return True
                # TCP can stay open while the venue has stopped publishing.
                # An idle socket is a dead feed: reconnect rather than sit on it.
                logger.warning(
                    "feed idle beyond receive timeout; forcing reconnect",
                    extra={"recv_timeout_seconds": timeout},
                )
                return False
            return True
        finally:
            stopper.cancel()
            with contextlib.suppress(BaseException):
                await stopper

    def _parse(self, raw: str | bytes) -> list[Tick]:
        try:
            payload = json.loads(raw)
        except (TypeError, ValueError):
            self._metrics.malformed_messages += 1
            return []
        records = payload if isinstance(payload, list) else [payload]
        ingest_time = utc_now()
        ticks: list[Tick] = []
        for record in records:
            tick = self._to_tick(record, ingest_time)
            if tick is None:
                self._metrics.malformed_messages += 1
            else:
                ticks.append(tick)
        return ticks


    def _to_tick(self, record: Any, ingest_time: datetime) -> Tick | None:
        """Normalise one feed record, tolerating common broker field aliases."""
        if not isinstance(record, dict):
            return None
        bid = first_number(record, ("bid", "bidPrice", "b"))
        ask = first_number(record, ("ask", "askPrice", "a"))
        if bid is None or ask is None or bid <= 0.0 or ask <= 0.0:
            return None
        if ask < bid:
            # A crossed quote is unusable for spread and volatility maths, and
            # this record would otherwise reach a signed risk policy. Drop it.
            return None
        volume = first_number(record, ("volume", "tickVolume", "size", "v")) or 0.0
        symbol = str(
            record.get("symbol") or record.get("s") or self._feed.symbol
        ).strip()
        event_time = parse_event_time(
            record.get("timestamp")
            or record.get("time")
            or record.get("ts")
            or record.get("t")
        )
        return Tick(
            symbol=symbol or self._feed.symbol,
            bid=bid,
            ask=ask,
            volume=int(volume),
            event_time=event_time or ingest_time,
            ingest_time=ingest_time,
            sequence=self._next_sequence(),
            source=self.name,
        )


def build_source(feed: FeedSettings, metrics: Metrics) -> TickSource:
    """Select the source implementation named by ``FEED_MODE``."""
    if feed.mode == "live":
        return WebSocketTickSource(feed, metrics)
    return SimulatedTickSource(feed, metrics)


# --------------------------------------------------------------------------- #
# Sinks
# --------------------------------------------------------------------------- #

#: Logical stream name -> settings attribute holding the physical container.
CONTAINER_KEYS: Final[tuple[str, str, str]] = ("ticks", "context", "bars")


class DocumentSink:
    """Persists batches of documents to one of the logical streams."""

    name: str = "base"

    async def start(self) -> None:
        return None

    async def write(self, stream: str, documents: Sequence[dict[str, Any]]) -> int:
        raise NotImplementedError

    async def aclose(self) -> None:
        return None


class StdoutDocumentSink(DocumentSink):
    """Newline-delimited JSON to stdout - for local runs, CI, and smoke tests."""

    name = "stdout"

    def __init__(self, metrics: Metrics) -> None:
        self._metrics = metrics

    async def write(self, stream: str, documents: Sequence[dict[str, Any]]) -> int:
        for document in documents:
            sys.stdout.write(
                json.dumps(
                    {"stream": stream, "document": document},
                    default=str,
                    separators=(",", ":"),
                )
                + "\n"
            )
        sys.stdout.flush()
        return len(documents)


class CosmosDocumentSink(DocumentSink):
    """Azure Cosmos DB sink using the async SDK and bounded write concurrency.

    Writes are issued as concurrent point upserts rather than a transactional
    batch: ticks do not need all-or-nothing semantics, a partial batch is
    recoverable through the deterministic ``id``, and point upserts are not
    constrained to a single partition key - which matters because a batch can
    straddle the hour boundary that defines the partition.
    """

    name = "cosmos"

    def __init__(self, cosmos: CosmosSettings, metrics: Metrics) -> None:
        self._cosmos = cosmos
        self._metrics = metrics
        self._semaphore = asyncio.Semaphore(cosmos.max_concurrent_writes)
        self._client: Any = None
        self._credential: Any = None
        self._containers: dict[str, Any] = {}
        self._http_error: type[Exception] = Exception

    def _container_name(self, stream: str) -> str:
        return {
            "ticks": self._cosmos.ticks_container,
            "context": self._cosmos.context_container,
            "bars": self._cosmos.bars_container,
        }[stream]

    def _ttl_for(self, stream: str) -> int:
        return {
            "ticks": self._cosmos.tick_ttl_seconds,
            "context": self._cosmos.context_ttl_seconds,
            "bars": self._cosmos.bar_ttl_seconds,
        }[stream]

    async def start(self) -> None:
        # Imported lazily so simulated/stdout runs need no Azure wheels.
        from azure.cosmos import PartitionKey, exceptions
        from azure.cosmos.aio import CosmosClient

        self._http_error = exceptions.CosmosHttpResponseError
        cosmos = self._cosmos

        credential: Any
        if cosmos.uses_managed_identity:
            from azure.identity.aio import DefaultAzureCredential

            self._credential = DefaultAzureCredential()
            credential = self._credential
        else:
            credential = cosmos.key


        client_kwargs: dict[str, Any] = {
            "retry_total": cosmos.retry_total,
            "connection_timeout": cosmos.connection_timeout_seconds,
            "user_agent": "xau-dynamics-tickforge",
        }
        if cosmos.preferred_locations:
            client_kwargs["preferred_locations"] = list(cosmos.preferred_locations)

        self._client = CosmosClient(cosmos.endpoint, credential=credential, **client_kwargs)

        if cosmos.provision_on_start:
            # Container creation is a *control-plane* operation. The built-in
            # Cosmos DB Data Contributor role cannot perform it, so this path
            # only works with a key or an ARM-scoped principal. In production
            # leave it false and provision through Bicep.
            database = await self._client.create_database_if_not_exists(
                id=cosmos.database
            )
            for stream in CONTAINER_KEYS:
                await database.create_container_if_not_exists(
                    id=self._container_name(stream),
                    partition_key=PartitionKey(path=cosmos.partition_key_path),
                    default_ttl=self._ttl_for(stream) or None,
                )
            logger.info("cosmos containers provisioned", extra={"database": cosmos.database})
        else:
            database = self._client.get_database_client(cosmos.database)

        for stream in CONTAINER_KEYS:
            self._containers[stream] = database.get_container_client(
                self._container_name(stream)
            )

        logger.info(
            "cosmos sink ready",
            extra={
                "endpoint": cosmos.endpoint,
                "database": cosmos.database,
                "auth_mode": cosmos.auth_mode,
                "max_concurrent_writes": cosmos.max_concurrent_writes,
            },
        )


    async def write(self, stream: str, documents: Sequence[dict[str, Any]]) -> int:
        if not documents:
            return 0
        container = self._containers[stream]
        results = await asyncio.gather(
            *(self._upsert(container, document) for document in documents)
        )
        return sum(results)

    async def _upsert(self, container: Any, document: dict[str, Any]) -> int:
        """Upsert one item. Never raises: a failed record is counted, not fatal."""
        async with self._semaphore:
            try:
                await container.upsert_item(body=document)
                return 1
            except asyncio.CancelledError:
                raise
            except self._http_error as exc:  # type: ignore[misc]
                self._metrics.write_failures += 1
                status = getattr(exc, "status_code", None)
                logger.error(
                    "cosmos write rejected",
                    extra={
                        "document_id": document.get("id"),
                        "status_code": status,
                        "sub_status": getattr(exc, "sub_status", None),
                        "throttled": status == 429,
                    },
                )
                return 0
            except Exception as exc:  # noqa: BLE001 - transport faults are counted
                self._metrics.write_failures += 1
                logger.error(
                    "cosmos write failed",
                    extra={
                        "document_id": document.get("id"),
                        "error": f"{type(exc).__name__}: {exc}",
                    },
                )
                return 0

    async def aclose(self) -> None:
        for closeable in (self._client, self._credential):
            if closeable is None:
                continue
            with contextlib.suppress(Exception):
                await closeable.close()
        self._client = None
        self._credential = None


def build_sink(settings: Settings, metrics: Metrics) -> DocumentSink:
    """Select the sink implementation named by ``PIPELINE_SINK_MODE``."""
    if settings.pipeline.sink_mode == "cosmos":
        return CosmosDocumentSink(settings.cosmos, metrics)
    return StdoutDocumentSink(metrics)


# --------------------------------------------------------------------------- #
# Market context (vectorised, numpy)
# --------------------------------------------------------------------------- #


def compute_market_context(
    ticks: Sequence[Tick], *, shard_id: str, ttl_seconds: int
) -> dict[str, Any] | None:
    """Per-batch market context: the summary RiskShield and NitroShield consume.

    Every field is deterministic arithmetic over the batch - no smoothing, no
    model, nothing that could not be recomputed from the stored ticks. That
    property is what lets a downstream signed policy cite this document as
    evidence. Returns ``None`` for a batch too small to yield a return series.
    """
    count = len(ticks)
    if count < 2:
        return None

    epoch = np.fromiter(
        (tick.event_time.timestamp() for tick in ticks), dtype=np.float64, count=count
    )
    bid = np.fromiter((tick.bid for tick in ticks), dtype=np.float64, count=count)
    ask = np.fromiter((tick.ask for tick in ticks), dtype=np.float64, count=count)
    volume = np.fromiter(
        (float(tick.volume) for tick in ticks), dtype=np.float64, count=count
    )
    latency_ms = np.fromiter(
        (
            (tick.ingest_time - tick.event_time).total_seconds() * 1_000.0
            for tick in ticks
        ),
        dtype=np.float64,
        count=count,
    )

    # Feeds can deliver slightly out of order; sort once so OHLC and the return
    # series are computed against real chronology.
    order = np.argsort(epoch, kind="stable")
    epoch, bid, ask = epoch[order], bid[order], ask[order]
    volume, latency_ms = volume[order], latency_ms[order]

    mid = (bid + ask) / 2.0
    spread = ask - bid
    log_returns = np.diff(np.log(mid))

    return_stdev = float(np.std(log_returns, ddof=1)) if log_returns.size > 1 else 0.0
    elapsed = float(epoch[-1] - epoch[0])
    mean_dt = elapsed / (count - 1) if elapsed > 0.0 else 0.0
    # Annualised by scaling the per-tick standard deviation by the number of
    # mean tick intervals in a calendar year. Valid only while tick arrival is
    # roughly regular - it is a regime indicator, not a pricing input.
    annualised_vol = (
        return_stdev * math.sqrt(SECONDS_PER_YEAR / mean_dt) if mean_dt > 0.0 else 0.0
    )


    window_start = datetime.fromtimestamp(float(epoch[0]), tz=timezone.utc)
    window_end = datetime.fromtimestamp(float(epoch[-1]), tz=timezone.utc)
    symbol = ticks[0].symbol
    partition = safe_key(f"{symbol}-{window_start:%Y%m%d%H}")

    document: dict[str, Any] = {
        "id": safe_key(f"{symbol}-ctx-{int(float(epoch[0]) * 1_000_000)}"),
        "pk": partition,
        "symbol": symbol,
        "shard": shard_id,
        "window_start": iso_z(window_start),
        "window_end": iso_z(window_end),
        "window_seconds": round(elapsed, 6),
        "tick_count": count,
        "ticks_per_second": round(count / elapsed, 3) if elapsed > 0.0 else None,
        "mean_tick_interval_seconds": round(mean_dt, 6) if mean_dt > 0.0 else None,
        "mid_open": round(float(mid[0]), PRICE_DECIMALS),
        "mid_high": round(float(np.max(mid)), PRICE_DECIMALS),
        "mid_low": round(float(np.min(mid)), PRICE_DECIMALS),
        "mid_close": round(float(mid[-1]), PRICE_DECIMALS),
        "mid_mean": round(float(np.mean(mid)), PRICE_DECIMALS),
        "spread_mean": round(float(np.mean(spread)), PRICE_DECIMALS),
        "spread_median": round(float(np.median(spread)), PRICE_DECIMALS),
        "spread_max": round(float(np.max(spread)), PRICE_DECIMALS),
        "spread_p95": round(float(np.percentile(spread, 95.0)), PRICE_DECIMALS),
        "volume_sum": float(np.sum(volume)),
        "log_return_stdev": round(return_stdev, 12),
        "max_abs_log_return": (
            round(float(np.max(np.abs(log_returns))), 12) if log_returns.size else 0.0
        ),
        "realized_vol_annualized": round(annualised_vol, 8),
        "ingest_latency_ms_mean": round(float(np.mean(latency_ms)), 3),
        "ingest_latency_ms_p95": round(float(np.percentile(latency_ms, 95.0)), 3),
        "generated_at": iso_z(utc_now()),
        "schema_version": 1,
    }
    if ttl_seconds > 0:
        document["ttl"] = ttl_seconds
    return document


# --------------------------------------------------------------------------- #
# OHLCV bar aggregation (pandas)
# --------------------------------------------------------------------------- #


class BarAggregator:
    """Resamples buffered ticks into fixed-interval OHLCV bars.

    Runs on its own slower cadence (``PIPELINE_BAR_FLUSH_SECONDS``) rather than
    per write batch, so pandas' per-call overhead is amortised over hundreds of
    ticks instead of being paid on the hot path. Set
    ``PIPELINE_ENABLE_BAR_AGGREGATION=false`` to disable it and drop pandas from
    the image entirely.
    """

    def __init__(
        self, *, interval: str, shard_id: str, ttl_seconds: int
    ) -> None:
        import pandas as pd  # lazy: only needed when aggregation is enabled

        self._pd = pd
        # Fail now, with a clear message, rather than on the first flush.
        pd.tseries.frequencies.to_offset(interval)
        self._interval = interval
        self._shard_id = shard_id
        self._ttl_seconds = ttl_seconds
        self._rows: list[tuple[float, float, float, int, str]] = []

    @property
    def pending(self) -> int:
        return len(self._rows)

    def add(self, ticks: Sequence[Tick]) -> None:
        self._rows.extend(
            (
                tick.event_time.timestamp(),
                tick.mid,
                tick.spread,
                tick.volume,
                tick.symbol,
            )
            for tick in ticks
        )


    def build(self, *, final: bool) -> list[dict[str, Any]]:
        """Emit completed bars. Unless ``final``, the open bucket is held back."""
        if not self._rows:
            return []
        pd = self._pd
        symbol = self._rows[-1][4]

        frame = pd.DataFrame(
            self._rows, columns=["epoch", "mid", "spread", "volume", "symbol"]
        )
        frame.index = pd.to_datetime(frame["epoch"], unit="s", utc=True)
        frame = frame.sort_index()

        grouped = frame.resample(self._interval)
        bars = pd.concat(
            [
                grouped["mid"].ohlc(),
                grouped["volume"].sum().rename("volume"),
                grouped["spread"].mean().rename("spread_mean"),
                grouped["spread"].max().rename("spread_max"),
                grouped["mid"].count().rename("tick_count"),
            ],
            axis=1,
        )
        bars = bars[bars["tick_count"] > 0]
        if bars.empty:
            return []

        open_bucket = bars.index[-1]
        if final:
            self._rows = []
        else:
            # The newest bucket may still be receiving ticks. Publish everything
            # before it and carry its ticks forward so the bar is only written
            # once, complete.
            bars = bars.iloc[:-1]
            cutoff = open_bucket.timestamp()
            self._rows = [row for row in self._rows if row[0] >= cutoff]
            if bars.empty:
                return []

        return [
            self._document(symbol, bucket, row) for bucket, row in bars.iterrows()
        ]


    def _document(self, symbol: str, bucket: Any, row: Any) -> dict[str, Any]:
        bar_start = bucket.to_pydatetime()
        document: dict[str, Any] = {
            "id": safe_key(
                f"{symbol}-{self._interval}-{int(bucket.timestamp())}"
            ),
            "pk": safe_key(f"{symbol}-{bar_start:%Y%m%d%H}"),
            "symbol": symbol,
            "shard": self._shard_id,
            "interval": self._interval,
            "bar_start": iso_z(bar_start),
            "open": round(float(row["open"]), PRICE_DECIMALS),
            "high": round(float(row["high"]), PRICE_DECIMALS),
            "low": round(float(row["low"]), PRICE_DECIMALS),
            "close": round(float(row["close"]), PRICE_DECIMALS),
            "volume": float(row["volume"]),
            "spread_mean": round(float(row["spread_mean"]), PRICE_DECIMALS),
            "spread_max": round(float(row["spread_max"]), PRICE_DECIMALS),
            "tick_count": int(row["tick_count"]),
            "generated_at": iso_z(utc_now()),
            "schema_version": 1,
        }
        if self._ttl_seconds > 0:
            document["ttl"] = self._ttl_seconds
        return document


# --------------------------------------------------------------------------- #
# Health endpoint
# --------------------------------------------------------------------------- #


class HealthServer:
    """Minimal stdlib HTTP server for Container Apps liveness/readiness probes.

    Deliberately dependency-free and read-only: it exposes counters and never
    accepts input. Bind it to the container's private port and keep ingress
    internal - it is unauthenticated by design, like any probe endpoint.
    """

    def __init__(
        self,
        host: str,
        port: int,
        state_provider: Callable[[], tuple[bool, dict[str, Any]]],
    ) -> None:
        self._host = host
        self._port = port
        self._state_provider = state_provider
        self._server: asyncio.AbstractServer | None = None


    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._handle, host=self._host, port=self._port
        )
        logger.info(
            "health endpoint listening",
            extra={"host": self._host, "port": self._port},
        )

    async def aclose(self) -> None:
        if self._server is None:
            return
        self._server.close()
        with contextlib.suppress(Exception):
            await self._server.wait_closed()
        self._server = None

    async def _handle(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            request_line = await asyncio.wait_for(reader.readline(), timeout=5.0)
            if not request_line:
                return
            # Drain the headers, bounded, then ignore them.
            for _ in range(64):
                line = await asyncio.wait_for(reader.readline(), timeout=5.0)
                if line in (b"\r\n", b"\n", b""):
                    break

            parts = request_line.decode("latin-1", "replace").split()
            path = parts[1].split("?", 1)[0] if len(parts) >= 2 else "/"
            ready, snapshot = self._state_provider()

            if path in ("/health/live", "/healthz", "/"):
                status, body = 200, {"status": "alive", "metrics": snapshot}
            elif path in ("/health/ready", "/readyz"):
                status = 200 if ready else 503
                body = {
                    "status": "ready" if ready else "not-ready",
                    "metrics": snapshot,
                }
            else:
                status, body = 404, {"status": "not-found", "path": path}

            await self._respond(writer, status, body)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - a probe must never crash the engine
            logger.debug("health request failed", exc_info=True)
        finally:
            with contextlib.suppress(Exception):
                writer.close()
                await writer.wait_closed()


    @staticmethod
    async def _respond(
        writer: asyncio.StreamWriter, status: int, body: dict[str, Any]
    ) -> None:
        reason = {200: "OK", 404: "Not Found", 503: "Service Unavailable"}.get(
            status, "OK"
        )
        payload = json.dumps(body, default=str, separators=(",", ":")).encode("utf-8")
        writer.write(
            (
                f"HTTP/1.1 {status} {reason}\r\n"
                "Content-Type: application/json\r\n"
                f"Content-Length: {len(payload)}\r\n"
                "Cache-Control: no-store\r\n"
                "Connection: close\r\n\r\n"
            ).encode("latin-1")
            + payload
        )
        await writer.drain()


# --------------------------------------------------------------------------- #
# Engine
# --------------------------------------------------------------------------- #


class XAUDataPipeline:
    """Orchestrates source, queue, batching, sinks, health, and shutdown."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._metrics = Metrics()
        self._queue: asyncio.Queue[Tick] = asyncio.Queue(
            maxsize=settings.pipeline.queue_maxsize
        )
        self._stop = asyncio.Event()
        # Set when the producer has left the feed loop for good. The consumer
        # keys its exit off *this*, not off the stop event: a producer with
        # frames still in flight can enqueue ticks after stop is requested,
        # and a consumer that has already exited would orphan them.
        self._producer_done = asyncio.Event()
        self._source: TickSource = build_source(settings.feed, self._metrics)
        self._sink: DocumentSink = build_sink(settings, self._metrics)
        self._aggregator: BarAggregator | None = None
        self._health: HealthServer | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._last_queue_warning = 0.0
        self._stop_reason = "not-stopped"
        self._started_monotonic = time.monotonic()
        self._last_tick_monotonic = 0.0


    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #

    def _request_stop(self, reason: str) -> None:
        if self._stop.is_set():
            return
        self._stop_reason = reason
        self._stop.set()
        logger.info("shutdown requested", extra={"reason": reason})

    def _install_signal_handlers(self) -> None:
        """Register SIGTERM/SIGINT handlers, falling back for Windows.

        Container Apps sends SIGTERM before it terminates a revision; catching
        it is what turns a kill into a clean drain and final flush.
        ``loop.add_signal_handler`` is POSIX-only, so the ``signal.signal``
        fallback keeps local Windows development identical.
        """
        loop = asyncio.get_running_loop()
        for name in ("SIGTERM", "SIGINT", "SIGBREAK"):
            signum = getattr(signal, name, None)
            if signum is None:
                continue
            try:
                loop.add_signal_handler(signum, self._request_stop, f"signal:{name}")
                continue
            except (NotImplementedError, RuntimeError, ValueError):
                pass

            def handler(_signum: int, _frame: Any, label: str = name) -> None:
                # Runs outside the event loop; hop back on before touching it.
                if self._loop is not None:
                    self._loop.call_soon_threadsafe(
                        self._request_stop, f"signal:{label}"
                    )

            with contextlib.suppress(ValueError, OSError, RuntimeError):
                signal.signal(signum, handler)

    def _health_state(self) -> tuple[bool, dict[str, Any]]:
        """Readiness fails on a stale feed, so probes catch a silent venue."""
        reference = self._last_tick_monotonic or self._started_monotonic
        age = time.monotonic() - reference
        ready = (
            not self._stop.is_set()
            and age <= self._settings.observability.health_max_tick_age_seconds
        )
        snapshot = self._metrics.snapshot()
        snapshot["queue_depth"] = self._queue.qsize()
        snapshot["seconds_since_last_tick"] = round(age, 3)
        snapshot["shard"] = self._settings.pipeline.shard_id
        snapshot["feed_mode"] = self._settings.feed.mode
        snapshot["sink_mode"] = self._settings.pipeline.sink_mode
        return ready, snapshot

    # ------------------------------------------------------------------ #
    # Producer
    # ------------------------------------------------------------------ #

    async def _enqueue(self, tick: Tick) -> None:
        """Apply the configured backpressure policy for one inbound tick.

        See :class:`Metrics` for why ``ticks_enqueued`` and ``ticks_dropped``
        overlap under ``drop_oldest``: the evicted tick was counted as admitted
        when it entered, and is counted again as dropped when it leaves unwritten.
        """
        self._metrics.ticks_received += 1
        self._metrics.last_tick_at = iso_z(tick.ingest_time)
        self._last_tick_monotonic = time.monotonic()
        try:
            self._queue.put_nowait(tick)
        except asyncio.QueueFull:
            self._metrics.queue_full_events += 1
            self._warn_queue_full()
            if self._settings.pipeline.queue_full_policy == "drop_oldest":
                # Evict the stalest tick: under this policy latency matters more
                # than completeness, and the freshest quote is the useful one.
                with contextlib.suppress(asyncio.QueueEmpty):
                    self._queue.get_nowait()
                    self._queue.task_done()
                    self._metrics.ticks_dropped += 1
                try:
                    self._queue.put_nowait(tick)
                except asyncio.QueueFull:
                    # A consumer that raced us back to full: drop the new tick
                    # rather than spin, and account for it here.
                    self._metrics.ticks_dropped += 1
                    return
            else:
                # Block: let the queue apply real backpressure to the feed
                # rather than silently deciding which ticks matter. The wait is
                # bounded so shutdown can never deadlock behind a consumer that
                # has already drained and exited.
                while True:
                    try:
                        await asyncio.wait_for(
                            self._queue.put(tick), timeout=IDLE_POLL_SECONDS
                        )
                        break
                    except asyncio.TimeoutError:
                        if self._stop.is_set():
                            self._metrics.ticks_dropped += 1
                            logger.warning(
                                "dropped a tick: queue still full after shutdown",
                                extra={"tick_id": tick.document_id},
                            )
                            return
        self._metrics.ticks_enqueued += 1

    def _warn_queue_full(self) -> None:
        """Log a queue-full event at most once every five seconds."""
        now = time.monotonic()
        if now - self._last_queue_warning < 5.0:
            return
        self._last_queue_warning = now
        logger.warning(
            "ingest queue saturated - the sink is slower than the feed",
            extra={
                "queue_maxsize": self._settings.pipeline.queue_maxsize,
                "policy": self._settings.pipeline.queue_full_policy,
                "queue_full_events": self._metrics.queue_full_events,
                "ticks_dropped": self._metrics.ticks_dropped,
            },
        )

    async def _produce(self) -> None:
        try:
            await self._source.run(self._stop, self._enqueue)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("producer terminated abnormally")
            self._request_stop("producer-failure")
            raise
        finally:
            # Reached on success, failure, and cancellation alike: the consumer
            # must never be left waiting on a producer that is gone.
            self._producer_done.set()
        if not self._stop.is_set():
            self._request_stop("feed-exhausted")


    # ------------------------------------------------------------------ #
    # Consumer
    # ------------------------------------------------------------------ #

    async def _consume(self) -> None:
        """Batch on size **or** age, then flush whatever remains on shutdown.

        The age trigger is what stops a quiet market from parking records in
        memory indefinitely, and the post-loop flush is the guarantee that a
        partial batch is never discarded.
        """
        max_records = self._settings.pipeline.batch_max_records
        max_seconds = self._settings.pipeline.batch_max_seconds
        batch: list[Tick] = []
        deadline = time.monotonic() + max_seconds

        while True:
            now = time.monotonic()
            if batch and (len(batch) >= max_records or now >= deadline):
                await self._flush(batch)
                batch = []
                deadline = time.monotonic() + max_seconds
                continue
            if self._producer_done.is_set() and self._queue.empty():
                break

            wait = min(deadline - now, IDLE_POLL_SECONDS) if batch else IDLE_POLL_SECONDS
            try:
                tick = await asyncio.wait_for(self._queue.get(), timeout=max(wait, 0.005))
            except asyncio.TimeoutError:
                continue
            self._queue.task_done()
            if not batch:
                # Age is measured from the oldest tick actually held.
                deadline = time.monotonic() + max_seconds
            batch.append(tick)

        if batch:
            logger.info(
                "flushing partial batch on shutdown", extra={"records": len(batch)}
            )
            await self._flush(batch)

    async def _flush(self, batch: Sequence[Tick]) -> None:
        if not batch:
            return
        pipeline = self._settings.pipeline
        documents = [
            tick.to_document(
                shard_id=pipeline.shard_id,
                ttl_seconds=self._settings.cosmos.tick_ttl_seconds,
            )
            for tick in batch
        ]
        written = await self._sink.write("ticks", documents)
        self._metrics.batches_flushed += 1
        self._metrics.ticks_written += written
        # Stream-scoped loss: `write_failures` also counts context and bar
        # documents, so it cannot close the tick conservation law on its own.
        self._metrics.ticks_failed += len(documents) - written
        self._metrics.last_flush_at = iso_z(utc_now())

        if pipeline.enable_market_context:
            context = compute_market_context(
                batch,
                shard_id=pipeline.shard_id,
                ttl_seconds=self._settings.cosmos.context_ttl_seconds,
            )
            if context is not None:
                self._metrics.context_documents_written += await self._sink.write(
                    "context", [context]
                )

        if self._aggregator is not None:
            self._aggregator.add(batch)

        logger.debug(
            "batch flushed",
            extra={
                "records": len(documents),
                "written": written,
                "queue_depth": self._queue.qsize(),
            },
        )

    # ------------------------------------------------------------------ #
    # Background tasks
    # ------------------------------------------------------------------ #

    async def _aggregate(self) -> None:
        """Publish completed OHLCV bars on the slower aggregation cadence."""
        interval = self._settings.pipeline.bar_flush_seconds
        while not self._stop.is_set():
            if await sleep_unless_stopped(self._stop, interval):
                break
            await self._flush_bars(final=False)

    async def _flush_bars(self, *, final: bool) -> None:
        if self._aggregator is None or self._aggregator.pending == 0:
            return
        try:
            bars = self._aggregator.build(final=final)
        except Exception:
            logger.exception("bar aggregation failed")
            return
        if not bars:
            return
        self._metrics.bars_written += await self._sink.write("bars", bars)
        logger.debug("bars published", extra={"bars": len(bars), "final": final})

    async def _report(self) -> None:
        """Emit one structured metrics line per interval for Log Analytics."""
        interval = self._settings.observability.metrics_interval_seconds
        while not self._stop.is_set():
            if await sleep_unless_stopped(self._stop, interval):
                break
            _, snapshot = self._health_state()
            logger.info("metrics", extra=snapshot)

    async def _deadline(self) -> None:
        """Stop after ``PIPELINE_RUN_DURATION_SECONDS`` - used by smoke tests."""
        duration = self._settings.pipeline.run_duration_seconds
        if await sleep_unless_stopped(self._stop, duration):
            return
        self._request_stop("run-duration-elapsed")


    # ------------------------------------------------------------------ #
    # Entry point
    # ------------------------------------------------------------------ #

    async def run(self) -> int:
        settings = self._settings
        self._loop = asyncio.get_running_loop()
        self._started_monotonic = time.monotonic()
        self._install_signal_handlers()

        logger.info("tickforge starting", extra={"configuration": settings.describe()})
        if settings.feed.mode == "simulated":
            logger.warning(
                "FEED_MODE=simulated: prices are synthetic and must not be "
                "treated as market data",
                extra={"environment": settings.environment},
            )

        if settings.pipeline.enable_bar_aggregation:
            self._aggregator = BarAggregator(
                interval=settings.pipeline.bar_interval,
                shard_id=settings.pipeline.shard_id,
                ttl_seconds=settings.cosmos.bar_ttl_seconds,
            )

        await self._sink.start()

        if settings.observability.health_enabled:
            self._health = HealthServer(
                settings.observability.health_host,
                settings.observability.health_port,
                self._health_state,
            )
            await self._health.start()

        producer = asyncio.create_task(self._produce(), name="producer")
        consumer = asyncio.create_task(self._consume(), name="consumer")
        helpers: list[asyncio.Task[None]] = [
            asyncio.create_task(self._report(), name="metrics")
        ]
        if self._aggregator is not None:
            helpers.append(asyncio.create_task(self._aggregate(), name="aggregator"))
        if settings.pipeline.run_duration_seconds > 0:
            helpers.append(asyncio.create_task(self._deadline(), name="deadline"))

        exit_code = EXIT_OK
        try:
            await asyncio.wait(
                {producer, consumer}, return_when=asyncio.FIRST_COMPLETED
            )
        except asyncio.CancelledError:
            self._request_stop("cancelled")
            raise
        finally:
            exit_code = await self._shutdown(producer, consumer, helpers)

        return exit_code


    async def _shutdown(
        self,
        producer: asyncio.Task[None],
        consumer: asyncio.Task[None],
        helpers: Sequence[asyncio.Task[None]],
    ) -> int:
        """Drain in strict order: producer, queue, consumer, helpers, resources."""
        grace = self._settings.pipeline.shutdown_grace_seconds
        if not self._stop.is_set():
            self._request_stop("task-completed")

        failures = 0

        # 1. Stop the producer first: no new tick may enter a draining queue.
        if await self._settle(producer, timeout=grace, label="producer"):
            failures += 1

        # 2. Every queued tick must be dequeued before the consumer exits.
        #    This is the invariant that pairs with task_done() on each get.
        if not consumer.done():
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._queue.join(), timeout=grace)

        # 3. The consumer flushes its partial batch as it leaves the loop.
        if await self._settle(consumer, timeout=grace, label="consumer"):
            failures += 1

        # 4. Helpers hold no unflushed state and can be cancelled outright.
        for task in helpers:
            task.cancel()
        await asyncio.gather(*helpers, return_exceptions=True)

        # 5. Emit the trailing partial bar while the sink is still open.
        with contextlib.suppress(Exception):
            await self._flush_bars(final=True)

        # 6. Release resources in reverse order of acquisition.
        for close, label in (
            (self._source.aclose, "source"),
            (self._health.aclose if self._health else None, "health"),
            (self._sink.aclose, "sink"),
        ):
            if close is None:
                continue
            try:
                await close()
            except Exception:  # noqa: BLE001 - shutdown must not raise
                logger.exception("failed to close %s", label)

        _, snapshot = self._health_state()
        logger.info(
            "tickforge stopped",
            extra={"reason": self._stop_reason, "failures": failures, **snapshot},
        )

        # Final integrity check. Downstream, realised volatility computed from
        # these ticks is signed into a risk policy, so a silently missing tick
        # is worse than a crash: it produces a plausible number that is wrong.
        # Prove conservation instead of assuming it, and fail the exit code if
        # the books do not balance.
        depth = self._queue.qsize()
        if depth:
            logger.error(
                "ticks remained in the queue at exit",
                extra={"queue_depth": depth},
            )
            failures += 1
        missing = self._metrics.unaccounted(in_flight=depth)
        if missing:
            logger.error(
                "tick accounting does not balance; ticks were lost in flight",
                extra={
                    "unaccounted": missing,
                    "ticks_received": self._metrics.ticks_received,
                    "ticks_written": self._metrics.ticks_written,
                    "ticks_failed": self._metrics.ticks_failed,
                    "ticks_dropped": self._metrics.ticks_dropped,
                    "queue_depth": depth,
                },
            )
            failures += 1
        return EXIT_OK if failures == 0 else EXIT_FAILURE

    async def _settle(
        self,
        task: asyncio.Task[None],
        *,
        timeout: float,
        label: str,
    ) -> bool:
        """Await one task within ``timeout``. Returns ``True`` if it failed."""
        _, pending = await asyncio.wait({task}, timeout=timeout)
        if pending:
            # Name the await that overran. Without this an operator sees only
            # "did not finish" and has to guess which stage hung.
            frames = task.get_stack(limit=8)
            where = (
                " <- ".join(f"{f.f_code.co_name}:{f.f_lineno}" for f in reversed(frames))
                or "<no frames>"
            )
            logger.error(
                "%s did not finish inside the shutdown grace window; cancelling",
                label,
                extra={"grace_seconds": timeout, "blocked_at": where},
            )
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            return True
        try:
            task.result()
        except asyncio.CancelledError:
            return False
        except Exception as exc:  # noqa: BLE001 - reported, never propagated
            logger.error(
                "%s failed",
                label,
                extra={"error": f"{type(exc).__name__}: {exc}"},
                exc_info=True,
            )
            return True
        return False


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #


def main() -> int:
    """Resolve configuration, run the engine, and map the outcome to an exit code."""
    try:
        settings = get_settings()
    except ConfigError as exc:
        # Logging is not configured yet: a misconfigured service must still be
        # able to explain itself in the container logs.
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
        logging.getLogger("tickforge").critical("configuration error: %s", exc)
        return EXIT_CONFIG

    configure_logging(settings)
    try:
        return asyncio.run(XAUDataPipeline(settings).run())
    except KeyboardInterrupt:  # pragma: no cover - interactive use only
        logger.warning("interrupted")
        return EXIT_INTERRUPT


if __name__ == "__main__":
    sys.exit(main())
