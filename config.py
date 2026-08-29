"""XAU Dynamics · TickForge - configuration layer.

Every runtime knob is resolved from the process environment exactly once,
validated eagerly, and then frozen. Two rules are non-negotiable in this module:

1. **No secret carries a default.** A missing credential raises
   :class:`ConfigError` at start-up rather than degrading into a placeholder
   that "works" in development and fails silently in production.
2. **No secret reaches a log record.** Credential fields are excluded from
   ``repr()``, and :meth:`Settings.describe` returns a redacted view that is
   safe to emit to Log Analytics.

Run ``python config.py`` to print the resolved, redacted configuration. This is
the fastest way to verify environment wiring inside a container before starting
the engine.

Target runtime: Python 3.10+.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import sys
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Final, Literal

from dotenv import load_dotenv

SERVICE_NAME: Final[str] = "tickforge"
SERVICE_COMPONENT: Final[str] = "data-pipeline"

#: Environment values accepted as boolean true / false.
_TRUTHY: Final[frozenset[str]] = frozenset({"1", "true", "t", "yes", "y", "on"})
_FALSY: Final[frozenset[str]] = frozenset({"0", "false", "f", "no", "n", "off"})

#: Substrings that mark a credential as a non-functional placeholder. Shipping
#: one of these is a configuration error, not a warning.
_PLACEHOLDER_MARKERS: Final[tuple[str, ...]] = (
    "mock",
    "placeholder",
    "changeme",
    "change-me",
    "replace-me",
    "your-key",
    "todo",
    "dummy",
)

_LOG_LEVELS: Final[frozenset[str]] = frozenset(
    {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
)


class ConfigError(RuntimeError):
    """Raised when required configuration is absent, malformed, or unsafe."""


# --------------------------------------------------------------------------- #
# Typed primitive readers
# --------------------------------------------------------------------------- #


def _raw(name: str) -> str | None:
    """Return a stripped environment value, treating blank strings as unset."""
    value = os.getenv(name)
    if value is None:
        return None
    value = value.strip()
    return value or None


def _str(
    name: str,
    default: str | None = None,
    *,
    required: bool = False,
    choices: Sequence[str] | None = None,
    lower: bool = False,
) -> Any:
    value = _raw(name)
    if value is None:
        if required:
            raise ConfigError(f"{name} is required but is not set")
        value = default
    if value is not None and lower:
        value = value.lower()
    if value is not None and choices and value not in choices:
        raise ConfigError(
            f"{name} must be one of {sorted(choices)} - got {value!r}"
        )
    return value


def _int(
    name: str,
    default: int,
    *,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    value = _raw(name)
    if value is None:
        result = default
    else:
        try:
            result = int(value, 10)
        except ValueError as exc:
            raise ConfigError(f"{name} must be an integer - got {value!r}") from exc
    if minimum is not None and result < minimum:
        raise ConfigError(f"{name} must be >= {minimum} - got {result}")
    if maximum is not None and result > maximum:
        raise ConfigError(f"{name} must be <= {maximum} - got {result}")
    return result


def _float(
    name: str,
    default: float,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
) -> float:
    value = _raw(name)
    if value is None:
        result = default
    else:
        try:
            result = float(value)
        except ValueError as exc:
            raise ConfigError(f"{name} must be a number - got {value!r}") from exc
    if minimum is not None and result < minimum:
        raise ConfigError(f"{name} must be >= {minimum} - got {result}")
    if maximum is not None and result > maximum:
        raise ConfigError(f"{name} must be <= {maximum} - got {result}")
    return result


def _bool(name: str, default: bool) -> bool:
    value = _raw(name)
    if value is None:
        return default
    lowered = value.lower()
    if lowered in _TRUTHY:
        return True
    if lowered in _FALSY:
        return False
    raise ConfigError(f"{name} must be a boolean - got {value!r}")


def _csv(name: str, default: Sequence[str] = ()) -> tuple[str, ...]:
    value = _raw(name)
    if value is None:
        return tuple(default)
    return tuple(part.strip() for part in value.split(",") if part.strip())


def _secret(name: str, *, required: bool) -> str | None:
    """Read a credential. Never falls back to a literal, never logs the value."""
    value = _raw(name)
    if value is None:
        if required:
            raise ConfigError(
                f"{name} is required. Supply it from a secret store - never commit "
                "it to source. On Azure Container Apps, prefer a managed identity "
                "(AZURE_COSMOS_AUTH_MODE=aad) so no key exists to leak."
            )
        return None
    lowered = value.lower()
    marker = next((m for m in _PLACEHOLDER_MARKERS if m in lowered), None)
    if marker is not None:
        raise ConfigError(
            f"{name} looks like a placeholder (contains {marker!r}). Refusing to "
            "start with a non-functional credential."
        )
    return value


# --------------------------------------------------------------------------- #
# Settings groups
# --------------------------------------------------------------------------- #


@dataclass(frozen=True, slots=True)
class CosmosSettings:
    """Azure Cosmos DB connection, container topology, and write behaviour."""

    endpoint: str
    auth_mode: Literal["aad", "key"]
    database: str
    ticks_container: str
    context_container: str
    bars_container: str
    partition_key_path: str
    tick_ttl_seconds: int
    context_ttl_seconds: int
    bar_ttl_seconds: int
    max_concurrent_writes: int
    connection_timeout_seconds: int
    retry_total: int
    provision_on_start: bool
    preferred_locations: tuple[str, ...]
    #: Only populated when ``auth_mode == "key"``. Excluded from repr by design.
    key: str | None = field(default=None, repr=False)

    @property
    def uses_managed_identity(self) -> bool:
        return self.auth_mode == "aad"


@dataclass(frozen=True, slots=True)
class FeedSettings:
    """Upstream market-data feed, plus the deterministic load-generator knobs."""

    mode: Literal["live", "simulated"]
    url: str
    symbol: str
    ping_interval_seconds: float
    ping_timeout_seconds: float
    open_timeout_seconds: float
    recv_timeout_seconds: float
    close_timeout_seconds: float
    max_message_bytes: int
    reconnect_initial_seconds: float
    reconnect_max_seconds: float
    reconnect_jitter: float
    sim_base_price: float
    sim_tick_interval_seconds: float
    sim_price_step: float
    sim_half_spread: float
    sim_min_volume: int
    sim_max_volume: int
    sim_seed: int | None
    api_key: str | None = field(default=None, repr=False)



@dataclass(frozen=True, slots=True)
class PipelineSettings:
    """Queue, batching, sharding, and shutdown behaviour of the engine."""

    shard_id: str
    queue_maxsize: int
    queue_full_policy: Literal["block", "drop_oldest"]
    batch_max_records: int
    batch_max_seconds: float
    shutdown_grace_seconds: float
    run_duration_seconds: float
    sink_mode: Literal["cosmos", "stdout"]
    enable_market_context: bool
    enable_bar_aggregation: bool
    bar_interval: str
    bar_flush_seconds: float


@dataclass(frozen=True, slots=True)
class ObservabilitySettings:
    """Logging, health probes, and metrics emission."""

    log_level: str
    log_format: Literal["json", "text"]
    health_enabled: bool
    health_host: str
    health_port: int
    health_max_tick_age_seconds: float
    metrics_interval_seconds: float


@dataclass(frozen=True, slots=True)
class Settings:
    """The complete, validated runtime configuration for one pipeline replica."""

    environment: str
    allow_unsafe_production: bool
    cosmos: CosmosSettings
    feed: FeedSettings
    pipeline: PipelineSettings
    observability: ObservabilitySettings

    @property
    def is_production(self) -> bool:
        return self.environment.lower() in {"production", "prod"}

    # ----------------------------------------------------------------- #
    # Construction
    # ----------------------------------------------------------------- #

    @classmethod
    def from_env(cls) -> Settings:
        """Resolve, coerce, and validate the full configuration from the env.

        A local ``.env`` file is loaded first but never overrides a real
        environment variable, so Container Apps secrets always win.
        """
        load_dotenv(os.getenv("TICKFORGE_ENV_FILE", ".env"), override=False)

        environment = _str("TICKFORGE_ENV", "development", lower=True)
        sink_mode = _str(
            "PIPELINE_SINK_MODE", "cosmos", choices=("cosmos", "stdout"), lower=True
        )
        auth_mode = _str(
            "AZURE_COSMOS_AUTH_MODE", "aad", choices=("aad", "key"), lower=True
        )

        cosmos = CosmosSettings(
            endpoint=_str(
                "AZURE_COSMOS_URI",
                "https://xau-dynamics-db.documents.azure.com:443/",
            ),
            auth_mode=auth_mode,
            key=_secret(
                "AZURE_COSMOS_KEY",
                required=(auth_mode == "key" and sink_mode == "cosmos"),
            ),
            database=_str("AZURE_COSMOS_DATABASE", "XAUDynamicsDB"),
            ticks_container=_str("AZURE_COSMOS_TICKS_CONTAINER", "MarketTicks"),
            context_container=_str("AZURE_COSMOS_CONTEXT_CONTAINER", "MarketContext"),
            bars_container=_str("AZURE_COSMOS_BARS_CONTAINER", "MarketBars"),
            partition_key_path=_str("AZURE_COSMOS_PARTITION_KEY_PATH", "/pk"),
            tick_ttl_seconds=_int(
                "AZURE_COSMOS_TICK_TTL_SECONDS", 604_800, minimum=-1
            ),
            context_ttl_seconds=_int(
                "AZURE_COSMOS_CONTEXT_TTL_SECONDS", 2_592_000, minimum=-1
            ),
            bar_ttl_seconds=_int(
                "AZURE_COSMOS_BAR_TTL_SECONDS", 7_776_000, minimum=-1
            ),
            max_concurrent_writes=_int(
                "AZURE_COSMOS_MAX_CONCURRENT_WRITES", 32, minimum=1, maximum=512
            ),
            connection_timeout_seconds=_int(
                "AZURE_COSMOS_CONNECTION_TIMEOUT_SECONDS", 10, minimum=1, maximum=300
            ),
            retry_total=_int("AZURE_COSMOS_RETRY_TOTAL", 9, minimum=0, maximum=100),
            provision_on_start=_bool("AZURE_COSMOS_PROVISION_ON_START", False),
            preferred_locations=_csv("AZURE_COSMOS_PREFERRED_LOCATIONS"),
        )


        # ``RETRY_DELAY_SECONDS`` is the legacy name from the first revision of
        # this service. It is honoured as the default for the new, explicit
        # reconnect knob so existing deployments keep working unchanged.
        legacy_retry_delay = _float(
            "RETRY_DELAY_SECONDS", 1.0, minimum=0.05, maximum=300.0
        )

        feed = FeedSettings(
            mode=_str(
                "FEED_MODE", "simulated", choices=("live", "simulated"), lower=True
            ),
            url=_str("GOLD_FEED_URL", "wss://stream-api.xau-dynamics.io/v3/gold"),
            symbol=_str("FEED_SYMBOL", "XAUUSD"),
            api_key=_secret("FEED_API_KEY", required=False),
            ping_interval_seconds=_float(
                "FEED_PING_INTERVAL_SECONDS", 20.0, minimum=1.0, maximum=600.0
            ),
            ping_timeout_seconds=_float(
                "FEED_PING_TIMEOUT_SECONDS", 20.0, minimum=1.0, maximum=600.0
            ),
            open_timeout_seconds=_float(
                "FEED_OPEN_TIMEOUT_SECONDS", 10.0, minimum=1.0, maximum=300.0
            ),
            recv_timeout_seconds=_float(
                "FEED_RECV_TIMEOUT_SECONDS", 30.0, minimum=1.0, maximum=900.0
            ),
            close_timeout_seconds=_float(
                "FEED_CLOSE_TIMEOUT_SECONDS", 5.0, minimum=0.1, maximum=120.0
            ),
            max_message_bytes=_int(
                "FEED_MAX_MESSAGE_BYTES", 1_048_576, minimum=1_024
            ),
            reconnect_initial_seconds=_float(
                "FEED_RECONNECT_INITIAL_SECONDS",
                legacy_retry_delay,
                minimum=0.05,
                maximum=300.0,
            ),
            reconnect_max_seconds=_float(
                "FEED_RECONNECT_MAX_SECONDS", 60.0, minimum=0.1, maximum=3_600.0
            ),
            reconnect_jitter=_float(
                "FEED_RECONNECT_JITTER", 0.25, minimum=0.0, maximum=1.0
            ),
            sim_base_price=_float("SIM_BASE_PRICE", 2_350.00, minimum=0.01),
            sim_tick_interval_seconds=_float(
                "SIM_TICK_INTERVAL_SECONDS", 0.1, minimum=0.0001, maximum=60.0
            ),
            sim_price_step=_float("SIM_PRICE_STEP", 0.75, minimum=0.0),
            sim_half_spread=_float("SIM_HALF_SPREAD", 0.15, minimum=0.0),
            sim_min_volume=_int("SIM_MIN_VOLUME", 10, minimum=0),
            sim_max_volume=_int("SIM_MAX_VOLUME", 150, minimum=0),
            sim_seed=_int("SIM_SEED", 0) if _raw("SIM_SEED") is not None else None,
        )


        pipeline = PipelineSettings(
            shard_id=_str("PIPELINE_SHARD_ID", "shard-0"),
            queue_maxsize=_int(
                "PIPELINE_QUEUE_MAXSIZE", 1_000, minimum=1, maximum=1_000_000
            ),
            queue_full_policy=_str(
                "PIPELINE_QUEUE_FULL_POLICY",
                "block",
                choices=("block", "drop_oldest"),
                lower=True,
            ),
            batch_max_records=_int(
                "PIPELINE_BATCH_MAX_RECORDS", 50, minimum=1, maximum=1_000
            ),
            batch_max_seconds=_float(
                "PIPELINE_BATCH_MAX_SECONDS", 2.0, minimum=0.05, maximum=300.0
            ),
            shutdown_grace_seconds=_float(
                "PIPELINE_SHUTDOWN_GRACE_SECONDS", 20.0, minimum=0.5, maximum=600.0
            ),
            run_duration_seconds=_float(
                "PIPELINE_RUN_DURATION_SECONDS", 0.0, minimum=0.0
            ),
            sink_mode=sink_mode,
            enable_market_context=_bool("PIPELINE_ENABLE_MARKET_CONTEXT", True),
            enable_bar_aggregation=_bool("PIPELINE_ENABLE_BAR_AGGREGATION", True),
            bar_interval=_str("PIPELINE_BAR_INTERVAL", "1s"),
            bar_flush_seconds=_float(
                "PIPELINE_BAR_FLUSH_SECONDS", 5.0, minimum=0.5, maximum=3_600.0
            ),
        )

        observability = ObservabilitySettings(
            log_level=_str("LOG_LEVEL", "INFO").upper(),
            log_format=_str(
                "LOG_FORMAT", "json", choices=("json", "text"), lower=True
            ),
            health_enabled=_bool("HEALTH_ENABLED", True),
            health_host=_str("HEALTH_HOST", "0.0.0.0"),
            health_port=_int("HEALTH_PORT", 8_080, minimum=1, maximum=65_535),
            health_max_tick_age_seconds=_float(
                "HEALTH_MAX_TICK_AGE_SECONDS", 60.0, minimum=1.0
            ),
            metrics_interval_seconds=_float(
                "METRICS_INTERVAL_SECONDS", 30.0, minimum=1.0, maximum=3_600.0
            ),
        )


        settings = cls(
            environment=environment,
            allow_unsafe_production=_bool("ALLOW_UNSAFE_PRODUCTION", False),
            cosmos=cosmos,
            feed=feed,
            pipeline=pipeline,
            observability=observability,
        )
        settings.validate()
        return settings

    # ----------------------------------------------------------------- #
    # Validation
    # ----------------------------------------------------------------- #

    def validate(self) -> None:
        """Fail fast on any combination that cannot work or is unsafe to run."""
        if self.observability.log_level not in _LOG_LEVELS:
            raise ConfigError(
                f"LOG_LEVEL must be one of {sorted(_LOG_LEVELS)} - "
                f"got {self.observability.log_level!r}"
            )

        if self.pipeline.sink_mode == "cosmos":
            if not self.cosmos.endpoint.startswith("https://"):
                raise ConfigError(
                    "AZURE_COSMOS_URI must be an https:// endpoint - got "
                    f"{self.cosmos.endpoint!r}"
                )
            if not self.cosmos.partition_key_path.startswith("/"):
                raise ConfigError(
                    "AZURE_COSMOS_PARTITION_KEY_PATH must start with '/' - got "
                    f"{self.cosmos.partition_key_path!r}"
                )

        if self.feed.mode == "live" and not self.feed.url.startswith(("ws://", "wss://")):
            raise ConfigError(
                f"GOLD_FEED_URL must be a ws:// or wss:// URL - got {self.feed.url!r}"
            )

        if self.feed.sim_min_volume > self.feed.sim_max_volume:
            raise ConfigError(
                f"SIM_MIN_VOLUME ({self.feed.sim_min_volume}) must be <= "
                f"SIM_MAX_VOLUME ({self.feed.sim_max_volume})"
            )

        if self.pipeline.queue_maxsize < self.pipeline.batch_max_records:
            raise ConfigError(
                f"PIPELINE_QUEUE_MAXSIZE ({self.pipeline.queue_maxsize}) must be >= "
                f"PIPELINE_BATCH_MAX_RECORDS ({self.pipeline.batch_max_records}); "
                "a queue smaller than one batch cannot amortise writes."
            )


        # Production guard rails. The simulator and the stdout sink are
        # legitimate development tools, but silently shipping either one to a
        # production tenant would publish fabricated prices into the audit
        # trail that RiskShield and NitroShield read. Both are refused unless
        # explicitly and deliberately overridden.
        if self.is_production and not self.allow_unsafe_production:
            if self.feed.mode == "simulated":
                raise ConfigError(
                    "FEED_MODE=simulated is refused when TICKFORGE_ENV=production: "
                    "the simulator emits synthetic prices. Set FEED_MODE=live, or "
                    "set ALLOW_UNSAFE_PRODUCTION=true if this really is a "
                    "production-tenant load test."
                )
            if self.pipeline.sink_mode == "stdout":
                raise ConfigError(
                    "PIPELINE_SINK_MODE=stdout is refused when "
                    "TICKFORGE_ENV=production: ticks would never be persisted. "
                    "Set PIPELINE_SINK_MODE=cosmos, or set "
                    "ALLOW_UNSAFE_PRODUCTION=true to override."
                )

    # ----------------------------------------------------------------- #
    # Safe rendering
    # ----------------------------------------------------------------- #

    def describe(self) -> dict[str, Any]:
        """Return a redacted, JSON-serialisable view that is safe to log."""

        def redact(value: str | None) -> str | None:
            return "***redacted***" if value else None

        return {
            "service": SERVICE_NAME,
            "component": SERVICE_COMPONENT,
            "environment": self.environment,
            "cosmos": {
                "endpoint": self.cosmos.endpoint,
                "auth_mode": self.cosmos.auth_mode,
                "key": redact(self.cosmos.key),
                "database": self.cosmos.database,
                "containers": {
                    "ticks": self.cosmos.ticks_container,
                    "context": self.cosmos.context_container,
                    "bars": self.cosmos.bars_container,
                },
                "partition_key_path": self.cosmos.partition_key_path,
                "ttl_seconds": {
                    "ticks": self.cosmos.tick_ttl_seconds,
                    "context": self.cosmos.context_ttl_seconds,
                    "bars": self.cosmos.bar_ttl_seconds,
                },
                "max_concurrent_writes": self.cosmos.max_concurrent_writes,
                "retry_total": self.cosmos.retry_total,
                "provision_on_start": self.cosmos.provision_on_start,
                "preferred_locations": list(self.cosmos.preferred_locations),
            },

            "feed": {
                "mode": self.feed.mode,
                "url": self.feed.url,
                "symbol": self.feed.symbol,
                "api_key": redact(self.feed.api_key),
                "reconnect_initial_seconds": self.feed.reconnect_initial_seconds,
                "reconnect_max_seconds": self.feed.reconnect_max_seconds,
                "simulator": {
                    "base_price": self.feed.sim_base_price,
                    "tick_interval_seconds": self.feed.sim_tick_interval_seconds,
                    "price_step": self.feed.sim_price_step,
                    "half_spread": self.feed.sim_half_spread,
                    "volume_range": [
                        self.feed.sim_min_volume,
                        self.feed.sim_max_volume,
                    ],
                    "seed": self.feed.sim_seed,
                },
            },
            "pipeline": {
                "shard_id": self.pipeline.shard_id,
                "sink_mode": self.pipeline.sink_mode,
                "queue_maxsize": self.pipeline.queue_maxsize,
                "queue_full_policy": self.pipeline.queue_full_policy,
                "batch_max_records": self.pipeline.batch_max_records,
                "batch_max_seconds": self.pipeline.batch_max_seconds,
                "shutdown_grace_seconds": self.pipeline.shutdown_grace_seconds,
                "run_duration_seconds": self.pipeline.run_duration_seconds,
                "enable_market_context": self.pipeline.enable_market_context,
                "enable_bar_aggregation": self.pipeline.enable_bar_aggregation,
                "bar_interval": self.pipeline.bar_interval,
                "bar_flush_seconds": self.pipeline.bar_flush_seconds,
            },
            "observability": {
                "log_level": self.observability.log_level,
                "log_format": self.observability.log_format,
                "health_enabled": self.observability.health_enabled,
                "health_endpoint": (
                    f"http://{self.observability.health_host}:"
                    f"{self.observability.health_port}/health/live"
                ),
                "health_max_tick_age_seconds": (
                    self.observability.health_max_tick_age_seconds
                ),
                "metrics_interval_seconds": (
                    self.observability.metrics_interval_seconds
                ),
            },
        }


# --------------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------------- #

#: Attributes present on every LogRecord; anything else was passed via ``extra``
#: and is promoted to a top-level field in the JSON payload.
_RESERVED_LOG_ATTRS: Final[frozenset[str]] = frozenset(
    vars(logging.makeLogRecord({})).keys()
) | {"message", "asctime", "taskName"}


class JsonLogFormatter(logging.Formatter):
    """Render one JSON object per line - parsed natively by Log Analytics."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "service": SERVICE_NAME,
            "component": SERVICE_COMPONENT,
        }
        for key, value in record.__dict__.items():
            if key not in _RESERVED_LOG_ATTRS and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        if record.stack_info:
            payload["stack"] = self.formatStack(record.stack_info)
        return json.dumps(payload, default=str, separators=(",", ":"))


def configure_logging(settings: Settings) -> None:
    """Install a single stdout handler and quiet down third-party loggers."""
    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)

    # Force UTF-8 on the log stream. Log Analytics ingests UTF-8, and a venue
    # error string or instrument name outside the host's locale codec would
    # otherwise raise UnicodeEncodeError *inside* the handler and lose the
    # record. Windows redirects stdout as cp1252 by default, so this is not
    # hypothetical. errors="replace" guarantees the record survives, degraded.
    stream = sys.stdout
    reconfigure = getattr(stream, "reconfigure", None)
    if reconfigure is not None:  # text streams only; absent if stdout is bytes
        with contextlib.suppress(Exception):
            reconfigure(encoding="utf-8", errors="replace")

    handler = logging.StreamHandler(stream=stream)
    if settings.observability.log_format == "json":
        handler.setFormatter(JsonLogFormatter())
    else:
        handler.setFormatter(
            logging.Formatter(
                fmt=(
                    "%(asctime)s | %(levelname)-8s | "
                    f"{SERVICE_NAME}" + " | %(name)s | %(message)s"
                ),
                datefmt="%Y-%m-%dT%H:%M:%S%z",
            )
        )
    root.addHandler(handler)
    root.setLevel(settings.observability.log_level)

    # The Azure and websockets SDKs are extremely verbose at DEBUG and will
    # drown out pipeline events; hold them at WARNING or above.
    for noisy in ("azure", "websockets", "urllib3", "asyncio"):
        logging.getLogger(noisy).setLevel(max(logging.WARNING, root.level))
    logging.captureWarnings(True)


# --------------------------------------------------------------------------- #
# Accessor
# --------------------------------------------------------------------------- #


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide settings singleton, building it on first call."""
    return Settings.from_env()


def main() -> int:
    """Print the resolved, redacted configuration. Exit 78 on ConfigError."""
    try:
        settings = get_settings()
    except ConfigError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 78  # EX_CONFIG
    print(json.dumps(settings.describe(), indent=2, sort_keys=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
