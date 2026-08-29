# syntax=docker/dockerfile:1.7
# =========================================================================== #
#  XAU Dynamics · TickForge — hardened multi-stage image
#  Target: Azure Container Apps (Linux/amd64) · Python 3.12 · non-root
#
#  Build:  docker build -t tickforge:local .
#  Run:    docker run --rm -p 8080:8080 \
#            -e FEED_MODE=simulated -e PIPELINE_SINK_MODE=stdout \
#            tickforge:local
#
#  No stage ever receives a secret. Credentials arrive at *runtime* from
#  Container Apps secrets or, preferably, from a managed identity
#  (AZURE_COSMOS_AUTH_MODE=aad) so that no key exists to leak.
# =========================================================================== #

# --------------------------------------------------------------------------- #
# Stage 1 — builder: compile wheels into a self-contained virtualenv.
# --------------------------------------------------------------------------- #
FROM python:3.12-slim-bookworm AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_ROOT_USER_ACTION=ignore

WORKDIR /build

# Only the dependency manifest is copied here, so the expensive install layer
# is reused whenever application code changes but requirements do not.
COPY requirements.txt ./

RUN python -m venv /opt/venv \
 && /opt/venv/bin/pip install --upgrade pip setuptools wheel \
 && /opt/venv/bin/pip install -r requirements.txt \
 && find /opt/venv -name '__pycache__' -type d -prune -exec rm -rf {} + \
 && find /opt/venv -name '*.pyc' -delete

# --------------------------------------------------------------------------- #
# Stage 2 — runtime: no compilers, no pip cache, no build tooling.
# --------------------------------------------------------------------------- #
FROM python:3.12-slim-bookworm AS runtime

LABEL org.opencontainers.image.title="XAU Dynamics TickForge" \
      org.opencontainers.image.description="Asynchronous high-throughput XAUUSD tick ingestion engine feeding RiskShield and NitroShield." \
      org.opencontainers.image.vendor="XAU Dynamics" \
      org.opencontainers.image.licenses="Proprietary" \
      org.opencontainers.image.source="https://github.com/XAUDynamics-Labs/XAU-Dynamics-DataPipeline" \
      org.opencontainers.image.base.name="docker.io/library/python:3.12-slim-bookworm"

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONFAULTHANDLER=1 \
    PYTHONHASHSEED=random \
    PATH="/opt/venv/bin:$PATH" \
    HEALTH_PORT=8080 \
    LOG_FORMAT=json

# Apply outstanding security patches from the base image, then drop apt state.
# No build-essential, no curl, no shell utilities beyond the slim defaults:
# a smaller surface is a smaller CVE report.
RUN apt-get update \
 && apt-get upgrade -y --no-install-recommends \
 && rm -rf /var/lib/apt/lists/*

# Fixed high uid/gid. Container Apps and Kubernetes policies that assert
# runAsNonRoot compare the numeric id, not the name.
RUN groupadd --gid 10001 tickforge \
 && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin tickforge

COPY --from=builder --chown=root:root /opt/venv /opt/venv

WORKDIR /app

# Application code is owned by root and only readable by the runtime user, so
# the process cannot rewrite its own logic at runtime. chmod is a separate RUN
# rather than `COPY --chmod`, which is a BuildKit-only extension and fails on
# the legacy builder still shipped as a fallback.
COPY --chown=root:root config.py pipeline.py ./
RUN chmod 0444 /app/config.py /app/pipeline.py

USER 10001:10001

EXPOSE 8080

# Container Apps sends SIGTERM before terminating a revision; pipeline.py
# converts it into a stop event, drains the queue, and flushes the partial
# batch inside PIPELINE_SHUTDOWN_GRACE_SECONDS.
STOPSIGNAL SIGTERM

# Probes with the stdlib, so no extra package (curl/wget) enters the image.
# urlopen raises on a 503, which is exactly the readiness contract: a non-zero
# exit marks the container unhealthy.
#
# NOTE: Azure Container Apps IGNORES this directive and uses its own liveness /
# readiness probe configuration. Point those at /health/live and /health/ready
# on port 8080 in your Bicep template.
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD ["python", "-c", "import os,urllib.request;urllib.request.urlopen('http://127.0.0.1:'+os.environ.get('HEALTH_PORT','8080')+'/health/ready',timeout=4)"]

# -u is redundant with PYTHONUNBUFFERED but survives an env override, keeping
# the JSON log stream flushed line-by-line into Log Analytics.
ENTRYPOINT ["python", "-u", "pipeline.py"]
