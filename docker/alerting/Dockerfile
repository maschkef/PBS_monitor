FROM python:3.11-slim

LABEL org.opencontainers.image.source=https://github.com/maschkef/PBS_monitor
LABEL org.opencontainers.image.description="PBS Monitor Alerting Service"
LABEL org.opencontainers.image.licenses=MIT

# Install gosu and apply OS security patches (e.g. openssl CVE-2026-28390)
RUN apt-get update && apt-get upgrade -y && apt-get install -y --no-install-recommends gosu \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user (used after privilege drop in entrypoint)
RUN groupadd -r pbs && useradd -r -g pbs pbs
WORKDIR /app

# Upgrade pip-bundled tools to fix vendored CVEs (jaraco.context, wheel)
RUN pip install --no-cache-dir --upgrade setuptools wheel

# Install dependencies
COPY alerting/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY alerting/ ./alerting/
COPY .env.example .

# Create data directory; ownership is fixed at runtime by the entrypoint
# so that host-mounted volumes (initially root-owned) become writable
RUN mkdir -p /app/data

# Entrypoint: runs as root, chowns /app/data, then execs as pbs
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV DOCKER_ENV=true
# ALERTING_DATA_DIR points monitor.py to the mounted volume for config/state
ENV ALERTING_DATA_DIR=/app/data

WORKDIR /app/alerting

ENTRYPOINT ["/entrypoint.sh"]
CMD ["python", "monitor.py", "--daemon", "1800"]