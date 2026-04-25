FROM python:3.14-slim

LABEL org.opencontainers.image.source=https://github.com/maschkef/PBS_monitor
LABEL org.opencontainers.image.description="PBS Monitor Web UI"
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
COPY webui/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY webui/ ./webui/
# alerting/ is required: app.py imports from alerting.monitor
COPY alerting/ ./alerting/
COPY .env.example .

# Application code is owned by root and not writable by the pbs runtime user.
# This is already true by default (COPY produces root:root 644/755 files), but
# made explicit here to document the intent and guard against accidental changes.
RUN chmod -R a-w /app/webui /app/alerting

# Create data directory with restricted permissions.
# The entrypoint re-chowns this to pbs:pbs at runtime so bind-mounted host
# directories (initially root-owned) become writable by the application.
RUN mkdir -p /app/data && chmod 750 /app/data

# Entrypoint: runs as root, chowns /app/data, then execs as pbs
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV DOCKER_ENV=true
ENV WEBUI_HOST=0.0.0.0
ENV WEBUI_PORT=5111
# ALERTING_DATA_DIR points app.py to the mounted volume for config/state
ENV ALERTING_DATA_DIR=/app/data

WORKDIR /app/webui

EXPOSE 5111

ENTRYPOINT ["/entrypoint.sh"]
CMD ["python", "app.py"]