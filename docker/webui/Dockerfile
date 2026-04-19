FROM python:3.11-slim

LABEL org.opencontainers.image.source=https://github.com/maschkef/PBS_monitor
LABEL org.opencontainers.image.description="PBS Monitor Web UI"
LABEL org.opencontainers.image.licenses=MIT

# Install gosu for privilege dropping in the entrypoint
RUN apt-get update && apt-get install -y --no-install-recommends gosu \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user (used after privilege drop in entrypoint)
RUN groupadd -r pbs && useradd -r -g pbs pbs
WORKDIR /app

# Install dependencies
COPY webui/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY webui/ ./webui/
# alerting/ is required: app.py imports from alerting.monitor
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
ENV WEBUI_HOST=0.0.0.0
ENV WEBUI_PORT=5111
# ALERTING_DATA_DIR points app.py to the mounted volume for config/state
ENV ALERTING_DATA_DIR=/app/data

WORKDIR /app/webui

EXPOSE 5111

ENTRYPOINT ["/entrypoint.sh"]
CMD ["python", "app.py"]