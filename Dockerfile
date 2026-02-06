FROM python:3.12-slim-bookworm

WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends git curl && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies first (cached layer)
COPY pyproject.toml README.md LICENSE ./
RUN pip install --no-cache-dir .

# Copy source
COPY databot/ databot/

# Reinstall with source
RUN pip install --no-cache-dir ".[all]"

# Create data directory
RUN mkdir -p /root/.databot

# Gateway port
EXPOSE 18790

ENTRYPOINT ["databot"]
CMD ["status"]
