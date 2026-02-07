FROM python:3.12-slim-bookworm

WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends git curl && \
    rm -rf /var/lib/apt/lists/*

# Create a non-root user
RUN groupadd -r databot && useradd -r -g databot -m -d /home/databot -s /bin/bash databot

# Install Python dependencies first (cached layer)
COPY pyproject.toml README.md LICENSE ./
RUN pip install --no-cache-dir .

# Copy source
COPY databot/ databot/

# Reinstall with source
RUN pip install --no-cache-dir ".[all]"

# Create data directory and set permissions
RUN mkdir -p /home/databot/.databot && \
    chown -R databot:databot /app /home/databot/.databot

# Gateway port
EXPOSE 18790

# Switch to non-root user
USER databot

ENTRYPOINT ["databot"]
CMD ["status"]
