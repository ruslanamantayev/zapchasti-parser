FROM python:3.11-slim

WORKDIR /app

# Install system deps for psycopg2 and lxml
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev libxml2-dev libxslt1-dev \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install dependencies (cached layer)
COPY pyproject.toml uv.lock ./
RUN uv sync --no-dev --no-install-project

# Copy source
COPY . .

# Create data directories
RUN mkdir -p data/raw data/logs

CMD ["uv", "run", "python", "scripts/run_continuous.py"]
