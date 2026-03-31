FROM python:3.11-slim

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install dependencies
COPY pyproject.toml .
RUN uv sync --no-dev --no-install-project

# Copy source
COPY . .

CMD ["uv", "run", "python", "scripts/run_continuous.py"]
