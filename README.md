# zapchasti-parser

Unified parsing engine for [куплюзапчасти.рф](https://куплюзапчасти.рф) — 134+ data sources.

## Setup

```bash
uv sync
cp .env.example .env
# Edit .env with your DATABASE_URL
```

## Usage

```bash
# Run a specific collector in bulk mode
python scripts/run_bulk.py dgis_suppliers

# Run continuous monitoring (scheduler)
python scripts/run_continuous.py
```

## Architecture

```
src/
├── collectors/     # Data source implementations (11 categories)
├── pipeline/       # Normalizer → Deduplicator → Classifier → Writer
├── models/         # SQLAlchemy models (shared with platform)
├── scheduler/      # APScheduler for continuous mode
├── config/         # Settings, database connection
└── utils/          # HTTP client, logging, metrics
```

## Data Sources

| Category | Sources | Status |
|----------|---------|--------|
| Suppliers | 28 | 3 active |
| Parts Catalogs | 11 | 1 active |
| Equipment Owners | 23 | planned |
| Service Centers | 8 | planned |
| Tenders | 5 | planned |
| Used Equipment | 18 | planned |
| Prices & Stock | 6 | planned |
| Reviews | 7 | planned |
| Forums & Content | 12 | planned |
| Jobs | 10 | planned |
| Search Queries | 6 | planned |
