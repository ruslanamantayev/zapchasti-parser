# zapchasti-parser — AI Context

## Overview
Unified parsing engine for куплюзапчасти.рф. Collects data from 134+ sources across 11 categories.
Shares PostgreSQL database with zapchasti-platform.

## Tech Stack
- Python 3.11+, uv package manager
- httpx (async HTTP), requests (sync HTTP), BeautifulSoup4
- SQLAlchemy 2.0, psycopg2-binary
- APScheduler (continuous mode)
- Pydantic 2.x, pydantic-settings

## Architecture
```
src/
├── collectors/     # Data source implementations (BaseCollector)
│   ├── suppliers/  # 2GIS, exkavator.ru
│   └── parts/      # hespareparts.com
├── pipeline/       # normalizer → deduplicator → classifier → enricher → writer
├── models/         # SQLAlchemy models (shared with platform)
├── scheduler/      # APScheduler for continuous mode
├── config/         # settings.py, database.py, sources.yaml
└── utils/          # http.py (HttpClient), logging.py
```

## Key Commands
```bash
# Bulk parsing
uv run python scripts/run_bulk.py dgis_suppliers --city "Москва"
uv run python scripts/run_bulk.py hespareparts --brand caterpillar
uv run python scripts/run_bulk.py exkavator
uv run python scripts/run_bulk.py --list

# Continuous mode
uv run python scripts/run_continuous.py

# Deploy
ssh zapchasti-prod "cd /srv/projects/zapchasti-parser && ./deploy.sh --build"
```

## Database
- Same DB as zapchasti-platform (DATABASE_URL in .env)
- Main table: `suppliers` (shared, do NOT create migrations here)
- Models in src/models/ mirror platform models with `extend_existing=True`

## Adding a New Collector
1. Create `src/collectors/<category>/<source>.py`
2. Inherit from `BaseCollector`, implement `collect_bulk()` and `collect_new()`
3. Register in `scripts/run_bulk.py` COLLECTORS dict
4. Add to `src/config/sources.yaml` for scheduled runs
