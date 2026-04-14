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
uv run python scripts/run_bulk.py hespareparts --brand komatsu --pages 5
uv run python scripts/run_bulk.py exkavator --max-categories 5 --max-ads 10
uv run python scripts/run_bulk.py --list
uv run python scripts/run_bulk.py --check-db

# Continuous mode
uv run python scripts/run_continuous.py

# Deploy
ssh zapchasti-prod "cd /srv/projects/zapchasti-parser && ./deploy.sh --build"
```

## Database
- Same DB as zapchasti-platform (DATABASE_URL in .env)
- Tables: `parsed_suppliers` (спарсенные поставщики), `catalog_parts` (артикулы)
- `companies` — НЕ ТРОГАТЬ, это зарегистрированные пользователи платформы
- Models in src/models/ mirror platform models with `extend_existing=True`
- Парсер НЕ создаёт миграции — таблицы создаются в zapchasti-platform

## Правила работы с данными

### ОБЯЗАТЕЛЬНО: валидация перед записью в БД

При добавлении нового коллектора или изменении существующего:

1. **Сначала парсить в JSONL** — сырые данные сохранять в `data/raw/<source>/`
2. **Проверить структуру** — просмотреть первые 10-20 записей, убедиться что поля заполнены корректно
3. **Сверить со схемой БД** — каждое поле из JSONL должно маппиться на колонку в таблице:
   - Типы данных совпадают (строка → VARCHAR, массив → ARRAY, число → INTEGER)
   - Обязательные поля (NOT NULL) всегда заполнены
   - Уникальные ключи не дублируются
4. **Нормализовать** — перед записью прогнать через pipeline:
   - Телефоны: `normalize_phone()` → формат +7XXXXXXXXXX
   - Email: lowercase, trim
   - Артикулы: `normalize_article()` → убрать дефисы/пробелы, UPPER
   - Названия: убрать лишние пробелы, trim
5. **Тестовый прогон** — сначала запустить с ограничением (--pages 5, --max-categories 5)
6. **Проверить в БД** — SELECT COUNT, выборочный просмотр записей
7. **Только потом** — полный прогон

### Маппинг данных

**Поставщики** (2GIS, Exkavator → `parsed_suppliers`):
- Пишем через pipeline (normalizer → deduplicator → writer)
- Дедупликация по 6 уровням: source_id, INN, phone, email, website, name

**Артикулы** (hespareparts → `catalog_parts`):
- Сохраняем JSONL в `data/raw/hespareparts/`
- Импорт отдельным скриптом с `ON CONFLICT (article_clean, brand) DO NOTHING`
- article_clean = `re.sub(r'[\s\-\./]', '', article).upper()`
- slug = `{brand}/{article.lower()}`

### Что НЕ делать
- Не писать напрямую в БД без нормализации
- Не пропускать тестовый прогон
- Не игнорировать дубликаты — всегда ON CONFLICT или deduplicator
- Не трогать таблицу `companies` — это зарегистрированные пользователи

## Adding a New Collector
1. Create `src/collectors/<category>/<source>.py`
2. Inherit from `BaseCollector`, implement `collect_bulk()` and `collect_new()`
3. Register in `scripts/run_bulk.py` COLLECTORS dict
4. Add to `src/config/sources.yaml` for scheduled runs
5. **Обязательно**: тестовый прогон → проверка данных → полный прогон (см. "Правила работы с данными")
