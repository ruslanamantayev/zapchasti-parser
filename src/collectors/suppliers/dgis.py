"""
2GIS supplier collector — parses companies from 2gis.ru internal API.
Migrated from zapchasti-platform/backend/app/parsers/sources/dgis.py

Usage:
    python scripts/run_bulk.py dgis_suppliers
    python scripts/run_bulk.py dgis_suppliers --city "Москва"
    python scripts/run_bulk.py dgis_suppliers --all
"""
import asyncio
import logging
import re
from datetime import datetime
from typing import Optional

from src.collectors.base import BaseCollector, CollectorStats
from src.config.database import SessionLocal
from src.pipeline.writer import SupplierWriter
from src.utils.http import HttpClient

logger = logging.getLogger(__name__)

DGIS_SEARCH_URL = "https://catalog.api.2gis.ru/3.0/items"

# Base search queries
BASE_QUERIES = [
    "запчасти для спецтехники",
    "запчасти для тракторов",
    "запчасти для экскаваторов",
    "запчасти для погрузчиков",
    "ходовая часть спецтехника",
    "гидравлика запчасти",
    "двигатели для спецтехники",
    "фильтры спецтехника",
    "ковши экскаватор",
    "гусеницы запчасти",
    "турбины спецтехника",
    "разборка спецтехники",
    "склад запчастей спецтехника",
]

# All cities with 2GIS region_id (92 regions)
CITIES = {
    "Москва": 32, "Санкт-Петербург": 38, "Новосибирск": 22, "Екатеринбург": 7,
    "Казань": 10, "Нижний Новгород": 20, "Красноярск": 15, "Челябинск": 48,
    "Самара": 34, "Уфа": 43, "Ростов-на-Дону": 33, "Краснодар": 14,
    "Омск": 24, "Воронеж": 4, "Пермь": 27, "Волгоград": 3,
    "Тюмень": 42, "Саратов": 35, "Тольятти": 53, "Ижевск": 57,
    "Барнаул": 1, "Иркутск": 8, "Ульяновск": 44, "Хабаровск": 46,
    "Владивосток": 75, "Ярославль": 55, "Махачкала": 56, "Томск": 41,
    "Оренбург": 25, "Кемерово": 11, "Новокузнецк": 67, "Рязань": 61,
    "Набережные Челны": 50, "Астрахань": 2, "Пенза": 26, "Киров": 12,
    "Липецк": 18, "Чебоксары": 47, "Тула": 54, "Калининград": 9,
    "Курск": 17, "Ставрополь": 40, "Сочи": 39, "Улан-Удэ": 58,
    "Тверь": 52, "Брянск": 63, "Белгород": 62, "Сургут": 68,
    "Иваново": 60, "Владимир": 49, "Чита": 59, "Архангельск": 29,
    "Смоленск": 37, "Калуга": 64, "Волжский": 65, "Курган": 16,
    "Орёл": 36, "Череповец": 51, "Вологда": 30, "Мурманск": 19,
    "Саранск": 69, "Тамбов": 31, "Грозный": 73, "Якутск": 45,
    "Кострома": 13, "Комсомольск-на-Амуре": 78, "Стерлитамак": 70,
    "Таганрог": 71, "Нижневартовск": 23, "Петрозаводск": 28,
    "Йошкар-Ола": 72, "Новороссийск": 74, "Нижнекамск": 76,
    "Сыктывкар": 6, "Нальчик": 77, "Братск": 21, "Абакан": 5,
    # CIS
    "Алматы": 81, "Астана": 82, "Караганда": 83, "Минск": 84,
    "Шымкент": 85, "Ташкент": 86, "Актобе": 87, "Бишкек": 88,
    "Тбилиси": 89, "Павлодар": 90, "Усть-Каменогорск": 91,
    "Актау": 94, "Атырау": 95, "Семей": 96, "Кокшетау": 97,
    "Нижний Тагил": 80, "Прокопьевск": 99,
}


class DgisSupplierCollector(BaseCollector):
    """Collects suppliers from 2GIS directory"""
    source_name = "2gis"

    def __init__(self):
        self.api_key: Optional[str] = None
        self.http = HttpClient(
            delay_min=3.0,
            delay_max=6.0,
            headers={
                "Referer": "https://2gis.ru/",
                "Origin": "https://2gis.ru",
            },
        )

    async def _fetch_api_key(self) -> Optional[str]:
        """Get current API key from 2gis.ru page"""
        if self.api_key:
            return self.api_key

        try:
            text = await self.http.get_text("https://2gis.ru/moscow")
            # Outsource key works for API (main webApiKey gets blocked)
            match = re.search(r'"webApiOutsourceKey"\s*:\s*"([a-f0-9-]{36})"', text)
            if match:
                self.api_key = match.group(1)
                logger.info(f"Got 2GIS outsource key: {self.api_key}")
                return self.api_key

            match = re.search(r'"webApiKey"\s*:\s*"([a-f0-9-]{36})"', text)
            if match:
                self.api_key = match.group(1)
                logger.info(f"Got 2GIS key: {self.api_key}")
                return self.api_key
        except Exception as e:
            logger.warning(f"Failed to get API key: {e}")

        return None

    async def _search(self, query: str, region_id: int, max_pages: int = 5) -> list[dict]:
        """Search companies in 2GIS for a given query and region"""
        api_key = await self._fetch_api_key()
        if not api_key:
            logger.error("No 2GIS API key available")
            return []

        all_items = []

        for page in range(1, max_pages + 1):
            params = {
                "key": api_key,
                "q": query,
                "region_id": region_id,
                "type": "branch",
                "page": page,
                "page_size": 50,
                "fields": "items.contact_groups,items.reviews,items.schedule",
                "locale": "ru_RU",
            }

            try:
                response = await self.http.get(DGIS_SEARCH_URL, params=params)

                if response.status_code == 429:
                    logger.warning(f"Rate limited on page {page}, stopping")
                    break

                if response.status_code != 200:
                    logger.warning(f"2GIS API: {response.status_code}")
                    break

                data = response.json()
                meta = data.get("meta", {})

                if meta.get("code") == 403:
                    logger.error(f"2GIS API key blocked: {meta.get('error', {}).get('message')}")
                    self.api_key = None
                    break

                if meta.get("code") and meta["code"] != 200:
                    logger.warning(f"2GIS API error: {meta}")
                    break

                result = data.get("result", {})
                items = result.get("items", [])

                if not items:
                    break

                all_items.extend(items)

                total = result.get("total", 0)
                if len(all_items) >= total:
                    break

            except Exception as e:
                logger.error(f"Error page {page}: {e}")
                break

        return all_items

    def _parse_item(self, raw: dict, city: str = "") -> dict:
        """Parse a single 2GIS item into supplier dict"""
        phones = []
        emails = []
        website = None

        for group in raw.get("contact_groups", []):
            for contact in group.get("contacts", []):
                ctype = contact.get("type")
                value = contact.get("value")
                if not value:
                    continue
                if ctype == "phone":
                    phones.append(value)
                elif ctype == "email":
                    emails.append(value)
                elif ctype == "website" and not website:
                    website = value

        address_name = raw.get("address_name", "")
        full_address = raw.get("full_address_name", "")
        rubrics = [r.get("name", "") for r in raw.get("rubrics", [])]

        reviews_data = raw.get("reviews", {})
        rating = reviews_data.get("general_rating")
        reviews_count = reviews_data.get("general_review_count", 0)

        source_id = str(raw.get("id", ""))

        return {
            "name": raw.get("name", ""),
            "phone": phones[0] if phones else None,
            "email": emails[0] if emails else None,
            "website": website,
            "city": raw.get("city_name", "") or city,
            "address": full_address or address_name,
            "source": "2gis",
            "source_url": f"https://2gis.ru/firm/{source_id}" if source_id else None,
            "source_id": source_id,
            "rating_2gis": float(rating) if rating else None,
            "reviews_count": int(reviews_count) if reviews_count else None,
            "raw_data": {
                "rubrics": rubrics,
                "phones": phones,
                "emails": emails,
                "address_name": address_name,
                "schedule": raw.get("schedule", {}),
            },
        }

    async def collect_bulk(
        self,
        city: Optional[str] = None,
        cities: Optional[list[str]] = None,
        queries: Optional[list[str]] = None,
        **kwargs,
    ) -> CollectorStats:
        """
        Bulk collection from 2GIS.

        Args:
            city: Single city name
            cities: List of city names (overrides city)
            queries: Custom queries (default: BASE_QUERIES)
        """
        stats = CollectorStats(collector_name="dgis_suppliers")
        query_list = queries or BASE_QUERIES

        # Determine cities to parse
        if cities:
            city_list = [(c, CITIES[c]) for c in cities if c in CITIES]
        elif city:
            region_id = CITIES.get(city)
            if not region_id:
                logger.error(f"City '{city}' not found in CITIES dict")
                return stats
            city_list = [(city, region_id)]
        else:
            # All cities
            city_list = list(CITIES.items())

        db = SessionLocal()
        writer = SupplierWriter(db)

        try:
            for city_name, region_id in city_list:
                for query in query_list:
                    logger.info(f"Parsing: '{query}' in {city_name} (region={region_id})")

                    raw_items = await self._search(query, region_id)
                    stats.total_found += len(raw_items)

                    if not raw_items:
                        continue

                    parsed = []
                    for raw in raw_items:
                        try:
                            item = self._parse_item(raw, city=city_name)
                            if item.get("name"):
                                parsed.append(item)
                        except Exception as e:
                            logger.warning(f"Parse error: {e}")
                            stats.errors += 1

                    if parsed:
                        result = writer.batch_import(parsed)
                        stats.created += result.created
                        stats.updated += result.updated
                        stats.skipped += result.skipped
                        stats.errors += result.errors

                        logger.info(
                            f"  '{query}' in {city_name}: found {len(raw_items)}, "
                            f"created {result.created}, updated {result.updated}, "
                            f"skipped {result.skipped}"
                        )
        finally:
            db.close()
            await self.http.close()

        stats.finished_at = datetime.utcnow()
        return stats

    async def collect_new(self, since: datetime, **kwargs) -> CollectorStats:
        """2GIS doesn't support 'since' filtering — runs full bulk"""
        return await self.collect_bulk(**kwargs)

    def get_schedule(self) -> str:
        return "0 0 */3 * *"  # Every 3 days

    async def close(self):
        await self.http.close()
