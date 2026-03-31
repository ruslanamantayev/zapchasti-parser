"""
Exkavator.ru collector — ads + company contacts from exkavator.ru.
10,399 ads in 156 subcategories.
Migrated from zapchasti-platform/backend/app/parsers/sources/exkavator.py

Strategy:
1. Get subcategories from /trade/parts/search/
2. For each subcategory — get ad IDs
3. For each ad — extract JSON-LD (article, brand, price, description, images)
4. For each unique company — parse contacts from company page
5. Save companies → suppliers table, ads → data/raw/
"""
import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Optional

from src.collectors.base import BaseCollector, CollectorStats
from src.config.database import SessionLocal
from src.pipeline.writer import SupplierWriter
from src.utils.http import HttpClient

logger = logging.getLogger(__name__)

BASE_URL = "https://exkavator.ru"


class ExkavatorCollector(BaseCollector):
    """Collects suppliers and ads from exkavator.ru"""
    source_name = "exkavator"

    def __init__(self, output_dir: Optional[Path] = None):
        self.http = HttpClient(delay_min=2.0, delay_max=4.0, verify_ssl=False)
        self.output_dir = output_dir or Path("data/raw/exkavator")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._parsed_companies: set[str] = set()

    async def get_subcategories(self) -> list[dict]:
        """Get all parts subcategories with ad counts"""
        html = await self.http.get_text(f"{BASE_URL}/trade/parts/search/")

        cats = re.findall(r'href="(/trade/parts/[a-z\-]+/[a-z\-]+/)"', html)
        unique = list(dict.fromkeys(cats))

        result = []
        for href in unique:
            idx = html.find(f'href="{href}"')
            chunk = html[idx:idx + 300]
            qty_match = re.search(r'class="quantity">(\d+)', chunk)
            qty = int(qty_match.group(1)) if qty_match else 0
            if qty > 0:
                result.append({"url": href, "count": qty})

        logger.info(f"Found {len(result)} subcategories with {sum(c['count'] for c in result)} ads")
        return result

    async def get_ads_from_category(self, category_url: str) -> list[str]:
        """Get ad IDs from a subcategory page"""
        html = await self.http.get_text(f"{BASE_URL}{category_url}")
        ad_ids = re.findall(r'href="/trade/parts/message/(\d+)\.html"', html)
        return list(dict.fromkeys(ad_ids))

    async def get_ad_detail(self, ad_id: str) -> Optional[dict]:
        """Get ad details via JSON-LD"""
        url = f"/trade/parts/message/{ad_id}.html"
        html = await self.http.get_text(f"{BASE_URL}{url}")

        # JSON-LD
        jsonld_blocks = re.findall(
            r'<script[^>]*type="application/ld\+json"[^>]*>([\s\S]*?)</script>',
            html,
        )

        product = None
        for block in jsonld_blocks:
            try:
                data = json.loads(block)
                graph = data.get("@graph", [data])
                for item in graph:
                    if item.get("@type") == "Product":
                        product = item
                        break
            except (json.JSONDecodeError, TypeError):
                continue

        if not product:
            return None

        # Company
        company_match = re.findall(r'href="(/trade/company/[^"]+/)"[^>]*>([^<]+)', html)
        company_name = None
        company_url = None
        for href, name in company_match:
            name = name.strip()
            if name and name != "Продажа спецтехники":
                company_name = name
                company_url = href
                break

        # City
        city = None
        city_match = re.findall(
            r'(?:Москва|Санкт-Петербург|Екатеринбург|Новосибирск|Челябинск|Красноярск|'
            r'Казань|Краснодар|Ростов-на-Дону|Самара|Нижний Новгород|Уфа|Пермь|Омск|'
            r'Воронеж|Волгоград|Тюмень|Иркутск|Хабаровск|Владивосток|Барнаул|Кемерово|'
            r'Якутск|Магадан|Томск|Саратов|Тольятти)',
            html,
        )
        if city_match:
            city = city_match[0]

        # Parse product fields
        offers = product.get("offers", {})
        brand_data = product.get("brand", {})
        images = product.get("image", [])
        if isinstance(images, str):
            images = [images]

        return {
            "ad_id": ad_id,
            "title": product.get("name", ""),
            "article": product.get("sku", ""),
            "brand": brand_data.get("name") if isinstance(brand_data, dict) else None,
            "description": product.get("description", ""),
            "price": float(offers.get("price")) if offers.get("price") else None,
            "currency": offers.get("priceCurrency", "RUB"),
            "images": images,
            "company_name": company_name,
            "company_url": company_url,
            "city": city,
            "source_url": f"{BASE_URL}{url}",
        }

    async def get_company_contacts(self, company_url: str) -> dict:
        """Parse company contacts from profile page"""
        html = await self.http.get_text(f"{BASE_URL}{company_url}")

        phones = re.findall(r'\+7\s*\(\d{3}\)\s*\d{3}[\-\s]\d{2}[\-\s]\d{2}', html)
        emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', html)
        emails = [e for e in emails if 'exkavator' not in e.lower()]

        city = None
        meta_match = re.findall(r'Россия,\s*(?:[\w\s]+область,\s*)?([\w\s\-]+?)(?:,|\s*<)', html)
        if meta_match:
            city = meta_match[0].strip()

        h1 = re.findall(r'<h1[^>]*>([\s\S]*?)</h1>', html)
        name = None
        if h1:
            name = re.sub(r'<[^>]+>', '', h1[0]).strip().replace("Компания ", "")

        return {
            "name": name,
            "phone": phones[0] if phones else None,
            "email": emails[0] if emails else None,
            "city": city,
            "source_url": f"{BASE_URL}{company_url}",
        }

    async def collect_bulk(self, max_categories: int = 0, max_ads_per_cat: int = 0, **kwargs) -> CollectorStats:
        """
        Bulk collect from exkavator.ru.

        Args:
            max_categories: Limit categories (0 = all)
            max_ads_per_cat: Limit ads per category (0 = all)
        """
        stats = CollectorStats(collector_name="exkavator")
        db = SessionLocal()
        writer = SupplierWriter(db)
        ads_file = self.output_dir / "ads.jsonl"

        try:
            # Step 1: Get subcategories
            categories = await self.get_subcategories()
            if max_categories > 0:
                categories = categories[:max_categories]

            # Step 2-4: Parse each category
            for cat_idx, cat in enumerate(categories):
                logger.info(f"Category {cat_idx + 1}/{len(categories)}: {cat['url']} ({cat['count']} ads)")

                ad_ids = await self.get_ads_from_category(cat["url"])
                if max_ads_per_cat > 0:
                    ad_ids = ad_ids[:max_ads_per_cat]

                for ad_id in ad_ids:
                    try:
                        ad = await self.get_ad_detail(ad_id)
                        if not ad:
                            continue

                        stats.total_found += 1

                        # Save ad to JSONL
                        with open(ads_file, "a", encoding="utf-8") as f:
                            f.write(json.dumps(ad, ensure_ascii=False) + "\n")

                        # Parse company if new
                        company_url = ad.get("company_url")
                        if company_url and company_url not in self._parsed_companies:
                            self._parsed_companies.add(company_url)
                            contacts = await self.get_company_contacts(company_url)

                            supplier_data = {
                                "name": contacts.get("name") or ad.get("company_name", ""),
                                "phone": contacts.get("phone"),
                                "email": contacts.get("email"),
                                "city": contacts.get("city") or ad.get("city"),
                                "source": "exkavator",
                                "source_url": contacts.get("source_url"),
                                "source_id": company_url.rstrip("/").split("/")[-1],
                            }

                            if supplier_data["name"]:
                                result = writer.batch_import([supplier_data])
                                stats.created += result.created
                                stats.updated += result.updated
                                stats.skipped += result.skipped

                    except Exception as e:
                        logger.warning(f"Error processing ad {ad_id}: {e}")
                        stats.errors += 1

        finally:
            db.close()
            await self.http.close()

        stats.finished_at = datetime.utcnow()
        logger.info(
            f"exkavator done: {stats.total_found} ads, "
            f"{stats.created} suppliers created, {len(self._parsed_companies)} companies parsed"
        )
        return stats

    async def collect_new(self, since: datetime, **kwargs) -> CollectorStats:
        """Exkavator doesn't support filtering by date"""
        return await self.collect_bulk(**kwargs)

    def get_schedule(self) -> str:
        return "0 0 */7 * *"  # Weekly

    async def close(self):
        await self.http.close()
