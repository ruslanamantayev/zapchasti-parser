"""
HeSpareParts.com parts catalog collector.
Parses aftermarket parts for heavy equipment brands.
Migrated from zapchasti-platform/scripts/parsers/hespareparts_parser.py

Usage:
    python scripts/run_bulk.py hespareparts
    python scripts/run_bulk.py hespareparts --brand caterpillar --pages 5
"""
import csv
import json
import logging
import re
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from src.collectors.base import BaseCollector, CollectorStats

logger = logging.getLogger(__name__)

BASE_URL = "https://hespareparts.com"
RATE_LIMIT = 1.0
USER_AGENT = "Mozilla/5.0 (compatible; ZapChastiBot/1.0; +https://xn--80aatggg1adtkd7c1e.xn--p1ai)"

BRANDS = {
    "caterpillar": "/caterpillar-aftermarket/",
    "komatsu": "/komatsu-aftermarket/",
    "volvo": "/volvo-aftermarket/",
    "hitachi": "/hitachi-aftermarket/",
    "hyundai": "/hyundai-aftermarket/",
    "jcb": "/jcb-aftermarket/",
    "doosan": "/doosan-aftermarket/",
    "cummins": "/cummins-aftermarket/",
    "john-deere": "/john-deere-aftermarket/",
    "kubota": "/kubota-aftermarket/",
    "case": "/case-aftermarket/",
    "kobelco": "/kobelco-aftermarket/",
    "terex": "/terex-aftermarket/",
    "liebherr": "/liebherr-aftermarket/",
    "perkins": "/perkins-aftermarket/",
}

EQUIPMENT_MAP = {
    "EXCAVATOR": "excavator",
    "HYDRAULIC EXCAVATOR": "excavator",
    "MINI HYDRAULIC EXCAVATOR": "mini_excavator",
    "WHEEL LOADER": "wheel_loader",
    "TRACK-TYPE LOADER": "track_loader",
    "TRACK-TYPE TRACTOR": "bulldozer",
    "BULLDOZER": "bulldozer",
    "MOTOR GRADER": "grader",
    "SCRAPER": "scraper",
    "WHEEL TRACTOR-SCRAPER": "scraper",
    "PIPELAYER": "pipelayer",
    "OFF-HIGHWAY TRUCK": "dump_truck",
    "ARTICULATED TRUCK": "articulated_truck",
    "MINING TRUCK": "mining_truck",
    "COMPACTOR": "compactor",
    "PAVER": "paver",
    "ASPHALT PAVER": "paver",
    "COLD PLANER": "cold_planer",
    "BACKHOE LOADER": "backhoe_loader",
    "SKID STEER LOADER": "skid_steer",
    "TELEHANDLER": "telehandler",
    "FOREST PRODUCTS": "forestry",
    "MATERIAL HANDLER": "material_handler",
    "CRANE": "crane",
    "INTEGRATED TOOLCARRIER": "toolcarrier",
    "EXPANDED MINING": "mining",
    "UNDERGROUND MINING": "mining",
    "OEM SOLUTIONS": "oem",
}


@dataclass
class PartListItem:
    article: str
    name: str
    url: str
    brand: str


@dataclass
class PartDetail:
    article: str
    name: str
    brand: str
    url: str
    length_mm: Optional[float] = None
    width_mm: Optional[float] = None
    height_mm: Optional[float] = None
    weight_kg: Optional[float] = None
    compatible_models: list = field(default_factory=list)
    equipment_types: list = field(default_factory=list)
    oem_alternative_url: Optional[str] = None
    image_url: Optional[str] = None
    source: str = "hespareparts"


class SyncHttpClient:
    """Synchronous HTTP client with rate limiting (hespareparts blocks async)"""

    def __init__(self, rate_limit: float = RATE_LIMIT):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
        })
        self.rate_limit = rate_limit
        self.last_request_time = 0
        self.request_count = 0

    def get(self, url: str, retries: int = 3) -> Optional[BeautifulSoup]:
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)

        for attempt in range(retries):
            try:
                self.last_request_time = time.time()
                self.request_count += 1

                response = self.session.get(url, timeout=30)

                if response.status_code == 200:
                    return BeautifulSoup(response.text, "html.parser")
                elif response.status_code == 404:
                    logger.warning(f"404: {url}")
                    return None
                elif response.status_code == 429:
                    wait = (attempt + 1) * 15
                    logger.warning(f"429 Rate limit, waiting {wait}s")
                    time.sleep(wait)
                else:
                    logger.warning(f"HTTP {response.status_code}: {url}")
                    time.sleep(5)

            except requests.RequestException as e:
                logger.error(f"Request error (attempt {attempt + 1}): {e}")
                time.sleep(10)

        logger.error(f"Failed after {retries} retries: {url}")
        return None

    def close(self):
        self.session.close()


class HesparepartsCollector(BaseCollector):
    """Collects aftermarket parts catalog from hespareparts.com"""
    source_name = "hespareparts"

    def __init__(self, output_dir: Optional[Path] = None):
        self.client = SyncHttpClient()
        self.output_dir = output_dir or Path("data/raw/hespareparts")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.progress_file = self.output_dir / "progress.json"
        self.progress = self._load_progress()

    def _load_progress(self) -> dict:
        if self.progress_file.exists():
            with open(self.progress_file) as f:
                return json.load(f)
        return {}

    def _save_progress(self):
        with open(self.progress_file, "w") as f:
            json.dump(self.progress, f, indent=2)

    def _is_page_done(self, brand: str, page: int) -> bool:
        return str(page) in self.progress.get(brand, {}).get("done_pages", [])

    def _mark_page_done(self, brand: str, page: int):
        if brand not in self.progress:
            self.progress[brand] = {"done_pages": [], "total_pages": 0, "parts_count": 0}
        if str(page) not in self.progress[brand]["done_pages"]:
            self.progress[brand]["done_pages"].append(str(page))
        self._save_progress()

    def discover_brand_pages(self, brand: str) -> int:
        url_path = BRANDS.get(brand)
        if not url_path:
            return 0

        soup = self.client.get(BASE_URL + url_path)
        if not soup:
            return 0

        total_pages = 1
        for link in soup.select("a[href*='/page/']"):
            match = re.search(r"/page/(\d+)", link.get("href", ""))
            if match:
                total_pages = max(total_pages, int(match.group(1)))

        if brand not in self.progress:
            self.progress[brand] = {"done_pages": [], "total_pages": 0, "parts_count": 0}
        self.progress[brand]["total_pages"] = total_pages
        self._save_progress()

        logger.info(f"{brand}: {total_pages} pages")
        return total_pages

    def parse_listing_page(self, brand: str, page: int) -> list[PartListItem]:
        url_path = BRANDS.get(brand)
        url = BASE_URL + url_path if page == 1 else BASE_URL + url_path + f"page/{page}/"

        soup = self.client.get(url)
        if not soup:
            return []

        items = []
        for link in soup.select("a[href*='-new-aftermarket.html'], a[href*='-aftermarket.html']"):
            href = link.get("href", "")
            text = link.get_text(strip=True)
            if not href or not text:
                continue

            match = re.match(r"^([\w\-]+)\s*-\s*(.+?)(?:\s*-\s*New Aftermarket)?$", text)
            if match:
                article = match.group(1).strip()
                name = match.group(2).strip()
            else:
                url_match = re.search(r"/([a-z0-9\-]+?)-[a-z]+-(?:new-)?aftermarket\.html", href.lower())
                if url_match:
                    article = url_match.group(1).upper()
                    name = text
                else:
                    continue

            items.append(PartListItem(article=article, name=name, url=urljoin(BASE_URL, href), brand=brand))

        # Deduplicate by article
        seen = set()
        unique = []
        for item in items:
            if item.article not in seen:
                seen.add(item.article)
                unique.append(item)
        return unique

    def parse_detail_page(self, item: PartListItem) -> Optional[PartDetail]:
        soup = self.client.get(item.url)
        if not soup:
            return None

        detail = PartDetail(article=item.article, name=item.name, brand=item.brand, url=item.url)
        text_content = soup.get_text("\n", strip=True)

        # Dimensions
        for dim, attr in [("Длина", "length_mm"), ("Ширина", "width_mm"), ("Высота", "height_mm")]:
            match = re.search(rf"{dim}:\s*(\d+\.?\d*)\s*мм", text_content)
            if match:
                setattr(detail, attr, float(match.group(1)))

        weight_match = re.search(r"Вес:\s*(\d+\.?\d*)\s*(?:кг|kg)", text_content)
        if weight_match:
            detail.weight_kg = float(weight_match.group(1))

        # Compatible models
        compat_idx = text_content.find("COMPATIBLE MODELS:")
        if compat_idx >= 0:
            compat_text = text_content[compat_idx:]
            models = []
            equipment_types = []

            for line in compat_text.split("\n"):
                line = line.strip().rstrip(",")
                if not line:
                    continue

                line_upper = line.upper()
                is_eq = False
                for eq_key, eq_val in EQUIPMENT_MAP.items():
                    if eq_key in line_upper:
                        if eq_val not in equipment_types:
                            equipment_types.append(eq_val)
                        is_eq = True
                        break

                if is_eq:
                    continue

                for part in re.split(r"[,\s]+", line):
                    part = part.strip().rstrip(",")
                    if 2 <= len(part) <= 20 and re.search(r"\d", part):
                        if part not in ("New", "Aftermarket", "OEM", "USA", "Miami"):
                            models.append(part)

            detail.compatible_models = models
            detail.equipment_types = equipment_types

        # Image
        for img in soup.select("img[src]"):
            src = img.get("src", "")
            if src and "logo" not in src.lower():
                if any(x in src.lower() for x in ["product", "image/cache", "catalog", "data/"]):
                    detail.image_url = urljoin(BASE_URL, src)
                    break

        # OEM link
        for link in soup.select("a[href]"):
            text = link.get_text(strip=True).lower()
            if "oem" in text or "original" in text:
                detail.oem_alternative_url = urljoin(BASE_URL, link.get("href", ""))
                break

        return detail

    def save_parts(self, parts: list[PartDetail], brand: str):
        """Save parts to CSV + JSONL"""
        csv_file = self.output_dir / f"{brand}.csv"
        jsonl_file = self.output_dir / f"{brand}.jsonl"
        file_exists = csv_file.exists()

        fieldnames = [
            "article", "name", "brand", "url",
            "length_mm", "width_mm", "height_mm", "weight_kg",
            "compatible_models", "equipment_types",
            "oem_alternative_url", "image_url", "source",
        ]

        with open(csv_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            for part in parts:
                row = asdict(part)
                row["compatible_models"] = ";".join(row["compatible_models"])
                row["equipment_types"] = ";".join(row["equipment_types"])
                writer.writerow(row)

        with open(jsonl_file, "a", encoding="utf-8") as f:
            for part in parts:
                f.write(json.dumps(asdict(part), ensure_ascii=False) + "\n")

    async def collect_bulk(
        self,
        brand: Optional[str] = None,
        max_pages: int = 0,
        resume: bool = True,
        parse_details: bool = True,
        **kwargs,
    ) -> CollectorStats:
        """
        Bulk collect parts from hespareparts.com.

        Args:
            brand: Single brand slug (e.g. "caterpillar"). None = all brands.
            max_pages: Max pages per brand (0 = all)
            resume: Skip already-parsed pages
            parse_details: Parse detail pages (slow but complete)
        """
        stats = CollectorStats(collector_name="hespareparts")
        brands_to_parse = [brand] if brand else list(BRANDS.keys())

        for brand_slug in brands_to_parse:
            if brand_slug not in BRANDS:
                logger.error(f"Unknown brand: {brand_slug}")
                continue

            logger.info(f"=== Parsing {brand_slug} ===")
            total_pages = self.discover_brand_pages(brand_slug)
            if total_pages == 0:
                continue

            end_page = min(max_pages, total_pages) if max_pages > 0 else total_pages

            for page in range(1, end_page + 1):
                if resume and self._is_page_done(brand_slug, page):
                    continue

                logger.info(f"[{brand_slug}] Page {page}/{end_page}")
                items = self.parse_listing_page(brand_slug, page)
                stats.total_found += len(items)

                if not items:
                    continue

                if parse_details:
                    details = []
                    for item in items:
                        detail = self.parse_detail_page(item)
                        if detail:
                            details.append(detail)
                    if details:
                        self.save_parts(details, brand_slug)
                        stats.created += len(details)
                else:
                    quick = [PartDetail(article=i.article, name=i.name, brand=i.brand, url=i.url) for i in items]
                    self.save_parts(quick, brand_slug)
                    stats.created += len(quick)

                self._mark_page_done(brand_slug, page)

                if page % 50 == 0:
                    logger.info(f"  Pausing 10s after {page} pages...")
                    time.sleep(10)

        stats.finished_at = datetime.utcnow()
        logger.info(f"=== hespareparts done: {stats.created} parts, {self.client.request_count} requests ===")
        return stats

    async def collect_new(self, since: datetime, **kwargs) -> CollectorStats:
        """Run bulk with resume=True to only get new pages"""
        return await self.collect_bulk(resume=True, **kwargs)

    def get_schedule(self) -> str:
        return "0 0 * * 0"  # Weekly on Sunday

    async def close(self):
        self.client.close()
