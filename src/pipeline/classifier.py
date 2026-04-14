"""
Classifier: auto-tag suppliers with equipment types, part categories, and brands
based on text analysis of rubrics, company name, and raw_data.
"""
import logging
import re
from typing import Optional

from sqlalchemy.orm import Session

from src.models.supplier import (
    ParsedSupplier as Supplier,
    ParsedSupplierBrand as SupplierBrand,
    ParsedSupplierEquipmentType as SupplierEquipmentType,
    ParsedSupplierPartCategory as SupplierPartCategory,
)

logger = logging.getLogger(__name__)

# Keyword → equipment_type_id mapping (IDs from zapchasti DB)
# These will be loaded from DB in production; hardcoded here as fallback
EQUIPMENT_KEYWORDS = {
    "экскаватор": "excavator",
    "погрузчик": "loader",
    "бульдозер": "bulldozer",
    "кран": "crane",
    "трактор": "tractor",
    "комбайн": "combine",
    "грейдер": "grader",
    "каток": "roller",
    "самосвал": "dump_truck",
    "автокран": "truck_crane",
    "манипулятор": "manipulator",
    "вилочный": "forklift",
    "мини-погрузчик": "skid_steer",
    "телескопический": "telehandler",
    "асфальтоукладчик": "paver",
    "excavator": "excavator",
    "loader": "loader",
    "bulldozer": "bulldozer",
    "crane": "crane",
    "tractor": "tractor",
    "grader": "grader",
}

CATEGORY_KEYWORDS = {
    "гидравлик": "hydraulics",
    "двигател": "engine",
    "фильтр": "filters",
    "ходовая": "undercarriage",
    "гусениц": "tracks",
    "ковш": "bucket",
    "турбин": "turbo",
    "электрик": "electrical",
    "тормоз": "brakes",
    "кабин": "cabin",
    "масл": "oils",
    "подшипник": "bearings",
    "уплотнен": "seals",
    "hydraulic": "hydraulics",
    "engine": "engine",
    "filter": "filters",
    "bucket": "bucket",
    "track": "tracks",
}

BRAND_KEYWORDS = {
    "caterpillar": "caterpillar", "cat ": "caterpillar",
    "komatsu": "komatsu",
    "volvo": "volvo",
    "hitachi": "hitachi",
    "hyundai": "hyundai",
    "jcb": "jcb",
    "doosan": "doosan",
    "cummins": "cummins",
    "john deere": "john_deere", "deere": "john_deere",
    "kubota": "kubota",
    "liebherr": "liebherr",
    "case": "case",
    "kobelco": "kobelco",
    "камаз": "kamaz",
    "мтз": "mtz",
    "белаз": "belaz",
    "xcmg": "xcmg",
    "shantui": "shantui",
    "lonking": "lonking",
    "sdlg": "sdlg",
}


class Classifier:
    """Classify suppliers by equipment types, part categories, and brands"""

    def __init__(self, db: Session):
        self.db = db
        self._equipment_type_ids: dict[str, int] = {}
        self._part_category_ids: dict[str, int] = {}
        self._brand_ids: dict[str, int] = {}
        self._loaded = False

    def _load_ids_from_db(self):
        """Load ID mappings from reference tables"""
        if self._loaded:
            return

        try:
            from sqlalchemy import text
            # Load equipment types
            rows = self.db.execute(text("SELECT id, slug FROM equipment_types")).fetchall()
            self._equipment_type_ids = {row[1]: row[0] for row in rows}

            # Load part categories
            rows = self.db.execute(text("SELECT id, slug FROM part_categories")).fetchall()
            self._part_category_ids = {row[1]: row[0] for row in rows}

            # Load brands
            rows = self.db.execute(text("SELECT id, slug FROM brands")).fetchall()
            self._brand_ids = {row[1]: row[0] for row in rows}

            self._loaded = True
            logger.info(
                f"Classifier loaded: {len(self._equipment_type_ids)} equipment types, "
                f"{len(self._part_category_ids)} categories, {len(self._brand_ids)} brands"
            )
        except Exception as e:
            logger.warning(f"Could not load IDs from DB: {e}")

    def classify_text(self, text: str) -> dict:
        """
        Extract equipment types, categories, and brands from text.
        Returns dict with slug lists.
        """
        text_lower = text.lower()

        equipment = set()
        for keyword, slug in EQUIPMENT_KEYWORDS.items():
            if keyword in text_lower:
                equipment.add(slug)

        categories = set()
        for keyword, slug in CATEGORY_KEYWORDS.items():
            if keyword in text_lower:
                categories.add(slug)

        brands = set()
        for keyword, slug in BRAND_KEYWORDS.items():
            if keyword in text_lower:
                brands.add(slug)

        return {
            "equipment_types": list(equipment),
            "part_categories": list(categories),
            "brands": list(brands),
        }

    def classify_supplier(self, supplier: Supplier) -> dict:
        """Classify a supplier based on its name, rubrics, and raw_data"""
        parts = [supplier.name or ""]

        if supplier.raw_data:
            rubrics = supplier.raw_data.get("rubrics", [])
            if isinstance(rubrics, list):
                parts.extend(rubrics)

        text = " ".join(parts)
        return self.classify_text(text)

    def apply_classification(self, supplier: Supplier, classification: dict):
        """Apply classification by setting M2M relationships"""
        self._load_ids_from_db()

        # Equipment types
        for slug in classification.get("equipment_types", []):
            et_id = self._equipment_type_ids.get(slug)
            if et_id:
                existing = (
                    self.db.query(SupplierEquipmentType)
                    .filter_by(supplier_id=supplier.id, equipment_type_id=et_id)
                    .first()
                )
                if not existing:
                    self.db.add(SupplierEquipmentType(supplier_id=supplier.id, equipment_type_id=et_id))

        # Part categories
        for slug in classification.get("part_categories", []):
            pc_id = self._part_category_ids.get(slug)
            if pc_id:
                existing = (
                    self.db.query(SupplierPartCategory)
                    .filter_by(supplier_id=supplier.id, part_category_id=pc_id)
                    .first()
                )
                if not existing:
                    self.db.add(SupplierPartCategory(supplier_id=supplier.id, part_category_id=pc_id))

        # Brands
        for slug in classification.get("brands", []):
            brand_id = self._brand_ids.get(slug)
            if brand_id:
                existing = (
                    self.db.query(SupplierBrand)
                    .filter_by(supplier_id=supplier.id, brand_id=brand_id)
                    .first()
                )
                if not existing:
                    self.db.add(SupplierBrand(supplier_id=supplier.id, brand_id=brand_id))

        self.db.flush()
