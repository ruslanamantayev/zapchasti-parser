"""Find cross-reference / alternative part numbers using LLM."""
import logging

from src.utils.llm import LLMClient
from .models import SearchRequest

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are an industrial spare parts cross-reference expert.
Given a part number and context, return alternative/cross-reference part numbers
that are interchangeable or equivalent.

Return JSON: {"cross_references": [{"article": "...", "manufacturer": "...", "notes": "..."}]}

Rules:
- Only return numbers you are confident about
- Include OEM equivalents, aftermarket alternatives, and known cross-references
- Consider manufacturer acquisition chains (e.g., Bucyrus → CAT, P&H → Komatsu)
- If unsure, return empty list: {"cross_references": []}"""


class CrossReferenceEngine:
    def __init__(self, llm: LLMClient):
        self.llm = llm

    async def find_alternatives(self, request: SearchRequest) -> list[str]:
        """Return list of alternative article numbers."""
        prompt_parts = [f"Part number: {request.article}"]
        if request.name:
            prompt_parts.append(f"Part name: {request.name}")
        if request.name_en:
            prompt_parts.append(f"Part name (EN): {request.name_en}")
        if request.equipment_brand:
            prompt_parts.append(
                f"Equipment: {request.equipment_brand} {request.equipment_model}".strip()
            )
        if request.equipment_type:
            prompt_parts.append(f"Equipment type: {request.equipment_type}")

        try:
            data = await self.llm.complete_json("\n".join(prompt_parts), system=SYSTEM_PROMPT)
            refs = data.get("cross_references", [])
            articles = [r["article"] for r in refs if r.get("article")]
            if articles:
                logger.info(f"Found {len(articles)} cross-references: {articles}")
            return articles
        except Exception as e:
            logger.warning(f"Cross-reference lookup failed: {e}")
            return []
