"""Generate search queries for a spare part using LLM."""
import logging

from src.utils.llm import LLMClient
from .models import SearchRequest, SearchQuery, SearchDepth

logger = logging.getLogger(__name__)

DEPTH_LIMITS = {
    SearchDepth.MINIMAL: 1,
    SearchDepth.QUICK: 5,
    SearchDepth.NORMAL: 15,
    SearchDepth.DEEP: 25,
}

SYSTEM_PROMPT = """You are a search query generator for industrial spare parts procurement.
Your task: generate Google search queries to find suppliers who sell a specific part.

Return a JSON array of objects: [{"query": "...", "strategy": "...", "language": "..."}]

Strategies to use:
- direct: exact article number + buy/price/in-stock keywords
- description: part name/description + equipment context + supplier/dealer keywords
- manufacturer: OEM brand + dealer/distributor/authorized keywords
- equipment: equipment model + spare parts/components keywords
- cross_reference: known cross-reference or OEM numbers (if you know them)
- service: repair/maintenance centers that service this equipment type (only if requested)

Rules:
- Always start with direct queries using the exact article number
- Mix languages based on the provided languages list
- Russian keywords: "купить", "цена", "в наличии", "запчасти", "поставщик", "склад"
- English keywords: "buy", "price", "in stock", "spare parts", "supplier", "warehouse"
- When equipment info is available, use it to generate equipment-specific queries
- Avoid generic queries — be specific to the part and industry
- Each query should be distinct and target different potential supplier types"""


class QueryGenerator:
    def __init__(self, llm: LLMClient):
        self.llm = llm

    async def generate(self, request: SearchRequest) -> list[SearchQuery]:
        """Generate search queries using LLM, with deterministic fallback."""
        limit = DEPTH_LIMITS[request.depth]
        user_prompt = self._build_prompt(request, limit)

        try:
            data = await self.llm.complete_json(user_prompt, system=SYSTEM_PROMPT)
            if not isinstance(data, list):
                raise ValueError(f"Expected list, got {type(data)}")
            queries = [SearchQuery(**item) for item in data]
            logger.info(f"LLM generated {len(queries)} queries")
            return queries[:limit]
        except Exception as e:
            logger.warning(f"LLM query generation failed: {e}, using fallback")
            return self._fallback_queries(request, limit)

    def _build_prompt(self, req: SearchRequest, limit: int) -> str:
        parts = [f"Article: {req.article}"]
        if req.name:
            parts.append(f"Part name (RU): {req.name}")
        if req.name_en:
            parts.append(f"Part name (EN): {req.name_en}")
        if req.equipment_brand:
            parts.append(f"Equipment brand: {req.equipment_brand}")
        if req.equipment_model:
            parts.append(f"Equipment model: {req.equipment_model}")
        if req.equipment_type:
            parts.append(f"Equipment type: {req.equipment_type}")
        parts.append(f"Languages: {', '.join(req.languages)}")
        parts.append(f"Include service centers: {req.include_services}")
        parts.append(f"Generate exactly {limit} queries.")
        return "\n".join(parts)

    def _fallback_queries(self, req: SearchRequest, limit: int) -> list[SearchQuery]:
        """Deterministic fallback when LLM is unavailable."""
        queries = []
        art = req.article

        # Direct queries
        queries.append(SearchQuery(query=f"{art} купить", strategy="direct", language="ru"))
        queries.append(SearchQuery(query=f"{art} цена в наличии", strategy="direct", language="ru"))
        queries.append(SearchQuery(query=f"{art} buy price", strategy="direct", language="en"))

        # Description queries
        if req.name:
            queries.append(SearchQuery(
                query=f"{req.name} {art} поставщик",
                strategy="description", language="ru",
            ))
        if req.name_en:
            queries.append(SearchQuery(
                query=f"{req.name_en} {art} supplier",
                strategy="description", language="en",
            ))

        # Equipment queries
        if req.equipment_brand:
            eq = f"{req.equipment_brand} {req.equipment_model}".strip()
            queries.append(SearchQuery(
                query=f"{eq} запчасти {art}",
                strategy="equipment", language="ru",
            ))
            queries.append(SearchQuery(
                query=f"{eq} parts {art}",
                strategy="equipment", language="en",
            ))

        # Manufacturer queries
        queries.append(SearchQuery(
            query=f"{art} dealer distributor",
            strategy="manufacturer", language="en",
        ))
        queries.append(SearchQuery(
            query=f"{art} дилер дистрибьютор",
            strategy="manufacturer", language="ru",
        ))

        # Service (if requested)
        if req.include_services and req.equipment_brand:
            queries.append(SearchQuery(
                query=f"{req.equipment_brand} сервис ремонт запчасти",
                strategy="service", language="ru",
            ))

        return queries[:limit]
