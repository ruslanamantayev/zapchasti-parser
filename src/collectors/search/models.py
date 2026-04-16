"""Data models for the search engine pipeline."""
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class SearchDepth(str, Enum):
    MINIMAL = "minimal"
    QUICK = "quick"
    NORMAL = "normal"
    DEEP = "deep"


class SearchRequest(BaseModel):
    """Input: what to search for."""
    article: str
    name: str = ""
    name_en: str = ""
    equipment_brand: str = ""
    equipment_model: str = ""
    equipment_type: str = ""
    depth: SearchDepth = SearchDepth.MINIMAL
    languages: list[str] = Field(default_factory=lambda: ["ru", "en"])
    include_analogs: bool = True
    include_services: bool = False
    # Link back to CRM
    position_id: Optional[int] = None
    request_id: Optional[int] = None
    task_id: Optional[int] = None      # crm.search_tasks.id


class SearchQuery(BaseModel):
    """A generated search query with metadata."""
    query: str
    strategy: str  # direct / description / manufacturer / equipment / cross_reference / service
    language: str = "ru"


class GoogleResult(BaseModel):
    """A single Google search result."""
    url: str
    title: str = ""
    snippet: str = ""
    query: str = ""
    strategy: str = ""


class SitePage(BaseModel):
    """Downloaded page content."""
    url: str
    html: str
    is_contact_page: bool = False


class ExtractedContact(BaseModel):
    """Contacts extracted from a single site."""
    source_url: str
    company_name: str = ""
    emails: list[str] = Field(default_factory=list)
    phones: list[str] = Field(default_factory=list)
    website: str = ""
    city: str = ""
    country: str = ""
    price: Optional[float] = None
    currency: str = ""
    in_stock: Optional[bool] = None
    part_number: str = ""


class SearchResultItem(BaseModel):
    """Final ranked result ready for DB write."""
    company_name: str
    email: str = ""
    phone: str = ""
    website: str = ""
    city: str = ""
    country: str = ""
    price: Optional[float] = None
    currency: str = ""
    in_stock: Optional[bool] = None
    delivery_days: Optional[int] = None
    part_number: str = ""
    condition: str = ""
    source_url: str = ""
    search_query: str = ""
    search_strategy: str = ""
    confidence: float = 0.0
    notes: str = ""


class SearchStats(BaseModel):
    """Stats for a search run."""
    article: str
    queries_generated: int = 0
    google_results: int = 0
    sites_parsed: int = 0
    contacts_extracted: int = 0
    results_after_dedup: int = 0
    suppliers_created: int = 0
    suppliers_updated: int = 0
    results_linked: int = 0
    errors: list[str] = Field(default_factory=list)
