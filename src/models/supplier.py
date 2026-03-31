"""
Supplier model — maps to the same 'suppliers' table used by zapchasti-platform.
This is a read/write mapping, not a copy. Both platform and parser share one DB.
"""
import uuid
from sqlalchemy import (
    Column, String, Integer, Boolean, DateTime, Text, Float,
    ARRAY, ForeignKey, UniqueConstraint, Index,
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from src.config.database import Base


class Supplier(Base):
    __tablename__ = "suppliers"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    # Core
    name = Column(String, nullable=False)
    slug = Column(String, unique=True, index=True)
    inn = Column(String, unique=True, nullable=True)
    supplier_type = Column(String)
    legal_form = Column(String)

    # Contacts
    phone = Column(String)
    phone_extra = Column(ARRAY(String), default=list)
    email = Column(String)
    email_extra = Column(ARRAY(String), default=list)
    telegram = Column(String)
    whatsapp = Column(String)
    website = Column(String)
    vk = Column(String)

    # Location
    city = Column(String)
    region = Column(String)
    country = Column(String, default="Россия")
    address = Column(String)

    # Source tracking
    source = Column(String)
    source_url = Column(String)
    source_id = Column(String)
    raw_data = Column(JSONB, default=dict)

    # CRM / Outreach
    status = Column(String, default="new")
    outreach_channel = Column(String)
    outreach_count = Column(Integer, default=0)
    last_outreach_at = Column(DateTime)
    last_response_at = Column(DateTime)
    response_type = Column(String)
    notes = Column(Text)

    # Platform link
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=True)
    has_pricelist = Column(Boolean, default=False)
    pricelist_updated_at = Column(DateTime)

    # Quality metrics
    rating_2gis = Column(Float)
    reviews_count = Column(Integer)
    ads_count_avito = Column(Integer)
    employee_count = Column(Integer)
    revenue_estimate = Column(String)

    # Meta
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    is_duplicate = Column(Boolean, default=False)
    duplicate_of = Column(UUID(as_uuid=True), ForeignKey("suppliers.id"), nullable=True)

    __table_args__ = (
        Index("idx_suppliers_status", "status"),
        Index("idx_suppliers_city", "city"),
        Index("idx_suppliers_source", "source"),
        Index(
            "idx_suppliers_source_dedup",
            "source", "source_id",
            unique=True,
            postgresql_where=(source_id != None),  # noqa: E711
        ),
        {"extend_existing": True},
    )


class SupplierBrand(Base):
    __tablename__ = "supplier_brands"

    id = Column(Integer, primary_key=True)
    supplier_id = Column(UUID(as_uuid=True), ForeignKey("suppliers.id", ondelete="CASCADE"), nullable=False)
    brand_id = Column(Integer, ForeignKey("brands.id", ondelete="CASCADE"), nullable=False)

    __table_args__ = (
        UniqueConstraint("supplier_id", "brand_id"),
        {"extend_existing": True},
    )


class SupplierEquipmentType(Base):
    __tablename__ = "supplier_equipment_types"

    id = Column(Integer, primary_key=True)
    supplier_id = Column(UUID(as_uuid=True), ForeignKey("suppliers.id", ondelete="CASCADE"), nullable=False)
    equipment_type_id = Column(Integer, ForeignKey("equipment_types.id", ondelete="CASCADE"), nullable=False)

    __table_args__ = (
        UniqueConstraint("supplier_id", "equipment_type_id"),
        {"extend_existing": True},
    )


class SupplierPartCategory(Base):
    __tablename__ = "supplier_part_categories"

    id = Column(Integer, primary_key=True)
    supplier_id = Column(UUID(as_uuid=True), ForeignKey("suppliers.id", ondelete="CASCADE"), nullable=False)
    part_category_id = Column(Integer, ForeignKey("part_categories.id", ondelete="CASCADE"), nullable=False)

    __table_args__ = (
        UniqueConstraint("supplier_id", "part_category_id"),
        {"extend_existing": True},
    )
