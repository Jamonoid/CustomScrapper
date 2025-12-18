"""SQLAlchemy models for the price monitoring service."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
    JSON,
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class Product(Base):
    """Represents an internal product."""

    __tablename__ = "products"

    id: int = Column(Integer, primary_key=True)
    sku_interno: str = Column(String, unique=True, nullable=False)
    nombre: str = Column(String, nullable=False)
    marca: Optional[str] = Column(String, nullable=True)
    modelo: Optional[str] = Column(String, nullable=True)
    ean: Optional[str] = Column(String, nullable=True)
    activo: bool = Column(Boolean, default=True, nullable=False)

    listings = relationship("Listing", back_populates="product")


class Channel(Base):
    """Represents a sales or competitive channel."""

    __tablename__ = "channels"

    id: int = Column(Integer, primary_key=True)
    nombre: str = Column(String, unique=True, nullable=False)
    tipo: str = Column(String, nullable=False)

    listings = relationship("Listing", back_populates="channel")


class Listing(Base):
    """Associates an internal product with a channel-specific listing."""

    __tablename__ = "listings"
    __table_args__ = (
        UniqueConstraint("product_id", "channel_id", name="uq_product_channel"),
    )

    id: int = Column(Integer, primary_key=True)
    product_id: int = Column(Integer, ForeignKey("products.id"), nullable=False)
    channel_id: int = Column(Integer, ForeignKey("channels.id"), nullable=False)
    seller_sku: Optional[str] = Column(String, nullable=True)
    listing_id: Optional[str] = Column(String, nullable=True)
    url_pdp: Optional[str] = Column(String, nullable=True)
    monitorear_competencia: bool = Column(Boolean, default=False, nullable=False)
    monitorear_propio: bool = Column(Boolean, default=False, nullable=False)
    frecuencia_minutos: int = Column(Integer, nullable=False, default=60)

    product = relationship("Product", back_populates="listings")
    channel = relationship("Channel", back_populates="listings")
    own_snapshots = relationship("OwnPriceSnapshot", back_populates="listing")
    competitor_snapshots = relationship("CompetitorPriceSnapshot", back_populates="listing")
    alerts = relationship("Alert", back_populates="listing")


class OwnPriceSnapshot(Base):
    """Stores price history for the owned listings."""

    __tablename__ = "own_price_snapshots"

    id: int = Column(Integer, primary_key=True)
    timestamp: datetime = Column(DateTime, default=datetime.utcnow, nullable=False)
    listing_id: int = Column(Integer, ForeignKey("listings.id"), nullable=False)
    precio: float = Column(Numeric(12, 2), nullable=False)
    stock: Optional[int] = Column(Integer, nullable=True)
    moneda: str = Column(String, default="CLP", nullable=False)
    raw_source: Optional[dict] = Column(JSON, nullable=True)

    listing = relationship("Listing", back_populates="own_snapshots")


class CompetitorPriceSnapshot(Base):
    """Stores competitor price history for monitored listings."""

    __tablename__ = "competitor_price_snapshots"

    id: int = Column(Integer, primary_key=True)
    timestamp: datetime = Column(DateTime, default=datetime.utcnow, nullable=False)
    listing_id: int = Column(Integer, ForeignKey("listings.id"), nullable=False)
    competitor_name: str = Column(String, nullable=False)
    precio: float = Column(Numeric(12, 2), nullable=False)
    stock: Optional[int] = Column(Integer, nullable=True)
    extra: Optional[dict] = Column(JSON, nullable=True)

    listing = relationship("Listing", back_populates="competitor_snapshots")


class Alert(Base):
    """Represents pricing or scraping alerts to act upon."""

    __tablename__ = "alerts"

    id: int = Column(Integer, primary_key=True)
    timestamp: datetime = Column(DateTime, default=datetime.utcnow, nullable=False)
    listing_id: int = Column(Integer, ForeignKey("listings.id"), nullable=False)
    tipo: str = Column(String, nullable=False)
    detalle: str = Column(String, nullable=False)
    resuelta: bool = Column(Boolean, default=False, nullable=False)

    listing = relationship("Listing", back_populates="alerts")
