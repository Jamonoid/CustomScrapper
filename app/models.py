"""Modelos SQLAlchemy para el servicio de monitoreo de precios."""
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
    """Representa un producto interno."""

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
    """Representa un canal de ventas o competitivo."""

    __tablename__ = "channels"

    id: int = Column(Integer, primary_key=True)
    nombre: str = Column(String, unique=True, nullable=False)
    tipo: str = Column(String, nullable=False)

    listings = relationship("Listing", back_populates="channel")


class Listing(Base):
    """Asocia un producto interno con un listing específico por canal."""

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


class WatchItem(Base):
    """Representa una URL monitoreada proveniente de una watchlist externa."""

    __tablename__ = "watch_items"
    __table_args__ = (
        UniqueConstraint("group_id", "channel", "role", "url", name="uq_watchitem_identity"),
    )

    id: int = Column(Integer, primary_key=True)
    product_key: str = Column(String, nullable=False)
    channel: str = Column(String, nullable=False)
    role: str = Column(String, nullable=False)
    url: str = Column(String, nullable=False)
    competitor_name: Optional[str] = Column(String, nullable=True)
    group_id: str = Column(String, nullable=False)
    frecuencia_minutos: int = Column(Integer, nullable=False, default=60)
    umbral_gap: float = Column(Numeric(6, 4), nullable=False, default=0.10)
    activo: bool = Column(Boolean, default=True, nullable=False)


class OwnPriceSnapshot(Base):
    """Guarda el historial de precios para listings propios."""

    __tablename__ = "own_price_snapshots"

    id: int = Column(Integer, primary_key=True)
    timestamp: datetime = Column(DateTime, default=datetime.utcnow, nullable=False)
    listing_id: int = Column(Integer, ForeignKey("listings.id"), nullable=False)
    precio: float = Column(Numeric(12, 2), nullable=False)
    stock: Optional[int] = Column(Integer, nullable=True)
    moneda: str = Column(String, default="CLP", nullable=False)
    raw_source: Optional[dict] = Column(JSON, nullable=True)

    listing = relationship("Listing", back_populates="own_snapshots")


class OwnPriceSnapshotV2(Base):
    """Guarda precios propios por group_id desde la watchlist."""

    __tablename__ = "own_price_snapshots_v2"

    id: int = Column(Integer, primary_key=True)
    timestamp: datetime = Column(DateTime, default=datetime.utcnow, nullable=False)
    group_id: str = Column(String, nullable=False)
    channel: str = Column(String, nullable=False)
    url: str = Column(String, nullable=False)
    precio: float = Column(Numeric(12, 2), nullable=False)
    stock: Optional[int] = Column(Integer, nullable=True)
    moneda: str = Column(String, default="CLP", nullable=False)
    raw_source: Optional[dict] = Column(JSON, nullable=True)


class CompetitorPriceSnapshot(Base):
    """Guarda el historial de precios de competidores para listings monitoreados."""

    __tablename__ = "competitor_price_snapshots"

    id: int = Column(Integer, primary_key=True)
    timestamp: datetime = Column(DateTime, default=datetime.utcnow, nullable=False)
    listing_id: int = Column(Integer, ForeignKey("listings.id"), nullable=False)
    competitor_name: str = Column(String, nullable=False)
    precio: float = Column(Numeric(12, 2), nullable=False)
    stock: Optional[int] = Column(Integer, nullable=True)
    extra: Optional[dict] = Column(JSON, nullable=True)

    listing = relationship("Listing", back_populates="competitor_snapshots")


class CompetitorPriceSnapshotV2(Base):
    """Guarda precios de competidores por group_id desde la watchlist."""

    __tablename__ = "competitor_price_snapshots_v2"

    id: int = Column(Integer, primary_key=True)
    timestamp: datetime = Column(DateTime, default=datetime.utcnow, nullable=False)
    group_id: str = Column(String, nullable=False)
    channel: str = Column(String, nullable=False)
    url: str = Column(String, nullable=False)
    competitor_name: str = Column(String, nullable=False)
    precio: float = Column(Numeric(12, 2), nullable=False)
    stock: Optional[int] = Column(Integer, nullable=True)
    extra: Optional[dict] = Column(JSON, nullable=True)


class Alert(Base):
    """Representa alertas de precio o scraping a gestionar."""

    __tablename__ = "alerts"

    id: int = Column(Integer, primary_key=True)
    timestamp: datetime = Column(DateTime, default=datetime.utcnow, nullable=False)
    listing_id: int = Column(Integer, ForeignKey("listings.id"), nullable=False)
    tipo: str = Column(String, nullable=False)
    detalle: str = Column(String, nullable=False)
    resuelta: bool = Column(Boolean, default=False, nullable=False)

    listing = relationship("Listing", back_populates="alerts")


class AlertV2(Base):
    """Alertas basadas en group_id (watchlist)."""

    __tablename__ = "alerts_v2"

    id: int = Column(Integer, primary_key=True)
    timestamp: datetime = Column(DateTime, default=datetime.utcnow, nullable=False)
    group_id: str = Column(String, nullable=False)
    channel: str = Column(String, nullable=False)
    tipo: str = Column(String, nullable=False)
    detalle: str = Column(String, nullable=False)
    resuelta: bool = Column(Boolean, default=False, nullable=False)
    own_price: Optional[float] = Column(Numeric(12, 2), nullable=True)
    min_competitor_price: Optional[float] = Column(Numeric(12, 2), nullable=True)
    gap_pct: Optional[float] = Column(Numeric(8, 4), nullable=True)
    url_own: Optional[str] = Column(String, nullable=True)
    url_min_competitor: Optional[str] = Column(String, nullable=True)
