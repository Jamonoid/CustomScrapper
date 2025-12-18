"""Database configuration and helper functions."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Iterable, List, Optional

from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session, sessionmaker

from .models import (
    Alert,
    Channel,
    CompetitorPriceSnapshot,
    Listing,
    OwnPriceSnapshot,
    Base,
)
from .settings import get_settings


settings = get_settings()
engine = create_engine(settings.database_dsn, echo=False, future=True)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False, future=True)


def init_db() -> None:
    """Create database tables."""

    Base.metadata.create_all(bind=engine)


def get_session() -> Session:
    """Provide a SQLAlchemy session."""

    return SessionLocal()


def _listing_recency_filter(mode: str, frequency_minutes: int, last_own: Optional[datetime], last_comp: Optional[datetime]) -> bool:
    """Determine whether a listing should run based on last snapshot timestamps."""

    now = datetime.utcnow()
    threshold = now - timedelta(minutes=frequency_minutes)

    if mode == "own":
        return last_own is None or last_own <= threshold
    if mode == "competitor":
        return last_comp is None or last_comp <= threshold
    # mode == "both"
    return (last_own is None or last_own <= threshold) or (last_comp is None or last_comp <= threshold)


def get_listings_to_monitor(session: Session, channel_name: str, mode: str) -> List[Listing]:
    """Return listings to monitor for a channel honoring frequency constraints."""

    if mode not in {"own", "competitor", "both"}:
        raise ValueError("mode must be one of 'own', 'competitor', or 'both'")

    subquery = (
        select(
            Listing.id.label("listing_id"),
            func.max(OwnPriceSnapshot.timestamp).label("last_own"),
            func.max(CompetitorPriceSnapshot.timestamp).label("last_comp"),
        )
        .join(OwnPriceSnapshot, OwnPriceSnapshot.listing_id == Listing.id, isouter=True)
        .join(CompetitorPriceSnapshot, CompetitorPriceSnapshot.listing_id == Listing.id, isouter=True)
        .group_by(Listing.id)
        .subquery()
    )

    listings_stmt = (
        select(Listing, subquery.c.last_own, subquery.c.last_comp)
        .join(Channel, Listing.channel_id == Channel.id)
        .join(subquery, subquery.c.listing_id == Listing.id)
        .where(Channel.nombre == channel_name)
    )

    if mode == "own":
        listings_stmt = listings_stmt.where(Listing.monitorear_propio.is_(True))
    elif mode == "competitor":
        listings_stmt = listings_stmt.where(Listing.monitorear_competencia.is_(True))
    else:
        listings_stmt = listings_stmt.where(
            (Listing.monitorear_propio.is_(True)) | (Listing.monitorear_competencia.is_(True))
        )

    result = session.execute(listings_stmt).all()

    filtered: List[Listing] = []
    for listing, last_own, last_comp in result:
        if _listing_recency_filter(mode, listing.frecuencia_minutos, last_own, last_comp):
            filtered.append(listing)

    return filtered


def insert_own_snapshot(
    session: Session,
    *,
    listing_id: int,
    precio: float,
    stock: Optional[int] = None,
    moneda: str = "CLP",
    raw_source: Optional[dict] = None,
) -> OwnPriceSnapshot:
    """Insert an own price snapshot."""

    snapshot = OwnPriceSnapshot(
        listing_id=listing_id,
        precio=precio,
        stock=stock,
        moneda=moneda,
        raw_source=raw_source,
    )
    session.add(snapshot)
    session.flush()
    return snapshot


def insert_competitor_snapshot(
    session: Session,
    *,
    listing_id: int,
    competitor_name: str,
    precio: float,
    stock: Optional[int] = None,
    extra: Optional[dict] = None,
) -> CompetitorPriceSnapshot:
    """Insert a competitor price snapshot."""

    snapshot = CompetitorPriceSnapshot(
        listing_id=listing_id,
        competitor_name=competitor_name,
        precio=precio,
        stock=stock,
        extra=extra,
    )
    session.add(snapshot)
    session.flush()
    return snapshot


def insert_alert(
    session: Session,
    *,
    listing_id: int,
    tipo: str,
    detalle: str,
    resuelta: bool = False,
) -> Alert:
    """Insert an alert record."""

    alert = Alert(
        listing_id=listing_id,
        tipo=tipo,
        detalle=detalle,
        resuelta=resuelta,
    )
    session.add(alert)
    session.flush()
    return alert
