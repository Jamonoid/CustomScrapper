"""Lógica de generación de alertas basada en reglas de precios."""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from typing import List, Optional, Tuple

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from app.db import insert_alert
from app.models import Alert, CompetitorPriceSnapshot, Listing, OwnPriceSnapshot


def _latest_own_snapshot(session: Session, listing_id: int) -> Optional[OwnPriceSnapshot]:
    stmt = (
        select(OwnPriceSnapshot)
        .where(OwnPriceSnapshot.listing_id == listing_id)
        .order_by(desc(OwnPriceSnapshot.timestamp))
        .limit(1)
    )
    result = session.execute(stmt).scalar_one_or_none()
    return result


def _latest_competitor_prices(
    session: Session, listing_id: int
) -> Tuple[Optional[CompetitorPriceSnapshot], Optional[Decimal]]:
    latest_ts_stmt = (
        select(CompetitorPriceSnapshot.timestamp)
        .where(CompetitorPriceSnapshot.listing_id == listing_id)
        .order_by(desc(CompetitorPriceSnapshot.timestamp))
        .limit(1)
    )
    latest_ts = session.execute(latest_ts_stmt).scalar_one_or_none()
    if latest_ts is None:
        return None, None

    prices_stmt = (
        select(CompetitorPriceSnapshot)
        .where(
            CompetitorPriceSnapshot.listing_id == listing_id,
            CompetitorPriceSnapshot.timestamp == latest_ts,
        )
    )
    snapshots = session.execute(prices_stmt).scalars().all()
    if not snapshots:
        return None, None
    min_price = min(Decimal(str(s.precio)) for s in snapshots)
    return snapshots[0], min_price


def _latest_open_alert(
    session: Session,
    *,
    listing_id: int,
    tipo: str,
    recent_hours: int = 24,
) -> Optional[Alert]:
    """Busca la alerta abierta más reciente para evitar duplicados."""

    cutoff = datetime.utcnow() - timedelta(hours=recent_hours)
    stmt = (
        select(Alert)
        .where(
            Alert.listing_id == listing_id,
            Alert.tipo == tipo,
            Alert.resuelta.is_(False),
            Alert.timestamp >= cutoff,
        )
        .order_by(desc(Alert.timestamp))
        .limit(1)
    )
    return session.execute(stmt).scalar_one_or_none()


def process_new_snapshots(session: Session) -> List[str]:
    """
    Genera alertas comparando precios propios vs los últimos precios de competidores.

    Si el precio propio es >10% mayor que el precio mínimo de competidor para el mismo
    listing en la última ejecución, crea una alerta con tipo 'gap_mayor_10'.
    """

    alerts_created: List[str] = []
    listings = session.execute(select(Listing)).scalars().all()

    for listing in listings:
        own_snapshot = _latest_own_snapshot(session, listing.id)
        competitor_snapshot, min_comp_price = _latest_competitor_prices(session, listing.id)
        if not own_snapshot or min_comp_price is None:
            continue
        if not competitor_snapshot:
            continue

        own_price = Decimal(str(own_snapshot.precio))
        if min_comp_price == 0:
            continue

        gap = (own_price - min_comp_price) / min_comp_price
        if gap > Decimal("0.10"):
            if _latest_open_alert(session, listing_id=listing.id, tipo="gap_mayor_10"):
                continue
            detalle = (
                f"Listing {listing.id} own price {own_price} vs min competitor {min_comp_price} "
                f"gap {gap:.2%}"
            )
            insert_alert(session, listing_id=listing.id, tipo="gap_mayor_10", detalle=detalle)
            alerts_created.append(detalle)

    return alerts_created
