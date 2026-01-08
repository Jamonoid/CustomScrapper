"""Lógica de generación de alertas basada en reglas de precios."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from app.db import insert_alert, insert_alert_v2
from app.models import (
    Alert,
    AlertV2,
    CompetitorPriceSnapshot,
    CompetitorPriceSnapshotV2,
    Listing,
    OwnPriceSnapshot,
    OwnPriceSnapshotV2,
    WatchItem,
)


@dataclass
class AlertResult:
    timestamp: datetime
    group_id: str
    channel: str
    tipo: str
    own_price: Decimal
    min_competitor_price: Decimal
    gap_pct: Decimal
    detalle: str
    url_own: Optional[str]
    url_min_competitor: Optional[str]


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


def _latest_own_snapshot_v2(session: Session, group_id: str, channel: str) -> Optional[OwnPriceSnapshotV2]:
    stmt = (
        select(OwnPriceSnapshotV2)
        .where(OwnPriceSnapshotV2.group_id == group_id, OwnPriceSnapshotV2.channel == channel)
        .order_by(desc(OwnPriceSnapshotV2.timestamp))
        .limit(1)
    )
    return session.execute(stmt).scalar_one_or_none()


def _latest_competitor_prices_v2(
    session: Session, group_id: str, channel: str
) -> Tuple[Optional[CompetitorPriceSnapshotV2], Optional[Decimal]]:
    latest_ts_stmt = (
        select(CompetitorPriceSnapshotV2.timestamp)
        .where(
            CompetitorPriceSnapshotV2.group_id == group_id,
            CompetitorPriceSnapshotV2.channel == channel,
        )
        .order_by(desc(CompetitorPriceSnapshotV2.timestamp))
        .limit(1)
    )
    latest_ts = session.execute(latest_ts_stmt).scalar_one_or_none()
    if latest_ts is None:
        return None, None

    prices_stmt = (
        select(CompetitorPriceSnapshotV2)
        .where(
            CompetitorPriceSnapshotV2.group_id == group_id,
            CompetitorPriceSnapshotV2.channel == channel,
            CompetitorPriceSnapshotV2.timestamp == latest_ts,
        )
    )
    snapshots = session.execute(prices_stmt).scalars().all()
    if not snapshots:
        return None, None
    min_snapshot = min(snapshots, key=lambda snapshot: Decimal(str(snapshot.precio)))
    min_price = Decimal(str(min_snapshot.precio))
    return min_snapshot, min_price


def _latest_open_alert_v2(
    session: Session,
    *,
    group_id: str,
    channel: str,
    tipo: str,
    recent_hours: int = 24,
) -> Optional[AlertV2]:
    cutoff = datetime.utcnow() - timedelta(hours=recent_hours)
    stmt = (
        select(AlertV2)
        .where(
            AlertV2.group_id == group_id,
            AlertV2.channel == channel,
            AlertV2.tipo == tipo,
            AlertV2.resuelta.is_(False),
            AlertV2.timestamp >= cutoff,
        )
        .order_by(desc(AlertV2.timestamp))
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


def process_new_watchitem_alerts(session: Session) -> List[AlertResult]:
    """Genera alertas por group_id comparando own vs mínimo competitor."""

    alerts_created: List[AlertResult] = []

    threshold_rows = session.execute(
        select(WatchItem.group_id, WatchItem.channel, WatchItem.umbral_gap).where(
            WatchItem.role == "own",
            WatchItem.activo.is_(True),
        )
    ).all()
    threshold_map: Dict[Tuple[str, str], Decimal] = {
        (row[0], row[1]): Decimal(str(row[2])) for row in threshold_rows
    }

    groups = session.execute(
        select(WatchItem.group_id, WatchItem.channel)
        .where(WatchItem.activo.is_(True))
        .distinct()
    ).all()

    for group_id, channel in groups:
        own_snapshot = _latest_own_snapshot_v2(session, group_id, channel)
        competitor_snapshot, min_comp_price = _latest_competitor_prices_v2(session, group_id, channel)
        if not own_snapshot or min_comp_price is None or not competitor_snapshot:
            continue

        own_price = Decimal(str(own_snapshot.precio))
        if min_comp_price == 0:
            continue

        gap = (own_price - min_comp_price) / min_comp_price
        threshold = threshold_map.get((group_id, channel), Decimal("0.10"))

        if gap > threshold:
            if _latest_open_alert_v2(session, group_id=group_id, channel=channel, tipo="gap_mayor_10"):
                continue
            detalle = (
                f"Group {group_id} channel {channel} own price {own_price} vs min competitor {min_comp_price} "
                f"gap {gap:.2%}"
            )
            insert_alert_v2(
                session,
                group_id=group_id,
                channel=channel,
                tipo="gap_mayor_10",
                detalle=detalle,
                own_price=float(own_price),
                min_competitor_price=float(min_comp_price),
                gap_pct=float(gap),
                url_own=own_snapshot.url,
                url_min_competitor=competitor_snapshot.url,
            )
            alerts_created.append(
                AlertResult(
                    timestamp=datetime.utcnow(),
                    group_id=group_id,
                    channel=channel,
                    tipo="gap_mayor_10",
                    own_price=own_price,
                    min_competitor_price=min_comp_price,
                    gap_pct=gap,
                    detalle=detalle,
                    url_own=own_snapshot.url,
                    url_min_competitor=competitor_snapshot.url,
                )
            )

    return alerts_created
