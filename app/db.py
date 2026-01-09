"""Configuración de base de datos y funciones auxiliares."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Iterable, List, Optional, Sequence

from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session, sessionmaker

from .models import (
    Alert,
    AlertV2,
    Channel,
    CompetitorPriceSnapshot,
    CompetitorPriceSnapshotV2,
    Listing,
    OwnPriceSnapshotV2,
    OwnPriceSnapshot,
    WatchItem,
    Base,
)
from .settings import get_settings


settings = get_settings()
engine = create_engine(settings.database_dsn, echo=False, future=True)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False, future=True)


def init_db() -> None:
    """Crea las tablas de base de datos."""

    Base.metadata.create_all(bind=engine)


def get_session() -> Session:
    """Provee una sesión de SQLAlchemy."""

    return SessionLocal()


def _listing_recency_filter(mode: str, frequency_minutes: int, last_own: Optional[datetime], last_comp: Optional[datetime]) -> bool:
    """Determina si un listing debe ejecutarse según los últimos snapshots."""

    now = datetime.utcnow()
    threshold = now - timedelta(minutes=frequency_minutes)

    if mode == "own":
        return last_own is None or last_own <= threshold
    if mode == "competitor":
        return last_comp is None or last_comp <= threshold
    # modo == "both"
    return (last_own is None or last_own <= threshold) or (last_comp is None or last_comp <= threshold)


def get_listings_to_monitor(session: Session, channel_name: str, mode: str) -> List[Listing]:
    """Devuelve listings a monitorear para un canal respetando la frecuencia."""

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


def _watchitem_recency_filter(frequency_minutes: int, last_seen: Optional[datetime]) -> bool:
    """Determina si un watchitem debe ejecutarse según el último snapshot."""

    now = datetime.utcnow()
    threshold = now - timedelta(minutes=frequency_minutes)
    return last_seen is None or last_seen <= threshold


def _latest_watchitem_snapshot_ts(session: Session, watchitem: WatchItem) -> Optional[datetime]:
    """Obtiene el último timestamp asociado a un watchitem."""

    if watchitem.role == "own":
        stmt = select(func.max(OwnPriceSnapshotV2.timestamp)).where(
            OwnPriceSnapshotV2.group_id == watchitem.group_id,
            OwnPriceSnapshotV2.channel == watchitem.channel,
            OwnPriceSnapshotV2.url == watchitem.url,
        )
    else:
        stmt = select(func.max(CompetitorPriceSnapshotV2.timestamp)).where(
            CompetitorPriceSnapshotV2.group_id == watchitem.group_id,
            CompetitorPriceSnapshotV2.channel == watchitem.channel,
            CompetitorPriceSnapshotV2.url == watchitem.url,
        )
    return session.execute(stmt).scalar_one_or_none()


def get_watchitems_to_monitor(session: Session, channel_name: str, mode: str) -> List[WatchItem]:
    """Devuelve watchitems activos a monitorear respetando la frecuencia."""

    if mode not in {"own", "competitor", "both"}:
        raise ValueError("mode must be one of 'own', 'competitor', or 'both'")

    stmt = select(WatchItem).where(WatchItem.channel == channel_name, WatchItem.activo.is_(True))
    if mode == "own":
        stmt = stmt.where(WatchItem.role == "own")
    elif mode == "competitor":
        stmt = stmt.where(WatchItem.role == "competitor")

    watchitems = session.execute(stmt).scalars().all()
    filtered: List[WatchItem] = []
    for watchitem in watchitems:
        last_seen = _latest_watchitem_snapshot_ts(session, watchitem)
        if _watchitem_recency_filter(watchitem.frecuencia_minutos, last_seen):
            filtered.append(watchitem)
    return filtered


def filter_watchitems_by_frequency(
    session: Session,
    watchitems: Sequence[WatchItem],
    mode: str,
) -> List[WatchItem]:
    """Filtra watchitems en memoria aplicando la frecuencia definida por fila."""

    if mode not in {"own", "competitor", "both"}:
        raise ValueError("mode must be one of 'own', 'competitor', or 'both'")

    filtered: List[WatchItem] = []
    for watchitem in watchitems:
        if mode == "own" and watchitem.role != "own":
            continue
        if mode == "competitor" and watchitem.role != "competitor":
            continue
        last_seen = _latest_watchitem_snapshot_ts(session, watchitem)
        if _watchitem_recency_filter(watchitem.frecuencia_minutos, last_seen):
            filtered.append(watchitem)
    return filtered


def upsert_watchitems(session: Session, watchitems: Iterable[WatchItem]) -> List[WatchItem]:
    """Inserta o actualiza watchitems basados en group_id/canal/rol/url."""

    stored: List[WatchItem] = []
    for watchitem in watchitems:
        stmt = select(WatchItem).where(
            WatchItem.group_id == watchitem.group_id,
            WatchItem.channel == watchitem.channel,
            WatchItem.role == watchitem.role,
            WatchItem.url == watchitem.url,
        )
        existing = session.execute(stmt).scalar_one_or_none()
        if existing:
            existing.product_key = watchitem.product_key
            existing.competitor_name = watchitem.competitor_name
            existing.frecuencia_minutos = watchitem.frecuencia_minutos
            existing.umbral_gap = watchitem.umbral_gap
            existing.activo = watchitem.activo
            stored.append(existing)
        else:
            session.add(watchitem)
            stored.append(watchitem)
    session.flush()
    return stored


def insert_own_snapshot(
    session: Session,
    *,
    listing_id: int,
    precio: float,
    stock: Optional[int] = None,
    moneda: str = "CLP",
    raw_source: Optional[dict] = None,
) -> OwnPriceSnapshot:
    """Inserta un snapshot de precio propio."""

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
    """Inserta un snapshot de precio de competidor."""

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


def insert_own_snapshot_v2(
    session: Session,
    *,
    group_id: str,
    channel: str,
    url: str,
    precio: float,
    stock: Optional[int] = None,
    moneda: str = "CLP",
    raw_source: Optional[dict] = None,
) -> OwnPriceSnapshotV2:
    """Inserta un snapshot de precio propio (v2)."""

    snapshot = OwnPriceSnapshotV2(
        group_id=group_id,
        channel=channel,
        url=url,
        precio=precio,
        stock=stock,
        moneda=moneda,
        raw_source=raw_source,
    )
    session.add(snapshot)
    session.flush()
    return snapshot


def insert_competitor_snapshot_v2(
    session: Session,
    *,
    group_id: str,
    channel: str,
    url: str,
    competitor_name: str,
    precio: float,
    stock: Optional[int] = None,
    extra: Optional[dict] = None,
) -> CompetitorPriceSnapshotV2:
    """Inserta un snapshot de precio competidor (v2)."""

    snapshot = CompetitorPriceSnapshotV2(
        group_id=group_id,
        channel=channel,
        url=url,
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
    """Inserta un registro de alerta."""

    alert = Alert(
        listing_id=listing_id,
        tipo=tipo,
        detalle=detalle,
        resuelta=resuelta,
    )
    session.add(alert)
    session.flush()
    return alert


def insert_alert_v2(
    session: Session,
    *,
    group_id: str,
    channel: str,
    tipo: str,
    detalle: str,
    resuelta: bool = False,
    own_price: Optional[float] = None,
    min_competitor_price: Optional[float] = None,
    gap_pct: Optional[float] = None,
    url_own: Optional[str] = None,
    url_min_competitor: Optional[str] = None,
) -> AlertV2:
    """Inserta una alerta v2 por group_id."""

    alert = AlertV2(
        group_id=group_id,
        channel=channel,
        tipo=tipo,
        detalle=detalle,
        resuelta=resuelta,
        own_price=own_price,
        min_competitor_price=min_competitor_price,
        gap_pct=gap_pct,
        url_own=url_own,
        url_min_competitor=url_min_competitor,
    )
    session.add(alert)
    session.flush()
    return alert


def get_open_alerts_v2(session: Session, *, channel: Optional[str] = None) -> List[AlertV2]:
    """Devuelve alertas v2 abiertas, opcionalmente filtradas por canal."""

    stmt = select(AlertV2).where(AlertV2.resuelta.is_(False))
    if channel:
        stmt = stmt.where(AlertV2.channel == channel)
    stmt = stmt.order_by(AlertV2.timestamp.desc())
    return session.execute(stmt).scalars().all()


def get_alerts_v2_created_since(session: Session, since_dt: datetime) -> List[AlertV2]:
    """Devuelve alertas v2 creadas desde la fecha indicada."""

    stmt = select(AlertV2).where(AlertV2.timestamp >= since_dt).order_by(AlertV2.timestamp.desc())
    return session.execute(stmt).scalars().all()
