"""Punto de entrada CLI para el monitoreo de precios."""

from __future__ import annotations

import argparse
import logging
from collections import defaultdict
from typing import Dict, Iterable, List, Type

from sqlalchemy import select

from app import db
from app.integrations.google_sheets import AlertRow, load_watchlist_from_sheet, upsert_watchlist_to_db, write_alerts_to_sheet
from app.models import WatchItem
from app.rules.alerts import AlertResult, process_new_snapshots, process_new_watchitem_alerts
from app.settings import get_settings
from app.utils.logging import configure_logging
from app.workers.base import BaseWorker
from app.workers.falabella import FalabellaWorker
from app.workers.paris import ParisWorker
from app.workers.prochef import ProchefWorker
from app.workers.ripley import RipleyWorker
from app.workers.walmart import WalmartWorker


WORKER_MAP: Dict[str, Type[BaseWorker]] = {
    "prochef": ProchefWorker,
    "falabella": FalabellaWorker,
    "ripley": RipleyWorker,
    "paris": ParisWorker,
    "walmart": WalmartWorker,
}


def build_worker(channel: str, session, settings) -> BaseWorker:
    """Instancia un worker para el canal indicado."""

    worker_cls = WORKER_MAP.get(channel)
    if not worker_cls:
        raise ValueError(f"Unsupported channel: {channel}")

    channel_config = settings.default_channel_config.copy()
    channel_config.update(settings.channel_specific_config.get(channel, {}))
    return worker_cls(channel, channel_config, session)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Price monitor CLI")
    parser.add_argument("--channel", help="Nombre del canal (prochef, falabella, ripley, paris, walmart)")
    parser.add_argument("--mode", choices=["own", "competitor", "both"], default="both", help="Modo de monitoreo")
    parser.add_argument("--source", choices=["sheet", "db"], default="sheet", help="Fuente de watchlist")
    parser.add_argument("--sheet_id", help="ID del Google Sheet")
    parser.add_argument("--watchlist_tab", default="WATCHLIST", help="Nombre de la pestaña WATCHLIST")
    parser.add_argument("--alerts_tab", default="ALERTAS", help="Nombre de la pestaña ALERTAS")
    parser.add_argument("--upsert_watchlist", action="store_true", help="Persistir watchlist del sheet en la DB")
    parser.add_argument("--legacy_listings", action="store_true", help="Usar listings clásicos en lugar de watchitems")
    return parser.parse_args()


def _group_by_channel(watchitems: Iterable[WatchItem]) -> Dict[str, List[WatchItem]]:
    grouped: Dict[str, List[WatchItem]] = defaultdict(list)
    for watchitem in watchitems:
        grouped[watchitem.channel].append(watchitem)
    return grouped


def _convert_listings_to_watchitems(
    listings: Iterable,
    channel: str,
    mode: str,
) -> List[WatchItem]:
    watchitems: List[WatchItem] = []
    for listing in listings:
        product_key = listing.product.sku_interno if listing.product else str(listing.product_id)
        if mode in {"own", "both"} and listing.monitorear_propio:
            watchitems.append(
                WatchItem(
                    product_key=product_key,
                    channel=channel,
                    role="own",
                    url=listing.url_pdp or "",
                    competitor_name=None,
                    group_id=product_key,
                    frecuencia_minutos=listing.frecuencia_minutos,
                    umbral_gap=0.10,
                    activo=True,
                )
            )
        if mode in {"competitor", "both"} and listing.monitorear_competencia:
            watchitems.append(
                WatchItem(
                    product_key=product_key,
                    channel=channel,
                    role="competitor",
                    url=listing.url_pdp or "",
                    competitor_name=channel,
                    group_id=product_key,
                    frecuencia_minutos=listing.frecuencia_minutos,
                    umbral_gap=0.10,
                    activo=True,
                )
            )
    return watchitems


def _alert_results_to_sheet_rows(alerts: Iterable[AlertResult]) -> List[AlertRow]:
    return [
        AlertRow(
            timestamp=alert.timestamp,
            sku=alert.group_id,
            canal=alert.channel,
            tipo=alert.tipo,
            own_price=alert.own_price,
            min_competitor_price=alert.min_competitor_price,
            gap_pct=alert.gap_pct,
            detalle=alert.detalle,
            url_own=alert.url_own,
            url_min_competitor=alert.url_min_competitor,
            resuelta=False,
        )
        for alert in alerts
    ]


def main() -> None:
    args = parse_args()
    configure_logging()

    settings = get_settings()
    db.init_db()

    session = db.get_session()
    try:
        use_watchitems = not args.legacy_listings
        watchitems: List[WatchItem] = []

        if args.source == "sheet":
            if not args.sheet_id:
                raise ValueError("--sheet_id is required when source=sheet")
            watchitems = load_watchlist_from_sheet(args.sheet_id, watchlist_tab=args.watchlist_tab)
            watchitems = [watchitem for watchitem in watchitems if watchitem.activo]
            if args.upsert_watchlist:
                upsert_watchlist_to_db(session, watchitems)
                session.commit()
            watchitems = db.filter_watchitems_by_frequency(session, watchitems, args.mode)
        elif use_watchitems:
            if args.channel:
                watchitems = db.get_watchitems_to_monitor(session, args.channel, args.mode)
            else:
                watchitems = db.filter_watchitems_by_frequency(
                    session,
                    session.execute(select(WatchItem)).scalars().all(),
                    args.mode,
                )
        else:
            if not args.channel:
                raise ValueError("--channel is required when using legacy listings")
            listings = db.get_listings_to_monitor(session, args.channel, args.mode)
            watchitems = _convert_listings_to_watchitems(listings, args.channel, args.mode)

        if args.channel:
            watchitems = [item for item in watchitems if item.channel == args.channel]

        grouped_watchitems = _group_by_channel(watchitems)
        for channel, channel_watchitems in grouped_watchitems.items():
            worker = build_worker(channel, session, settings)
            if args.mode in {"own", "both"}:
                logging.info("Fetching own prices for %s watchitems", len(channel_watchitems))
                worker.fetch_own_prices(channel_watchitems)

            if args.mode in {"competitor", "both"}:
                logging.info("Fetching competitor prices for %s watchitems", len(channel_watchitems))
                worker.fetch_competitor_prices(channel_watchitems)

        session.commit()
        if use_watchitems or args.source == "sheet":
            alerts = process_new_watchitem_alerts(session)
        else:
            alerts = process_new_snapshots(session)
        session.commit()
        logging.info("Generated %s alerts", len(alerts))

        if args.sheet_id and alerts:
            alert_rows = _alert_results_to_sheet_rows(alerts) if use_watchitems or args.source == "sheet" else []
            if alert_rows:
                write_alerts_to_sheet(args.sheet_id, alert_rows, alerts_tab=args.alerts_tab)
    finally:
        session.close()


if __name__ == "__main__":
    main()
