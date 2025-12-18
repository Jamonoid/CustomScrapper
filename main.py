"""CLI entrypoint for price monitoring."""

from __future__ import annotations

import argparse
import logging
from typing import Dict, Type

from app import db
from app.rules.alerts import process_new_snapshots
from app.settings import get_settings
from app.utils.logging import configure_logging
from app.workers import (
    BaseWorker,
    FalabellaWorker,
    ParisWorker,
    ProchefWorker,
    RipleyWorker,
    WalmartWorker,
)


WORKER_MAP: Dict[str, Type[BaseWorker]] = {
    "prochef": ProchefWorker,
    "falabella": FalabellaWorker,
    "ripley": RipleyWorker,
    "paris": ParisWorker,
    "walmart": WalmartWorker,
}


def build_worker(channel: str, session, settings) -> BaseWorker:
    """Instantiate a worker for the given channel."""

    worker_cls = WORKER_MAP.get(channel)
    if not worker_cls:
        raise ValueError(f"Unsupported channel: {channel}")

    channel_config = settings.default_channel_config.copy()
    channel_config.update(settings.channel_specific_config.get(channel, {}))
    return worker_cls(channel, channel_config, session)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Price monitor CLI")
    parser.add_argument("--channel", required=True, help="Nombre del canal (prochef, falabella, ripley, paris, walmart)")
    parser.add_argument("--mode", choices=["own", "competitor", "both"], default="both", help="Modo de monitoreo")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    configure_logging()

    settings = get_settings()
    db.init_db()

    session = db.get_session()
    try:
        worker = build_worker(args.channel, session, settings)
        listings = db.get_listings_to_monitor(session, args.channel, args.mode)

        if args.mode in {"own", "both"}:
            logging.info("Fetching own prices for %s listings", len(listings))
            worker.fetch_own_prices(listings)

        if args.mode in {"competitor", "both"}:
            logging.info("Fetching competitor prices for %s listings", len(listings))
            worker.fetch_competitor_prices(listings)

        session.commit()
        alerts = process_new_snapshots(session)
        logging.info("Generated %s alerts", len(alerts))
    finally:
        session.close()


if __name__ == "__main__":
    main()
