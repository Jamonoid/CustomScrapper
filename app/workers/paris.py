"""Worker para el marketplace Paris."""

from __future__ import annotations

import asyncio
from typing import List

from app.db import insert_competitor_snapshot, insert_own_snapshot
from app.models import Listing
from app.utils.http import fetch_page_content
from .base import BaseWorker


class ParisWorker(BaseWorker):
    """Recolecta precios propios por exportación y precios de competidores scrapeados."""

    def fetch_own_prices(self, listings: List[Listing]) -> None:
        """
        Marcador de posición para la ingesta de exportaciones de Paris.

        Pasos esperados:
        - Descargar/ingerir un CSV o feed entregado por Paris.
        - Mapear SKUs a listings.
        - Persistir snapshots de precio.
        """

        for listing in listings:
            insert_own_snapshot(
                self.db_session,
                listing_id=listing.id,
                precio=0,  # TODO: parsear desde el archivo de exportación.
                stock=None,
            )

    def fetch_competitor_prices(self, listings: List[Listing]) -> None:
        """Extrae precios de competidores para listings de Paris."""

        scraping_cfg = self.channel_config.get("scraping", {})
        selector_price = scraping_cfg.get("selector_price")

        async def _run_scrape() -> None:
            for listing in listings:
                if not listing.url_pdp:
                    continue
                content = await fetch_page_content(
                    listing.url_pdp,
                    user_agent=self.channel_config.get("user_agent"),
                    wait_selector=selector_price,
                )
                insert_competitor_snapshot(
                    self.db_session,
                    listing_id=listing.id,
                    competitor_name="paris",
                    precio=0,  # TODO: parsear desde el HTML.
                    stock=None,
                    extra={"raw_html_excerpt": content[:500]},
                )
                await asyncio.sleep(scraping_cfg.get("throttling", {}).get("min_delay", 1))

        asyncio.run(_run_scrape())
