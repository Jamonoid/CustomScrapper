"""Worker para el marketplace Paris."""

from __future__ import annotations

import asyncio
from typing import List

from app.db import insert_competitor_snapshot, insert_own_snapshot
from app.models import Listing
from app.utils.http import PlaywrightClient
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
        throttling = self._get_throttling() or (0.0, 0.0)
        viewport = scraping_cfg.get("viewport")

        async def _run_scrape() -> None:
            client = PlaywrightClient(
                user_agent=self._get_user_agent(),
                headless=self._get_headless(),
                min_delay=throttling[0],
                max_delay=throttling[1],
                viewport=viewport,
            )
            await client.start()
            try:
                for listing in listings:
                    if not listing.url_pdp:
                        continue
                    content = await client.get_content(
                        listing.url_pdp,
                        wait_selector=selector_price,
                        timeout_ms=self._get_timeout_ms(),
                    )
                    insert_competitor_snapshot(
                        self.db_session,
                        listing_id=listing.id,
                        competitor_name="paris",
                        precio=0,  # TODO: parsear desde el HTML.
                        stock=None,
                        extra={"raw_html_excerpt": content[:500]},
                    )
            finally:
                await client.stop()

        asyncio.run(_run_scrape())
