"""Worker para el marketplace Walmart Chile."""

from __future__ import annotations

import asyncio
from typing import List

from app.db import insert_competitor_snapshot, insert_own_snapshot
from app.models import Listing
from app.utils.http import PlaywrightClient, request_with_retries
from .base import BaseWorker


class WalmartWorker(BaseWorker):
    """Gestiona la API de Walmart y el scraping de competidores."""

    def fetch_own_prices(self, listings: List[Listing]) -> None:
        """
        Marcador de posición para precios vía la API de Walmart.

        Implementa autenticación usando variables de entorno definidas en la
        configuración YAML, luego llama al endpoint de precios por listing y guarda resultados.
        """

        api_cfg = self.channel_config.get("api", {})
        base_url = api_cfg.get("base_url", "")
        headers = {"User-Agent": api_cfg.get("user_agent", "price-monitor/1.0")}

        for listing in listings:
            url = f"{base_url}/listings/{listing.listing_id}"
            response = request_with_retries("GET", url, headers=headers)
            data = response.json()
            insert_own_snapshot(
                self.db_session,
                listing_id=listing.id,
                precio=data.get("price"),
                stock=data.get("stock"),
                raw_source=data,
            )

    def fetch_competitor_prices(self, listings: List[Listing]) -> None:
        """Extrae precios de competidores desde PDPs de Walmart."""

        scraping_cfg = self.channel_config.get("scraping", {})
        selector_price = scraping_cfg.get("selector_price")
        throttling = self._get_throttling() or (0.0, 0.0)
        viewport = scraping_cfg.get("viewport")

        async def _run_scrape() -> None:
            client = PlaywrightClient(
                user_agent=self._get_user_agent(),
                headless=bool(self.channel_config.get("headless", True)),
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
                        timeout_ms=scraping_cfg.get("timeout_ms", 30000),
                    )
                    insert_competitor_snapshot(
                        self.db_session,
                        listing_id=listing.id,
                        competitor_name="walmart",
                        precio=0,  # TODO: parsear desde el HTML.
                        stock=None,
                        extra={"raw_html_excerpt": content[:500]},
                    )
            finally:
                await client.stop()

        asyncio.run(_run_scrape())
