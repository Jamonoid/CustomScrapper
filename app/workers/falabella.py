"""Worker para el marketplace Falabella."""

from __future__ import annotations

import asyncio
from typing import List

from app.db import insert_competitor_snapshot, insert_own_snapshot
from app.models import Listing
from app.utils.http import PlaywrightClient, parse_price_clp
from .base import BaseWorker


class FalabellaWorker(BaseWorker):
    """Recolecta precios propios y de competidores para listings de Falabella."""

    def fetch_own_prices(self, listings: List[Listing]) -> None:
        """
        Obtiene precios propios usando scraping del PDP.

        Pasos esperados:
        - Abrir la URL del PDP.
        - Esperar el selector de precio configurado en YAML.
        - Parsear precio/stock desde el HTML (TODO).
        - Persistir el snapshot vía insert_own_snapshot.
        """

        scraping_cfg = self.channel_config.get("scraping", {})
        selector_price = scraping_cfg.get("selector_price")
        selector_stock = scraping_cfg.get("selector_stock")
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
                    # TODO: extraer texto de precio con selector_price desde el HTML.
                    price_text = ""
                    price = parse_price_clp(price_text) or 0.0
                    stock = None
                    if selector_stock:
                        # TODO: extraer stock usando selector_stock desde el HTML.
                        stock = None
                    insert_own_snapshot(
                        self.db_session,
                        listing_id=listing.id,
                        precio=price,
                        stock=stock,
                        raw_source={"raw_html_excerpt": content[:500]},
                    )
            finally:
                await client.stop()

        asyncio.run(_run_scrape())

    def fetch_competitor_prices(self, listings: List[Listing]) -> None:
        """
        Extrae ofertas de competidores desde PDPs de Falabella usando Playwright.

        Pasos a implementar:
        - Abrir la URL del PDP.
        - Esperar los selectores de precio y bloque de ofertas (desde la config YAML de scraping).
        - Extraer precio principal y ofertas de competidores si aplica.
        - Persistir con insert_competitor_snapshot por cada competidor encontrado.
        """

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
                    # TODO: parsear `content` y extraer ofertas/sellers si existen.
                    price_text = ""
                    price = parse_price_clp(price_text) or 0.0
                    insert_competitor_snapshot(
                        self.db_session,
                        listing_id=listing.id,
                        competitor_name="falabella",
                        precio=price,
                        stock=None,
                        extra={"raw_html_excerpt": content[:500]},
                    )
            finally:
                await client.stop()

        asyncio.run(_run_scrape())
