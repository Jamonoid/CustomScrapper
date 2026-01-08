"""Worker para el marketplace Walmart Chile."""
from __future__ import annotations

import asyncio
from typing import List

from app.db import insert_competitor_snapshot_v2, insert_own_snapshot_v2
from app.models import WatchItem
from app.utils.http import PlaywrightClient, parse_price_clp
from .base import BaseWorker


class WalmartWorker(BaseWorker):
    """Gestiona el scraping de precios propios y competidores en Walmart."""

    def fetch_own_prices(self, watchitems: List[WatchItem]) -> None:
        """
        Marcador de posición para scraping de precios propios desde el PDP.

        Pasos esperados:
        - Abrir la URL del PDP.
        - Esperar el selector de precio configurado en YAML.
        - Parsear precio/stock desde el HTML (TODO).
        - Persistir snapshots de precio.
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
                for watchitem in watchitems:
                    if watchitem.role != "own":
                        continue
                    if not watchitem.url:
                        continue
                    content = await client.get_content(
                        watchitem.url,
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
                    insert_own_snapshot_v2(
                        self.db_session,
                        group_id=watchitem.group_id,
                        channel=watchitem.channel,
                        url=watchitem.url,
                        precio=price,
                        stock=stock,
                        raw_source={"raw_html_excerpt": content[:500]},
                    )
            finally:
                await client.stop()

        asyncio.run(_run_scrape())

    def fetch_competitor_prices(self, watchitems: List[WatchItem]) -> None:
        """Extrae precios de competidores desde PDPs de Walmart."""

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
                for watchitem in watchitems:
                    if watchitem.role != "competitor":
                        continue
                    if not watchitem.url:
                        continue
                    content = await client.get_content(
                        watchitem.url,
                        wait_selector=selector_price,
                        timeout_ms=self._get_timeout_ms(),
                    )
                    # TODO: parsear ofertas/sellers si existen.
                    price_text = ""
                    price = parse_price_clp(price_text) or 0.0
                    insert_competitor_snapshot_v2(
                        self.db_session,
                        group_id=watchitem.group_id,
                        channel=watchitem.channel,
                        url=watchitem.url,
                        competitor_name=watchitem.competitor_name or "walmart",
                        precio=price,
                        stock=None,
                        extra={"raw_html_excerpt": content[:500]},
                    )
            finally:
                await client.stop()

        asyncio.run(_run_scrape())
