"""Worker para el marketplace Falabella."""

from __future__ import annotations

import asyncio
from typing import List

from app.db import insert_competitor_snapshot, insert_own_snapshot
from app.models import Listing
from app.utils.http import fetch_page_content, request_with_retries
from .base import BaseWorker


class FalabellaWorker(BaseWorker):
    """Recolecta precios propios y de competidores para listings de Falabella."""

    def fetch_own_prices(self, listings: List[Listing]) -> None:
        """
        Obtiene precios propios usando la API de Falabella.

        Este método debe:
        - Construir headers de autenticación usando variables de entorno definidas en YAML env_keys.
        - Llamar al endpoint de listings o precios y parsear precio/stock.
        - Persistir el snapshot vía insert_own_snapshot.
        """

        api_cfg = self.channel_config.get("api", {})
        base_url = api_cfg.get("base_url", "")
        headers = {
            "User-Agent": api_cfg.get("user_agent", "price-monitor/1.0"),
            # TODO: Inyectar autorización usando credenciales del entorno.
        }

        for listing in listings:
            url = f"{base_url}/listings/{listing.listing_id}"
            response = request_with_retries("GET", url, headers=headers)
            data = response.json()
            price = data.get("price")
            stock = data.get("stock")
            insert_own_snapshot(
                self.db_session,
                listing_id=listing.id,
                precio=price,
                stock=stock,
                raw_source=data,
            )

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
        min_delay = scraping_cfg.get("throttling", {}).get("min_delay", 1)
        max_delay = scraping_cfg.get("throttling", {}).get("max_delay", 3)

        async def _run_scrape() -> None:
            for listing in listings:
                if not listing.url_pdp:
                    continue
                content = await fetch_page_content(
                    listing.url_pdp,
                    user_agent=self.channel_config.get("user_agent"),
                    wait_selector=selector_price,
                    timeout_ms=scraping_cfg.get("timeout_ms", 30000),
                )
                # TODO: Parsear `content` con BeautifulSoup o consultas de Playwright.
                price = 0  # marcador de posición
                insert_competitor_snapshot(
                    self.db_session,
                    listing_id=listing.id,
                    competitor_name="falabella",
                    precio=price,
                    stock=None,
                    extra={"raw_html_excerpt": content[:500]},
                )
                await asyncio.sleep(min_delay)

        asyncio.run(_run_scrape())
