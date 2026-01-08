"""Worker para el sitio propio de Prochef."""

from __future__ import annotations

from typing import List

from app.db import insert_own_snapshot
from app.models import Listing
from app.utils.http import request_with_retries
from .base import BaseWorker


class ProchefWorker(BaseWorker):
    """Gestiona el monitoreo del sitio propio de Prochef."""

    def fetch_own_prices(self, listings: List[Listing]) -> None:
        """
        Obtiene precios propios para el canal directo de Prochef.

        Se espera llamar a APIs internas (p. ej., autenticadas) y parsear precio/stock.
        Las credenciales deben proveerse vía variables de entorno referenciadas en YAML.
        """

        api_config = self.channel_config.get("api", {})
        base_url: str = api_config.get("base_url", "")
        headers = {"User-Agent": api_config.get("user_agent", "price-monitor/1.0")}

        for listing in listings:
            # TODO: Reemplazar con el endpoint real y la estrategia de autenticación.
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
        """Prochef no tiene modo de competidores para este worker."""

        # No se monitorean competidores en el canal propio; no hay nada que hacer.
        return
