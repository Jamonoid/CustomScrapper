"""Definición abstracta de worker para recolectores por canal."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from app.models import WatchItem


class BaseWorker(ABC):
    """Worker base que provee estructura compartida para implementaciones por canal."""

    def __init__(self, channel_name: str, channel_config: Dict, db_session: Session) -> None:
        self.channel_name = channel_name
        self.channel_config = channel_config
        self.db_session = db_session

    @abstractmethod
    def fetch_own_prices(self, watchitems: List[WatchItem]) -> None:
        """Recolecta precios propios para los watchitems indicados."""

    @abstractmethod
    def fetch_competitor_prices(self, watchitems: List[WatchItem]) -> None:
        """Recolecta precios de competidores para los watchitems indicados."""

    def _get_user_agent(self) -> Optional[str]:
        """Obtiene el user agent configurado para el canal."""

        return self.channel_config.get("user_agent")

    def _get_throttling(self) -> Optional[Tuple[float, float]]:
        """Devuelve throttling (min_delay, max_delay) si está configurado."""

        scraping_cfg = self.channel_config.get("scraping", {})
        throttling = scraping_cfg.get("throttling", {})
        min_delay = throttling.get("min_delay")
        max_delay = throttling.get("max_delay")

        if min_delay is None and max_delay is None:
            return None
        if min_delay is None:
            min_delay = 0.0
        if max_delay is None:
            max_delay = min_delay
        return float(min_delay), float(max_delay)

    def _get_headless(self) -> bool:
        """Obtiene si el navegador debe ejecutarse en modo headless."""

        if "headless" in self.channel_config:
            return bool(self.channel_config.get("headless", True))
        scraping_cfg = self.channel_config.get("scraping", {})
        return bool(scraping_cfg.get("headless", True))

    def _get_timeout_ms(self) -> int:
        """Obtiene el timeout configurado para scraping en milisegundos."""

        if "timeout_ms" in self.channel_config:
            return int(self.channel_config.get("timeout_ms", 30000))
        scraping_cfg = self.channel_config.get("scraping", {})
        return int(scraping_cfg.get("timeout_ms", 30000))
