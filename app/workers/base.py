"""Definición abstracta de worker para recolectores por canal."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, List

from sqlalchemy.orm import Session

from app.models import Listing


class BaseWorker(ABC):
    """Worker base que provee estructura compartida para implementaciones por canal."""

    def __init__(self, channel_name: str, channel_config: Dict, db_session: Session) -> None:
        self.channel_name = channel_name
        self.channel_config = channel_config
        self.db_session = db_session

    @abstractmethod
    def fetch_own_prices(self, listings: List[Listing]) -> None:
        """Recolecta precios propios para los listings indicados."""

    @abstractmethod
    def fetch_competitor_prices(self, listings: List[Listing]) -> None:
        """Recolecta precios de competidores para los listings indicados."""
