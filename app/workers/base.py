"""Abstract worker definition for channel-specific collectors."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, List

from sqlalchemy.orm import Session

from app.models import Listing


class BaseWorker(ABC):
    """Base worker providing shared structure for channel implementations."""

    def __init__(self, channel_name: str, channel_config: Dict, db_session: Session) -> None:
        self.channel_name = channel_name
        self.channel_config = channel_config
        self.db_session = db_session

    @abstractmethod
    def fetch_own_prices(self, listings: List[Listing]) -> None:
        """Collect own prices for the given listings."""

    @abstractmethod
    def fetch_competitor_prices(self, listings: List[Listing]) -> None:
        """Collect competitor prices for the given listings."""
