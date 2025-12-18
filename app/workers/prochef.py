"""Worker for Prochef first-party site."""

from __future__ import annotations

from typing import List

from app.db import insert_own_snapshot
from app.models import Listing
from app.utils.http import request_with_retries
from .base import BaseWorker


class ProchefWorker(BaseWorker):
    """Handles Prochef own site monitoring."""

    def fetch_own_prices(self, listings: List[Listing]) -> None:
        """
        Fetch own prices for Prochef's direct channel.

        Expected to call internal APIs (e.g., authenticated) and parse price/stock.
        Credentials should be provided via environment variables referenced in YAML.
        """

        api_config = self.channel_config.get("api", {})
        base_url: str = api_config.get("base_url", "")
        headers = {"User-Agent": api_config.get("user_agent", "price-monitor/1.0")}

        for listing in listings:
            # TODO: Replace with real endpoint and authentication strategy.
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
        """Prochef has no competitor mode for this worker."""

        # No competitor monitoring on own channel; nothing to do.
        return
