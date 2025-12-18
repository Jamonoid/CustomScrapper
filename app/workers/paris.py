"""Worker for Paris marketplace."""

from __future__ import annotations

import asyncio
from typing import List

from app.db import insert_competitor_snapshot, insert_own_snapshot
from app.models import Listing
from app.utils.http import fetch_page_content
from .base import BaseWorker


class ParisWorker(BaseWorker):
    """Collects export-based own prices and scraped competitor prices."""

    def fetch_own_prices(self, listings: List[Listing]) -> None:
        """
        Placeholder for Paris export ingestion.

        Expected steps:
        - Download/ingest a CSV or feed delivered via Paris.
        - Map SKUs to listings.
        - Persist price snapshots.
        """

        for listing in listings:
            insert_own_snapshot(
                self.db_session,
                listing_id=listing.id,
                precio=0,  # TODO: parse from export file.
                stock=None,
            )

    def fetch_competitor_prices(self, listings: List[Listing]) -> None:
        """Scrape competitor prices for Paris listings."""

        scraping_cfg = self.channel_config.get("scraping", {})
        selector_price = scraping_cfg.get("selector_price")

        async def _run_scrape() -> None:
            for listing in listings:
                if not listing.url_pdp:
                    continue
                content = await fetch_page_content(
                    listing.url_pdp,
                    user_agent=self.channel_config.get("user_agent"),
                    wait_selector=selector_price,
                )
                insert_competitor_snapshot(
                    self.db_session,
                    listing_id=listing.id,
                    competitor_name="paris",
                    precio=0,  # TODO: parse from HTML.
                    stock=None,
                    extra={"raw_html_excerpt": content[:500]},
                )
                await asyncio.sleep(scraping_cfg.get("throttling", {}).get("min_delay", 1))

        asyncio.run(_run_scrape())
