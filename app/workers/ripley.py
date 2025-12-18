"""Worker for Ripley marketplace."""

from __future__ import annotations

import asyncio
from typing import List

from app.db import insert_competitor_snapshot, insert_own_snapshot
from app.models import Listing
from app.utils.http import fetch_page_content, request_with_retries
from .base import BaseWorker


class RipleyWorker(BaseWorker):
    """Collects own and competitor prices for Ripley listings."""

    def fetch_own_prices(self, listings: List[Listing]) -> None:
        """Placeholder for Ripley API calls to fetch own prices."""

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
        """Placeholder for Ripley scraping logic using Playwright."""

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
                    competitor_name="ripley",
                    precio=0,  # TODO: parse from HTML.
                    stock=None,
                    extra={"raw_html_excerpt": content[:500]},
                )
                await asyncio.sleep(scraping_cfg.get("throttling", {}).get("min_delay", 1))

        asyncio.run(_run_scrape())
