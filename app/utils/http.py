"""HTTP utilities for API calls and Playwright scraping."""

from __future__ import annotations

import logging
import random
import time
from typing import Any, Dict, Optional

import requests
from requests import Response
from playwright.async_api import async_playwright, Browser, Page, Playwright


logger = logging.getLogger(__name__)


def request_with_retries(
    method: str,
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    json: Optional[Dict[str, Any]] = None,
    retries: int = 3,
    timeout: int = 30,
    backoff_factor: float = 1.5,
) -> Response:
    """Perform an HTTP request with simple retry logic."""

    for attempt in range(1, retries + 1):
        try:
            response = requests.request(
                method,
                url,
                headers=headers,
                params=params,
                json=json,
                timeout=timeout,
            )
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            if attempt == retries:
                logger.error("Request failed after %s attempts: %s", attempt, exc)
                raise
            sleep_time = backoff_factor**attempt + random.random()
            logger.warning(
                "Request error (%s/%s), retrying in %.2fs: %s",
                attempt,
                retries,
                sleep_time,
                exc,
            )
            time.sleep(sleep_time)

    raise RuntimeError("Unhandled retry state")


async def _create_browser(user_agent: Optional[str] = None) -> tuple[Playwright, Browser]:
    """Create a Playwright browser instance."""

    playwright = await async_playwright().start()
    browser = await playwright.chromium.launch(headless=True)
    return playwright, browser


async def fetch_page_content(
    url: str,
    *,
    user_agent: Optional[str] = None,
    wait_selector: Optional[str] = None,
    timeout_ms: int = 30000,
) -> str:
    """Fetch page content using Playwright with optional waiting."""

    playwright: Optional[Playwright] = None
    browser: Optional[Browser] = None
    page: Optional[Page] = None
    try:
        playwright, browser = await _create_browser(user_agent=user_agent)
        context = await browser.new_context(user_agent=user_agent)
        page = await context.new_page()
        await page.goto(url, timeout=timeout_ms)
        if wait_selector:
            await page.wait_for_selector(wait_selector, timeout=timeout_ms)
        content = await page.content()
        return content
    finally:
        if page:
            await page.close()
        if browser:
            await browser.close()
        if playwright:
            await playwright.stop()
