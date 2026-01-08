"""Utilidades HTTP para llamadas a APIs y scraping con Playwright."""

from __future__ import annotations

import asyncio
import logging
import random
import time
from typing import Any, Dict, Optional

import requests
from playwright.async_api import Browser, BrowserContext, Page, Playwright, async_playwright
from requests import Response


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
    """Realiza una petición HTTP con lógica simple de reintentos."""

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


class PlaywrightClient:
    """Cliente reutilizable de Playwright para scraping por lotes."""

    def __init__(
        self,
        *,
        user_agent: Optional[str] = None,
        headless: bool = True,
        min_delay: float = 0.0,
        max_delay: float = 0.0,
        viewport: Optional[Dict[str, int]] = None,
    ) -> None:
        self.user_agent = user_agent
        self.headless = headless
        self.min_delay = float(min_delay)
        self.max_delay = float(max_delay)
        self.viewport = viewport
        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None

    async def start(self) -> None:
        """Inicializa Playwright, el navegador y el contexto compartido."""

        if self._playwright:
            return

        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(headless=self.headless)

        context_kwargs: Dict[str, Any] = {}
        if self.user_agent:
            context_kwargs["user_agent"] = self.user_agent
        if self.viewport is not None:
            context_kwargs["viewport"] = self.viewport

        self._context = await self._browser.new_context(**context_kwargs)

    async def stop(self) -> None:
        """Cierra contexto, browser y Playwright de forma ordenada."""

        if self._context:
            await self._context.close()
            self._context = None
        if self._browser:
            await self._browser.close()
            self._browser = None
        if self._playwright:
            await self._playwright.stop()
            self._playwright = None

    async def new_page(self) -> Page:
        """Crea una nueva página dentro del contexto existente."""

        if not self._context:
            raise RuntimeError("PlaywrightClient.start() must be called before new_page().")
        return await self._context.new_page()

    async def get_content(
        self,
        url: str,
        wait_selector: Optional[str],
        timeout_ms: int,
    ) -> str:
        """Navega a una URL y devuelve el HTML de la página."""

        page: Optional[Page] = None
        try:
            page = await self.new_page()
            await page.goto(url, timeout=timeout_ms)
            if wait_selector:
                await page.wait_for_selector(wait_selector, timeout=timeout_ms)
            content = await page.content()
            return content
        except Exception:
            logger.exception("Playwright error while fetching %s", url)
            raise
        finally:
            if page:
                await page.close()
            await self._apply_throttling()

    async def _apply_throttling(self) -> None:
        """Aplica un delay aleatorio entre páginas cuando está configurado."""

        if self.max_delay <= 0 and self.min_delay <= 0:
            return

        max_delay = max(self.max_delay, self.min_delay)
        wait_time = random.uniform(self.min_delay, max_delay)
        if wait_time > 0:
            await asyncio.sleep(wait_time)


_shared_clients: Dict[Optional[str], PlaywrightClient] = {}


async def _get_shared_client(user_agent: Optional[str]) -> PlaywrightClient:
    client = _shared_clients.get(user_agent)
    if not client:
        client = PlaywrightClient(user_agent=user_agent)
        _shared_clients[user_agent] = client
    await client.start()
    return client


async def fetch_page_content(
    url: str,
    *,
    user_agent: Optional[str] = None,
    wait_selector: Optional[str] = None,
    timeout_ms: int = 30000,
) -> str:
    """Obtiene el contenido de una página con un navegador reutilizable."""

    client = await _get_shared_client(user_agent)
    return await client.get_content(url, wait_selector=wait_selector, timeout_ms=timeout_ms)
