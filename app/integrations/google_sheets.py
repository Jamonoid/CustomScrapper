"""Integración con Google Sheets para cargar watchlists y escribir alertas.

Tabs recomendados en el Sheet (fila 1 como headers):

- SKUS_HEADERS (tab SKUS):
  sku | nombre | activo | categoria | marca
  Ejemplo: ABC123 | Zapatilla Pro | TRUE | Calzado | MarcaX

- NUESTROS_HEADERS (tab NUESTROS_LISTINGS):
  sku | canal | url | seller_name | frecuencia_min | umbral_gap | activo
  Ejemplo: ABC123 | marketplace | https://... | Tienda Oficial | 60 | 0.10 | TRUE

- COMPETIDORES_HEADERS (tab COMPETIDORES):
  competitor_id | nombre_visible | activo
  Ejemplo: falabella | Falabella.com | TRUE

- COMP_URLS_HEADERS (tab COMPETENCIA_URLS):
  sku | canal | competitor_id | url | frecuencia_min | activo
  Ejemplo: ABC123 | marketplace | falabella | https://... | 120 | TRUE

- REGLAS_HEADERS (tab REGLAS_CANAL, opcional):
  canal | frecuencia_default | umbral_gap_default
  Ejemplo: marketplace | 60 | 0.10

- ALERT_HEADERS (tab ALERTAS_ABIERTAS / ALERTAS_HISTORIAL):
  timestamp | sku | canal | tipo | own_price | min_competitor_price | gap_pct | detalle |
  url_own | url_min_competitor | resuelta
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Dict, Iterable, List, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials

from app import db
from app.models import WatchItem

WATCHLIST_HEADERS = [
    "sku",
    "canal",
    "rol",
    "url",
    "competitor_name",
    "frecuencia_minutos",
    "umbral_gap",
    "activo",
]

SKUS_HEADERS = [
    "sku",
    "nombre",
    "activo",
    "categoria",
    "marca",
]

NUESTROS_HEADERS = [
    "sku",
    "canal",
    "url",
    "seller_name",
    "frecuencia_min",
    "umbral_gap",
    "activo",
]

COMPETIDORES_HEADERS = [
    "competitor_id",
    "nombre_visible",
    "activo",
]

COMP_URLS_HEADERS = [
    "sku",
    "canal",
    "competitor_id",
    "url",
    "frecuencia_min",
    "activo",
]

REGLAS_HEADERS = [
    "canal",
    "frecuencia_default",
    "umbral_gap_default",
]

ALERT_HEADERS = [
    "timestamp",
    "sku",
    "canal",
    "tipo",
    "own_price",
    "min_competitor_price",
    "gap_pct",
    "detalle",
    "url_own",
    "url_min_competitor",
    "resuelta",
]


@dataclass
class AlertRow:
    timestamp: datetime
    sku: str
    canal: str
    tipo: str
    own_price: Optional[Decimal]
    min_competitor_price: Optional[Decimal]
    gap_pct: Optional[Decimal]
    detalle: str
    url_own: Optional[str]
    url_min_competitor: Optional[str]
    resuelta: bool = False

    def to_sheet_row(self) -> List[str]:
        return [
            self.timestamp.isoformat(),
            self.sku,
            self.canal,
            self.tipo,
            f"{self.own_price:.2f}" if self.own_price is not None else "",
            f"{self.min_competitor_price:.2f}" if self.min_competitor_price is not None else "",
            f"{self.gap_pct:.4f}" if self.gap_pct is not None else "",
            self.detalle,
            self.url_own or "",
            self.url_min_competitor or "",
            "TRUE" if self.resuelta else "FALSE",
        ]


@dataclass(frozen=True)
class TabsConfig:
    skus_tab: str = "SKUS"
    nuestros_tab: str = "NUESTROS_LISTINGS"
    competidores_tab: str = "COMPETIDORES"
    competencia_tab: str = "COMPETENCIA_URLS"
    reglas_tab: Optional[str] = "REGLAS_CANAL"


def _parse_bool(value: Optional[str], default: bool = True) -> bool:
    if value is None:
        return default
    normalized = str(value).strip().lower()
    if normalized in {"true", "1", "yes", "y", "si", "sí"}:
        return True
    if normalized in {"false", "0", "no", "n"}:
        return False
    return default


def _parse_int(value: Optional[str], default: int) -> int:
    if value is None or value == "":
        return default
    try:
        return int(float(value))
    except ValueError:
        return default


def _parse_decimal(value: Optional[str], default: Decimal) -> Decimal:
    if value is None or value == "":
        return default
    try:
        return Decimal(str(value))
    except (ValueError, ArithmeticError):
        return default


def _normalize_channel(value: Optional[str]) -> str:
    return str(value or "").strip().lower()


def _get_client() -> gspread.Client:
    credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not credentials_path:
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS is not set")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_file(credentials_path, scopes=scopes)
    return gspread.authorize(credentials)


def _get_worksheet(sheet_id: str, tab_name: str) -> gspread.Worksheet:
    client = _get_client()
    spreadsheet = client.open_by_key(sheet_id)
    return spreadsheet.worksheet(tab_name)


def _get_optional_worksheet(sheet_id: str, tab_name: Optional[str]) -> Optional[gspread.Worksheet]:
    if not tab_name:
        return None
    try:
        return _get_worksheet(sheet_id, tab_name)
    except gspread.WorksheetNotFound:
        return None


def load_watchlist_from_sheet(sheet_id: str, watchlist_tab: str = "WATCHLIST") -> List[WatchItem]:
    """Carga la watchlist desde Google Sheets y la normaliza."""

    worksheet = _get_worksheet(sheet_id, watchlist_tab)
    rows = worksheet.get_all_records()
    watchitems: List[WatchItem] = []

    for row in rows:
        sku = str(row.get("sku", "")).strip()
        canal = str(row.get("canal", "")).strip().lower()
        rol = str(row.get("rol", "")).strip().lower()
        url = str(row.get("url", "")).strip()
        if not sku or not canal or not rol or not url:
            continue

        competitor_name = row.get("competitor_name")
        competitor_value = str(competitor_name).strip() if competitor_name else None
        frecuencia = _parse_int(row.get("frecuencia_minutos"), default=60)
        umbral_gap = _parse_decimal(row.get("umbral_gap"), default=Decimal("0.10"))
        activo = _parse_bool(row.get("activo"), default=True)

        watchitems.append(
            WatchItem(
                product_key=sku,
                channel=canal,
                role=rol,
                url=url,
                competitor_name=competitor_value,
                group_id=sku,
                frecuencia_minutos=frecuencia,
                umbral_gap=float(umbral_gap),
                activo=activo,
            )
        )

    return watchitems


def _load_skus(worksheet: Optional[gspread.Worksheet]) -> Dict[str, bool]:
    if worksheet is None:
        return {}
    rows = worksheet.get_all_records()
    sku_status: Dict[str, bool] = {}
    for row in rows:
        sku = str(row.get("sku", "")).strip()
        if not sku:
            continue
        sku_status[sku] = _parse_bool(row.get("activo"), default=True)
    return sku_status


def _load_competitors(worksheet: Optional[gspread.Worksheet]) -> Dict[str, str]:
    if worksheet is None:
        return {}
    rows = worksheet.get_all_records()
    mapping: Dict[str, str] = {}
    for row in rows:
        competitor_id = str(row.get("competitor_id", "")).strip()
        if not competitor_id:
            continue
        if not _parse_bool(row.get("activo"), default=True):
            continue
        nombre = str(row.get("nombre_visible") or "").strip() or competitor_id
        mapping[competitor_id] = nombre
    return mapping


def _load_channel_rules(worksheet: Optional[gspread.Worksheet]) -> Dict[str, Tuple[int, Decimal]]:
    if worksheet is None:
        return {}
    rows = worksheet.get_all_records()
    rules: Dict[str, Tuple[int, Decimal]] = {}
    for row in rows:
        canal = _normalize_channel(row.get("canal"))
        if not canal:
            continue
        frecuencia = _parse_int(row.get("frecuencia_default"), default=60)
        umbral_gap = _parse_decimal(row.get("umbral_gap_default"), default=Decimal("0.10"))
        rules[canal] = (frecuencia, umbral_gap)
    return rules


def load_watchitems_from_tabs(sheet_id: str, tabs_config: TabsConfig) -> List[WatchItem]:
    """Carga watchitems desde tabs dedicados (SKUS, NUESTROS_LISTINGS, COMPETIDORES, COMPETENCIA_URLS)."""

    skus_ws = _get_optional_worksheet(sheet_id, tabs_config.skus_tab)
    nuestros_ws = _get_optional_worksheet(sheet_id, tabs_config.nuestros_tab)
    competidores_ws = _get_optional_worksheet(sheet_id, tabs_config.competidores_tab)
    competencia_ws = _get_optional_worksheet(sheet_id, tabs_config.competencia_tab)
    reglas_ws = _get_optional_worksheet(sheet_id, tabs_config.reglas_tab)

    sku_status = _load_skus(skus_ws)
    competitor_map = _load_competitors(competidores_ws)
    channel_rules = _load_channel_rules(reglas_ws)

    watchitems: List[WatchItem] = []
    own_frequency_map: Dict[Tuple[str, str], int] = {}

    if nuestros_ws is not None:
        rows = nuestros_ws.get_all_records()
        for row in rows:
            sku = str(row.get("sku", "")).strip()
            canal = _normalize_channel(row.get("canal"))
            url = str(row.get("url", "")).strip()
            if not sku or not canal or not url:
                continue
            if sku_status and not sku_status.get(sku, False):
                continue
            if not _parse_bool(row.get("activo"), default=True):
                continue

            reglas_frecuencia, reglas_umbral = channel_rules.get(canal, (60, Decimal("0.10")))
            frecuencia = _parse_int(row.get("frecuencia_min"), default=reglas_frecuencia)
            umbral_gap = _parse_decimal(row.get("umbral_gap"), default=reglas_umbral)
            seller_name = str(row.get("seller_name") or "").strip() or None

            own_frequency_map[(sku, canal)] = frecuencia
            watchitems.append(
                WatchItem(
                    product_key=sku,
                    channel=canal,
                    role="own",
                    url=url,
                    competitor_name=seller_name,
                    group_id=sku,
                    frecuencia_minutos=frecuencia,
                    umbral_gap=float(umbral_gap),
                    activo=True,
                )
            )

    if competencia_ws is not None:
        rows = competencia_ws.get_all_records()
        for row in rows:
            sku = str(row.get("sku", "")).strip()
            canal = _normalize_channel(row.get("canal"))
            competitor_id = str(row.get("competitor_id", "")).strip()
            url = str(row.get("url", "")).strip()
            if not sku or not canal or not competitor_id or not url:
                continue
            if sku_status and not sku_status.get(sku, False):
                continue
            if not _parse_bool(row.get("activo"), default=True):
                continue

            nombre_visible = competitor_map.get(competitor_id, competitor_id)
            reglas_frecuencia, _ = channel_rules.get(canal, (60, Decimal("0.10")))
            default_frecuencia = own_frequency_map.get((sku, canal), reglas_frecuencia)
            frecuencia = _parse_int(row.get("frecuencia_min"), default=default_frecuencia)

            watchitems.append(
                WatchItem(
                    product_key=sku,
                    channel=canal,
                    role="competitor",
                    url=url,
                    competitor_name=nombre_visible,
                    group_id=sku,
                    frecuencia_minutos=frecuencia,
                    umbral_gap=float(Decimal("0.10")),
                    activo=True,
                )
            )

    return watchitems


def upsert_watchlist_to_db(session, watchitems: Iterable[WatchItem]) -> List[WatchItem]:
    """Persistencia incremental de watchitems provenientes del sheet."""

    return db.upsert_watchitems(session, watchitems)


def upsert_watchitems_to_db(session, watchitems: Iterable[WatchItem]) -> List[WatchItem]:
    """Persistencia incremental de watchitems provenientes de tabs multi-lista."""

    return db.upsert_watchitems(session, watchitems)


def write_open_alerts_to_sheet(
    sheet_id: str,
    alert_rows: Iterable[AlertRow],
    tab: str = "ALERTAS_ABIERTAS",
) -> None:
    """Escribe alertas abiertas al tab especificado de forma idempotente."""

    worksheet = _get_worksheet(sheet_id, tab)
    worksheet.clear()
    worksheet.append_row(ALERT_HEADERS)
    rows = [alert.to_sheet_row() for alert in alert_rows]
    if rows:
        worksheet.append_rows(rows, value_input_option="USER_ENTERED")


def write_alerts_history_to_sheet(
    sheet_id: str,
    alert_rows: Iterable[AlertRow],
    tab: str = "ALERTAS_HISTORIAL",
) -> None:
    """Escribe alertas al tab de historial (append)."""

    rows = [alert.to_sheet_row() for alert in alert_rows]
    if not rows:
        return

    worksheet = _get_worksheet(sheet_id, tab)
    existing = worksheet.get_all_values()
    if not existing:
        worksheet.append_row(ALERT_HEADERS)
    worksheet.append_rows(rows, value_input_option="USER_ENTERED")
