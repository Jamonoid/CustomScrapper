"""Integración con Google Sheets para cargar watchlists y escribir alertas."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Iterable, List, Optional

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


def upsert_watchlist_to_db(session, watchitems: Iterable[WatchItem]) -> List[WatchItem]:
    """Persistencia incremental de watchitems provenientes del sheet."""

    return db.upsert_watchitems(session, watchitems)


def write_alerts_to_sheet(
    sheet_id: str,
    alert_rows: Iterable[AlertRow],
    alerts_tab: str = "ALERTAS",
) -> None:
    """Escribe alertas al tab de ALERTAS en Google Sheets."""

    rows = [alert.to_sheet_row() for alert in alert_rows]
    if not rows:
        return

    worksheet = _get_worksheet(sheet_id, alerts_tab)
    existing = worksheet.get_all_values()
    if not existing:
        worksheet.append_row(ALERT_HEADERS)
    worksheet.append_rows(rows, value_input_option="USER_ENTERED")
