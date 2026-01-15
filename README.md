# CustomScrapper

CLI para monitoreo de precios que toma una watchlist desde Google Sheets o la base de datos, ejecuta workers por canal y genera alertas por diferencias de precios.

## Características

- Monitorea precios propios y de competidores por canal (`prochef`, `falabella`, `ripley`, `paris`, `walmart`).
- Integra Google Sheets para cargar watchlists y publicar alertas.
- Soporta configuración por canal mediante `config/channels.yaml`.

## Requisitos

- Python 3.10+ (recomendado).
- Dependencias instaladas desde `requirements.txt`.
- Base de datos accesible por `DATABASE_DSN`.
- Credenciales de Google en `GOOGLE_APPLICATION_CREDENTIALS` si se usa `--source sheet`.

## Instalación

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Configuración

### Variables de entorno

- `DATABASE_DSN`: DSN de SQLAlchemy (por defecto: `postgresql+psycopg2://user:pass@localhost:5432/pricemonitor`).
- `GOOGLE_APPLICATION_CREDENTIALS`: ruta al JSON de la cuenta de servicio para Google Sheets.
- Variables requeridas por los canales en `config/channels.yaml` (por ejemplo `PROCHEF_API_TOKEN`, `FALABELLA_CLIENT_ID`, `FALABELLA_CLIENT_SECRET`).

### Canales

Edita `config/channels.yaml` para ajustar timeouts, reintentos y parámetros específicos de cada canal.

## Uso

Ejemplo básico leyendo desde Google Sheets:

```bash
python main.py \
  --source sheet \
  --sheet_id "<ID_DEL_SHEET>" \
  --watchlist_tab WATCHLIST \
  --alerts_tab ALERTAS
```

Ejemplo leyendo desde la base de datos para un canal:

```bash
python main.py --source db --channel falabella --mode both
```

### Parámetros principales

- `--channel`: canal a ejecutar (`prochef`, `falabella`, `ripley`, `paris`, `walmart`).
- `--mode`: `own`, `competitor` o `both`.
- `--source`: `sheet` o `db`.
- `--sheet_id`: ID del Google Sheet.
- `--watchlist_tab`: nombre de la pestaña con la watchlist (por defecto `WATCHLIST`).
- `--alerts_tab`: nombre de la pestaña de alertas (por defecto `ALERTAS`).
- `--upsert_watchlist`: persiste la watchlist del sheet en la DB.
- `--legacy_listings`: usa la fuente legacy de listings.

## Formato de la watchlist (Google Sheets)

La pestaña `WATCHLIST` espera columnas mínimas: `sku`, `canal`, `rol`, `url`. Columnas opcionales: `competitor_name`, `frecuencia_minutos`, `umbral_gap`, `activo`.

Ejemplo de fila:

```
ABC123 | falabella | own | https://... | Tienda X | 60 | 0.10 | TRUE
```

Para más detalles de tabs y headers recomendados, revisar la integración en `app/integrations/google_sheets.py`.

## Alertas

Al finalizar la ejecución, se generan alertas y (si se usa `--sheet_id`) se escriben en la pestaña de alertas configurada.

## Desarrollo

- La configuración de canales vive en `config/channels.yaml`.
- Los workers por canal están en `app/workers/`.
