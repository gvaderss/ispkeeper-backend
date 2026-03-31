"""
ISPKeeper Dashboard — Backend
==============================
• Sincroniza tickets de ISPKeeper cada N minutos en background
• Usa ticket_grupo como campo de localidad (más preciso que ticket_sucursal)
• Persiste en Google Sheets a través de un Apps Script (sin Google Cloud ni tarjeta)
• Carga tickets en memoria al arrancar → el endpoint /tickets responde en < 10 ms
• El Sheet puede ser público y visible por todo el equipo

Variables de entorno requeridas:
  ISPKEEPER_API_KEY       API key de ISPKeeper

Variables para Google Sheets (opcionales — sin ellas solo usa memoria):
  APPS_SCRIPT_URL         URL del Web App de Google Apps Script
  APPS_SCRIPT_TOKEN       Token secreto definido en el Apps Script
  GOOGLE_SHEETS_ID        ID del Google Spreadsheet (para leer al arrancar)

Variables opcionales:
  ISPKEEPER_BASE_URL      Default: https://api.anatod.ar/api
  SYNC_INTERVAL_MIN       Default: 15
  DIAS_VENTANA            Default: 120
  BATCH_SIZE              Páginas en paralelo por batch. Default: 20
"""

from __future__ import annotations
import asyncio, csv, io, logging, os
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone

# Zona horaria Argentina (UTC-3, sin horario de verano)
ARG_TZ = timezone(timedelta(hours=-3))
def now_arg() -> datetime:
    return datetime.now(ARG_TZ)
from typing import Any

import aiohttp
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# ─────────────────────────── CONFIG ────────────────────────────────────────
API_KEY           = os.getenv("ISPKEEPER_API_KEY",  "mojEu45nVV39nGvDLhChW9MTe2rLmIUi4JZJabUD")
ISPKEEPER_BASE    = os.getenv("ISPKEEPER_BASE_URL", "https://api.anatod.ar/api")
SYNC_INTERVAL_MIN = int(os.getenv("SYNC_INTERVAL_MIN", "15"))
DIAS_VENTANA      = int(os.getenv("DIAS_VENTANA",       "120"))
BATCH_SIZE        = int(os.getenv("BATCH_SIZE",         "20"))

APPS_SCRIPT_URL   = os.getenv("APPS_SCRIPT_URL",   "")
APPS_SCRIPT_TOKEN = os.getenv("APPS_SCRIPT_TOKEN", "changeme")
GOOGLE_SHEETS_ID  = os.getenv("GOOGLE_SHEETS_ID",  "")

# ─────────────────────────── CATEGORÍAS ────────────────────────────────────
SUBCAT_IDS: dict[int, set[int]] = {
    1:   {16, 116, 280, 342, 343, 344},          # Instalación
    2:   {267, 268, 108, 149, 281, 269},          # Reparación
    120: {20, 270, 423},                          # Mudanza
}
TODOS_SUBCAT  = set().union(*SUBCAT_IDS.values())
SUBCAT_TO_CAT = {sc: cat for cat, scs in SUBCAT_IDS.items() for sc in scs}

TICKET_COLS   = ["ticket_id","ticket_subcategoria","ticket_categoria",
                 "ticket_grupo","ticket_visita_dia","ticket_dia",
                 "ticket_cliente","synced_at"]
SNAPSHOT_COLS = ["taken_at","grupo_id","categoria_id","estado","cantidad"]
SYNCLOG_COLS  = ["started_at","finished_at","tickets_found","duracion_seg","status","error_msg"]

# ─────────────────────────── ESTADO EN MEMORIA ─────────────────────────────
_tickets_cache:    list[dict] = []
_last_sync_info:   dict       = {}

log = logging.getLogger("uvicorn.error")

# ─────────────────────────── HELPERS ──────────────────────────────────────
def _clasificar(ticket: dict) -> str:
    vd = ticket.get("ticket_visita_dia")
    if not vd or str(vd) in ("0000-00-00", "", "None"):
        return "ab"
    try:
        return "ve" if date.fromisoformat(str(vd)[:10]) < date.today() else "ab"
    except (ValueError, TypeError):
        return "ab"

# ─────────────────────────── GOOGLE SHEETS (vía Apps Script) ────────────────

async def _sheets_post(session: aiohttp.ClientSession, payload: dict) -> None:
    """Envía un POST al Apps Script. No bloquea el sync si falla."""
    if not APPS_SCRIPT_URL:
        return
    try:
        payload["token"] = APPS_SCRIPT_TOKEN
        async with session.post(
            APPS_SCRIPT_URL, json=payload,
            timeout=aiohttp.ClientTimeout(total=60),
        ) as r:
            result = await r.json(content_type=None)
            if result.get("error"):
                log.warning(f"Apps Script error: {result['error']}")
    except Exception as e:
        log.warning(f"No se pudo escribir en Sheets: {e}")


async def _sheets_save_tickets(
    session: aiohttp.ClientSession,
    tickets: list[dict],
    now: str,
) -> None:
    rows = [
        {col: (t.get(col) if t.get(col) is not None else "") for col in TICKET_COLS}
        for t in tickets
    ]
    # Asegurar que synced_at esté en cada fila
    for r in rows:
        r["synced_at"] = now
    await _sheets_post(session, {"action": "save_tickets", "tickets": rows})


async def _sheets_append_snapshot(
    session: aiohttp.ClientSession,
    tickets: list[dict],
    taken_at: str,
) -> None:
    agg: dict[tuple, int] = {}
    for t in tickets:
        key = (
            t.get("ticket_grupo") or "",
            SUBCAT_TO_CAT.get(t.get("ticket_subcategoria"), 0),
            _clasificar(t),
        )
        agg[key] = agg.get(key, 0) + 1

    rows = [
        {"taken_at": taken_at, "grupo_id": grp, "categoria_id": cat,
         "estado": est, "cantidad": cnt}
        for (grp, cat, est), cnt in agg.items()
    ]
    await _sheets_post(session, {"action": "append_snapshot", "rows": rows})


async def _sheets_append_sync_log(
    session: aiohttp.ClientSession,
    log_data: dict,
) -> None:
    await _sheets_post(session, {"action": "append_sync_log", "row": log_data})


async def _sheets_load_tickets_on_startup() -> list[dict]:
    """
    Lee la tab 'tickets' del Sheet público como CSV para poblar el caché al arrancar.
    Formato URL: .../export?format=csv&gid=0  (gid=0 asume que 'tickets' es la primera tab)
    Si el Sheet tiene otro orden, cambiá gid por el correcto.
    """
    if not GOOGLE_SHEETS_ID:
        return []
    url = (f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEETS_ID}"
           f"/gviz/tq?tqx=out:csv&sheet=tickets")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as r:
                if r.status != 200:
                    log.warning(f"Sheets startup load: HTTP {r.status}")
                    return []
                content = await r.text()

        reader = csv.DictReader(io.StringIO(content))
        tickets: list[dict] = []
        for row in reader:
            try:
                sc = int(row.get("ticket_subcategoria") or 0)
                if sc not in TODOS_SUBCAT:
                    continue
                tickets.append({
                    "ticket_id":           int(row.get("ticket_id") or 0),
                    "ticket_subcategoria": sc,
                    "ticket_categoria":    int(row.get("ticket_categoria") or 0),
                    "ticket_grupo":        row.get("ticket_grupo") or None,
                    "ticket_visita_dia":   row.get("ticket_visita_dia") or None,
                    "ticket_dia":          row.get("ticket_dia")        or None,
                    "ticket_cliente":      int(row["ticket_cliente"]) if row.get("ticket_cliente") else None,
                })
            except (ValueError, KeyError):
                continue
        log.info(f"Sheets startup: {len(tickets)} tickets cargados.")
        return tickets
    except Exception as e:
        log.warning(f"Sheets startup load falló: {e}")
        return []


# ─────────────────────────── SYNC ──────────────────────────────────────────
ISP_HDR    = {"X-API-Key": API_KEY, "X-Requested-With": "XMLHttpRequest"}
_sync_lock = asyncio.Lock()


async def _fetch_page(session: aiohttp.ClientSession, page: int, desde: str) -> dict:
    url = f"{ISPKEEPER_BASE}/tickets?per_page=100&page={page}&altaDesde={desde}"
    async with session.get(url, headers=ISP_HDR,
                           timeout=aiohttp.ClientTimeout(total=30)) as r:
        return await r.json(content_type=None)


async def sync_tickets() -> None:
    """
    1. Descarga todos los tickets relevantes de ISPKeeper
    2. Actualiza la caché en memoria (swap atómico)
    3. Persiste en Google Sheets vía Apps Script
    Localidad: se lee de ticket_grupo (siempre presente en la API, no requiere enriquecimiento).
    """
    global _tickets_cache, _last_sync_info

    if _sync_lock.locked():
        log.info("Sync ya en curso — saltando este ciclo.")
        return

    async with _sync_lock:
        started = now_arg()

        try:
            desde  = (date.today() - timedelta(days=DIAS_VENTANA)).isoformat()
            buffer: dict[int, dict] = {}

            async with aiohttp.ClientSession() as session:
                # ── Obtener total de páginas ───────────────────────────
                first   = await _fetch_page(session, 1, desde)
                last_pg = first.get("last_page", 1)

                def _procesar(data: list | None) -> None:
                    for t in (data or []):
                        if (t.get("ticket_finalizado") != "Y"
                                and t.get("ticket_subcategoria") in TODOS_SUBCAT):
                            buffer[t["ticket_id"]] = t

                _procesar(first.get("data"))
                log.info(f"Sync iniciada — {last_pg} páginas desde {desde}")

                # ── Páginas restantes (del final hacia el inicio) ───────
                for end in range(last_pg, 0, -BATCH_SIZE):
                    start = max(end - BATCH_SIZE + 1, 1)
                    pages = [p for p in range(start, end + 1) if p != 1]
                    if not pages:
                        continue
                    results = await asyncio.gather(
                        *[_fetch_page(session, p, desde) for p in pages],
                        return_exceptions=True,
                    )
                    for r in results:
                        if isinstance(r, dict):
                            _procesar(r.get("data"))

                # ── Actualizar caché en memoria ────────────────────────
                tickets_data = list(buffer.values())
                now     = now_arg()
                now_str = now.isoformat()
                dur     = int((now - started).total_seconds())

                _tickets_cache = tickets_data
                _last_sync_info = {
                    "started_at":    started.isoformat(),
                    "finished_at":   now_str,
                    "tickets_found": len(tickets_data),
                    "duracion_seg":  dur,
                    "status":        "ok",
                    "error_msg":     None,
                }
                log.info(f"Sync OK — {len(tickets_data)} tickets en {dur}s")

                # ── Persistir en Google Sheets ─────────────────────────
                if APPS_SCRIPT_URL:
                    await _sheets_save_tickets(session, tickets_data, now_str)
                    await _sheets_append_snapshot(session, tickets_data, now_str)
                    await _sheets_append_sync_log(session, {
                        "started_at":    started.isoformat(),
                        "finished_at":   now_str,
                        "tickets_found": len(tickets_data),
                        "duracion_seg":  dur,
                        "status":        "ok",
                        "error_msg":     "",
                    })
                    log.info("Sheets: datos enviados al Apps Script.")

        except Exception as exc:
            log.exception("Error en sync_tickets")
            err_str = str(exc)
            _last_sync_info = {
                "started_at":  started.isoformat(),
                "finished_at": now_arg().isoformat(),
                "status":      "error",
                "error_msg":   err_str,
            }
            if APPS_SCRIPT_URL:
                try:
                    async with aiohttp.ClientSession() as session:
                        await _sheets_append_sync_log(session, {
                            "started_at":  started.isoformat(),
                            "finished_at": now_arg().isoformat(),
                            "status":      "error",
                            "error_msg":   err_str,
                        })
                except Exception:
                    pass


# ─────────────────────────── FASTAPI APP ───────────────────────────────────

scheduler = AsyncIOScheduler(timezone="America/Argentina/Buenos_Aires")


@asynccontextmanager
async def lifespan(_: FastAPI):
    """
    Al arrancar:
    1. Si GOOGLE_SHEETS_ID está configurado, carga tickets del Sheet público → caché inmediata
    2. Inicia scheduler y primera sync en background
    """
    if GOOGLE_SHEETS_ID:
        log.info("Cargando tickets existentes desde Google Sheets…")
        loaded = await _sheets_load_tickets_on_startup()
        if loaded:
            global _tickets_cache
            _tickets_cache = loaded

    # Sync en horarios fijos hora Argentina: 11:00, 14:00, 17:30, 19:00, 23:30
    scheduler.add_job(sync_tickets, "cron", hour=11, minute=0,  id="sync_1100", timezone="America/Argentina/Buenos_Aires")
    scheduler.add_job(sync_tickets, "cron", hour=14, minute=0,  id="sync_1400", timezone="America/Argentina/Buenos_Aires")
    scheduler.add_job(sync_tickets, "cron", hour=17, minute=30, id="sync_1730", timezone="America/Argentina/Buenos_Aires")
    scheduler.add_job(sync_tickets, "cron", hour=19, minute=0,  id="sync_1900", timezone="America/Argentina/Buenos_Aires")
    scheduler.add_job(sync_tickets, "cron", hour=23, minute=30, id="sync_2330", timezone="America/Argentina/Buenos_Aires")
    scheduler.start()
    asyncio.create_task(sync_tickets())   # primera sync inmediata en background
    yield
    scheduler.shutdown()


app = FastAPI(title="ISPKeeper Dashboard API", version="3.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


# ── Endpoints ───────────────────────────────────────────────────────────────

@app.get("/")
def root():
    return {"status": "ok", "service": "ISPKeeper Dashboard API", "version": "3.0.0"}


@app.get("/tickets")
def get_tickets():
    """Tickets abiertos actuales desde caché en memoria. Respuesta < 10 ms."""
    return _tickets_cache


@app.get("/status")
def get_status():
    nj = scheduler.get_job("sync")
    return {
        "tickets_count":       len(_tickets_cache),
        "sync_horarios":       "11:00 · 14:00 · 17:30 · 19:00 · 23:30",
        "sheets_configurado":  bool(APPS_SCRIPT_URL),
        "last_sync":           _last_sync_info or None,
        "next_sync":           nj.next_run_time.isoformat() if nj and nj.next_run_time else None,
    }


@app.get("/historico")
def get_historico(dias: int = 30):
    """
    Lee snapshots históricos desde la tab 'snapshots' del Google Sheet público.
    Útil para graficar carga por día, vencimientos, etc.
    """
    if not GOOGLE_SHEETS_ID:
        return {"error": "GOOGLE_SHEETS_ID no configurado"}

    import asyncio as _asyncio

    async def _fetch():
        url = (f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEETS_ID}"
               f"/gviz/tq?tqx=out:csv&sheet=snapshots")
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as r:
                return await r.text() if r.status == 200 else ""

    content = _asyncio.get_event_loop().run_until_complete(_fetch())
    if not content:
        return []

    desde_str = (now_arg() - timedelta(days=dias)).isoformat()
    reader = csv.DictReader(io.StringIO(content))
    return [
        row for row in reader
        if str(row.get("taken_at", "")) >= desde_str
    ]


@app.get("/config")
async def get_config():
    """
    Lee la hoja 'config' del Google Sheet y devuelve umbrales y cupos por localidad,
    indexado por código de grupo (RG, LH, RT, etc.).
    Columnas: A=nombre, B=umbral_inst, C=umbral_rep, D=umbral_mud, E=cupo_inst, F=cupo_rep, G=cupo_mud
    Datos desde fila 3 (se saltan 2 filas de encabezado).
    """
    if not GOOGLE_SHEETS_ID:
        return {"error": "GOOGLE_SHEETS_ID no configurado"}

    import unicodedata

    def norm(s: str) -> str:
        """Normaliza acentos, quita marks, normaliza espacios a ASCII."""
        s = unicodedata.normalize("NFD", s)
        s = "".join(c for c in s if unicodedata.category(c) != "Mn")
        s = " ".join(s.split())   # colapsa cualquier tipo de espacio (nbsp, tab, etc.)
        return s.lower()

    # Palabras clave únicas por grupo para substring matching
    GRUPO_KEYWORDS = {
        "RG": ["gallegos"],
        "LH": ["heras"],
        "RT": ["turbio"],
        "GG": ["gregores"],
        "SC": ["santa cruz"],
        "SJ": ["julian"],
        "PB": ["piedra buena", "piedrabuena"],
        "PM": ["perito"],
        "TL": ["lagos"],
        "PC": ["truncado"],
    }

    def match_grupo(nombre: str):
        n = norm(nombre)
        for grupo, keywords in GRUPO_KEYWORDS.items():
            if any(kw in n for kw in keywords):
                return grupo
        return None

    url = (f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEETS_ID}"
           f"/gviz/tq?tqx=out:csv&sheet=config")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as r:
                content = await r.text() if r.status == 200 else ""
    except Exception as e:
        return {"error": f"Error al leer sheet: {e}"}

    if not content or content.strip().startswith("<"):
        return {"error": "No se pudo leer el sheet (¿es público con 'cualquier persona puede ver'?)"}

    def parse_num(v):
        try:
            return float(v.replace(",", ".").strip()) if v and v.strip() else None
        except ValueError:
            return None

    lines = content.strip().split("\n")
    data_lines = lines[1:]  # saltar 1 fila de encabezado (el CSV del gviz solo tiene 1)
    result = {}
    for line in data_lines:
        cols = []
        cur, in_q = "", False
        for ch in line:
            if ch == '"':
                in_q = not in_q
            elif ch == "," and not in_q:
                cols.append(cur.strip())
                cur = ""
            else:
                cur += ch
        cols.append(cur.strip())
        nombre = (cols[0] if cols else "").replace('"', '').strip()
        if not nombre:
            continue
        grupo = match_grupo(nombre)
        if not grupo:
            logger.warning("config: no se pudo mapear localidad '%s'", nombre)
            continue
        result[grupo] = {
            "nombre":      nombre,
            "umbral_inst": parse_num(cols[1] if len(cols) > 1 else ""),
            "umbral_rep":  parse_num(cols[2] if len(cols) > 2 else ""),
            "umbral_mud":  parse_num(cols[3] if len(cols) > 3 else ""),
            "cupo_inst":   parse_num(cols[4] if len(cols) > 4 else ""),
            "cupo_rep":    parse_num(cols[5] if len(cols) > 5 else ""),
            "cupo_mud":    parse_num(cols[6] if len(cols) > 6 else ""),
        }
    return result


@app.get("/config-raw")
async def get_config_raw():
    """Debug: devuelve todas las filas del sheet config sin matching, con repr() para ver caracteres ocultos."""
    if not GOOGLE_SHEETS_ID:
        return {"error": "GOOGLE_SHEETS_ID no configurado"}
    url = (f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEETS_ID}"
           f"/gviz/tq?tqx=out:csv&sheet=config")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as r:
                content = await r.text() if r.status == 200 else ""
    except Exception as e:
        return {"error": str(e)}
    lines = content.strip().split("\n")
    return {
        "total_lines": len(lines),
        "lines": [{"index": i, "raw": line, "repr": repr(line)} for i, line in enumerate(lines)]
    }


@app.post("/sync")
async def trigger_sync():
    """Dispara una sincronización manual."""
    asyncio.create_task(sync_tickets())
    return {"message": "Sync iniciada en background"}
