"""Sistema origen simulado de la prueba Data Engineer.

Expone, como lo harían los sistemas de una cadena de moda, los datos que tu pipeline debe integrar. Por HTTP:
los exports nocturnos del TPV legacy, los snapshots de tiendas, los tipos de cambio y los totales de control.
Por Kafka: los tickets del TPV nuevo y los cambios del catálogo. Un reloj simulado (`as_of`) libera los datos
día a día.

Este servicio es deliberadamente "tonto": sirve y publica ficheros desde DATA_DIR. La evaluación se hace
con OTRO dataset servido por este mismo servicio, así que tu código debe consumir siempre la API y los topics.
"""
from __future__ import annotations

import asyncio
import gzip
import json
import os
import random
import threading
import time
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from datetime import date, timedelta
from pathlib import Path
from typing import Literal, Optional

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field

DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
API_KEY = os.getenv("SOURCE_API_KEY", "de-test-2026")
CHAOS = os.getenv("CHAOS", "on").lower() in ("1", "on", "true", "yes")
FAIL_RATE = float(os.getenv("CHAOS_FAIL_RATE", "0.02"))
RATE_LIMIT_RPS = int(os.getenv("RATE_LIMIT_RPS", "20"))
LATENCY_MIN_MS, LATENCY_MAX_MS = 20, 120
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "redpanda:9092")

MANIFEST = json.loads((DATA_DIR / "manifest.json").read_text(encoding="utf-8"))
PERIOD_START = date.fromisoformat(MANIFEST["period"]["start"])
PERIOD_END = date.fromisoformat(MANIFEST["period"]["end"])
FILES = {(f["system"], f["file"]): f for f in MANIFEST["files"]}
FX = {r["date"]: r for r in json.loads((DATA_DIR / "fx" / "rates.json").read_text(encoding="utf-8"))}
CHANGELOG = json.loads((DATA_DIR / "changelog.json").read_text(encoding="utf-8"))
TOPICS: dict[str, dict] = MANIFEST["topics"]
# Cada día se publica primero el catálogo y después los tickets.
PUBLISH_ORDER = ["catalog.products", "pos-a.tickets"]
EVENTS: dict[str, list[dict]] = {}
for _topic in PUBLISH_ORDER:
    with gzip.open(DATA_DIR / "events" / f"{_topic}.jsonl.gz", "rt", encoding="utf-8") as _fh:
        EVENTS[_topic] = [json.loads(line) for line in _fh if line.strip()]

_clock = {"as_of": date.fromisoformat(os.getenv("SOURCE_CLOCK") or MANIFEST["default_clock"])}
_hits: dict[str, deque] = defaultdict(deque)

DESCRIPTION = """
API de integración de los sistemas origen de una cadena de moda.

**Autenticación:** cabecera `X-API-Key` en todos los endpoints `/v1/*`.

**Reloj simulado:** el origen solo publica lo ocurrido *antes* de `as_of` (consúltalo en `GET /v1/clock`).
Un día `D` queda publicado cuando el reloj está en `D+1` o después, tanto por HTTP como en Kafka. En local
puedes moverlo con `PUT /admin/clock`; en la evaluación lo controla el evaluador.

**Kafka:** los tickets del TPV nuevo y los cambios del catálogo se publican en topics (ver el readme del kit).

**Exports del TPV legacy (`pos-b`):** CSV comprimido separado por `;`, una fila por línea de ticket, con la
fecha-hora local de la tienda.
"""


class EventPublisher:
    """Publica en Kafka, en orden, los eventos que el reloj ya da por ocurridos. Es el único productor de los
    topics de entrada; si el origen se reinicia, continúa donde se quedó."""

    def __init__(self, bootstrap: str):
        self.bootstrap = bootstrap
        self.lock = threading.Lock()
        self.sent = {t: 0 for t in PUBLISH_ORDER}
        self.ready = False
        self.error: Optional[str] = None
        self.producer = None

    def connect(self, timeout_s: float = 180) -> None:
        from confluent_kafka import Producer
        from confluent_kafka.admin import AdminClient, NewTopic

        deadline = time.time() + timeout_s
        while True:
            try:
                admin = AdminClient({"bootstrap.servers": self.bootstrap})
                existing = admin.list_topics(timeout=5).topics
                missing = [NewTopic(t, num_partitions=cfg["partitions"], replication_factor=1,
                                    config={"retention.ms": "-1", "cleanup.policy": "delete"})
                           for t, cfg in TOPICS.items() if t not in existing]
                for fut in (admin.create_topics(missing).values() if missing else []):
                    try:
                        fut.result()
                    except Exception as e:  # noqa: BLE001
                        if "TOPIC_ALREADY_EXISTS" not in str(e):
                            raise
                self.sent = self._published_counts()
                self.producer = Producer({"bootstrap.servers": self.bootstrap, "enable.idempotence": True,
                                          "linger.ms": 20, "compression.type": "lz4"})
                self.error = None  # "ready" llega cuando termina la publicación inicial
                return
            except Exception as e:  # noqa: BLE001
                self.error = str(e)
                if time.time() > deadline:
                    raise
                time.sleep(2)

    def _published_counts(self) -> dict[str, int]:
        from confluent_kafka import Consumer, TopicPartition

        c = Consumer({"bootstrap.servers": self.bootstrap, "group.id": "source-system-probe",
                      "enable.auto.commit": False})
        try:
            topics = c.list_topics(timeout=10).topics
            return {t: sum(c.get_watermark_offsets(TopicPartition(t, p), timeout=10)[1] for p in topics[t].partitions)
                    for t in PUBLISH_ORDER}
        finally:
            c.close()

    def _produce(self, topic: str, e: dict) -> None:
        headers = [(k, v.encode("utf-8")) for k, v in e["headers"].items()]
        while True:
            try:
                self.producer.produce(topic, key=e["key"].encode("utf-8"), value=e["value"].encode("utf-8"),
                                      headers=headers)
                return
            except BufferError:
                self.producer.poll(0.5)

    def publish_until(self, as_of: date) -> dict[str, int]:
        """Publica los eventos pendientes con publish_date < as_of y espera a que Kafka los confirme."""
        if self.producer is None:
            return {}
        cutoff = as_of.isoformat()
        sent_now = {}
        with self.lock:
            for t in PUBLISH_ORDER:
                evs, i0 = EVENTS[t], self.sent[t]
                i = i0
                while i < len(evs) and evs[i]["publish_date"] < cutoff:
                    self._produce(t, evs[i])
                    i += 1
                    self.producer.poll(0)
                if self.producer.flush(60) > 0:
                    raise RuntimeError(f"Kafka no confirmó todos los mensajes de {t}")
                self.sent[t] = i
                sent_now[t] = i - i0
        return sent_now


publisher = EventPublisher(KAFKA_BOOTSTRAP)


def _start_publisher() -> None:
    try:
        publisher.connect()
        publisher.publish_until(_clock["as_of"])
        publisher.ready = True
    except Exception as e:  # noqa: BLE001
        publisher.ready, publisher.error = False, f"Kafka no disponible: {e}"


@asynccontextmanager
async def lifespan(_app: FastAPI):
    threading.Thread(target=_start_publisher, daemon=True).start()
    yield


app = FastAPI(title="Retail Source System (simulado)", version="2.0.0", description=DESCRIPTION, lifespan=lifespan)
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def require_key(key: Optional[str] = Depends(api_key_header)) -> None:
    if key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing X-API-Key")


@app.middleware("http")
async def chaos(request: Request, call_next):
    path = request.url.path
    if CHAOS and path.startswith("/v1/") and path != "/v1/clock":
        client = request.client.host if request.client else "unknown"
        now = time.monotonic()
        hits = _hits[client]
        while hits and now - hits[0] > 1.0:
            hits.popleft()
        if len(hits) >= RATE_LIMIT_RPS:
            return JSONResponse(status_code=429, content={"error": "Rate limit exceeded"}, headers={"Retry-After": "1"})
        hits.append(now)
        await asyncio.sleep(random.uniform(LATENCY_MIN_MS, LATENCY_MAX_MS) / 1000)
        if random.random() < FAIL_RATE:
            return JSONResponse(status_code=random.choice([500, 502, 503]),
                                content={"error": "Upstream temporarily unavailable"})
    return await call_next(request)


# ----------------------------------------------------------------------------- modelos (documentación)
class Clock(BaseModel):
    as_of: date = Field(..., description="Fecha actual del origen. Publicado = todo lo ocurrido antes de esta fecha.")
    period_start: date
    period_end: date


class ClockUpdate(BaseModel):
    as_of: date


class ExportFile(BaseModel):
    system: Literal["pos-b"]
    export_date: date = Field(..., description="Fecha del export nocturno")
    file: str
    records: int = Field(..., description="Filas del fichero")
    sha256: str = Field(..., description="Hash SHA-256 del fichero comprimido")
    url: str


class ExportList(BaseModel):
    files: list[ExportFile]


class Store(BaseModel):
    store_code: str
    name: str
    country_code: str
    city: str
    currency: str
    timezone: str = Field(..., description="Zona horaria IANA de la tienda")
    pos_system: Literal["pos-a", "pos-b"]
    region: str
    format: str
    opened_on: date


class StoreSnapshot(BaseModel):
    date: date
    stores: list[Store]


class FxRate(BaseModel):
    date: date
    base: Literal["EUR"]
    rates: dict[str, str] = Field(..., description="Unidades de cada divisa por 1 EUR (p. ej. MXN: 20.4123)")


class ControlTotal(BaseModel):
    store_code: str
    business_date: date
    currency: str
    tickets: int = Field(..., description="Tickets con al menos una línea de venta")
    sales_units: int
    sales_net: str = Field(..., description="Ventas netas (sin impuestos) en moneda local")
    returns_units: int = Field(..., description="Unidades devueltas (positivo)")
    returns_net: str = Field(..., description="Devoluciones netas en moneda local (positivo)")


class ControlTotals(BaseModel):
    date: date
    stores: list[ControlTotal]


class ChangelogEntry(BaseModel):
    date: date
    system: str
    text: str


# ----------------------------------------------------------------------------- helpers
def _published(d: date) -> bool:
    return d < _clock["as_of"]


def _check_period(d: date) -> None:
    if d < PERIOD_START or d > PERIOD_END:
        raise HTTPException(404, detail=f"Sin datos para {d}: el origen cubre {PERIOD_START}..{PERIOD_END}")


def _check_published(d: date) -> None:
    _check_period(d)
    if not _published(d):
        raise HTTPException(404, detail=f"{d} aún no está publicado (as_of={_clock['as_of']})")


# ----------------------------------------------------------------------------- endpoints
@app.get("/health", include_in_schema=False)
async def health():
    if not publisher.ready:
        return JSONResponse(status_code=503, content={"status": "starting", "kafka": publisher.error or "conectando"})
    return {"status": "ok", "kafka": "ready", "published": publisher.sent}


@app.get("/v1/clock", response_model=Clock, tags=["Reloj"], dependencies=[Depends(require_key)])
async def get_clock():
    """Fecha actual del origen (`as_of`) y periodo que cubre el dataset."""
    return Clock(as_of=_clock["as_of"], period_start=PERIOD_START, period_end=PERIOD_END)


@app.put("/admin/clock", response_model=Clock, tags=["Reloj"], dependencies=[Depends(require_key)])
async def set_clock(body: ClockUpdate):
    """**Solo para desarrollo local.** Mueve el reloj del origen y publica en Kafka lo que pase a estar ocurrido.
    En la evaluación lo controla el evaluador."""
    _clock["as_of"] = body.as_of
    if not publisher.ready:
        raise HTTPException(503, detail=publisher.error or "Kafka aún no está listo")
    await asyncio.to_thread(publisher.publish_until, body.as_of)
    return Clock(as_of=_clock["as_of"], period_start=PERIOD_START, period_end=PERIOD_END)


@app.get("/v1/changelog", response_model=list[ChangelogEntry], tags=["Documentación"],
         dependencies=[Depends(require_key)])
async def changelog():
    """Avisos publicados por los equipos de los sistemas origen."""
    return [e for e in CHANGELOG if date.fromisoformat(e["date"]) < _clock["as_of"]]


@app.get("/v1/exports", response_model=ExportList, tags=["TPV legacy"], dependencies=[Depends(require_key)])
async def list_exports(system: Literal["pos-b"] = "pos-b",
                       date_: Optional[date] = Query(None, alias="date", description="Fecha de export concreta"),
                       from_: Optional[date] = Query(None, alias="from", description="Inicio del rango (incluido)"),
                       to: Optional[date] = Query(None, description="Fin del rango (incluido)")):
    """Lista los ficheros de export nocturno publicados del TPV legacy."""
    if date_ is not None:
        from_, to = date_, date_
    lo = from_ or PERIOD_START
    hi = to or PERIOD_END
    out = []
    for (system_, name), f in sorted(FILES.items()):
        d = date.fromisoformat(f["export_date"])
        if system_ == system and lo <= d <= hi and _published(d):
            out.append(ExportFile(system=system, export_date=d, file=name, records=f["records"], sha256=f["sha256"],
                                  url=f"/v1/exports/{system}/{name}"))
    return ExportList(files=out)


@app.get("/v1/exports/{system}/{file_name}", tags=["TPV legacy"], dependencies=[Depends(require_key)],
         responses={200: {"content": {"application/gzip": {}}, "description": "Fichero comprimido (gzip)"}})
async def download_export(system: Literal["pos-b"], file_name: str):
    """Descarga un fichero de export (gzip)."""
    f = FILES.get((system, file_name))
    if f is None or not _published(date.fromisoformat(f["export_date"])):
        raise HTTPException(404, detail="Fichero no encontrado o aún no publicado")
    return FileResponse(DATA_DIR / "exports" / system / file_name, media_type="application/gzip", filename=file_name)


@app.get("/v1/stores/snapshot", response_model=StoreSnapshot, tags=["Tiendas"], dependencies=[Depends(require_key)])
async def store_snapshot(date_: date = Query(..., alias="date")):
    """Foto de las tiendas abiertas en una fecha."""
    _check_published(date_)
    stores = json.loads((DATA_DIR / "stores" / f"{date_.isoformat()}.json").read_text(encoding="utf-8"))
    return StoreSnapshot(date=date_, stores=stores)


@app.get("/v1/fx-rates", response_model=list[FxRate], tags=["Tipos de cambio"], dependencies=[Depends(require_key)])
async def fx_rates(date_: Optional[date] = Query(None, alias="date"),
                   from_: Optional[date] = Query(None, alias="from"), to: Optional[date] = None):
    """Tipos de cambio de referencia (base EUR)."""
    if date_ is not None:
        from_, to = date_, date_
    lo = from_ or PERIOD_START - timedelta(days=10)
    hi = to or PERIOD_END
    out = [FxRate(**r) for d, r in sorted(FX.items())
           if lo <= date.fromisoformat(d) <= hi and _published(date.fromisoformat(d))]
    if date_ is not None and not out:
        raise HTTPException(404, detail=f"Sin publicación de tipos de cambio para {date_}")
    return out


@app.get("/v1/control-totals", response_model=ControlTotals, tags=["Control"], dependencies=[Depends(require_key)])
async def control_totals(date_: date = Query(..., alias="date")):
    """Totales de cierre de caja por tienda para un día de negocio (fecha local de la tienda), calculados por un
    sistema independiente de los TPV."""
    _check_published(date_)
    rows = json.loads((DATA_DIR / "control" / f"{date_.isoformat()}.json").read_text(encoding="utf-8"))
    return ControlTotals(date=date_, stores=rows)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=3001)
