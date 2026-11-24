#!/usr/bin/env python3
"""Autochequeo de la prueba: comprueba que la evaluación podrá leer tu vista GOLD.V_SALES_LINES y, si se lo
pides, tu topic sales.enriched y tu API, con la forma de los contratos. NO comprueba si los datos son correctos.

  python selfcheck/selfcheck.py                         # usa las variables SNOWFLAKE_* de tu entorno / .env
  python selfcheck/selfcheck.py --api http://localhost:5000 --kafka localhost:19092
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path

HERE = Path(__file__).resolve().parent
CONTRACT = json.loads((HERE.parent / "contracts" / "contract.json").read_text(encoding="utf-8"))
VIEW = "V_SALES_LINES"
COUNTRIES = ("ES", "FR", "MX", "PT", "GB")
ENRICHED = "sales.enriched"
ENRICHED_FIELDS = set(json.loads((HERE.parent / "contracts" / "sales-enriched.schema.json").read_text(encoding="utf-8"))["required"])


class Snowflake:
    def __init__(self) -> None:
        import snowflake.connector
        from cryptography.hazmat.primitives import serialization

        pw = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE") or None
        with open(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"], "rb") as fh:
            key = serialization.load_pem_private_key(fh.read(), password=pw.encode() if pw else None)
        der = key.private_bytes(serialization.Encoding.DER, serialization.PrivateFormat.PKCS8,
                                serialization.NoEncryption())
        self.db = os.environ["SNOWFLAKE_DATABASE"]
        self.con = snowflake.connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"], user=os.environ["SNOWFLAKE_USER"], private_key=der,
            role=os.getenv("SNOWFLAKE_ROLE"), warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"), database=self.db,
            session_parameters={"QUERY_TAG": "selfcheck"})

    def q(self, sql: str):
        cur = self.con.cursor()
        try:
            return cur.execute(sql).fetchall()
        finally:
            cur.close()

    def columns(self) -> dict:
        out: dict = {}
        for t, c, dt in self.q(f"SELECT table_name, column_name, data_type FROM {self.db}.INFORMATION_SCHEMA.COLUMNS "
                               "WHERE table_schema = 'GOLD'"):
            out.setdefault(t.upper(), {})[c.upper()] = dt.upper()
        return out


def gold_checks(db) -> list[tuple[str, bool, str]]:
    res: list[tuple[str, bool, str]] = []

    def check(name: str, ok: bool, detail: str = "") -> None:
        res.append((name, bool(ok), detail))

    def sql_check(name: str, sql: str, pred, fmt=lambda r: "") -> None:
        try:
            rows = db.q(sql)
            check(name, pred(rows), fmt(rows))
        except Exception as e:  # noqa: BLE001
            check(name, False, f"error: {str(e).splitlines()[0][:160]}")

    cols = db.columns()
    for obj, req in CONTRACT["objects"].items():
        have = cols.get(obj)
        if not have:
            check(f"GOLD.{obj} existe", False, "no existe")
            continue
        miss = [c for c in req if c not in have]
        check(f"GOLD.{obj} tiene las columnas del contrato", not miss, f"faltan: {miss}" if miss else "")
    sql_check(f"{VIEW}: tiene datos", f"SELECT COUNT(*), MIN(business_date), MAX(business_date) FROM GOLD.{VIEW}",
              lambda r: r[0][0] > 0, lambda r: f"{r[0][0]} filas ({r[0][1]}..{r[0][2]})")
    sql_check(f"{VIEW}: LINE_TYPE solo vale SALE o RETURN",
              f"SELECT COUNT(*) FROM GOLD.{VIEW} WHERE line_type IS NULL OR line_type NOT IN ('SALE', 'RETURN')",
              lambda r: r[0][0] == 0, lambda r: f"{r[0][0]} filas con otro valor")
    return res


def api_checks(base: str, db) -> list[tuple[str, bool, str]]:
    import requests

    res: list[tuple[str, bool, str]] = []

    def call(name, path, params, code, keys=()):
        try:
            r = requests.get(base.rstrip("/") + path, params=params, timeout=30)
            body = r.json()
            missing = [k for k in keys if k not in body]
            ok = r.status_code == code and not missing and (code == 200 or {"title", "status"} <= set(body))
            res.append((name, ok, f"HTTP {r.status_code}" + (f", faltan {missing}" if missing else "")))
            return body if ok else None
        except Exception as e:  # noqa: BLE001
            res.append((name, False, str(e)[:160]))
            return None

    lo, hi, store = db.q(f"SELECT MIN(business_date), MAX(business_date), MIN(store_code) FROM GOLD.{VIEW}")[0]
    country = store[:2] if store and store[:2] in COUNTRIES else COUNTRIES[0]
    to = date.fromisoformat(str(hi)[:10])
    fr = max(date.fromisoformat(str(lo)[:10]), to - timedelta(days=6))
    rng = {"from": fr.isoformat(), "to": to.isoformat()}
    n_days = (to - fr).days + 1
    b = call("GET /v1/kpis/daily", "/v1/kpis/daily", {"country": country, **rng}, 200,
             ("country", "currency", "from", "to", "days", "totals"))
    if b is not None:
        res.append(("kpis/daily: un elemento por día del rango", len(b["days"]) == n_days, f"{len(b['days'])} de {n_days}"))
    b = call(f"GET /v1/stores/{store}/kpis", f"/v1/stores/{store}/kpis", rng, 200,
             ("store_code", "country_code", "currency", "region", "days", "totals"))
    if b is not None:
        res.append(("stores/kpis: totals.returns_rate presente", "returns_rate" in b["totals"], ""))
    call("GET /v1/products/top", "/v1/products/top", {"country": country, **rng, "limit": 5}, 200, ("items",))
    call("GET /v1/meta/freshness", "/v1/meta/freshness", None, 200, ("last_business_date", "loaded_at"))
    call("Tienda inexistente -> 404 Problem Details", "/v1/stores/ZZ-NOPE-999/kpis", rng, 404)
    call("from > to -> 400 Problem Details", "/v1/kpis/daily", {"country": country, "from": rng["to"], "to": rng["from"]}, 400)
    return res


def kafka_checks(bootstrap: str) -> list[tuple[str, bool, str]]:
    from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition

    res: list[tuple[str, bool, str]] = []
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": "selfcheck", "enable.auto.commit": False})
    try:
        topics = c.list_topics(timeout=10).topics
        if ENRICHED not in topics:
            return [(f"topic {ENRICHED} existe", False, "no existe")]
        parts = sorted(topics[ENRICHED].partitions)
        total = sum(c.get_watermark_offsets(TopicPartition(ENRICHED, p), timeout=10)[1] for p in parts)
        res.append((f"{ENRICHED}: tiene mensajes", total > 0, f"{total} mensajes"))
        c.assign([TopicPartition(ENRICHED, p, OFFSET_BEGINNING) for p in parts])
        msgs, t0 = [], time.time()
        while total and len(msgs) < 200 and time.time() - t0 < 15:
            msgs += [m for m in c.consume(200, timeout=1.0) if m.error() is None]
        not_json = missing = other_key = 0
        for m in msgs[:200]:
            try:
                v = json.loads(m.value())
            except (TypeError, ValueError):
                not_json += 1
                continue
            if not isinstance(v, dict) or not ENRICHED_FIELDS <= set(v):
                missing += 1
                continue
            if (m.key() or b"").decode("utf-8", "replace") != f"{v['ticket_id']}-{v['line_no']}":
                other_key += 1
        n = min(len(msgs), 200)
        res.append((f"{ENRICHED}: JSON con los campos del contrato", n > 0 and not_json == missing == 0,
                    f"{n} leídos, {not_json} no son JSON, {missing} sin todos los campos"))
        res.append((f"{ENRICHED}: clave TICKET_ID-LINE_NO", n > 0 and other_key == 0, f"{other_key} con otra clave"))
    finally:
        c.close()
    return res


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--api", help="URL base de tu API, p. ej. http://localhost:5000")
    ap.add_argument("--kafka", help="bootstrap de Kafka para revisar sales.enriched, p. ej. localhost:19092")
    args = ap.parse_args()
    db = Snowflake()
    results = gold_checks(db)
    if args.kafka:
        results += kafka_checks(args.kafka)
    if args.api:
        results += api_checks(args.api, db)
    width = max(len(r[0]) for r in results)
    for name, ok, detail in results:
        print(f"{'OK  ' if ok else 'FAIL'}  {name.ljust(width)}  {detail}")
    failed = sum(1 for r in results if not r[1])
    print(f"\n{len(results) - failed}/{len(results)} comprobaciones OK")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
