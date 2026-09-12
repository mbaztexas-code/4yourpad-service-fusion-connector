
import os
import json
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import httpx
import psycopg
from fastapi import FastAPI, Header, HTTPException, Query

app = FastAPI(title="4YourPad Service Fusion Connector", version="1.2.0")

SERVICE_FUSION_CLIENT_ID = os.getenv("SERVICE_FUSION_CLIENT_ID")
SERVICE_FUSION_CLIENT_SECRET = os.getenv("SERVICE_FUSION_CLIENT_SECRET")
CONNECTOR_API_KEY = os.getenv("CONNECTOR_API_KEY")
SERVICE_FUSION_BASE = os.getenv("SERVICE_FUSION_BASE", "https://api.servicefusion.com/v1")
SERVICE_FUSION_TOKEN_URL = os.getenv(
    "SERVICE_FUSION_TOKEN_URL",
    "https://api.servicefusion.com/oauth/access_token",
)
DATABASE_URL = os.getenv("DATABASE_URL")

_token_cache: Dict[str, Any] = {"access_token": None, "expires_at": None}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def require_connector_key(x_connector_key: Optional[str]) -> None:
    if not CONNECTOR_API_KEY:
        raise HTTPException(status_code=500, detail="CONNECTOR_API_KEY is not configured")
    if x_connector_key != CONNECTOR_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


async def get_access_token() -> str:
    cached = _token_cache.get("access_token")
    expires_at = _token_cache.get("expires_at")
    if cached and expires_at and utcnow() < expires_at:
        return cached

    if not SERVICE_FUSION_CLIENT_ID or not SERVICE_FUSION_CLIENT_SECRET:
        raise HTTPException(status_code=500, detail="Service Fusion credentials are not configured")

    payload = {
        "grant_type": "client_credentials",
        "client_id": SERVICE_FUSION_CLIENT_ID,
        "client_secret": SERVICE_FUSION_CLIENT_SECRET,
    }

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=20.0)) as client:
            response = await client.post(SERVICE_FUSION_TOKEN_URL, json=payload)
            response.raise_for_status()
            data = response.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=502, detail="Service Fusion token request timed out")
    except httpx.RequestError as exc:
        raise HTTPException(status_code=502, detail=f"Service Fusion token request failed: {exc}")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Service Fusion token request returned {exc.response.status_code}: {exc.response.text[:500]}",
        )
    except ValueError:
        raise HTTPException(status_code=502, detail="Service Fusion token response was not valid JSON")

    token = data.get("access_token")
    if not token:
        raise HTTPException(status_code=502, detail="Service Fusion token response did not include access_token")

    expires_in = int(data.get("expires_in", 3600))
    _token_cache["access_token"] = token
    _token_cache["expires_at"] = utcnow() + timedelta(seconds=max(expires_in - 60, 60))
    return token


async def sf_get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    token = await get_access_token()
    url = f"{SERVICE_FUSION_BASE.rstrip('/')}/{path.lstrip('/')}"
    headers = {"Authorization": f"Bearer {token}"}

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=20.0)) as client:
            response = await client.get(url, headers=headers, params=params or {})
            response.raise_for_status()
            return response.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=502, detail=f"Service Fusion request timed out: {path}")
    except httpx.RequestError as exc:
        raise HTTPException(status_code=502, detail=f"Service Fusion request failed: {exc}")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Service Fusion returned {exc.response.status_code} for {path}: {exc.response.text[:500]}",
        )
    except ValueError:
        raise HTTPException(status_code=502, detail=f"Service Fusion returned invalid JSON for {path}")


def extract_items(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    for key in ("items", "data", "records", "results"):
        value = payload.get(key)
        if isinstance(value, list):
            return value
    if isinstance(payload, list):
        return payload
    return []


def extract_meta(payload: Dict[str, Any]) -> Dict[str, Any]:
    meta = payload.get("_meta")
    return meta if isinstance(meta, dict) else {}


def require_database() -> None:
    if not DATABASE_URL:
        raise HTTPException(status_code=500, detail="DATABASE_URL is not configured")


def db_connect():
    require_database()
    return psycopg.connect(DATABASE_URL)


def init_db() -> None:
    require_database()
    ddl = """
    CREATE TABLE IF NOT EXISTS sf_customers (
        sf_id TEXT PRIMARY KEY,
        name TEXT,
        phone TEXT,
        email TEXT,
        city TEXT,
        state TEXT,
        postal_code TEXT,
        created_at_sf TIMESTAMPTZ,
        updated_at_sf TIMESTAMPTZ,
        raw_json JSONB NOT NULL,
        synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS sf_jobs (
        sf_id TEXT PRIMARY KEY,
        customer_id TEXT,
        number TEXT,
        status TEXT,
        category TEXT,
        source TEXT,
        start_date TIMESTAMPTZ,
        completion_date TIMESTAMPTZ,
        total NUMERIC,
        raw_json JSONB NOT NULL,
        synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_sf_jobs_customer_id ON sf_jobs(customer_id);
    CREATE INDEX IF NOT EXISTS idx_sf_jobs_start_date ON sf_jobs(start_date);

    CREATE TABLE IF NOT EXISTS sf_invoices (
        sf_id TEXT PRIMARY KEY,
        customer_id TEXT,
        job_id TEXT,
        number TEXT,
        invoice_date TIMESTAMPTZ,
        due_date TIMESTAMPTZ,
        status TEXT,
        subtotal NUMERIC,
        tax NUMERIC,
        total NUMERIC,
        balance NUMERIC,
        raw_json JSONB NOT NULL,
        synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_sf_invoices_customer_id ON sf_invoices(customer_id);
    CREATE INDEX IF NOT EXISTS idx_sf_invoices_job_id ON sf_invoices(job_id);
    CREATE INDEX IF NOT EXISTS idx_sf_invoices_date ON sf_invoices(invoice_date);

    CREATE TABLE IF NOT EXISTS sf_techs (
        sf_id TEXT PRIMARY KEY,
        name TEXT,
        email TEXT,
        phone TEXT,
        raw_json JSONB NOT NULL,
        synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS sf_job_statuses (
        sf_id TEXT PRIMARY KEY,
        name TEXT,
        raw_json JSONB NOT NULL,
        synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS sf_sources (
        sf_id TEXT PRIMARY KEY,
        name TEXT,
        raw_json JSONB NOT NULL,
        synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS sf_payment_types (
        sf_id TEXT PRIMARY KEY,
        name TEXT,
        raw_json JSONB NOT NULL,
        synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS sf_sync_state (
        resource TEXT PRIMARY KEY,
        last_page INTEGER,
        last_success_at TIMESTAMPTZ,
        total_count BIGINT,
        note TEXT
    );
    """
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()


def get_nested(d: Dict[str, Any], *keys: str):
    cur = d
    for key in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def first_nonempty(*vals):
    for v in vals:
        if v not in (None, "", []):
            return v
    return None


def get_id(obj: Dict[str, Any]) -> Optional[str]:
    val = first_nonempty(obj.get("id"), obj.get("customer_id"), obj.get("job_id"), obj.get("invoice_id"))
    return str(val) if val is not None else None


def parse_dt(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def upsert_customer(cur, row: Dict[str, Any]) -> bool:
    sf_id = get_id(row)
    if not sf_id:
        return False

    contacts = row.get("contacts") if isinstance(row.get("contacts"), list) else []
    locations = row.get("locations") if isinstance(row.get("locations"), list) else []
    contact0 = contacts[0] if contacts else {}
    loc0 = locations[0] if locations else {}

    phones = contact0.get("phones") if isinstance(contact0, dict) else []
    emails = contact0.get("emails") if isinstance(contact0, dict) else []

    phone = None
    email = None
    if isinstance(phones, list) and phones:
        p0 = phones[0]
        phone = p0.get("phone") if isinstance(p0, dict) else str(p0)
    if isinstance(emails, list) and emails:
        e0 = emails[0]
        email = e0.get("email") if isinstance(e0, dict) else str(e0)

    name = first_nonempty(row.get("customer_name"), row.get("name"), row.get("company_name"))
    city = first_nonempty(loc0.get("city") if isinstance(loc0, dict) else None, row.get("city"))
    state = first_nonempty(loc0.get("state_prov") if isinstance(loc0, dict) else None, row.get("state"))
    postal = first_nonempty(loc0.get("postal_code") if isinstance(loc0, dict) else None, row.get("postal_code"))

    cur.execute(
        """
        INSERT INTO sf_customers
        (sf_id, name, phone, email, city, state, postal_code, created_at_sf, updated_at_sf, raw_json, synced_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,NOW())
        ON CONFLICT (sf_id) DO UPDATE SET
          name=EXCLUDED.name,
          phone=EXCLUDED.phone,
          email=EXCLUDED.email,
          city=EXCLUDED.city,
          state=EXCLUDED.state,
          postal_code=EXCLUDED.postal_code,
          created_at_sf=EXCLUDED.created_at_sf,
          updated_at_sf=EXCLUDED.updated_at_sf,
          raw_json=EXCLUDED.raw_json,
          synced_at=NOW()
        """,
        (
            sf_id, name, phone, email, city, state, postal,
            parse_dt(row.get("created_at")), parse_dt(row.get("updated_at")),
            json.dumps(row),
        ),
    )
    return True


def upsert_job(cur, row: Dict[str, Any]) -> bool:
    sf_id = get_id(row)
    if not sf_id:
        return False
    cur.execute(
        """
        INSERT INTO sf_jobs
        (sf_id, customer_id, number, status, category, source, start_date, completion_date, total, raw_json, synced_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,NOW())
        ON CONFLICT (sf_id) DO UPDATE SET
          customer_id=EXCLUDED.customer_id,
          number=EXCLUDED.number,
          status=EXCLUDED.status,
          category=EXCLUDED.category,
          source=EXCLUDED.source,
          start_date=EXCLUDED.start_date,
          completion_date=EXCLUDED.completion_date,
          total=EXCLUDED.total,
          raw_json=EXCLUDED.raw_json,
          synced_at=NOW()
        """,
        (
            sf_id,
            str(first_nonempty(row.get("customer_id"), get_nested(row, "customer", "id")) or "") or None,
            first_nonempty(row.get("number"), row.get("job_number")),
            first_nonempty(get_nested(row, "status", "name"), row.get("status")),
            first_nonempty(get_nested(row, "category", "name"), row.get("category")),
            first_nonempty(get_nested(row, "source", "name"), row.get("source")),
            parse_dt(first_nonempty(row.get("start_date"), row.get("scheduled_start"))),
            parse_dt(first_nonempty(row.get("completion_date"), row.get("completed_at"))),
            first_nonempty(row.get("total"), row.get("total_amount")),
            json.dumps(row),
        ),
    )
    return True


def upsert_invoice(cur, row: Dict[str, Any]) -> bool:
    sf_id = get_id(row)
    if not sf_id:
        return False
    cur.execute(
        """
        INSERT INTO sf_invoices
        (sf_id, customer_id, job_id, number, invoice_date, due_date, status, subtotal, tax, total, balance, raw_json, synced_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,NOW())
        ON CONFLICT (sf_id) DO UPDATE SET
          customer_id=EXCLUDED.customer_id,
          job_id=EXCLUDED.job_id,
          number=EXCLUDED.number,
          invoice_date=EXCLUDED.invoice_date,
          due_date=EXCLUDED.due_date,
          status=EXCLUDED.status,
          subtotal=EXCLUDED.subtotal,
          tax=EXCLUDED.tax,
          total=EXCLUDED.total,
          balance=EXCLUDED.balance,
          raw_json=EXCLUDED.raw_json,
          synced_at=NOW()
        """,
        (
            sf_id,
            str(first_nonempty(row.get("customer_id"), get_nested(row, "customer", "id")) or "") or None,
            str(first_nonempty(row.get("job_id"), get_nested(row, "job", "id")) or "") or None,
            first_nonempty(row.get("number"), row.get("invoice_number")),
            parse_dt(first_nonempty(row.get("date"), row.get("invoice_date"))),
            parse_dt(row.get("due_date")),
            first_nonempty(get_nested(row, "status", "name"), row.get("status")),
            row.get("subtotal"),
            first_nonempty(row.get("tax"), row.get("tax_total")),
            row.get("total"),
            first_nonempty(row.get("balance"), row.get("balance_due")),
            json.dumps(row),
        ),
    )
    return True


def upsert_simple(cur, table: str, row: Dict[str, Any]) -> bool:
    sf_id = get_id(row)
    if not sf_id:
        return False
    name = first_nonempty(row.get("name"), row.get("label"), row.get("description"))
    cur.execute(
        f"""
        INSERT INTO {table} (sf_id, name, raw_json, synced_at)
        VALUES (%s,%s,%s::jsonb,NOW())
        ON CONFLICT (sf_id) DO UPDATE SET
          name=EXCLUDED.name,
          raw_json=EXCLUDED.raw_json,
          synced_at=NOW()
        """,
        (sf_id, name, json.dumps(row)),
    )
    return True


async def sync_resource(resource: str, max_pages: int = 1, per_page: int = 50) -> Dict[str, Any]:
    require_database()
    init_db()

    config = {
        "customers": {"path": "customers", "sort": None},
        "jobs": {"path": "jobs", "sort": "-start_date"},
        "invoices": {"path": "invoices", "sort": "-date"},
        "techs": {"path": "techs", "sort": None},
        "job-statuses": {"path": "job-statuses", "sort": None},
        "sources": {"path": "sources", "sort": None},
        "payment-types": {"path": "payment-types", "sort": None},
    }
    if resource not in config:
        raise HTTPException(status_code=400, detail=f"Unsupported resource: {resource}")

    path = config[resource]["path"]
    sort = config[resource]["sort"]
    inserted = 0
    pages_done = 0
    last_total = None

    with db_connect() as conn:
        for page in range(1, max_pages + 1):
            params = {"page": page, "per-page": per_page}
            if sort:
                params["sort"] = sort
            payload = await sf_get(path, params=params)
            items = extract_items(payload)
            meta = extract_meta(payload)
            last_total = meta.get("totalCount", last_total)

            if not items:
                break

            with conn.cursor() as cur:
                for row in items:
                    ok = False
                    if resource == "customers":
                        ok = upsert_customer(cur, row)
                    elif resource == "jobs":
                        ok = upsert_job(cur, row)
                    elif resource == "invoices":
                        ok = upsert_invoice(cur, row)
                    elif resource == "techs":
                        ok = upsert_simple(cur, "sf_techs", row)
                    elif resource == "job-statuses":
                        ok = upsert_simple(cur, "sf_job_statuses", row)
                    elif resource == "sources":
                        ok = upsert_simple(cur, "sf_sources", row)
                    elif resource == "payment-types":
                        ok = upsert_simple(cur, "sf_payment_types", row)
                    if ok:
                        inserted += 1

                cur.execute(
                    """
                    INSERT INTO sf_sync_state(resource, last_page, last_success_at, total_count, note)
                    VALUES (%s,%s,NOW(),%s,%s)
                    ON CONFLICT(resource) DO UPDATE SET
                      last_page=EXCLUDED.last_page,
                      last_success_at=NOW(),
                      total_count=EXCLUDED.total_count,
                      note=EXCLUDED.note
                    """,
                    (resource, page, last_total, f"Synced {inserted} rows in this run"),
                )
            conn.commit()
            pages_done += 1

            page_count = meta.get("pageCount")
            if page_count and page >= int(page_count):
                break

            await asyncio.sleep(1.05)

    return {
        "ok": True,
        "resource": resource,
        "pages_synced": pages_done,
        "rows_processed": inserted,
        "service_fusion_total": last_total,
    }


@app.get("/")
async def root():
    return {
        "ok": True,
        "service": "4YourPad Service Fusion Connector",
        "version": "1.2.0",
        "database_configured": bool(DATABASE_URL),
    }


@app.get("/health")
async def health():
    return {"ok": True}


@app.get("/test")
async def test(x_connector_key: Optional[str] = Header(None)):
    require_connector_key(x_connector_key)
    data = await sf_get("me")
    return {"ok": True, "service_fusion": data}


@app.post("/db/init")
async def db_init(x_connector_key: Optional[str] = Header(None)):
    require_connector_key(x_connector_key)
    init_db()
    return {"ok": True, "message": "Database tables created or already exist"}


@app.get("/db/status")
async def db_status(x_connector_key: Optional[str] = Header(None)):
    require_connector_key(x_connector_key)
    require_database()
    init_db()
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                  (SELECT COUNT(*) FROM sf_customers),
                  (SELECT COUNT(*) FROM sf_jobs),
                  (SELECT COUNT(*) FROM sf_invoices),
                  (SELECT COUNT(*) FROM sf_techs),
                  (SELECT COUNT(*) FROM sf_job_statuses),
                  (SELECT COUNT(*) FROM sf_sources),
                  (SELECT COUNT(*) FROM sf_payment_types)
            """)
            counts = cur.fetchone()
    return {
        "customers": counts[0],
        "jobs": counts[1],
        "invoices": counts[2],
        "techs": counts[3],
        "job_statuses": counts[4],
        "sources": counts[5],
        "payment_types": counts[6],
    }


@app.post("/sync/{resource}")
async def sync_endpoint(
    resource: str,
    max_pages: int = Query(1, ge=1, le=5000),
    per_page: int = Query(50, ge=1, le=50),
    x_connector_key: Optional[str] = Header(None),
):
    require_connector_key(x_connector_key)
    return await sync_resource(resource, max_pages=max_pages, per_page=per_page)


@app.get("/customers")
async def customers(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=50),
    x_connector_key: Optional[str] = Header(None),
):
    require_connector_key(x_connector_key)
    return await sf_get("customers", {"page": page, "per-page": per_page})


@app.get("/jobs")
async def jobs(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=50),
    x_connector_key: Optional[str] = Header(None),
):
    require_connector_key(x_connector_key)
    return await sf_get("jobs", {"page": page, "per-page": per_page, "sort": "-start_date"})


@app.get("/invoices")
async def invoices(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=50),
    x_connector_key: Optional[str] = Header(None),
):
    require_connector_key(x_connector_key)
    return await sf_get("invoices", {"page": page, "per-page": per_page, "sort": "-date"})


@app.get("/techs")
async def techs(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=50),
    x_connector_key: Optional[str] = Header(None),
):
    require_connector_key(x_connector_key)
    return await sf_get("techs", {"page": page, "per-page": per_page})


@app.get("/job-statuses")
async def job_statuses(x_connector_key: Optional[str] = Header(None)):
    require_connector_key(x_connector_key)
    return await sf_get("job-statuses")


@app.get("/sources")
async def sources(x_connector_key: Optional[str] = Header(None)):
    require_connector_key(x_connector_key)
    return await sf_get("sources")


@app.get("/payment-types")
async def payment_types(x_connector_key: Optional[str] = Header(None)):
    require_connector_key(x_connector_key)
    return await sf_get("payment-types")
