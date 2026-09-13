import os
import json
import time
import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple, List

import httpx
import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from fastapi import FastAPI, HTTPException, Header, Query

app = FastAPI(title="4 Your Pad Company Data Connector", version="1.6.0")

SF_BASE = os.getenv("SERVICE_FUSION_BASE", "https://api.servicefusion.com/v1").rstrip("/")
SF_TOKEN_URL = os.getenv("SERVICE_FUSION_TOKEN_URL", "https://api.servicefusion.com/oauth/access_token")
SF_CLIENT_ID = os.getenv("SERVICE_FUSION_CLIENT_ID", "")
SF_CLIENT_SECRET = os.getenv("SERVICE_FUSION_CLIENT_SECRET", "")
CONNECTOR_API_KEY = os.getenv("CONNECTOR_API_KEY", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")
STRIPE_RESTRICTED_KEY = os.getenv("STRIPE_RESTRICTED_KEY", "")
STRIPE_BASE = "https://api.stripe.com/v1"

_token: Dict[str, Any] = {}
_sync_tasks: Dict[str, asyncio.Task] = {}
_reconcile_tasks: Dict[str, asyncio.Task] = {}
_stripe_sync_tasks: Dict[str, asyncio.Task] = {}

RESOURCE_CONFIG = {
    "customers": {"path": "/customers", "sort": None, "table": "sf_customers"},
    "jobs": {"path": "/jobs", "sort": "-start_date", "table": "sf_jobs"},
    "invoices": {"path": "/invoices", "sort": "-date", "table": "sf_invoices"},
    "techs": {"path": "/techs", "sort": None, "table": "sf_techs"},
    "job-statuses": {"path": "/job-statuses", "sort": None, "table": "sf_job_statuses"},
    "sources": {"path": "/sources", "sort": None, "table": "sf_sources"},
    "payment-types": {"path": "/payment-types", "sort": None, "table": "sf_payment_types"},
}


STRIPE_RESOURCE_CONFIG = {
    "charges": {"path": "/charges", "table": "stripe_charges"},
    "balance-transactions": {"path": "/balance_transactions", "table": "stripe_balance_transactions"},
    "payouts": {"path": "/payouts", "table": "stripe_payouts"},
    "refunds": {"path": "/refunds", "table": "stripe_refunds"},
    "disputes": {"path": "/disputes", "table": "stripe_disputes"},
}


def utcnow():
    return datetime.now(timezone.utc)


def require_key(x_connector_key: Optional[str]):
    if not CONNECTOR_API_KEY:
        raise HTTPException(status_code=500, detail="CONNECTOR_API_KEY is not configured")
    if x_connector_key != CONNECTOR_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid connector key")


def get_conn():
    if not DATABASE_URL:
        raise HTTPException(status_code=500, detail="DATABASE_URL is not configured")
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


async def get_token() -> str:
    now = time.time()
    if _token.get("access_token") and _token.get("expires_at", 0) > now:
        return _token["access_token"]

    if not SF_CLIENT_ID or not SF_CLIENT_SECRET:
        raise HTTPException(status_code=500, detail="Service Fusion credentials are not configured")

    payload = {
        "grant_type": "client_credentials",
        "client_id": SF_CLIENT_ID,
        "client_secret": SF_CLIENT_SECRET,
    }

    async with httpx.AsyncClient(timeout=45) as client:
        r = await client.post(SF_TOKEN_URL, json=payload)

    if r.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"Service Fusion OAuth error {r.status_code}: {r.text[:500]}")

    data = r.json()
    access_token = data.get("access_token")
    if not access_token:
        raise HTTPException(status_code=502, detail="Service Fusion did not return an access token")

    expires_in = int(data.get("expires_in", 3600))
    _token.clear()
    _token.update({
        "access_token": access_token,
        "expires_at": now + max(60, expires_in - 60),
    })
    return access_token


async def sf_get(path: str, params: Optional[dict] = None) -> Any:
    token = await get_token()
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    url = f"{SF_BASE}{path}"

    async with httpx.AsyncClient(timeout=60) as client:
        try:
            r = await client.get(url, headers=headers, params=params or {})
        except (httpx.TimeoutException, httpx.RequestError):
            raise

    if r.status_code == 401:
        _token.clear()
        token = await get_token()
        headers["Authorization"] = f"Bearer {token}"
        async with httpx.AsyncClient(timeout=60) as client:
            r = await client.get(url, headers=headers, params=params or {})

    if r.status_code >= 400:
        raise HTTPException(
            status_code=502,
            detail=f"Service Fusion error {r.status_code}: {r.text[:500]}"
        )

    try:
        return r.json()
    except Exception:
        raise HTTPException(status_code=502, detail=f"Service Fusion returned non-JSON content: {r.text[:500]}")


async def sf_get_with_retry(path: str, params: Optional[dict] = None, note_cb=None) -> Any:
    retry_delays = [5, 10, 20, 30, 45, 60, 60, 60]
    last_error = None

    for attempt in range(len(retry_delays) + 1):
        try:
            return await sf_get(path, params)
        except HTTPException as e:
            last_error = e
            retryable = e.status_code in (429, 502, 503, 504)
            if not retryable or attempt >= len(retry_delays):
                raise
            delay = retry_delays[attempt]
            if note_cb:
                note_cb(f"Temporary Service Fusion error; retry {attempt + 1}/{len(retry_delays)} in {delay}s")
            await asyncio.sleep(delay)
        except (httpx.TimeoutException, httpx.RequestError) as e:
            last_error = e
            if attempt >= len(retry_delays):
                raise
            delay = retry_delays[attempt]
            if note_cb:
                note_cb(f"Temporary connection error; retry {attempt + 1}/{len(retry_delays)} in {delay}s")
            await asyncio.sleep(delay)

    raise last_error or RuntimeError("Unable to fetch Service Fusion data")


def extract_items(payload: Any) -> Tuple[List[dict], Optional[int]]:
    if isinstance(payload, list):
        return payload, None

    if not isinstance(payload, dict):
        return [], None

    items = None
    for key in ("items", "data", "results"):
        if isinstance(payload.get(key), list):
            items = payload[key]
            break
    if items is None:
        items = []

    meta = payload.get("_meta") or payload.get("meta") or {}
    total = None
    for key in ("totalCount", "total_count", "total"):
        value = meta.get(key) if isinstance(meta, dict) else None
        if value is None:
            value = payload.get(key)
        if value is not None:
            try:
                total = int(value)
                break
            except Exception:
                pass

    return items, total


def pick(obj: dict, *keys, default=None):
    for key in keys:
        value = obj.get(key)
        if value not in (None, ""):
            return value
    return default


def nested_name(value):
    if isinstance(value, dict):
        return pick(value, "name", "label", "value", "title")
    return value


def nested_id(value):
    if isinstance(value, dict):
        return pick(value, "id", "sf_id", "job_id", "customer_id")
    return value


def normalize_money(value):
    if isinstance(value, dict):
        value = pick(value, "amount", "value", "total")
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def init_db():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
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
        )
        """)

        cur.execute("""
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
        )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sf_jobs_customer_id ON sf_jobs(customer_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sf_jobs_start_date ON sf_jobs(start_date)")

        cur.execute("""
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
        )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sf_invoices_customer_id ON sf_invoices(customer_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sf_invoices_job_id ON sf_invoices(job_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sf_invoices_invoice_date ON sf_invoices(invoice_date)")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS sf_techs (
            sf_id TEXT PRIMARY KEY,
            name TEXT,
            email TEXT,
            phone TEXT,
            raw_json JSONB NOT NULL,
            synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """)

        for table in ("sf_job_statuses", "sf_sources", "sf_payment_types"):
            cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                sf_id TEXT PRIMARY KEY,
                name TEXT,
                raw_json JSONB NOT NULL,
                synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS stripe_charges (
            stripe_id TEXT PRIMARY KEY,
            amount BIGINT,
            amount_refunded BIGINT,
            currency TEXT,
            created_at_stripe TIMESTAMPTZ,
            balance_transaction_id TEXT,
            customer_id TEXT,
            payment_intent_id TEXT,
            paid BOOLEAN,
            captured BOOLEAN,
            refunded BOOLEAN,
            status TEXT,
            description TEXT,
            raw_json JSONB NOT NULL,
            synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_stripe_charges_created ON stripe_charges(created_at_stripe)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_stripe_charges_customer ON stripe_charges(customer_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_stripe_charges_payment_intent ON stripe_charges(payment_intent_id)")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS stripe_balance_transactions (
            stripe_id TEXT PRIMARY KEY,
            amount BIGINT,
            fee BIGINT,
            net BIGINT,
            currency TEXT,
            created_at_stripe TIMESTAMPTZ,
            available_on TIMESTAMPTZ,
            type TEXT,
            reporting_category TEXT,
            source_id TEXT,
            status TEXT,
            description TEXT,
            raw_json JSONB NOT NULL,
            synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_stripe_bt_created ON stripe_balance_transactions(created_at_stripe)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_stripe_bt_source ON stripe_balance_transactions(source_id)")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS stripe_payouts (
            stripe_id TEXT PRIMARY KEY,
            amount BIGINT,
            currency TEXT,
            created_at_stripe TIMESTAMPTZ,
            arrival_date TIMESTAMPTZ,
            status TEXT,
            type TEXT,
            method TEXT,
            automatic BOOLEAN,
            balance_transaction_id TEXT,
            description TEXT,
            statement_descriptor TEXT,
            failure_code TEXT,
            failure_message TEXT,
            raw_json JSONB NOT NULL,
            synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_stripe_payouts_created ON stripe_payouts(created_at_stripe)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_stripe_payouts_arrival ON stripe_payouts(arrival_date)")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS stripe_refunds (
            stripe_id TEXT PRIMARY KEY,
            amount BIGINT,
            currency TEXT,
            created_at_stripe TIMESTAMPTZ,
            charge_id TEXT,
            payment_intent_id TEXT,
            balance_transaction_id TEXT,
            status TEXT,
            reason TEXT,
            raw_json JSONB NOT NULL,
            synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_stripe_refunds_created ON stripe_refunds(created_at_stripe)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_stripe_refunds_charge ON stripe_refunds(charge_id)")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS stripe_disputes (
            stripe_id TEXT PRIMARY KEY,
            amount BIGINT,
            currency TEXT,
            created_at_stripe TIMESTAMPTZ,
            charge_id TEXT,
            payment_intent_id TEXT,
            reason TEXT,
            status TEXT,
            is_charge_refundable BOOLEAN,
            raw_json JSONB NOT NULL,
            synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_stripe_disputes_created ON stripe_disputes(created_at_stripe)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_stripe_disputes_charge ON stripe_disputes(charge_id)")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS stripe_sync_state (
            resource TEXT PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'idle',
            rows_processed INTEGER NOT NULL DEFAULT 0,
            last_object_id TEXT,
            last_success_at TIMESTAMPTZ,
            error TEXT,
            note TEXT
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS sf_sync_state (
            resource TEXT PRIMARY KEY,
            last_page INTEGER NOT NULL DEFAULT 0,
            last_success_at TIMESTAMPTZ,
            total_count INTEGER,
            rows_processed INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'idle',
            error TEXT,
            note TEXT
        )
        """)
        cur.execute("ALTER TABLE sf_sync_state ADD COLUMN IF NOT EXISTS rows_processed INTEGER NOT NULL DEFAULT 0")
        cur.execute("ALTER TABLE sf_sync_state ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'idle'")
        cur.execute("ALTER TABLE sf_sync_state ADD COLUMN IF NOT EXISTS error TEXT")
        cur.execute("ALTER TABLE sf_sync_state ADD COLUMN IF NOT EXISTS note TEXT")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS sf_reconcile_state (
            resource TEXT PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'idle',
            target_count INTEGER,
            db_count_start INTEGER,
            db_count_current INTEGER,
            rows_added INTEGER NOT NULL DEFAULT 0,
            last_sort TEXT,
            last_page INTEGER NOT NULL DEFAULT 0,
            passes_completed INTEGER NOT NULL DEFAULT 0,
            last_success_at TIMESTAMPTZ,
            error TEXT,
            note TEXT
        )
        """)
        conn.commit()


def get_state(resource: str) -> dict:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM sf_sync_state WHERE resource=%s", (resource,))
        row = cur.fetchone()
        if row:
            return dict(row)
        cur.execute("INSERT INTO sf_sync_state(resource) VALUES (%s) ON CONFLICT DO NOTHING", (resource,))
        conn.commit()
        return {
            "resource": resource, "last_page": 0, "last_success_at": None,
            "total_count": None, "rows_processed": 0, "status": "idle",
            "error": None, "note": None
        }


def update_state(resource: str, **kwargs):
    current = get_state(resource)
    values = {
        "last_page": kwargs.get("last_page", current.get("last_page", 0)),
        "last_success_at": kwargs.get("last_success_at", current.get("last_success_at")),
        "total_count": kwargs.get("total_count", current.get("total_count")),
        "rows_processed": kwargs.get("rows_processed", current.get("rows_processed", 0)),
        "status": kwargs.get("status", current.get("status", "idle")),
        "error": kwargs["error"] if "error" in kwargs else current.get("error"),
        "note": kwargs["note"] if "note" in kwargs else current.get("note"),
    }
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO sf_sync_state
                (resource,last_page,last_success_at,total_count,rows_processed,status,error,note)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT(resource) DO UPDATE SET
                last_page=EXCLUDED.last_page,
                last_success_at=EXCLUDED.last_success_at,
                total_count=EXCLUDED.total_count,
                rows_processed=EXCLUDED.rows_processed,
                status=EXCLUDED.status,
                error=EXCLUDED.error,
                note=EXCLUDED.note
        """, (
            resource, values["last_page"], values["last_success_at"], values["total_count"],
            values["rows_processed"], values["status"], values["error"], values["note"]
        ))
        conn.commit()


def get_reconcile_state(resource="jobs") -> dict:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM sf_reconcile_state WHERE resource=%s", (resource,))
        row = cur.fetchone()
        if row:
            return dict(row)
        cur.execute("INSERT INTO sf_reconcile_state(resource) VALUES (%s) ON CONFLICT DO NOTHING", (resource,))
        conn.commit()
        cur.execute("SELECT * FROM sf_reconcile_state WHERE resource=%s", (resource,))
        return dict(cur.fetchone())


def update_reconcile_state(resource="jobs", **kwargs):
    current = get_reconcile_state(resource)
    fields = {
        "status": kwargs.get("status", current.get("status", "idle")),
        "target_count": kwargs.get("target_count", current.get("target_count")),
        "db_count_start": kwargs.get("db_count_start", current.get("db_count_start")),
        "db_count_current": kwargs.get("db_count_current", current.get("db_count_current")),
        "rows_added": kwargs.get("rows_added", current.get("rows_added", 0)),
        "last_sort": kwargs.get("last_sort", current.get("last_sort")),
        "last_page": kwargs.get("last_page", current.get("last_page", 0)),
        "passes_completed": kwargs.get("passes_completed", current.get("passes_completed", 0)),
        "last_success_at": kwargs.get("last_success_at", current.get("last_success_at")),
        "error": kwargs["error"] if "error" in kwargs else current.get("error"),
        "note": kwargs["note"] if "note" in kwargs else current.get("note"),
    }
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO sf_reconcile_state
            (resource,status,target_count,db_count_start,db_count_current,rows_added,last_sort,last_page,
             passes_completed,last_success_at,error,note)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT(resource) DO UPDATE SET
                status=EXCLUDED.status,
                target_count=EXCLUDED.target_count,
                db_count_start=EXCLUDED.db_count_start,
                db_count_current=EXCLUDED.db_count_current,
                rows_added=EXCLUDED.rows_added,
                last_sort=EXCLUDED.last_sort,
                last_page=EXCLUDED.last_page,
                passes_completed=EXCLUDED.passes_completed,
                last_success_at=EXCLUDED.last_success_at,
                error=EXCLUDED.error,
                note=EXCLUDED.note
        """, (
            resource, fields["status"], fields["target_count"], fields["db_count_start"],
            fields["db_count_current"], fields["rows_added"], fields["last_sort"],
            fields["last_page"], fields["passes_completed"], fields["last_success_at"],
            fields["error"], fields["note"]
        ))
        conn.commit()


def count_table(table: str) -> int:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) AS n FROM {table}")
        return int(cur.fetchone()["n"])


def upsert_record(resource: str, rec: dict):
    sf_id = pick(rec, "id", "sf_id")
    if sf_id is None:
        return False
    sf_id = str(sf_id)

    with get_conn() as conn, conn.cursor() as cur:
        if resource == "customers":
            name = pick(rec, "name", "company_name", "display_name")
            if not name:
                first = pick(rec, "first_name", "firstName", default="")
                last = pick(rec, "last_name", "lastName", default="")
                name = (f"{first} {last}").strip() or None
            phone = pick(rec, "phone", "phone_number", "primary_phone")
            email = pick(rec, "email", "email_address", "primary_email")
            address = rec.get("address") if isinstance(rec.get("address"), dict) else {}
            city = pick(rec, "city") or pick(address, "city")
            state = pick(rec, "state") or pick(address, "state")
            postal = pick(rec, "postal_code", "zip", "zip_code") or pick(address, "postal_code", "zip", "zip_code")
            created = pick(rec, "created_at", "created", "created_date")
            updated = pick(rec, "updated_at", "updated", "modified_date")
            cur.execute("""
                INSERT INTO sf_customers
                (sf_id,name,phone,email,city,state,postal_code,created_at_sf,updated_at_sf,raw_json,synced_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                ON CONFLICT(sf_id) DO UPDATE SET
                    name=EXCLUDED.name, phone=EXCLUDED.phone, email=EXCLUDED.email,
                    city=EXCLUDED.city, state=EXCLUDED.state, postal_code=EXCLUDED.postal_code,
                    created_at_sf=EXCLUDED.created_at_sf, updated_at_sf=EXCLUDED.updated_at_sf,
                    raw_json=EXCLUDED.raw_json, synced_at=NOW()
            """, (sf_id, name, phone, email, city, state, postal, created, updated, Jsonb(rec)))

        elif resource == "jobs":
            customer_id = nested_id(pick(rec, "customer_id", "customer"))
            number = pick(rec, "number", "job_number", "job_no")
            status = nested_name(pick(rec, "status", "job_status"))
            category = nested_name(pick(rec, "category", "job_category"))
            source = nested_name(pick(rec, "source", "lead_source"))
            start_date = pick(rec, "start_date", "scheduled_date", "date")
            completion_date = pick(rec, "completion_date", "completed_at", "completed_date")
            total = normalize_money(pick(rec, "total", "grand_total", "amount"))
            cur.execute("""
                INSERT INTO sf_jobs
                (sf_id,customer_id,number,status,category,source,start_date,completion_date,total,raw_json,synced_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                ON CONFLICT(sf_id) DO UPDATE SET
                    customer_id=EXCLUDED.customer_id, number=EXCLUDED.number,
                    status=EXCLUDED.status, category=EXCLUDED.category, source=EXCLUDED.source,
                    start_date=EXCLUDED.start_date, completion_date=EXCLUDED.completion_date,
                    total=EXCLUDED.total, raw_json=EXCLUDED.raw_json, synced_at=NOW()
            """, (
                sf_id, str(customer_id) if customer_id is not None else None, number, status,
                category, source, start_date, completion_date, total, Jsonb(rec)
            ))

        elif resource == "invoices":
            customer_id = nested_id(pick(rec, "customer_id", "customer"))
            job_id = nested_id(pick(rec, "job_id", "job"))
            number = pick(rec, "number", "invoice_number", "invoice_no")
            invoice_date = pick(rec, "date", "invoice_date", "created_at")
            due_date = pick(rec, "due_date")
            status = nested_name(pick(rec, "status"))
            subtotal = normalize_money(pick(rec, "subtotal"))
            tax = normalize_money(pick(rec, "tax", "tax_total"))
            total = normalize_money(pick(rec, "total", "grand_total"))
            balance = normalize_money(pick(rec, "balance", "amount_due"))
            cur.execute("""
                INSERT INTO sf_invoices
                (sf_id,customer_id,job_id,number,invoice_date,due_date,status,subtotal,tax,total,balance,raw_json,synced_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                ON CONFLICT(sf_id) DO UPDATE SET
                    customer_id=EXCLUDED.customer_id, job_id=EXCLUDED.job_id, number=EXCLUDED.number,
                    invoice_date=EXCLUDED.invoice_date, due_date=EXCLUDED.due_date,
                    status=EXCLUDED.status, subtotal=EXCLUDED.subtotal, tax=EXCLUDED.tax,
                    total=EXCLUDED.total, balance=EXCLUDED.balance,
                    raw_json=EXCLUDED.raw_json, synced_at=NOW()
            """, (
                sf_id, str(customer_id) if customer_id is not None else None,
                str(job_id) if job_id is not None else None, number, invoice_date, due_date,
                status, subtotal, tax, total, balance, Jsonb(rec)
            ))

        elif resource == "techs":
            name = pick(rec, "name", "display_name")
            if not name:
                first = pick(rec, "first_name", "firstName", default="")
                last = pick(rec, "last_name", "lastName", default="")
                name = (f"{first} {last}").strip() or None
            email = pick(rec, "email", "email_address")
            phone = pick(rec, "phone", "phone_number")
            cur.execute("""
                INSERT INTO sf_techs(sf_id,name,email,phone,raw_json,synced_at)
                VALUES (%s,%s,%s,%s,%s,NOW())
                ON CONFLICT(sf_id) DO UPDATE SET
                    name=EXCLUDED.name, email=EXCLUDED.email, phone=EXCLUDED.phone,
                    raw_json=EXCLUDED.raw_json, synced_at=NOW()
            """, (sf_id, name, email, phone, Jsonb(rec)))

        else:
            table = RESOURCE_CONFIG[resource]["table"]
            name = nested_name(pick(rec, "name", "label", "title"))
            cur.execute(f"""
                INSERT INTO {table}(sf_id,name,raw_json,synced_at)
                VALUES (%s,%s,%s,NOW())
                ON CONFLICT(sf_id) DO UPDATE SET
                    name=EXCLUDED.name, raw_json=EXCLUDED.raw_json, synced_at=NOW()
            """, (sf_id, name, Jsonb(rec)))

        conn.commit()
    return True


def upsert_records(resource: str, items: List[dict]) -> int:
    count = 0
    for rec in items:
        if isinstance(rec, dict) and upsert_record(resource, rec):
            count += 1
    return count


async def run_import(resource: str, reset: bool = False, per_page: int = 50):
    cfg = RESOURCE_CONFIG[resource]

    try:
        init_db()

        if reset:
            update_state(
                resource, last_page=0, rows_processed=0, status="idle",
                error=None, note="Reset for full import"
            )

        state = get_state(resource)
        page = int(state.get("last_page") or 0) + 1
        processed = int(state.get("rows_processed") or 0)
        total_count = state.get("total_count")

        update_state(resource, status="running", error=None, note=f"Starting page {page}")

        while True:
            params = {"page": page, "per-page": per_page}
            if cfg.get("sort"):
                params["sort"] = cfg["sort"]

            def note_cb(msg):
                update_state(resource, status="running", error=None, note=f"{msg} on page {page}")

            payload = await sf_get_with_retry(cfg["path"], params, note_cb)
            items, total = extract_items(payload)

            if total is not None:
                total_count = total

            if not items:
                update_state(
                    resource, last_page=max(0, page - 1), rows_processed=processed,
                    total_count=total_count, status="complete", error=None,
                    note="Import complete", last_success_at=utcnow()
                )
                break

            saved = upsert_records(resource, items)
            processed += saved

            update_state(
                resource, last_page=page, rows_processed=processed,
                total_count=total_count, status="running", error=None,
                note=f"Completed page {page}", last_success_at=utcnow()
            )

            if len(items) < per_page:
                update_state(
                    resource, last_page=page, rows_processed=processed,
                    total_count=total_count, status="complete", error=None,
                    note="Import complete", last_success_at=utcnow()
                )
                break

            if total_count is not None and page * per_page >= total_count:
                update_state(
                    resource, last_page=page, rows_processed=processed,
                    total_count=total_count, status="complete", error=None,
                    note="Import complete", last_success_at=utcnow()
                )
                break

            page += 1
            await asyncio.sleep(1.05)

    except Exception as e:
        update_state(
            resource, status="error", error=str(e),
            note=f"Stopped after page {max(0, locals().get('page', 1) - 1)}"
        )
    finally:
        _sync_tasks.pop(resource, None)


async def recover_invoice_linked_jobs() -> int:
    """Fetch jobs directly by ID when an invoice references a job that is not yet in sf_jobs."""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT i.job_id::text AS job_id
            FROM sf_invoices i
            LEFT JOIN sf_jobs j ON j.sf_id::text = i.job_id::text
            WHERE i.job_id IS NOT NULL
              AND i.job_id::text <> ''
              AND j.sf_id IS NULL
            ORDER BY i.job_id::text
        """)
        ids = [r["job_id"] for r in cur.fetchall()]

    recovered = 0
    for idx, job_id in enumerate(ids, start=1):
        def note_cb(msg):
            update_reconcile_state(
                "jobs", status="running",
                note=f"Direct invoice-linked job {idx}/{len(ids)} ({job_id}): {msg}"
            )

        try:
            payload = await sf_get_with_retry(f"/jobs/{job_id}", None, note_cb)
            if isinstance(payload, dict):
                # Some APIs wrap a single result in data/item.
                rec = payload.get("data") if isinstance(payload.get("data"), dict) else payload
                rec = payload.get("item") if isinstance(payload.get("item"), dict) else rec
                before = count_table("sf_jobs")
                upsert_record("jobs", rec)
                after = count_table("sf_jobs")
                if after > before:
                    recovered += 1
        except HTTPException as e:
            # A referenced job may have been deleted/archived in Service Fusion.
            update_reconcile_state(
                "jobs", status="running",
                note=f"Could not fetch invoice-linked job {job_id}; continuing"
            )

        await asyncio.sleep(1.05)

    return recovered


async def reconcile_job_scan(sort_value: str, per_page: int, target_count: Optional[int], pass_number: int) -> Tuple[int, Optional[int]]:
    """Scan jobs in a chosen order. Reverse ordering helps recover IDs skipped by moving pagination."""
    page = 1
    last_total = target_count

    while True:
        params = {"page": page, "per-page": per_page, "sort": sort_value}

        def note_cb(msg):
            update_reconcile_state(
                "jobs", status="running", last_sort=sort_value, last_page=page,
                note=f"Pass {pass_number}, sort {sort_value}, page {page}: {msg}"
            )

        payload = await sf_get_with_retry("/jobs", params, note_cb)
        items, total = extract_items(payload)
        if total is not None:
            last_total = total

        if not items:
            break

        upsert_records("jobs", items)
        db_now = count_table("sf_jobs")
        state = get_reconcile_state("jobs")
        update_reconcile_state(
            "jobs",
            status="running",
            target_count=last_total,
            db_count_current=db_now,
            rows_added=max(0, db_now - int(state.get("db_count_start") or db_now)),
            last_sort=sort_value,
            last_page=page,
            note=f"Pass {pass_number}: completed {sort_value} page {page}; database has {db_now} unique jobs",
            last_success_at=utcnow(),
            error=None,
        )

        if last_total is not None and db_now >= last_total:
            return db_now, last_total

        if len(items) < per_page:
            break
        if last_total is not None and page * per_page >= last_total:
            break

        page += 1
        await asyncio.sleep(1.05)

    return count_table("sf_jobs"), last_total


async def run_jobs_reconcile(per_page: int = 50):
    """
    v1.5 reconciliation strategy:
      1) Directly recover invoice-referenced jobs that are missing.
      2) Scan /jobs oldest-to-newest (sort=start_date).
      3) If still short, scan newest-to-oldest (sort=-start_date).
      4) If still short, make one more oldest-to-newest pass.
    Existing rows are never deleted; every job is upserted by Service Fusion ID.
    """
    try:
        init_db()
        start_count = count_table("sf_jobs")

        update_reconcile_state(
            "jobs",
            status="running",
            target_count=None,
            db_count_start=start_count,
            db_count_current=start_count,
            rows_added=0,
            last_sort=None,
            last_page=0,
            passes_completed=0,
            last_success_at=utcnow(),
            error=None,
            note=f"Reconciliation started with {start_count} unique jobs"
        )

        recovered_direct = await recover_invoice_linked_jobs()
        db_now = count_table("sf_jobs")
        update_reconcile_state(
            "jobs",
            status="running",
            db_count_current=db_now,
            rows_added=db_now - start_count,
            note=f"Direct recovery finished; added {recovered_direct} invoice-linked jobs. Starting reverse-order scan."
        )

        target = None
        passes = ["start_date", "-start_date", "start_date"]

        for pass_no, sort_value in enumerate(passes, start=1):
            before_pass = count_table("sf_jobs")
            db_now, target = await reconcile_job_scan(sort_value, per_page, target, pass_no)
            added_this_pass = db_now - before_pass

            update_reconcile_state(
                "jobs",
                status="running",
                target_count=target,
                db_count_current=db_now,
                rows_added=db_now - start_count,
                passes_completed=pass_no,
                note=f"Pass {pass_no} complete using {sort_value}; added {added_this_pass} unique jobs",
                last_success_at=utcnow(),
                error=None,
            )

            if target is not None and db_now >= target:
                update_reconcile_state(
                    "jobs",
                    status="complete",
                    target_count=target,
                    db_count_current=db_now,
                    rows_added=db_now - start_count,
                    passes_completed=pass_no,
                    note="Reconciliation complete; database reached Service Fusion total",
                    last_success_at=utcnow(),
                    error=None,
                )
                return

            # If the second scan adds nothing, a third scan is unlikely to help much,
            # but we still allow the planned third pass because the source is live.
            await asyncio.sleep(2)

        db_now = count_table("sf_jobs")
        if target is not None and db_now >= target:
            status = "complete"
            note = "Reconciliation complete; database reached Service Fusion total"
        else:
            status = "needs_review"
            gap = (target - db_now) if target is not None else None
            note = (
                f"Reconciliation passes finished; {gap} jobs still not captured. "
                "Do not run blind full resets; use targeted review next."
                if gap is not None else
                "Reconciliation passes finished; target total unavailable."
            )

        update_reconcile_state(
            "jobs",
            status=status,
            target_count=target,
            db_count_current=db_now,
            rows_added=db_now - start_count,
            passes_completed=len(passes),
            last_success_at=utcnow(),
            error=None,
            note=note,
        )

    except Exception as e:
        update_reconcile_state(
            "jobs",
            status="error",
            db_count_current=count_table("sf_jobs") if DATABASE_URL else None,
            error=str(e),
            note="Jobs reconciliation stopped with an error"
        )
    finally:
        _reconcile_tasks.pop("jobs", None)



def stripe_ts(value):
    if value in (None, ""):
        return None
    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc)
    except Exception:
        return None


async def stripe_get(path: str, params: Optional[dict] = None) -> Any:
    if not STRIPE_RESTRICTED_KEY:
        raise HTTPException(status_code=500, detail="STRIPE_RESTRICTED_KEY is not configured")

    url = f"{STRIPE_BASE}{path}"
    retry_delays = [2, 5, 10, 20, 30]
    last_error = None

    for attempt in range(len(retry_delays) + 1):
        try:
            async with httpx.AsyncClient(timeout=60) as client:
                r = await client.get(
                    url,
                    params=params or {},
                    auth=(STRIPE_RESTRICTED_KEY, "")
                )
        except (httpx.TimeoutException, httpx.RequestError) as e:
            last_error = e
            if attempt >= len(retry_delays):
                raise HTTPException(status_code=502, detail=f"Stripe connection error: {e}")
            await asyncio.sleep(retry_delays[attempt])
            continue

        if r.status_code < 400:
            try:
                return r.json()
            except Exception:
                raise HTTPException(status_code=502, detail="Stripe returned invalid JSON")

        last_error = HTTPException(
            status_code=502,
            detail=f"Stripe error {r.status_code}: {r.text[:500]}"
        )

        if r.status_code not in (429, 500, 502, 503, 504) or attempt >= len(retry_delays):
            raise last_error

        await asyncio.sleep(retry_delays[attempt])

    raise last_error or HTTPException(status_code=502, detail="Stripe request failed")


def get_stripe_state(resource: str) -> dict:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM stripe_sync_state WHERE resource=%s", (resource,))
        row = cur.fetchone()
        if row:
            return dict(row)
        cur.execute(
            "INSERT INTO stripe_sync_state(resource) VALUES (%s) ON CONFLICT DO NOTHING",
            (resource,)
        )
        conn.commit()
        cur.execute("SELECT * FROM stripe_sync_state WHERE resource=%s", (resource,))
        return dict(cur.fetchone())


def update_stripe_state(resource: str, **kwargs):
    current = get_stripe_state(resource)
    fields = {
        "status": kwargs.get("status", current.get("status", "idle")),
        "rows_processed": kwargs.get("rows_processed", current.get("rows_processed", 0)),
        "last_object_id": kwargs.get("last_object_id", current.get("last_object_id")),
        "last_success_at": kwargs.get("last_success_at", current.get("last_success_at")),
        "error": kwargs["error"] if "error" in kwargs else current.get("error"),
        "note": kwargs["note"] if "note" in kwargs else current.get("note"),
    }
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO stripe_sync_state
                (resource,status,rows_processed,last_object_id,last_success_at,error,note)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT(resource) DO UPDATE SET
                status=EXCLUDED.status,
                rows_processed=EXCLUDED.rows_processed,
                last_object_id=EXCLUDED.last_object_id,
                last_success_at=EXCLUDED.last_success_at,
                error=EXCLUDED.error,
                note=EXCLUDED.note
        """, (
            resource, fields["status"], fields["rows_processed"],
            fields["last_object_id"], fields["last_success_at"],
            fields["error"], fields["note"]
        ))
        conn.commit()


def upsert_stripe_record(resource: str, rec: dict):
    sid = rec.get("id")
    if not sid:
        return False

    with get_conn() as conn, conn.cursor() as cur:
        if resource == "charges":
            cur.execute("""
                INSERT INTO stripe_charges
                (stripe_id,amount,amount_refunded,currency,created_at_stripe,
                 balance_transaction_id,customer_id,payment_intent_id,paid,captured,
                 refunded,status,description,raw_json,synced_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                ON CONFLICT(stripe_id) DO UPDATE SET
                    amount=EXCLUDED.amount,
                    amount_refunded=EXCLUDED.amount_refunded,
                    currency=EXCLUDED.currency,
                    created_at_stripe=EXCLUDED.created_at_stripe,
                    balance_transaction_id=EXCLUDED.balance_transaction_id,
                    customer_id=EXCLUDED.customer_id,
                    payment_intent_id=EXCLUDED.payment_intent_id,
                    paid=EXCLUDED.paid,
                    captured=EXCLUDED.captured,
                    refunded=EXCLUDED.refunded,
                    status=EXCLUDED.status,
                    description=EXCLUDED.description,
                    raw_json=EXCLUDED.raw_json,
                    synced_at=NOW()
            """, (
                sid, rec.get("amount"), rec.get("amount_refunded"), rec.get("currency"),
                stripe_ts(rec.get("created")), rec.get("balance_transaction"),
                rec.get("customer"), rec.get("payment_intent"),
                rec.get("paid"), rec.get("captured"), rec.get("refunded"),
                rec.get("status"), rec.get("description"), Jsonb(rec)
            ))

        elif resource == "balance-transactions":
            cur.execute("""
                INSERT INTO stripe_balance_transactions
                (stripe_id,amount,fee,net,currency,created_at_stripe,available_on,
                 type,reporting_category,source_id,status,description,raw_json,synced_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                ON CONFLICT(stripe_id) DO UPDATE SET
                    amount=EXCLUDED.amount,
                    fee=EXCLUDED.fee,
                    net=EXCLUDED.net,
                    currency=EXCLUDED.currency,
                    created_at_stripe=EXCLUDED.created_at_stripe,
                    available_on=EXCLUDED.available_on,
                    type=EXCLUDED.type,
                    reporting_category=EXCLUDED.reporting_category,
                    source_id=EXCLUDED.source_id,
                    status=EXCLUDED.status,
                    description=EXCLUDED.description,
                    raw_json=EXCLUDED.raw_json,
                    synced_at=NOW()
            """, (
                sid, rec.get("amount"), rec.get("fee"), rec.get("net"), rec.get("currency"),
                stripe_ts(rec.get("created")), stripe_ts(rec.get("available_on")),
                rec.get("type"), rec.get("reporting_category"), rec.get("source"),
                rec.get("status"), rec.get("description"), Jsonb(rec)
            ))

        elif resource == "payouts":
            cur.execute("""
                INSERT INTO stripe_payouts
                (stripe_id,amount,currency,created_at_stripe,arrival_date,status,type,method,
                 automatic,balance_transaction_id,description,statement_descriptor,
                 failure_code,failure_message,raw_json,synced_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                ON CONFLICT(stripe_id) DO UPDATE SET
                    amount=EXCLUDED.amount,
                    currency=EXCLUDED.currency,
                    created_at_stripe=EXCLUDED.created_at_stripe,
                    arrival_date=EXCLUDED.arrival_date,
                    status=EXCLUDED.status,
                    type=EXCLUDED.type,
                    method=EXCLUDED.method,
                    automatic=EXCLUDED.automatic,
                    balance_transaction_id=EXCLUDED.balance_transaction_id,
                    description=EXCLUDED.description,
                    statement_descriptor=EXCLUDED.statement_descriptor,
                    failure_code=EXCLUDED.failure_code,
                    failure_message=EXCLUDED.failure_message,
                    raw_json=EXCLUDED.raw_json,
                    synced_at=NOW()
            """, (
                sid, rec.get("amount"), rec.get("currency"), stripe_ts(rec.get("created")),
                stripe_ts(rec.get("arrival_date")), rec.get("status"), rec.get("type"),
                rec.get("method"), rec.get("automatic"), rec.get("balance_transaction"),
                rec.get("description"), rec.get("statement_descriptor"),
                rec.get("failure_code"), rec.get("failure_message"), Jsonb(rec)
            ))

        elif resource == "refunds":
            cur.execute("""
                INSERT INTO stripe_refunds
                (stripe_id,amount,currency,created_at_stripe,charge_id,payment_intent_id,
                 balance_transaction_id,status,reason,raw_json,synced_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                ON CONFLICT(stripe_id) DO UPDATE SET
                    amount=EXCLUDED.amount,
                    currency=EXCLUDED.currency,
                    created_at_stripe=EXCLUDED.created_at_stripe,
                    charge_id=EXCLUDED.charge_id,
                    payment_intent_id=EXCLUDED.payment_intent_id,
                    balance_transaction_id=EXCLUDED.balance_transaction_id,
                    status=EXCLUDED.status,
                    reason=EXCLUDED.reason,
                    raw_json=EXCLUDED.raw_json,
                    synced_at=NOW()
            """, (
                sid, rec.get("amount"), rec.get("currency"), stripe_ts(rec.get("created")),
                rec.get("charge"), rec.get("payment_intent"), rec.get("balance_transaction"),
                rec.get("status"), rec.get("reason"), Jsonb(rec)
            ))

        elif resource == "disputes":
            cur.execute("""
                INSERT INTO stripe_disputes
                (stripe_id,amount,currency,created_at_stripe,charge_id,payment_intent_id,
                 reason,status,is_charge_refundable,raw_json,synced_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                ON CONFLICT(stripe_id) DO UPDATE SET
                    amount=EXCLUDED.amount,
                    currency=EXCLUDED.currency,
                    created_at_stripe=EXCLUDED.created_at_stripe,
                    charge_id=EXCLUDED.charge_id,
                    payment_intent_id=EXCLUDED.payment_intent_id,
                    reason=EXCLUDED.reason,
                    status=EXCLUDED.status,
                    is_charge_refundable=EXCLUDED.is_charge_refundable,
                    raw_json=EXCLUDED.raw_json,
                    synced_at=NOW()
            """, (
                sid, rec.get("amount"), rec.get("currency"), stripe_ts(rec.get("created")),
                rec.get("charge"), rec.get("payment_intent"), rec.get("reason"),
                rec.get("status"), rec.get("is_charge_refundable"), Jsonb(rec)
            ))
        else:
            return False

        conn.commit()
    return True


async def run_stripe_import(resource: str, reset: bool = False):
    try:
        init_db()

        if reset:
            update_stripe_state(
                resource,
                status="idle",
                rows_processed=0,
                last_object_id=None,
                last_success_at=None,
                error=None,
                note="Reset for full historical import"
            )

        state = get_stripe_state(resource)
        starting_after = None if reset or state.get("status") == "complete" else state.get("last_object_id")
        processed = 0 if reset or state.get("status") == "complete" else int(state.get("rows_processed") or 0)

        # A completed import starts a fresh full upsert scan unless reset=False and the
        # caller later moves to an incremental sync version. This is safe but API-heavier.
        if state.get("status") == "complete" and not reset:
            starting_after = None
            processed = 0

        update_stripe_state(
            resource,
            status="running",
            rows_processed=processed,
            last_object_id=starting_after,
            error=None,
            note="Stripe import started"
        )

        path = STRIPE_RESOURCE_CONFIG[resource]["path"]

        while True:
            params = {"limit": 100}
            if starting_after:
                params["starting_after"] = starting_after

            payload = await stripe_get(path, params)
            items = payload.get("data", []) if isinstance(payload, dict) else []

            if not items:
                update_stripe_state(
                    resource,
                    status="complete",
                    rows_processed=processed,
                    last_object_id=starting_after,
                    last_success_at=utcnow(),
                    error=None,
                    note="Stripe import complete"
                )
                break

            for rec in items:
                if isinstance(rec, dict) and upsert_stripe_record(resource, rec):
                    processed += 1

            starting_after = items[-1].get("id")
            update_stripe_state(
                resource,
                status="running",
                rows_processed=processed,
                last_object_id=starting_after,
                last_success_at=utcnow(),
                error=None,
                note=f"Imported {processed} rows"
            )

            if not payload.get("has_more"):
                update_stripe_state(
                    resource,
                    status="complete",
                    rows_processed=processed,
                    last_object_id=starting_after,
                    last_success_at=utcnow(),
                    error=None,
                    note="Stripe import complete"
                )
                break

            await asyncio.sleep(0.15)

    except Exception as e:
        update_stripe_state(
            resource,
            status="error",
            error=str(e),
            note="Stripe import stopped with an error"
        )
    finally:
        _stripe_sync_tasks.pop(resource, None)


async def run_all_stripe_imports(reset: bool = False):
    # Sequential by design to keep API and database load gentle.
    for resource in STRIPE_RESOURCE_CONFIG:
        await run_stripe_import(resource, reset=reset)


@app.get("/")
async def root():
    return {
        "ok": True,
        "service": "4 Your Pad Service Fusion Connector",
        "version": "1.6.0",
        "database_configured": bool(DATABASE_URL),
        "features": ["service-fusion-sync", "jobs-reconciliation", "stripe-sync"]
    }


@app.get("/health")
async def health():
    return {"ok": True, "version": "1.6.0"}


@app.get("/test")
async def test(x_connector_key: Optional[str] = Header(default=None)):
    require_key(x_connector_key)
    data = await sf_get("/me")
    return {"ok": True, "service_fusion": data}


@app.post("/db/init")
async def db_init(x_connector_key: Optional[str] = Header(default=None)):
    require_key(x_connector_key)
    init_db()
    return {"ok": True, "message": "Database tables created or already exist"}


@app.get("/db/status")
async def db_status(x_connector_key: Optional[str] = Header(default=None)):
    require_key(x_connector_key)
    init_db()
    return {
        "customers": count_table("sf_customers"),
        "jobs": count_table("sf_jobs"),
        "invoices": count_table("sf_invoices"),
        "techs": count_table("sf_techs"),
        "job_statuses": count_table("sf_job_statuses"),
        "sources": count_table("sf_sources"),
        "payment_types": count_table("sf_payment_types"),
        "stripe_charges": count_table("stripe_charges"),
        "stripe_balance_transactions": count_table("stripe_balance_transactions"),
        "stripe_payouts": count_table("stripe_payouts"),
        "stripe_refunds": count_table("stripe_refunds"),
        "stripe_disputes": count_table("stripe_disputes"),
    }


@app.get("/sync/progress")
async def sync_progress(x_connector_key: Optional[str] = Header(default=None)):
    require_key(x_connector_key)
    init_db()
    result = {}
    for resource in RESOURCE_CONFIG:
        s = get_state(resource)
        result[resource] = {
            "resource": resource,
            "last_page": s.get("last_page", 0),
            "last_success_at": s.get("last_success_at"),
            "total_count": s.get("total_count"),
            "rows_processed": s.get("rows_processed", 0),
            "status": s.get("status", "idle"),
            "error": s.get("error"),
            "note": s.get("note"),
        }
    return result


@app.post("/sync/start/{resource}")
async def sync_start(
    resource: str,
    reset: bool = Query(default=False),
    per_page: int = Query(default=50, ge=1, le=50),
    x_connector_key: Optional[str] = Header(default=None)
):
    require_key(x_connector_key)
    if resource not in RESOURCE_CONFIG:
        raise HTTPException(status_code=404, detail="Unknown resource")
    init_db()

    existing = _sync_tasks.get(resource)
    if existing and not existing.done():
        return {"ok": True, "resource": resource, "message": "Import already running"}

    old_state = get_state(resource)
    resume_from = 1 if reset else int(old_state.get("last_page") or 0) + 1

    task = asyncio.create_task(run_import(resource, reset=reset, per_page=per_page))
    _sync_tasks[resource] = task

    return {
        "ok": True,
        "resource": resource,
        "message": "Background import started",
        "resume_from_page": resume_from
    }


@app.post("/sync/start-all")
async def sync_start_all(
    reset: bool = Query(default=False),
    x_connector_key: Optional[str] = Header(default=None)
):
    require_key(x_connector_key)
    started = []
    for resource in RESOURCE_CONFIG:
        existing = _sync_tasks.get(resource)
        if not existing or existing.done():
            _sync_tasks[resource] = asyncio.create_task(run_import(resource, reset=reset, per_page=50))
            started.append(resource)
    return {"ok": True, "started": started, "warning": "Sequential imports are preferred for large historical loads."}


@app.post("/sync/{resource}")
async def legacy_sync(
    resource: str,
    max_pages: int = Query(default=1, ge=1, le=1000),
    per_page: int = Query(default=50, ge=1, le=50),
    x_connector_key: Optional[str] = Header(default=None)
):
    require_key(x_connector_key)
    if resource not in RESOURCE_CONFIG:
        raise HTTPException(status_code=404, detail="Unknown resource")

    cfg = RESOURCE_CONFIG[resource]
    pages_synced = 0
    rows_processed = 0
    total = None

    for page in range(1, max_pages + 1):
        params = {"page": page, "per-page": per_page}
        if cfg.get("sort"):
            params["sort"] = cfg["sort"]
        payload = await sf_get_with_retry(cfg["path"], params)
        items, this_total = extract_items(payload)
        if this_total is not None:
            total = this_total
        if not items:
            break
        rows_processed += upsert_records(resource, items)
        pages_synced += 1
        if len(items) < per_page:
            break
        if total is not None and page * per_page >= total:
            break
        await asyncio.sleep(1.05)

    update_state(
        resource,
        last_page=pages_synced,
        last_success_at=utcnow(),
        total_count=total,
        note=f"Synced {rows_processed} rows in this run"
    )
    return {
        "ok": True,
        "resource": resource,
        "pages_synced": pages_synced,
        "rows_processed": rows_processed,
        "service_fusion_total": total
    }


@app.post("/reconcile/jobs/start")
async def reconcile_jobs_start(
    per_page: int = Query(default=50, ge=1, le=50),
    x_connector_key: Optional[str] = Header(default=None)
):
    require_key(x_connector_key)
    init_db()

    existing = _reconcile_tasks.get("jobs")
    if existing and not existing.done():
        return {
            "ok": True,
            "resource": "jobs",
            "message": "Jobs reconciliation is already running",
            "state": get_reconcile_state("jobs")
        }

    task = asyncio.create_task(run_jobs_reconcile(per_page=per_page))
    _reconcile_tasks["jobs"] = task

    return {
        "ok": True,
        "resource": "jobs",
        "message": "Jobs reconciliation started",
        "database_jobs_before_start": count_table("sf_jobs")
    }


@app.get("/reconcile/jobs/status")
async def reconcile_jobs_status(x_connector_key: Optional[str] = Header(default=None)):
    require_key(x_connector_key)
    init_db()
    state = get_reconcile_state("jobs")
    state["actual_database_jobs"] = count_table("sf_jobs")
    if state.get("target_count") is not None:
        state["remaining_gap"] = max(0, int(state["target_count"]) - int(state["actual_database_jobs"]))
    else:
        state["remaining_gap"] = None
    return state



@app.get("/stripe/test")
async def stripe_test(x_connector_key: Optional[str] = Header(default=None)):
    require_key(x_connector_key)
    data = await stripe_get("/balance")
    return {
        "ok": True,
        "stripe_connected": True,
        "available": data.get("available", []),
        "pending": data.get("pending", [])
    }


@app.get("/stripe/db/status")
async def stripe_db_status(x_connector_key: Optional[str] = Header(default=None)):
    require_key(x_connector_key)
    init_db()
    return {
        "charges": count_table("stripe_charges"),
        "balance_transactions": count_table("stripe_balance_transactions"),
        "payouts": count_table("stripe_payouts"),
        "refunds": count_table("stripe_refunds"),
        "disputes": count_table("stripe_disputes"),
    }


@app.get("/stripe/sync/progress")
async def stripe_sync_progress(x_connector_key: Optional[str] = Header(default=None)):
    require_key(x_connector_key)
    init_db()
    result = {}
    for resource in STRIPE_RESOURCE_CONFIG:
        result[resource] = get_stripe_state(resource)
    return result


@app.post("/stripe/sync/start/{resource}")
async def stripe_sync_start(
    resource: str,
    reset: bool = Query(default=False),
    x_connector_key: Optional[str] = Header(default=None)
):
    require_key(x_connector_key)
    if resource not in STRIPE_RESOURCE_CONFIG:
        raise HTTPException(status_code=404, detail="Unknown Stripe resource")

    init_db()
    existing = _stripe_sync_tasks.get(resource)
    if existing and not existing.done():
        return {"ok": True, "resource": resource, "message": "Stripe import already running"}

    task = asyncio.create_task(run_stripe_import(resource, reset=reset))
    _stripe_sync_tasks[resource] = task
    return {"ok": True, "resource": resource, "message": "Stripe background import started"}


@app.post("/stripe/sync/start-all")
async def stripe_sync_start_all(
    reset: bool = Query(default=False),
    x_connector_key: Optional[str] = Header(default=None)
):
    require_key(x_connector_key)
    init_db()

    task_name = "__all__"
    existing = _stripe_sync_tasks.get(task_name)
    if existing and not existing.done():
        return {"ok": True, "message": "Stripe full import already running"}

    task = asyncio.create_task(run_all_stripe_imports(reset=reset))
    _stripe_sync_tasks[task_name] = task

    def cleanup(_):
        _stripe_sync_tasks.pop(task_name, None)
    task.add_done_callback(cleanup)

    return {
        "ok": True,
        "message": "Sequential Stripe import started",
        "resources": list(STRIPE_RESOURCE_CONFIG.keys())
    }


@app.get("/{resource}")
async def proxy_resource(
    resource: str,
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=50, ge=1, le=50),
    x_connector_key: Optional[str] = Header(default=None)
):
    require_key(x_connector_key)
    if resource not in RESOURCE_CONFIG:
        raise HTTPException(status_code=404, detail="Unknown resource")
    cfg = RESOURCE_CONFIG[resource]
    params = {"page": page, "per-page": per_page}
    if cfg.get("sort"):
        params["sort"] = cfg["sort"]
    return await sf_get(cfg["path"], params)


@app.on_event("startup")
async def startup():
    if DATABASE_URL:
        try:
            init_db()
        except Exception:
            pass
