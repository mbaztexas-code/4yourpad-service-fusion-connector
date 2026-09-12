import os
import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx
import psycopg
from fastapi import FastAPI, Header, HTTPException, Query
from psycopg.types.json import Jsonb

app = FastAPI(title="4 Your Pad Service Fusion Connector", version="1.4.0")

SF_CLIENT_ID = os.getenv("SERVICE_FUSION_CLIENT_ID", "")
SF_CLIENT_SECRET = os.getenv("SERVICE_FUSION_CLIENT_SECRET", "")
CONNECTOR_API_KEY = os.getenv("CONNECTOR_API_KEY", "")
SF_BASE = os.getenv("SERVICE_FUSION_BASE", "https://api.servicefusion.com/v1").rstrip("/")
SF_TOKEN_URL = os.getenv("SERVICE_FUSION_TOKEN_URL", "https://api.servicefusion.com/oauth/access_token")
DATABASE_URL = os.getenv("DATABASE_URL", "")

_token: Dict[str, Any] = {}
_sync_tasks: Dict[str, asyncio.Task] = {}

RESOURCE_CONFIG = {
    "customers": {"path": "/customers", "sort": None, "table": "sf_customers"},
    "jobs": {"path": "/jobs", "sort": "-start_date", "table": "sf_jobs"},
    "invoices": {"path": "/invoices", "sort": "-date", "table": "sf_invoices"},
    "techs": {"path": "/techs", "sort": None, "table": "sf_techs"},
    "job-statuses": {"path": "/job-statuses", "sort": None, "table": "sf_job_statuses"},
    "sources": {"path": "/sources", "sort": None, "table": "sf_sources"},
    "payment-types": {"path": "/payment-types", "sort": None, "table": "sf_payment_types"},
}

def require_key(x_connector_key: Optional[str]):
    if not CONNECTOR_API_KEY or x_connector_key != CONNECTOR_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid connector key")

def db_conn():
    if not DATABASE_URL:
        raise HTTPException(status_code=500, detail="DATABASE_URL is not configured")
    return psycopg.connect(DATABASE_URL)

def pick(d: Dict[str, Any], *keys, default=None):
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return default

def nested(d: Dict[str, Any], *keys):
    cur: Any = d
    for k in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur

def parse_dt(v):
    if not v:
        return None
    if isinstance(v, datetime):
        return v
    s = str(v).strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s)
    except Exception:
        try:
            return datetime.fromisoformat(s[:10])
        except Exception:
            return None

def num(v):
    if v in (None, ""):
        return None
    try:
        return float(str(v).replace(",", "").replace("$", ""))
    except Exception:
        return None

async def token():
    now = datetime.now(timezone.utc).timestamp()
    if _token.get("access_token") and _token.get("expires_at", 0) > now:
        return _token["access_token"]
    if not SF_CLIENT_ID or not SF_CLIENT_SECRET:
        raise HTTPException(status_code=500, detail="Service Fusion credentials are not configured")
    async with httpx.AsyncClient(timeout=45) as client:
        r = await client.post(SF_TOKEN_URL, json={
            "grant_type": "client_credentials",
            "client_id": SF_CLIENT_ID,
            "client_secret": SF_CLIENT_SECRET,
        })
        r.raise_for_status()
        data = r.json()
    _token["access_token"] = data["access_token"]
    _token["expires_at"] = now + int(data.get("expires_in", 3600)) - 60
    return _token["access_token"]

async def sf_get(path: str, params=None):
    t = await token()
    async with httpx.AsyncClient(timeout=90) as client:
        r = await client.get(
            f"{SF_BASE}{path}",
            params=params or {},
            headers={"Authorization": f"Bearer {t}", "Accept": "application/json"},
        )
        if r.status_code >= 400:
            raise HTTPException(status_code=502, detail=f"Service Fusion error {r.status_code}: {r.text[:500]}")
        return r.json()

def extract_items(payload):
    if isinstance(payload, list):
        return payload, len(payload)
    if not isinstance(payload, dict):
        return [], None
    for key in ("items", "data", "results"):
        if isinstance(payload.get(key), list):
            items = payload[key]
            break
    else:
        items = []
    meta = payload.get("_meta") or payload.get("meta") or {}
    total = pick(meta, "totalCount", "total_count", "total", default=None)
    return items, total

def init_db():
    ddl = """
    CREATE TABLE IF NOT EXISTS sf_customers (
      sf_id BIGINT PRIMARY KEY, name TEXT, phone TEXT, email TEXT, city TEXT,
      state TEXT, postal_code TEXT, created_at_sf TIMESTAMPTZ, updated_at_sf TIMESTAMPTZ,
      raw_json JSONB NOT NULL, synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS sf_jobs (
      sf_id BIGINT PRIMARY KEY, customer_id BIGINT, number TEXT, status TEXT,
      category TEXT, source TEXT, start_date TIMESTAMPTZ, completion_date TIMESTAMPTZ,
      total NUMERIC, raw_json JSONB NOT NULL, synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS ix_sf_jobs_customer ON sf_jobs(customer_id);
    CREATE INDEX IF NOT EXISTS ix_sf_jobs_start ON sf_jobs(start_date);

    CREATE TABLE IF NOT EXISTS sf_invoices (
      sf_id BIGINT PRIMARY KEY, customer_id BIGINT, job_id BIGINT, number TEXT,
      invoice_date TIMESTAMPTZ, due_date TIMESTAMPTZ, status TEXT, subtotal NUMERIC,
      tax NUMERIC, total NUMERIC, balance NUMERIC, raw_json JSONB NOT NULL,
      synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS ix_sf_invoices_customer ON sf_invoices(customer_id);
    CREATE INDEX IF NOT EXISTS ix_sf_invoices_job ON sf_invoices(job_id);
    CREATE INDEX IF NOT EXISTS ix_sf_invoices_date ON sf_invoices(invoice_date);

    CREATE TABLE IF NOT EXISTS sf_techs (
      sf_id BIGINT PRIMARY KEY, name TEXT, email TEXT, phone TEXT,
      raw_json JSONB NOT NULL, synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS sf_job_statuses (
      sf_id BIGINT PRIMARY KEY, name TEXT, raw_json JSONB NOT NULL,
      synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS sf_sources (
      sf_id BIGINT PRIMARY KEY, name TEXT, raw_json JSONB NOT NULL,
      synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS sf_payment_types (
      sf_id BIGINT PRIMARY KEY, name TEXT, raw_json JSONB NOT NULL,
      synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS sf_sync_state (
      resource TEXT PRIMARY KEY,
      last_page INTEGER NOT NULL DEFAULT 0,
      last_success_at TIMESTAMPTZ,
      total_count INTEGER,
      rows_processed INTEGER NOT NULL DEFAULT 0,
      status TEXT NOT NULL DEFAULT 'idle',
      error TEXT,
      note TEXT
    );
    ALTER TABLE sf_sync_state ADD COLUMN IF NOT EXISTS rows_processed INTEGER NOT NULL DEFAULT 0;
    ALTER TABLE sf_sync_state ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'idle';
    ALTER TABLE sf_sync_state ADD COLUMN IF NOT EXISTS error TEXT;
    """
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()

def upsert_customer(cur, x):
    sf_id = pick(x, "id", "customer_id")
    if sf_id is None: return False
    name = pick(x, "name", "customer_name", default=None)
    if not name:
        name = " ".join(filter(None, [pick(x, "first_name", "firstName"), pick(x, "last_name", "lastName")])) or None
    phone = pick(x, "phone", "phone_number")
    email = pick(x, "email")
    city = pick(x, "city") or nested(x, "primary_location", "city")
    state = pick(x, "state") or nested(x, "primary_location", "state")
    postal = pick(x, "postal_code", "zip", "zipcode") or nested(x, "primary_location", "postal_code")
    cur.execute("""INSERT INTO sf_customers
      (sf_id,name,phone,email,city,state,postal_code,created_at_sf,updated_at_sf,raw_json,synced_at)
      VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
      ON CONFLICT(sf_id) DO UPDATE SET
      name=EXCLUDED.name,phone=EXCLUDED.phone,email=EXCLUDED.email,city=EXCLUDED.city,
      state=EXCLUDED.state,postal_code=EXCLUDED.postal_code,created_at_sf=EXCLUDED.created_at_sf,
      updated_at_sf=EXCLUDED.updated_at_sf,raw_json=EXCLUDED.raw_json,synced_at=NOW()""",
      (sf_id,name,phone,email,city,state,postal,parse_dt(pick(x,"created_at","createdAt")),
       parse_dt(pick(x,"updated_at","updatedAt")),Jsonb(x)))
    return True

def upsert_job(cur, x):
    sf_id = pick(x, "id", "job_id")
    if sf_id is None: return False
    customer_id = pick(x, "customer_id", "customerId") or nested(x, "customer", "id")
    status = pick(x, "status") or nested(x, "status", "name")
    if isinstance(status, dict): status = pick(status, "name")
    category = pick(x, "category") or nested(x, "category", "name")
    if isinstance(category, dict): category = pick(category, "name")
    source = pick(x, "source") or nested(x, "source", "name")
    if isinstance(source, dict): source = pick(source, "name")
    cur.execute("""INSERT INTO sf_jobs
      (sf_id,customer_id,number,status,category,source,start_date,completion_date,total,raw_json,synced_at)
      VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
      ON CONFLICT(sf_id) DO UPDATE SET customer_id=EXCLUDED.customer_id,number=EXCLUDED.number,
      status=EXCLUDED.status,category=EXCLUDED.category,source=EXCLUDED.source,start_date=EXCLUDED.start_date,
      completion_date=EXCLUDED.completion_date,total=EXCLUDED.total,raw_json=EXCLUDED.raw_json,synced_at=NOW()""",
      (sf_id,customer_id,pick(x,"number","job_number"),status,category,source,
       parse_dt(pick(x,"start_date","startDate")),parse_dt(pick(x,"completion_date","completed_at","completionDate")),
       num(pick(x,"total","total_amount","grand_total")),Jsonb(x)))
    return True

def upsert_invoice(cur, x):
    sf_id = pick(x, "id", "invoice_id")
    if sf_id is None: return False
    customer_id = pick(x,"customer_id","customerId") or nested(x,"customer","id")
    job_id = pick(x,"job_id","jobId") or nested(x,"job","id")
    status = pick(x,"status")
    if isinstance(status, dict): status = pick(status,"name")
    cur.execute("""INSERT INTO sf_invoices
      (sf_id,customer_id,job_id,number,invoice_date,due_date,status,subtotal,tax,total,balance,raw_json,synced_at)
      VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
      ON CONFLICT(sf_id) DO UPDATE SET customer_id=EXCLUDED.customer_id,job_id=EXCLUDED.job_id,
      number=EXCLUDED.number,invoice_date=EXCLUDED.invoice_date,due_date=EXCLUDED.due_date,status=EXCLUDED.status,
      subtotal=EXCLUDED.subtotal,tax=EXCLUDED.tax,total=EXCLUDED.total,balance=EXCLUDED.balance,
      raw_json=EXCLUDED.raw_json,synced_at=NOW()""",
      (sf_id,customer_id,job_id,pick(x,"number","invoice_number"),
       parse_dt(pick(x,"date","invoice_date","invoiceDate")),parse_dt(pick(x,"due_date","dueDate")),
       status,num(pick(x,"subtotal")),num(pick(x,"tax","tax_amount")),num(pick(x,"total","total_amount")),
       num(pick(x,"balance","balance_due")),Jsonb(x)))
    return True

def upsert_simple(cur, table, x):
    sf_id = pick(x, "id")
    if sf_id is None: return False
    name = pick(x,"name","display_name","label")
    if table == "sf_techs":
        if not name:
            name = " ".join(filter(None,[pick(x,"first_name","firstName"),pick(x,"last_name","lastName")])) or None
        cur.execute("""INSERT INTO sf_techs(sf_id,name,email,phone,raw_json,synced_at)
          VALUES(%s,%s,%s,%s,%s,NOW()) ON CONFLICT(sf_id) DO UPDATE SET
          name=EXCLUDED.name,email=EXCLUDED.email,phone=EXCLUDED.phone,raw_json=EXCLUDED.raw_json,synced_at=NOW()""",
          (sf_id,name,pick(x,"email"),pick(x,"phone","phone_number"),Jsonb(x)))
    else:
        cur.execute(f"""INSERT INTO {table}(sf_id,name,raw_json,synced_at)
          VALUES(%s,%s,%s,NOW()) ON CONFLICT(sf_id) DO UPDATE SET
          name=EXCLUDED.name,raw_json=EXCLUDED.raw_json,synced_at=NOW()""",(sf_id,name,Jsonb(x)))
    return True

def save_items(resource, items):
    cfg = RESOURCE_CONFIG[resource]
    count = 0
    with db_conn() as conn:
        with conn.cursor() as cur:
            for x in items:
                if not isinstance(x, dict): continue
                if resource == "customers": ok = upsert_customer(cur,x)
                elif resource == "jobs": ok = upsert_job(cur,x)
                elif resource == "invoices": ok = upsert_invoice(cur,x)
                else: ok = upsert_simple(cur,cfg["table"],x)
                count += 1 if ok else 0
        conn.commit()
    return count

def get_state(resource):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""SELECT last_page,last_success_at,total_count,rows_processed,status,error,note
                           FROM sf_sync_state WHERE resource=%s""",(resource,))
            row=cur.fetchone()
    if not row:
        return {"resource":resource,"last_page":0,"rows_processed":0,"status":"idle"}
    return {"resource":resource,"last_page":row[0],"last_success_at":row[1],
            "total_count":row[2],"rows_processed":row[3],"status":row[4],"error":row[5],"note":row[6]}

def update_state(resource, **kwargs):
    state=get_state(resource)
    vals={
        "last_page": kwargs.get("last_page",state.get("last_page",0)),
        "last_success_at": kwargs.get("last_success_at",state.get("last_success_at")),
        "total_count": kwargs.get("total_count",state.get("total_count")),
        "rows_processed": kwargs.get("rows_processed",state.get("rows_processed",0)),
        "status": kwargs.get("status",state.get("status","idle")),
        "error": kwargs.get("error",state.get("error")),
        "note": kwargs.get("note",state.get("note")),
    }
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO sf_sync_state
              (resource,last_page,last_success_at,total_count,rows_processed,status,error,note)
              VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
              ON CONFLICT(resource) DO UPDATE SET last_page=EXCLUDED.last_page,
              last_success_at=EXCLUDED.last_success_at,total_count=EXCLUDED.total_count,
              rows_processed=EXCLUDED.rows_processed,status=EXCLUDED.status,error=EXCLUDED.error,note=EXCLUDED.note""",
              (resource,vals["last_page"],vals["last_success_at"],vals["total_count"],vals["rows_processed"],
               vals["status"],vals["error"],vals["note"]))
        conn.commit()

async def run_import(resource: str, reset: bool=False, per_page: int=50):
    cfg=RESOURCE_CONFIG[resource]
    if reset:
        update_state(resource,last_page=0,rows_processed=0,status="idle",error=None,note="Reset for full import")
    state=get_state(resource)
    page=max(1,int(state.get("last_page",0))+1)
    processed=int(state.get("rows_processed",0))
    update_state(resource,status="running",error=None,note="Historical/background sync running")
    try:
        while True:
            params={"page":page,"per-page":per_page}
            if cfg["sort"]: params["sort"]=cfg["sort"]
            # Service Fusion occasionally returns temporary 500/502/503/504 errors.
            # Retry the SAME page with increasing delays instead of stopping the import.
            retry_delays = [5, 10, 20, 30, 45, 60, 60, 60]
            payload = None
            last_error = None
            for attempt in range(len(retry_delays) + 1):
                try:
                    payload = await sf_get(cfg["path"], params)
                    break
                except HTTPException as e:
                    last_error = e
                    retryable = e.status_code in (429, 502, 503, 504)
                    if not retryable or attempt >= len(retry_delays):
                        raise
                    delay = retry_delays[attempt]
                    update_state(
                        resource,
                        status="running",
                        error=None,
                        note=f"Temporary Service Fusion error on page {page}; retry {attempt + 1}/{len(retry_delays)} in {delay}s",
                    )
                    await asyncio.sleep(delay)
                except (httpx.TimeoutException, httpx.RequestError) as e:
                    last_error = e
                    if attempt >= len(retry_delays):
                        raise
                    delay = retry_delays[attempt]
                    update_state(
                        resource,
                        status="running",
                        error=None,
                        note=f"Temporary connection error on page {page}; retry {attempt + 1}/{len(retry_delays)} in {delay}s",
                    )
                    await asyncio.sleep(delay)

            if payload is None:
                raise last_error or RuntimeError(f"Unable to fetch page {page}")

            items,total=extract_items(payload)
            if total is not None:
                try: total=int(total)
                except: total=None
            if not items:
                update_state(resource,status="complete",last_success_at=datetime.now(timezone.utc),
                             total_count=total or state.get("total_count"),note="Import complete")
                break
            saved=save_items(resource,items)
            processed += saved
            update_state(resource,last_page=page,rows_processed=processed,total_count=total,
                         last_success_at=datetime.now(timezone.utc),status="running",error=None,
                         note=f"Completed page {page}")
            if len(items) < per_page or (total and page*per_page >= total):
                update_state(resource,status="complete",last_success_at=datetime.now(timezone.utc),
                             note="Import complete")
                break
            page += 1
            await asyncio.sleep(1.05)
    except Exception as e:
        update_state(resource,status="error",error=str(e)[:1000],
                     last_success_at=datetime.now(timezone.utc),note=f"Stopped after page {page-1}")
    finally:
        _sync_tasks.pop(resource,None)

@app.get("/")
def root():
    return {"ok":True,"service":"4 Your Pad Service Fusion Connector","version":"1.4.0",
            "database_configured":bool(DATABASE_URL)}

@app.get("/health")
def health():
    return {"ok":True}

@app.get("/test")
async def test(x_connector_key: Optional[str]=Header(None)):
    require_key(x_connector_key)
    return {"ok":True,"me":await sf_get("/me")}

@app.post("/db/init")
def db_init(x_connector_key: Optional[str]=Header(None)):
    require_key(x_connector_key); init_db()
    return {"ok":True,"message":"Database tables created or already exist"}

@app.get("/db/status")
def db_status(x_connector_key: Optional[str]=Header(None)):
    require_key(x_connector_key); init_db()
    tables={"customers":"sf_customers","jobs":"sf_jobs","invoices":"sf_invoices","techs":"sf_techs",
            "job_statuses":"sf_job_statuses","sources":"sf_sources","payment_types":"sf_payment_types"}
    out={}
    with db_conn() as conn:
        with conn.cursor() as cur:
            for k,t in tables.items():
                cur.execute(f"SELECT COUNT(*) FROM {t}")
                out[k]=cur.fetchone()[0]
    return out

@app.get("/sync/progress")
def sync_progress(x_connector_key: Optional[str]=Header(None)):
    require_key(x_connector_key); init_db()
    return {r:get_state(r) for r in RESOURCE_CONFIG}

@app.post("/sync/start/{resource}")
async def sync_start(resource: str, reset: bool=Query(False), per_page: int=Query(50,ge=1,le=50),
                     x_connector_key: Optional[str]=Header(None)):
    require_key(x_connector_key); init_db()
    if resource not in RESOURCE_CONFIG:
        raise HTTPException(404,detail=f"Unknown resource. Choose: {', '.join(RESOURCE_CONFIG)}")
    existing=_sync_tasks.get(resource)
    if existing and not existing.done():
        return {"ok":True,"resource":resource,"message":"Already running","progress":get_state(resource)}
    task=asyncio.create_task(run_import(resource,reset=reset,per_page=per_page))
    _sync_tasks[resource]=task
    return {"ok":True,"resource":resource,"message":"Background import started","resume_from_page":get_state(resource).get("last_page",0)+1}

@app.post("/sync/start-all")
async def sync_start_all(reset: bool=Query(False), x_connector_key: Optional[str]=Header(None)):
    require_key(x_connector_key); init_db()
    started=[]
    for r in RESOURCE_CONFIG:
        if r not in _sync_tasks or _sync_tasks[r].done():
            _sync_tasks[r]=asyncio.create_task(run_import(r,reset=reset,per_page=50))
            started.append(r)
    return {"ok":True,"started":started,"message":"Background imports started. Check /sync/progress."}

@app.post("/sync/{resource}")
async def legacy_sync(resource: str, max_pages: int=Query(1,ge=1,le=5000),
                      per_page: int=Query(50,ge=1,le=50), x_connector_key: Optional[str]=Header(None)):
    require_key(x_connector_key); init_db()
    if resource not in RESOURCE_CONFIG: raise HTTPException(404,detail="Unknown resource")
    cfg=RESOURCE_CONFIG[resource]; rows=0; pages=0; total=None
    for page in range(1,max_pages+1):
        params={"page":page,"per-page":per_page}
        if cfg["sort"]: params["sort"]=cfg["sort"]
        payload=await sf_get(cfg["path"],params)
        items,total=extract_items(payload)
        if not items: break
        rows += save_items(resource,items); pages += 1
        if len(items)<per_page: break
        if page<max_pages: await asyncio.sleep(1.05)
    return {"ok":True,"resource":resource,"pages_synced":pages,"rows_processed":rows,"service_fusion_total":total}

@app.get("/{resource}")
async def proxy_resource(resource: str, page: int=1, per_page: int=Query(5,ge=1,le=50),
                         x_connector_key: Optional[str]=Header(None)):
    require_key(x_connector_key)
    if resource not in RESOURCE_CONFIG: raise HTTPException(404,detail="Unknown resource")
    cfg=RESOURCE_CONFIG[resource]
    params={"page":page,"per-page":per_page}
    if cfg["sort"]: params["sort"]=cfg["sort"]
    return await sf_get(cfg["path"],params)
