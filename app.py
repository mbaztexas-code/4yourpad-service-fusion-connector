import os
import time
from typing import Optional

import httpx
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.responses import JSONResponse

SERVICE_FUSION_BASE = os.getenv("SERVICE_FUSION_BASE", "https://api.servicefusion.com/v1")
TOKEN_URL = os.getenv("SERVICE_FUSION_TOKEN_URL", "https://api.servicefusion.com/oauth/access_token")
CLIENT_ID = os.getenv("SERVICE_FUSION_CLIENT_ID")
CLIENT_SECRET = os.getenv("SERVICE_FUSION_CLIENT_SECRET")
CONNECTOR_API_KEY = os.getenv("CONNECTOR_API_KEY")

app = FastAPI(
    title="4 Your Pad - Service Fusion Connector",
    version="1.0.0",
    description="Read-only connector for Service Fusion valuation data."
)

_token_cache = {
    "access_token": None,
    "expires_at": 0,
}


def require_connector_key(x_connector_key: Optional[str]):
    if not CONNECTOR_API_KEY:
        raise HTTPException(
            status_code=500,
            detail="CONNECTOR_API_KEY is not configured on the server."
        )
    if x_connector_key != CONNECTOR_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid connector API key.")


async def get_access_token() -> str:
    if not CLIENT_ID or not CLIENT_SECRET:
        raise HTTPException(
            status_code=500,
            detail="Service Fusion credentials are not configured."
        )

    now = time.time()
    cached = _token_cache.get("access_token")
    expires_at = _token_cache.get("expires_at", 0)

    if cached and now < expires_at - 60:
        return cached

    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }

    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.post(
            TOKEN_URL,
            json=payload,
            headers={"Accept": "application/json", "Content-Type": "application/json"},
        )

    if response.status_code >= 400:
        raise HTTPException(
            status_code=502,
            detail={
                "message": "Service Fusion token request failed.",
                "status_code": response.status_code,
                "response": response.text[:1000],
            },
        )

    data = response.json()
    token = data.get("access_token")
    if not token:
        raise HTTPException(
            status_code=502,
            detail="Service Fusion did not return an access_token."
        )

    expires_in = int(data.get("expires_in", 3600))
    _token_cache["access_token"] = token
    _token_cache["expires_at"] = now + expires_in
    return token


async def sf_get(path: str, params: Optional[dict] = None):
    token = await get_access_token()
    url = f"{SERVICE_FUSION_BASE.rstrip('/')}/{path.lstrip('/')}"

    async with httpx.AsyncClient(timeout=60) as client:
        response = await client.get(
            url,
            params=params or {},
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
            },
        )

    if response.status_code >= 400:
        raise HTTPException(
            status_code=502,
            detail={
                "message": f"Service Fusion request failed for {path}.",
                "status_code": response.status_code,
                "response": response.text[:1500],
            },
        )

    if not response.content:
        return {}
    return response.json()


@app.get("/")
async def root():
    return {
        "name": "4 Your Pad - Service Fusion Connector",
        "status": "online",
        "mode": "read-only",
    }


@app.get("/health")
async def health():
    return {"ok": True}


@app.get("/test")
async def test_connection(x_connector_key: Optional[str] = Header(default=None)):
    require_connector_key(x_connector_key)
    data = await sf_get("/me")
    return {"ok": True, "service_fusion": data}


@app.get("/customers")
async def customers(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=100),
    x_connector_key: Optional[str] = Header(default=None),
):
    require_connector_key(x_connector_key)
    return await sf_get("/customers", {"page": page, "per-page": per_page})


@app.get("/jobs")
async def jobs(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=100),
    sort: str = Query("-start_date"),
    x_connector_key: Optional[str] = Header(default=None),
):
    require_connector_key(x_connector_key)
    return await sf_get(
        "/jobs",
        {"page": page, "per-page": per_page, "sort": sort},
    )


@app.get("/invoices")
async def invoices(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=100),
    x_connector_key: Optional[str] = Header(default=None),
):
    require_connector_key(x_connector_key)
    return await sf_get("/invoices", {"page": page, "per-page": per_page})


@app.get("/techs")
async def techs(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=100),
    x_connector_key: Optional[str] = Header(default=None),
):
    require_connector_key(x_connector_key)
    return await sf_get("/techs", {"page": page, "per-page": per_page})


@app.get("/job-statuses")
async def job_statuses(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=100),
    x_connector_key: Optional[str] = Header(default=None),
):
    require_connector_key(x_connector_key)
    return await sf_get("/job-statuses", {"page": page, "per-page": per_page})


@app.get("/sources")
async def sources(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=100),
    x_connector_key: Optional[str] = Header(default=None),
):
    require_connector_key(x_connector_key)
    return await sf_get("/sources", {"page": page, "per-page": per_page})


@app.get("/payment-types")
async def payment_types(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=100),
    x_connector_key: Optional[str] = Header(default=None),
):
    require_connector_key(x_connector_key)
    return await sf_get("/payment-types", {"page": page, "per-page": per_page})


@app.exception_handler(httpx.RequestError)
async def httpx_error_handler(request, exc):
    return JSONResponse(
        status_code=502,
        content={"detail": f"Upstream connection error: {str(exc)}"},
    )
