import os
import time
import secrets
from typing import Any, Dict

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Query

app = FastAPI(title="4 Your Pad Service Fusion Connector", version="1.1.0")

SERVICE_FUSION_BASE = os.getenv(
    "SERVICE_FUSION_BASE",
    "https://api.servicefusion.com/v1",
).rstrip("/")

SERVICE_FUSION_TOKEN_URL = os.getenv(
    "SERVICE_FUSION_TOKEN_URL",
    "https://api.servicefusion.com/oauth/access_token",
)

CLIENT_ID = os.getenv("SERVICE_FUSION_CLIENT_ID")
CLIENT_SECRET = os.getenv("SERVICE_FUSION_CLIENT_SECRET")
CONNECTOR_API_KEY = os.getenv("CONNECTOR_API_KEY")

_token_cache: Dict[str, Any] = {}


def require_connector_key(x_connector_key: str | None = Header(default=None)) -> None:
    if not CONNECTOR_API_KEY:
        raise HTTPException(
            status_code=500,
            detail="CONNECTOR_API_KEY is not configured on the server.",
        )

    if not x_connector_key or not secrets.compare_digest(
        x_connector_key, CONNECTOR_API_KEY
    ):
        raise HTTPException(status_code=401, detail="Invalid connector API key.")


async def get_access_token() -> str:
    if not CLIENT_ID or not CLIENT_SECRET:
        raise HTTPException(
            status_code=500,
            detail="Service Fusion credentials are not configured.",
        )

    now = time.time()
    cached_token = _token_cache.get("access_token")
    expires_at = _token_cache.get("expires_at", 0)

    if cached_token and now < expires_at:
        return cached_token

    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                SERVICE_FUSION_TOKEN_URL,
                json=payload,
                headers={"Accept": "application/json"},
            )
    except httpx.RequestError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Service Fusion token connection error: {type(exc).__name__}: {exc}",
        ) from exc

    if response.status_code >= 400:
        body = response.text[:1500]
        raise HTTPException(
            status_code=502,
            detail=f"Service Fusion token request failed ({response.status_code}): {body}",
        )

    data = response.json()
    access_token = data.get("access_token")

    if not access_token:
        raise HTTPException(
            status_code=502,
            detail=f"Service Fusion token response did not contain access_token: {data}",
        )

    expires_in = int(data.get("expires_in", 3600))
    _token_cache["access_token"] = access_token
    _token_cache["expires_at"] = now + max(expires_in - 60, 60)

    return access_token


async def sf_get(path: str, params: Dict[str, Any] | None = None) -> Any:
    token = await get_access_token()
    url = f"{SERVICE_FUSION_BASE}/{path.lstrip('/')}"

    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(60.0, connect=20.0)
        ) as client:
            response = await client.get(
                url,
                params=params or {},
                headers={
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/json",
                },
            )
    except httpx.TimeoutException as exc:
        raise HTTPException(
            status_code=504,
            detail=f"Service Fusion request timed out for {path}: {type(exc).__name__}: {exc}",
        ) from exc
    except httpx.RequestError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Service Fusion connection error for {path}: {type(exc).__name__}: {exc}",
        ) from exc

    if response.status_code >= 400:
        body = response.text[:2000]
        raise HTTPException(
            status_code=502,
            detail=f"Service Fusion {path} request failed ({response.status_code}): {body}",
        )

    try:
        return response.json()
    except ValueError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Service Fusion returned invalid JSON for {path}: {response.text[:1000]}",
        ) from exc


def paging_params(page: int, per_page: int) -> Dict[str, Any]:
    return {"page": page, "per-page": per_page}


@app.get("/")
async def root():
    return {
        "ok": True,
        "service": "4 Your Pad Service Fusion Connector",
        "version": "1.1.0",
    }


@app.get("/health")
async def health():
    return {"ok": True}


@app.get("/test", dependencies=[Depends(require_connector_key)])
async def test_connection():
    data = await sf_get("/me")
    return {"ok": True, "service_fusion": data}


@app.get("/customers", dependencies=[Depends(require_connector_key)])
async def customers(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
):
    return await sf_get("/customers", paging_params(page, per_page))


@app.get("/jobs", dependencies=[Depends(require_connector_key)])
async def jobs(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
):
    params = paging_params(page, per_page)
    params["sort"] = "-start_date"
    return await sf_get("/jobs", params)


@app.get("/invoices", dependencies=[Depends(require_connector_key)])
async def invoices(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=50),
):
    # Service Fusion invoice listing behaves more reliably when explicitly sorted.
    # "-date" returns newest invoices first.
    params = paging_params(page, per_page)
    params["sort"] = "-date"
    return await sf_get("/invoices", params)


@app.get("/techs", dependencies=[Depends(require_connector_key)])
async def techs(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
):
    return await sf_get("/techs", paging_params(page, per_page))


@app.get("/job-statuses", dependencies=[Depends(require_connector_key)])
async def job_statuses(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
):
    return await sf_get("/job-statuses", paging_params(page, per_page))


@app.get("/sources", dependencies=[Depends(require_connector_key)])
async def sources(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
):
    return await sf_get("/sources", paging_params(page, per_page))


@app.get("/payment-types", dependencies=[Depends(require_connector_key)])
async def payment_types(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
):
    return await sf_get("/payment-types", paging_params(page, per_page))
