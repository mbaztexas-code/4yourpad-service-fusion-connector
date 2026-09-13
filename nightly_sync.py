import os
import sys
import time
import httpx

BASE_URL = os.getenv(
    "CONNECTOR_BASE_URL",
    "https://fouryourpad-service-fusion-connector.onrender.com",
).rstrip("/")
API_KEY = os.getenv("CONNECTOR_API_KEY", "")
TIMEOUT = 900.0

if not API_KEY:
    print("ERROR: CONNECTOR_API_KEY is not configured.")
    sys.exit(1)

HEADERS = {"X-Connector-Key": API_KEY}


def request(method: str, path: str):
    url = f"{BASE_URL}{path}"
    with httpx.Client(timeout=TIMEOUT) as client:
        r = client.request(method, url, headers=HEADERS)
    if r.status_code >= 400:
        raise RuntimeError(f"{method} {path} failed: {r.status_code} {r.text[:500]}")
    return r.json()


def wait_for_jobs_reconciliation():
    print("Starting jobs reconciliation...")
    start = request("POST", "/reconcile/jobs/start?per_page=50")
    print(start)

    while True:
        time.sleep(30)
        state = request("GET", "/reconcile/jobs/status")
        print(
            "jobs:",
            "status=", state.get("status"),
            "db=", state.get("actual_database_jobs"),
            "target=", state.get("target_count"),
            "gap=", state.get("remaining_gap"),
            "page=", state.get("last_page"),
            "note=", state.get("note"),
        )

        status = state.get("status")
        if status == "complete":
            return
        if status in ("error", "needs_review"):
            raise RuntimeError(f"Jobs reconciliation ended with status {status}: {state}")


def full_sync(resource: str):
    print(f"Syncing {resource}...")
    result = request(
        "POST",
        f"/sync/{resource}?max_pages=1000&per_page=50",
    )
    print(result)


def main():
    print("=== 4 Your Pad nightly Service Fusion sync started ===")

    wait_for_jobs_reconciliation()

    for resource in (
        "customers",
        "invoices",
        "techs",
        "job-statuses",
        "sources",
        "payment-types",
    ):
        full_sync(resource)
        time.sleep(3)

    status = request("GET", "/db/status")
    print("Final database counts:", status)
    print("=== nightly sync complete ===")


if __name__ == "__main__":
    main()
