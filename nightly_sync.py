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


def request(method: str, path: str, retries: int = 3):
    url = f"{BASE_URL}{path}"

    for attempt in range(1, retries + 1):
        try:
            with httpx.Client(timeout=TIMEOUT) as client:
                r = client.request(method, url, headers=HEADERS)

            if r.status_code >= 400:
                raise RuntimeError(
                    f"{method} {path} failed: {r.status_code} {r.text[:500]}"
                )

            return r.json()

        except (httpx.ReadTimeout, httpx.ConnectTimeout) as e:
            print(
                f"Timeout calling {path} "
                f"(attempt {attempt} of {retries}): {e}"
            )

            if attempt == retries:
                raise

            print("Waiting 30 seconds before retry...")
            time.sleep(30)

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
    print(f"Syncing {resource} in batches...")

    batch_pages = 20
    start_page = 1
    total_rows = 0

    while True:
        print(
            f"{resource}: syncing pages "
            f"{start_page}-{start_page + batch_pages - 1}..."
        )

        result = request(
            "POST",
            f"/sync/{resource}"
            f"?start_page={start_page}"
            f"&max_pages={batch_pages}"
            f"&per_page=50",
        )

        print(result)

        rows = int(result.get("rows_processed") or 0)
        pages = int(result.get("pages_synced") or 0)
        total_rows += rows

        print(
            f"{resource}: batch complete; "
            f"pages_synced={pages}, "
            f"rows_processed={rows}, "
            f"running_rows={total_rows}"
        )

        if pages < batch_pages or rows < batch_pages * 50:
            print(
                f"{resource}: reached final page. "
                f"Total rows processed this run: {total_rows}"
            )
            break

        start_page += batch_pages
        time.sleep(3)


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
