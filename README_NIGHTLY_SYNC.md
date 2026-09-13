# 4 Your Pad Nightly Sync

Upload `nightly_sync.py` to the root of the existing GitHub repository.

Render Cron Job configuration:

- Runtime: Python
- Repository: same 4yourpad-service-fusion-connector repository
- Branch: main
- Build command: `pip install -r requirements.txt`
- Start command: `python nightly_sync.py`
- Region: Ohio

Environment variables needed:
- CONNECTOR_API_KEY (same secret value used by the web service)
- CONNECTOR_BASE_URL=https://fouryourpad-service-fusion-connector.onrender.com

The script runs jobs reconciliation first, then customers, invoices, techs,
job statuses, sources, and payment types sequentially.
