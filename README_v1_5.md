# 4 Your Pad Service Fusion Connector v1.5

This version adds targeted Jobs reconciliation without deleting existing database rows.

New endpoints:

POST /reconcile/jobs/start?per_page=50
GET  /reconcile/jobs/status

Strategy:
1. Recover missing jobs referenced by invoices via GET /jobs/{job-id}.
2. Scan jobs oldest-to-newest using sort=start_date.
3. If needed, scan newest-to-oldest using sort=-start_date.
4. If still needed, make one final oldest-to-newest pass.

All writes are UPSERTS by Service Fusion Job ID. Existing jobs are not deleted.
