# 4 Your Pad - Service Fusion Connector

Read-only connector for pulling Service Fusion operating data for the
4 Your Pad Inc. valuation project.

## What this version does

- Authenticates to Service Fusion using OAuth 2.0 Client Credentials
- Keeps the Client ID and Client Secret in environment variables
- Caches the Service Fusion access token in memory
- Exposes only GET/read endpoints
- Protects data endpoints with a separate `X-Connector-Key`
- Pulls:
  - `/me`
  - `/customers`
  - `/jobs`
  - `/invoices`
  - `/techs`
  - `/job-statuses`
  - `/sources`
  - `/payment-types`

## Security

Never put the actual Service Fusion Client Secret inside `app.py`.
Store it as a host environment variable.

The connector also requires its own `CONNECTOR_API_KEY`, so the public
URL alone does not expose customer or company data.

## Local setup

1. Install Python 3.12+
2. Copy `.env.example` values into your shell/environment.
3. Install dependencies:

```bash
pip install -r requirements.txt
```

4. Start:

```bash
uvicorn app:app --reload
```

5. Public health test:

```text
http://127.0.0.1:8000/health
```

6. Protected Service Fusion test:

```bash
curl -H "X-Connector-Key: YOUR_CONNECTOR_API_KEY" \
  http://127.0.0.1:8000/test
```

If authentication works, `/test` should return the Service Fusion `/me`
response.

## Render deployment

Create a new Web Service and deploy this repository/folder.

Set these environment variables in Render:

- `SERVICE_FUSION_CLIENT_ID`
- `SERVICE_FUSION_CLIENT_SECRET`
- `CONNECTOR_API_KEY`

Do not paste those values into source code.

Build command:

```text
pip install -r requirements.txt
```

Start command:

```text
uvicorn app:app --host 0.0.0.0 --port $PORT
```

After deployment, test:

```text
https://YOUR-RENDER-URL/health
```

Then test Service Fusion authentication with:

```bash
curl -H "X-Connector-Key: YOUR_CONNECTOR_API_KEY" \
  https://YOUR-RENDER-URL/test
```

## First valuation endpoints

Examples:

```text
GET /customers?page=1&per_page=50
GET /jobs?page=1&per_page=50
GET /invoices?page=1&per_page=50
GET /techs?page=1&per_page=50
GET /job-statuses
GET /sources
GET /payment-types
```

Each protected request must include:

```text
X-Connector-Key: YOUR_CONNECTOR_API_KEY
```

## Next phase

After the connection is verified, the next version should add a
database sync so we can create valuation tables such as:

- monthly revenue
- jobs completed
- average ticket
- repeat customer rate
- customer lifetime value
- revenue by technician
- revenue by source
- revenue by city / market
- unpaid invoices / A/R
- service mix
- contractor labor percentage
- gross margin
- normalized EBITDA / SDE
