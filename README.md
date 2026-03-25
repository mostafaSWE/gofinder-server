# GOFinder AI — Collection Server

A lightweight Flask server that receives, deduplicates, and stores
reinforcement feedback uploaded by local GOFinder AI instances.

## Quick Start

```bash
# 1. Create a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Set environment variables
export COLLECTION_API_KEY="your-secret-api-key"
export ADMIN_USER="admin"
export ADMIN_PASSWORD="your-admin-password"

# 4. Run the server
python3 server.py
```

The server starts on port **5050** by default (set `COLLECTION_PORT` to change).

## Endpoints

| Endpoint | Method | Auth | Description |
|---|---|---|---|
| `/collect` | POST | `X-API-Key` header | Receive feedback rows from GOFinder instances |
| `/health` | GET | None | Health check |
| `/admin/dashboard` | GET | HTTP Basic Auth | Web dashboard with stats and recent rows |
| `/admin/stats` | GET | HTTP Basic Auth | JSON statistics |
| `/admin/export/<table>` | GET | HTTP Basic Auth | CSV download of collected data |

### Tables

- `reinforcement_rows` — Standard GO Relevance / Automate feedback
- `reinforcement_entity_rows` — GO Light / GO Extract entity-level feedback

## Admin Dashboard

Visit `http://your-server:5050/admin/dashboard` in your browser.
You'll be prompted for the username and password set via `ADMIN_USER` / `ADMIN_PASSWORD`.

## Data Storage

All data is stored in a single SQLite file at `data/collections.db` (auto-created on first run).
