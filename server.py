"""
GOFinder AI — Reinforcement Data Collection Server
====================================================
A lightweight Flask server that receives, deduplicates, and stores
reinforcement feedback uploaded by local GOFinder AI instances.

Endpoints
---------
POST /collect             API-key auth.  Ingest feedback rows.
GET  /health              Public.        Health check.
GET  /admin/dashboard     Basic auth.    Web UI with stats & recent rows.
GET  /admin/stats         Basic auth.    JSON stats.
GET  /admin/export/<tbl>  Basic auth.    CSV download.

Environment Variables
---------------------
COLLECTION_API_KEY   API key that local instances send in X-API-Key header.
ADMIN_USER           Username for /admin/* endpoints  (default: admin).
ADMIN_PASSWORD       Password for /admin/* endpoints  (default: changeme).
COLLECTION_PORT      Port to listen on                (default: 5050).
"""

import csv
import hashlib
import io
import json
import os
import secrets
import sqlite3
import urllib.parse
from datetime import datetime
from functools import wraps

try:
    import psycopg2
    import psycopg2.extras
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

from flask import Flask, Response, g, jsonify, render_template_string, request
from flask_cors import CORS

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "data", "collections.db")

COLLECTION_API_KEY = os.environ.get("COLLECTION_API_KEY", "changeme-api-key")
ADMIN_USER = os.environ.get("ADMIN_USER", "admin")
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "changeme")
COLLECTION_PORT = int(os.environ.get("COLLECTION_PORT", 5050))
DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite")
IS_POSTGRES = DATABASE_URL.startswith("postgres")

if IS_POSTGRES and not HAS_PSYCOPG2:
    raise RuntimeError("DATABASE_URL is a PostgreSQL string but psycopg2 is not installed!")

# ---------------------------------------------------------------------------
# Schemas — must match the GOFinder backend FIELDNAMES / ENTITY_FIELDNAMES
# ---------------------------------------------------------------------------
REINFORCEMENT_COLUMNS = [
    "Gene",
    "Biological Entity of Interest",
    "Ensembl Transcript Accession ID (ENST)",
    "Ensembl Gene Accession ID (ENSG)",
    "Paper ID",
    "Title of the Paper",
    "Paper Text",
    "GO ID",
    "GO Name",
    "GO Definition",
    "GO Synonyms",
    "Verdict",
    "Supporting Text Citation",
    "Reason",
    "Feedback",
    "Timestamp",
]

ENTITY_COLUMNS = [
    "Paper Title",
    "Paper Text (truncated)",
    "GO ID",
    "GO Name",
    "GO Definition",
    "GO Synonyms",
    "IS_RELEVANT",
    "GO_EVIDENCE_JSON",
    "GO_REASON",
    "ENTITIES_JSON",
    "Feedback",
    "Timestamp",
]

# ---------------------------------------------------------------------------
# Flask app
# ---------------------------------------------------------------------------
app = Flask(__name__)
CORS(app)


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------
def get_db():
    """Return a per-request database connection."""
    if "db" not in g:
        if IS_POSTGRES:
            g.db = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.DictCursor)
            g.db.autocommit = True
        else:
            os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
            g.db = sqlite3.connect(DB_PATH)
            g.db.row_factory = sqlite3.Row
            g.db.execute("PRAGMA journal_mode=WAL")
    return g.db


@app.teardown_appcontext
def close_db(_exc):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def query_db(query, args=(), one=False, commit=False):
    """Unified query executor for SQLite and Postgres."""
    db = get_db()
    
    if IS_POSTGRES:
        # Translate SQLite ? syntax to Postgres %s syntax
        q = query.replace("?", "%s")
        # Translate SQLite INSERT OR IGNORE to Postgres ON CONFLICT
        if "INSERT OR IGNORE INTO" in q:
            q = q.replace("INSERT OR IGNORE INTO", "INSERT INTO")
            q += " ON CONFLICT (row_hash) DO NOTHING"
            
        with db.cursor() as cur:
            cur.execute(q, args)
            if commit:
                return cur.rowcount
            if q.strip().upper().startswith("SELECT"):
                rv = cur.fetchall()
                return (rv[0] if rv else None) if one else rv
            return cur.rowcount
            
    else: # sqlite3
        cur = db.execute(query, args)
        if commit:
            db.commit()
            return cur.rowcount
        if query.strip().upper().startswith("SELECT"):
            rv = cur.fetchall()
            return (rv[0] if rv else None) if one else rv
        return cur.rowcount


def init_db():
    """Create tables if they do not exist."""
    auto_id = "SERIAL PRIMARY KEY" if IS_POSTGRES else "INTEGER PRIMARY KEY AUTOINCREMENT"
    
    query_db(f"""
        CREATE TABLE IF NOT EXISTS reinforcement_rows (
            id                  {auto_id},
            row_hash            TEXT    UNIQUE NOT NULL,
            installation_id     TEXT    NOT NULL,
            gene                TEXT,
            biological_entity   TEXT,
            enst                TEXT,
            ensg                TEXT,
            paper_id            TEXT,
            paper_title         TEXT,
            paper_text          TEXT,
            go_id               TEXT,
            go_name             TEXT,
            go_definition       TEXT,
            go_synonyms         TEXT,
            verdict             TEXT,
            supporting_citation TEXT,
            reason              TEXT,
            feedback            TEXT,
            original_timestamp  TEXT,
            received_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """, commit=True)
    
    query_db(f"""
        CREATE TABLE IF NOT EXISTS reinforcement_entity_rows (
            id                      {auto_id},
            row_hash                TEXT    UNIQUE NOT NULL,
            installation_id         TEXT    NOT NULL,
            paper_title             TEXT,
            paper_text_truncated    TEXT,
            go_id                   TEXT,
            go_name                 TEXT,
            go_definition           TEXT,
            go_synonyms             TEXT,
            is_relevant             TEXT,
            go_evidence_json        TEXT,
            go_reason               TEXT,
            entities_json           TEXT,
            feedback                TEXT,
            original_timestamp      TEXT,
            received_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """, commit=True)


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------
def require_api_key(f):
    """Decorator: reject requests without a valid X-API-Key header."""
    @wraps(f)
    def decorated(*args, **kwargs):
        key = request.headers.get("X-API-Key", "")
        if not secrets.compare_digest(key, COLLECTION_API_KEY):
            return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return decorated


def require_admin(f):
    """Decorator: HTTP Basic Auth for admin endpoints."""
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if (
            not auth
            or not secrets.compare_digest(auth.username, ADMIN_USER)
            or not secrets.compare_digest(auth.password, ADMIN_PASSWORD)
        ):
            return Response(
                "Unauthorized",
                401,
                {"WWW-Authenticate": 'Basic realm="GOFinder Collection Admin"'},
            )
        return f(*args, **kwargs)
    return decorated


# ---------------------------------------------------------------------------
# Hashing
# ---------------------------------------------------------------------------
def _hash_row(row_dict: dict) -> str:
    """Deterministic SHA-256 hash of a row dict (sorted keys, stripped vals)."""
    canonical = json.dumps(
        {k: str(v).strip() for k, v in sorted(row_dict.items())},
        sort_keys=True,
        ensure_ascii=True,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Column mapping helpers
# ---------------------------------------------------------------------------
# Map incoming JSON keys (from the CSV schema) → DB column names
_REINFORCEMENT_KEY_MAP = {
    "Gene": "gene",
    "Biological Entity of Interest": "biological_entity",
    "Ensembl Transcript Accession ID (ENST)": "enst",
    "Ensembl Gene Accession ID (ENSG)": "ensg",
    "Paper ID": "paper_id",
    "Title of the Paper": "paper_title",
    "Paper Text": "paper_text",
    "GO ID": "go_id",
    "GO Name": "go_name",
    "GO Definition": "go_definition",
    "GO Synonyms": "go_synonyms",
    "Verdict": "verdict",
    "Supporting Text Citation": "supporting_citation",
    "Reason": "reason",
    "Feedback": "feedback",
    "Timestamp": "original_timestamp",
}

_ENTITY_KEY_MAP = {
    "Paper Title": "paper_title",
    "Paper Text (truncated)": "paper_text_truncated",
    "GO ID": "go_id",
    "GO Name": "go_name",
    "GO Definition": "go_definition",
    "GO Synonyms": "go_synonyms",
    "IS_RELEVANT": "is_relevant",
    "GO_EVIDENCE_JSON": "go_evidence_json",
    "GO_REASON": "go_reason",
    "ENTITIES_JSON": "entities_json",
    "Feedback": "feedback",
    "Timestamp": "original_timestamp",
}


def _map_row(row: dict, key_map: dict) -> dict:
    """Map incoming JSON row keys to DB column names."""
    return {db_col: str(row.get(csv_key, "")).strip()
            for csv_key, db_col in key_map.items()}


# ---------------------------------------------------------------------------
# /collect — ingest endpoint
# ---------------------------------------------------------------------------
@app.route("/collect", methods=["POST"])
@require_api_key
def collect():
    """
    Receive reinforcement feedback rows from a local GOFinder instance.

    Payload::

        {
            "installation_id": "uuid4",
            "rows": {
                "reinforcement": [ {row_dict}, ... ],
                "reinforcement_entity": [ {row_dict}, ... ]
            }
        }

    Returns counts of inserted rows and skipped duplicates.
    """
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"error": "Invalid JSON payload"}), 400

    installation_id = data.get("installation_id", "unknown")
    rows_payload = data.get("rows", {})

    if not isinstance(rows_payload, dict):
        return jsonify({"error": "'rows' must be a dict with keys 'reinforcement' and/or 'reinforcement_entity'"}), 400

    db = get_db()
    result = {"inserted": {}, "duplicates_skipped": {}}

    # --- Reinforcement rows ---
    reinf_rows = rows_payload.get("reinforcement", [])
    inserted_r, dupes_r = 0, 0
    for row in reinf_rows:
        row_hash = _hash_row(row)
        mapped = _map_row(row, _REINFORCEMENT_KEY_MAP)
        mapped["row_hash"] = row_hash
        mapped["installation_id"] = installation_id

        cols = ", ".join(mapped.keys())
        placeholders = ", ".join(["?"] * len(mapped))
        try:
            changes = query_db(
                f"INSERT OR IGNORE INTO reinforcement_rows ({cols}) VALUES ({placeholders})",
                list(mapped.values()),
                commit=True
            )
            if changes and changes > 0:
                inserted_r += 1
            else:
                dupes_r += 1
        except Exception as e:
            if "UNIQUE" in str(e).upper() or "INTEGRITY" in str(e).upper():
                dupes_r += 1
            else:
                raise

    result["inserted"]["reinforcement"] = inserted_r
    result["duplicates_skipped"]["reinforcement"] = dupes_r

    # --- Entity rows ---
    entity_rows = rows_payload.get("reinforcement_entity", [])
    inserted_e, dupes_e = 0, 0
    for row in entity_rows:
        row_hash = _hash_row(row)
        mapped = _map_row(row, _ENTITY_KEY_MAP)
        mapped["row_hash"] = row_hash
        mapped["installation_id"] = installation_id

        cols = ", ".join(mapped.keys())
        placeholders = ", ".join(["?"] * len(mapped))
        try:
            changes = query_db(
                f"INSERT OR IGNORE INTO reinforcement_entity_rows ({cols}) VALUES ({placeholders})",
                list(mapped.values()),
                commit=True
            )
            if changes and changes > 0:
                inserted_e += 1
            else:
                dupes_e += 1
        except Exception as e:
            if "UNIQUE" in str(e).upper() or "INTEGRITY" in str(e).upper():
                dupes_e += 1
            else:
                raise

    result["inserted"]["reinforcement_entity"] = inserted_e
    result["duplicates_skipped"]["reinforcement_entity"] = dupes_e

    return jsonify({"status": "ok", **result}), 200


# ---------------------------------------------------------------------------
# /health
# ---------------------------------------------------------------------------
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "timestamp": datetime.utcnow().isoformat()}), 200


# ---------------------------------------------------------------------------
# /admin/stats
# ---------------------------------------------------------------------------
@app.route("/admin/stats", methods=["GET"])
@require_admin
def admin_stats():
    """Return JSON statistics about collected data."""
    for table in ("reinforcement_rows", "reinforcement_entity_rows"):
        row = query_db(f"""
            SELECT
                COUNT(*)                         AS total_rows,
                COUNT(DISTINCT installation_id)  AS unique_installations,
                MIN(received_at)                 AS earliest,
                MAX(received_at)                 AS latest
            FROM {table}
        """, one=True)
        stats[table] = dict(row) if row else {}

    return jsonify(stats), 200


# ---------------------------------------------------------------------------
# /admin/export/<table>
# ---------------------------------------------------------------------------
@app.route("/admin/export/<table_name>", methods=["GET"])
@require_admin
def admin_export(table_name):
    """Download all rows from a table as CSV."""
    allowed = {"reinforcement_rows", "reinforcement_entity_rows"}
    if table_name not in allowed:
        return jsonify({"error": f"Unknown table. Choose from: {allowed}"}), 400

    rows = query_db(f"SELECT * FROM {table_name} ORDER BY id")
    if not rows:
        return Response("No data", mimetype="text/plain")
        
    columns = list(dict(rows[0]).keys()) if IS_POSTGRES else rows[0].keys()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(columns)
    for row in rows:
        writer.writerow(list(row))

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={table_name}.csv"},
    )


# ---------------------------------------------------------------------------
# /admin/dashboard — simple HTML dashboard
# ---------------------------------------------------------------------------
DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>GOFinder Collection Dashboard</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Inter', 'Segoe UI', system-ui, sans-serif;
            background: #0f1117;
            color: #e4e4e7;
            min-height: 100vh;
        }
        .header {
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
            border-bottom: 1px solid rgba(255,255,255,0.06);
            padding: 24px 32px;
        }
        .header h1 {
            font-size: 22px;
            font-weight: 700;
            background: linear-gradient(135deg, #60a5fa, #a78bfa);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }
        .header p { color: #71717a; font-size: 13px; margin-top: 4px; }
        .container { max-width: 1100px; margin: 0 auto; padding: 28px 24px; }
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
            gap: 16px;
            margin-bottom: 32px;
        }
        .stat-card {
            background: rgba(255,255,255,0.04);
            border: 1px solid rgba(255,255,255,0.06);
            border-radius: 12px;
            padding: 20px;
        }
        .stat-card .label { font-size: 12px; color: #71717a; text-transform: uppercase; letter-spacing: 0.5px; }
        .stat-card .value { font-size: 28px; font-weight: 700; margin-top: 6px; color: #f4f4f5; }
        .stat-card .sub { font-size: 12px; color: #52525b; margin-top: 4px; }
        .section-title {
            font-size: 16px; font-weight: 600; margin-bottom: 14px;
            display: flex; align-items: center; gap: 8px;
        }
        .section-title .badge {
            background: rgba(96,165,250,0.15);
            color: #60a5fa;
            font-size: 11px;
            padding: 2px 8px;
            border-radius: 6px;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 13px;
            margin-bottom: 28px;
        }
        th {
            text-align: left;
            padding: 10px 12px;
            color: #71717a;
            font-weight: 500;
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            border-bottom: 1px solid rgba(255,255,255,0.06);
        }
        td {
            padding: 10px 12px;
            border-bottom: 1px solid rgba(255,255,255,0.03);
            max-width: 200px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }
        tr:hover td { background: rgba(255,255,255,0.02); }
        .btn {
            display: inline-block;
            padding: 8px 18px;
            border-radius: 8px;
            font-size: 13px;
            font-weight: 500;
            text-decoration: none;
            cursor: pointer;
            border: 1px solid rgba(255,255,255,0.1);
            background: rgba(255,255,255,0.05);
            color: #e4e4e7;
            transition: all 0.15s;
        }
        .btn:hover { background: rgba(255,255,255,0.08); border-color: rgba(255,255,255,0.15); }
        .btn-primary { background: rgba(96,165,250,0.15); color: #60a5fa; border-color: rgba(96,165,250,0.3); }
        .btn-primary:hover { background: rgba(96,165,250,0.25); }
        .export-bar { display: flex; gap: 10px; margin-bottom: 28px; }
        .empty-state { color: #52525b; padding: 32px; text-align: center; }
    </style>
</head>
<body>
    <div class="header">
        <h1>GOFinder Collection Dashboard</h1>
        <p>Reinforcement feedback collected from local GOFinder AI instances</p>
    </div>
    <div class="container">
        <div class="stats-grid">
            <div class="stat-card">
                <div class="label">Reinforcement Rows</div>
                <div class="value">{{ r_stats.total_rows or 0 }}</div>
                <div class="sub">{{ r_stats.unique_installations or 0 }} installation(s)</div>
            </div>
            <div class="stat-card">
                <div class="label">Entity Rows</div>
                <div class="value">{{ e_stats.total_rows or 0 }}</div>
                <div class="sub">{{ e_stats.unique_installations or 0 }} installation(s)</div>
            </div>
            <div class="stat-card">
                <div class="label">Total Rows</div>
                <div class="value">{{ (r_stats.total_rows or 0) + (e_stats.total_rows or 0) }}</div>
                <div class="sub">
                    {% if r_stats.earliest %}Since {{ r_stats.earliest[:10] }}{% else %}No data yet{% endif %}
                </div>
            </div>
        </div>

        <div class="export-bar">
            <a class="btn btn-primary" href="/admin/export/reinforcement_rows">⬇ Export Reinforcement CSV</a>
            <a class="btn btn-primary" href="/admin/export/reinforcement_entity_rows">⬇ Export Entity CSV</a>
        </div>

        <div class="section-title">
            Recent Reinforcement Rows <span class="badge">last 20</span>
        </div>
        {% if recent_r %}
        <table>
            <thead><tr>
                <th>ID</th><th>Installation</th><th>GO ID</th><th>GO Name</th>
                <th>Feedback</th><th>Verdict</th><th>Received</th>
            </tr></thead>
            <tbody>
            {% for row in recent_r %}
            <tr>
                <td>{{ row.id }}</td>
                <td title="{{ row.installation_id }}">{{ row.installation_id[:8] }}…</td>
                <td>{{ row.go_id }}</td>
                <td>{{ row.go_name }}</td>
                <td>{{ row.feedback }}</td>
                <td>{{ row.verdict }}</td>
                <td>{{ row.received_at }}</td>
            </tr>
            {% endfor %}
            </tbody>
        </table>
        {% else %}
        <div class="empty-state">No reinforcement rows collected yet.</div>
        {% endif %}

        <div class="section-title">
            Recent Entity Rows <span class="badge">last 20</span>
        </div>
        {% if recent_e %}
        <table>
            <thead><tr>
                <th>ID</th><th>Installation</th><th>GO ID</th><th>GO Name</th>
                <th>Relevant</th><th>Feedback</th><th>Received</th>
            </tr></thead>
            <tbody>
            {% for row in recent_e %}
            <tr>
                <td>{{ row.id }}</td>
                <td title="{{ row.installation_id }}">{{ row.installation_id[:8] }}…</td>
                <td>{{ row.go_id }}</td>
                <td>{{ row.go_name }}</td>
                <td>{{ row.is_relevant }}</td>
                <td>{{ row.feedback }}</td>
                <td>{{ row.received_at }}</td>
            </tr>
            {% endfor %}
            </tbody>
        </table>
        {% else %}
        <div class="empty-state">No entity rows collected yet.</div>
        {% endif %}
    </div>
</body>
</html>
"""


@app.route("/admin/dashboard", methods=["GET"])
@require_admin
def admin_dashboard():
    """Render a simple admin dashboard with stats and recent rows."""
    r_stats = query_db("""
        SELECT COUNT(*) AS total_rows,
               COUNT(DISTINCT installation_id) AS unique_installations,
               MIN(received_at) AS earliest,
               MAX(received_at) AS latest
        FROM reinforcement_rows
    """, one=True)

    e_stats = query_db("""
        SELECT COUNT(*) AS total_rows,
               COUNT(DISTINCT installation_id) AS unique_installations,
               MIN(received_at) AS earliest,
               MAX(received_at) AS latest
        FROM reinforcement_entity_rows
    """, one=True)

    recent_r = query_db("SELECT * FROM reinforcement_rows ORDER BY id DESC LIMIT 20")
    recent_e = query_db("SELECT * FROM reinforcement_entity_rows ORDER BY id DESC LIMIT 20")

    return render_template_string(
        DASHBOARD_HTML,
        r_stats=dict(r_stats),
        e_stats=dict(e_stats),
        recent_r=recent_r,
        recent_e=recent_e,
    )


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------
with app.app_context():
    init_db()

if __name__ == "__main__":
    print(f"  Collection server starting on port {COLLECTION_PORT}")
    print(f"  Dashboard: http://localhost:{COLLECTION_PORT}/admin/dashboard")
    app.run(host="0.0.0.0", port=COLLECTION_PORT, debug=True)
