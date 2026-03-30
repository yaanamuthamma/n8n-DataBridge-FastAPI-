# n8n Data Pipelines — Controlled 2-Stage Pipeline

Fixed 2-stage ETL pipeline: MongoDB → ClickHouse → Syntheta. No destination choice — the flow is fixed.

## Architecture

```
MongoDB (user-provided credentials)
    │
    ▼  Stage 1: Extract + Flatten + Insert/Update to ClickHouse
ClickHouse (user-provided credentials)
    │
    ▼  Stage 2: MANUAL TRIGGER ONLY — Type-map + Load to Syntheta
Syntheta (connection_id based — internal MySQL or external DB)
```

## Stage 1: MongoDB → ClickHouse

Triggered by "Sync to ClickHouse" (insert) or "Update Data" (update) buttons.

### Mode Behavior

| Mode | Behavior |
|------|----------|
| `insert` | Creates table if not exists, inserts data normally |
| `update` | If table exists → TRUNCATE then insert. If not → create + insert |

No duplicate tables. No duplicate data. No new database per run.

### Payload

```json
{
  "source": {
    "type": "mongodb",
    "host": "localhost",
    "port": 27017,
    "user": "myuser",
    "password": "mypass",
    "database": "my_app_db",
    "authDb": "admin"
  },
  "syncMode": "database",
  "collection": null,
  "mode": "insert",
  "clickhouse": {
    "host": "34.93.96.87",
    "port": 8123,
    "database": "Syntheta_logs",
    "user": "admin",
    "password": "secret"
  }
}
```

## Stage 2: ClickHouse → Syntheta

Triggered ONLY by "Send to Syntheta" button. Never runs automatically.

### Connection Logic

| connection_id | Behavior |
|---------------|----------|
| `0` | Uses internal MySQL from `.env` (MYSQL_HOST, MYSQL_DATABASE, etc.) |
| `> 0` | Fetches credentials from Syntheta's `DBconnection` table |

Supported DB types: MySQL, ClickHouse, PostgreSQL, MSSQL, MariaDB, SQLite.

### Payload

```json
{
  "source": {
    "type": "clickhouse",
    "host": "34.93.96.87",
    "port": 8123,
    "user": "admin",
    "password": "secret",
    "database": "Syntheta_logs"
  },
  "syncMode": "database",
  "table": null,
  "destination": {
    "connection_id": 0
  }
}
```

## UI Buttons

| Button | Action | Stage |
|--------|--------|-------|
| Sync to ClickHouse | Stage 1 with `mode: insert` | 1 |
| Update Data | Stage 1 with `mode: update` (truncate + insert) | 1 |
| Send to Syntheta | Stage 2 — manual only | 2 |

## Quick Start

```bash
cp .env.example .env
# Edit .env with your credentials
docker-compose up -d
```

Import workflows into n8n:
- `workflows/mongo_to_clickhouse.json` — Stage 1
- `workflows/clickhouse_to_syntheta.json` — Stage 2

Open `trigger/index.html` in a browser.

## Key Rules

- No env fallbacks in Stage 1 — everything from payload
- Stage 2 uses env ONLY for `connection_id=0` (internal MySQL)
- No "full pipeline" mode — stages are independent
- Tables in Syntheta use `syn_` prefix
- Stage 2 does DROP + CREATE to prevent duplicates
