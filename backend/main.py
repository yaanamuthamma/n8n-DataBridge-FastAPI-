from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional, List
from pymongo import MongoClient
import requests
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("datasync")

app = FastAPI(title="Syntheta DataSync API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Models ──────────────────────────────────────────────

class MongoListRequest(BaseModel):
    host: str
    port: int = 27017
    database: str
    user: Optional[str] = None
    password: Optional[str] = None
    authDb: str = "admin"


class UnionTableRequest(BaseModel):
    chHost: str = "localhost"
    chPort: int = 8123
    chUser: str = "default"
    chPassword: str = ""
    database: str
    unionTableName: str
    sourceTables: List[str]
    primaryKey: str


# ── ClickHouse helpers ──────────────────────────────────

def ch_query(host, port, user, password, query):
    resp = requests.post(
        f"http://{host}:{port}/",
        params={"user": user, "password": password, "query": query},
        timeout=300,
    )
    if resp.status_code != 200:
        raise Exception(f"ClickHouse error: {resp.text}")
    return resp.text


def ch_query_json(host, port, user, password, query):
    resp = requests.post(
        f"http://{host}:{port}/",
        params={"user": user, "password": password, "query": query + " FORMAT JSON"},
        timeout=300,
    )
    if resp.status_code != 200:
        raise Exception(f"ClickHouse error: {resp.text}")
    return resp.json()


# ── Routes ──────────────────────────────────────────────

@app.post("/api/mongo/list-collections")
async def list_mongo_collections(req: MongoListRequest):
    try:
        user = req.user if req.user else None
        password = req.password if req.password else None
        if user and password:
            uri = f"mongodb://{user}:{password}@{req.host}:{req.port}/{req.database}?authSource={req.authDb}"
        else:
            uri = f"mongodb://{req.host}:{req.port}/{req.database}"
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        db = client[req.database]
        collections = [c for c in db.list_collection_names() if not c.startswith("system.")]
        client.close()
        logger.info(f"Listed {len(collections)} collections from {req.database}")
        return {"collections": sorted(collections)}
    except Exception as e:
        logger.error(f"Failed to list MongoDB collections: {e}")
        return {"collections": [], "error": str(e)}


@app.post("/api/clickhouse/list-tables")
async def list_clickhouse_tables(req: dict):
    try:
        host = req.get("chHost", "localhost")
        port = req.get("chPort", 8123)
        user = req.get("chUser", "default")
        password = req.get("chPassword", "")
        database = req.get("database", "")
        if not database:
            return {"tables": [], "error": "Database name is required"}
        result = ch_query_json(host, port, user, password, f"SHOW TABLES FROM {database}")
        tables = [row["name"] for row in result.get("data", [])]
        return {"tables": sorted(tables)}
    except Exception as e:
        logger.error(f"Failed to list ClickHouse tables: {e}")
        return {"tables": [], "error": str(e)}


@app.post("/api/clickhouse/create-union")
async def create_or_refresh_union(req: UnionTableRequest):
    try:
        db = req.database
        union = req.unionTableName
        tables = req.sourceTables
        pk = req.primaryKey
        ch = (req.chHost, req.chPort, req.chUser, req.chPassword)

        if not tables or len(tables) < 2:
            return {"status": "error", "message": "At least 2 source tables required"}

        schema_result = ch_query_json(*ch, f"DESCRIBE TABLE {db}.{tables[0]}")
        columns = schema_result.get("data", [])
        if not columns:
            return {"status": "error", "message": f"No columns found in {tables[0]}"}

        col_defs, col_names = [], []
        for col in columns:
            col_defs.append(f"{col['name']} {col['type']}")
            col_names.append(col['name'])
        col_defs.append("updated_at DateTime")
        col_defs.append("source_table String")

        exists_result = ch_query(*ch, f"EXISTS TABLE {db}.{union}")
        table_exists = exists_result.strip() == "1"

        if not table_exists:
            create_sql = (
                f"CREATE TABLE {db}.{union} (\n"
                f"  {', '.join(col_defs)}\n"
                f") ENGINE = ReplacingMergeTree(updated_at)\n"
                f"ORDER BY ({pk})"
            )
            ch_query(*ch, create_sql)
            logger.info(f"Created union table {db}.{union}")

        select_parts = []
        for t in tables:
            cols_str = ", ".join(col_names)
            select_parts.append(
                f"SELECT {cols_str}, now() AS updated_at, '{t}' AS source_table FROM {db}.{t}"
            )
        ch_query(*ch, f"INSERT INTO {db}.{union} {' UNION ALL '.join(select_parts)}")

        try:
            ch_query(*ch, f"OPTIMIZE TABLE {db}.{union} FINAL")
        except Exception as opt_err:
            logger.warning(f"OPTIMIZE failed (non-critical): {opt_err}")

        count_result = ch_query_json(*ch, f"SELECT count() as cnt FROM {db}.{union} FINAL")
        row_count = count_result.get("data", [{}])[0].get("cnt", 0)
        action = "created and populated" if not table_exists else "refreshed"
        logger.info(f"Union table {db}.{union} {action} with {row_count} rows")

        return {
            "status": "success", "action": action,
            "table": f"{db}.{union}", "sourceTables": tables, "rowCount": row_count,
        }
    except Exception as e:
        logger.error(f"Failed to create/refresh union table: {e}")
        return {"status": "error", "message": str(e)}


# ── Serve the HTML UI ───────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def serve_ui():
    # Works both locally (cd backend/) and on Render (root = n8n-pipelines/)
    base = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.join(base, "..", "trigger", "index.html"),  # local dev
        os.path.join(os.getcwd(), "trigger", "index.html"),  # Render
    ]
    for path in candidates:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return HTMLResponse(content=f.read())
    return HTMLResponse(content="<h1>index.html not found</h1>", status_code=500)
