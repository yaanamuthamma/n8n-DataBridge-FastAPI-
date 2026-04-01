"""
Syntheta DataSync Local Agent
─────────────────────────────
Run this on your machine to bridge the Render backend with your local databases.

Usage:
    pip install pymongo requests
    python agent.py

It will:
  1. Register with the Render backend
  2. Poll for tasks every 2 seconds
  3. Execute tasks against your local MongoDB / ClickHouse
  4. Send results back to the backend
"""

import time
import requests
import sys

# ── CONFIG ──────────────────────────────────────────────
BACKEND_URL = "https://n8n-databridge-fastapi.onrender.com"
AGENT_SECRET = "datasync-secret-2024"
AGENT_NAME = "local-agent"
POLL_INTERVAL = 2  # seconds
# ────────────────────────────────────────────────────────

agent_id = None
agent_token = None


def register():
    """Register this agent with the backend (retries if Render is waking up)."""
    global agent_id, agent_token
    for attempt in range(3):
        try:
            print(f"Connecting to {BACKEND_URL}... (attempt {attempt + 1}/3)")
            if attempt == 0:
                print("  (First request may take ~30s if Render is waking up)")
            resp = requests.post(f"{BACKEND_URL}/agent/register", json={
                "name": AGENT_NAME,
                "secret": AGENT_SECRET,
            }, timeout=60)
            if resp.status_code != 200:
                print(f"Registration failed: {resp.status_code} {resp.text}")
                sys.exit(1)
            data = resp.json()
            agent_id = data["agent_id"]
            agent_token = data["token"]
            print(f"Registered as agent: {agent_id}")
            return
        except requests.exceptions.Timeout:
            print(f"  Timeout — Render is still waking up, retrying...")
        except requests.exceptions.ConnectionError as e:
            print(f"  Connection error: {e}, retrying...")
        time.sleep(5)
    print("Failed to connect after 3 attempts. Check your internet or Render status.")
    sys.exit(1)


def poll_tasks():
    """Ask the backend if there are any pending tasks."""
    resp = requests.get(
        f"{BACKEND_URL}/agent/{agent_id}/tasks",
        headers={"X-Agent-Token": agent_token},
        timeout=10,
    )
    if resp.status_code != 200:
        print(f"Poll error: {resp.status_code}")
        return []
    return resp.json().get("tasks", [])


def send_result(task_id, result):
    """Send task result back to the backend."""
    resp = requests.post(
        f"{BACKEND_URL}/agent/{agent_id}/result",
        headers={"X-Agent-Token": agent_token},
        json={"task_id": task_id, "result": result},
        timeout=10,
    )
    if resp.status_code != 200:
        print(f"Failed to send result for {task_id}: {resp.text}")


# ══════════════════════════════════════════════════════════
#  TASK HANDLERS — these run on YOUR machine
# ══════════════════════════════════════════════════════════

def handle_mongo_list_collections(params):
    """Connect to local MongoDB and list collections."""
    from pymongo import MongoClient

    host = params.get("host", "localhost")
    port = int(params.get("port", 27017))
    database = params.get("database", "")
    user = params.get("user") or None
    password = params.get("password") or None
    auth_db = params.get("authDb", "admin")

    if user and password:
        uri = f"mongodb://{user}:{password}@{host}:{port}/{database}?authSource={auth_db}"
    else:
        uri = f"mongodb://{host}:{port}/{database}"

    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    db = client[database]
    collections = [c for c in db.list_collection_names() if not c.startswith("system.")]
    client.close()
    return {"collections": sorted(collections)}


def handle_ch_list_tables(params):
    """Connect to local ClickHouse and list tables."""
    host = params.get("chHost", "localhost")
    port = int(params.get("chPort", 8123))
    user = params.get("chUser", "default")
    password = params.get("chPassword", "")
    database = params.get("database", "")

    if not database:
        return {"tables": [], "error": "Database name is required"}

    resp = requests.post(
        f"http://{host}:{port}/",
        params={"user": user, "password": password,
                "query": f"SHOW TABLES FROM {database} FORMAT JSON"},
        timeout=300,
    )
    if resp.status_code != 200:
        return {"tables": [], "error": f"ClickHouse error: {resp.text}"}

    data = resp.json()
    tables = [row["name"] for row in data.get("data", [])]
    return {"tables": sorted(tables)}


def handle_ch_create_union(params):
    """Create or refresh a union table in local ClickHouse."""
    host = params.get("chHost", "localhost")
    port = int(params.get("chPort", 8123))
    user = params.get("chUser", "default")
    password = params.get("chPassword", "")
    database = params.get("database", "")
    union = params.get("unionTableName", "")
    source_tables = params.get("sourceTables", [])
    pk = params.get("primaryKey", "")

    def ch_q(query):
        r = requests.post(f"http://{host}:{port}/",
                          params={"user": user, "password": password, "query": query}, timeout=300)
        if r.status_code != 200:
            raise Exception(f"ClickHouse error: {r.text}")
        return r.text

    def ch_qj(query):
        r = requests.post(f"http://{host}:{port}/",
                          params={"user": user, "password": password, "query": query + " FORMAT JSON"}, timeout=300)
        if r.status_code != 200:
            raise Exception(f"ClickHouse error: {r.text}")
        return r.json()

    if not source_tables or len(source_tables) < 2:
        return {"status": "error", "message": "At least 2 source tables required"}

    schema = ch_qj(f"DESCRIBE TABLE {database}.{source_tables[0]}")
    columns = schema.get("data", [])
    if not columns:
        return {"status": "error", "message": f"No columns found in {source_tables[0]}"}

    col_defs, col_names = [], []
    for col in columns:
        col_defs.append(f"{col['name']} {col['type']}")
        col_names.append(col['name'])
    col_defs.append("updated_at DateTime")
    col_defs.append("source_table String")

    exists = ch_q(f"EXISTS TABLE {database}.{union}").strip() == "1"

    if not exists:
        ch_q(f"CREATE TABLE {database}.{union} ({', '.join(col_defs)}) "
             f"ENGINE = ReplacingMergeTree(updated_at) ORDER BY ({pk})")

    parts = []
    for t in source_tables:
        parts.append(f"SELECT {', '.join(col_names)}, now() AS updated_at, '{t}' AS source_table FROM {database}.{t}")
    ch_q(f"INSERT INTO {database}.{union} {' UNION ALL '.join(parts)}")

    try:
        ch_q(f"OPTIMIZE TABLE {database}.{union} FINAL")
    except:
        pass

    count = ch_qj(f"SELECT count() as cnt FROM {database}.{union} FINAL")
    row_count = count.get("data", [{}])[0].get("cnt", 0)
    action = "created and populated" if not exists else "refreshed"

    return {"status": "success", "action": action, "table": f"{database}.{union}",
            "sourceTables": source_tables, "rowCount": row_count}


# ══════════════════════════════════════════════════════════
#  TASK ROUTER
# ══════════════════════════════════════════════════════════

HANDLERS = {
    "mongo_list_collections": handle_mongo_list_collections,
    "ch_list_tables": handle_ch_list_tables,
    "ch_create_union": handle_ch_create_union,
}


def execute_task(task):
    """Route a task to the right handler."""
    task_type = task["task_type"]
    task_id = task["task_id"]
    params = task["params"]

    handler = HANDLERS.get(task_type)
    if not handler:
        return {"error": f"Unknown task type: {task_type}"}

    print(f"  Executing: {task_type} (task {task_id})")
    try:
        result = handler(params)
        print(f"  Done: {task_type}")
        return result
    except Exception as e:
        print(f"  Error: {e}")
        return {"error": str(e)}


# ══════════════════════════════════════════════════════════
#  MAIN LOOP
# ══════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 50)
    print("  Syntheta DataSync — Local Agent")
    print("=" * 50)

    register()
    print(f"Polling for tasks every {POLL_INTERVAL}s...")
    print("Keep this running. Press Ctrl+C to stop.\n")

    while True:
        try:
            pending = poll_tasks()
            for task in pending:
                result = execute_task(task)
                send_result(task["task_id"], result)
        except KeyboardInterrupt:
            print("\nAgent stopped.")
            break
        except Exception as e:
            print(f"Poll error: {e}")
        time.sleep(POLL_INTERVAL)
