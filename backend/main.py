from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import uuid
import time
import asyncio
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


# ══════════════════════════════════════════════════════════
#  IN-MEMORY STORES (task queue + agent registry)
# ══════════════════════════════════════════════════════════

# Registered agents: { agent_id: { token, name, last_seen } }
agents: Dict[str, dict] = {}

# Task queue: { task_id: { agent_id, type, params, status, result, created_at } }
tasks: Dict[str, dict] = {}

# Simple shared secret — agent must send this to register
AGENT_SECRET = os.environ.get("AGENT_SECRET", "datasync-secret-2024")


# ══════════════════════════════════════════════════════════
#  MODELS
# ══════════════════════════════════════════════════════════

class AgentRegisterRequest(BaseModel):
    name: str = "default-agent"
    secret: str

class TaskSubmitRequest(BaseModel):
    task_type: str          # "mongo_list_collections", "ch_list_tables", "ch_create_union"
    params: Dict[str, Any]

class TaskResultRequest(BaseModel):
    task_id: str
    result: Dict[str, Any]


# ══════════════════════════════════════════════════════════
#  AGENT ENDPOINTS (called by the local agent)
# ══════════════════════════════════════════════════════════

@app.post("/agent/register")
async def register_agent(req: AgentRegisterRequest):
    """Agent calls this once on startup to get an agent_id + token."""
    if req.secret != AGENT_SECRET:
        raise HTTPException(status_code=403, detail="Invalid secret")

    agent_id = str(uuid.uuid4())[:8]
    token = str(uuid.uuid4())
    agents[agent_id] = {"token": token, "name": req.name, "last_seen": time.time()}
    logger.info(f"Agent registered: {agent_id} ({req.name})")
    return {"agent_id": agent_id, "token": token}


def verify_agent(agent_id: str, token: str):
    """Check agent_id + token are valid."""
    agent = agents.get(agent_id)
    if not agent or agent["token"] != token:
        raise HTTPException(status_code=403, detail="Invalid agent credentials")
    agent["last_seen"] = time.time()
    return agent


@app.get("/agent/{agent_id}/tasks")
async def get_pending_tasks(agent_id: str, token: str = Header(alias="X-Agent-Token")):
    """Agent polls this every few seconds to get pending tasks."""
    verify_agent(agent_id, token)

    pending = []
    for task_id, task in tasks.items():
        if task["agent_id"] == agent_id and task["status"] == "pending":
            pending.append({
                "task_id": task_id,
                "task_type": task["task_type"],
                "params": task["params"],
            })
    return {"tasks": pending}


@app.post("/agent/{agent_id}/result")
async def submit_task_result(agent_id: str, req: TaskResultRequest,
                             token: str = Header(alias="X-Agent-Token")):
    """Agent sends back the result of a completed task."""
    verify_agent(agent_id, token)

    task = tasks.get(req.task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    task["status"] = "completed"
    task["result"] = req.result
    task["completed_at"] = time.time()
    logger.info(f"Task {req.task_id} completed by agent {agent_id}")
    return {"status": "ok"}


# ══════════════════════════════════════════════════════════
#  UI-FACING ENDPOINTS (called by the browser)
# ══════════════════════════════════════════════════════════

def find_active_agent() -> Optional[str]:
    """Find an agent that was seen in the last 30 seconds."""
    now = time.time()
    for agent_id, agent in agents.items():
        if now - agent["last_seen"] < 30:
            return agent_id
    return None


def create_task(task_type: str, params: dict) -> str:
    """Create a task and assign it to an active agent."""
    agent_id = find_active_agent()
    if not agent_id:
        raise HTTPException(status_code=503, detail="No agent connected. Run agent.py on your machine first.")

    task_id = str(uuid.uuid4())[:12]
    tasks[task_id] = {
        "agent_id": agent_id,
        "task_type": task_type,
        "params": params,
        "status": "pending",
        "result": None,
        "created_at": time.time(),
        "completed_at": None,
    }
    logger.info(f"Task {task_id} ({task_type}) created for agent {agent_id}")
    return task_id


async def wait_for_result(task_id: str, timeout: int = 30) -> dict:
    """Wait for the agent to complete the task (poll every 0.3s)."""
    start = time.time()
    while time.time() - start < timeout:
        task = tasks.get(task_id)
        if task and task["status"] == "completed":
            result = task["result"]
            # Clean up old task
            del tasks[task_id]
            return result
        await asyncio.sleep(0.3)
    # Timeout — clean up
    if task_id in tasks:
        del tasks[task_id]
    raise HTTPException(status_code=504, detail="Agent did not respond in time. Is agent.py running?")


@app.post("/api/mongo/list-collections")
async def list_mongo_collections(req: dict):
    task_id = create_task("mongo_list_collections", req)
    return await wait_for_result(task_id)


@app.post("/api/clickhouse/list-tables")
async def list_clickhouse_tables(req: dict):
    task_id = create_task("ch_list_tables", req)
    return await wait_for_result(task_id)


@app.post("/api/clickhouse/create-union")
async def create_or_refresh_union(req: dict):
    task_id = create_task("ch_create_union", req)
    return await wait_for_result(task_id)


# ══════════════════════════════════════════════════════════
#  STATUS + HTML
# ══════════════════════════════════════════════════════════

@app.get("/agent/status")
async def agent_status():
    """Check if any agent is connected (UI can show this)."""
    agent_id = find_active_agent()
    if agent_id:
        return {"connected": True, "agent_id": agent_id, "name": agents[agent_id]["name"]}
    return {"connected": False}


@app.get("/", response_class=HTMLResponse)
async def serve_ui():
    base = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.join(base, "..", "trigger", "index.html"),
        os.path.join(os.getcwd(), "trigger", "index.html"),
    ]
    for path in candidates:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return HTMLResponse(content=f.read())
    return HTMLResponse(content="<h1>index.html not found</h1>", status_code=500)
