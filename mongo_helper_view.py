from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional, List
from pymongo import MongoClient
from main import api_logger

router31 = APIRouter()


class MongoListRequest(BaseModel):
    host: str
    port: int = 27017
    database: str
    user: Optional[str] = None
    password: Optional[str] = None
    authDb: str = "admin"


@router31.post("/api/mongo/list-collections")
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

        api_logger.info(f"Listed {len(collections)} collections from {req.database}")
        return {"collections": sorted(collections)}

    except Exception as e:
        api_logger.error(f"Failed to list MongoDB collections: {e}")
        return {"collections": [], "error": str(e)}
