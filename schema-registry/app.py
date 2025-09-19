from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime
import os
from pymongo import MongoClient, ReturnDocument

MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017")
DB = os.getenv("MONGO_DB", "schema_db")
COLL = os.getenv("MONGO_COLLECTION", "schemas")

client = MongoClient(MONGO_URL)
col = client[DB][COLL]
app = FastAPI(title="Simple Schema Registry")

class SchemaDoc(BaseModel):
    name: str
    version: int | None = None
    schema: dict

@app.get("/schemas/{name}")
def get_latest(name: str, version: int | None = None):
    q = {"name": name} if version is None else {"name": name, "version": version}
    doc = col.find_one(q, sort=[("version",-1)]) if version is None else col.find_one(q)
    if not doc: raise HTTPException(404, "Not found")
    return {"name": doc["name"], "version": doc["version"], "schema": doc["schema"]}

@app.put("/schemas/{name}")
def put_schema(name: str, payload: SchemaDoc):
    # auto-increment version if not provided
    latest = col.find_one({"name": name}, sort=[("version",-1)])
    next_ver = payload.version or ((latest["version"] + 1) if latest else 1)
    doc = {"name": name, "version": next_ver, "schema": payload.schema, "ts": datetime.utcnow()}
    col.find_one_and_replace({"name": name, "version": next_ver}, doc, upsert=True, return_document=ReturnDocument.AFTER)
    return {"ok": True, "name": name, "version": next_ver}
