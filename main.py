# main.py

from fastapi import FastAPI, Request
from pymongo import MongoClient
from handler import handle_crud, handle_schema_memory, query_schemas
import os

app = FastAPI()
client = MongoClient(os.getenv("MONGO_URI"))
db = client["MSE"]
schemas = db["schemas"]

@app.post("/mcp")
async def mcp_entry(request: Request):
    payload = await request.json()
    operation = payload.get("operation")

    if operation == "crud":
        return handle_crud(payload, db)
    elif operation == "schema_memory":
        return handle_schema_memory(payload, db)

    return {"error": "unsupported operation"}

@app.get("/schemas/discover")
async def discover_schemas(domain: str = None, tags: str = None, keyword: str = None):
    filter = {}
    if domain:
        filter["domain"] = domain
    if tags:
        filter["tags"] = tags.split(",")
    if keyword:
        filter["keyword"] = keyword
    return query_schemas(filter, schemas)