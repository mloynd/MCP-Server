from fastapi import FastAPI, Request
from pymongo import MongoClient
from handler import handle_crud, handle_schema_memory, query_schemas, handle_log
import os

app = FastAPI()

client = MongoClient(os.getenv("MONGO_URI"))
db = client["MSE"]
schemas = db["schemas"]

@app.post("/mcp")
async def mcp_entry(request: Request):
    payload = await request.json()
    operation = payload.get("operation")

    # Operation inference if not provided
    if not operation:
        if "command" in payload and "collection" in payload:
            operation = "crud"
        elif "action" in payload and "target" in payload:
            operation = "crud"
        elif "command" in payload and "schema_id" in payload:
            operation = "schema_memory"

    if operation == "crud":
        return handle_crud(payload, db)
    elif operation == "schema_memory":
        return handle_schema_memory(payload, db)
    elif operation == "log_conversation":
        return handle_log(payload, db)

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
