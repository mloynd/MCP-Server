from fastapi import FastAPI, Request
from pymongo import MongoClient
from handler import handle_crud, handle_schema_memory, query_schemas
import os

app = FastAPI()

# Connect to MongoDB
client = MongoClient(os.getenv("MONGO_URI"))
db = client["MSE1"]
schemas = db["Dogs"]

@app.post("/mcp")
async def mcp_entry(request: Request):
    payload = await request.json()
    operation = payload.get("operation")

    # Operation inference if not provided
    if not operation:
        if "command" in payload and "collection" in payload:
            operation = "schema_memory"
        elif "action" in payload and "target" in payload:
            operation = "crud"

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
