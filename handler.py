from fastapi import FastAPI, Request
from pymongo import MongoClient
import os
import sys

app = FastAPI()

# ✅ Secure MongoDB connection with validation
uri = os.getenv("MONGO_URI")
if not uri:
    print("❌ ERROR: MONGO_URI not set in environment", file=sys.stderr)
    raise RuntimeError("Missing MONGO_URI")

try:
    client = MongoClient(uri)
    db = client["MSE1"]
    print("✅ MongoDB connection initialized.")
except Exception as e:
    print("❌ MongoDB connection failed:", e, file=sys.stderr)
    raise e

# ✅ Health check route
@app.get("/")
async def root():
    return {"status": "MCP server is live"}

# ✅ Main MCP route
@app.post("/mcp")
async def handle_mcp(request: Request):
    try:
        payload = await request.json()
        print("✅ Received payload:", payload)
    except Exception as e:
        print("❌ JSON parsing error:", e)
        return {"status": "invalid JSON", "error": str(e)}

    op = payload.get("operation")
    collection_name = payload.get("collection")
    data = payload.get("data")

    if not all([op, collection_name, data]):
        print("❌ Missing required fields")
        return {"status": "error", "message": "Missing required fields"}

    try:
        collection = db[collection_name]
        if op == "create":
            result = collection.insert_one(data)
            print(f"✅ Document inserted into '{collection_name}' with ID: {result.inserted_id}")
            return {"status": "ok", "inserted_id": str(result.inserted_id)}
        else:
            print("⚠️ Unsupported operation:", op)
            return {"status": "unsupported operation"}
    except Exception as e:
        print("❌ MongoDB Error:", e)
        return {"status": "error", "message": "MongoDB operation failed", "detail": str(e)}
