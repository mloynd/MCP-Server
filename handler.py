from fastapi import FastAPI, Request
from pymongo import MongoClient
import os

app = FastAPI()
client = MongoClient(os.getenv("MONGO_URI"))
db = client["MSE1"]

@app.get("/")
async def root():
    return {"status": "MCP server is live"}

@app.post("/mcp")
async def handle_mcp(request: Request):
    try:
        payload = await request.json()
        print("✅ Received payload:", payload)
    except Exception as e:
        print("❌ JSON parsing error:", e)
        return {"status": "invalid JSON", "error": str(e)}
    
    op = payload.get("operation")
    collection = payload.get("collection")
    data = payload.get("data")

    print(f"Operation: {op}, Collection: {collection}, Data: {data}")

    if op == "create":
        result = db[collection].insert_one(data)
        return {"status": "ok", "inserted_id": str(result.inserted_id)}

    return {"status": "unsupported operation"}
