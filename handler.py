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
    payload = await request.json()
    op = payload.get("operation")
    col = db[payload.get("collection")]

    if op == "create":
        result = col.insert_one(payload.get("data"))
        return {"status": "ok", "inserted_id": str(result.inserted_id)}
    
    return {"status": "unsupported operation"}
