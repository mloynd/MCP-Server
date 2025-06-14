# handler.py

import uuid
from datetime import datetime

def handle_crud(payload, db):
    operation = payload.get("command")
    collection = db[payload.get("collection")]

    if operation == "create":
        result = collection.insert_one(payload.get("data", {}))
        return {"status": "created", "inserted_id": str(result.inserted_id)}

    elif operation == "read":
        query = payload.get("filter", {})
        return list(collection.find(query, {"_id": 0}))

    elif operation == "update":
        query = payload.get("filter", {})
        update = {"$set": payload.get("update", {})}
        result = collection.update_many(query, update)
        return {"status": "updated", "modified_count": result.modified_count}

    elif operation == "delete":
        query = payload.get("filter", {})
        result = collection.delete_many(query)
        return {"status": "deleted", "deleted_count": result.deleted_count}

    else:
        return {"error": "unsupported crud command"}

def handle_schema_memory(payload, db):
    schemas = db["schemas"]
    instances = db["instances"]
    command = payload.get("command")

    if command == "create_schema":
        schemas.insert_one({
            "schema_id": payload["schema_id"],
            "description": payload["description"],
            "domain": payload["domain"],
            "version": payload.get("version", "1.0.0"),
            "fields": payload["fields"],
            "created_by": payload["created_by"],
            "tags": payload.get("tags", []),
            "created_at": datetime.utcnow(),
            "deleted": False
        })
        return {"status": "schema_created", "schema_id": payload["schema_id"]}

    elif command == "update_schema":
        result = schemas.update_one(
            {"schema_id": payload["schema_id"], "deleted": False},
            {"$set": payload.get("updates", {})}
        )
        return {"status": "schema_updated", "matched": result.matched_count}

    elif command == "delete_schema":
        result = schemas.update_one(
            {"schema_id": payload["schema_id"]},
            {"$set": {"deleted": True}}
        )
        return {"status": "schema_deleted", "matched": result.matched_count}

    elif command == "create_instance":
        schema = schemas.find_one({"schema_id": payload["schema_id"], "deleted": False})
        if not schema:
            return {"error": "schema not found"}
        data = payload["data"]
        missing = [field["name"] for field in schema["fields"] if field.get("required") and field["name"] not in data]
        if missing:
            return {"error": "missing required fields", "fields": missing}
        instance_id = str(uuid.uuid4())
        instances.insert_one({
            "instance_id": instance_id,
            "schema_id": payload["schema_id"],
            "data": data,
            "created_by": payload["created_by"],
            "created_at": datetime.utcnow(),
            "deleted": False
        })
        return {"status": "instance_created", "instance_id": instance_id}

    elif command == "update_instance":
        result = instances.update_one(
            {"instance_id": payload["instance_id"], "deleted": False},
            {"$set": payload.get("data", {})}
        )
        return {"status": "instance_updated", "matched": result.matched_count}

    elif command == "delete_instance":
        result = instances.update_one(
            {"instance_id": payload["instance_id"]},
            {"$set": {"deleted": True}}
        )
        return {"status": "instance_deleted", "matched": result.matched_count}

    elif command == "list_schemas":
        return query_schemas(payload.get("filter", {}), schemas)

    elif command == "get_schema":
        schema = schemas.find_one({"schema_id": payload.get("schema_id"), "deleted": False}, {"_id": 0})
        return schema if schema else {"error": "schema not found"}

    elif command == "list_instances":
        return list(instances.find({"schema_id": payload.get("schema_id"), "deleted": False}, {"_id": 0}))

    elif command == "get_instance":
        instance = instances.find_one({"instance_id": payload.get("instance_id"), "deleted": False}, {"_id": 0})
        return instance if instance else {"error": "instance not found"}

    else:
        return {"error": "unsupported schema_memory command"}

def query_schemas(filter, schemas):
    query = {"deleted": False}
    if "domain" in filter:
        query["domain"] = filter["domain"]
    if "tags" in filter:
        query["tags"] = {"$in": filter["tags"]}
    if "keyword" in filter:
        keyword = filter["keyword"]
        query["$or"] = [
            {"schema_id": {"$regex": keyword, "$options": "i"}},
            {"description": {"$regex": keyword, "$options": "i"}},
            {"domain": {"$regex": keyword, "$options": "i"}},
            {"tags": {"$regex": keyword, "$options": "i"}}
        ]
    return list(schemas.find(query, {"_id": 0}))