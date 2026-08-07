#!/usr/bin/env python3
"""Add the oc-rotator provider node to the VPS 9router DB (idempotent).

The rotator listens on 127.0.0.1:18080 on the host; from inside the 9router
container that's the docker bridge gateway 172.17.0.1. baseUrl is the API
root ending in /v1 — 9router appends /chat/completions and /models itself.
"""
import json
import sqlite3
import uuid

DB = "/var/lib/docker/volumes/9router-data/_data/db/data.sqlite"
NODE_ID = "openai-compatible-chat-oc-rotator"
PREFIX = "ocr"
BASE = "http://172.17.0.1:18080/v1"

con = sqlite3.connect(DB)
cur = con.cursor()

# 1. upsert provider node
node_data = json.dumps({"prefix": PREFIX, "apiType": "chat", "baseUrl": BASE})
cur.execute(
    "INSERT OR REPLACE INTO providerNodes(id,type,name,data,createdAt,updatedAt) "
    "VALUES(?,?,?,?,datetime('now'),datetime('now'))",
    (NODE_ID, "openai-compatible", "oc-rotator", node_data),
)
print(f"provider node {NODE_ID} ({PREFIX} -> {BASE}) ok")

# 2. ensure a connection exists (delete stale oc-rotator connections first,
#    then insert fresh — mirrors dashboard 'Add Connection')
cur.execute("DELETE FROM providerConnections WHERE provider=?", (NODE_ID,))
conn_data = json.dumps({
    "apiKey": "public",
    "testStatus": "active",
    "providerSpecificData": {
        "prefix": PREFIX,
        "apiType": "chat",
        "baseUrl": BASE,
        "nodeName": "oc-rotator",
        "connectionProxyEnabled": False,
        "connectionProxyUrl": "",
        "connectionNoProxy": "",
    },
})
cid = str(uuid.uuid4())
cur.execute(
    "INSERT INTO providerConnections"
    "(id,provider,authType,name,email,priority,isActive,data,createdAt,updatedAt) "
    "VALUES(?,?,'apikey','oc-rotator',NULL,1,1,?,datetime('now'),datetime('now'))",
    (cid, NODE_ID, conn_data),
)
print(f"connection {cid} ok (priority 1)")

con.commit()
con.close()
print("done — restart 9router to pick up the node")
