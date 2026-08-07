#!/usr/bin/env python3
"""Clean the main combo: remove dead Ra/oc/* entries, dedupe ocr/* models."""
import json
import sqlite3

DB = "/var/lib/docker/volumes/9router-data/_data/db/data.sqlite"
con = sqlite3.connect(DB)
cur = con.cursor()

row = cur.execute("SELECT id, models FROM combos WHERE name='main'").fetchone()
if not row:
    print("main combo not found")
    raise SystemExit(1)
cid, models_json = row
models = json.loads(models_json)

before = len(models)
seen = set()
clean = []
removed = []
for m in models:
    s = str(m)
    if s.startswith("Ra/oc/"):
        removed.append(s)
        continue  # drop dead Railway entry
    key = s
    if key in seen:
        removed.append(s + " (dup)")
        continue
    seen.add(key)
    clean.append(s)

cur.execute("UPDATE combos SET models=?, updatedAt=datetime('now') WHERE id=?", (json.dumps(clean), cid))
con.commit()
con.close()

print(f"before={before} after={len(clean)} removed={len(removed)}")
for r in removed:
    print("  removed:", r)
print("ocr entries now:", [x for x in clean if x.startswith('ocr/')])
