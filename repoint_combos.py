#!/usr/bin/env python3
"""Repoint all oc/* combo entries to the local oc-rotator (ocr/) node.

- Ra/oc/X   -> ocr/X      (dead Railway relay)
- oc/X      -> ocr/X      (9router built-in direct egress, burned VPS IP)
- ocf/X     -> ocr/X      (opencodezen node, also direct)
"""
import json
import sqlite3

DB = "/var/lib/docker/volumes/9router-data/_data/db/data.sqlite"
con = sqlite3.connect(DB)
cur = con.cursor()

rows = cur.execute("SELECT id, name, models FROM combos").fetchall()
changed = 0
for cid, name, models_json in rows:
    try:
        models = json.loads(models_json)
    except Exception:
        continue
    if not isinstance(models, list):
        continue
    new_models = []
    hits = []
    for m in models:
        s = str(m)
        if s.startswith("Ra/oc/"):
            nm = "ocr/" + s[len("Ra/oc/"):]
            hits.append((s, nm))
            new_models.append(nm)
        elif s.startswith("ocf/"):
            nm = "ocr/" + s[len("ocf/"):]
            hits.append((s, nm))
            new_models.append(nm)
        elif s.startswith("oc/"):
            nm = "ocr/" + s[len("oc/"):]
            hits.append((s, nm))
            new_models.append(nm)
        else:
            new_models.append(s)
    if hits:
        cur.execute(
            "UPDATE combos SET models=?, updatedAt=datetime('now') WHERE id=?",
            (json.dumps(new_models), cid),
        )
        changed += 1
        print(f"[{name}]")
        for old, new in hits:
            print(f"    {old}  ->  {new}")

con.commit()
con.close()
print(f"\n{changed} combo(s) updated")
