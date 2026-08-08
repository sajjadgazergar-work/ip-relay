#!/usr/bin/env python3
"""Load test for ip-relay — measures real capacity against the free tier.

Usage: python3 loadtest.py [burst_size] [burst_count]
"""
import asyncio
import sys
import time

import httpx

URL = "http://127.0.0.1:18080/v1/chat/completions"
MODEL = "deepseek-v4-flash-free"
HEADERS = {"Content-Type": "application/json", "Authorization": "Bearer loadtest"}

BURST = int(sys.argv[1]) if len(sys.argv) > 1 else 10
ROUNDS = int(sys.argv[2]) if len(sys.argv) > 2 else 5


async def one(client: httpx.AsyncClient, i: int) -> dict:
    t0 = time.time()
    try:
        r = await client.post(
            URL,
            headers=HEADERS,
            json={
                "model": MODEL,
                "messages": [{"role": "user", "content": f"Count to {i}"}],
                "max_tokens": 10,
                "stream": False,
            },
            timeout=60,
        )
        dt = time.time() - t0
        ok = r.status_code == 200
        err = ""
        if not ok:
            try:
                err = r.json().get("error", {}).get("message", "")[:80]
            except Exception:
                err = r.text[:80]
        return {"ok": ok, "status": r.status_code, "dt": dt, "err": err}
    except Exception as e:
        return {"ok": False, "status": 0, "dt": time.time() - t0, "err": str(e)[:80]}


async def run_round(round_no: int, n: int) -> dict:
    async with httpx.AsyncClient(limits=httpx.Limits(max_connections=20, max_keepalive_connections=5)) as client:
        results = await asyncio.gather(*(one(client, round_no * 100 + i) for i in range(n)))
    oks = [r for r in results if r["ok"]]
    fails = [r for r in results if not r["ok"]]
    dts = [r["dt"] for r in results]
    errs = {}
    for f in fails:
        errs[f["status"]] = errs.get(f["status"], 0) + 1
    return {
        "round": round_no,
        "n": n,
        "ok": len(oks),
        "fail": len(fails),
        "pct": round(100 * len(oks) / n, 1),
        "avg_s": round(sum(dts) / len(dts), 2),
        "max_s": round(max(dts), 2),
        "errors": errs,
    }


async def main():
    print(f"=== ip-relay load test: {BURST} parallel × {ROUNDS} rounds ===")
    print(f"target: {URL} model={MODEL}\n")
    totals = {"ok": 0, "fail": 0}
    all_dts = []
    for rnd in range(1, ROUNDS + 1):
        res = await run_round(rnd, BURST)
        totals["ok"] += res["ok"]
        totals["fail"] += res["fail"]
        all_dts.extend([res["avg_s"]])
        print(
            f"round {res['round']:>2}: {res['ok']}/{res['n']} ok ({res['pct']}%) "
            f"avg={res['avg_s']}s max={res['max_s']}s errors={res['errors']}"
        )
        await asyncio.sleep(1)

    total = totals["ok"] + totals["fail"]
    print(f"\n=== TOTAL: {totals['ok']}/{total} ok ({round(100*totals['ok']/total,1)}%) ===")
    print(f"avg latency: {round(sum(all_dts)/len(all_dts), 2)}s")

    # pool state after
    try:
        async with httpx.AsyncClient() as c:
            h = (await c.get("http://127.0.0.1:18080/healthz")).json()
        print(f"pool after: {h['pool']} | stats: {h['stats']}")
    except Exception:
        pass


asyncio.run(main())
