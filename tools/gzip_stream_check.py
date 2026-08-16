"""Same question as gzip_stream_check.py, but over a REAL socket.

httpx.ASGITransport buffers the whole response, so the in-process test could not
tell streaming from buffering (both showed 1.27s). This runs two real uvicorn
servers on loopback and reads raw bytes with a wall clock.

Verdict criterion: the generator emits 5 frames 0.25s apart. If the first bytes
reach the client at ~0.0-0.3s the response is genuinely streaming; if they only
arrive at ~1.0s+ the middleware is holding them, which would break the /v1
streaming relay and the dashboard SSE feed.
"""
import asyncio
import multiprocessing
import time

import httpx
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import StreamingResponse

FRAMES = 5
GAP = 0.25


def make_app(with_gzip: bool) -> FastAPI:
    app = FastAPI()
    if with_gzip:
        app.add_middleware(GZipMiddleware, minimum_size=1024)

    @app.get("/sse")
    async def sse():
        async def gen():
            for i in range(FRAMES):
                yield f'data: {{"chunk":{i},"pad":"{"x" * 40}"}}\n\n'.encode()
                await asyncio.sleep(GAP)
            yield b'data: {"usage":{"prompt_tokens":11,"completion_tokens":22}}\n\n'
            yield b"data: [DONE]\n\n"
        return StreamingResponse(gen(), media_type="text/event-stream")

    return app


def serve(port: int, with_gzip: bool):
    uvicorn.run(make_app(with_gzip), host="127.0.0.1", port=port, log_level="error")


async def probe(port: int, label: str, accept_gzip: bool):
    headers = {"Accept-Encoding": "gzip" if accept_gzip else "identity"}
    async with httpx.AsyncClient(timeout=20) as c:
        t0 = time.time()
        arrivals = []
        async with c.stream("GET", f"http://127.0.0.1:{port}/sse", headers=headers) as r:
            enc = r.headers.get("content-encoding", "identity")
            # aiter_raw = wire bytes; aiter_text would hide gzip coalescing.
            async for raw in r.aiter_raw():
                if raw:
                    arrivals.append((round(time.time() - t0, 2), len(raw)))
        first = arrivals[0][0] if arrivals else None
        # The real question for an SSE relay is not "did byte 1 arrive early" but
        # "does frame N arrive near N*GAP" — i.e. is the stream still INCREMENTAL.
        # zlib buffers internally, so gzip can deliver headers instantly and then
        # coalesce every payload frame into one late flush.
        late = [t for t, _ in arrivals if t >= FRAMES * GAP * 0.8]
        incremental = len([t for t, _ in arrivals if t < FRAMES * GAP * 0.8])
        print(f"{label:30} enc={enc:8} chunks={len(arrivals):2} first={first}s")
        print(f"{'':30} timeline={arrivals}")
        print(f"{'':30} -> {incremental} chunk(s) arrived DURING generation, "
              f"{len(late)} at/after the end "
              f"({'INCREMENTAL' if incremental >= FRAMES - 1 else 'COALESCED — token streaming is lost'})")
        return arrivals


async def main():
    print(f"generator: {FRAMES} frames, {GAP}s apart -> last frame at ~{FRAMES * GAP:.2f}s\n")
    await probe(8931, "plain, client wants gzip", True)
    await probe(8932, "GZipMiddleware, wants gzip", True)
    await probe(8932, "GZipMiddleware, identity", False)


if __name__ == "__main__":
    procs = [multiprocessing.Process(target=serve, args=(8931, False), daemon=True),
             multiprocessing.Process(target=serve, args=(8932, True), daemon=True)]
    for p in procs:
        p.start()
    time.sleep(2.5)
    try:
        asyncio.run(main())
    finally:
        for p in procs:
            p.terminate()
