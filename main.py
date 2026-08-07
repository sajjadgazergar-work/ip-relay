# Compatibility entrypoint — the real code lives in ip_relay.py.
# Kept so existing deploys (systemd, Docker, old env vars) keep working.
from ip_relay import app

if __name__ == "__main__":
    import os
    import uvicorn

    port = int(os.environ.get("PORT", "8080"))
    uvicorn.run(app, host="0.0.0.0", port=port)
