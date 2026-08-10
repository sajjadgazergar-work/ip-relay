"""Shared test setup.

Set before ip_relay is imported anywhere: TestClient(app) as a context manager
runs the real lifespan, which would otherwise start the pool manager and begin
scraping public proxy lists from the test suite.
"""
import os

os.environ.setdefault("IP_RELAY_NO_BACKGROUND", "1")
os.environ.setdefault("RELAY_ALLOW_ANONYMOUS", "1")
