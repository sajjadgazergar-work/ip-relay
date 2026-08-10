@echo off
cd /d "C:\Users\Sgaze\AppData\Local\ip-relay"
".venv\Scripts\python.exe" -m uvicorn ip_relay:app --host 0.0.0.0 --port 8080
