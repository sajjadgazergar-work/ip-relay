@echo off
setlocal enabledelayedexpansion

cd /d "%~dp0"

echo =========================================================
echo  ip-relay // egress rotator launcher
echo =========================================================

rem Always surface the real error — never let the window vanish silently.
set "ERR=0"

rem ---- 1. Check Python / venv ----
if not exist ".venv\Scripts\python.exe" (
    echo [!] Virtual environment (.venv) not found in: %~dp0
    echo     Trying to create it...
    echo.
    python -m venv .venv 2>nul
    if errorlevel 1 (
        py -3 -m venv .venv 2>nul
        if errorlevel 1 (
            echo.
            echo [ERROR] Python 3 was not found on your system.
            echo   Install Python 3 from https://python.org and CHECK "Add Python to PATH".
            echo   Then re-run this file.
            echo.
            pause
            exit /b 1
        )
    )
    echo [i] Installing dependencies...
    ".venv\Scripts\python.exe" -m pip install --upgrade pip -q
    if errorlevel 1 ( set "ERR=1" )
    ".venv\Scripts\python.exe" -m pip install -r requirements.txt
    if errorlevel 1 ( set "ERR=1" )
    if "!ERR!"=="1" (
        echo.
        echo [ERROR] Failed to install dependencies.
        echo   Check your internet connection, then re-run this file.
        pause
        exit /b 1
    )
    echo [OK] Setup complete.
    echo.
)

rem ---- 2. Verify the venv python actually runs ----
".venv\Scripts\python.exe" -c "import sys; sys.exit(0)" >nul 2>nul
if errorlevel 1 (
    echo.
    echo [ERROR] The .venv Python is broken (cannot execute).
    echo   Delete the ".venv" folder and re-run this file to rebuild it.
    pause
    exit /b 1
)

echo Starting ip-relay server on http://localhost:18080 ...
echo Press Ctrl+C to stop.
echo.

".venv\Scripts\python.exe" -m uvicorn ip_relay:app --host 0.0.0.0 --port 18080

if errorlevel 1 (
    echo.
    echo [ERROR] ip-relay stopped with code %errorlevel%.
    echo   - If you see "address already in use": another server is on port 18080.
    echo     Close it, or edit --port in this file.
    echo   - If you see a Python traceback above, copy it and report it.
    echo.
    pause
)
