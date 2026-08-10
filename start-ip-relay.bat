@echo off
setlocal enabledelayedexpansion

cd /d "%~dp0"

echo =========================================================
echo  ip-relay // egress rotator launcher
echo =========================================================

if not exist ".venv\Scripts\python.exe" (
    echo [!] Python virtual environment (.venv) not found in this folder.
    echo     Initializing virtual environment and installing dependencies...
    echo.
    
    where python >nul 2>nul
    if errorlevel 1 (
        where py >nul 2>nul
        if errorlevel 1 (
            echo [ERROR] Python 3 was not found on your system!
            echo Please install Python 3 from https://python.org and make sure
            echo "Add Python to PATH" is checked during installation.
            echo.
            pause
            exit /b 1
        )
        set PY_CMD=py -3
    ) else (
        set PY_CMD=python
    )

    %PY_CMD% -m venv .venv
    if errorlevel 1 (
        echo [ERROR] Failed to create virtual environment!
        pause
        exit /b 1
    )
    
    echo [i] Installing required packages...
    ".venv\Scripts\python.exe" -m pip install --upgrade pip -q
    ".venv\Scripts\python.exe" -m pip install -r requirements.txt
    if errorlevel 1 (
        echo [ERROR] Failed to install dependencies from requirements.txt!
        pause
        exit /b 1
    )
    echo [OK] Setup completed successfully!
    echo.
)

echo Starting ip-relay server on http://localhost:18080 ...
echo Press Ctrl+C to stop.
echo.

".venv\Scripts\python.exe" -m uvicorn ip_relay:app --host 0.0.0.0 --port 18080

if errorlevel 1 (
    echo.
    echo [ERROR] ip-relay stopped unexpectedly (Exit code: %errorlevel%).
    echo If port 18080 is in use, stop the conflicting service and try again.
    pause
)
