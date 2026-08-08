<#
.SYNOPSIS
  ip-relay one-shot install / update for Windows (PowerShell 5.1+).
.DESCRIPTION
  Installs ip-relay into $env:LOCALAPPDATA\ip-relay (or -Dir), creates a
  venv, installs deps. Creates a start-ip-relay.bat launcher by default,
  or runs manually with -Manual. Safe: never touches existing config,
  backs up code before overwriting.
.EXAMPLE
  .\install.ps1
  .\install.ps1 -Dir D:\ip-relay -Manual
  .\install.ps1 -Docker
#>
param(
  [string]$Dir = "$env:LOCALAPPDATA\ip-relay",
  [switch]$Manual,
  [switch]$Docker
)
$ErrorActionPreference = 'Stop'
$Repo = 'sajjadgazergar-work/ip-relay'
$Tag  = 'v0.5.0'

function Log([string]$m) { Write-Host "==> $m" -ForegroundColor Cyan }
function Die([string]$m) { Write-Host "ERROR: $m" -ForegroundColor Red; exit 1 }

# ── deps ───────────────────────────────────────────────
if ($Docker) {
  docker --version *> $null 2>&1 | Out-Null
  if ($LASTEXITCODE -ne 0) { Die "Docker not found — install Docker Desktop first" }
} else {
  $py = Get-Command python -ErrorAction SilentlyContinue
  if (-not $py) {
    # try py launcher
    $py = Get-Command py -ErrorAction SilentlyContinue
    if (-not $py) { Die 'Python 3 not found — install from https://python.org (check "Add to PATH")' }
  }
  $ver = (& $py.Source -c "import sys; print(sys.version_info[0])" 2>$null | Out-String).Trim()
  if ($ver -ne '3') { Die "Python 3 required (found: $ver)" }
}

# ── fetch release ──────────────────────────────────────
Log "Fetching $Repo@$Tag ..."
$zip = Join-Path $env:TEMP "ip-relay-$Tag.zip"
$dest = Join-Path $env:TEMP "ip-relay-$($Tag.TrimStart('v'))"
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
Invoke-WebRequest -Uri "https://github.com/$Repo/archive/refs/tags/$Tag.zip" -OutFile $zip
Expand-Archive -Path $zip -DestinationPath $env:TEMP -Force
$src = $dest

# ── docker mode ─────────────────────────────────────────
if ($Docker) {
  Log 'Building Docker image ...'
  docker build -t "ip-relay:$Tag" $src
  Log 'Running container (port 8080)...'
  docker rm -f ip-relay *> $null
  docker run -d --name ip-relay --restart unless-stopped -p 8080:8080 -e PORT=8080 "ip-relay:$Tag"
  Log "Done. Dashboard: http://localhost:8080"
  exit 0
}

# ── venv install ────────────────────────────────────────
New-Item -ItemType Directory -Force -Path $Dir | Out-Null
cd $Dir

# backup existing code
if (Test-Path "$Dir\ip_relay.py") { Copy-Item "$Dir\ip_relay.py" "$Dir\ip_relay.py.bak" -Force }
if (Test-Path "$Dir\main.py")    { Copy-Item "$Dir\main.py"    "$Dir\main.py.bak"    -Force }

# code
Copy-Item "$src\ip_relay.py" $Dir -Force
Copy-Item "$src\main.py"     $Dir -Force
# env — never clobber existing
if (-not (Test-Path "$Dir\.env")) {
  Copy-Item "$src\.env.example" "$Dir\.env" -Force
  Log "Created .env — edit to change upstream/key (optional)"
}

# venv + deps
if (-not (Test-Path "$Dir\.venv")) {
  Log 'Creating venv ...'
  & (Get-Command python).Source -m venv "$Dir\.venv"
}
Log 'Installing dependencies ...'
& "$Dir\.venv\Scripts\pip.exe" install --upgrade pip -q
& "$Dir\.venv\Scripts\pip.exe" install -q -r "$src\requirements.txt"

# ── start ─────────────────────────────────────────────────
if ($Manual) {
  Log "Manual mode — run:"
  Log "  & '$Dir\.venv\Scripts\python.exe' -m uvicorn ip_relay:app --host 0.0.0.0 --port 8080"
} else {
  Log 'Creating one-click launcher ...'
  $bat = @"
@echo off
cd /d "$Dir"
".venv\Scripts\python.exe" -m uvicorn ip_relay:app --host 0.0.0.0 --port 8080
"@
  Set-Content -Path "$Dir\start-ip-relay.bat" -Value $bat -Encoding ASCII
  Log "Launcher: $Dir\start-ip-relay.bat (double-click to run)"
}

Log "Installed. Next steps:"
Log "  code dir : $Dir"
Log "  start    : $Dir\start-ip-relay.bat"
Log '  dashboard: http://localhost:8080'