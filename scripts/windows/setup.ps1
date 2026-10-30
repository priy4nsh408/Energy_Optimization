<#
.SYNOPSIS
  One-time setup on Windows: virtual environment, packages, .env, Docker services, data and models.
.EXAMPLE
  powershell -ExecutionPolicy Bypass -File scripts\windows\setup.ps1
  powershell -ExecutionPolicy Bypass -File scripts\windows\setup.ps1 -ClosedLoop
#>
param([switch]$ClosedLoop)
$ErrorActionPreference = "Stop"
Set-Location (Resolve-Path "$PSScriptRoot\..\..")

function Step($text) { Write-Host "`n== $text" -ForegroundColor Cyan }

Step "Python 3.10-3.13"
$python = if (Get-Command py -ErrorAction SilentlyContinue) { "py" } else { "python" }
$version = & $python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"
if ([version]$version -lt [version]"3.10" -or [version]$version -ge [version]"3.14") {
    throw "Python $version found; install Python 3.12 from python.org"
}

Step "virtual environment (.venv)"
if (-not (Test-Path ".venv\Scripts\python.exe")) { & $python -m venv .venv }
& .venv\Scripts\python.exe -m pip install --upgrade pip
& .venv\Scripts\python.exe -m pip install -r requirements.txt

Step ".env"
if (Test-Path ".env") {
    Write-Host ".env exists - left unchanged"
} else {
    Copy-Item ".env.example" ".env"
    Write-Host "Created .env from .env.example. Set POSTGRES_PASSWORD (Docker default: change_me) and review it." -ForegroundColor Yellow
}

Step "Docker services (Kafka + PostgreSQL)"
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { throw "Docker Desktop is not installed or not running" }
docker compose -f docker/docker-compose.yml --env-file .env up -d
if ($LASTEXITCODE -ne 0) { throw "docker compose failed" }

Step "data and models (python -m scripts.bootstrap)"
$bootstrapArgs = @("-m", "scripts.bootstrap")
if ($ClosedLoop) { $bootstrapArgs += "--closed-loop" }
& .venv\Scripts\python.exe @bootstrapArgs
if ($LASTEXITCODE -ne 0) { throw "bootstrap failed (see the output above)" }

Write-Host "`nSetup finished. Start the system with: scripts\windows\start.ps1" -ForegroundColor Green
