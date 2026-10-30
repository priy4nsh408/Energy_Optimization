<#
.SYNOPSIS
  Start the whole system: Docker services, preflight, all workers and the dashboard.
  Ctrl+C stops every service (Docker containers keep running; use stop.ps1 for those).
.EXAMPLE
  powershell -ExecutionPolicy Bypass -File scripts\windows\start.ps1
  powershell -ExecutionPolicy Bypass -File scripts\windows\start.ps1 -Mode closed_loop -MockExternal
#>
param(
    [ValidateSet("advisory", "closed_loop")][string]$Mode = "",
    [switch]$MockExternal,
    [switch]$Autostart
)
$ErrorActionPreference = "Stop"
Set-Location (Resolve-Path "$PSScriptRoot\..\..")
if (-not (Test-Path ".venv\Scripts\python.exe")) { throw "Run scripts\windows\setup.ps1 first" }

docker compose -f docker/docker-compose.yml --env-file .env up -d
if ($LASTEXITCODE -ne 0) { throw "docker compose failed - is Docker Desktop running?" }

$stackArgs = @("-m", "scripts.run_stack")
if ($Mode) { $stackArgs += @("--mode", $Mode) }
if ($MockExternal) { $stackArgs += "--mock-external" }
if ($Autostart) { $stackArgs += "--autostart" }
& .venv\Scripts\python.exe @stackArgs
