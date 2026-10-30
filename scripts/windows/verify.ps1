<#
.SYNOPSIS
  End-to-end verification in an isolated database/topic set (about 10 minutes per mode).
.EXAMPLE
  powershell -ExecutionPolicy Bypass -File scripts\windows\verify.ps1
  powershell -ExecutionPolicy Bypass -File scripts\windows\verify.ps1 -Mode closed_loop
#>
param([ValidateSet("advisory", "closed_loop")][string]$Mode = "advisory")
$ErrorActionPreference = "Stop"
Set-Location (Resolve-Path "$PSScriptRoot\..\..")
docker compose -f docker/docker-compose.yml --env-file .env up -d
& .venv\Scripts\python.exe -m scripts.verify_system --launch --mode $Mode --report "docs\results\system_verification_$Mode.md"
exit $LASTEXITCODE
