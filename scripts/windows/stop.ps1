<#
.SYNOPSIS
  Stop the Docker services (Kafka, PostgreSQL). Data is kept in Docker volumes.
  Stop the Python services with Ctrl+C in the start.ps1 window.
#>
Set-Location (Resolve-Path "$PSScriptRoot\..\..")
docker compose -f docker/docker-compose.yml --env-file .env stop
