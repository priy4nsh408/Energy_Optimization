@echo off
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0verify.ps1" %*
