@echo off
setlocal
REM Windows runner shim: call PowerShell script
powershell -ExecutionPolicy Bypass -File "%~dp0run.ps1"
set EXITCODE=%ERRORLEVEL%
exit /b %EXITCODE%


