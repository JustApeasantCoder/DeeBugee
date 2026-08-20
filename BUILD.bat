@echo off
setlocal
cd /d "%~dp0"

powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\Bump-PatchVersion.ps1"
if errorlevel 1 exit /b %errorlevel%

cargo build --release -p dee-bugee
if errorlevel 1 exit /b %errorlevel%
echo Built target\release\dee-bugee.exe
