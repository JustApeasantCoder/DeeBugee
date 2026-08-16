@echo off
setlocal
cd /d "%~dp0"
cargo build --release -p debug-logging-toolkit
if errorlevel 1 exit /b %errorlevel%
echo Built target\release\debug-logging-toolkit.exe
