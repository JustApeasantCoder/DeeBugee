@echo off
setlocal
cd /d "%~dp0"
cargo build --release -p dee-bugee
if errorlevel 1 exit /b %errorlevel%
echo Built target\release\dee-bugee.exe
