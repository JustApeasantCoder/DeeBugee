@echo off
setlocal EnableExtensions DisableDelayedExpansion
cd /d "%~dp0"

rem Publishes a new patch release of DeeBugee and uploads the portable EXE.
rem One-time setup: install GitHub CLI (https://cli.github.com/) and run "gh auth login".

set "GH=gh"
where gh >nul 2>nul
if not errorlevel 1 goto gh_ready

set "GH=%ProgramFiles%\GitHub CLI\gh.exe"
if exist "%GH%" goto gh_ready

set "GH=%LOCALAPPDATA%\Programs\GitHub CLI\gh.exe"
if exist "%GH%" goto gh_ready

echo GitHub CLI ^(gh^) is required. Install it from https://cli.github.com/
exit /b 1

:gh_ready

"%GH%" auth status --hostname github.com >nul 2>nul
if errorlevel 1 (
    echo GitHub CLI is not authenticated. Run: gh auth login
    exit /b 1
)

for /f "delims=" %%B in ('git branch --show-current') do set "RELEASE_BRANCH=%%B"
if /i not "%RELEASE_BRANCH%"=="main" (
    echo Releases must be made from the main branch. Current branch: %RELEASE_BRANCH%
    exit /b 1
)

for /f "delims=" %%S in ('git status --porcelain') do (
    echo Release stopped: the working tree has uncommitted changes.
    echo Commit or stash them first so the release tag matches the published source.
    exit /b 1
)

git fetch origin main --quiet
if errorlevel 1 exit /b %errorlevel%

for /f "delims=" %%L in ('git rev-parse HEAD') do set "LOCAL_HEAD=%%L"
for /f "delims=" %%R in ('git rev-parse origin/main') do set "REMOTE_HEAD=%%R"
if /i not "%LOCAL_HEAD%"=="%REMOTE_HEAD%" (
    echo Release stopped: local main does not match origin/main.
    echo Pull/rebase and push the current source before releasing.
    exit /b 1
)

for /f "delims=" %%V in ('powershell -NoProfile -Command "$content = Get-Content -Raw 'Cargo.toml'; $match = [regex]::Match($content, '(?m)^version\s*=\s*\"\d+\.\d+\.\d+\"\s*$'); if (-not $match.Success) { exit 1 }; $parts = ($match.Value -replace '[^0-9.]', '') -split '\.'; '{0}.{1}.{2}' -f $parts[0], $parts[1], (([int]$parts[2]) + 1)"') do set "NEXT_VERSION=%%V"
if not defined NEXT_VERSION (
    echo Could not determine the next patch version from Cargo.toml.
    exit /b 1
)

set "NEXT_TAG=v%NEXT_VERSION%"
for /f "tokens=1" %%T in ('git ls-remote --tags origin "refs/tags/%NEXT_TAG%"') do set "EXISTING_TAG=%%T"
if defined EXISTING_TAG (
    echo Release stopped: %NEXT_TAG% already exists on GitHub.
    exit /b 1
)

call "%~dp0BUILD.bat"
if errorlevel 1 exit /b %errorlevel%

for /f "delims=" %%V in ('powershell -NoProfile -Command "$content = Get-Content -Raw 'Cargo.toml'; $match = [regex]::Match($content, '(?m)^version\s*=\s*\"\d+\.\d+\.\d+\"\s*$'); if (-not $match.Success) { exit 1 }; $match.Value -replace '[^0-9.]', ''"') do set "VERSION=%%V"
if not defined VERSION (
    echo Could not read the release version from Cargo.toml.
    exit /b 1
)

set "TAG=v%VERSION%"
set "ASSET=target\release\dee-bugee.exe"
if not exist "%ASSET%" (
    echo Release build did not create %ASSET%.
    exit /b 1
)

git diff --check
if errorlevel 1 exit /b %errorlevel%

git add Cargo.toml Cargo.lock adapters\electron\package.json adapters\electron\package-lock.json adapters\dotnet\DeeBugee.Extensions.Logging\DeeBugee.Extensions.Logging.csproj
if errorlevel 1 exit /b %errorlevel%

git commit -m "chore: release %TAG%"
if errorlevel 1 exit /b %errorlevel%

git push origin main
if errorlevel 1 exit /b %errorlevel%

"%GH%" release create "%TAG%" "%ASSET%#dee-bugee.exe" --title "DeeBugee %TAG%" --generate-notes --target main
if errorlevel 1 exit /b %errorlevel%

echo.
echo Published DeeBugee %TAG%:
echo https://github.com/JustApeasantCoder/DeeBugee/releases/tag/%TAG%
