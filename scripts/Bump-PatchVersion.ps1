[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'

function Get-PatchVersion([string]$value) {
    if ($value -notmatch '^(?<major>0|[1-9]\d*)\.(?<minor>0|[1-9]\d*)\.(?<patch>0|[1-9]\d*)$') {
        throw "Expected a semantic version with major.minor.patch, received '$value'."
    }

    return '{0}.{1}.{2}' -f $Matches.major, $Matches.minor, ([int]$Matches.patch + 1)
}

function Replace-ExactlyOnce([string]$path, [string]$pattern, [string]$replacement) {
    $content = [System.IO.File]::ReadAllText($path)
    $expression = [regex]::new($pattern)
    if ($expression.Matches($content).Count -ne 1) {
        throw "Expected exactly one matching version field in $path."
    }

    $updated = $expression.Replace($content, $replacement, 1)
    [System.IO.File]::WriteAllText($path, $updated, [System.Text.UTF8Encoding]::new($false))
}

$root = Split-Path -Parent $PSScriptRoot
$cargoToml = Join-Path $root 'Cargo.toml'
$electronPackage = Join-Path $root 'adapters\electron\package.json'
$electronLock = Join-Path $root 'adapters\electron\package-lock.json'
$dotnetProject = Join-Path $root 'adapters\dotnet\DeeBugee.Extensions.Logging\DeeBugee.Extensions.Logging.csproj'

$cargoContent = [System.IO.File]::ReadAllText($cargoToml)
$cargoMatch = [regex]::Match($cargoContent, '(?m)^version\s*=\s*"(?<version>\d+\.\d+\.\d+)"\s*$')
if (-not $cargoMatch.Success) {
    throw "Could not find [workspace.package] version in $cargoToml."
}

$current = $cargoMatch.Groups['version'].Value
$next = Get-PatchVersion $current

Replace-ExactlyOnce $cargoToml '(?m)^version\s*=\s*"\d+\.\d+\.\d+"\s*$' ('version = "' + $next + '"')
Replace-ExactlyOnce $electronPackage '(?m)^  "version": "\d+\.\d+\.\d+",$' ('  "version": "' + $next + '",')
Replace-ExactlyOnce $electronLock '(?s)\A(\{\s+"name": "@deebugee/electron",\s+"version": ")\d+\.\d+\.\d+"' ('${1}' + $next + '"')
Replace-ExactlyOnce $electronLock '(?s)("packages": \{\s+"": \{\s+"name": "@deebugee/electron",\s+"version": ")\d+\.\d+\.\d+"' ('${1}' + $next + '"')
Replace-ExactlyOnce $dotnetProject '<Version>\d+\.\d+\.\d+</Version>' ('<Version>' + $next + '</Version>')

Write-Host "Bumped DeeBugee version $current -> $next"
