[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'

function Get-PatchVersion([string]$value) {
    if ($value -notmatch '^(?<major>0|[1-9]\d*)\.(?<minor>0|[1-9]\d*)\.(?<patch>0|[1-9]\d*)$') {
        throw "Expected a semantic version with major.minor.patch, received '$value'."
    }

    return '{0}.{1}.{2}' -f $Matches.major, $Matches.minor, ([int]$Matches.patch + 1)
}

function Get-ReplacedContent([string]$path, [string]$content, [string]$pattern, [string]$replacement) {
    $expression = [regex]::new($pattern)
    if ($expression.Matches($content).Count -ne 1) {
        throw "Expected exactly one matching version field in $path."
    }

    return $expression.Replace($content, $replacement, 1)
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

$electronPackageContent = [System.IO.File]::ReadAllText($electronPackage)
$electronLockContent = [System.IO.File]::ReadAllText($electronLock)
$dotnetProjectContent = [System.IO.File]::ReadAllText($dotnetProject)

$cargoEol = ([regex]::Match($cargoContent, '(?m)^version\s*=\s*"\d+\.\d+\.\d+"(?<eol>\r?)$')).Groups['eol'].Value
$electronEol = ([regex]::Match($electronPackageContent, '(?m)^  "version": "\d+\.\d+\.\d+",(?<eol>\r?)$')).Groups['eol'].Value
$updatedCargo = Get-ReplacedContent $cargoToml $cargoContent '(?m)^version\s*=\s*"\d+\.\d+\.\d+"(?<eol>\r?)$' ('version = "' + $next + '"' + $cargoEol)
$updatedElectronPackage = Get-ReplacedContent $electronPackage $electronPackageContent '(?m)^  "version": "\d+\.\d+\.\d+",(?<eol>\r?)$' ('  "version": "' + $next + '",' + $electronEol)
$updatedElectronLock = Get-ReplacedContent $electronLock $electronLockContent '(?s)\A(\{\s+"name": "@deebugee/electron",\s+"version": ")\d+\.\d+\.\d+"' ('${1}' + $next + '"')
$updatedElectronLock = Get-ReplacedContent $electronLock $updatedElectronLock '(?s)("packages": \{\s+"": \{\s+"name": "@deebugee/electron",\s+"version": ")\d+\.\d+\.\d+"' ('${1}' + $next + '"')
$updatedDotnetProject = Get-ReplacedContent $dotnetProject $dotnetProjectContent '<Version>\d+\.\d+\.\d+</Version>' ('<Version>' + $next + '</Version>')

$updates = @(
    [pscustomobject]@{ Path = $cargoToml; Original = $cargoContent; Updated = $updatedCargo }
    [pscustomobject]@{ Path = $electronPackage; Original = $electronPackageContent; Updated = $updatedElectronPackage }
    [pscustomobject]@{ Path = $electronLock; Original = $electronLockContent; Updated = $updatedElectronLock }
    [pscustomobject]@{ Path = $dotnetProject; Original = $dotnetProjectContent; Updated = $updatedDotnetProject }
)

$written = @()
try {
    foreach ($update in $updates) {
        $written += $update
        [System.IO.File]::WriteAllText($update.Path, $update.Updated, [System.Text.UTF8Encoding]::new($false))
    }
}
catch {
    foreach ($update in $written) {
        [System.IO.File]::WriteAllText($update.Path, $update.Original, [System.Text.UTF8Encoding]::new($false))
    }
    throw
}

Write-Host "Bumped DeeBugee version $current -> $next"
