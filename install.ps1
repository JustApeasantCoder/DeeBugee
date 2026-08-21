<#
.SYNOPSIS
Downloads and installs the portable DeeBugee viewer for the current user.

.DESCRIPTION
Downloads the latest DeeBugee GitHub release (or a requested version), verifies
the release asset's SHA-256 digest, and atomically installs dee-bugee.exe in a
stable user-level directory. Project repositories are not modified.

.EXAMPLE
.\install.ps1 -AddToPath

.EXAMPLE
.\install.ps1 -Version 1.0.21 -AddToPath
#>
[CmdletBinding()]
param(
    [Parameter()]
    [ValidateNotNullOrEmpty()]
    [string]$Version,

    [Parameter()]
    [ValidateNotNullOrEmpty()]
    [string]$InstallDirectory,

    [Parameter()]
    [switch]$AddToPath
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repository = "JustApeasantCoder/DeeBugee"
$assetName = "dee-bugee.exe"
$apiHeaders = @{
    Accept                 = "application/vnd.github+json"
    "User-Agent"           = "DeeBugee-Installer"
    "X-GitHub-Api-Version" = "2022-11-28"
}

if ([Net.ServicePointManager]::SecurityProtocol -band [Net.SecurityProtocolType]::Tls12) {
    # TLS 1.2 is already enabled.
}
else {
    [Net.ServicePointManager]::SecurityProtocol =
        [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12
}

function Add-DirectoryToUserPath {
    param(
        [Parameter(Mandatory)]
        [string]$Directory
    )

    $normalizedDirectory = $Directory.TrimEnd("\", "/")
    $userPath = [Environment]::GetEnvironmentVariable("Path", "User")
    $entries = @(
        $userPath -split ";" |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
            ForEach-Object { $_.Trim().TrimEnd("\", "/") }
    )

    if ($entries -notcontains $normalizedDirectory) {
        $updatedUserPath = (@($entries) + $normalizedDirectory) -join ";"
        [Environment]::SetEnvironmentVariable("Path", $updatedUserPath, "User")
        Write-Host "Added $Directory to the user PATH."
    }
    else {
        Write-Host "$Directory is already in the user PATH."
    }

    $processEntries = @(
        $env:Path -split ";" |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
            ForEach-Object { $_.Trim().TrimEnd("\", "/") }
    )
    if ($processEntries -notcontains $normalizedDirectory) {
        $env:Path = (@($env:Path.TrimEnd(";"), $Directory) -join ";").TrimStart(";")
    }
}

if (-not $PSBoundParameters.ContainsKey("InstallDirectory")) {
    if ([string]::IsNullOrWhiteSpace($env:LOCALAPPDATA)) {
        throw "LOCALAPPDATA is not set. Pass -InstallDirectory with an explicit destination."
    }
    $InstallDirectory = Join-Path $env:LOCALAPPDATA "Programs\DeeBugee"
}

if ($Version) {
    $releaseTag = if ($Version.StartsWith("v", [StringComparison]::OrdinalIgnoreCase)) {
        $Version
    }
    else {
        "v$Version"
    }
    $escapedTag = [Uri]::EscapeDataString($releaseTag)
    $releaseEndpoint = "https://api.github.com/repos/$repository/releases/tags/$escapedTag"
}
else {
    $releaseEndpoint = "https://api.github.com/repos/$repository/releases/latest"
}

Write-Host "Fetching DeeBugee release metadata..."
$release = Invoke-RestMethod -Uri $releaseEndpoint -Headers $apiHeaders
$asset = @($release.assets | Where-Object { $_.name -eq $assetName })

if ($asset.Count -ne 1) {
    throw "Expected exactly one '$assetName' asset in release '$($release.tag_name)', but found $($asset.Count)."
}

$digest = [string]$asset[0].digest
if ($digest -notmatch "^sha256:([0-9a-fA-F]{64})$") {
    throw "Release asset '$assetName' does not provide a valid SHA-256 digest. Installation was stopped."
}
$expectedHash = $Matches[1].ToUpperInvariant()

$resolvedInstallDirectory = [IO.Path]::GetFullPath($InstallDirectory)
[IO.Directory]::CreateDirectory($resolvedInstallDirectory) | Out-Null
$destination = Join-Path $resolvedInstallDirectory $assetName
$stagedDownload = Join-Path $resolvedInstallDirectory ".$assetName.download.$([Guid]::NewGuid().ToString('N'))"
$backup = Join-Path $resolvedInstallDirectory ".$assetName.backup.$([Guid]::NewGuid().ToString('N'))"
$previousProgressPreference = $ProgressPreference

try {
    $ProgressPreference = "SilentlyContinue"
    Write-Host "Downloading $assetName $($release.tag_name)..."
    Invoke-WebRequest -Uri $asset[0].browser_download_url -Headers $apiHeaders -OutFile $stagedDownload -UseBasicParsing

    $actualHash = (Get-FileHash -LiteralPath $stagedDownload -Algorithm SHA256).Hash.ToUpperInvariant()
    if ($actualHash -ne $expectedHash) {
        throw "SHA-256 verification failed for '$assetName'. Installation was stopped."
    }

    if (Test-Path -LiteralPath $destination -PathType Leaf) {
        [IO.File]::Replace($stagedDownload, $destination, $backup, $true)
        Remove-Item -LiteralPath $backup -Force
    }
    else {
        [IO.File]::Move($stagedDownload, $destination)
    }
}
finally {
    $ProgressPreference = $previousProgressPreference
    if (Test-Path -LiteralPath $stagedDownload) {
        Remove-Item -LiteralPath $stagedDownload -Force
    }
}

if ($AddToPath) {
    Add-DirectoryToUserPath -Directory $resolvedInstallDirectory
}

Write-Host "Installed DeeBugee $($release.tag_name) to:"
Write-Host $destination
if (-not $AddToPath) {
    Write-Host "Run with -AddToPath if you want to use 'dee-bugee.exe' from any terminal."
}
