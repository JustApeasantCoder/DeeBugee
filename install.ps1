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
.\install.ps1 -Version 1.0.30 -AddToPath
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
$assetNames = @("dee-bugee.exe", "dee-bugee-updater.exe")
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
$assets = foreach ($assetName in $assetNames) {
    $asset = @($release.assets | Where-Object { $_.name -eq $assetName })
    if ($asset.Count -ne 1) {
        throw "Expected exactly one '$assetName' asset in release '$($release.tag_name)', but found $($asset.Count)."
    }
    $digest = [string]$asset[0].digest
    if ($digest -notmatch "^sha256:([0-9a-fA-F]{64})$") {
        throw "Release asset '$assetName' does not provide a valid SHA-256 digest. Installation was stopped."
    }
    [PSCustomObject]@{
        Name = $assetName
        DownloadUrl = $asset[0].browser_download_url
        ExpectedHash = $Matches[1].ToUpperInvariant()
    }
}

$resolvedInstallDirectory = [IO.Path]::GetFullPath($InstallDirectory)
[IO.Directory]::CreateDirectory($resolvedInstallDirectory) | Out-Null
$stagedDownloads = @()
$backups = @()
$previousProgressPreference = $ProgressPreference

try {
    $ProgressPreference = "SilentlyContinue"
    foreach ($asset in $assets) {
        $stagedDownload = Join-Path $resolvedInstallDirectory ".$($asset.Name).download.$([Guid]::NewGuid().ToString('N'))"
        $stagedDownloads += $stagedDownload
        Write-Host "Downloading $($asset.Name) $($release.tag_name)..."
        Invoke-WebRequest -Uri $asset.DownloadUrl -Headers $apiHeaders -OutFile $stagedDownload -UseBasicParsing

        $actualHash = (Get-FileHash -LiteralPath $stagedDownload -Algorithm SHA256).Hash.ToUpperInvariant()
        if ($actualHash -ne $asset.ExpectedHash) {
            throw "SHA-256 verification failed for '$($asset.Name)'. Installation was stopped."
        }
    }

    foreach ($index in 0..($assets.Count - 1)) {
        $asset = $assets[$index]
        $destination = Join-Path $resolvedInstallDirectory $asset.Name
        $stagedDownload = $stagedDownloads[$index]
        $backup = Join-Path $resolvedInstallDirectory ".$($asset.Name).backup.$([Guid]::NewGuid().ToString('N'))"
        if (Test-Path -LiteralPath $destination -PathType Leaf) {
            [IO.File]::Replace($stagedDownload, $destination, $backup, $true)
            $backups += $backup
        }
        else {
            [IO.File]::Move($stagedDownload, $destination)
        }
    }
}
finally {
    $ProgressPreference = $previousProgressPreference
    foreach ($stagedDownload in $stagedDownloads) {
        if (Test-Path -LiteralPath $stagedDownload) {
            Remove-Item -LiteralPath $stagedDownload -Force
        }
    }
    foreach ($backup in $backups) {
        if (Test-Path -LiteralPath $backup) {
            Remove-Item -LiteralPath $backup -Force
        }
    }
}

if ($AddToPath) {
    Add-DirectoryToUserPath -Directory $resolvedInstallDirectory
}

Write-Host "Installed DeeBugee $($release.tag_name) to:"
Write-Host (Join-Path $resolvedInstallDirectory "dee-bugee.exe")
if (-not $AddToPath) {
    Write-Host "Run with -AddToPath if you want to use 'dee-bugee.exe' from any terminal."
}
