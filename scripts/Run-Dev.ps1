[CmdletBinding()]
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$AppArguments
)

$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $PSScriptRoot
$executable = Join-Path $root 'target\debug\dee-bugee.exe'

function Get-SourceStamp {
    $files = @(
        Get-Item (Join-Path $root 'Cargo.toml'), (Join-Path $root 'Cargo.lock')
        Get-ChildItem (Join-Path $root 'crates') -Recurse -File -Include '*.rs', '*.toml'
    ) | Sort-Object FullName

    return ($files | ForEach-Object {
        '{0}|{1}|{2}' -f $_.FullName, $_.Length, $_.LastWriteTimeUtc.Ticks
    }) -join "`n"
}

function Stop-Viewer([System.Diagnostics.Process]$Process) {
    if ($null -eq $Process -or $Process.HasExited) {
        return
    }

    & taskkill /PID $Process.Id /T /F | Out-Null
    $Process.WaitForExit()
}

function ConvertTo-CommandLineArgument([string]$Argument) {
    if ($Argument.Length -gt 0 -and $Argument -notmatch '[\s"]') {
        return $Argument
    }

    $escaped = [regex]::Replace($Argument, '(\\*)"', '$1$1\"')
    $escaped = [regex]::Replace($escaped, '(\\+)$', '$1$1')
    return '"' + $escaped + '"'
}

Push-Location $root
try {
    $viewer = $null
    $sourceStamp = $null

    while ($true) {
        Write-Host 'Building DeeBugee development target...'
        & cargo build -p dee-bugee
        if ($LASTEXITCODE -ne 0) {
            exit $LASTEXITCODE
        }

        $argumentLine = ($AppArguments | Where-Object { $_.Length -gt 0 } | ForEach-Object {
            ConvertTo-CommandLineArgument $_
        }) -join ' '
        $viewer = if ($argumentLine.Length -gt 0) {
            Start-Process -FilePath $executable -ArgumentList $argumentLine -PassThru
        }
        else {
            Start-Process -FilePath $executable -PassThru
        }
        $sourceStamp = Get-SourceStamp
        Write-Host 'DeeBugee is running. Save Rust source files to rebuild and restart it. Press Ctrl+C to stop.'

        $restartRequested = $false
        while (-not $viewer.HasExited) {
            Start-Sleep -Milliseconds 500
            $nextSourceStamp = Get-SourceStamp
            if ($nextSourceStamp -ne $sourceStamp) {
                Write-Host 'Source change detected; restarting DeeBugee...'
                Stop-Viewer $viewer
                $restartRequested = $true
                break
            }
        }

        if (-not $restartRequested) {
            break
        }
    }
}
finally {
    Stop-Viewer $viewer
    Pop-Location
}
