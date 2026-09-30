<#
.SYNOPSIS
    Generates a project from every template of the redb.Templates pack, with each option that changes the
    code, and builds it.

.DESCRIPTION
    Installs the given template pack, runs `dotnet new` for each case of $Cases into a temporary folder,
    then `dotnet build`. A case with Run = $true is also started and must finish on its own with exit
    code 0; the web sites, the worker and the chat keep running, so they are built only.

    Before a release is on nuget.org, pass -LocalFeed with the folder that holds the freshly built
    redb.* packages: a nuget.config pointing at it is written next to the generated projects.

.EXAMPLE
    ./smoke-templates.ps1 -PackagePath ../bin/Release/redb.Templates.4.2.0.nupkg -LocalFeed ../../nupkg
    ./smoke-templates.ps1 -PackagePath ../bin/Release/redb.Templates.4.2.0.nupkg -Only redb-chat
#>
param(
    [Parameter(Mandatory = $true)]
    [string] $PackagePath,

    [string] $LocalFeed,

    # Short names to run; empty runs every case.
    [string[]] $Only = @(),

    [string] $WorkDir = (Join-Path ([System.IO.Path]::GetTempPath()) "redb-templates-smoke")
)

$ErrorActionPreference = 'Stop'

# Every template, and every option that puts different code into the project.
$Cases = @(
    @{ Id = 'redb';                Template = 'redb';        Args = @();                          Run = $true  }
    @{ Id = 'redb-free';           Template = 'redb';        Args = @('--pro', 'false');          Run = $true  }
    @{ Id = 'redb-postgres';       Template = 'redb';        Args = @('--db', 'postgres');        Run = $false }
    @{ Id = 'redb-mssql';          Template = 'redb';        Args = @('--db', 'mssql');           Run = $false }
    @{ Id = 'redb-razor';          Template = 'redb-razor';  Args = @();                          Run = $false }
    @{ Id = 'redb-blazor';         Template = 'redb-blazor'; Args = @();                          Run = $false }
    @{ Id = 'redb-worker';         Template = 'redb-worker'; Args = @();                          Run = $false }
    @{ Id = 'redb-chat';           Template = 'redb-chat';   Args = @();                          Run = $false }
    @{ Id = 'redb-chat-shell';     Template = 'redb-chat';   Args = @('--tools', 'shell');        Run = $false }
    @{ Id = 'redb-chat-mcp-audit'; Template = 'redb-chat';   Args = @('--tools', 'mcp', '--audit', 'true'); Run = $false }
    @{ Id = 'redb-app';            Template = 'redb-app';    Args = @();                          Run = $false }
    @{ Id = 'redb-bff';            Template = 'redb-bff';    Args = @();                          Run = $false }
)

function Invoke-Checked([string] $what, [scriptblock] $action) {
    & $action
    if ($LASTEXITCODE -ne 0) { throw "$what failed with exit code $LASTEXITCODE" }
}

$PackagePath = (Resolve-Path $PackagePath).Path

if (Test-Path $WorkDir) { Remove-Item -Recurse -Force $WorkDir }
New-Item -ItemType Directory -Force $WorkDir | Out-Null

if ($LocalFeed) {
    $feed = (Resolve-Path $LocalFeed).Path
    @"
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <packageSources>
    <clear />
    <add key="local" value="$feed" />
    <add key="nuget.org" value="https://api.nuget.org/v3/index.json" />
  </packageSources>
</configuration>
"@ | Set-Content -Path (Join-Path $WorkDir 'nuget.config') -Encoding utf8
}

# `dotnet new install` over an already installed pack of the same id replaces it.
Invoke-Checked "dotnet new install" { dotnet new install $PackagePath --force }

$failed = @()
foreach ($case in $Cases) {
    if ($Only.Count -gt 0 -and $Only -notcontains $case.Template) { continue }

    $id = $case.Id
    $out = Join-Path $WorkDir $id
    Write-Host "=== $id ===" -ForegroundColor Cyan
    try {
        $newArgs = @($case.Template, '-n', 'SmokeApp', '-o', $out) + $case.Args
        Invoke-Checked "dotnet new $id" { dotnet new @newArgs }
        Invoke-Checked "dotnet build $id" { dotnet build $out -c Release }
        if ($case.Run) {
            Push-Location $out
            try { Invoke-Checked "dotnet run $id" { dotnet run -c Release --no-build } }
            finally { Pop-Location }
        }
        Write-Host "OK   $id" -ForegroundColor Green
    }
    catch {
        Write-Host "FAIL $id : $($_.Exception.Message)" -ForegroundColor Red
        $failed += $id
    }
}

if ($failed.Count -gt 0) { throw "Failed cases: $($failed -join ', ')" }
Write-Host "All cases passed." -ForegroundColor Green
