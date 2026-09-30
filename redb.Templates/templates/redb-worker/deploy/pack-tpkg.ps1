#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Builds RedbWorker.Module and packs it into RedbWorker.tpkg for a Tsak worker.

.DESCRIPTION
    The package holds the manifest, the module config and RedbWorker.Module.dll. redb and redb.Route
    ship with the Tsak worker, so they stay out of the package. A module that uses a NuGet package the
    worker does not ship copies that DLL into the package too.

.EXAMPLE
    ./deploy/pack-tpkg.ps1
    ./deploy/pack-tpkg.ps1 -Configuration Debug
#>
param(
    [ValidateSet("Debug", "Release")]
    [string]$Configuration = "Release"
)

$ErrorActionPreference = "Stop"

$root = Resolve-Path (Join-Path $PSScriptRoot "..")
$module = Join-Path $root "RedbWorker.Module"
$publish = Join-Path ([IO.Path]::GetTempPath()) "redbworker-publish"
$staging = Join-Path ([IO.Path]::GetTempPath()) "redbworker-tpkg"
$output = Join-Path $PSScriptRoot "output"
$package = Join-Path $output "RedbWorker.tpkg"

# publish, not build: a class library's build output does not contain its NuGet dependencies.
if (Test-Path $publish) { Remove-Item -Recurse -Force $publish }
dotnet publish (Join-Path $module "RedbWorker.Module.csproj") -c $Configuration -o $publish --nologo
if ($LASTEXITCODE -ne 0) { throw "Publish of RedbWorker.Module failed." }

if (Test-Path $staging) { Remove-Item -Recurse -Force $staging }
New-Item -ItemType Directory -Force $staging | Out-Null
New-Item -ItemType Directory -Force $output | Out-Null

Copy-Item (Join-Path $module "manifest.json") $staging
Copy-Item (Join-Path $module "RedbWorker.Module.config.json") $staging
Copy-Item (Join-Path $publish "RedbWorker.Module.dll") $staging

if (Test-Path $package) { Remove-Item -Force $package }
Add-Type -AssemblyName System.IO.Compression.FileSystem
[IO.Compression.ZipFile]::CreateFromDirectory($staging, $package)

Write-Host "Packed $package"
