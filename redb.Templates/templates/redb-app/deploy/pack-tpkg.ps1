#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Builds RedbApp.Api and packs it into RedbApp.tpkg for a Tsak worker.

.DESCRIPTION
    The package holds the manifest, the module config, RedbApp.Api.dll and RedbApp.Models.dll. redb,
    redb.Route and Microsoft.IdentityModel ship with the Tsak worker, so they stay out of the package.

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
$module = Join-Path $root "RedbApp.Api"
$publish = Join-Path ([IO.Path]::GetTempPath()) "redbapp-publish"
$staging = Join-Path ([IO.Path]::GetTempPath()) "redbapp-tpkg"
$output = Join-Path $PSScriptRoot "output"
$package = Join-Path $output "RedbApp.tpkg"

# publish, not build: a class library's build output does not contain its NuGet dependencies.
if (Test-Path $publish) { Remove-Item -Recurse -Force $publish }
dotnet publish (Join-Path $module "RedbApp.Api.csproj") -c $Configuration -o $publish --nologo
if ($LASTEXITCODE -ne 0) { throw "Publish of RedbApp.Api failed." }

if (Test-Path $staging) { Remove-Item -Recurse -Force $staging }
New-Item -ItemType Directory -Force $staging | Out-Null
New-Item -ItemType Directory -Force $output | Out-Null

Copy-Item (Join-Path $module "manifest.json") $staging
Copy-Item (Join-Path $module "RedbApp.Api.config.json") $staging
Copy-Item (Join-Path $publish "RedbApp.Api.dll") $staging
Copy-Item (Join-Path $publish "RedbApp.Models.dll") $staging

if (Test-Path $package) { Remove-Item -Force $package }
Add-Type -AssemblyName System.IO.Compression.FileSystem
[IO.Compression.ZipFile]::CreateFromDirectory($staging, $package)

Write-Host "Packed $package"
