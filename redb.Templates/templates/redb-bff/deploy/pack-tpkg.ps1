#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Builds RedbBff.Backend and packs it into RedbBff.tpkg for a Tsak worker.

.DESCRIPTION
    The package holds the manifest, the module config, RedbBff.Backend.dll and RedbBff.Models.dll. redb,
    redb.Route and redb.Route.Controllers ship with the Tsak worker, so they stay out of the package.

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
$module = Join-Path $root "RedbBff.Backend"
$publish = Join-Path ([IO.Path]::GetTempPath()) "redbbff-publish"
$staging = Join-Path ([IO.Path]::GetTempPath()) "redbbff-tpkg"
$output = Join-Path $PSScriptRoot "output"
$package = Join-Path $output "RedbBff.tpkg"

# publish, not build: a class library's build output does not contain its NuGet dependencies.
if (Test-Path $publish) { Remove-Item -Recurse -Force $publish }
dotnet publish (Join-Path $module "RedbBff.Backend.csproj") -c $Configuration -o $publish --nologo
if ($LASTEXITCODE -ne 0) { throw "Publish of RedbBff.Backend failed." }

if (Test-Path $staging) { Remove-Item -Recurse -Force $staging }
New-Item -ItemType Directory -Force $staging | Out-Null
New-Item -ItemType Directory -Force $output | Out-Null

Copy-Item (Join-Path $module "manifest.json") $staging
Copy-Item (Join-Path $module "RedbBff.Backend.config.json") $staging
Copy-Item (Join-Path $publish "RedbBff.Backend.dll") $staging
Copy-Item (Join-Path $publish "RedbBff.Models.dll") $staging

if (Test-Path $package) { Remove-Item -Force $package }
Add-Type -AssemblyName System.IO.Compression.FileSystem
[IO.Compression.ZipFile]::CreateFromDirectory($staging, $package)

Write-Host "Packed $package"
