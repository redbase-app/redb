#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Builds RedbApp.Api and packs it into RedbApp.tpkg for a Tsak worker.

.DESCRIPTION
    The package holds the manifest, the module config, RedbApp.Api.dll and every dependency the worker
    does not ship itself (RedbApp.Models and redb.Route.File, for example).
    deploy/shipped-module-deps.txt lists what the worker ships (its header says how to regenerate the
    list); the package contents are written next to it as RedbApp.tpkg.contents.txt.

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
$entryDll = "RedbApp.Api.dll"
$publish = Join-Path ([IO.Path]::GetTempPath()) "redbapp-publish"
$staging = Join-Path ([IO.Path]::GetTempPath()) "redbapp-tpkg"
$output = Join-Path $PSScriptRoot "output"
$package = Join-Path $output "RedbApp.tpkg"
$shippedList = Join-Path $PSScriptRoot "shipped-module-deps.txt"

# publish, not build: a class library's build output does not contain its NuGet dependencies.
if (Test-Path $publish) { Remove-Item -Recurse -Force $publish }
dotnet publish (Join-Path $module "RedbApp.Api.csproj") -c $Configuration -o $publish --nologo
if ($LASTEXITCODE -ne 0) { throw "Publish of RedbApp.Api failed." }

# The worker resolves only the assemblies it ships (shipped-module-deps.txt). Every other dependency,
# RedbApp.Models included, has to travel inside the package, otherwise the module fails to load.
$shipped = @(Get-Content $shippedList | ForEach-Object { $_.Trim() } | Where-Object { $_ -and -not $_.StartsWith('#') })
$extra = @(Get-ChildItem $publish -Filter *.dll | Where-Object { $_.Name -ne $entryDll -and $shipped -notcontains $_.Name })

if (Test-Path $staging) { Remove-Item -Recurse -Force $staging }
New-Item -ItemType Directory -Force $staging | Out-Null
New-Item -ItemType Directory -Force $output | Out-Null

Copy-Item (Join-Path $module "manifest.json") $staging
Copy-Item (Join-Path $module "RedbApp.Api.config.json") $staging
Copy-Item (Join-Path $publish $entryDll) $staging
foreach ($dependency in $extra) { Copy-Item $dependency.FullName $staging }

# Native assets of a dependency (runtimes/<rid>/native/...), if the module brought any.
$runtimes = Join-Path $publish "runtimes"
if (Test-Path $runtimes) { Copy-Item $runtimes $staging -Recurse }

if (Test-Path $package) { Remove-Item -Force $package }
Add-Type -AssemblyName System.IO.Compression.FileSystem
[IO.Compression.ZipFile]::CreateFromDirectory($staging, $package)

# What the package holds, next to it: handy when a Tsak node refuses to load the module.
$contents = Get-ChildItem $staging -Recurse -File |
    ForEach-Object { $_.FullName.Substring($staging.Length + 1).Replace('\', '/') }
Set-Content -Path ($package + ".contents.txt") -Value $contents -Encoding utf8

Write-Host "Packed $package"
Write-Host "  module:              $entryDll"
Write-Host ("  packed dependencies: {0}" -f $extra.Count)
foreach ($dependency in $extra) { Write-Host ("      " + $dependency.Name) }
Write-Host ("  not packed:          {0} assemblies shipped by the worker" -f $shipped.Count)
