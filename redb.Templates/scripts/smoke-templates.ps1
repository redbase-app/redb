<#
.SYNOPSIS
    Generates a project from every template of the redb.Templates pack, with each option that changes the
    code, and builds it.

.DESCRIPTION
    Installs the given template pack, runs `dotnet new` for each case of $Cases into a temporary folder,
    then `dotnet build` and, per case, one behavioural check: a console app is started and its output is
    read, a web site is started and a page is checked, the worker's module is packed and the package is
    inspected, the chat is started without an API key and must refuse to run without leaving a database
    behind.

    Before a release is on nuget.org, pass -LocalFeed with the folder that holds the freshly built
    redb.* packages: a nuget.config pointing at it is written next to the generated projects.

.EXAMPLE
    ./smoke-templates.ps1 -PackagePath ../../nupkg/redb.Templates.4.2.1.nupkg -LocalFeed ../../nupkg
    ./smoke-templates.ps1 -PackagePath ../../nupkg/redb.Templates.4.2.1.nupkg -Only redb-chat
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

# Build nodes keep output files locked between runs, which breaks the cleanup of the work directory.
$env:MSBUILDDISABLENODEREUSE = '1'

# Every template, and every option that puts different code into the project.
#   RunDll / ExpectOut  - start the built app (relative to the project folder) and check its output
#   ExpectFail          - the app must stop with an error instead of starting
#   Web / WebProject    - start the site, wait for the url and check the answer with Assert
#   Pack                - run deploy/pack-tpkg.ps1 and check what the package holds
$Cases = @(
    @{ Id = 'redb';                Template = 'redb';        Args = @();                          RunDll = 'bin/Release/net10.0/SmokeApp.dll';      ExpectOut = 'Cleaned up. Done!' }
    @{ Id = 'redb-free';           Template = 'redb';        Args = @('--pro', 'false');          RunDll = 'bin/Release/net10.0/SmokeApp.dll';      ExpectOut = 'Cleaned up. Done!' }
    @{ Id = 'redb-postgres';       Template = 'redb';        Args = @('--db', 'postgres') }
    @{ Id = 'redb-mssql';          Template = 'redb';        Args = @('--db', 'mssql') }
    @{ Id = 'redb-razor';          Template = 'redb-razor';  Args = @(); Web = 'http://localhost:5080/'; WebProject = '.'
       Assert = { param($page)
           # The decimal price is parsed and printed with the invariant culture: a site on a machine
           # with a comma locale must answer the same as everywhere else (see the README, "Culture").
           if ($page.Content -notmatch '1299\.00') { throw 'the price list does not use the invariant culture' } } }
    @{ Id = 'redb-blazor';         Template = 'redb-blazor'; Args = @(); Web = 'http://localhost:5081/'; WebProject = '.'
       Assert = { param($page)
           if ($page.Content -notmatch '5 product\(s\)') { throw 'the product list did not render' }
           if ($page.Content -notmatch 'prerenderId') { throw 'the page was not prerendered' } } }
    @{ Id = 'redb-worker';         Template = 'redb-worker'; Args = @(); Pack = $true }
    @{ Id = 'redb-chat';           Template = 'redb-chat';   Args = @();                          ExpectFail = $true; RunDll = 'SmokeApp.Host/bin/Release/net10.0/SmokeApp.Host.dll'
       ExpectOut = 'The LLM API key is not set'; NoFiles = '*.db' }
    @{ Id = 'redb-chat-shell';     Template = 'redb-chat';   Args = @('--tools', 'shell');        ExpectFail = $true; RunDll = 'SmokeApp.Host/bin/Release/net10.0/SmokeApp.Host.dll'
       ExpectOut = 'The LLM API key is not set'; NoFiles = '*.db' }
    @{ Id = 'redb-chat-mcp-audit'; Template = 'redb-chat';   Args = @('--tools', 'mcp', '--audit', 'true'); ExpectFail = $true; RunDll = 'SmokeApp.Host/bin/Release/net10.0/SmokeApp.Host.dll'
       ExpectOut = 'The LLM API key is not set'; NoFiles = '*.db' }
    @{ Id = 'redb-app';            Template = 'redb-app';    Args = @(); Web = 'http://localhost:5082/'; WebProject = 'SmokeApp.Web'
       Assert = { param($page)
           # .NET 10 serves the client with an import map and fingerprinted names (see the template).
           if ($page.Content -notmatch 'type="importmap"') { throw 'the client index.html has no import map' }
           if ($page.Content -notmatch 'blazor\.webassembly\.[0-9a-z]+\.js') { throw 'the client loader is not fingerprinted' } } }
    @{ Id = 'redb-bff';            Template = 'redb-bff';    Args = @(); Web = 'http://localhost:5083/login'; WebProject = 'SmokeApp.Web'
       Assert = { param($page)
           if ($page.Content -notmatch '<form') { throw 'the sign-in form is missing' } } }
)

function Invoke-Checked([string] $what, [scriptblock] $action) {
    & $action
    if ($LASTEXITCODE -ne 0) { throw "$what failed with exit code $LASTEXITCODE" }
}

$PackagePath = (Resolve-Path $PackagePath).Path

Remove-Item -Recurse -Force $WorkDir -ErrorAction SilentlyContinue
for ($attempt = 0; $attempt -lt 3 -and (Test-Path $WorkDir); $attempt++) {
    Start-Sleep -Seconds 1
    Remove-Item -Recurse -Force $WorkDir -ErrorAction SilentlyContinue
}
if (Test-Path $WorkDir) { throw "the previous work directory is locked: $WorkDir (stop the running apps and run again)" }
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

function Wait-ForUrl([string] $url, [int] $TimeoutSeconds = 90) {
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            $answer = Invoke-WebRequest $url -UseBasicParsing -TimeoutSec 5 -SkipHttpErrorCheck
            if ($answer.StatusCode -lt 500) { return $answer }
        }
        catch { }
        Start-Sleep -Milliseconds 500
    }
    throw "no answer from $url within $TimeoutSeconds s"
}

# `dotnet run` leaves the app it started behind, so kill the children first.
function Stop-ProcessTree([int] $ProcessId) {
    Get-CimInstance Win32_Process -Filter "ParentProcessId=$ProcessId" -ErrorAction SilentlyContinue |
        ForEach-Object { Stop-ProcessTree $_.ProcessId }
    Stop-Process -Id $ProcessId -Force -ErrorAction SilentlyContinue
}

# The pack script must put the module and its own dependencies in the package and nothing the worker
# already ships, which is what deploy/shipped-module-deps.txt lists.
function Invoke-PackageCheck([string] $ProjectDir, [string] $PackageName, [string] $ModuleDll) {
    Push-Location $ProjectDir
    try {
        & (Join-Path $ProjectDir 'deploy\pack-tpkg.ps1') | Out-Null
        $package = Join-Path $ProjectDir ('deploy\output\' + $PackageName)
        if (-not (Test-Path $package)) { throw "the pack script produced no $PackageName" }

        Add-Type -AssemblyName System.IO.Compression.FileSystem
        $zip = [IO.Compression.ZipFile]::OpenRead($package)
        try {
            $names = @($zip.Entries | ForEach-Object { $_.Name })
            $shipped = @(Get-Content (Join-Path $ProjectDir 'deploy\shipped-module-deps.txt') |
                ForEach-Object { $_.Trim() } | Where-Object { $_ -and -not $_.StartsWith('#') })

            $repeated = @($names | Where-Object { $shipped -contains $_ })
            if ($repeated.Count -gt 0) { throw "the package repeats what the worker ships: $($repeated -join ', ')" }
            if ($names -notcontains $ModuleDll) { throw "$ModuleDll is not in the package" }
            if ($names -notcontains 'manifest.json') { throw 'manifest.json is not in the package' }
            if (-not (Test-Path ($package + '.contents.txt'))) { throw 'the package contents list was not written' }
            Write-Host "     package: $($names.Count) entries" -ForegroundColor DarkGray
        }
        finally { $zip.Dispose() }
    }
    finally { Pop-Location }
}

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

        if ($case.Web) {
            $projectDir = if ($case.WebProject -eq '.') { $out } else { Join-Path $out $case.WebProject }
            $env:ASPNETCORE_ENVIRONMENT = 'Development'
            $site = Start-Process dotnet -ArgumentList @('run', '-c', 'Release', '--no-build', '--project', $projectDir) -PassThru `
                -RedirectStandardOutput (Join-Path $out 'smoke.out.log') -RedirectStandardError (Join-Path $out 'smoke.err.log')
            try {
                $answer = Wait-ForUrl $case.Web
                & $case.Assert $answer
            }
            finally { Stop-ProcessTree $site.Id }
        }

        if ($case.RunDll -or $case.ExpectFail) {
            Push-Location $out
            try {
                $output = (& dotnet ($case.RunDll -replace '/', '\') 2>&1 | Out-String)
                Write-Host $output -ForegroundColor DarkGray
                if ($case.ExpectFail -and $LASTEXITCODE -eq 0) { throw 'the app was expected to stop with an error, it started instead' }
                if ($case.RunDll -and -not $case.ExpectFail -and $LASTEXITCODE -ne 0) { throw "the app exited with $LASTEXITCODE" }
                if ($case.ExpectOut -and $output -notmatch $case.ExpectOut) { throw "the output has no '$($case.ExpectOut)'" }
                if ($case.NoFiles) {
                    $left = @(Get-ChildItem -Path $out -Filter $case.NoFiles -ErrorAction SilentlyContinue)
                    if ($left.Count -gt 0) { throw "the app created $($left[0].Name) before it stopped" }
                }
            }
            finally { Pop-Location }
        }

        if ($case.Pack) { Invoke-PackageCheck $out 'SmokeApp.tpkg' 'SmokeApp.Module.dll' }

        Write-Host "OK   $id" -ForegroundColor Green
    }
    catch {
        Write-Host "FAIL $id : $($_.Exception.Message)" -ForegroundColor Red
        $failed += $id
    }
}

if ($failed.Count -gt 0) { throw "Failed cases: $($failed -join ', ')" }
Write-Host "All cases passed." -ForegroundColor Green
