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
    redb.* packages: a nuget.config pointing at it is written next to the generated projects, and the
    restored packages go to <WorkDir>/packages instead of the global NuGet cache.

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
# pwsh -File passes "-Only a,b" as ONE element; split it like the other release scripts do. Without this a list
# matched no template, no case ran, and the run still ended with "All cases passed".
$Only = @($Only | ForEach-Object { $_ -split ',' } | ForEach-Object { $_.Trim() } | Where-Object { $_ })

# Build nodes keep output files locked between runs, which breaks the cleanup of the work directory.
$env:MSBUILDDISABLENODEREUSE = '1'

# Every template, and every option that puts different code into the project.
#   RunDll / ExpectOut  - start the built app (relative to the project folder) and check its output
#   ExpectFail          - the app must stop with an error instead of starting
#   Web / WebProject    - start the site, wait for the url and check the answer with Assert
#   Pack / PackHas      - run deploy/pack-tpkg.ps1 and check what the package holds, PackHas: files it must carry
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
    # An app on redb.Identity in a Tsak worker: the Web site starts in Development without Identity and without a
    # database (OIDC calls Identity only at sign-in). redb.Identity.Contracts.dll comes from the worker's Identity
    # contracts carrier and must stay out of the package (shipped-module-deps.txt).
    @{ Id = 'redb-identity-app';       Template = 'redb-identity-app'; Args = @(); Pack = $true; PackHas = @('SmokeApp.Contracts.dll'); Web = 'http://localhost:5085/'; WebProject = 'SmokeApp.Web'
       Assert = { param($page)
           if ($page.Content -notmatch 'account/login') { throw 'the sign-in link is missing' } } }
    @{ Id = 'redb-identity-app-noweb'; Template = 'redb-identity-app'; Args = @('--web', 'false'); Pack = $true; PackHas = @('SmokeApp.Contracts.dll') }
    @{ Id = 'redb-identity-app-net8';  Template = 'redb-identity-app'; Args = @('--framework', 'net8.0') }
    # Route-XML modules (template.json tags.redbSmoke = "pack"): the build packs the module with the redb-route-xml
    # tool of .config/dotnet-tools.json (restored by the build itself), no broker or database needed. The package is
    # named after the project in lower case.
    @{ Id = 'redb-route';               Template = 'redb-route'; Args = @(); RoutePack = @{ Entries = @('SmokeApp.dll') } }
    @{ Id = 'redb-route-orders-net8';   Template = 'redb-route'; Args = @('--context', 'orders', '--framework', 'net8.0'); RoutePack = @{ ContextName = 'orders'; Entries = @('SmokeApp.dll') } }
    # A key without a default must reach the manifest, so the worker refuses to start the module without it.
    @{ Id = 'redb-route-rabbitmq';      Template = 'redb-route-rabbitmq'; Args = @(); RoutePack = @{ RequiredConfigKey = 'rabbit.password'; Entries = @('SmokeApp.dll') } }
    @{ Id = 'redb-route-kafka';         Template = 'redb-route-kafka';    Args = @(); RoutePack = @{ RequiredConfigKey = 'kafka.brokers'; Entries = @('SmokeApp.dll') } }
    # REST over redb.Route.Http: no key (the database bean of context.xml is a comment), the JSON schema of the order
    # travels in the package.
    @{ Id = 'redb-route-rest';          Template = 'redb-route-rest';     Args = @()
       RoutePack = @{ NoRequiredConfigKeys = $true; Entries = @('resources/order.schema.json', 'SmokeApp.dll'); PackageRef = 'redb.Route.Http' } }
    @{ Id = 'redb-route-eip';          Template = 'redb-route-eip';      Args = @(); RoutePack = @{ NoRequiredConfigKeys = $true; Entries = @('SmokeApp.dll') } }
    @{ Id = 'redb-route-scatter-gather'; Template = 'redb-route-scatter-gather'; Args = @(); RoutePack = @{ NoRequiredConfigKeys = $true; Entries = @('SmokeApp.dll') } }
    # AS4 through the redb.Route.As4 component: the signing key, its password and the partner certificate have no default.
    # The deploy/ stand (Holodeck, docker) is not started by the smoke.
    @{ Id = 'redb-route-as4';           Template = 'redb-route-as4';      Args = @()
       RoutePack = @{ RequiredConfigKeys = @('as4.keyFile', 'as4.keyPassword', 'as4.partnerCertificateFile'); Entries = @('SmokeApp.dll') } }
    # AS2 through redb.Route.As2, packed without a partner: the signing key, its password and the partner certificate
    # have no default. The deploy/ OpenAS2 stand is not started by the smoke.
    @{ Id = 'redb-route-as2';           Template = 'redb-route-as2';      Args = @()
       RoutePack = @{ RequiredConfigKeys = @('as2.keyFile', 'as2.keyPassword', 'as2.partnerCertificateFile'); Entries = @('SmokeApp.dll') } }
    # LDAP through redb.Route.Ldap, packed without a directory: every connection key has no default. The deploy/ OpenLDAP
    # stand is not started by the smoke.
    @{ Id = 'redb-route-ldap';          Template = 'redb-route-ldap';     Args = @()
       RoutePack = @{ RequiredConfigKeys = @('ldap.baseDn', 'ldap.bindDn', 'ldap.bindPassword', 'ldap.usersDn'); Entries = @('SmokeApp.dll') } }
    # S3 through redb.Route.S3, packed without a store: the access key and the secret have no default. The deploy/ RustFS
    # stand is not started by the smoke.
    @{ Id = 'redb-route-s3';            Template = 'redb-route-s3';       Args = @()
       RoutePack = @{ RequiredConfigKeys = @('s3.accessKey', 's3.secretKey'); Entries = @('SmokeApp.dll') } }
    # Mail through redb.Route.Mail, packed without a server: both routes (send and read) travel in the package and every
    # account key has no default. The deploy/ GreenMail stand is not started by the smoke.
    @{ Id = 'redb-route-email';         Template = 'redb-route-email';    Args = @()
       RoutePack = @{ RequiredConfigKeys = @('mail.from', 'mail.password', 'mail.to', 'mail.username'); Entries = @('routes/send.route.xml', 'routes/read.route.xml', 'SmokeApp.dll') } }
    # The transport symbol (file, ftp, sftp): the XSD travels in the package, a server transport asks for its account,
    # the project references the component of the chosen transport and no template condition is left anywhere.
    @{ Id = 'redb-route-file-file';     Template = 'redb-route-file'; Args = @('--transport', 'file')
       RoutePack = @{ NoRequiredConfigKeys = $true; Entries = @('resources/order.xsd', 'SmokeApp.dll'); PackageRef = 'redb.Route.File'; NoConditionsIn = @('context.xml', 'deploy/docker-compose.files.yml', 'config/context.sample.json', 'routes/CsvFilesRoute.cs') } }
    @{ Id = 'redb-route-file-ftp';      Template = 'redb-route-file'; Args = @('--transport', 'ftp')
       RoutePack = @{ RequiredConfigKeys = @('files.password', 'files.username'); Entries = @('resources/order.xsd', 'SmokeApp.dll'); PackageRef = 'redb.Route.Ftp'; NoConditionsIn = @('context.xml', 'deploy/docker-compose.files.yml', 'config/context.sample.json', 'routes/CsvFilesRoute.cs') } }
    @{ Id = 'redb-route-file-sftp';     Template = 'redb-route-file'; Args = @('--transport', 'sftp')
       RoutePack = @{ RequiredConfigKeys = @('files.password', 'files.username'); Entries = @('resources/order.xsd', 'SmokeApp.dll'); PackageRef = 'redb.Route.Sftp'; NoConditionsIn = @('context.xml', 'deploy/docker-compose.files.yml', 'config/context.sample.json', 'routes/CsvFilesRoute.cs') } }
    # AMQP 1.0 through redb.Route.Amqp, packed without a broker: the user and the password have no default. The deploy/
    # broker stand is not started by the smoke.
    @{ Id = 'redb-route-amqp';          Template = 'redb-route-amqp';     Args = @()
       RoutePack = @{ RequiredConfigKeys = @('amqp.password', 'amqp.user'); Entries = @('SmokeApp.dll'); PackageRef = 'redb.Route.Amqp' } }
    # MQTT through redb.Route.MqttNet, packed without a broker: the username and the password have no default. The deploy/
    # Mosquitto stand is not started by the smoke.
    @{ Id = 'redb-route-mqtt';          Template = 'redb-route-mqtt';     Args = @()
       RoutePack = @{ RequiredConfigKeys = @('mqtt.password', 'mqtt.username'); Entries = @('SmokeApp.dll'); PackageRef = 'redb.Route.MqttNet' } }
    # Scheduled routes through redb.Route.Quartz: nothing external, no key.
    @{ Id = 'redb-route-cron';          Template = 'redb-route-cron';     Args = @(); RoutePack = @{ NoRequiredConfigKeys = $true; Entries = @('SmokeApp.dll') } }
    # A C# bean class in the project: RoutePackageIncludeAssembly=true makes the build pass the project's DLL as --entry.
    @{ Id = 'redb-route-beans';         Template = 'redb-route-beans';    Args = @()
       RoutePack = @{ NoRequiredConfigKeys = $true; Entries = @('SmokeApp.dll') } }
    # A module on redb itself: the store is configured by the Redb.local section of context.json, not by keys. The sample
    # config the user copies next to the package names the chosen provider, with the template conditions resolved.
    @{ Id = 'redb-route-redb-sqlite';   Template = 'redb-route-redb'; Args = @('--db', 'sqlite');   RoutePack = @{ NoRequiredConfigKeys = $true; Entries = @('SmokeApp.dll'); SampleProvider = 'sqlite' } }
    @{ Id = 'redb-route-redb-postgres'; Template = 'redb-route-redb'; Args = @('--db', 'postgres'); RoutePack = @{ NoRequiredConfigKeys = $true; Entries = @('SmokeApp.dll'); SampleProvider = 'postgres' } }
    @{ Id = 'redb-route-redb-mssql';    Template = 'redb-route-redb'; Args = @('--db', 'mssql');    RoutePack = @{ NoRequiredConfigKeys = $true; Entries = @('SmokeApp.dll'); SampleProvider = 'mssql' } }
    @{ Id = 'redb-route-validator-xslt'; Template = 'redb-route-validator-xslt'; Args = @()
       RoutePack = @{ NoRequiredConfigKeys = $true; Entries = @('resources/order.xsd', 'resources/order-to-invoice.xslt', 'SmokeApp.dll') } }
    # The db symbol: SQLite needs no key, a server database asks for db.connection; the conditions of the template are
    # resolved in the packed context.xml (no "#if" left) and it names the provider of the chosen database.
    @{ Id = 'redb-route-sql-sqlite';         Template = 'redb-route-sql';       Args = @('--db', 'sqlite');   RoutePack = @{ NoRequiredConfigKeys = $true;          Provider = 'Microsoft.Data.Sqlite'; Entries = @('SmokeApp.dll') } }
    @{ Id = 'redb-route-sql-postgres';       Template = 'redb-route-sql';       Args = @('--db', 'postgres'); RoutePack = @{ RequiredConfigKey = 'db.connection'; Provider = 'Npgsql'; Entries = @('SmokeApp.dll') } }
    @{ Id = 'redb-route-sql-mssql';          Template = 'redb-route-sql';       Args = @('--db', 'mssql');    RoutePack = @{ RequiredConfigKey = 'db.connection'; Provider = 'Microsoft.Data.SqlClient'; Entries = @('SmokeApp.dll') } }
    @{ Id = 'redb-route-sql-batch-sqlite';   Template = 'redb-route-sql-batch'; Args = @('--db', 'sqlite');   RoutePack = @{ NoRequiredConfigKeys = $true;          Provider = 'Microsoft.Data.Sqlite'; Entries = @('SmokeApp.dll') } }
    @{ Id = 'redb-route-sql-batch-postgres'; Template = 'redb-route-sql-batch'; Args = @('--db', 'postgres'); RoutePack = @{ RequiredConfigKey = 'db.connection'; Provider = 'Npgsql'; Entries = @('SmokeApp.dll') } }
    @{ Id = 'redb-route-sql-batch-mssql';    Template = 'redb-route-sql-batch'; Args = @('--db', 'mssql');    RoutePack = @{ RequiredConfigKey = 'db.connection'; Provider = 'Microsoft.Data.SqlClient'; Entries = @('SmokeApp.dll') } }
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
    # The packages restored from the local feed must not land in the global cache: there they would stand in
    # for the published packages of the same version on every later restore of this machine. They go to a
    # folder of the work directory, which the next run removes with it. Restored paths run to ~190
    # characters below this folder, so a long -WorkDir hits MAX_PATH and the build loses its references.
    $env:NUGET_PACKAGES = Join-Path $WorkDir 'packages'
}

# `dotnet new install --force` over an installed pack of the same id adds a second entry instead of replacing
# it, and every `dotnet new` of its templates then fails with "Sequence contains more than one matching
# element" (exit 70). Every entry of the id goes first; it is redb.Templates, whichever source it came from.
for ($i = 0; $i -lt 5; $i++) { dotnet new uninstall redb.Templates *> $null; if ($LASTEXITCODE -ne 0) { break } }
Invoke-Checked "dotnet new install" { dotnet new install $PackagePath }

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
# A Route-XML module packed by its build: pkg/<name>-0.0.1.tpkg, and the context name the template was asked for
# in pkg/<name>-0.0.1/<name>.config.json.
function Invoke-RoutePackCheck([string] $ProjectDir, [string] $Name, [hashtable] $Expect) {
    $package = Join-Path $ProjectDir "pkg\$Name-0.0.1.tpkg"
    if (-not (Test-Path $package)) { throw "the build produced no pkg\$Name-0.0.1.tpkg" }
    # Every redb-route-* carries an AGENTS.md assembled at pack time (route-xml/_agents + the scenario part of the
    # template). Its placeholders must be replaced by the template engine like everywhere else in the project.
    $agents = Join-Path $ProjectDir 'AGENTS.md'
    if (-not (Test-Path $agents)) { throw 'the project has no AGENTS.md' }
    $agentsText = Get-Content $agents -Raw
    $left = @('modulectx', 'routemodule', 'RouteModule' | Where-Object { $agentsText -cmatch "\b$_\b" })
    if ($left.Count -gt 0) { throw "AGENTS.md still carries the template placeholder(s): $($left -join ', ')" }
    if ($Expect.ContextName) {
        $config = Join-Path $ProjectDir "pkg\$Name-0.0.1\$Name.config.json"
        if (-not (Test-Path $config)) { throw "no pkg\$Name-0.0.1\$Name.config.json" }
        $context = (Get-Content $config -Raw | ConvertFrom-Json).ContextName
        if ($context -ne $Expect.ContextName) { throw "ContextName is '$context', expected '$($Expect.ContextName)'" }
    }
    if ($Expect.RequiredConfigKey -or $Expect.RequiredConfigKeys -or $Expect.NoRequiredConfigKeys) {
        $manifest = Join-Path $ProjectDir "pkg\$Name-0.0.1\manifest.json"
        if (-not (Test-Path $manifest)) { throw "no pkg\$Name-0.0.1\manifest.json" }
        $keys = @((Get-Content $manifest -Raw | ConvertFrom-Json).RequiredConfigKeys | Where-Object { $_ })
        # RequiredConfigKeys: the exact set, in any order; a key short or one too many both fail.
        if ($Expect.RequiredConfigKeys) {
            $diff = @(Compare-Object @($Expect.RequiredConfigKeys) $keys)
            if ($diff.Count -gt 0) { throw "RequiredConfigKeys is [$($keys -join ', ')], expected [$($Expect.RequiredConfigKeys -join ', ')]" }
        }
        if ($Expect.RequiredConfigKey -and $keys -notcontains $Expect.RequiredConfigKey) { throw "RequiredConfigKeys has no $($Expect.RequiredConfigKey) (has: $($keys -join ', '))" }
        # A module that needs no key must not ask for one: the worker would refuse to start it.
        if ($Expect.NoRequiredConfigKeys -and $keys.Count -gt 0) { throw "RequiredConfigKeys should be empty, has: $($keys -join ', ')" }
    }
    if ($Expect.Entries) {
        # Resources the routes read at run time travel inside the package, not next to it.
        Add-Type -AssemblyName System.IO.Compression.FileSystem
        $zip = [IO.Compression.ZipFile]::OpenRead($package)
        try { $inside = @($zip.Entries | ForEach-Object { $_.FullName -replace '\\', '/' }) } finally { $zip.Dispose() }
        $missing = @($Expect.Entries | Where-Object { $inside -notcontains $_ })
        if ($missing.Count -gt 0) { throw "the package has no $($missing -join ', ')" }
    }
    if ($Expect.PackageRef) {
        $csproj = Join-Path $ProjectDir 'SmokeApp.csproj'
        if ((Get-Content $csproj -Raw) -notmatch ('<PackageReference\s+Include="' + [regex]::Escape($Expect.PackageRef) + '"')) { throw "SmokeApp.csproj has no PackageReference $($Expect.PackageRef)" }
    }
    if ($Expect.NoConditionsIn) {
        # The packed context.xml as well as the project files the user edits or runs.
        foreach ($rel in @($Expect.NoConditionsIn) + "pkg/$Name-0.0.1/context.xml") {
            $file = Join-Path $ProjectDir ($rel -replace '/', '\')
            if (-not (Test-Path $file)) { throw "no $rel" }
            if ((Get-Content $file -Raw) -match '#if\b') { throw "$rel still carries #if: the template conditions were not resolved" }
        }
    }
    if ($Expect.SampleProvider) {
        $sample = Join-Path $ProjectDir 'config\context.sample.json'
        if (-not (Test-Path $sample)) { throw 'no config\context.sample.json' }
        $sampleText = Get-Content $sample -Raw
        if ($sampleText -match '//\s*#if\b') { throw 'context.sample.json still carries //#if: the template conditions were not resolved' }
        $providers = @([regex]::Matches($sampleText, '"Provider"\s*:\s*"(?<v>[^"]+)"') | ForEach-Object { $_.Groups['v'].Value } | Sort-Object -Unique)
        if ($providers.Count -eq 0) { throw 'context.sample.json names no Provider' }
        if (@($providers | Where-Object { $_ -ne $Expect.SampleProvider }).Count -gt 0) { throw "context.sample.json Provider is $($providers -join ', '), expected $($Expect.SampleProvider)" }
    }
    if ($Expect.Provider) {
        $contextXml = Join-Path $ProjectDir "pkg\$Name-0.0.1\context.xml"
        if (-not (Test-Path $contextXml)) { throw "no pkg\$Name-0.0.1\context.xml" }
        $text = Get-Content $contextXml -Raw
        if ($text -match '#if\b') { throw 'context.xml still carries #if: the template conditions were not resolved' }
        # <property key="ProviderName" value="..."/> of the redb.Route XML, or an attribute ProviderName="...".
        $declared = @([regex]::Matches($text, 'key\s*=\s*"ProviderName"\s+value\s*=\s*"(?<v>[^"]+)"|\bProviderName\s*=\s*"(?<v>[^"]+)"') |
                      ForEach-Object { $_.Groups['v'].Value } | Sort-Object -Unique)
        if ($declared.Count -eq 0) { throw 'context.xml declares no ProviderName' }
        if (@($declared | Where-Object { $_ -ne $Expect.Provider }).Count -gt 0) { throw "ProviderName is $($declared -join ', '), expected $($Expect.Provider)" }
    }
    Write-Host ("     package: pkg\{0}-0.0.1.tpkg, {1:N0} KB" -f $Name, ((Get-Item $package).Length / 1KB)) -ForegroundColor DarkGray
}

function Invoke-PackageCheck([string] $ProjectDir, [string] $PackageName, [string] $ModuleDll, [string[]] $MustHave = @()) {
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
            $absent = @($MustHave | Where-Object { $names -notcontains $_ })
            if ($absent.Count -gt 0) { throw "the package has no $($absent -join ', ')" }
            if (-not (Test-Path ($package + '.contents.txt'))) { throw 'the package contents list was not written' }
            Write-Host "     package: $($names.Count) entries" -ForegroundColor DarkGray
        }
        finally { $zip.Dispose() }
    }
    finally { Pop-Location }
}

$failed = @()
$ran = 0
foreach ($case in $Cases) {
    if ($Only.Count -gt 0 -and $Only -notcontains $case.Template) { continue }
    $ran++

    $id = $case.Id
    $out = Join-Path $WorkDir $id
    Write-Host "=== $id ===" -ForegroundColor Cyan
    try {
        $newArgs = @($case.Template, '-n', 'SmokeApp', '-o', $out) + $case.Args
        Invoke-Checked "dotnet new $id" { dotnet new @newArgs }
        $buildArgs = @($out, '-c', 'Release')
        if ($case.RoutePack) { $buildArgs += '-p:PackRouteOnBuild=true', '-p:Version=0.0.1' }
        Invoke-Checked "dotnet build $id" { dotnet build @buildArgs }
        if ($case.RoutePack) { Invoke-RoutePackCheck $out 'smokeapp' $case.RoutePack }

        if ($case.Web) {
            $projectDir = if ($case.WebProject -eq '.') { $out } else { Join-Path $out $case.WebProject }
            $env:ASPNETCORE_ENVIRONMENT = 'Development'
            # -NoNewWindow: without it Windows gives the site a console window of its own, which pops up on the
            # desktop of whoever runs the smoke. The output goes to the two logs either way.
            $site = Start-Process dotnet -ArgumentList @('run', '-c', 'Release', '--no-build', '--project', $projectDir) -PassThru `
                -NoNewWindow -RedirectStandardOutput (Join-Path $out 'smoke.out.log') -RedirectStandardError (Join-Path $out 'smoke.err.log')
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

        if ($case.Pack) { Invoke-PackageCheck $out 'SmokeApp.tpkg' 'SmokeApp.Module.dll' @($case.PackHas | Where-Object { $_ }) }

        Write-Host "OK   $id" -ForegroundColor Green
    }
    catch {
        Write-Host "FAIL $id : $($_.Exception.Message)" -ForegroundColor Red
        $failed += $id
    }
}

if ($ran -eq 0) { throw "No case ran: -Only $($Only -join ', ') matches no template of the cases list." }
if ($failed.Count -gt 0) { throw "Failed cases: $($failed -join ', ')" }
Write-Host "All $ran case(s) passed." -ForegroundColor Green
