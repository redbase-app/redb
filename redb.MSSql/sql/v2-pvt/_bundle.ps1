# =====================================================================
# _bundle.ps1 (MSSql)
# ---------------------------------------------------------------------
# Concatenates all *.sql from this v2-pvt folder (by numeric prefix:
# 00, 01, ..., 20, 99) into a single file pvt_bundle.sql. Convenient for
# standalone deploy without rebuilding the C# package:
#
#     sqlcmd -S <host>,<port> -U <user> -P <pwd> -d <db> -b -i pvt_bundle.sql
#
# Ignores:
#   - subfolders (deprecated/);
#   - this script and any *.ps1;
#   - 99_smoke_tests.sql / 99_smoke_auto.sql (use -IncludeSmoke to add);
#   - pvt_bundle.sql itself (overwritten if exists).
#
# Usage:
#   ./_bundle.ps1
#   ./_bundle.ps1 -IncludeSmoke
#   ./_bundle.ps1 -OutFile pvt_full.sql -IncludeSmoke
# =====================================================================

param(
    [string]$OutFile = 'pvt_bundle.sql',
    [switch]$IncludeSmoke
)

$ErrorActionPreference = 'Stop'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $here

$files = Get-ChildItem -Path $here -Filter '*.sql' -File `
    | Where-Object { $_.Name -ne $OutFile } `
    | Where-Object { $IncludeSmoke -or ($_.Name -ne '99_smoke_tests.sql' -and $_.Name -ne '99_smoke_auto.sql') } `
    | Sort-Object Name

if (-not $files) {
    throw "No *.sql files to bundle in $here"
}

$outPath = Join-Path $here $OutFile
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)

$header = @"
-- =====================================================================
-- pvt_bundle.sql (MSSql) -- AUTO-GENERATED, DO NOT EDIT BY HAND
-- ---------------------------------------------------------------------
-- Generated:    $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss zzz')
-- Generator:    redb.MSSql/sql/v2-pvt/_bundle.ps1
-- Source files: $($files.Count) (alphabetical by numeric prefix)
-- Include smoke tests: $IncludeSmoke
--
-- To re-generate, run from this folder:
--     pwsh ./_bundle.ps1                # without smoke
--     pwsh ./_bundle.ps1 -IncludeSmoke  # with smoke tests
--
-- To deploy:
--     sqlcmd -S <host>,<port> -U <user> -P <pwd> -d <db> -b -i $OutFile
-- =====================================================================

"@

$sb = [System.Text.StringBuilder]::new()
[void]$sb.Append($header)

foreach ($f in $files) {
    [void]$sb.AppendLine()
    [void]$sb.AppendLine('-- =====================================================================')
    [void]$sb.AppendLine("-- >>> BEGIN FILE: $($f.Name)")
    [void]$sb.AppendLine('-- =====================================================================')
    $content = [System.IO.File]::ReadAllText($f.FullName, $utf8NoBom)
    [void]$sb.Append($content)
    if (-not $content.EndsWith("`n")) { [void]$sb.AppendLine() }
    [void]$sb.AppendLine('-- =====================================================================')
    [void]$sb.AppendLine("-- <<< END FILE: $($f.Name)")
    [void]$sb.AppendLine('-- =====================================================================')
}

[System.IO.File]::WriteAllText($outPath, $sb.ToString(), $utf8NoBom)

Write-Host "Bundled $($files.Count) file(s) into:" -ForegroundColor Green
Write-Host "  $outPath" -ForegroundColor Green
Write-Host ""
Write-Host "Files included:" -ForegroundColor Cyan
$files | ForEach-Object { Write-Host ("  " + $_.Name) }
