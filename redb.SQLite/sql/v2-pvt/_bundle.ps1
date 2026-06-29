# =====================================================================
# _bundle.ps1
# ---------------------------------------------------------------------
# Конкатенирует все *.sql из текущей папки v2-pvt (по числовому префиксу:
# 00, 01, ..., 20, 99) в один файл pvt_bundle.sql. Удобно для деплоя:
#     psql -h <host> -U <user> -d <db> -v ON_ERROR_STOP=1 -f pvt_bundle.sql
#
# Игнорирует:
#   - подпапки (deprecated/);
#   - сам этот скрипт и любые *.ps1;
#   - 99_smoke_tests.sql (по умолчанию НЕ включается; чтобы включить — флаг -IncludeSmoke);
#   - pvt_bundle.sql (если уже лежит рядом — перезаписывается).
#
# Использование:
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

$files = Get-ChildItem -Path $here -Filter '*.sql' -File
    | Where-Object { $_.Name -ne $OutFile }
    | Where-Object { $IncludeSmoke -or ($_.Name -ne '99_smoke_tests.sql' -and $_.Name -ne '99_smoke_auto.sql') }
    | Sort-Object Name

if (-not $files) {
    throw "No *.sql files to bundle in $here"
}

$outPath = Join-Path $here $OutFile
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)

$header = @"
-- =====================================================================
-- pvt_bundle.sql — AUTO-GENERATED, DO NOT EDIT BY HAND
-- ---------------------------------------------------------------------
-- Generated:    $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss zzz')
-- Generator:    redb.Postgres/sql/v2-pvt/_bundle.ps1
-- Source files: $($files.Count) (alphabetical by numeric prefix)
-- Include smoke tests: $IncludeSmoke
--
-- To re-generate, run from this folder:
--     pwsh ./_bundle.ps1                # без smoke
--     pwsh ./_bundle.ps1 -IncludeSmoke  # вместе со смоук-тестами
--
-- To deploy:
--     psql -h <host> -U <user> -d <db> -v ON_ERROR_STOP=1 -f $OutFile
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
