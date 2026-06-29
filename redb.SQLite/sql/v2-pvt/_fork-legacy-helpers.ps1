# =====================================================================
# _fork-legacy-helpers.ps1
# ---------------------------------------------------------------------
# One-shot script that forks 8 helper functions from legacy SQL into
# the v2-pvt module under the pvt_* prefix.
#
# Usage (from repo root):
#   pwsh redb.Postgres/sql/v2-pvt/_fork-legacy-helpers.ps1
#
# Idempotent: overwrites target files every run.
# Does NOT touch legacy SQL files.
# =====================================================================
[CmdletBinding()]
param(
    [string]$RepoRoot = (Resolve-Path "$PSScriptRoot/../../..").Path,
    [string]$ForkDate = '2026-05-18'
)

$ErrorActionPreference = 'Stop'

$facets = Join-Path $RepoRoot 'redb.Postgres/sql/redb_facets_search.sql'
$lazy   = Join-Path $RepoRoot 'redb.Postgres/sql/redb_lazy_loading_search.sql'
$outDir = Join-Path $RepoRoot 'redb.Postgres/sql/v2-pvt'

if (-not (Test-Path $facets)) { throw "Source not found: $facets" }
if (-not (Test-Path $lazy))   { throw "Source not found: $lazy" }

$facetsLines = Get-Content -LiteralPath $facets
$lazyLines   = Get-Content -LiteralPath $lazy

# Cross-reference replacements applied to every forked function body.
# Order matters only if patterns overlap — these do not.
$nameMap = @(
    @{ From = '_normalize_base_field_name';   To = 'pvt_normalize_base_field_name' }
    @{ From = '_parse_field_path';            To = 'pvt_parse_field_path' }
    @{ From = '_get_listitem_field_type_info';To = 'pvt_get_listitem_field_type_info' }
    @{ From = '_find_structure_info';         To = 'pvt_find_structure_info' }
    @{ From = '_build_inner_condition';       To = 'pvt_build_inner_condition' }
    @{ From = '_build_single_facet_condition';To = 'pvt_build_single_facet_condition' }
    @{ From = 'build_hierarchical_conditions';To = 'pvt_build_hierarchical_conditions' }
    @{ From = 'get_object_base_fields';       To = 'pvt_get_object_base_fields' }
)

# Note on order: `_get_listitem_field_type_info` shares no overlapping
# substring with other From values; `build_hierarchical_conditions` is
# matched without the leading underscore by design (legacy uses no
# underscore prefix for it). To avoid partial matches we apply with
# word-boundary regex below.

function Apply-NameMap {
    param([string[]]$Lines)
    $text = ($Lines -join "`n")
    foreach ($m in $nameMap) {
        # Word-boundary replacement; \b matches start/end of identifier.
        $pattern = '(?<![A-Za-z0-9_])' + [regex]::Escape($m.From) + '(?![A-Za-z0-9_])'
        $text = [regex]::Replace($text, $pattern, $m.To)
    }
    return $text
}

# Extracts function block [startLine .. endLineOfClosingDollar], plus
# an optional COMMENT ON FUNCTION line that follows.
function Extract-Block {
    param(
        [string[]]$Lines,
        [int]$StartLine1Based
    )
    $idx = $StartLine1Based - 1
    $end = -1
    # Plpgsql functions in this codebase end with '$BODY$;' or '$$;'.
    for ($i = $idx; $i -lt $Lines.Length; $i++) {
        $trim = $Lines[$i].Trim()
        if ($trim -eq '$BODY$;' -or $trim -eq '$$;') {
            $end = $i
            break
        }
    }
    if ($end -lt 0) { throw "End-of-function marker not found starting at line $StartLine1Based" }

    # Include following COMMENT ON FUNCTION (if any), skipping blank lines.
    $j = $end + 1
    while ($j -lt $Lines.Length -and [string]::IsNullOrWhiteSpace($Lines[$j])) { $j++ }
    if ($j -lt $Lines.Length -and $Lines[$j] -match '^\s*COMMENT ON FUNCTION') {
        # The COMMENT statement may wrap, ending in ';' on its own or same line.
        while ($j -lt $Lines.Length) {
            if ($Lines[$j].TrimEnd().EndsWith(';')) {
                $end = $j
                break
            }
            $j++
        }
    }

    return $Lines[$idx..$end]
}

function Build-Header {
    param([string]$SourceFile, [int]$SourceLine, [string]$Purpose)
    @"
-- =====================================================================
-- $Purpose
-- ---------------------------------------------------------------------
-- Forked from $SourceFile L$SourceLine on $ForkDate.
-- Legacy version stays untouched. Mirror legacy bug fixes here.
-- =====================================================================

"@
}

function Write-Forked {
    param(
        [string]$OutFile,
        [hashtable[]]$Blocks
    )
    $sb = New-Object System.Text.StringBuilder
    foreach ($b in $Blocks) {
        $null = $sb.AppendLine((Build-Header -SourceFile $b.SourceFile -SourceLine $b.SourceLine -Purpose $b.Purpose))
        $raw = Extract-Block -Lines $b.Lines -StartLine1Based $b.SourceLine
        $renamed = Apply-NameMap -Lines $raw
        $null = $sb.AppendLine($renamed)
        $null = $sb.AppendLine()
    }
    $full = Join-Path $outDir $OutFile
    [System.IO.File]::WriteAllText($full, $sb.ToString(), [System.Text.UTF8Encoding]::new($false))
    Write-Host "  wrote $OutFile" -ForegroundColor Green
}

Write-Host "Forking legacy helpers into v2-pvt/" -ForegroundColor Cyan

# 01_pvt_field_path.sql — two helpers
Write-Forked -OutFile '01_pvt_field_path.sql' -Blocks @(
    @{ Lines = $facetsLines; SourceFile = 'redb_facets_search.sql'; SourceLine = 51;  Purpose = 'pvt_normalize_base_field_name: map C# base field names to _objects columns' }
    @{ Lines = $facetsLines; SourceFile = 'redb_facets_search.sql'; SourceLine = 185; Purpose = 'pvt_parse_field_path: split dotted/bracketed field paths into components' }
)

# 02_pvt_type_info.sql
Write-Forked -OutFile '02_pvt_type_info.sql' -Blocks @(
    @{ Lines = $facetsLines; SourceFile = 'redb_facets_search.sql'; SourceLine = 268; Purpose = 'pvt_get_listitem_field_type_info: resolve type info for ListItem-typed fields' }
)

# 03_pvt_structure_info.sql
Write-Forked -OutFile '03_pvt_structure_info.sql' -Blocks @(
    @{ Lines = $facetsLines; SourceFile = 'redb_facets_search.sql'; SourceLine = 291; Purpose = 'pvt_find_structure_info: look up structure metadata by field path' }
)

# 04_pvt_inner_condition.sql
Write-Forked -OutFile '04_pvt_inner_condition.sql' -Blocks @(
    @{ Lines = $facetsLines; SourceFile = 'redb_facets_search.sql'; SourceLine = 362; Purpose = 'pvt_build_inner_condition: build SQL operator/value fragment for a typed value column' }
)

# 05_pvt_single_facet.sql
Write-Forked -OutFile '05_pvt_single_facet.sql' -Blocks @(
    @{ Lines = $facetsLines; SourceFile = 'redb_facets_search.sql'; SourceLine = 1311; Purpose = 'pvt_build_single_facet_condition: build a single field facet WHERE fragment (legacy EXISTS engine; used as fallback for complex ops in PVT)' }
)

# 06_pvt_hierarchical.sql
Write-Forked -OutFile '06_pvt_hierarchical.sql' -Blocks @(
    @{ Lines = $facetsLines; SourceFile = 'redb_facets_search.sql'; SourceLine = 2818; Purpose = 'pvt_build_hierarchical_conditions: build tree predicates ($hasAncestor, $hasDescendant, $level, ...)' }
)

# 07_pvt_base_fields.sql
Write-Forked -OutFile '07_pvt_base_fields.sql' -Blocks @(
    @{ Lines = $lazyLines; SourceFile = 'redb_lazy_loading_search.sql'; SourceLine = 17; Purpose = 'pvt_get_object_base_fields: return JSONB with all base fields of a single object (no Props)' }
)

# Audit: ensure no unprefixed legacy names slipped through into pvt files.
Write-Host "Auditing fork output for unprefixed legacy references..." -ForegroundColor Cyan
$pattern = '(?<![A-Za-z0-9_])(' + ($nameMap.From -join '|') + ')(?![A-Za-z0-9_])'
$bad = Get-ChildItem $outDir -Filter '0[1-7]_pvt_*.sql' |
    ForEach-Object {
        $f = $_
        Select-String -LiteralPath $f.FullName -Pattern $pattern -AllMatches
    }
if ($bad) {
    Write-Host "AUDIT FAILED — unprefixed legacy references remain:" -ForegroundColor Red
    $bad | ForEach-Object { Write-Host ("  {0}:{1}: {2}" -f $_.Path, $_.LineNumber, $_.Line) -ForegroundColor Red }
    exit 1
}

Write-Host "Audit OK. Fork complete." -ForegroundColor Green
