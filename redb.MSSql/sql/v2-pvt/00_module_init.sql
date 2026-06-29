-- =====================================================================
-- v2-pvt module init (MSSql)
-- =====================================================================
-- Purpose: PVT-based search engine for REDB free (SQL Server).
-- Owner  : redb core team. Mirrors redb.Postgres/sql/v2-pvt/.
-- Version: see dbo.pvt_module_version() at the bottom of this file.
--
-- This file must be applied FIRST. It performs three things:
--   1. Verifies that system infrastructure of REDB is in place
--      (core tables; dbo.get_object_json is now module-owned, see step 4).
--   2. Drops every function this module owns so the module can be
--      redeployed cleanly.
--   3. Creates dbo.pvt_module_version() -- used by the C# client to
--      verify compatibility on InitializeAsync(). No runtime fallback.
-- =====================================================================
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

-- ---------- 1. System infrastructure check ------------------------------
-- NOTE: dbo.get_object_json (and its helpers) is now OWNED by this module
-- (defined in 09_core_object_json.sql), so it is no longer guarded as an
-- external prerequisite — it is (re)created later in the same bundle. This
-- lets its bug fixes ride the versioned auto-redeploy.
IF OBJECT_ID(N'dbo._objects', N'U') IS NULL
    THROW 50000, N'v2-pvt: required table dbo._objects is missing.', 1;
IF OBJECT_ID(N'dbo._values', N'U') IS NULL
    THROW 50000, N'v2-pvt: required table dbo._values is missing.', 1;
IF OBJECT_ID(N'dbo._structures', N'U') IS NULL
    THROW 50000, N'v2-pvt: required table dbo._structures is missing.', 1;
IF OBJECT_ID(N'dbo._list_items', N'U') IS NULL
    THROW 50000, N'v2-pvt: required table dbo._list_items is missing.', 1;
IF OBJECT_ID(N'dbo._scheme_metadata_cache', N'U') IS NULL
    THROW 50000,
        N'v2-pvt: required cache table dbo._scheme_metadata_cache is missing. Deploy redb_metadata_cache.sql first.',
        1;
GO

-- ---------- 2. DROP every pvt_* object this module owns -----------------
-- Universal drop: enumerate all functions / procedures / types in the dbo
-- schema whose name starts with `pvt_` and drop them. Protects the module
-- against signature drift between releases.
DECLARE @drop_sql nvarchar(max) = N'';

-- Scalar / inline / table-valued functions and procedures.
SELECT @drop_sql = @drop_sql
    + N'DROP ' +
        CASE o.[type]
            WHEN 'P'  THEN N'PROCEDURE '
            WHEN 'FN' THEN N'FUNCTION '
            WHEN 'IF' THEN N'FUNCTION '
            WHEN 'TF' THEN N'FUNCTION '
        END
    + QUOTENAME(SCHEMA_NAME(o.[schema_id])) + N'.' + QUOTENAME(o.name) + N';' + CHAR(13) + CHAR(10)
FROM sys.objects o
WHERE o.[type] IN ('P','FN','IF','TF')
  AND SCHEMA_NAME(o.[schema_id]) = N'dbo'
  AND o.name LIKE N'pvt[_]%';

-- User-defined types (TT = table type, others handled as needed).
SELECT @drop_sql = @drop_sql
    + N'DROP TYPE '
    + QUOTENAME(SCHEMA_NAME(t.[schema_id])) + N'.' + QUOTENAME(t.name) + N';' + CHAR(13) + CHAR(10)
FROM sys.types t
WHERE t.is_user_defined = 1
  AND SCHEMA_NAME(t.[schema_id]) = N'dbo'
  AND t.name LIKE N'pvt[_]%';

IF LEN(@drop_sql) > 0
    EXEC sp_executesql @drop_sql;
GO

-- ---------- 3. Module version function ---------------------------------
-- semver: bump MAJOR on breaking changes to entry-point signatures or
-- result shape; bump MINOR on additive features; bump PATCH on bug fixes.
CREATE FUNCTION dbo.pvt_module_version()
RETURNS nvarchar(50)
WITH SCHEMABINDING
AS
BEGIN
    -- 0.1.2 - 13_pvt_condition.sql: pvt_build_field_condition dict_key
    --         branch now short-circuits to `_pvt_cte.[FieldName] OP val`
    --         when called in a post-pivot context (@base_prefix = 'o.'
    --         or '_pvt_cte.'). The nested-dict CTE already materializes
    --         the pivot column; emitting an independent EXISTS over
    --         dbo._values for the outer WHERE duplicated the lookup
    --         work. Symmetric with PG's pvt_build_field_condition.
    -- 0.1.1 - narrow-with-nested CTE shape + stable ORDER BY +
    --         Pro-parity nested-dict pushdown:
    --         * 12_pvt_cte_builder.sql: each LEFT JOIN nested derived
    --           table now folds `_id_scheme = X` (+ extra_where +
    --           tree_filter) into a dp._id_object IN (SELECT _id FROM
    --           _objects ...) subquery — mirrors PG PRO.
    --         * narrow-with-nested body skips INNER JOIN _values v
    --           when @sids = N'' (nested-only): no scalar pivot
    --           sids, no point in expanding+collapsing _values.
    --         * 20_pvt_build_query_sql.sql: narrow eligibility now
    --           allows nested groups; default ORDER BY @base_prefix
    --           + [_id] when paging is present without ORDER BY.
    -- 0.1.3 - fix: DISTINCT (@distinct=1) outer ORDER BY referenced the inner
    --         alias prefix (o./_pvt_cte.) outside the `_dist` wrapper -> "multi-part
    --         identifier 'o._id' could not be bound" with .Distinct().Take(). Outer
    --         order now uses the projected [_id] (@order_sql_dist) in all 3 branches.
    -- 0.1.4 - Soft-delete read-path fix + object-json materializer ownership:
    --         * The whole object->JSON materializer (dbo.get_object_json plus
    --           helpers build_properties / build_field_json / build_listitem_json
    --           / escape_json_string) moved from core (redb_json_objects.sql,
    --           now deleted) into the module (09_core_object_json.sql) so its
    --           fixes auto-redeploy to existing databases via the version check
    --           (full redb_init.sql is not re-run once _schemes exists).
    --         * dbo.get_object_json now treats soft-deleted objects
    --           (_id_scheme = -10, @@__deleted) as non-existent: a nested
    --           _Object reference to a trashed object resolves to NULL instead
    --           of materializing the tombstone. The _values pointer stays
    --           intact, so soft-delete remains reversible.
    -- 0.1.0 - skeleton: module bootstrap, drop-all, version function.
    --         Builder functions (pvt_build_query_sql etc.) not implemented yet.
    RETURN N'0.1.4';
END;
GO

-- ---------- 4. Smoke -----------------------------------------------------
DECLARE @v nvarchar(50) = dbo.pvt_module_version();
PRINT N'v2-pvt module init OK, version: ' + @v;
GO
