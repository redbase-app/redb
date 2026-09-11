-- =====================================================================
-- v2-pvt module init (MSSql)
-- =====================================================================
-- Purpose: PVT-based search engine for REDB free (SQL Server).
-- Owner  : redb core team. Mirrors redb.Postgres/sql/v2-pvt/.
-- Version: see dbo.pvt_module_version() in 99_module_version.sql (applied LAST).
--
-- This file must be applied FIRST. It performs two things:
--   1. Verifies that system infrastructure of REDB is in place
--      (core tables; dbo.get_object_json is module-owned, 09_core_object_json.sql).
--   2. Drops every function this module owns so the module can be
--      redeployed cleanly -- dbo.pvt_module_version included, which is why
--      the version is recreated LAST, in 99_module_version.sql.
-- =====================================================================
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

-- ---------- 0. Schema upgrades (idempotent, run on every redeploy) ----------
-- DDL for columns added after a database was created. The base DDL in
-- redbMSSQL.sql stays the source of truth for fresh databases; this block is
-- the delivery to existing ones (SCHEMA_DELIVERY plan, V4). GO after the
-- ALTERs is mandatory: a later statement naming a new column will not
-- compile in the same batch.

-- V4 (UNIQUE stage 2): [RedbUnique] key metadata and the key hash column.
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo._structures') AND name = N'_unique')
    ALTER TABLE [dbo].[_structures] ADD [_unique] BIT NULL;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo._structures') AND name = N'_unique_version')
    ALTER TABLE [dbo].[_structures] ADD [_unique_version] BIGINT NULL;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo._values') AND name = N'_unique')
    ALTER TABLE [dbo].[_values] ADD [_unique] UNIQUEIDENTIFIER NULL;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo._scheme_metadata_cache') AND name = N'_unique')
    ALTER TABLE [dbo].[_scheme_metadata_cache] ADD [_unique] BIT NULL;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo._scheme_metadata_cache') AND name = N'_unique_version')
    ALTER TABLE [dbo].[_scheme_metadata_cache] ADD [_unique_version] BIGINT NULL;
-- V4 (LAZY Л2): the lazy-reference marker, read from `virtual` at synchronisation.
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo._structures') AND name = N'_lazy')
    ALTER TABLE [dbo].[_structures] ADD [_lazy] BIT NULL;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo._scheme_metadata_cache') AND name = N'_lazy')
    ALTER TABLE [dbo].[_scheme_metadata_cache] ADD [_lazy] BIT NULL;

-- S3 (0.2.13): element-key scope of collection keys, and the free-form _tags marker on
-- schemes, structures and the metadata cache (future / custom extensions; sync never wipes it).
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo._structures') AND name = N'_unique_scope')
    ALTER TABLE [dbo].[_structures] ADD [_unique_scope] BIGINT NULL;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo._structures') AND name = N'_tags')
    ALTER TABLE [dbo].[_structures] ADD [_tags] NVARCHAR(450) NULL;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo._schemes') AND name = N'_tags')
    ALTER TABLE [dbo].[_schemes] ADD [_tags] NVARCHAR(450) NULL;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo._scheme_metadata_cache') AND name = N'_unique_scope')
    ALTER TABLE [dbo].[_scheme_metadata_cache] ADD [_unique_scope] BIGINT NULL;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo._scheme_metadata_cache') AND name = N'_tags')
    ALTER TABLE [dbo].[_scheme_metadata_cache] ADD [_tags] NVARCHAR(450) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._structures') AND name = N'IX__structures__tags')
    CREATE INDEX [IX__structures__tags] ON [dbo].[_structures]([_tags]) WHERE [_tags] IS NOT NULL;
UPDATE c SET c.[_unique_scope] = s.[_unique_scope], c.[_tags] = s.[_tags]
    FROM [dbo].[_scheme_metadata_cache] c JOIN [dbo].[_structures] s ON s.[_id] = c.[_structure_id]
    WHERE ISNULL(c.[_unique_scope], -1) <> ISNULL(s.[_unique_scope], -1)
       OR ISNULL(c.[_tags], N'') <> ISNULL(s.[_tags], N'');
-- V4 (UNIQUE stage 1): the object key column.
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo._objects') AND name = N'_value_unique')
    ALTER TABLE [dbo].[_objects] ADD [_value_unique] NVARCHAR(440) NULL;
GO
-- V4 (LAZY L2): repair caches synced between the column upgrade and the marker write.
-- Below the GO on purpose: inside the ALTER batch the new column does not compile.
UPDATE c SET c.[_lazy] = s.[_lazy]
    FROM [dbo].[_scheme_metadata_cache] c JOIN [dbo].[_structures] s ON s.[_id] = c.[_structure_id]
    WHERE ISNULL(c.[_lazy], 0) <> ISNULL(s.[_lazy], 0);
GO
-- S2 (subtree-unique plan, 0.2.12): the key index covers keyed rows at ANY position now -
-- nested scalar keys carry _array_parent_id, and the old positional filter kept them OUT of
-- the index, so their "uniqueness" silently never fired. Recreate on the old predicate.
IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._values')
           AND name = N'UIX__values__structure_unique'
           AND filter_definition LIKE N'%[_]array[_]parent[_]id%')
    DROP INDEX [UIX__values__structure_unique] ON [dbo].[_values];
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._values') AND name = N'UIX__values__structure_unique')
    CREATE UNIQUE INDEX [UIX__values__structure_unique] ON [dbo].[_values]([_id_structure], [_unique])
        INCLUDE ([_id_object])
        WHERE [_unique] IS NOT NULL;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._objects') AND name = N'UIX__objects__scheme_unique')
    CREATE UNIQUE INDEX [UIX__objects__scheme_unique] ON [dbo].[_objects]([_id_scheme], [_value_unique])
        INCLUDE ([_id])
        WHERE [_value_unique] IS NOT NULL;
GO

-- FK-column indexes of _values (perf, 2026-09-10): _Object/_ListItem carry foreign keys, and
-- without a leading index every DELETE of a referenced object or list item scans the whole
-- table for the FK check. Filtered form keeps them nearly empty. NOTE: filtered indexes
-- require QUOTED_IDENTIFIER ON in the creating session (ADO.NET default; sqlcmd needs -I).
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._values') AND name = N'IX__values__ListItem_not_null')
    CREATE INDEX [IX__values__ListItem_not_null] ON [dbo].[_values]([_ListItem]) WHERE [_ListItem] IS NOT NULL;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._values') AND name = N'IX__values__Object_not_null')
    CREATE INDEX [IX__values__Object_not_null] ON [dbo].[_values]([_Object]) WHERE [_Object] IS NOT NULL;
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

-- =====================================================
-- V4 (owner decision 2026-09-02): _objects._value_string is an identifier column - NVARCHAR(450),
-- the index-key width _name already uses; long text belongs in _note or in a Props field.
-- NVARCHAR(MAX) cannot be an index key at all (Msg 1919), so every filter on the column scanned.
-- A database holding longer values is REFUSED, loudly: silent truncation is not an option.
-- =====================================================
IF EXISTS (SELECT 1 FROM sys.columns
            WHERE object_id = OBJECT_ID(N'dbo._objects') AND name = N'_value_string' AND max_length = -1)
BEGIN
    IF EXISTS (SELECT 1 FROM [dbo].[_objects] WHERE LEN([_value_string]) > 450)
        RAISERROR('redb upgrade: _objects._value_string is being narrowed to NVARCHAR(450) - it is an identifier column, long text belongs in _note or in a Props field. This database holds longer values; move them first (offenders: SELECT _id, LEN(_value_string) FROM _objects WHERE LEN(_value_string) > 450), then start again. Nothing was changed.', 16, 1);
    ELSE
        ALTER TABLE [dbo].[_objects] ALTER COLUMN [_value_string] NVARCHAR(450) NULL;
END
GO

IF COL_LENGTH('dbo._objects', '_value_string') = 900
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._objects') AND name = N'IX__objects__value_string')
    CREATE INDEX [IX__objects__value_string] ON [dbo].[_objects]([_value_string]) WHERE [_value_string] IS NOT NULL
GO

-- =====================================================
-- Owner decision 2026-09-02: the full-text index on _values._String is dropped. It served no
-- reader (redb translates every string predicate to LIKE, which never uses full-text) and cost a
-- background reindex on every _values write (CHANGE_TRACKING AUTO). sys.fulltext_* exist whether
-- or not Full-Text is installed, so the guards are safe everywhere.
-- =====================================================
IF EXISTS (SELECT 1 FROM sys.fulltext_indexes WHERE object_id = OBJECT_ID(N'dbo._values'))
    DROP FULLTEXT INDEX ON [dbo].[_values]
GO
IF EXISTS (SELECT 1 FROM sys.fulltext_catalogs WHERE name = 'redb_fulltext_catalog')
    DROP FULLTEXT CATALOG [redb_fulltext_catalog]
GO

