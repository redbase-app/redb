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

-- Index cleanup (0.2.29, 2026-10-14): indexes another index of the same table fully replaces, the
-- same set PostgreSQL dropped in its module 0.7.13. They were removed from redbMSSQL.sql, but that
-- file only runs on a new database, so an existing one keeps them until they are dropped here.
--   IX__values__object_structure_array_index - same key as IX__values__object_structure_lookup,
--       which has no filter and carries the values in INCLUDE.
--   ix__objects_parent_scheme - same key and filter as IX__objects__parent_id_descendant_lookup.
--   IX__structures__not_collection - a prefix of IX__structures__not_collection_enhanced, same filter.
--   the rest - a single column that is the leading column of a UNIQUE constraint.
-- DROP INDEX takes a short exclusive lock on its table.
-- IF QUERIES GOT SLOWER AFTER THIS UPGRADE, the statements that bring them back are below. This
-- block drops them again whenever the module version changes, so report the case.
--   CREATE INDEX [IX__values__object_structure_array_index] ON [dbo].[_values]([_id_object], [_id_structure], [_array_index]) WHERE [_array_index] IS NOT NULL;
--   CREATE INDEX [ix__objects_parent_scheme] ON [dbo].[_objects]([_id_parent], [_id_scheme]) WHERE [_id_parent] IS NOT NULL;
--   CREATE INDEX [IX__structures__not_collection] ON [dbo].[_structures]([_id], [_name], [_id_scheme]) WHERE [_collection_type] IS NULL;
--   CREATE INDEX [IX__functions__schemes] ON [dbo].[_functions]([_id_scheme]);
--   CREATE INDEX [IX__permissions__roles] ON [dbo].[_permissions]([_id_role]);
--   CREATE INDEX [IX__dependencies__schemes_1] ON [dbo].[_dependencies]([_id_scheme_1]);
--   CREATE INDEX [IX__users_roles__roles] ON [dbo].[_users_roles]([_id_role]);
-- IX__values__structure_object_lookup is NOT dropped here, unlike PostgreSQL: the streams form reads
-- string fields through it.
IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._values') AND name = N'IX__values__object_structure_array_index')
    DROP INDEX [IX__values__object_structure_array_index] ON [dbo].[_values];
IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._objects') AND name = N'ix__objects_parent_scheme')
    DROP INDEX [ix__objects_parent_scheme] ON [dbo].[_objects];
IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._structures') AND name = N'IX__structures__not_collection')
    DROP INDEX [IX__structures__not_collection] ON [dbo].[_structures];
IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._functions') AND name = N'IX__functions__schemes')
    DROP INDEX [IX__functions__schemes] ON [dbo].[_functions];
IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._permissions') AND name = N'IX__permissions__roles')
    DROP INDEX [IX__permissions__roles] ON [dbo].[_permissions];
IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._dependencies') AND name = N'IX__dependencies__schemes_1')
    DROP INDEX [IX__dependencies__schemes_1] ON [dbo].[_dependencies];
IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._users_roles') AND name = N'IX__users_roles__roles')
    DROP INDEX [IX__users_roles__roles] ON [dbo].[_users_roles];
GO

-- Value-leading indexes (0.2.30, 2026-10-16), the set PostgreSQL got in 0.7.16: a filter on a field finds its
-- objects by value. They replace eleven object-leading forms (_id_structure, _id_object, <value>), three of them
-- duplicates of three others. The string has none: NVARCHAR(MAX) cannot be a key. Built before the old ones are
-- dropped. Without ONLINE = ON (Enterprise only) writes to _values wait while they are built.
-- An index of one of these names whose definition differs in anything (key columns or their order, INCLUDE, filter,
-- uniqueness, disabled) is dropped and built again: IF NOT EXISTS alone would keep it. A research build left such
-- indexes with _array_index in the filter, which a query that does not state _array_index IS NULL cannot use.
-- IF QUERIES GOT SLOWER AFTER THIS UPGRADE, these bring the old ones back; this block drops them again on the next
-- version change, so report the case:
--   CREATE INDEX [IX__values__Long_not_null] ON [dbo].[_values]([_id_structure], [_id_object], [_Long]) WHERE [_Long] IS NOT NULL;
--   CREATE INDEX [IX__values__DateTimeOffset_not_null] ON [dbo].[_values]([_id_structure], [_id_object], [_DateTimeOffset]) WHERE [_DateTimeOffset] IS NOT NULL;
--   CREATE INDEX [IX__values__Numeric_not_null] ON [dbo].[_values]([_id_structure], [_id_object], [_Numeric]) WHERE [_Numeric] IS NOT NULL;
--   CREATE INDEX [IX__values__Long_filter] ON [dbo].[_values]([_id_structure], [_id_object], [_Long]) WHERE [_Long] IS NOT NULL;
--   CREATE INDEX [IX__values__Guid_filter] ON [dbo].[_values]([_id_structure], [_id_object], [_Guid]) WHERE [_Guid] IS NOT NULL;
--   CREATE INDEX [IX__values__Double_filter] ON [dbo].[_values]([_id_structure], [_id_object], [_Double]) WHERE [_Double] IS NOT NULL;
--   CREATE INDEX [IX__values__DateTimeOffset_filter] ON [dbo].[_values]([_id_structure], [_id_object], [_DateTimeOffset]) WHERE [_DateTimeOffset] IS NOT NULL;
--   CREATE INDEX [IX__values__Boolean_filter] ON [dbo].[_values]([_id_structure], [_id_object], [_Boolean]) WHERE [_Boolean] IS NOT NULL;
--   CREATE INDEX [IX__values__Numeric_filter] ON [dbo].[_values]([_id_structure], [_id_object], [_Numeric]) WHERE [_Numeric] IS NOT NULL;
--   CREATE INDEX [IX__values__ListItem_filter] ON [dbo].[_values]([_id_structure], [_id_object], [_ListItem]) WHERE [_ListItem] IS NOT NULL;
--   CREATE INDEX [IX__values__Object_filter] ON [dbo].[_values]([_id_structure], [_id_object], [_Object]) WHERE [_Object] IS NOT NULL;
DECLARE @drop NVARCHAR(MAX);
SELECT @drop = STRING_AGG(CAST(N'DROP INDEX ' + QUOTENAME(i.name) + N' ON [dbo].[_values];' AS NVARCHAR(MAX)), N' ')
FROM (VALUES
    (N'IX__values__structure_long_object', N'_Long'),
    (N'IX__values__structure_numeric_object', N'_Numeric'),
    (N'IX__values__structure_double_object', N'_Double'),
    (N'IX__values__structure_datetimeoffset_object', N'_DateTimeOffset'),
    (N'IX__values__structure_boolean_object', N'_Boolean'),
    (N'IX__values__structure_guid_object', N'_Guid'),
    (N'IX__values__structure_listitem_object', N'_ListItem'),
    (N'IX__values__structure_object_object', N'_Object')
) x(name, col)
JOIN sys.indexes i ON i.object_id = OBJECT_ID(N'dbo._values') AND i.name = x.name
WHERE i.is_disabled = 1 OR i.is_unique = 1 OR i.type <> 2
   OR ISNULL(i.filter_definition, N'') <> N'([' + x.col + N'] IS NOT NULL)'
   OR ISNULL((SELECT STRING_AGG(c.name + CASE WHEN ic.is_descending_key = 1 THEN N' DESC' ELSE N'' END, N',') WITHIN GROUP (ORDER BY ic.key_ordinal)
              FROM sys.index_columns ic JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
              WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 0), N'')
      <> N'_id_structure,' + x.col + N',_id_object'
   OR ISNULL((SELECT STRING_AGG(c.name, N',')
              FROM sys.index_columns ic JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
              WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 1), N'') <> N'_array_index';
-- Only a role that may alter _values rebuilds an index. A role without the right (the DBA took owner rights back
-- after the installation) may not see the whole definition of an index, takes a correct one for a wrong one, and
-- its DROP fails with 1088 ("does not exist or you do not have permissions"), which is no privilege error to the
-- caller: the start-up died with a driver error instead of RedbSchemaOutdatedException. Without the right the
-- check is skipped, and the first DDL below reports the missing right as before.
IF @drop IS NOT NULL AND HAS_PERMS_BY_NAME(N'dbo._values', N'OBJECT', N'ALTER') = 1
    EXEC sp_executesql @drop;
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._values') AND name = N'IX__values__structure_long_object')
    CREATE INDEX [IX__values__structure_long_object] ON [dbo].[_values]([_id_structure], [_Long], [_id_object]) INCLUDE ([_array_index]) WHERE [_Long] IS NOT NULL;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._values') AND name = N'IX__values__structure_numeric_object')
    CREATE INDEX [IX__values__structure_numeric_object] ON [dbo].[_values]([_id_structure], [_Numeric], [_id_object]) INCLUDE ([_array_index]) WHERE [_Numeric] IS NOT NULL;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._values') AND name = N'IX__values__structure_double_object')
    CREATE INDEX [IX__values__structure_double_object] ON [dbo].[_values]([_id_structure], [_Double], [_id_object]) INCLUDE ([_array_index]) WHERE [_Double] IS NOT NULL;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._values') AND name = N'IX__values__structure_datetimeoffset_object')
    CREATE INDEX [IX__values__structure_datetimeoffset_object] ON [dbo].[_values]([_id_structure], [_DateTimeOffset], [_id_object]) INCLUDE ([_array_index]) WHERE [_DateTimeOffset] IS NOT NULL;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._values') AND name = N'IX__values__structure_boolean_object')
    CREATE INDEX [IX__values__structure_boolean_object] ON [dbo].[_values]([_id_structure], [_Boolean], [_id_object]) INCLUDE ([_array_index]) WHERE [_Boolean] IS NOT NULL;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._values') AND name = N'IX__values__structure_guid_object')
    CREATE INDEX [IX__values__structure_guid_object] ON [dbo].[_values]([_id_structure], [_Guid], [_id_object]) INCLUDE ([_array_index]) WHERE [_Guid] IS NOT NULL;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._values') AND name = N'IX__values__structure_listitem_object')
    CREATE INDEX [IX__values__structure_listitem_object] ON [dbo].[_values]([_id_structure], [_ListItem], [_id_object]) INCLUDE ([_array_index]) WHERE [_ListItem] IS NOT NULL;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._values') AND name = N'IX__values__structure_object_object')
    CREATE INDEX [IX__values__structure_object_object] ON [dbo].[_values]([_id_structure], [_Object], [_id_object]) INCLUDE ([_array_index]) WHERE [_Object] IS NOT NULL;
GO
IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._values') AND name = N'IX__values__Long_not_null')
    DROP INDEX [IX__values__Long_not_null] ON [dbo].[_values];
IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._values') AND name = N'IX__values__DateTimeOffset_not_null')
    DROP INDEX [IX__values__DateTimeOffset_not_null] ON [dbo].[_values];
IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._values') AND name = N'IX__values__Numeric_not_null')
    DROP INDEX [IX__values__Numeric_not_null] ON [dbo].[_values];
IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._values') AND name = N'IX__values__Long_filter')
    DROP INDEX [IX__values__Long_filter] ON [dbo].[_values];
IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._values') AND name = N'IX__values__Guid_filter')
    DROP INDEX [IX__values__Guid_filter] ON [dbo].[_values];
IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._values') AND name = N'IX__values__Double_filter')
    DROP INDEX [IX__values__Double_filter] ON [dbo].[_values];
IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._values') AND name = N'IX__values__DateTimeOffset_filter')
    DROP INDEX [IX__values__DateTimeOffset_filter] ON [dbo].[_values];
IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._values') AND name = N'IX__values__Boolean_filter')
    DROP INDEX [IX__values__Boolean_filter] ON [dbo].[_values];
IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._values') AND name = N'IX__values__Numeric_filter')
    DROP INDEX [IX__values__Numeric_filter] ON [dbo].[_values];
IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._values') AND name = N'IX__values__ListItem_filter')
    DROP INDEX [IX__values__ListItem_filter] ON [dbo].[_values];
IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._values') AND name = N'IX__values__Object_filter')
    DROP INDEX [IX__values__Object_filter] ON [dbo].[_values];
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

