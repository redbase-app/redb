-- ==========================================================
-- REDB: Combined schema initialization script (auto-generated)
-- DO NOT EDIT — this file is overwritten on every build.
-- ==========================================================

-- ===== redbMSSQL.sql =====
-- =====================================================
-- REDB MS SQL Server Schema
-- Version: 2.0 (with Array/Dictionary/JsonDocument support)
-- Compatible with SQL Server 2016+
-- Can be run multiple times (idempotent)
-- =====================================================


-- =====================================================
-- DROP ALL FOREIGN KEY CONSTRAINTS FIRST
-- =====================================================

DECLARE @sql NVARCHAR(MAX) = N'';

SELECT @sql += N'ALTER TABLE ' + QUOTENAME(OBJECT_SCHEMA_NAME(parent_object_id)) 
    + '.' + QUOTENAME(OBJECT_NAME(parent_object_id)) 
    + ' DROP CONSTRAINT ' + QUOTENAME(name) + ';' + CHAR(13)
FROM sys.foreign_keys
WHERE OBJECT_SCHEMA_NAME(parent_object_id) = 'dbo'
  AND OBJECT_NAME(parent_object_id) LIKE '[_]%';

IF LEN(@sql) > 0
    EXEC sp_executesql @sql;
GO

-- =====================================================
-- DROP EXISTING OBJECTS (clean install)
-- =====================================================

-- Drop stored procedures
IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'sp_GetChildObjects')
DROP PROCEDURE [dbo].[sp_GetChildObjects]
GO

-- Drop sequence
IF EXISTS (SELECT * FROM sys.sequences WHERE name = 'global_identity')
    DROP SEQUENCE [dbo].[global_identity]
GO

-- Drop tables (now safe - no FK constraints)
IF OBJECT_ID('[dbo].[_values]', 'U') IS NOT NULL DROP TABLE [dbo].[_values]
IF OBJECT_ID('[dbo].[_permissions]', 'U') IS NOT NULL DROP TABLE [dbo].[_permissions]
IF OBJECT_ID('[dbo].[_functions]', 'U') IS NOT NULL DROP TABLE [dbo].[_functions]
IF OBJECT_ID('[dbo].[_dependencies]', 'U') IS NOT NULL DROP TABLE [dbo].[_dependencies]
IF OBJECT_ID('[dbo].[_list_items]', 'U') IS NOT NULL DROP TABLE [dbo].[_list_items]
IF OBJECT_ID('[dbo].[_objects]', 'U') IS NOT NULL DROP TABLE [dbo].[_objects]
IF OBJECT_ID('[dbo].[_structures]', 'U') IS NOT NULL DROP TABLE [dbo].[_structures]
IF OBJECT_ID('[dbo].[_schemes]', 'U') IS NOT NULL DROP TABLE [dbo].[_schemes]
IF OBJECT_ID('[dbo].[_users_roles]', 'U') IS NOT NULL DROP TABLE [dbo].[_users_roles]
IF OBJECT_ID('[dbo].[_roles]', 'U') IS NOT NULL DROP TABLE [dbo].[_roles]
IF OBJECT_ID('[dbo].[_users]', 'U') IS NOT NULL DROP TABLE [dbo].[_users]
IF OBJECT_ID('[dbo].[_lists]', 'U') IS NOT NULL DROP TABLE [dbo].[_lists]
IF OBJECT_ID('[dbo].[_links]', 'U') IS NOT NULL DROP TABLE [dbo].[_links]
IF OBJECT_ID('[dbo].[_types]', 'U') IS NOT NULL DROP TABLE [dbo].[_types]
GO

-- =====================================================
-- CREATE TABLES
-- =====================================================

-- Types table (base types for structures)
CREATE TABLE [dbo].[_types](
    [_id] BIGINT NOT NULL,
    [_name] NVARCHAR(450) NOT NULL,  -- max for UNIQUE index (900 bytes)
    [_db_type] NVARCHAR(450) NULL,
    [_type] NVARCHAR(450) NULL,
    CONSTRAINT [PK__types] PRIMARY KEY CLUSTERED ([_id]),
    CONSTRAINT [IX__types_name] UNIQUE ([_name])
)
GO

-- Links table (many-to-many relations)
CREATE TABLE [dbo].[_links](
    [_id] BIGINT NOT NULL,
    [_id_1] BIGINT NOT NULL,
    [_id_2] BIGINT NOT NULL,
    CONSTRAINT [PK__links] PRIMARY KEY CLUSTERED ([_id]),
    CONSTRAINT [IX__links] UNIQUE ([_id_1], [_id_2]),
    CONSTRAINT [CK__links] CHECK ([_id_1] <> [_id_2])
)
GO

-- Lists table (reference lists/dictionaries)
CREATE TABLE [dbo].[_lists](
    [_id] BIGINT NOT NULL,
    [_name] NVARCHAR(450) NOT NULL,  -- max for UNIQUE index
    [_alias] NVARCHAR(450) NULL,
    CONSTRAINT [PK__lists] PRIMARY KEY CLUSTERED ([_id]),
    CONSTRAINT [IX__lists_name] UNIQUE ([_name])
)
GO

-- Roles table
CREATE TABLE [dbo].[_roles](
    [_id] BIGINT NOT NULL,
    [_name] NVARCHAR(450) NOT NULL,  -- max for UNIQUE index
    [_id_configuration] BIGINT NULL,
    CONSTRAINT [PK__roles] PRIMARY KEY CLUSTERED ([_id]),
    CONSTRAINT [IX__roles] UNIQUE ([_name])
)
GO

-- Users table
CREATE TABLE [dbo].[_users](
    [_id] BIGINT NOT NULL,
    [_login] NVARCHAR(450) NOT NULL,
    [_password] NVARCHAR(MAX) NOT NULL,
    [_name] NVARCHAR(450) NOT NULL,  -- max for UNIQUE index
    [_phone] NVARCHAR(450) NULL,
    [_email] NVARCHAR(450) NULL,
    [_date_register] DATETIMEOFFSET NOT NULL DEFAULT SYSDATETIMEOFFSET(),
    [_date_dismiss] DATETIMEOFFSET NULL,
    [_enabled] BIT NOT NULL DEFAULT 1,
    [_key] BIGINT NULL,
    [_code_int] BIGINT NULL,
    [_code_string] NVARCHAR(MAX) NULL,
    [_code_guid] UNIQUEIDENTIFIER NULL,
    [_note] NVARCHAR(MAX) NULL,
    [_hash] UNIQUEIDENTIFIER NULL,
    [_id_configuration] BIGINT NULL,
    CONSTRAINT [PK__users] PRIMARY KEY CLUSTERED ([_id]),
    CONSTRAINT [IX__users_name] UNIQUE ([_name])
)
GO

-- Users-Roles junction table
CREATE TABLE [dbo].[_users_roles](
    [_id] BIGINT NOT NULL,
    [_id_role] BIGINT NOT NULL,
    [_id_user] BIGINT NOT NULL,
    CONSTRAINT [PK__users_roles] PRIMARY KEY CLUSTERED ([_id]),
    CONSTRAINT [IX__users_roles] UNIQUE ([_id_role], [_id_user]),
    CONSTRAINT [FK__users_roles__roles] FOREIGN KEY ([_id_role]) REFERENCES [_roles]([_id]) ON DELETE CASCADE,
    CONSTRAINT [FK__users_roles__users] FOREIGN KEY ([_id_user]) REFERENCES [_users]([_id]) ON DELETE CASCADE
)
GO

-- Schemes table (class definitions)
CREATE TABLE [dbo].[_schemes](
    [_id] BIGINT NOT NULL,
    [_id_parent] BIGINT NULL,
    [_name] NVARCHAR(450) NOT NULL,  -- max for UNIQUE index
    [_alias] NVARCHAR(450) NULL,
    [_name_space] NVARCHAR(MAX) NULL,
    [_structure_hash] UNIQUEIDENTIFIER NULL,
    [_type] BIGINT NOT NULL DEFAULT -9223372036854775675, -- Class by default
    CONSTRAINT [PK__schemes] PRIMARY KEY CLUSTERED ([_id]),
    CONSTRAINT [IX__schemes] UNIQUE ([_name]),
    CONSTRAINT [FK__schemes__schemes] FOREIGN KEY ([_id_parent]) REFERENCES [_schemes]([_id]),
    CONSTRAINT [FK__schemes__types] FOREIGN KEY ([_type]) REFERENCES [_types]([_id])
)
GO

-- Structures table (field definitions)
CREATE TABLE [dbo].[_structures](
    [_id] BIGINT NOT NULL,
    [_id_parent] BIGINT NULL,
    [_id_scheme] BIGINT NOT NULL,
    [_id_override] BIGINT NULL,
    [_id_type] BIGINT NOT NULL,
    [_id_list] BIGINT NULL,
    [_name] NVARCHAR(440) NOT NULL,  -- max for composite UNIQUE (8+8+880 = 896 < 900 bytes)
    [_alias] NVARCHAR(450) NULL,
    [_order] BIGINT NULL,
    [_readonly] BIT NULL,
    [_allow_not_null] BIT NULL,
    [_collection_type] BIGINT NULL,  -- Array/Dictionary type ID or NULL for non-collections
    [_key_type] BIGINT NULL,         -- Key type for Dictionary fields
    [_is_compress] BIT NULL,
    [_store_null] BIT NULL,
    [_default_value] VARBINARY(MAX) NULL,
    [_default_editor] NVARCHAR(MAX) NULL,
    CONSTRAINT [PK__structures] PRIMARY KEY CLUSTERED ([_id]),
    CONSTRAINT [IX__structures] UNIQUE ([_id_scheme], [_name], [_id_parent]),
    CONSTRAINT [FK__structures__structures] FOREIGN KEY ([_id_parent]) REFERENCES [_structures]([_id]) ON DELETE NO ACTION,
    CONSTRAINT [FK__structures__schemes] FOREIGN KEY ([_id_scheme]) REFERENCES [_schemes]([_id]),
    CONSTRAINT [FK__structures__types] FOREIGN KEY ([_id_type]) REFERENCES [_types]([_id]),
    CONSTRAINT [FK__structures__lists] FOREIGN KEY ([_id_list]) REFERENCES [_lists]([_id]),
    CONSTRAINT [FK__structures__collection_type] FOREIGN KEY ([_collection_type]) REFERENCES [_types]([_id]),
    CONSTRAINT [FK__structures__key_type] FOREIGN KEY ([_key_type]) REFERENCES [_types]([_id])
)
GO

-- Dependencies table (scheme dependencies)
CREATE TABLE [dbo].[_dependencies](
    [_id] BIGINT NOT NULL,
    [_id_scheme_1] BIGINT NULL,
    [_id_scheme_2] BIGINT NOT NULL,
    CONSTRAINT [PK__dependencies] PRIMARY KEY CLUSTERED ([_id]),
    CONSTRAINT [IX__dependencies] UNIQUE ([_id_scheme_1], [_id_scheme_2]),
    CONSTRAINT [FK__dependencies__schemes_1] FOREIGN KEY ([_id_scheme_1]) REFERENCES [_schemes]([_id]),
    CONSTRAINT [FK__dependencies__schemes_2] FOREIGN KEY ([_id_scheme_2]) REFERENCES [_schemes]([_id]) ON DELETE CASCADE
)
GO

-- Objects table (data instances)
CREATE TABLE [dbo].[_objects](
    [_id] BIGINT NOT NULL,
    [_id_parent] BIGINT NULL,
    [_id_scheme] BIGINT NOT NULL,
    [_id_owner] BIGINT NOT NULL,
    [_id_who_change] BIGINT NOT NULL,
    [_date_create] DATETIMEOFFSET NOT NULL DEFAULT SYSDATETIMEOFFSET(),
    [_date_modify] DATETIMEOFFSET NOT NULL DEFAULT SYSDATETIMEOFFSET(),
    [_date_begin] DATETIMEOFFSET NULL,
    [_date_complete] DATETIMEOFFSET NULL,
    [_key] BIGINT NULL,
    [_name] NVARCHAR(450) NULL,  -- limited for index on IX__objects__name
    [_note] NVARCHAR(MAX) NULL,
    [_hash] UNIQUEIDENTIFIER NULL,
    -- Value columns for RedbPrimitive<T> (Props = primitive value stored directly)
    [_value_long] BIGINT NULL,
    [_value_string] NVARCHAR(MAX) NULL,
    [_value_guid] UNIQUEIDENTIFIER NULL,
    [_value_bool] BIT NULL,
    [_value_double] FLOAT NULL,
    [_value_numeric] DECIMAL(38, 18) NULL,
    [_value_datetime] DATETIMEOFFSET NULL,
    [_value_bytes] VARBINARY(MAX) NULL,
    CONSTRAINT [PK__objects] PRIMARY KEY CLUSTERED ([_id]),
    CONSTRAINT [FK__objects__objects] FOREIGN KEY ([_id_parent]) REFERENCES [_objects]([_id]) ON DELETE NO ACTION,
    CONSTRAINT [FK__objects__schemes] FOREIGN KEY ([_id_scheme]) REFERENCES [_schemes]([_id]) ON DELETE NO ACTION,
    CONSTRAINT [FK__objects__users1] FOREIGN KEY ([_id_owner]) REFERENCES [_users]([_id]),
    CONSTRAINT [FK__objects__users2] FOREIGN KEY ([_id_who_change]) REFERENCES [_users]([_id])
)
GO

-- List items table
CREATE TABLE [dbo].[_list_items](
    [_id] BIGINT NOT NULL,
    [_id_list] BIGINT NOT NULL,
    [_value] NVARCHAR(440) NULL,  -- max for composite UNIQUE (8+880 = 888 < 900 bytes)
    [_alias] NVARCHAR(450) NULL,
    [_id_object] BIGINT NULL,
    CONSTRAINT [PK__list_items] PRIMARY KEY CLUSTERED ([_id]),
    CONSTRAINT [FK__list_items__id_list] FOREIGN KEY ([_id_list]) REFERENCES [_lists]([_id]) ON DELETE CASCADE,
    CONSTRAINT [FK__list_items__objects] FOREIGN KEY ([_id_object]) REFERENCES [_objects]([_id]) ON DELETE SET NULL
)
GO

-- Unique index for list items (list + value must be unique)
CREATE UNIQUE INDEX [IX__list_items_unique] ON [dbo].[_list_items]([_id_list], [_value])
GO

-- Values table (EAV data storage)
CREATE TABLE [dbo].[_values](
    [_id] BIGINT NOT NULL,
    [_id_structure] BIGINT NOT NULL,
    [_id_object] BIGINT NOT NULL,
    [_String] NVARCHAR(MAX) NULL,
    [_Long] BIGINT NULL,
    [_Guid] UNIQUEIDENTIFIER NULL,
    [_Double] FLOAT NULL,
    [_DateTimeOffset] DATETIMEOFFSET NULL,
    [_Boolean] BIT NULL,
    [_ByteArray] VARBINARY(MAX) NULL,
    [_Numeric] DECIMAL(38, 18) NULL,
    [_ListItem] BIGINT NULL,
    [_Object] BIGINT NULL,
    -- Fields for relational collections (arrays, dictionaries, JSON/XML documents)
    [_array_parent_id] BIGINT NULL,
    [_array_index] NVARCHAR(430) NULL,  -- Text key for Dictionary (limited: 3*BIGINT + 860 bytes = 884 < 900)
    CONSTRAINT [PK__values] PRIMARY KEY CLUSTERED ([_id]),
    CONSTRAINT [FK__values__objects] FOREIGN KEY ([_id_object]) REFERENCES [_objects]([_id]) ON DELETE NO ACTION,
    CONSTRAINT [FK__values__structures] FOREIGN KEY ([_id_structure]) REFERENCES [_structures]([_id]) ON DELETE NO ACTION,
    CONSTRAINT [FK__values__array_parent] FOREIGN KEY ([_array_parent_id]) REFERENCES [_values]([_id]) ON DELETE NO ACTION,
    CONSTRAINT [FK__values__list_items] FOREIGN KEY ([_ListItem]) REFERENCES [_list_items]([_id]),
    CONSTRAINT [FK__values__objects_ref] FOREIGN KEY ([_Object]) REFERENCES [_objects]([_id])
)
-- NOTE: CASCADE delete handled by TR__objects__deleted_objects and TR__values__cascade_array_parent triggers
GO

-- Permissions table
CREATE TABLE [dbo].[_permissions](
    [_id] BIGINT NOT NULL,
    [_id_role] BIGINT NULL,
    [_id_user] BIGINT NULL,
    [_id_ref] BIGINT NOT NULL,
    [_select] BIT NULL,
    [_insert] BIT NULL,
    [_update] BIT NULL,
    [_delete] BIT NULL,
    CONSTRAINT [PK__permissions] PRIMARY KEY CLUSTERED ([_id]),
    CONSTRAINT [CK__permissions_users_roles] CHECK (
        ([_id_role] IS NOT NULL AND [_id_user] IS NULL) OR 
        ([_id_role] IS NULL AND [_id_user] IS NOT NULL)
    ),
    CONSTRAINT [IX__permissions] UNIQUE ([_id_role], [_id_user], [_id_ref], [_select], [_insert], [_update], [_delete]),
    CONSTRAINT [FK__permissions__roles] FOREIGN KEY ([_id_role]) REFERENCES [_roles]([_id]) ON DELETE CASCADE,
    CONSTRAINT [FK__permissions__users] FOREIGN KEY ([_id_user]) REFERENCES [_users]([_id]) ON DELETE CASCADE
)
GO

-- Functions table (stored procedures/expressions)
CREATE TABLE [dbo].[_functions](
    [_id] BIGINT NOT NULL,
    [_id_scheme] BIGINT NOT NULL,
    [_language] NVARCHAR(50) NOT NULL,
    [_name] NVARCHAR(440) NOT NULL,  -- max for composite UNIQUE (8 bytes for _id_scheme + 880 bytes = 888 < 900)
    [_body] NVARCHAR(MAX) NOT NULL,
    CONSTRAINT [PK__functions] PRIMARY KEY CLUSTERED ([_id]),
    CONSTRAINT [IX__functions_scheme_name] UNIQUE ([_id_scheme], [_name]),
    CONSTRAINT [FK__functions__schemes] FOREIGN KEY ([_id_scheme]) REFERENCES [_schemes]([_id])
)
GO

-- =====================================================
-- CREATE INDEXES
-- =====================================================

-- Users/Roles indexes
CREATE INDEX [IX__users_roles__roles] ON [dbo].[_users_roles]([_id_role])
CREATE INDEX [IX__users_roles__users] ON [dbo].[_users_roles]([_id_user])
CREATE INDEX [IX__users__id_configuration] ON [dbo].[_users]([_id_configuration]) WHERE [_id_configuration] IS NOT NULL
CREATE INDEX [IX__roles__id_configuration] ON [dbo].[_roles]([_id_configuration]) WHERE [_id_configuration] IS NOT NULL
GO

-- Schemes indexes
CREATE INDEX [IX__schemes__schemes] ON [dbo].[_schemes]([_id_parent])
CREATE INDEX [IX__schemes__structure_hash] ON [dbo].[_schemes]([_structure_hash])
CREATE INDEX [IX__schemes__type] ON [dbo].[_schemes]([_type])
GO

-- Structures indexes
CREATE INDEX [IX__structures__structures] ON [dbo].[_structures]([_id_parent])
CREATE INDEX [IX__structures__schemes] ON [dbo].[_structures]([_id_scheme])
CREATE INDEX [IX__structures__types] ON [dbo].[_structures]([_id_type])
CREATE INDEX [IX__structures__lists] ON [dbo].[_structures]([_id_list])
-- Covering index for ORDER BY queries by structure name
CREATE INDEX [IX__structures__name] ON [dbo].[_structures]([_name]) INCLUDE ([_id], [_id_type], [_collection_type], [_id_scheme])
-- Covering index for structure lookup by ID (critical for InitPlan/EXISTS)
CREATE INDEX [IX__structures__id_lookup] ON [dbo].[_structures]([_id]) INCLUDE ([_id_type], [_name], [_collection_type], [_id_scheme])
-- Partial index for non-collection structures
CREATE INDEX [IX__structures__not_collection] ON [dbo].[_structures]([_id], [_name], [_id_scheme]) WHERE [_collection_type] IS NULL
-- Enhanced partial index for non-collection structures with type
CREATE INDEX [IX__structures__not_collection_enhanced] ON [dbo].[_structures]([_id], [_name], [_id_scheme], [_id_type]) WHERE [_collection_type] IS NULL
-- Index for collection fields (Array/Dictionary)
CREATE INDEX [IX__structures__collection] ON [dbo].[_structures]([_id], [_id_scheme], [_collection_type]) WHERE [_collection_type] IS NOT NULL
CREATE INDEX [IX__structures__collection_type] ON [dbo].[_structures]([_collection_type]) WHERE [_collection_type] IS NOT NULL
CREATE INDEX [IX__structures__key_type] ON [dbo].[_structures]([_key_type]) WHERE [_key_type] IS NOT NULL
GO

-- Dependencies indexes
CREATE INDEX [IX__dependencies__schemes_1] ON [dbo].[_dependencies]([_id_scheme_1])
CREATE INDEX [IX__dependencies__schemes_2] ON [dbo].[_dependencies]([_id_scheme_2])
GO

-- Objects indexes
CREATE INDEX [IX__objects__objects] ON [dbo].[_objects]([_id_parent])
CREATE INDEX [IX__objects__schemes] ON [dbo].[_objects]([_id_scheme])
CREATE INDEX [IX__objects__users1] ON [dbo].[_objects]([_id_owner])
CREATE INDEX [IX__objects__users2] ON [dbo].[_objects]([_id_who_change])
CREATE INDEX [IX__objects__date_create] ON [dbo].[_objects]([_date_create])
CREATE INDEX [IX__objects__date_modify] ON [dbo].[_objects]([_date_modify])
CREATE INDEX [IX__objects__name] ON [dbo].[_objects]([_name])
CREATE INDEX [IX__objects__hash] ON [dbo].[_objects]([_hash])
-- RedbPrimitive<T> value indexes (without filtered - _value_string is NVARCHAR(MAX))
CREATE INDEX [IX__objects__value_long] ON [dbo].[_objects]([_value_long]) WHERE [_value_long] IS NOT NULL
CREATE INDEX [IX__objects__value_guid] ON [dbo].[_objects]([_value_guid]) WHERE [_value_guid] IS NOT NULL
CREATE INDEX [IX__objects__value_datetime] ON [dbo].[_objects]([_value_datetime]) WHERE [_value_datetime] IS NOT NULL
CREATE INDEX [IX__objects__value_numeric] ON [dbo].[_objects]([_value_numeric]) WHERE [_value_numeric] IS NOT NULL
-- Tree query indexes
CREATE INDEX [IX__objects__scheme_parent] ON [dbo].[_objects]([_id_scheme], [_id_parent], [_id])
CREATE INDEX [IX__objects__root_objects] ON [dbo].[_objects]([_id_scheme], [_id]) WHERE [_id_parent] IS NULL
CREATE INDEX [IX__objects__parent_scheme_id] ON [dbo].[_objects]([_id_parent], [_id_scheme], [_id]) WHERE [_id_parent] IS NOT NULL
CREATE INDEX [IX__objects__scheme_date_create] ON [dbo].[_objects]([_id_scheme], [_date_create] DESC, [_id])
CREATE INDEX [IX__objects__scheme_date_modify] ON [dbo].[_objects]([_id_scheme], [_date_modify] DESC, [_id])
CREATE INDEX [IX__objects__scheme_name] ON [dbo].[_objects]([_id_scheme], [_name], [_id])
-- Covering index for descendant lookup (WhereHasAncestor optimization)
CREATE INDEX [IX__objects__parent_id_descendant_lookup] ON [dbo].[_objects](
    [_id_parent], [_id_scheme]
) INCLUDE ([_id], [_id_owner], [_date_create], [_date_modify]) WHERE [_id_parent] IS NOT NULL
-- Covering index for tree queries with date sorting
CREATE INDEX [IX__objects__scheme_parent_date_create] ON [dbo].[_objects](
    [_id_scheme], [_id_parent], [_date_create] DESC
) INCLUDE ([_id], [_id_owner], [_date_modify])
-- Composite index for reverse ancestor lookup (child -> parent)
CREATE INDEX [IX__objects__id_parent_scheme] ON [dbo].[_objects]([_id], [_id_parent], [_id_scheme]) WHERE [_id_parent] IS NOT NULL
-- Covering index for tree queries with filtering
CREATE INDEX [IX__objects__scheme_parent_owner] ON [dbo].[_objects](
    [_id_scheme], [_id_parent]
) INCLUDE ([_id], [_id_owner], [_date_create], [_date_modify], [_name]) WHERE [_id_parent] IS NOT NULL
GO

-- List items indexes
CREATE INDEX [IX__list_items__id_list] ON [dbo].[_list_items]([_id_list])
CREATE INDEX [IX__list_items__objects] ON [dbo].[_list_items]([_id_object])
GO

-- Values indexes
CREATE INDEX [IX__values__objects] ON [dbo].[_values]([_id_object])
CREATE INDEX [IX__values__structures] ON [dbo].[_values]([_id_structure])
CREATE INDEX [IX__values__array_parent_id] ON [dbo].[_values]([_array_parent_id])
CREATE INDEX [IX__values__array_parent_index] ON [dbo].[_values]([_array_parent_id], [_array_index])
CREATE INDEX [IX__values__array_key] ON [dbo].[_values]([_id_structure], [_array_index]) WHERE [_array_index] IS NOT NULL
-- Index for nested Dictionary/Array field lookups via _array_parent_id (PRO PVT CTE)
CREATE INDEX [IX__values__parent_structure] ON [dbo].[_values]([_array_parent_id], [_id_structure]) WHERE [_array_parent_id] IS NOT NULL
-- Covering index for object-structure lookups with array_index
CREATE INDEX [IX__values__object_structure_lookup] ON [dbo].[_values](
    [_id_object], [_id_structure], [_array_index]
) INCLUDE (
    [_String], [_Long], [_DateTimeOffset], [_Boolean], [_Double], [_Guid], [_Numeric], [_ListItem], [_Object]
)
-- Covering index for non-array values (Index Only Scan for EXISTS/JOIN)
CREATE INDEX [IX__values__object_array_null] ON [dbo].[_values](
    [_id_object], [_id_structure]
) INCLUDE (
    [_String], [_Long], [_DateTimeOffset], [_Boolean], [_Double], [_Guid], [_Numeric], [_ListItem], [_Object]
) WHERE [_array_index] IS NULL
-- Critical: Covering index for facet queries (structure -> object)
CREATE INDEX [IX__values__structure_object_lookup] ON [dbo].[_values](
    [_id_structure], [_id_object]
) INCLUDE (
    [_String], [_Long], [_DateTimeOffset], [_Boolean], [_Double], [_Guid], [_Numeric], [_ListItem], [_Object]
)
-- Partial indexes for NOT NULL values (EXISTS with value conditions)
CREATE INDEX [IX__values__String_not_null] ON [dbo].[_values]([_id_structure], [_id_object]) WHERE [_String] IS NOT NULL
CREATE INDEX [IX__values__Long_not_null] ON [dbo].[_values]([_id_structure], [_id_object], [_Long]) WHERE [_Long] IS NOT NULL
CREATE INDEX [IX__values__DateTimeOffset_not_null] ON [dbo].[_values]([_id_structure], [_id_object], [_DateTimeOffset]) WHERE [_DateTimeOffset] IS NOT NULL
CREATE INDEX [IX__values__Numeric_not_null] ON [dbo].[_values]([_id_structure], [_id_object], [_Numeric]) WHERE [_Numeric] IS NOT NULL
-- Indexes for EXISTS/PVT queries - filter by value type
CREATE INDEX [IX__values__Long_filter] ON [dbo].[_values]([_id_structure], [_id_object], [_Long]) WHERE [_Long] IS NOT NULL
CREATE INDEX [IX__values__Guid_filter] ON [dbo].[_values]([_id_structure], [_id_object], [_Guid]) WHERE [_Guid] IS NOT NULL
CREATE INDEX [IX__values__Double_filter] ON [dbo].[_values]([_id_structure], [_id_object], [_Double]) WHERE [_Double] IS NOT NULL
CREATE INDEX [IX__values__DateTimeOffset_filter] ON [dbo].[_values]([_id_structure], [_id_object], [_DateTimeOffset]) WHERE [_DateTimeOffset] IS NOT NULL
CREATE INDEX [IX__values__Boolean_filter] ON [dbo].[_values]([_id_structure], [_id_object], [_Boolean]) WHERE [_Boolean] IS NOT NULL
CREATE INDEX [IX__values__Numeric_filter] ON [dbo].[_values]([_id_structure], [_id_object], [_Numeric]) WHERE [_Numeric] IS NOT NULL
CREATE INDEX [IX__values__ListItem_filter] ON [dbo].[_values]([_id_structure], [_id_object], [_ListItem]) WHERE [_ListItem] IS NOT NULL
CREATE INDEX [IX__values__Object_filter] ON [dbo].[_values]([_id_structure], [_id_object], [_Object]) WHERE [_Object] IS NOT NULL


-- Индекс для быстрого LEFT JOIN nested Dictionary полей (AddressBook[home].City)
-- Ускоряет Nested Loop → Index Only Scan для вложенных полей
CREATE NONCLUSTERED INDEX IX__values__structure_parent_batch
ON _values (_id_structure, _array_parent_id)
INCLUDE (_String, _Long, _Double, _Boolean, _Guid)
WHERE _array_parent_id IS NOT NULL;

CREATE NONCLUSTERED INDEX IX__values__array_null_structure
ON _values (_array_index, _id_structure)
INCLUDE (_id_object, _Long);

CREATE NONCLUSTERED INDEX IX__values__pvt_dict_lookup
ON [dbo].[_values] ([_id_structure], [_array_index], [_id_object])
INCLUDE ([_string], [_Long], [_Double], [_DateTimeOffset])

CREATE NONCLUSTERED INDEX IX__objects__id_parent 
ON _objects (_id_parent) 
INCLUDE (_id, _hash, _id_scheme);

GO

-- =====================================================
-- FULL-TEXT SEARCH FOR STRING VALUES (OPTIONAL)
-- =====================================================

-- Only create if Full-Text is installed
IF SERVERPROPERTY('IsFullTextInstalled') = 1
BEGIN
    IF NOT EXISTS (SELECT * FROM sys.fulltext_catalogs WHERE name = 'redb_fulltext_catalog')
        CREATE FULLTEXT CATALOG [redb_fulltext_catalog] AS DEFAULT
    
    IF NOT EXISTS (SELECT * FROM sys.fulltext_indexes WHERE object_id = OBJECT_ID('[dbo].[_values]'))
        CREATE FULLTEXT INDEX ON [dbo].[_values]([_String])
            KEY INDEX [PK__values]
            ON [redb_fulltext_catalog]
            WITH STOPLIST = OFF, CHANGE_TRACKING AUTO
END
ELSE
    PRINT 'WARNING: Full-Text Search is not installed. To install: apt-get install -y mssql-server-fts'
GO

-- Index for non-array values
CREATE INDEX [IX__values__object_structure_array_index] ON [dbo].[_values]([_id_object], [_id_structure], [_array_index]) WHERE [_array_index] IS NOT NULL
GO

-- Unique indexes for data integrity
CREATE UNIQUE INDEX [UIX__values__structure_object] ON [dbo].[_values]([_id_structure], [_id_object]) 
    WHERE [_array_index] IS NULL AND [_array_parent_id] IS NULL

CREATE UNIQUE INDEX [UIX__values__structure_object_parent] ON [dbo].[_values]([_id_structure], [_id_object], [_array_parent_id]) 
    WHERE [_array_index] IS NULL AND [_array_parent_id] IS NOT NULL

CREATE UNIQUE INDEX [UIX__values__structure_object_array_index] ON [dbo].[_values]([_id_structure], [_id_object], [_array_parent_id], [_array_index]) 
    WHERE [_array_index] IS NOT NULL
GO

-- Permissions indexes
CREATE INDEX [IX__permissions__roles] ON [dbo].[_permissions]([_id_role])
CREATE INDEX [IX__permissions__users] ON [dbo].[_permissions]([_id_user])
CREATE INDEX [IX__permissions__ref] ON [dbo].[_permissions]([_id_ref])
GO

-- Functions indexes
CREATE INDEX [IX__functions__schemes] ON [dbo].[_functions]([_id_scheme])
GO

-- =====================================================
-- CREATE SEQUENCE
-- =====================================================

CREATE SEQUENCE [dbo].[global_identity] 
    AS BIGINT
    START WITH 1000000
    INCREMENT BY 1
    MINVALUE 1000000
    MAXVALUE 9223372036854775807
    NO CACHE
GO

-- =====================================================
-- INSERT BASE TYPES
-- =====================================================

-- Core types
INSERT INTO [dbo].[_types] ([_id], [_name], [_db_type], [_type]) VALUES (-9223372036854775709, 'Boolean', 'Boolean', 'boolean')
INSERT INTO [dbo].[_types] ([_id], [_name], [_db_type], [_type]) VALUES (-9223372036854775708, 'DateTime', 'DateTimeOffset', 'DateTime')
INSERT INTO [dbo].[_types] ([_id], [_name], [_db_type], [_type]) VALUES (-9223372036854775707, 'Double', 'Double', 'double')
INSERT INTO [dbo].[_types] ([_id], [_name], [_db_type], [_type]) VALUES (-9223372036854775706, 'ListItem', 'ListItem', '_RListItem')
INSERT INTO [dbo].[_types] ([_id], [_name], [_db_type], [_type]) VALUES (-9223372036854775705, 'Guid', 'Guid', 'Guid')
INSERT INTO [dbo].[_types] ([_id], [_name], [_db_type], [_type]) VALUES (-9223372036854775704, 'Long', 'Long', 'long')
INSERT INTO [dbo].[_types] ([_id], [_name], [_db_type], [_type]) VALUES (-9223372036854775703, 'Object', 'Object', '_RObject')
INSERT INTO [dbo].[_types] ([_id], [_name], [_db_type], [_type]) VALUES (-9223372036854775701, 'ByteArray', 'ByteArray', 'byte[]')
INSERT INTO [dbo].[_types] ([_id], [_name], [_db_type], [_type]) VALUES (-9223372036854775700, 'String', 'String', 'string')

-- Extended types
INSERT INTO [dbo].[_types] ([_id], [_name], [_db_type], [_type]) VALUES (-9223372036854775699, 'Int', 'Long', 'int')
INSERT INTO [dbo].[_types] ([_id], [_name], [_db_type], [_type]) VALUES (-9223372036854775698, 'Short', 'Long', 'short')
INSERT INTO [dbo].[_types] ([_id], [_name], [_db_type], [_type]) VALUES (-9223372036854775697, 'Byte', 'Long', 'byte')
INSERT INTO [dbo].[_types] ([_id], [_name], [_db_type], [_type]) VALUES (-9223372036854775696, 'Float', 'Double', 'float')
INSERT INTO [dbo].[_types] ([_id], [_name], [_db_type], [_type]) VALUES (-9223372036854775695, 'Decimal', 'Numeric', 'decimal')
INSERT INTO [dbo].[_types] ([_id], [_name], [_db_type], [_type]) VALUES (-9223372036854775694, 'Char', 'String', 'char')
INSERT INTO [dbo].[_types] ([_id], [_name], [_db_type], [_type]) VALUES (-9223372036854775693, 'Url', 'String', 'string')
INSERT INTO [dbo].[_types] ([_id], [_name], [_db_type], [_type]) VALUES (-9223372036854775692, 'Email', 'String', 'string')
INSERT INTO [dbo].[_types] ([_id], [_name], [_db_type], [_type]) VALUES (-9223372036854775691, 'Phone', 'String', 'string')
INSERT INTO [dbo].[_types] ([_id], [_name], [_db_type], [_type]) VALUES (-9223372036854775690, 'Json', 'String', 'string')
INSERT INTO [dbo].[_types] ([_id], [_name], [_db_type], [_type]) VALUES (-9223372036854775689, 'Xml', 'String', 'string')
INSERT INTO [dbo].[_types] ([_id], [_name], [_db_type], [_type]) VALUES (-9223372036854775688, 'Base64', 'String', 'string')
INSERT INTO [dbo].[_types] ([_id], [_name], [_db_type], [_type]) VALUES (-9223372036854775687, 'Color', 'String', 'string')
INSERT INTO [dbo].[_types] ([_id], [_name], [_db_type], [_type]) VALUES (-9223372036854775686, 'DateOnly', 'DateTime', 'DateOnly')
INSERT INTO [dbo].[_types] ([_id], [_name], [_db_type], [_type]) VALUES (-9223372036854775685, 'TimeOnly', 'String', 'TimeOnly')
INSERT INTO [dbo].[_types] ([_id], [_name], [_db_type], [_type]) VALUES (-9223372036854775684, 'TimeSpan', 'String', 'TimeSpan')
INSERT INTO [dbo].[_types] ([_id], [_name], [_db_type], [_type]) VALUES (-9223372036854775683, 'Enum', 'String', 'Enum')
INSERT INTO [dbo].[_types] ([_id], [_name], [_db_type], [_type]) VALUES (-9223372036854775682, 'EnumInt', 'Long', 'Enum')
INSERT INTO [dbo].[_types] ([_id], [_name], [_db_type], [_type]) VALUES (-9223372036854775681, 'Latitude', 'Double', 'double')
INSERT INTO [dbo].[_types] ([_id], [_name], [_db_type], [_type]) VALUES (-9223372036854775680, 'Longitude', 'Double', 'double')
INSERT INTO [dbo].[_types] ([_id], [_name], [_db_type], [_type]) VALUES (-9223372036854775679, 'GeoPoint', 'String', 'string')
INSERT INTO [dbo].[_types] ([_id], [_name], [_db_type], [_type]) VALUES (-9223372036854775678, 'FilePath', 'String', 'string')
INSERT INTO [dbo].[_types] ([_id], [_name], [_db_type], [_type]) VALUES (-9223372036854775677, 'FileName', 'String', 'string')
INSERT INTO [dbo].[_types] ([_id], [_name], [_db_type], [_type]) VALUES (-9223372036854775676, 'MimeType', 'String', 'string')
INSERT INTO [dbo].[_types] ([_id], [_name], [_db_type], [_type]) VALUES (-9223372036854775675, 'Class', 'Guid', 'Object')
INSERT INTO [dbo].[_types] ([_id], [_name], [_db_type], [_type]) VALUES (-9223372036854775674, 'Numeric', 'Numeric', 'decimal')
INSERT INTO [dbo].[_types] ([_id], [_name], [_db_type], [_type]) VALUES (-9223372036854775673, 'DateTimeOffset', 'DateTimeOffset', 'DateTimeOffset')

-- Collection and document types
INSERT INTO [dbo].[_types] ([_id], [_name], [_db_type], [_type]) VALUES (-9223372036854775668, 'Array', 'Guid', 'Array')
INSERT INTO [dbo].[_types] ([_id], [_name], [_db_type], [_type]) VALUES (-9223372036854775667, 'Dictionary', 'Guid', 'Dictionary')
INSERT INTO [dbo].[_types] ([_id], [_name], [_db_type], [_type]) VALUES (-9223372036854775666, 'JsonDocument', 'Guid', 'JsonDocument')
INSERT INTO [dbo].[_types] ([_id], [_name], [_db_type], [_type]) VALUES (-9223372036854775665, 'XDocument', 'Guid', 'XDocument')
GO

-- =====================================================
-- INSERT DEFAULT USERS
-- =====================================================

INSERT INTO [dbo].[_users] ([_id], [_login], [_password], [_name], [_phone], [_email], [_date_register], [_date_dismiss], [_enabled])
VALUES (-1, 'default', '', 'default', NULL, NULL, '2023-12-26T01:14:34.410+00:00', NULL, 1)

INSERT INTO [dbo].[_users] ([_id], [_login], [_password], [_name], [_phone], [_email], [_date_register], [_date_dismiss], [_enabled])
VALUES (0, 'sys', '', 'sys', NULL, NULL, '2023-12-26T01:14:34.410+00:00', NULL, 1)

INSERT INTO [dbo].[_users] ([_id], [_login], [_password], [_name], [_phone], [_email], [_date_register], [_date_dismiss], [_enabled])
VALUES (1, 'admin', '', 'admin', NULL, NULL, '2023-12-26T01:14:34.410+00:00', NULL, 1)
GO

-- =====================================================
-- SOFT DELETE SYSTEM: Reserved scheme for deleted objects
-- =====================================================
-- Scheme @@__deleted is used for soft-delete functionality
-- Objects marked for deletion get _id_scheme = -10
-- Type = Object (-9223372036854775703) means no Props/structures

IF NOT EXISTS (SELECT 1 FROM [dbo].[_schemes] WHERE [_id] = -10)
INSERT INTO [dbo].[_schemes] ([_id], [_name], [_alias], [_type]) 
VALUES (-10, '@@__deleted', 'Deleted Objects', -9223372036854775703)
GO

-- =====================================================
-- ADD FOREIGN KEY FOR CONFIGURATION (after _objects exists)
-- =====================================================

ALTER TABLE [dbo].[_users] ADD CONSTRAINT [FK__users__configuration] 
    FOREIGN KEY ([_id_configuration]) REFERENCES [_objects]([_id]) ON DELETE SET NULL
GO

ALTER TABLE [dbo].[_roles] ADD CONSTRAINT [FK__roles__configuration] 
    FOREIGN KEY ([_id_configuration]) REFERENCES [_objects]([_id]) ON DELETE SET NULL
GO

-- =====================================================
-- TRIGGER: Cascade delete _values before _structures
-- (MSSQL workaround - no ON DELETE CASCADE for _values._id_structure)
-- =====================================================

IF EXISTS (SELECT * FROM sys.triggers WHERE name = 'TR__structures__cascade_values')
DROP TRIGGER [dbo].[TR__structures__cascade_values]
GO

CREATE TRIGGER [dbo].[TR__structures__cascade_values]
ON [dbo].[_structures]
INSTEAD OF DELETE
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Delete values first (cascade)
    DELETE v FROM [dbo].[_values] v
    INNER JOIN deleted d ON v.[_id_structure] = d.[_id];
    
    -- Delete the structures
    DELETE s FROM [dbo].[_structures] s
    INNER JOIN deleted d ON s.[_id] = d.[_id];
END
GO

-- =====================================================
-- TRIGGER: Cascade delete _values before _objects
-- (MSSQL workaround - no ON DELETE CASCADE for _values._id_object)
-- =====================================================

IF EXISTS (SELECT * FROM sys.triggers WHERE name = 'TR__objects__cascade_values')
DROP TRIGGER [dbo].[TR__objects__cascade_values]
GO

CREATE TRIGGER [dbo].[TR__objects__cascade_values]
ON [dbo].[_objects]
INSTEAD OF DELETE
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Delete values first (cascade)
    DELETE v FROM [dbo].[_values] v
    INNER JOIN deleted d ON v.[_id_object] = d.[_id];
    
    -- Delete the objects
    DELETE o FROM [dbo].[_objects] o
    INNER JOIN deleted d ON o.[_id] = d.[_id];
END
GO

-- =====================================================
-- TRIGGER: Cascade delete for _values._array_parent_id
-- (MSSQL workaround for multiple cascade paths restriction)
-- =====================================================

IF EXISTS (SELECT * FROM sys.triggers WHERE name = 'TR__values__cascade_array_parent')
DROP TRIGGER [dbo].[TR__values__cascade_array_parent]
GO

CREATE TRIGGER [dbo].[TR__values__cascade_array_parent]
ON [dbo].[_values]
INSTEAD OF DELETE
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Recursively delete children first (depth-first)
    ;WITH cte AS (
        SELECT [_id] FROM deleted
        UNION ALL
        SELECT v.[_id] FROM [dbo].[_values] v
        INNER JOIN cte ON v.[_array_parent_id] = cte.[_id]
    )
    DELETE FROM [dbo].[_values] 
    WHERE [_id] IN (SELECT [_id] FROM cte);
END
GO

-- =====================================================
-- PROCEDURE: Cascade delete object with all descendants
-- Deletes object and all its children recursively (bottom-up)
-- =====================================================

IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'delete_object_cascade')
DROP PROCEDURE [dbo].[delete_object_cascade]
GO

CREATE PROCEDURE [dbo].[delete_object_cascade]
    @object_id BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Collect all descendants into temp table
    CREATE TABLE #to_delete (_id BIGINT, depth INT);
    
    ;WITH Descendants AS (
        SELECT _id, 0 AS depth FROM _objects WHERE _id = @object_id
        UNION ALL
        SELECT o._id, d.depth + 1 
        FROM _objects o
        INNER JOIN Descendants d ON o._id_parent = d._id
    )
    INSERT INTO #to_delete SELECT _id, depth FROM Descendants;
    
    -- Delete bottom-up: deepest children first, then parents
    DECLARE @max_depth INT = (SELECT MAX(depth) FROM #to_delete);
    WHILE @max_depth >= 0
    BEGIN
        DELETE FROM _objects WHERE _id IN (SELECT _id FROM #to_delete WHERE depth = @max_depth);
        SET @max_depth = @max_depth - 1;
    END
    
    DROP TABLE #to_delete;
END
GO

-- =====================================================
-- PROCEDURE: Cascade delete multiple objects with all descendants
-- Deletes objects and all their children recursively (bottom-up)
-- =====================================================

IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'delete_objects_cascade')
DROP PROCEDURE [dbo].[delete_objects_cascade]
GO

CREATE PROCEDURE [dbo].[delete_objects_cascade]
    @object_ids NVARCHAR(MAX) -- Comma-separated list of IDs
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Parse IDs and collect all descendants into temp table
    CREATE TABLE #to_delete (_id BIGINT, depth INT);
    
    ;WITH Descendants AS (
        SELECT _id, 0 AS depth FROM _objects 
        WHERE _id IN (SELECT CAST(value AS BIGINT) FROM STRING_SPLIT(@object_ids, ','))
        UNION ALL
        SELECT o._id, d.depth + 1 
        FROM _objects o
        INNER JOIN Descendants d ON o._id_parent = d._id
    )
    INSERT INTO #to_delete SELECT _id, depth FROM Descendants;
    
    -- Delete bottom-up: deepest children first, then parents
    DECLARE @max_depth INT = (SELECT MAX(depth) FROM #to_delete);
    WHILE @max_depth >= 0
    BEGIN
        DELETE FROM _objects WHERE _id IN (SELECT _id FROM #to_delete WHERE depth = @max_depth);
        SET @max_depth = @max_depth - 1;
    END
    
    DROP TABLE #to_delete;
END
GO

-- =====================================================
-- VIEW: v_user_permissions
-- Hierarchical permission inheritance (PostgreSQL compatible)
-- =====================================================

IF OBJECT_ID('dbo.v_user_permissions', 'V') IS NOT NULL
    DROP VIEW dbo.v_user_permissions;
GO

CREATE VIEW dbo.v_user_permissions AS
WITH permission_search AS (
    -- Step 1: Each object searches for its permission
    SELECT 
        o._id AS object_id,
        o._id AS current_search_id,
        o._id_parent,
        0 AS level,
        CASE WHEN EXISTS(SELECT 1 FROM _permissions WHERE _id_ref = o._id) THEN 1 ELSE 0 END AS has_permission
    FROM _objects o
    
    UNION ALL
    
    -- Step 2: If NO permission - go to parent
    SELECT 
        ps.object_id,
        o._id AS current_search_id,
        o._id_parent,
        ps.level + 1,
        CASE WHEN EXISTS(SELECT 1 FROM _permissions WHERE _id_ref = o._id) THEN 1 ELSE 0 END AS has_permission
    FROM _objects o
    JOIN permission_search ps ON o._id = ps._id_parent
    WHERE ps.level < 50
      AND ps.has_permission = 0  -- continue only if NO permission
),
-- Get first found permission for each object (using ROW_NUMBER instead of DISTINCT ON)
object_permissions AS (
    SELECT 
        ps.object_id,
        p._id AS permission_id,
        p._id_user,
        p._id_role,
        p._select,
        p._insert,
        p._update,
        p._delete,
        ps.level,
        ROW_NUMBER() OVER (PARTITION BY ps.object_id ORDER BY ps.level) AS rn
    FROM permission_search ps
    JOIN _permissions p ON p._id_ref = ps.current_search_id
    WHERE ps.has_permission = 1
),
object_permissions_first AS (
    SELECT * FROM object_permissions WHERE rn = 1
),
-- Global permissions as virtual records with object_id = 0
global_permissions AS (
    SELECT 
        CAST(0 AS BIGINT) AS object_id,
        p._id AS permission_id,
        p._id_user,
        p._id_role,
        p._select,
        p._insert,
        p._update,
        p._delete,
        999 AS level,
        1 AS rn
    FROM _permissions p
    WHERE p._id_ref = 0
),
-- Combine specific and global permissions
all_permissions AS (
    SELECT object_id, permission_id, _id_user, _id_role, _select, _insert, _update, _delete, level
    FROM object_permissions_first
    UNION ALL
    SELECT object_id, permission_id, _id_user, _id_role, _select, _insert, _update, _delete, level
    FROM global_permissions
),
-- Get first by priority (specific > global)
final_permissions AS (
    SELECT 
        object_id, permission_id, _id_user, _id_role, _select, _insert, _update, _delete, level,
        ROW_NUMBER() OVER (PARTITION BY object_id ORDER BY level) AS rn
    FROM all_permissions
),
final_permissions_first AS (
    SELECT * FROM final_permissions WHERE rn = 1
)
-- Result: for user permissions - direct, for role - through users_roles
SELECT 
    fp.object_id,
    CASE 
        WHEN fp._id_user IS NOT NULL THEN fp._id_user
        ELSE ur._id_user
    END AS user_id,
    fp.permission_id,
    CASE 
        WHEN fp._id_user IS NOT NULL THEN N'user'
        ELSE N'role'
    END AS permission_type,
    fp._id_role,
    fp._select AS can_select,
    fp._insert AS can_insert,
    fp._update AS can_update,
    fp._delete AS can_delete
FROM final_permissions_first fp
LEFT JOIN _users_roles ur ON ur._id_role = fp._id_role
WHERE fp._id_user IS NOT NULL OR ur._id_user IS NOT NULL;
GO

PRINT '========================================='
PRINT 'REDB MS SQL Server Schema v2.0 created!'
PRINT 'Includes: Array, Dictionary, JsonDocument, XDocument support'
PRINT '========================================='
GO


-- ===== redb_metadata_cache.sql =====
-- ============================================================
-- METADATA CACHE: Solution for repeated JOINs
-- MS SQL Server version
-- ============================================================
-- Goal: Avoid repeated JOIN _structures <- _types in every query
-- Approach: Regular table + automatic sync via triggers on _structure_hash
-- Benefits:
--   No recursion issues (indexes created once)
--   Works with connection pooling (global table)
--   Automatic invalidation (triggers on _schemes._structure_hash)
--   No C# code changes required
--   Minimal cache rebuilds (only on real schema changes)
-- ============================================================


-- =====================================================
-- DROP EXISTING OBJECTS
-- =====================================================

IF EXISTS (SELECT * FROM sys.triggers WHERE name = 'TR__schemes__sync_metadata_cache')
    DROP TRIGGER [dbo].[TR__schemes__sync_metadata_cache]
GO

IF EXISTS (SELECT * FROM sys.triggers WHERE name = 'TR__schemes__cleanup_metadata_cache')
    DROP TRIGGER [dbo].[TR__schemes__cleanup_metadata_cache]
GO

IF EXISTS (SELECT * FROM sys.triggers WHERE name = 'TR__types__invalidate_metadata_cache')
    DROP TRIGGER [dbo].[TR__types__invalidate_metadata_cache]
GO

IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'sync_metadata_cache_for_scheme')
    DROP PROCEDURE [dbo].[sync_metadata_cache_for_scheme]
GO

IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'warmup_all_metadata_caches')
    DROP PROCEDURE [dbo].[warmup_all_metadata_caches]
GO

IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'check_metadata_cache_consistency')
    DROP PROCEDURE [dbo].[check_metadata_cache_consistency]
GO

IF OBJECT_ID('[dbo].[_scheme_metadata_cache]', 'U') IS NOT NULL 
    DROP TABLE [dbo].[_scheme_metadata_cache]
GO

-- =====================================================
-- 1. CREATE METADATA CACHE TABLE
-- =====================================================

CREATE TABLE [dbo].[_scheme_metadata_cache] (
    -- Identifiers
    [_scheme_id] BIGINT NOT NULL,
    [_structure_id] BIGINT NOT NULL,
    [_parent_structure_id] BIGINT NULL,
    [_id_override] BIGINT NULL,
    
    -- Names and aliases
    [_name] NVARCHAR(450) NOT NULL,
    [_alias] NVARCHAR(450) NULL,
    
    -- Structure type info
    [_type_id] BIGINT NOT NULL,
    [_list_id] BIGINT NULL,
    [type_name] NVARCHAR(450) NOT NULL,
    [db_type] NVARCHAR(450) NOT NULL,
    [type_semantic] NVARCHAR(450) NOT NULL,
    
    -- Scheme type info (Class/Array/Dictionary/JsonDocument/XDocument)
    [_scheme_type] BIGINT NULL,
    [scheme_type_name] NVARCHAR(450) NULL,
    
    -- Structure attributes
    [_order] BIGINT NULL,
    [_collection_type] BIGINT NULL,           -- NULL = not collection, Array/Dictionary type ID
    [collection_type_name] NVARCHAR(450) NULL, -- Collection type name
    [_key_type] BIGINT NULL,                  -- Key type for Dictionary
    [key_type_name] NVARCHAR(450) NULL,       -- Key type name
    [_readonly] BIT NULL,
    [_allow_not_null] BIT NULL,
    [_is_compress] BIT NULL,
    [_store_null] BIT NULL,
    
    -- Default values
    [_default_value] VARBINARY(MAX) NULL,
    [_default_editor] NVARCHAR(MAX) NULL
)
GO

-- =====================================================
-- 2. CREATE INDEXES
-- =====================================================

CREATE INDEX [idx_metadata_cache_lookup] 
    ON [dbo].[_scheme_metadata_cache]([_scheme_id], [_parent_structure_id], [_order])
GO

CREATE INDEX [idx_metadata_cache_structure] 
    ON [dbo].[_scheme_metadata_cache]([_structure_id])
GO

CREATE INDEX [idx_metadata_cache_scheme]
    ON [dbo].[_scheme_metadata_cache]([_scheme_id])
GO

CREATE INDEX [idx_metadata_cache_name]
    ON [dbo].[_scheme_metadata_cache]([_scheme_id], [_name])
GO

CREATE INDEX [idx_metadata_cache_collection]
    ON [dbo].[_scheme_metadata_cache]([_scheme_id], [_collection_type])
    WHERE [_collection_type] IS NOT NULL
GO

CREATE INDEX [idx_metadata_cache_scheme_type]
    ON [dbo].[_scheme_metadata_cache]([_scheme_id], [_scheme_type])
GO

CREATE INDEX [idx_metadata_cache_key_type]
    ON [dbo].[_scheme_metadata_cache]([_scheme_id], [_key_type])
    WHERE [_key_type] IS NOT NULL
GO

-- =====================================================
-- 3. SYNC PROCEDURE FOR SINGLE SCHEME
-- =====================================================

CREATE PROCEDURE [dbo].[sync_metadata_cache_for_scheme]
    @target_scheme_id BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Delete old cache data for scheme
    DELETE FROM [dbo].[_scheme_metadata_cache] 
    WHERE [_scheme_id] = @target_scheme_id;
    
    -- Insert current data (with collection types and scheme type support)
    INSERT INTO [dbo].[_scheme_metadata_cache] (
        [_scheme_id], [_structure_id], [_parent_structure_id], [_id_override],
        [_name], [_alias],
        [_type_id], [_list_id], [type_name], [db_type], [type_semantic],
        [_scheme_type], [scheme_type_name],
        [_order], [_collection_type], [collection_type_name], [_key_type], [key_type_name],
        [_readonly], [_allow_not_null], [_is_compress], [_store_null],
        [_default_value], [_default_editor]
    )
    SELECT 
        s.[_id_scheme],
        s.[_id],
        s.[_id_parent],
        s.[_id_override],
        s.[_name],
        s.[_alias],
        t.[_id],
        s.[_id_list],
        t.[_name],
        t.[_db_type],
        t.[_type],
        sch.[_type],                    -- Scheme type
        scht.[_name],                   -- Scheme type name
        s.[_order],
        s.[_collection_type],           -- Collection type (Array/Dictionary/NULL)
        ct.[_name],                     -- Collection type name
        s.[_key_type],                  -- Key type for Dictionary
        kt.[_name],                     -- Key type name
        s.[_readonly],
        s.[_allow_not_null],
        s.[_is_compress],
        s.[_store_null],
        s.[_default_value],
        s.[_default_editor]
    FROM [dbo].[_structures] s
    INNER JOIN [dbo].[_types] t ON t.[_id] = s.[_id_type]
    INNER JOIN [dbo].[_schemes] sch ON sch.[_id] = s.[_id_scheme]
    LEFT JOIN [dbo].[_types] scht ON scht.[_id] = sch.[_type]         -- Scheme type
    LEFT JOIN [dbo].[_types] ct ON ct.[_id] = s.[_collection_type]    -- Collection type
    LEFT JOIN [dbo].[_types] kt ON kt.[_id] = s.[_key_type]           -- Key type
    WHERE s.[_id_scheme] = @target_scheme_id;
END
GO

-- =====================================================
-- 4. TRIGGER: Sync cache on _structure_hash change
-- =====================================================

CREATE TRIGGER [dbo].[TR__schemes__sync_metadata_cache]
ON [dbo].[_schemes]
AFTER INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Handle UPDATE: rebuild cache if _structure_hash changed
    IF EXISTS (SELECT 1 FROM deleted)
    BEGIN
        DECLARE @scheme_id BIGINT;
        
        DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
            SELECT i.[_id]
            FROM inserted i
            INNER JOIN deleted d ON i.[_id] = d.[_id]
            WHERE i.[_structure_hash] IS NOT NULL 
              AND (d.[_structure_hash] IS NULL OR i.[_structure_hash] <> d.[_structure_hash]);
        
        OPEN cur;
        FETCH NEXT FROM cur INTO @scheme_id;
        
        WHILE @@FETCH_STATUS = 0
        BEGIN
            EXEC [dbo].[sync_metadata_cache_for_scheme] @scheme_id;
            PRINT 'Metadata cache rebuilt for scheme_id=' + CAST(@scheme_id AS NVARCHAR(20));
            FETCH NEXT FROM cur INTO @scheme_id;
        END
        
        CLOSE cur;
        DEALLOCATE cur;
    END
    ELSE
    BEGIN
        -- Handle INSERT: create cache for new schemes with hash
        DECLARE cur_insert CURSOR LOCAL FAST_FORWARD FOR
            SELECT [_id] FROM inserted WHERE [_structure_hash] IS NOT NULL;
        
        OPEN cur_insert;
        FETCH NEXT FROM cur_insert INTO @scheme_id;
        
        WHILE @@FETCH_STATUS = 0
        BEGIN
            EXEC [dbo].[sync_metadata_cache_for_scheme] @scheme_id;
            PRINT 'Metadata cache created for new scheme_id=' + CAST(@scheme_id AS NVARCHAR(20));
            FETCH NEXT FROM cur_insert INTO @scheme_id;
        END
        
        CLOSE cur_insert;
        DEALLOCATE cur_insert;
    END
END
GO

-- =====================================================
-- 5. TRIGGER: Cleanup cache on scheme delete
-- =====================================================

CREATE TRIGGER [dbo].[TR__schemes__cleanup_metadata_cache]
ON [dbo].[_schemes]
AFTER DELETE
AS
BEGIN
    SET NOCOUNT ON;
    
    DELETE c FROM [dbo].[_scheme_metadata_cache] c
    INNER JOIN deleted d ON c.[_scheme_id] = d.[_id];
    
    IF @@ROWCOUNT > 0
        PRINT 'Metadata cache cleared for deleted schemes';
END
GO

-- =====================================================
-- 6. TRIGGER: Invalidate all caches on _types change
-- =====================================================

CREATE TRIGGER [dbo].[TR__types__invalidate_metadata_cache]
ON [dbo].[_types]
AFTER UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    
    TRUNCATE TABLE [dbo].[_scheme_metadata_cache];
    PRINT 'All metadata caches invalidated due to _types change';
END
GO

-- =====================================================
-- 7. WARMUP PROCEDURE (for app start or after crash)
-- =====================================================

CREATE PROCEDURE [dbo].[warmup_all_metadata_caches]
AS
BEGIN
    SET NOCOUNT ON;
    
    TRUNCATE TABLE [dbo].[_scheme_metadata_cache];
    
    -- Rebuild cache for ALL schemes
    DECLARE @scheme_id BIGINT;
    
    DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
        SELECT [_id] FROM [dbo].[_schemes];
    
    OPEN cur;
    FETCH NEXT FROM cur INTO @scheme_id;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        EXEC [dbo].[sync_metadata_cache_for_scheme] @scheme_id;
        FETCH NEXT FROM cur INTO @scheme_id;
    END
    
    CLOSE cur;
    DEALLOCATE cur;
    
    -- Return statistics
    SELECT 
        s.[_id] AS scheme_id,
        COUNT(c.[_structure_id]) AS structures_count,
        s.[_name] AS scheme_name,
        s.[_structure_hash] AS structure_hash
    FROM [dbo].[_schemes] s
    LEFT JOIN [dbo].[_scheme_metadata_cache] c ON c.[_scheme_id] = s.[_id]
    GROUP BY s.[_id], s.[_name], s.[_structure_hash]
    ORDER BY s.[_id];
END
GO

-- =====================================================
-- 8. CONSISTENCY CHECK PROCEDURE
-- =====================================================

CREATE PROCEDURE [dbo].[check_metadata_cache_consistency]
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT 
        s.[_id] AS scheme_id,
        s.[_name] AS scheme_name,
        ISNULL(cache_stats.cached_count, 0) AS cached_count,
        ISNULL(actual_stats.actual_count, 0) AS actual_count,
        CASE 
            WHEN ISNULL(cache_stats.cached_count, 0) = ISNULL(actual_stats.actual_count, 0) 
            THEN CAST(1 AS BIT) 
            ELSE CAST(0 AS BIT) 
        END AS is_consistent
    FROM [dbo].[_schemes] s
    LEFT JOIN (
        SELECT [_scheme_id], COUNT(*) AS cached_count
        FROM [dbo].[_scheme_metadata_cache]
        GROUP BY [_scheme_id]
    ) cache_stats ON cache_stats.[_scheme_id] = s.[_id]
    LEFT JOIN (
        SELECT [_id_scheme], COUNT(*) AS actual_count
        FROM [dbo].[_structures]
        GROUP BY [_id_scheme]
    ) actual_stats ON actual_stats.[_id_scheme] = s.[_id]
    ORDER BY s.[_id];
END
GO

-- =====================================================
-- DONE! Usage:
-- 
-- INSTEAD OF:
--   FROM _structures s 
--   JOIN _types t ON t._id = s._id_type
--   WHERE s._id_scheme = @object_scheme_id
--
-- USE:
--   FROM _scheme_metadata_cache c
--   WHERE c._scheme_id = @object_scheme_id
-- 
-- AVAILABLE FIELDS:
--   _scheme_id, _structure_id, _parent_structure_id, _id_override
--   _name, _alias
--   _type_id, _list_id, type_name, db_type, type_semantic
--   _scheme_type, scheme_type_name (scheme type: Class/Array/Dictionary/JsonDocument/XDocument)
--   _order, _collection_type, collection_type_name (collection type: Array/Dictionary/NULL)
--   _key_type, key_type_name (key type for Dictionary)
--   _readonly, _allow_not_null, _is_compress, _store_null
--   _default_value, _default_editor
--
-- COLLECTION CHECK (instead of _is_array):
--   _collection_type IS NOT NULL = this is a collection (array or dictionary)
--   _collection_type IS NULL = not a collection
--
-- WARMUP:
--   EXEC warmup_all_metadata_caches
--
-- CONSISTENCY CHECK:
--   EXEC check_metadata_cache_consistency
-- =====================================================


PRINT '========================================='
PRINT 'Metadata cache for MS SQL Server created!'
PRINT 'Run: EXEC warmup_all_metadata_caches'
PRINT '========================================='
GO



-- ===== redb_window.sql =====
-- =====================================================
-- REDB EAV WINDOW FUNCTIONS (MSSQL)
-- Window functions for EAV model
-- ROW_NUMBER, RANK, SUM OVER, etc.
-- Ported from PostgreSQL version
-- =====================================================

SET NOCOUNT ON;
GO

-- ===== DROP EXISTING OBJECTS =====
IF OBJECT_ID('dbo.query_with_window', 'P') IS NOT NULL 
    DROP PROCEDURE dbo.query_with_window;
GO

IF OBJECT_ID('dbo.resolve_field_path', 'P') IS NOT NULL 
    DROP PROCEDURE dbo.resolve_field_path;
GO

-- =====================================================
-- Helper: resolve_field_path
-- Resolves field path to structure info
-- Supports: simple fields, nested paths (Address1.City),
-- dictionaries (PhoneBook[home]), nested dict (AddressBook[home].City)
-- =====================================================
CREATE PROCEDURE dbo.resolve_field_path
    @scheme_id BIGINT,
    @field_path NVARCHAR(255),
    @structure_id BIGINT OUTPUT,
    @db_type NVARCHAR(50) OUTPUT,
    @dict_key NVARCHAR(255) OUTPUT,
    @array_index NVARCHAR(50) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    SET @structure_id = NULL;
    SET @db_type = NULL;
    SET @dict_key = NULL;
    SET @array_index = NULL;
    
    DECLARE @remaining_path NVARCHAR(255) = @field_path;
    DECLARE @current_segment NVARCHAR(255);
    DECLARE @parent_structure_id BIGINT = NULL;
    DECLARE @dot_pos INT;
    DECLARE @bracket_pos INT;
    DECLARE @key_end INT;
    DECLARE @segment_name NVARCHAR(255);
    DECLARE @key_value NVARCHAR(255);
    
    -- Process path segments (split by dots, respecting brackets)
    WHILE @remaining_path IS NOT NULL AND LEN(@remaining_path) > 0
    BEGIN
        -- Find next segment: either up to dot or end of string
        -- But dots inside [] should be ignored
        SET @bracket_pos = CHARINDEX(N'[', @remaining_path);
        SET @dot_pos = CHARINDEX(N'.', @remaining_path);
        
        -- If bracket comes before dot, find dot after bracket end
        IF @bracket_pos > 0 AND (@dot_pos = 0 OR @bracket_pos < @dot_pos)
        BEGIN
            SET @key_end = CHARINDEX(N']', @remaining_path);
            -- Find dot after bracket end
            IF @key_end > 0
                SET @dot_pos = CHARINDEX(N'.', @remaining_path, @key_end);
        END
        
        IF @dot_pos > 0
        BEGIN
            SET @current_segment = LEFT(@remaining_path, @dot_pos - 1);
            SET @remaining_path = SUBSTRING(@remaining_path, @dot_pos + 1, LEN(@remaining_path));
        END
        ELSE
        BEGIN
            SET @current_segment = @remaining_path;
            SET @remaining_path = NULL;
        END
        
        -- Parse current segment for bracket notation
        SET @bracket_pos = CHARINDEX(N'[', @current_segment);
        IF @bracket_pos > 0
        BEGIN
            SET @key_end = CHARINDEX(N']', @current_segment);
            SET @segment_name = LEFT(@current_segment, @bracket_pos - 1);
            SET @key_value = SUBSTRING(@current_segment, @bracket_pos + 1, @key_end - @bracket_pos - 1);
            
            -- Save key when we find bracket notation (for dict/array access)
            -- This applies even for intermediate segments like AddressBook[home].City
            IF @key_value <> N'' AND @key_value NOT LIKE N'%[^0-9]%'
                SET @array_index = @key_value;
            ELSE IF @key_value <> N''
                SET @dict_key = @key_value;
        END
        ELSE
        BEGIN
            SET @segment_name = @current_segment;
        END
        
        -- Find structure for this segment
        IF @parent_structure_id IS NULL
        BEGIN
            -- Top-level field
            SELECT TOP 1
                @structure_id = c._structure_id,
                @db_type = c.db_type
            FROM _scheme_metadata_cache c
            WHERE c._scheme_id = @scheme_id
              AND c._name = @segment_name
              AND c._parent_structure_id IS NULL;
        END
        ELSE
        BEGIN
            -- Nested field
            SELECT TOP 1
                @structure_id = c._structure_id,
                @db_type = c.db_type
            FROM _scheme_metadata_cache c
            WHERE c._scheme_id = @scheme_id
              AND c._name = @segment_name
              AND c._parent_structure_id = @parent_structure_id;
        END
        
        -- If not found, try to find by full path (for backward compatibility)
        IF @structure_id IS NULL AND @parent_structure_id IS NULL
        BEGIN
            SELECT TOP 1
                @structure_id = c._structure_id,
                @db_type = c.db_type
            FROM _scheme_metadata_cache c
            WHERE c._scheme_id = @scheme_id
              AND c._name = @field_path
              AND c._parent_structure_id IS NULL;
            
            IF @structure_id IS NOT NULL
                RETURN; -- Found by full path, exit
        END
        
        IF @structure_id IS NULL
            RETURN; -- Segment not found
        
        -- Move to next level
        SET @parent_structure_id = @structure_id;
    END
END;
GO

-- =====================================================
-- query_with_window: Query with window functions
-- =====================================================
-- Parameters:
--   @scheme_id      - Scheme ID
--   @select_fields  - JSON array of SELECT fields:
--                     [{"field":"Name","alias":"Name"}]
--   @window_funcs   - JSON array of window functions:
--                     [{"func":"ROW_NUMBER","alias":"Rank"}]
--   @partition_by   - JSON array for PARTITION BY:
--                     [{"field":"Category"}]
--   @order_by       - JSON array for ORDER BY inside window:
--                     [{"field":"Stock","dir":"DESC"}]
--   @filter_json    - JSON filter (optional)
--   @limit          - Record limit
--   @frame_json     - Frame specification (optional)
--
-- Returns: JSON array of objects with window functions
-- =====================================================
CREATE PROCEDURE dbo.query_with_window
    @scheme_id BIGINT,
    @select_fields NVARCHAR(MAX),
    @window_funcs NVARCHAR(MAX),
    @partition_by NVARCHAR(MAX) = N'[]',
    @order_by NVARCHAR(MAX) = N'[]',
    @filter_json NVARCHAR(MAX) = NULL,
    @limit INT = 1000,
    @frame_json NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @select_parts NVARCHAR(MAX) = N'';
    DECLARE @join_parts NVARCHAR(MAX) = N'';
    DECLARE @partition_parts NVARCHAR(MAX) = N'';
    DECLARE @order_parts NVARCHAR(MAX) = N'';
    DECLARE @over_clause NVARCHAR(MAX);
    DECLARE @frame_clause NVARCHAR(500) = N'';
    DECLARE @where_clause NVARCHAR(MAX) = N'';
    
    DECLARE @join_idx INT = 0;
    DECLARE @join_alias NVARCHAR(10);
    DECLARE @column_name NVARCHAR(50);
    
    DECLARE @field_path NVARCHAR(255);
    DECLARE @alias NVARCHAR(255);
    DECLARE @func_name NVARCHAR(50);
    DECLARE @dir NVARCHAR(10);
    DECLARE @buckets INT;
    
    DECLARE @structure_id BIGINT;
    DECLARE @db_type NVARCHAR(50);
    DECLARE @dict_key NVARCHAR(255);
    DECLARE @array_index NVARCHAR(50);
    
    DECLARE @sql NVARCHAR(MAX);
    DECLARE @result NVARCHAR(MAX);
    
    -- =========================================
    -- 1. Base SELECT fields
    -- =========================================
    SET @select_parts = N'o._id AS [id], o._name AS [name]';
    
    -- Process select fields from JSON
    DECLARE @field_cursor CURSOR;
    SET @field_cursor = CURSOR FOR
        SELECT 
            JSON_VALUE(value, '$.field') AS field_path,
            ISNULL(JSON_VALUE(value, '$.alias'), JSON_VALUE(value, '$.field')) AS alias
        FROM OPENJSON(@select_fields);
    
    OPEN @field_cursor;
    FETCH NEXT FROM @field_cursor INTO @field_path, @alias;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @field_path IS NOT NULL
        BEGIN
            -- Check for base field (0$: prefix)
            IF @field_path LIKE N'0$:%'
            BEGIN
                DECLARE @raw_field NVARCHAR(100) = SUBSTRING(@field_path, 4, 100);
                DECLARE @sql_column NVARCHAR(100) = dbo.normalize_base_field_name(@raw_field);
                
                IF @sql_column IS NOT NULL
                    SET @select_parts = @select_parts + N', o.' + @sql_column + N' AS [' + @alias + N']';
            END
            ELSE
            BEGIN
                -- EAV field from _values
                EXEC resolve_field_path @scheme_id, @field_path, 
                    @structure_id OUTPUT, @db_type OUTPUT, @dict_key OUTPUT, @array_index OUTPUT;
                
                IF @structure_id IS NOT NULL
                BEGIN
                    SET @join_idx = @join_idx + 1;
                    SET @join_alias = N's' + CAST(@join_idx AS NVARCHAR(10));
                    
                    SET @column_name = CASE @db_type
                        WHEN N'Long' THEN N'_Long'
                        WHEN N'String' THEN N'_String'
                        WHEN N'Double' THEN N'_Double'
                        WHEN N'Numeric' THEN N'_Numeric'
                        WHEN N'DateTimeOffset' THEN N'_DateTimeOffset'
                        WHEN N'Boolean' THEN N'_Boolean'
                        ELSE N'_String'
                    END;
                    
                    SET @select_parts = @select_parts + N', ' + @join_alias + N'.' + @column_name + N' AS [' + @alias + N']';
                    
                    -- Build JOIN
                    IF @dict_key IS NOT NULL
                        SET @join_parts = @join_parts + N' LEFT JOIN _values ' + @join_alias + 
                            N' ON ' + @join_alias + N'._id_object = o._id AND ' + 
                            @join_alias + N'._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) +
                            N' AND ' + @join_alias + N'._array_index = N''' + @dict_key + N'''';
                    ELSE IF @array_index IS NOT NULL
                        SET @join_parts = @join_parts + N' LEFT JOIN _values ' + @join_alias + 
                            N' ON ' + @join_alias + N'._id_object = o._id AND ' + 
                            @join_alias + N'._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) +
                            N' AND ' + @join_alias + N'._array_index = N''' + @array_index + N'''';
                    ELSE
                        SET @join_parts = @join_parts + N' LEFT JOIN _values ' + @join_alias + 
                            N' ON ' + @join_alias + N'._id_object = o._id AND ' + 
                            @join_alias + N'._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) +
                            N' AND ' + @join_alias + N'._array_index IS NULL';
                END
            END
        END
        
        FETCH NEXT FROM @field_cursor INTO @field_path, @alias;
    END
    
    CLOSE @field_cursor;
    DEALLOCATE @field_cursor;
    
    -- =========================================
    -- 2. PARTITION BY
    -- =========================================
    SET @field_cursor = CURSOR FOR
        SELECT JSON_VALUE(value, '$.field') AS field_path
        FROM OPENJSON(@partition_by);
    
    OPEN @field_cursor;
    FETCH NEXT FROM @field_cursor INTO @field_path;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @field_path IS NOT NULL
        BEGIN
            IF @field_path LIKE N'0$:%'
            BEGIN
                SET @raw_field = SUBSTRING(@field_path, 4, 100);
                SET @sql_column = dbo.normalize_base_field_name(@raw_field);
                
                IF @sql_column IS NOT NULL
                BEGIN
                    IF @partition_parts <> N'' SET @partition_parts = @partition_parts + N', ';
                    SET @partition_parts = @partition_parts + N'o.' + @sql_column;
                END
            END
            ELSE
            BEGIN
                EXEC resolve_field_path @scheme_id, @field_path, 
                    @structure_id OUTPUT, @db_type OUTPUT, @dict_key OUTPUT, @array_index OUTPUT;
                
                IF @structure_id IS NOT NULL
                BEGIN
                    SET @join_idx = @join_idx + 1;
                    SET @join_alias = N'p' + CAST(@join_idx AS NVARCHAR(10));
                    
                    SET @column_name = CASE @db_type
                        WHEN N'Long' THEN N'_Long'
                        WHEN N'String' THEN N'_String'
                        WHEN N'Double' THEN N'_Double'
                        WHEN N'Numeric' THEN N'_Numeric'
                        WHEN N'DateTimeOffset' THEN N'_DateTimeOffset'
                        WHEN N'Boolean' THEN N'_Boolean'
                        ELSE N'_String'
                    END;
                    
                    IF @partition_parts <> N'' SET @partition_parts = @partition_parts + N', ';
                    SET @partition_parts = @partition_parts + @join_alias + N'.' + @column_name;
                    
                    -- Build JOIN
                    IF @dict_key IS NOT NULL
                        SET @join_parts = @join_parts + N' LEFT JOIN _values ' + @join_alias + 
                            N' ON ' + @join_alias + N'._id_object = o._id AND ' + 
                            @join_alias + N'._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) +
                            N' AND ' + @join_alias + N'._array_index = N''' + @dict_key + N'''';
                    ELSE IF @array_index IS NOT NULL
                        SET @join_parts = @join_parts + N' LEFT JOIN _values ' + @join_alias + 
                            N' ON ' + @join_alias + N'._id_object = o._id AND ' + 
                            @join_alias + N'._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) +
                            N' AND ' + @join_alias + N'._array_index = N''' + @array_index + N'''';
                    ELSE
                        SET @join_parts = @join_parts + N' LEFT JOIN _values ' + @join_alias + 
                            N' ON ' + @join_alias + N'._id_object = o._id AND ' + 
                            @join_alias + N'._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) +
                            N' AND ' + @join_alias + N'._array_index IS NULL';
                END
            END
        END
        
        FETCH NEXT FROM @field_cursor INTO @field_path;
    END
    
    CLOSE @field_cursor;
    DEALLOCATE @field_cursor;
    
    -- =========================================
    -- 3. ORDER BY inside window
    -- =========================================
    SET @field_cursor = CURSOR FOR
        SELECT 
            JSON_VALUE(value, '$.field') AS field_path,
            ISNULL(UPPER(JSON_VALUE(value, '$.dir')), N'ASC') AS dir
        FROM OPENJSON(@order_by);
    
    OPEN @field_cursor;
    FETCH NEXT FROM @field_cursor INTO @field_path, @dir;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @field_path IS NOT NULL
        BEGIN
            IF @field_path LIKE N'0$:%'
            BEGIN
                SET @raw_field = SUBSTRING(@field_path, 4, 100);
                SET @sql_column = dbo.normalize_base_field_name(@raw_field);
                
                IF @sql_column IS NOT NULL
                BEGIN
                    IF @order_parts <> N'' SET @order_parts = @order_parts + N', ';
                    SET @order_parts = @order_parts + N'o.' + @sql_column + N' ' + @dir;
                END
            END
            ELSE
            BEGIN
                EXEC resolve_field_path @scheme_id, @field_path, 
                    @structure_id OUTPUT, @db_type OUTPUT, @dict_key OUTPUT, @array_index OUTPUT;
                
                IF @structure_id IS NOT NULL
                BEGIN
                    SET @join_idx = @join_idx + 1;
                    SET @join_alias = N'w' + CAST(@join_idx AS NVARCHAR(10));
                    
                    SET @column_name = CASE @db_type
                        WHEN N'Long' THEN N'_Long'
                        WHEN N'Double' THEN N'_Double'
                        WHEN N'Numeric' THEN N'_Numeric'
                        WHEN N'DateTimeOffset' THEN N'_DateTimeOffset'
                        WHEN N'Boolean' THEN N'_Boolean'
                        ELSE N'_String'
                    END;
                    
                    IF @order_parts <> N'' SET @order_parts = @order_parts + N', ';
                    SET @order_parts = @order_parts + @join_alias + N'.' + @column_name + N' ' + @dir;
                    
                    -- Build JOIN
                    IF @dict_key IS NOT NULL
                        SET @join_parts = @join_parts + N' LEFT JOIN _values ' + @join_alias + 
                            N' ON ' + @join_alias + N'._id_object = o._id AND ' + 
                            @join_alias + N'._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) +
                            N' AND ' + @join_alias + N'._array_index = N''' + @dict_key + N'''';
                    ELSE IF @array_index IS NOT NULL
                        SET @join_parts = @join_parts + N' LEFT JOIN _values ' + @join_alias + 
                            N' ON ' + @join_alias + N'._id_object = o._id AND ' + 
                            @join_alias + N'._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) +
                            N' AND ' + @join_alias + N'._array_index = N''' + @array_index + N'''';
                    ELSE
                        SET @join_parts = @join_parts + N' LEFT JOIN _values ' + @join_alias + 
                            N' ON ' + @join_alias + N'._id_object = o._id AND ' + 
                            @join_alias + N'._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) +
                            N' AND ' + @join_alias + N'._array_index IS NULL';
                END
            END
        END
        
        FETCH NEXT FROM @field_cursor INTO @field_path, @dir;
    END
    
    CLOSE @field_cursor;
    DEALLOCATE @field_cursor;
    
    -- =========================================
    -- 4. Build OVER clause (with ROWS BETWEEN support)
    -- =========================================
    IF @frame_json IS NOT NULL AND @frame_json <> N'' AND @frame_json <> N'null'
    BEGIN
        DECLARE @frame_type NVARCHAR(10) = ISNULL(UPPER(JSON_VALUE(@frame_json, '$.type')), N'ROWS');
        DECLARE @start_kind NVARCHAR(50) = UPPER(JSON_VALUE(@frame_json, '$.start.kind'));
        DECLARE @start_offset INT = ISNULL(CAST(JSON_VALUE(@frame_json, '$.start.offset') AS INT), 0);
        DECLARE @end_kind NVARCHAR(50) = UPPER(JSON_VALUE(@frame_json, '$.end.kind'));
        DECLARE @end_offset INT = ISNULL(CAST(JSON_VALUE(@frame_json, '$.end.offset') AS INT), 0);
        
        SET @frame_clause = @frame_type + N' BETWEEN ';
        
        -- Start bound
        SET @frame_clause = @frame_clause + CASE @start_kind
            WHEN N'UNBOUNDEDPRECEDING' THEN N'UNBOUNDED PRECEDING'
            WHEN N'CURRENTROW' THEN N'CURRENT ROW'
            WHEN N'PRECEDING' THEN CAST(@start_offset AS NVARCHAR(10)) + N' PRECEDING'
            WHEN N'FOLLOWING' THEN CAST(@start_offset AS NVARCHAR(10)) + N' FOLLOWING'
            ELSE N'UNBOUNDED PRECEDING'
        END;
        
        SET @frame_clause = @frame_clause + N' AND ';
        
        -- End bound
        SET @frame_clause = @frame_clause + CASE @end_kind
            WHEN N'UNBOUNDEDFOLLOWING' THEN N'UNBOUNDED FOLLOWING'
            WHEN N'CURRENTROW' THEN N'CURRENT ROW'
            WHEN N'PRECEDING' THEN CAST(@end_offset AS NVARCHAR(10)) + N' PRECEDING'
            WHEN N'FOLLOWING' THEN CAST(@end_offset AS NVARCHAR(10)) + N' FOLLOWING'
            ELSE N'CURRENT ROW'
        END;
    END
    
    -- Build OVER clause
    SET @over_clause = N'OVER (';
    IF @partition_parts <> N''
        SET @over_clause = @over_clause + N'PARTITION BY ' + @partition_parts;
    IF @order_parts <> N''
    BEGIN
        IF @partition_parts <> N'' SET @over_clause = @over_clause + N' ';
        SET @over_clause = @over_clause + N'ORDER BY ' + @order_parts;
    END
    IF @frame_clause <> N''
    BEGIN
        IF @partition_parts <> N'' OR @order_parts <> N'' SET @over_clause = @over_clause + N' ';
        SET @over_clause = @over_clause + @frame_clause;
    END
    SET @over_clause = @over_clause + N')';
    
    -- =========================================
    -- 5. Window functions
    -- =========================================
    SET @field_cursor = CURSOR FOR
        SELECT 
            UPPER(JSON_VALUE(value, '$.func')) AS func_name,
            ISNULL(JSON_VALUE(value, '$.alias'), UPPER(JSON_VALUE(value, '$.func'))) AS alias,
            JSON_VALUE(value, '$.field') AS field_path,
            ISNULL(CAST(JSON_VALUE(value, '$.buckets') AS INT), 4) AS buckets
        FROM OPENJSON(@window_funcs);
    
    OPEN @field_cursor;
    FETCH NEXT FROM @field_cursor INTO @func_name, @alias, @field_path, @buckets;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @func_name IN (N'ROW_NUMBER', N'RANK', N'DENSE_RANK', N'COUNT')
        BEGIN
            -- Ranking functions (no field)
            IF @func_name = N'COUNT'
                SET @select_parts = @select_parts + N', COUNT(*) ' + @over_clause + N' AS [' + @alias + N']';
            ELSE
                SET @select_parts = @select_parts + N', ' + @func_name + N'() ' + @over_clause + N' AS [' + @alias + N']';
        END
        ELSE IF @func_name = N'NTILE'
        BEGIN
            SET @select_parts = @select_parts + N', NTILE(' + CAST(@buckets AS NVARCHAR(10)) + N') ' + @over_clause + N' AS [' + @alias + N']';
        END
        ELSE IF @func_name IN (N'SUM', N'AVG', N'MIN', N'MAX', N'LAG', N'LEAD', N'FIRST_VALUE', N'LAST_VALUE')
        BEGIN
            IF @field_path IS NOT NULL AND @field_path <> N''
            BEGIN
                EXEC resolve_field_path @scheme_id, @field_path, 
                    @structure_id OUTPUT, @db_type OUTPUT, @dict_key OUTPUT, @array_index OUTPUT;
                
                IF @structure_id IS NOT NULL
                BEGIN
                    SET @join_idx = @join_idx + 1;
                    SET @join_alias = N'f' + CAST(@join_idx AS NVARCHAR(10));
                    
                    SET @column_name = CASE @db_type
                        WHEN N'Long' THEN N'_Long'
                        WHEN N'Double' THEN N'_Double'
                        WHEN N'Numeric' THEN N'_Numeric'
                        WHEN N'String' THEN N'_String'
                        WHEN N'DateTimeOffset' THEN N'_DateTimeOffset'
                        WHEN N'Boolean' THEN N'_Boolean'
                        ELSE N'_Long'
                    END;
                    
                    -- Build JOIN
                    IF @dict_key IS NOT NULL
                        SET @join_parts = @join_parts + N' LEFT JOIN _values ' + @join_alias + 
                            N' ON ' + @join_alias + N'._id_object = o._id AND ' + 
                            @join_alias + N'._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) +
                            N' AND ' + @join_alias + N'._array_index = N''' + @dict_key + N'''';
                    ELSE IF @array_index IS NOT NULL
                        SET @join_parts = @join_parts + N' LEFT JOIN _values ' + @join_alias + 
                            N' ON ' + @join_alias + N'._id_object = o._id AND ' + 
                            @join_alias + N'._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) +
                            N' AND ' + @join_alias + N'._array_index = N''' + @array_index + N'''';
                    ELSE
                        SET @join_parts = @join_parts + N' LEFT JOIN _values ' + @join_alias + 
                            N' ON ' + @join_alias + N'._id_object = o._id AND ' + 
                            @join_alias + N'._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) +
                            N' AND ' + @join_alias + N'._array_index IS NULL';
                    
                    SET @select_parts = @select_parts + N', ' + @func_name + N'(' + @join_alias + N'.' + @column_name + N') ' + @over_clause + N' AS [' + @alias + N']';
                END
            END
        END
        
        FETCH NEXT FROM @field_cursor INTO @func_name, @alias, @field_path, @buckets;
    END
    
    CLOSE @field_cursor;
    DEALLOCATE @field_cursor;
    
    -- =========================================
    -- 6. Filter
    -- =========================================
    IF @filter_json IS NOT NULL AND @filter_json <> N'' AND @filter_json <> N'null'
    BEGIN
        -- Use internal_parse_filters to get WHERE clause
        DECLARE @filter_result NVARCHAR(MAX);
        EXEC internal_parse_filters @scheme_id, @filter_json, N'o', N'AND', 0, @filter_result OUTPUT;
        
        IF @filter_result IS NOT NULL AND @filter_result <> N''
            SET @where_clause = N' AND ' + @filter_result;
    END
    
    -- =========================================
    -- 7. Build and execute SQL
    -- =========================================
    SET @sql = N'
        SELECT @result = (
            SELECT ' + @select_parts + N'
            FROM _objects o' + @join_parts + N'
            WHERE o._id_scheme = ' + CAST(@scheme_id AS NVARCHAR(20));
    
    IF @where_clause IS NOT NULL AND @where_clause <> N''
        SET @sql = @sql + @where_clause;
    
    SET @sql = @sql + N'
            ORDER BY o._id
            OFFSET 0 ROWS FETCH NEXT ' + CAST(@limit AS NVARCHAR(20)) + N' ROWS ONLY
            FOR JSON PATH
        )';
    
    EXEC sp_executesql @sql, N'@result NVARCHAR(MAX) OUTPUT', @result = @result OUTPUT;
    
    SELECT ISNULL(@result, N'[]') AS result;
END;
GO

PRINT N'=========================================';
PRINT N'Window Functions procedures created!';
PRINT N'';
PRINT N'PROCEDURES:';
PRINT N'  query_with_window - Query with window functions';
PRINT N'  resolve_field_path - Helper to resolve field paths';
PRINT N'';
PRINT N'SUPPORTED FUNCTIONS:';
PRINT N'  Ranking: ROW_NUMBER, RANK, DENSE_RANK, NTILE';
PRINT N'  Aggregate: SUM, AVG, MIN, MAX, COUNT';
PRINT N'  Offset: LAG, LEAD, FIRST_VALUE, LAST_VALUE';
PRINT N'';
PRINT N'FEATURES:';
PRINT N'  - Base fields with 0$: prefix';
PRINT N'  - EAV fields from _values';
PRINT N'  - Dictionary support: PhoneBook[home]';
PRINT N'  - Array index support: Items[2]';
PRINT N'  - ROWS BETWEEN frame support';
PRINT N'=========================================';
GO



-- ===== migrate_structure_type.sql =====
-- ============================================================
-- MIGRATE STRUCTURE TYPE
-- MS SQL Server version
-- Atomic data migration when changing structure type
-- ============================================================


-- =====================================================
-- DROP EXISTING OBJECTS
-- =====================================================

IF OBJECT_ID('dbo.get_value_column', 'FN') IS NOT NULL
    DROP FUNCTION dbo.get_value_column;
GO

IF OBJECT_ID('dbo.migrate_structure_type', 'P') IS NOT NULL
    DROP PROCEDURE dbo.migrate_structure_type;
GO

-- =====================================================
-- HELPER FUNCTION: get_value_column
-- Returns column name for type
-- =====================================================
CREATE FUNCTION dbo.get_value_column(@type_name NVARCHAR(100))
RETURNS NVARCHAR(50)
WITH SCHEMABINDING
AS
BEGIN
    RETURN CASE LOWER(@type_name)
        -- String types (base + derived)
        WHEN 'string' THEN '_String'
        WHEN 'text' THEN '_String'
        WHEN 'mimetype' THEN '_String'
        WHEN 'filepath' THEN '_String'
        WHEN 'filename' THEN '_String'
        -- Long types
        WHEN 'long' THEN '_Long'
        WHEN 'int' THEN '_Long'
        WHEN 'int32' THEN '_Long'
        WHEN 'int64' THEN '_Long'
        WHEN 'short' THEN '_Long'
        WHEN 'byte' THEN '_Long'
        WHEN 'timespan' THEN '_Long'
        -- Double types
        WHEN 'double' THEN '_Double'
        WHEN 'float' THEN '_Double'
        -- Numeric types
        WHEN 'numeric' THEN '_Numeric'
        WHEN 'decimal' THEN '_Numeric'
        -- Boolean types
        WHEN 'boolean' THEN '_Boolean'
        WHEN 'bool' THEN '_Boolean'
        -- DateTime types
        WHEN 'datetimeoffset' THEN '_DateTimeOffset'
        WHEN 'datetime' THEN '_DateTimeOffset'
        WHEN 'dateonly' THEN '_DateTimeOffset'
        WHEN 'timeonly' THEN '_DateTimeOffset'
        -- Guid types
        WHEN 'guid' THEN '_Guid'
        WHEN 'uuid' THEN '_Guid'
        -- Binary types
        WHEN 'bytearray' THEN '_ByteArray'
        WHEN 'bytes' THEN '_ByteArray'
        -- Object/List types
        WHEN 'object' THEN '_Object'
        WHEN 'listitem' THEN '_ListItem'
        ELSE NULL
    END;
END;
GO

-- =====================================================
-- MAIN PROCEDURE: migrate_structure_type
-- Migrates data when changing structure type
-- =====================================================
CREATE PROCEDURE dbo.migrate_structure_type
    @p0 BIGINT,              -- structure_id
    @p1 NVARCHAR(100),       -- old_type_name
    @p2 NVARCHAR(100),       -- new_type_name
    @p3 BIT = 0              -- dry_run
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @v_source_col NVARCHAR(50);
    DECLARE @v_target_col NVARCHAR(50);
    DECLARE @v_affected_rows INT = 0;
    DECLARE @v_success_count INT = 0;
    DECLARE @v_has_collision BIT = 0;
    DECLARE @v_conversion_sql NVARCHAR(MAX);
    DECLARE @v_count_sql NVARCHAR(MAX);
    DECLARE @v_collision_sql NVARCHAR(MAX);
    
    -- Get column names
    SET @v_source_col = dbo.get_value_column(@p1);
    SET @v_target_col = dbo.get_value_column(@p2);
    
    -- Validate source type
    IF @v_source_col IS NULL
    BEGIN
        SELECT 0 AS affected_rows, 0 AS success_count, 0 AS error_count, 
               N'Unknown source type: ' + @p1 AS errors;
        RETURN;
    END;
    
    -- Validate target type
    IF @v_target_col IS NULL
    BEGIN
        SELECT 0 AS affected_rows, 0 AS success_count, 0 AS error_count, 
               N'Unknown target type: ' + @p2 AS errors;
        RETURN;
    END;
    
    -- Same columns - no migration needed (e.g. Int->Long both in _Long)
    IF @v_source_col = @v_target_col
    BEGIN
        SELECT 0 AS affected_rows, 0 AS success_count, 0 AS error_count, 
               CAST(NULL AS NVARCHAR(MAX)) AS errors;
        RETURN;
    END;
    
    -- Check structure exists
    IF NOT EXISTS (SELECT 1 FROM _structures WHERE _id = @p0)
    BEGIN
        SELECT 0 AS affected_rows, 0 AS success_count, 0 AS error_count, 
               N'Structure ' + CAST(@p0 AS NVARCHAR(20)) + N' not found' AS errors;
        RETURN;
    END;
    
    -- Count affected rows
    SET @v_count_sql = N'SELECT @cnt = COUNT(*) FROM _values WHERE _id_structure = @sid AND ' + 
                       QUOTENAME(@v_source_col) + N' IS NOT NULL';
    EXEC sp_executesql @v_count_sql, 
        N'@sid BIGINT, @cnt INT OUTPUT', 
        @sid = @p0, @cnt = @v_affected_rows OUTPUT;
    
    -- Dry run - only count
    IF @p3 = 1
    BEGIN
        SELECT @v_affected_rows AS affected_rows, 0 AS success_count, 0 AS error_count, 
               CAST(NULL AS NVARCHAR(MAX)) AS errors;
        RETURN;
    END;
    
    -- ========================================
    -- COLLISION CHECK (key point!)
    -- If target is filled but source is empty - data already migrated manually
    -- ========================================
    SET @v_collision_sql = N'SELECT @has_collision = CASE WHEN EXISTS(
        SELECT 1 FROM _values 
        WHERE _id_structure = @sid 
          AND ' + QUOTENAME(@v_target_col) + N' IS NOT NULL
          AND ' + QUOTENAME(@v_source_col) + N' IS NULL
    ) THEN 1 ELSE 0 END';
    
    EXEC sp_executesql @v_collision_sql,
        N'@sid BIGINT, @has_collision BIT OUTPUT',
        @sid = @p0, @has_collision = @v_has_collision OUTPUT;
    
    IF @v_has_collision = 1
    BEGIN
        SELECT @v_affected_rows AS affected_rows, 0 AS success_count, @v_affected_rows AS error_count,
               N'TYPE_MIGRATION_COLLISION: Data already in ' + @v_target_col + 
               N' but _id_type = ' + @p1 + 
               N'. Fix manually: UPDATE _structures SET _id_type = (SELECT _id FROM _types WHERE _name = ''' + 
               @p2 + N''') WHERE _id = ' + CAST(@p0 AS NVARCHAR(20)) AS errors;
        RETURN;
    END;
    
    -- No data to migrate
    IF @v_affected_rows = 0
    BEGIN
        SELECT 0 AS affected_rows, 0 AS success_count, 0 AS error_count, 
               CAST(NULL AS NVARCHAR(MAX)) AS errors;
        RETURN;
    END;
    
    -- ========================================
    -- CONVERSION MATRIX
    -- ========================================
    SET @v_conversion_sql = NULL;
    
    -- STRING -> *
    IF @v_source_col = '_String'
    BEGIN
        IF @v_target_col = '_Long'
            SET @v_conversion_sql = N'UPDATE _values SET _Long = TRY_CAST(_String AS BIGINT), _String = NULL 
                WHERE _id_structure = @sid AND _String IS NOT NULL AND TRY_CAST(_String AS BIGINT) IS NOT NULL';
        ELSE IF @v_target_col = '_Double'
            SET @v_conversion_sql = N'UPDATE _values SET [_Double] = TRY_CAST(_String AS FLOAT), _String = NULL 
                WHERE _id_structure = @sid AND _String IS NOT NULL AND TRY_CAST(_String AS FLOAT) IS NOT NULL';
        ELSE IF @v_target_col = '_Numeric'
            SET @v_conversion_sql = N'UPDATE _values SET _Numeric = TRY_CAST(_String AS DECIMAL(38,18)), _String = NULL 
                WHERE _id_structure = @sid AND _String IS NOT NULL AND TRY_CAST(_String AS DECIMAL(38,18)) IS NOT NULL';
        ELSE IF @v_target_col = '_Boolean'
            SET @v_conversion_sql = N'UPDATE _values SET _Boolean = CASE 
                WHEN LOWER(_String) IN (''true'', ''1'', ''yes'', ''t'', ''y'') THEN 1 
                WHEN LOWER(_String) IN (''false'', ''0'', ''no'', ''f'', ''n'') THEN 0 
                ELSE NULL END, _String = NULL 
                WHERE _id_structure = @sid AND _String IS NOT NULL';
        ELSE IF @v_target_col = '_DateTimeOffset'
            SET @v_conversion_sql = N'UPDATE _values SET _DateTimeOffset = TRY_CAST(_String AS DATETIMEOFFSET), _String = NULL 
                WHERE _id_structure = @sid AND _String IS NOT NULL AND TRY_CAST(_String AS DATETIMEOFFSET) IS NOT NULL';
        ELSE IF @v_target_col = '_Guid'
            SET @v_conversion_sql = N'UPDATE _values SET _Guid = TRY_CAST(_String AS UNIQUEIDENTIFIER), _String = NULL 
                WHERE _id_structure = @sid AND _String IS NOT NULL AND TRY_CAST(_String AS UNIQUEIDENTIFIER) IS NOT NULL';
    END
    
    -- LONG -> *
    ELSE IF @v_source_col = '_Long'
    BEGIN
        IF @v_target_col = '_String'
            SET @v_conversion_sql = N'UPDATE _values SET _String = CAST(_Long AS NVARCHAR(MAX)), _Long = NULL 
                WHERE _id_structure = @sid AND _Long IS NOT NULL';
        ELSE IF @v_target_col = '_Double'
            SET @v_conversion_sql = N'UPDATE _values SET [_Double] = CAST(_Long AS FLOAT), _Long = NULL 
                WHERE _id_structure = @sid AND _Long IS NOT NULL';
        ELSE IF @v_target_col = '_Numeric'
            SET @v_conversion_sql = N'UPDATE _values SET _Numeric = CAST(_Long AS DECIMAL(38,18)), _Long = NULL 
                WHERE _id_structure = @sid AND _Long IS NOT NULL';
        ELSE IF @v_target_col = '_Boolean'
            SET @v_conversion_sql = N'UPDATE _values SET _Boolean = CASE WHEN _Long != 0 THEN 1 ELSE 0 END, _Long = NULL 
                WHERE _id_structure = @sid AND _Long IS NOT NULL';
        ELSE IF @v_target_col = '_DateTimeOffset'
            SET @v_conversion_sql = N'UPDATE _values SET _DateTimeOffset = DATEADD(SECOND, _Long, ''1970-01-01''), _Long = NULL 
                WHERE _id_structure = @sid AND _Long IS NOT NULL';
    END
    
    -- DOUBLE -> *
    ELSE IF @v_source_col = '_Double'
    BEGIN
        IF @v_target_col = '_String'
            SET @v_conversion_sql = N'UPDATE _values SET _String = FORMAT([_Double], ''G'', ''en-US''), [_Double] = NULL 
                WHERE _id_structure = @sid AND [_Double] IS NOT NULL';
        ELSE IF @v_target_col = '_Long'
            SET @v_conversion_sql = N'UPDATE _values SET _Long = CAST(ROUND([_Double], 0) AS BIGINT), [_Double] = NULL 
                WHERE _id_structure = @sid AND [_Double] IS NOT NULL';
        ELSE IF @v_target_col = '_Numeric'
            SET @v_conversion_sql = N'UPDATE _values SET _Numeric = CAST([_Double] AS DECIMAL(38,18)), [_Double] = NULL 
                WHERE _id_structure = @sid AND [_Double] IS NOT NULL';
    END
    
    -- NUMERIC -> *
    ELSE IF @v_source_col = '_Numeric'
    BEGIN
        IF @v_target_col = '_String'
            SET @v_conversion_sql = N'UPDATE _values SET _String = CAST(_Numeric AS NVARCHAR(MAX)), _Numeric = NULL 
                WHERE _id_structure = @sid AND _Numeric IS NOT NULL';
        ELSE IF @v_target_col = '_Long'
            SET @v_conversion_sql = N'UPDATE _values SET _Long = CAST(ROUND(_Numeric, 0) AS BIGINT), _Numeric = NULL 
                WHERE _id_structure = @sid AND _Numeric IS NOT NULL';
        ELSE IF @v_target_col = '_Double'
            SET @v_conversion_sql = N'UPDATE _values SET [_Double] = CAST(_Numeric AS FLOAT), _Numeric = NULL 
                WHERE _id_structure = @sid AND _Numeric IS NOT NULL';
    END
    
    -- BOOLEAN -> *
    ELSE IF @v_source_col = '_Boolean'
    BEGIN
        IF @v_target_col = '_String'
            SET @v_conversion_sql = N'UPDATE _values SET _String = CASE WHEN _Boolean = 1 THEN ''true'' ELSE ''false'' END, _Boolean = NULL 
                WHERE _id_structure = @sid AND _Boolean IS NOT NULL';
        ELSE IF @v_target_col = '_Long'
            SET @v_conversion_sql = N'UPDATE _values SET _Long = CASE WHEN _Boolean = 1 THEN 1 ELSE 0 END, _Boolean = NULL 
                WHERE _id_structure = @sid AND _Boolean IS NOT NULL';
    END
    
    -- DATETIMEOFFSET -> *
    ELSE IF @v_source_col = '_DateTimeOffset'
    BEGIN
        IF @v_target_col = '_String'
            SET @v_conversion_sql = N'UPDATE _values SET _String = CONVERT(NVARCHAR(50), _DateTimeOffset, 127), _DateTimeOffset = NULL 
                WHERE _id_structure = @sid AND _DateTimeOffset IS NOT NULL';
        ELSE IF @v_target_col = '_Long'
            SET @v_conversion_sql = N'UPDATE _values SET _Long = DATEDIFF_BIG(SECOND, ''1970-01-01'', _DateTimeOffset), _DateTimeOffset = NULL 
                WHERE _id_structure = @sid AND _DateTimeOffset IS NOT NULL';
    END
    
    -- GUID -> *
    ELSE IF @v_source_col = '_Guid'
    BEGIN
        IF @v_target_col = '_String'
            SET @v_conversion_sql = N'UPDATE _values SET _String = CAST(_Guid AS NVARCHAR(50)), _Guid = NULL 
                WHERE _id_structure = @sid AND _Guid IS NOT NULL';
    END;
    
    -- Conversion not supported
    IF @v_conversion_sql IS NULL
    BEGIN
        SELECT @v_affected_rows AS affected_rows, 0 AS success_count, @v_affected_rows AS error_count,
               N'Conversion ' + @p1 + N' -> ' + @p2 + N' not supported' AS errors;
        RETURN;
    END;
    
    -- Execute migration
    EXEC sp_executesql @v_conversion_sql, N'@sid BIGINT', @sid = @p0;
    SET @v_success_count = @@ROWCOUNT;
    
    -- Return result
    SELECT @v_affected_rows AS affected_rows, 
           @v_success_count AS success_count, 
           @v_affected_rows - @v_success_count AS error_count, 
           CAST(NULL AS NVARCHAR(MAX)) AS errors;
END;
GO

PRINT 'migrate_structure_type procedure created!'
PRINT ''
PRINT 'Usage:'
PRINT '  EXEC migrate_structure_type @p0, @p_old_type, @p_new_type, @p3'
PRINT ''
PRINT 'Examples:'
PRINT '  EXEC migrate_structure_type 12345, ''String'', ''Long'', 1  -- dry run'
PRINT '  EXEC migrate_structure_type 12345, ''String'', ''Long'', 0  -- execute'
GO



-- ===== migration_drop_deleted_objects.sql =====
-- =====================================================
-- MIGRATION: Drop legacy _deleted_objects archive system
-- Part of Background Deletion System migration
-- =====================================================
-- This script removes the old _deleted_objects table and its trigger.
-- The new soft-delete system uses @@__deleted scheme and trash containers.
-- Run this script on existing databases to migrate to new system.
-- =====================================================

-- 1. Drop the trigger first
-- IF EXISTS (SELECT * FROM sys.triggers WHERE name = 'TR__objects__deleted_objects')
-- DROP TRIGGER [dbo].[TR__objects__deleted_objects]
-- GO

-- 2. Drop the archive table
IF OBJECT_ID('[dbo].[_deleted_objects]', 'U') IS NOT NULL 
DROP TABLE [dbo].[_deleted_objects]
GO

-- 3. Add the @@__deleted scheme if not exists
IF NOT EXISTS (SELECT 1 FROM [dbo].[_schemes] WHERE [_id] = -10)
INSERT INTO [dbo].[_schemes] ([_id], [_name], [_alias], [_type]) 
VALUES (-10, '@@__deleted', 'Deleted Objects', -9223372036854775703)
GO

-- =====================================================
-- VERIFICATION
-- =====================================================
-- After running this script, verify:
-- SELECT * FROM [dbo].[_schemes] WHERE [_id] = -10
-- Expected: @@__deleted scheme exists
-- 
-- SELECT * FROM sys.triggers WHERE name = 'TR__objects__deleted_objects'
-- Expected: No rows (trigger removed)
-- =====================================================



-- ===== redb_aggregation.sql =====
-- =====================================================
-- REDB EAV AGGREGATION FUNCTIONS (MSSQL)
-- Aggregations for EAV fields (_values)
-- Supports: simple fields, nested Class, arrays, dictionaries
-- Ported from PostgreSQL version
-- =====================================================

SET NOCOUNT ON;
GO

-- ===== DROP EXISTING OBJECTS =====
IF OBJECT_ID('dbo.aggregate_field', 'P') IS NOT NULL 
    DROP PROCEDURE dbo.aggregate_field;
GO

IF OBJECT_ID('dbo.aggregate_batch', 'P') IS NOT NULL 
    DROP PROCEDURE dbo.aggregate_batch;
GO

IF OBJECT_ID('dbo.aggregate_batch_preview', 'P') IS NOT NULL 
    DROP PROCEDURE dbo.aggregate_batch_preview;
GO

-- =====================================================
-- aggregate_field: Single field aggregation
-- =====================================================
-- Parameters:
--   @scheme_id    - Scheme ID
--   @field_path   - Path to field:
--                   "Price"             - simple field
--                   "Customer.Name"     - nested Class
--                   "Items[].Price"     - array (ALL elements)
--                   "Items[2].Price"    - array (SPECIFIC element)
--                   "PhoneBook[home]"   - Dictionary (SPECIFIC key)
--   @function     - SUM, AVG, MIN, MAX, COUNT
--   @filter_json  - JSON filter or NULL
--
-- Returns: numeric result
-- =====================================================
CREATE PROCEDURE dbo.aggregate_field
    @scheme_id BIGINT,
    @field_path NVARCHAR(255),
    @function NVARCHAR(20),
    @filter_json NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @structure_id BIGINT;
    DECLARE @db_type NVARCHAR(50);
    DECLARE @dict_key NVARCHAR(255);
    DECLARE @array_index NVARCHAR(50);
    DECLARE @column_name NVARCHAR(50);
    DECLARE @array_condition NVARCHAR(500) = N'';
    DECLARE @sql NVARCHAR(MAX);
    DECLARE @result DECIMAL(38, 10);
    
    -- 1. Resolve field path
    EXEC resolve_field_path @scheme_id, @field_path,
        @structure_id OUTPUT, @db_type OUTPUT, @dict_key OUTPUT, @array_index OUTPUT;
    
    IF @structure_id IS NULL
    BEGIN
        RAISERROR('Field "%s" not found in scheme %d', 16, 1, @field_path, @scheme_id);
        RETURN;
    END
    
    -- 2. Determine column by data type
    SET @column_name = CASE @db_type
        WHEN N'Long' THEN N'_Long'
        WHEN N'Double' THEN N'_Double'
        WHEN N'Numeric' THEN N'_Numeric'
        WHEN N'Int' THEN N'_Long'
        WHEN N'Decimal' THEN N'_Numeric'
        WHEN N'Money' THEN N'_Numeric'
        ELSE N'_Long'
    END;
    
    -- 3. Build array/dict condition (with SQL injection protection)
    IF @dict_key IS NOT NULL
        SET @array_condition = N' AND v._array_index = N''' + REPLACE(@dict_key, '''', '''''') + N'''';
    ELSE IF @array_index IS NOT NULL
        SET @array_condition = N' AND v._array_index = N''' + REPLACE(@array_index, '''', '''''') + N'''';
    ELSE IF @dict_key IS NULL AND @array_index IS NULL AND @field_path NOT LIKE N'%[]%'
        SET @array_condition = N' AND v._array_index IS NULL';
    
    -- 4. Build and execute SQL
    IF @filter_json IS NOT NULL AND @filter_json <> N'' AND @filter_json <> N'null'
    BEGIN
        -- With filter - use temp table for object IDs
        CREATE TABLE #filtered_ids (_id BIGINT PRIMARY KEY);
        
        INSERT INTO #filtered_ids
        EXEC get_filtered_object_ids @scheme_id, @filter_json, 10;
        
        IF NOT EXISTS (SELECT 1 FROM #filtered_ids)
        BEGIN
            IF @function = N'COUNT'
                SELECT 0 AS result;
            ELSE
                SELECT NULL AS result;
            DROP TABLE #filtered_ids;
            RETURN;
        END
        
        SET @sql = N'
            SELECT @res = ' + UPPER(@function) + N'(CAST(v.' + @column_name + N' AS DECIMAL(38,10)))
            FROM _values v
            WHERE v._id_structure = @struct_id
              AND v._id_object IN (SELECT _id FROM #filtered_ids)' + @array_condition;
        
        EXEC sp_executesql @sql, 
            N'@struct_id BIGINT, @res DECIMAL(38,10) OUTPUT',
            @struct_id = @structure_id,
            @res = @result OUTPUT;
        
        DROP TABLE #filtered_ids;
    END
    ELSE
    BEGIN
        -- Without filter - all objects in scheme
        SET @sql = N'
            SELECT @res = ' + UPPER(@function) + N'(CAST(v.' + @column_name + N' AS DECIMAL(38,10)))
            FROM _values v
            JOIN _objects o ON o._id = v._id_object
            WHERE v._id_structure = @struct_id
              AND o._id_scheme = @scheme' + @array_condition;
        
        EXEC sp_executesql @sql,
            N'@struct_id BIGINT, @scheme BIGINT, @res DECIMAL(38,10) OUTPUT',
            @struct_id = @structure_id,
            @scheme = @scheme_id,
            @res = @result OUTPUT;
    END
    
    SELECT @result AS result;
END;
GO

-- =====================================================
-- aggregate_batch: Multiple aggregations in ONE query
-- =====================================================
CREATE PROCEDURE dbo.aggregate_batch
    @scheme_id BIGINT,
    @aggregations NVARCHAR(MAX),
    @filter_json NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @select_parts NVARCHAR(MAX) = N'';
    DECLARE @has_eav_fields BIT = 0;
    DECLARE @sql NVARCHAR(MAX);
    DECLARE @field NVARCHAR(255);
    DECLARE @func NVARCHAR(20);
    DECLARE @alias NVARCHAR(255);
    DECLARE @structure_id BIGINT;
    DECLARE @db_type NVARCHAR(50);
    DECLARE @dict_key NVARCHAR(255);
    DECLARE @array_index NVARCHAR(50);
    DECLARE @column_name NVARCHAR(50);
    DECLARE @array_condition NVARCHAR(500);
    
    -- Parse aggregations JSON
    DECLARE @agg_cursor CURSOR;
    SET @agg_cursor = CURSOR FOR
        SELECT 
            JSON_VALUE(value, '$.field') AS field,
            UPPER(JSON_VALUE(value, '$.func')) AS func,
            JSON_VALUE(value, '$.alias') AS alias
        FROM OPENJSON(@aggregations);
    
    OPEN @agg_cursor;
    FETCH NEXT FROM @agg_cursor INTO @field, @func, @alias;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- COUNT(*) special case
        IF @func = N'COUNT' AND (@field IS NULL OR @field = N'*' OR @field = N'')
        BEGIN
            IF @select_parts <> N'' SET @select_parts = @select_parts + N', ';
            SET @select_parts = @select_parts + N'COUNT(DISTINCT v._id_object) AS [' + REPLACE(@alias, '''', '''''') + N']';
            SET @has_eav_fields = 1;
            FETCH NEXT FROM @agg_cursor INTO @field, @func, @alias;
            CONTINUE;
        END
        
        -- Base field with 0$: prefix
        IF @field LIKE N'0$:%'
        BEGIN
            DECLARE @raw_field NVARCHAR(100) = SUBSTRING(@field, 4, 100);
            DECLARE @sql_column NVARCHAR(100) = dbo.normalize_base_field_name(@raw_field);
            
            IF @sql_column IS NOT NULL
            BEGIN
                IF @select_parts <> N'' SET @select_parts = @select_parts + N', ';
                
                IF @func IN (N'SUM', N'AVG')
                    SET @select_parts = @select_parts + @func + N'(CAST(o.' + @sql_column + N' AS DECIMAL(38,10))) AS [' + REPLACE(@alias, '''', '''''') + N']';
                ELSE
                    SET @select_parts = @select_parts + @func + N'(o.' + @sql_column + N') AS [' + REPLACE(@alias, '''', '''''') + N']';
            END
        END
        ELSE
        BEGIN
            -- EAV field
            EXEC resolve_field_path @scheme_id, @field,
                @structure_id OUTPUT, @db_type OUTPUT, @dict_key OUTPUT, @array_index OUTPUT;
            
            IF @structure_id IS NOT NULL
            BEGIN
                SET @has_eav_fields = 1;
                
                SET @column_name = CASE @db_type
                    WHEN N'Long' THEN N'_Long'
                    WHEN N'Double' THEN N'_Double'
                    WHEN N'Numeric' THEN N'_Numeric'
                    WHEN N'Int' THEN N'_Long'
                    WHEN N'Decimal' THEN N'_Numeric'
                    WHEN N'Money' THEN N'_Numeric'
                    ELSE N'_Long'
                END;
                
                -- Build array condition (with SQL injection protection)
                IF @dict_key IS NOT NULL
                    SET @array_condition = N' AND v._array_index = N''' + REPLACE(@dict_key, '''', '''''') + N'''';
                ELSE IF @array_index IS NOT NULL
                    SET @array_condition = N' AND v._array_index = N''' + REPLACE(@array_index, '''', '''''') + N'''';
                ELSE IF @field NOT LIKE N'%[]%'
                    SET @array_condition = N' AND v._array_index IS NULL';
                ELSE
                    SET @array_condition = N'';
                
                IF @select_parts <> N'' SET @select_parts = @select_parts + N', ';
                SET @select_parts = @select_parts + @func + 
                    N'(CASE WHEN v._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + 
                    @array_condition + N' THEN CAST(v.' + @column_name + N' AS DECIMAL(38,10)) END) AS [' + REPLACE(@alias, '''', '''''') + N']';
            END
        END
        
        FETCH NEXT FROM @agg_cursor INTO @field, @func, @alias;
    END
    
    CLOSE @agg_cursor;
    DEALLOCATE @agg_cursor;
    
    IF @select_parts = N''
    BEGIN
        SELECT N'{}' AS result;
        RETURN;
    END
    
    -- Execute with or without filter
    IF @filter_json IS NOT NULL AND @filter_json <> N'' AND @filter_json <> N'null'
    BEGIN
        -- With filter
        CREATE TABLE #filtered_ids (_id BIGINT PRIMARY KEY);
        
        INSERT INTO #filtered_ids
        EXEC get_filtered_object_ids @scheme_id, @filter_json, 10;
        
        IF NOT EXISTS (SELECT 1 FROM #filtered_ids)
        BEGIN
            SELECT N'{}' AS result;
            DROP TABLE #filtered_ids;
            RETURN;
        END
        
        IF @has_eav_fields = 1
        BEGIN
            SET @sql = N'
                SELECT ' + @select_parts + N'
                FROM _values v
                JOIN _objects o ON o._id = v._id_object
                WHERE v._id_object IN (SELECT _id FROM #filtered_ids)
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER';
        END
        ELSE
        BEGIN
            SET @sql = N'
                SELECT ' + @select_parts + N'
                FROM _objects o
                WHERE o._id IN (SELECT _id FROM #filtered_ids)
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER';
        END
        
        EXEC sp_executesql @sql;
        DROP TABLE #filtered_ids;
    END
    ELSE
    BEGIN
        -- Without filter
        IF @has_eav_fields = 1
        BEGIN
            SET @sql = N'
                SELECT ' + @select_parts + N'
                FROM _values v
                JOIN _objects o ON o._id = v._id_object
                WHERE o._id_scheme = ' + CAST(@scheme_id AS NVARCHAR(20)) + N'
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER';
        END
        ELSE
        BEGIN
            SET @sql = N'
                SELECT ' + @select_parts + N'
                FROM _objects o
                WHERE o._id_scheme = ' + CAST(@scheme_id AS NVARCHAR(20)) + N'
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER';
        END
        
        EXEC sp_executesql @sql;
    END
END;
GO

-- =====================================================
-- aggregate_batch_preview: SQL Preview for debugging
-- =====================================================
CREATE PROCEDURE dbo.aggregate_batch_preview
    @scheme_id BIGINT,
    @aggregations NVARCHAR(MAX),
    @filter_json NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @select_parts NVARCHAR(MAX) = N'';
    DECLARE @structure_ids NVARCHAR(MAX) = N'';
    DECLARE @field NVARCHAR(255);
    DECLARE @func NVARCHAR(20);
    DECLARE @alias NVARCHAR(255);
    DECLARE @structure_id BIGINT;
    DECLARE @db_type NVARCHAR(50);
    DECLARE @dict_key NVARCHAR(255);
    DECLARE @array_index NVARCHAR(50);
    DECLARE @column_name NVARCHAR(50);
    DECLARE @array_condition NVARCHAR(500);
    DECLARE @sql NVARCHAR(MAX);
    
    -- Parse aggregations
    DECLARE @agg_cursor CURSOR;
    SET @agg_cursor = CURSOR FOR
        SELECT 
            JSON_VALUE(value, '$.field') AS field,
            UPPER(JSON_VALUE(value, '$.func')) AS func,
            JSON_VALUE(value, '$.alias') AS alias
        FROM OPENJSON(@aggregations);
    
    OPEN @agg_cursor;
    FETCH NEXT FROM @agg_cursor INTO @field, @func, @alias;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @func = N'COUNT' AND (@field IS NULL OR @field = N'*' OR @field = N'')
        BEGIN
            IF @select_parts <> N'' SET @select_parts = @select_parts + N',
  ';
            SET @select_parts = @select_parts + N'COUNT(DISTINCT v._id_object) AS [' + REPLACE(@alias, '''', '''''') + N']';
            FETCH NEXT FROM @agg_cursor INTO @field, @func, @alias;
            CONTINUE;
        END
        
        EXEC resolve_field_path @scheme_id, @field,
            @structure_id OUTPUT, @db_type OUTPUT, @dict_key OUTPUT, @array_index OUTPUT;
        
        IF @structure_id IS NOT NULL
        BEGIN
            IF @structure_ids <> N'' SET @structure_ids = @structure_ids + N', ';
            SET @structure_ids = @structure_ids + CAST(@structure_id AS NVARCHAR(20));
            
            SET @column_name = CASE @db_type
                WHEN N'Long' THEN N'_Long'
                WHEN N'Double' THEN N'_Double'
                WHEN N'Numeric' THEN N'_Numeric'
                ELSE N'_Long'
            END;
            
            IF @dict_key IS NOT NULL
                SET @array_condition = N' AND v._array_index = N''' + REPLACE(@dict_key, '''', '''''') + N'''';
            ELSE IF @array_index IS NOT NULL
                SET @array_condition = N' AND v._array_index = N''' + REPLACE(@array_index, '''', '''''') + N'''';
            ELSE IF @field NOT LIKE N'%[]%'
                SET @array_condition = N' AND v._array_index IS NULL';
            ELSE
                SET @array_condition = N'';
            
            IF @select_parts <> N'' SET @select_parts = @select_parts + N',
  ';
            SET @select_parts = @select_parts + @func + 
                N'(CASE WHEN v._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + 
                @array_condition + N' THEN CAST(v.' + @column_name + N' AS DECIMAL(38,10)) END) AS [' + 
                REPLACE(@alias, '''', '''''') + N'] /* ' + REPLACE(@field, '''', '''''') + N' */';
        END
        
        FETCH NEXT FROM @agg_cursor INTO @field, @func, @alias;
    END
    
    CLOSE @agg_cursor;
    DEALLOCATE @agg_cursor;
    
    IF @select_parts = N''
    BEGIN
        SELECT N'-- No aggregations to execute' AS sql_preview;
        RETURN;
    END
    
    -- Build preview SQL
    IF @filter_json IS NOT NULL
    BEGIN
        SET @sql = N'-- AGGREGATE BATCH SQL PREVIEW
-- Scheme: ' + CAST(@scheme_id AS NVARCHAR(20)) + N'
-- Filter: ' + @filter_json + N'

-- Step 1: Get object_ids via get_filtered_object_ids
-- Step 2: Aggregation by object_ids

SELECT
  ' + @select_parts + N'
FROM _values v
WHERE v._id_structure IN (' + @structure_ids + N')
  AND v._id_object IN (SELECT _id FROM #filtered_ids);';
    END
    ELSE
    BEGIN
        SET @sql = N'-- AGGREGATE BATCH SQL PREVIEW
-- Scheme: ' + CAST(@scheme_id AS NVARCHAR(20)) + N'
-- Filter: NULL

SELECT
  ' + @select_parts + N'
FROM _values v
JOIN _objects o ON o._id = v._id_object
WHERE v._id_structure IN (' + @structure_ids + N')
  AND o._id_scheme = ' + CAST(@scheme_id AS NVARCHAR(20)) + N';';
    END
    
    SELECT @sql AS sql_preview;
END;
GO

PRINT N'=========================================';
PRINT N'Aggregation procedures created!';
PRINT N'';
PRINT N'PROCEDURES:';
PRINT N'  aggregate_field - Single field aggregation';
PRINT N'  aggregate_batch - Multiple aggregations in one call';
PRINT N'  aggregate_batch_preview - SQL preview for debugging';
PRINT N'';
PRINT N'SUPPORTED FUNCTIONS:';
PRINT N'  SUM, AVG, MIN, MAX, COUNT';
PRINT N'';
PRINT N'FEATURES:';
PRINT N'  - Base fields with 0$: prefix';
PRINT N'  - EAV fields from _values';
PRINT N'  - Array support: Items[].Price (all), Items[2].Price (specific)';
PRINT N'  - Dictionary support: PhoneBook[home]';
PRINT N'  - Filters via get_filtered_object_ids';
PRINT N'=========================================';
GO



-- ===== redb_facets_search.sql =====
-- ===== REDB FACETS & SEARCH MODULE FOR MSSQL =====
-- Full featured faceted search and object filtering module
-- Architecture: Stored procedures with dynamic SQL (SQL injection safe)
-- Supports: LINQ operators, logical operators, Class fields, Arrays, Dictionary, hierarchical search
--
-- DEPENDENCIES: 
--   1. redbMSSQL.sql (core tables)
--   2. redb_metadata_cache.sql (metadata cache)
-- Run those scripts BEFORE this one!

-- =====================================================
-- CHECK DEPENDENCIES
-- =====================================================
IF OBJECT_ID('dbo._scheme_metadata_cache', 'U') IS NULL
BEGIN
    RAISERROR('ERROR: Table _scheme_metadata_cache not found! Run redb_metadata_cache.sql first.', 16, 1);
    RETURN;
END;

IF OBJECT_ID('dbo.sync_metadata_cache_for_scheme', 'P') IS NULL
BEGIN
    RAISERROR('ERROR: Procedure sync_metadata_cache_for_scheme not found! Run redb_metadata_cache.sql first.', 16, 1);
    RETURN;
END;
GO

PRINT 'Dependencies OK. Creating facets search module...'
GO

-- =====================================================
-- DROP EXISTING OBJECTS
-- =====================================================

IF OBJECT_ID('dbo.preview_facet_query', 'P') IS NOT NULL DROP PROCEDURE dbo.preview_facet_query;
IF OBJECT_ID('dbo.search_objects_with_facets', 'P') IS NOT NULL DROP PROCEDURE dbo.search_objects_with_facets;
IF OBJECT_ID('dbo.search_tree_objects_with_facets', 'P') IS NOT NULL DROP PROCEDURE dbo.search_tree_objects_with_facets;
IF OBJECT_ID('dbo.get_facets', 'P') IS NOT NULL DROP PROCEDURE dbo.get_facets;
IF OBJECT_ID('dbo.internal_parse_filters', 'P') IS NOT NULL DROP PROCEDURE dbo.internal_parse_filters;
IF OBJECT_ID('dbo.internal_build_exists', 'P') IS NOT NULL DROP PROCEDURE dbo.internal_build_exists;
IF OBJECT_ID('dbo.normalize_base_field_name', 'FN') IS NOT NULL DROP FUNCTION dbo.normalize_base_field_name;
IF OBJECT_ID('dbo.get_value_column_by_type', 'FN') IS NOT NULL DROP FUNCTION dbo.get_value_column_by_type;
IF OBJECT_ID('dbo.get_object_level', 'FN') IS NOT NULL DROP FUNCTION dbo.get_object_level;
IF OBJECT_ID('dbo.is_ancestor_of', 'FN') IS NOT NULL DROP FUNCTION dbo.is_ancestor_of;
IF OBJECT_ID('dbo.is_descendant_of', 'FN') IS NOT NULL DROP FUNCTION dbo.is_descendant_of;
IF OBJECT_ID('dbo.build_facet_field_path', 'FN') IS NOT NULL DROP FUNCTION dbo.build_facet_field_path;
IF OBJECT_ID('dbo.get_facets_extended', 'P') IS NOT NULL DROP PROCEDURE dbo.get_facets_extended;
IF OBJECT_ID('dbo.preview_tree_facet_query', 'P') IS NOT NULL DROP PROCEDURE dbo.preview_tree_facet_query;
GO

-- =====================================================
-- HELPER FUNCTION: normalize_base_field_name
-- Maps C# names to SQL column names on _objects table
-- =====================================================
CREATE FUNCTION dbo.normalize_base_field_name(@field_name NVARCHAR(100))
RETURNS NVARCHAR(100)
WITH SCHEMABINDING
AS
BEGIN
    RETURN CASE @field_name
        WHEN 'id' THEN '_id' WHEN 'Id' THEN '_id' WHEN '_id' THEN '_id'
        WHEN 'parent_id' THEN '_id_parent' WHEN 'ParentId' THEN '_id_parent' WHEN '_id_parent' THEN '_id_parent'
        WHEN 'scheme_id' THEN '_id_scheme' WHEN 'SchemeId' THEN '_id_scheme' WHEN '_id_scheme' THEN '_id_scheme'
        WHEN 'owner_id' THEN '_id_owner' WHEN 'OwnerId' THEN '_id_owner' WHEN '_id_owner' THEN '_id_owner'
        WHEN 'who_change_id' THEN '_id_who_change' WHEN 'WhoChangeId' THEN '_id_who_change' WHEN '_id_who_change' THEN '_id_who_change'
        WHEN 'value_long' THEN '_value_long' WHEN 'ValueLong' THEN '_value_long' WHEN '_value_long' THEN '_value_long'
        WHEN 'value_string' THEN '_value_string' WHEN 'ValueString' THEN '_value_string' WHEN '_value_string' THEN '_value_string'
        WHEN 'value_guid' THEN '_value_guid' WHEN 'ValueGuid' THEN '_value_guid' WHEN '_value_guid' THEN '_value_guid'
        WHEN 'key' THEN '_key' WHEN 'Key' THEN '_key' WHEN '_key' THEN '_key'
        WHEN 'name' THEN '_name' WHEN 'Name' THEN '_name' WHEN '_name' THEN '_name'
        WHEN 'note' THEN '_note' WHEN 'Note' THEN '_note' WHEN '_note' THEN '_note'
        WHEN 'value_bool' THEN '_value_bool' WHEN 'ValueBool' THEN '_value_bool' WHEN '_value_bool' THEN '_value_bool'
        WHEN 'value_double' THEN '_value_double' WHEN 'ValueDouble' THEN '_value_double' WHEN '_value_double' THEN '_value_double'
        WHEN 'value_numeric' THEN '_value_numeric' WHEN 'ValueNumeric' THEN '_value_numeric' WHEN '_value_numeric' THEN '_value_numeric'
        WHEN 'value_datetime' THEN '_value_datetime' WHEN 'ValueDatetime' THEN '_value_datetime' WHEN '_value_datetime' THEN '_value_datetime'
        WHEN 'value_bytes' THEN '_value_bytes' WHEN 'ValueBytes' THEN '_value_bytes' WHEN '_value_bytes' THEN '_value_bytes'
        WHEN 'hash' THEN '_hash' WHEN 'Hash' THEN '_hash' WHEN '_hash' THEN '_hash'
        WHEN 'date_create' THEN '_date_create' WHEN 'DateCreate' THEN '_date_create' WHEN '_date_create' THEN '_date_create'
        WHEN 'date_modify' THEN '_date_modify' WHEN 'DateModify' THEN '_date_modify' WHEN '_date_modify' THEN '_date_modify'
        WHEN 'date_begin' THEN '_date_begin' WHEN 'DateBegin' THEN '_date_begin' WHEN '_date_begin' THEN '_date_begin'
        WHEN 'date_complete' THEN '_date_complete' WHEN 'DateComplete' THEN '_date_complete' WHEN '_date_complete' THEN '_date_complete'
        ELSE NULL
    END;
END;
GO

-- =====================================================
-- HELPER FUNCTION: get_value_column_by_type
-- Returns _values column name based on db_type
-- =====================================================
CREATE FUNCTION dbo.get_value_column_by_type(@db_type NVARCHAR(50), @type_semantic NVARCHAR(100))
RETURNS NVARCHAR(50)
WITH SCHEMABINDING
AS
BEGIN
    RETURN CASE 
        WHEN @db_type = 'String' THEN '_String'
        WHEN @db_type = 'Long' AND @type_semantic = '_RObject' THEN '_Long'
        WHEN @db_type = 'Long' THEN '_Long'
        WHEN @db_type = 'Double' THEN '_Double'
        WHEN @db_type = 'Numeric' THEN '_Numeric'
        WHEN @db_type = 'Boolean' THEN '_Boolean'
        WHEN @db_type = 'DateTimeOffset' THEN '_DateTimeOffset'
        WHEN @db_type = 'Guid' THEN '_Guid'
        WHEN @db_type = 'ListItem' THEN '_listitem'
        WHEN @db_type = 'ByteArray' THEN '_ByteArray'
        ELSE '_String'
    END;
END;
GO

-- =====================================================
-- HELPER FUNCTION: get_object_level
-- Returns the hierarchical level of an object (0 = root)
-- =====================================================
CREATE FUNCTION dbo.get_object_level(@object_id BIGINT)
RETURNS INT
AS
BEGIN
    DECLARE @level INT = 0;
    DECLARE @current_parent BIGINT;
    
    SELECT @current_parent = _id_parent FROM _objects WHERE _id = @object_id;
    
    WHILE @current_parent IS NOT NULL AND @level < 100
    BEGIN
        SET @level = @level + 1;
        SELECT @current_parent = _id_parent FROM _objects WHERE _id = @current_parent;
    END;
    
    RETURN @level;
END;
GO

-- =====================================================
-- HELPER FUNCTION: is_ancestor_of
-- Returns 1 if @ancestor_id is an ancestor of @object_id
-- =====================================================
CREATE FUNCTION dbo.is_ancestor_of(@object_id BIGINT, @ancestor_id BIGINT)
RETURNS BIT
AS
BEGIN
    IF @object_id IS NULL OR @ancestor_id IS NULL RETURN 0;
    IF @object_id = @ancestor_id RETURN 0; -- not ancestor of itself
    
    DECLARE @current_parent BIGINT;
    DECLARE @depth INT = 0;
    
    SELECT @current_parent = _id_parent FROM _objects WHERE _id = @object_id;
    
    WHILE @current_parent IS NOT NULL AND @depth < 100
    BEGIN
        IF @current_parent = @ancestor_id RETURN 1;
        SELECT @current_parent = _id_parent FROM _objects WHERE _id = @current_parent;
        SET @depth = @depth + 1;
    END;
    
    RETURN 0;
END;
GO

-- =====================================================
-- HELPER FUNCTION: is_descendant_of
-- Returns 1 if @descendant_id is a descendant of @object_id
-- =====================================================
CREATE FUNCTION dbo.is_descendant_of(@object_id BIGINT, @descendant_id BIGINT)
RETURNS BIT
AS
BEGIN
    -- descendant_id is descendant of object_id means object_id is ancestor of descendant_id
    RETURN dbo.is_ancestor_of(@descendant_id, @object_id);
END;
GO

-- =====================================================
-- HELPER FUNCTION: build_order_by_clause
-- Creates ORDER BY clause from JSON specification
-- Format: [{"field": "Name", "direction": "ASC"}]
-- =====================================================
IF OBJECT_ID('dbo.build_order_by_clause', 'FN') IS NOT NULL 
    DROP FUNCTION dbo.build_order_by_clause;
GO

CREATE FUNCTION dbo.build_order_by_clause(
    @order_by NVARCHAR(MAX),
    @table_alias NVARCHAR(10)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    DECLARE @result NVARCHAR(MAX) = N'';
    
    -- If no order specified, return empty
    IF @order_by IS NULL OR @order_by = N'' OR @order_by = N'[]' OR @order_by = N'null'
        RETURN @result;
    
    -- Parse JSON array: [{"field": "Name", "direction": "ASC"}]
    SELECT @result = STRING_AGG(order_item, N', ')
    FROM (
        SELECT 
            CASE 
                -- Base field with 0$: prefix
                WHEN JSON_VALUE(arr.value, '$.field') LIKE N'0$:%' THEN
                    @table_alias + N'.' + dbo.normalize_base_field_name(
                        SUBSTRING(JSON_VALUE(arr.value, '$.field'), 4, 100)
                    ) + N' ' + 
                    UPPER(ISNULL(JSON_VALUE(arr.value, '$.direction'), N'ASC'))
                -- Regular Props field - sort via subquery to _values
                ELSE
                    N'(SELECT TOP 1 COALESCE(v._String, CAST(v._Long AS NVARCHAR(50)), FORMAT(v._Double, ''G'', ''en-US''))
                       FROM _values v 
                       JOIN _scheme_metadata_cache c ON c._structure_id = v._id_structure
                       WHERE v._id_object = ' + @table_alias + N'._id 
                         AND c._name = N''' + JSON_VALUE(arr.value, '$.field') + N'''
                         AND v._array_index IS NULL) ' +
                    UPPER(ISNULL(JSON_VALUE(arr.value, '$.direction'), N'ASC'))
            END AS order_item
        FROM OPENJSON(@order_by) arr
        WHERE JSON_VALUE(arr.value, '$.field') IS NOT NULL
    ) items;
    
    IF @result IS NOT NULL AND @result <> N''
        SET @result = N'ORDER BY ' + @result;
    
    RETURN @result;
END;
GO

-- =====================================================
-- INTERNAL: Build single EXISTS condition
-- Called by internal_parse_filters for each field
-- =====================================================
CREATE PROCEDURE dbo.internal_build_exists
    @scheme_id BIGINT,
    @field_path NVARCHAR(500),
    @operator NVARCHAR(50),
    @value NVARCHAR(MAX),
    @value_type NVARCHAR(20),
    @table_alias NVARCHAR(10),
    @result NVARCHAR(MAX) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @structure_id BIGINT;
    DECLARE @parent_structure_id BIGINT;
    DECLARE @nested_structure_id BIGINT;
    DECLARE @db_type NVARCHAR(50);
    DECLARE @type_semantic NVARCHAR(100);
    DECLARE @collection_type BIGINT;
    DECLARE @value_column NVARCHAR(50);
    
    DECLARE @root_field NVARCHAR(450);
    DECLARE @nested_field NVARCHAR(450);
    DECLARE @is_array BIT = 0;
    DECLARE @is_nested BIT = 0;
    DECLARE @dict_key NVARCHAR(500) = NULL;
    
    DECLARE @safe_value NVARCHAR(MAX);
    SET @safe_value = REPLACE(@value, N'''', N'''''');
    
    -- Property function: $length, $count
    DECLARE @property_function NVARCHAR(20) = NULL;
    
    -- Check for property function suffix: Name.$length, Tags[].$count
    IF @field_path LIKE '%.$length'
    BEGIN
        SET @property_function = '$length';
        SET @field_path = LEFT(@field_path, LEN(@field_path) - 8); -- Remove '.$length'
    END
    ELSE IF @field_path LIKE '%.$count'
    BEGIN
        SET @property_function = '$count';
        SET @field_path = LEFT(@field_path, LEN(@field_path) - 7); -- Remove '.$count'
    END;
    
    -- Parse field path
    -- Check for Dictionary: Field[key]
    IF @field_path LIKE '%[[]%]%' AND @field_path NOT LIKE '%[[]]%'
    BEGIN
        SET @root_field = LEFT(@field_path, CHARINDEX('[', @field_path) - 1);
        SET @dict_key = SUBSTRING(@field_path, CHARINDEX('[', @field_path) + 1, 
                        CHARINDEX(']', @field_path) - CHARINDEX('[', @field_path) - 1);
        -- Check for nested after dictionary: Field[key].Prop
        IF CHARINDEX('].', @field_path) > 0
        BEGIN
            SET @nested_field = SUBSTRING(@field_path, CHARINDEX('].', @field_path) + 2, LEN(@field_path));
            SET @is_nested = 1;
        END;
    END
    -- Check for Array: Field[]
    ELSE IF @field_path LIKE '%[[]]%'
    BEGIN
        SET @is_array = 1;
        IF CHARINDEX('.', REPLACE(@field_path, '[]', '')) > 0
        BEGIN
            SET @root_field = LEFT(REPLACE(@field_path, '[]', ''), CHARINDEX('.', REPLACE(@field_path, '[]', '')) - 1);
            SET @nested_field = SUBSTRING(REPLACE(@field_path, '[]', ''), CHARINDEX('.', REPLACE(@field_path, '[]', '')) + 1, LEN(@field_path));
            SET @is_nested = 1;
        END
        ELSE
        BEGIN
            SET @root_field = REPLACE(@field_path, '[]', '');
        END;
    END
    -- Check for nested: Field.Prop
    ELSE IF CHARINDEX('.', @field_path) > 0
    BEGIN
        SET @root_field = LEFT(@field_path, CHARINDEX('.', @field_path) - 1);
        SET @nested_field = SUBSTRING(@field_path, CHARINDEX('.', @field_path) + 1, LEN(@field_path));
        SET @is_nested = 1;
    END
    ELSE
    BEGIN
        SET @root_field = @field_path;
    END;
    
    -- Find root structure
    SELECT @structure_id = c._structure_id, 
           @db_type = c.db_type, 
           @type_semantic = c.type_semantic,
           @collection_type = c._collection_type
    FROM _scheme_metadata_cache c
    WHERE c._scheme_id = @scheme_id 
      AND c._name = @root_field
      AND c._parent_structure_id IS NULL;
    
    IF @structure_id IS NULL
    BEGIN
        -- Debug: Field not found in metadata cache
        -- Check if cache is empty or field doesn't exist
        DECLARE @total_cache INT;
        SELECT @total_cache = COUNT(*) FROM _scheme_metadata_cache WHERE _scheme_id = @scheme_id;
        
        IF @total_cache = 0
        BEGIN
            -- Cache is empty - likely not synced
            PRINT 'WARNING: Metadata cache is EMPTY for scheme ' + CAST(@scheme_id AS NVARCHAR(20)) + '. Run sync_metadata_cache_for_scheme first!';
        END
        ELSE
        BEGIN
            PRINT 'WARNING: Field "' + @root_field + '" not found in scheme ' + CAST(@scheme_id AS NVARCHAR(20)) + ' (cache has ' + CAST(@total_cache AS NVARCHAR(10)) + ' entries)';
        END;
        
        SET @result = N'1=1'; -- Field not found, skip
        RETURN;
    END;
    
    -- Find nested structure if needed
    IF @is_nested = 1 AND @nested_field IS NOT NULL
    BEGIN
        -- Special handling for ListItem: Value, Alias, Id, IdList are NOT structures in cache
        -- They are columns in _list_items table (like PostgreSQL _get_listitem_field_type_info)
        IF @type_semantic = '_RListItem' AND @nested_field IN ('Value', 'Alias', 'Id', 'IdList')
        BEGIN
            -- Keep @type_semantic = '_RListItem', don't search for nested structure
            SET @nested_structure_id = NULL;
            -- Set correct db_type for ListItem field so @condition is built correctly
            SET @db_type = CASE @nested_field
                WHEN 'Value' THEN 'String'
                WHEN 'Alias' THEN 'String'
                WHEN 'Id' THEN 'Long'
                WHEN 'IdList' THEN 'Long'
            END;
        END
        ELSE
        BEGIN
            SELECT @nested_structure_id = c._structure_id,
                   @db_type = c.db_type,
                   @type_semantic = c.type_semantic
            FROM _scheme_metadata_cache c
            WHERE c._scheme_id = @scheme_id
              AND c._name = @nested_field
              AND c._parent_structure_id = @structure_id;
        END;
    END;
    
    SET @value_column = dbo.get_value_column_by_type(@db_type, @type_semantic);
    
    -- Build EXISTS based on field type and operator
    DECLARE @condition NVARCHAR(MAX);
    
    -- Smart value condition based on operator and actual db_type from cache
    -- This handles type-aware comparisons like PostgreSQL version
    DECLARE @is_numeric BIT = CASE WHEN @value LIKE '[0-9]%' OR @value LIKE '-[0-9]%' THEN 1 ELSE 0 END;
    DECLARE @is_decimal BIT = CASE WHEN @is_numeric = 1 AND CHARINDEX('.', @value) > 0 THEN 1 ELSE 0 END;
    DECLARE @is_bool BIT = CASE WHEN @value IN ('true', 'false') THEN 1 ELSE 0 END;
    DECLARE @is_date BIT = CASE WHEN @value LIKE '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]%' THEN 1 ELSE 0 END;
    DECLARE @is_guid BIT = CASE WHEN @value LIKE '[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]-[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]-[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]-[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]-[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]' THEN 1 ELSE 0 END;
    
    -- Build value condition based on operator with TYPE-AWARE DETECTION (using @db_type from schema)
    SET @condition = CASE @operator
        WHEN '$eq' THEN 
            CASE 
                WHEN @value IS NULL OR @value = 'null' THEN N'v.' + @value_column + N' IS NULL'
                WHEN @db_type = 'Boolean' THEN N'v._Boolean = ' + CASE WHEN @value = 'true' THEN '1' ELSE '0' END
                WHEN @db_type = 'DateTimeOffset' THEN N'v._DateTimeOffset = CONVERT(datetimeoffset, ''' + @safe_value + ''', 127)'
                WHEN @db_type = 'Guid' THEN N'v._Guid = ''' + @safe_value + ''''
                WHEN @db_type = 'Long' THEN N'v._Long = ' + @value
                WHEN @db_type = 'Double' THEN N'v.[_Double] = ' + @value
                WHEN @db_type = 'Numeric' THEN N'v._Numeric = ' + @value
                WHEN @is_bool = 1 THEN N'v._Boolean = ' + CASE WHEN @value = 'true' THEN '1' ELSE '0' END
                WHEN @is_date = 1 THEN N'v._DateTimeOffset = CONVERT(datetimeoffset, ''' + @safe_value + ''', 127)'
                WHEN @is_guid = 1 THEN N'v._Guid = ''' + @safe_value + ''''
                WHEN @is_numeric = 1 THEN N'(v._Long = ' + @value + N' OR v.[_Double] = ' + @value + N' OR v._Numeric = ' + @value + N')'
                ELSE N'v._String = N''' + @safe_value + N'''' 
            END
        WHEN '$ne' THEN 
            CASE 
                WHEN @value IS NULL OR @value = 'null' THEN N'v.' + @value_column + N' IS NOT NULL'
                WHEN @db_type = 'Boolean' THEN N'v._Boolean <> ' + CASE WHEN @value = 'true' THEN '1' ELSE '0' END
                WHEN @db_type = 'DateTimeOffset' THEN N'v._DateTimeOffset <> CONVERT(datetimeoffset, ''' + @safe_value + ''', 127)'
                WHEN @db_type = 'Guid' THEN N'v._Guid <> ''' + @safe_value + ''''
                WHEN @db_type = 'Long' THEN N'v._Long <> ' + @value
                WHEN @db_type = 'Double' THEN N'v.[_Double] <> ' + @value
                WHEN @db_type = 'Numeric' THEN N'v._Numeric <> ' + @value
                WHEN @is_bool = 1 THEN N'v._Boolean <> ' + CASE WHEN @value = 'true' THEN '1' ELSE '0' END
                WHEN @is_date = 1 THEN N'v._DateTimeOffset <> CONVERT(datetimeoffset, ''' + @safe_value + ''', 127)'
                WHEN @is_guid = 1 THEN N'v._Guid <> ''' + @safe_value + ''''
                WHEN @is_numeric = 1 THEN N'(v._Long <> ' + @value + N' AND v.[_Double] <> ' + @value + N' AND v._Numeric <> ' + @value + N')'
                ELSE N'v._String <> N''' + @safe_value + N''''
            END
        WHEN '$gt' THEN 
            CASE 
                WHEN @db_type = 'DateTimeOffset' THEN N'v._DateTimeOffset > CONVERT(datetimeoffset, ''' + @safe_value + ''', 127)'
                WHEN @db_type = 'Long' THEN N'v._Long > ' + @value
                WHEN @db_type = 'Double' THEN N'v.[_Double] > ' + @value
                WHEN @db_type = 'Numeric' THEN N'v._Numeric > ' + @value
                WHEN @is_date = 1 THEN N'v._DateTimeOffset > CONVERT(datetimeoffset, ''' + @safe_value + ''', 127)'
                WHEN @is_numeric = 1 THEN N'(v._Long > ' + @value + N' OR v.[_Double] > ' + @value + N' OR v._Numeric > ' + @value + N')'
                ELSE N'v._String > N''' + @safe_value + ''''
            END
        WHEN '$gte' THEN 
            CASE 
                WHEN @db_type = 'DateTimeOffset' THEN N'v._DateTimeOffset >= CONVERT(datetimeoffset, ''' + @safe_value + ''', 127)'
                WHEN @db_type = 'Long' THEN N'v._Long >= ' + @value
                WHEN @db_type = 'Double' THEN N'v.[_Double] >= ' + @value
                WHEN @db_type = 'Numeric' THEN N'v._Numeric >= ' + @value
                WHEN @is_date = 1 THEN N'v._DateTimeOffset >= CONVERT(datetimeoffset, ''' + @safe_value + ''', 127)'
                WHEN @is_numeric = 1 THEN N'(v._Long >= ' + @value + N' OR v.[_Double] >= ' + @value + N' OR v._Numeric >= ' + @value + N')'
                ELSE N'v._String >= N''' + @safe_value + ''''
            END
        WHEN '$lt' THEN 
            CASE 
                WHEN @db_type = 'DateTimeOffset' THEN N'v._DateTimeOffset < CONVERT(datetimeoffset, ''' + @safe_value + ''', 127)'
                WHEN @db_type = 'Long' THEN N'v._Long < ' + @value
                WHEN @db_type = 'Double' THEN N'v.[_Double] < ' + @value
                WHEN @db_type = 'Numeric' THEN N'v._Numeric < ' + @value
                WHEN @is_date = 1 THEN N'v._DateTimeOffset < CONVERT(datetimeoffset, ''' + @safe_value + ''', 127)'
                WHEN @is_numeric = 1 THEN N'(v._Long < ' + @value + N' OR v.[_Double] < ' + @value + N' OR v._Numeric < ' + @value + N')'
                ELSE N'v._String < N''' + @safe_value + ''''
            END
        WHEN '$lte' THEN 
            CASE 
                WHEN @db_type = 'DateTimeOffset' THEN N'v._DateTimeOffset <= CONVERT(datetimeoffset, ''' + @safe_value + ''', 127)'
                WHEN @db_type = 'Long' THEN N'v._Long <= ' + @value
                WHEN @db_type = 'Double' THEN N'v.[_Double] <= ' + @value
                WHEN @db_type = 'Numeric' THEN N'v._Numeric <= ' + @value
                WHEN @is_date = 1 THEN N'v._DateTimeOffset <= CONVERT(datetimeoffset, ''' + @safe_value + ''', 127)'
                WHEN @is_numeric = 1 THEN N'(v._Long <= ' + @value + N' OR v.[_Double] <= ' + @value + N' OR v._Numeric <= ' + @value + N')'
                ELSE N'v._String <= N''' + @safe_value + ''''
            END
        WHEN '$contains' THEN N'v._String LIKE N''%' + @safe_value + N'%'''
        WHEN '$containsIgnoreCase' THEN N'v._String LIKE N''%' + @safe_value + N'%'' COLLATE Latin1_General_CI_AS'
        WHEN '$startsWith' THEN N'v._String LIKE N''' + @safe_value + N'%'''
        WHEN '$startsWithIgnoreCase' THEN N'v._String LIKE N''' + @safe_value + N'%'' COLLATE Latin1_General_CI_AS'
        WHEN '$endsWith' THEN N'v._String LIKE N''%' + @safe_value + N''''
        WHEN '$endsWithIgnoreCase' THEN N'v._String LIKE N''%' + @safe_value + N''' COLLATE Latin1_General_CI_AS'
        WHEN '$in' THEN 
            CASE 
                WHEN @db_type = 'Long' THEN N'v._Long IN (SELECT CAST([value] AS BIGINT) FROM OPENJSON(N''' + @safe_value + N'''))'
                WHEN @db_type = 'Double' THEN N'v.[_Double] IN (SELECT CAST([value] AS FLOAT) FROM OPENJSON(N''' + @safe_value + N'''))'
                WHEN @db_type = 'Numeric' THEN N'v._Numeric IN (SELECT CAST([value] AS DECIMAL(38,18)) FROM OPENJSON(N''' + @safe_value + N'''))'
                WHEN @is_numeric = 1 THEN N'(v._Long IN (SELECT CAST([value] AS BIGINT) FROM OPENJSON(N''' + @safe_value + N''')) OR v.[_Double] IN (SELECT CAST([value] AS FLOAT) FROM OPENJSON(N''' + @safe_value + N''')))'
                ELSE N'v._String IN (SELECT [value] FROM OPENJSON(N''' + @safe_value + N'''))'
            END
        WHEN '$notIn' THEN 
            CASE 
                WHEN @db_type = 'Long' THEN N'v._Long NOT IN (SELECT CAST([value] AS BIGINT) FROM OPENJSON(N''' + @safe_value + N'''))'
                WHEN @db_type = 'Double' THEN N'v.[_Double] NOT IN (SELECT CAST([value] AS FLOAT) FROM OPENJSON(N''' + @safe_value + N'''))'
                WHEN @db_type = 'Numeric' THEN N'v._Numeric NOT IN (SELECT CAST([value] AS DECIMAL(38,18)) FROM OPENJSON(N''' + @safe_value + N'''))'
                WHEN @is_numeric = 1 THEN N'v._Long NOT IN (SELECT CAST([value] AS BIGINT) FROM OPENJSON(N''' + @safe_value + N''')) AND v.[_Double] NOT IN (SELECT CAST([value] AS FLOAT) FROM OPENJSON(N''' + @safe_value + N'''))'
                ELSE N'v._String NOT IN (SELECT [value] FROM OPENJSON(N''' + @safe_value + N'''))'
            END
        WHEN '$exists' THEN CASE WHEN @value = 'true' THEN N'1=1' ELSE N'1=0' END  -- EXISTS/NOT EXISTS handles this
        WHEN '$between' THEN NULL  -- Special handling below
        -- $match: Full-Text Search (requires FTS catalog)
        WHEN '$match' THEN N'CONTAINS(v._String, N''' + @safe_value + N''')'
        WHEN '$matchPrefix' THEN N'CONTAINS(v._String, N''"' + @safe_value + N'*"'')'
        -- $regex alternative for MSSQL: pattern matching with LIKE
        WHEN '$like' THEN N'v._String LIKE N''' + @safe_value + N''''
        WHEN '$likeIgnoreCase' THEN N'v._String LIKE N''' + @safe_value + N''' COLLATE Latin1_General_CI_AS'
        -- Array operators (use @db_type for type-aware queries)
        WHEN '$arrayContains' THEN 
            CASE 
                WHEN @db_type = 'Boolean' THEN N'v._Boolean = ' + CASE WHEN @value = 'true' THEN '1' ELSE '0' END
                WHEN @db_type = 'Long' THEN N'v._Long = ' + @value
                WHEN @db_type = 'Double' THEN N'v.[_Double] = ' + @value
                WHEN @db_type = 'Numeric' THEN N'v._Numeric = ' + @value
                WHEN @is_bool = 1 THEN N'v._Boolean = ' + CASE WHEN @value = 'true' THEN '1' ELSE '0' END
                WHEN @is_numeric = 1 THEN N'(v._Long = ' + @value + N' OR v.[_Double] = ' + @value + N')'
                ELSE N'v._String = N''' + @safe_value + N''''
            END
        WHEN '$arrayAny' THEN N'1=1'
        WHEN '$arrayEmpty' THEN N'1=0'  -- Will be wrapped in NOT EXISTS
        WHEN '$arrayCount' THEN NULL  -- Special handling below
        WHEN '$arrayCountGt' THEN NULL
        WHEN '$arrayCountGte' THEN NULL
        WHEN '$arrayCountLt' THEN NULL
        WHEN '$arrayCountLte' THEN NULL
        -- Extended array operators
        WHEN '$arrayFirst' THEN 
            CASE 
                WHEN @db_type = 'Long' THEN N'v._array_index = ''0'' AND v._Long = ' + @value
                WHEN @db_type = 'Double' THEN N'v._array_index = ''0'' AND v.[_Double] = ' + @value
                WHEN @is_numeric = 1 THEN N'v._array_index = ''0'' AND (v._Long = ' + @value + N' OR v.[_Double] = ' + @value + N')'
                ELSE N'v._array_index = ''0'' AND v._String = N''' + @safe_value + N''''
            END
        WHEN '$arrayLast' THEN NULL  -- Special handling below
        WHEN '$arrayAt' THEN N'v._array_index = N''' + @safe_value + N''''
        WHEN '$arrayStartsWith' THEN N'v._String LIKE N''' + @safe_value + N'%'''
        WHEN '$arrayEndsWith' THEN N'v._String LIKE N''%' + @safe_value + N''''
        ELSE 
            CASE 
                WHEN @db_type = 'Boolean' THEN N'v._Boolean = ' + CASE WHEN @value = 'true' THEN '1' ELSE '0' END
                WHEN @db_type = 'DateTimeOffset' THEN N'v._DateTimeOffset = CONVERT(datetimeoffset, ''' + @safe_value + ''', 127)'
                WHEN @db_type = 'Guid' THEN N'v._Guid = ''' + @safe_value + ''''
                WHEN @db_type = 'Long' THEN N'v._Long = ' + @value
                WHEN @db_type = 'Double' THEN N'v.[_Double] = ' + @value
                WHEN @db_type = 'Numeric' THEN N'v._Numeric = ' + @value
                WHEN @is_bool = 1 THEN N'v._Boolean = ' + CASE WHEN @value = 'true' THEN '1' ELSE '0' END
                WHEN @is_date = 1 THEN N'v._DateTimeOffset = CONVERT(datetimeoffset, ''' + @safe_value + ''', 127)'
                WHEN @is_guid = 1 THEN N'v._Guid = ''' + @safe_value + ''''
                WHEN @is_numeric = 1 THEN N'(v._Long = ' + @value + N' OR v.[_Double] = ' + @value + N' OR v._Numeric = ' + @value + N')'
                ELSE N'v._String = N''' + @safe_value + N''''
            END
    END;
    
    -- Handle array count operators specially
    IF @operator LIKE '$arrayCount%'
    BEGIN
        DECLARE @count_op NVARCHAR(10) = CASE @operator
            WHEN '$arrayCount' THEN '='
            WHEN '$arrayCountGt' THEN '>'
            WHEN '$arrayCountGte' THEN '>='
            WHEN '$arrayCountLt' THEN '<'
            WHEN '$arrayCountLte' THEN '<='
        END;
        SET @result = N'(SELECT COUNT(*) FROM _values v WHERE v._id_object = ' + @table_alias + N'._id ' +
                      N'AND v._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' ' +
                      N'AND v._array_index IS NOT NULL) ' + @count_op + N' ' + @value;
        RETURN;
    END;
    
    -- Handle $arrayLast - needs subquery to find max index
    IF @operator = '$arrayLast'
    BEGIN
        SET @result = N'EXISTS (SELECT 1 FROM _values v WHERE v._id_object = ' + @table_alias + N'._id ' +
            N'AND v._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' ' +
            N'AND v._array_index = (SELECT MAX(v2._array_index) FROM _values v2 WHERE v2._id_object = ' + @table_alias + N'._id ' +
            N'AND v2._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' AND v2._array_index IS NOT NULL) ' +
            N'AND ' + CASE 
                WHEN @is_numeric = 1 THEN N'(v._Long = ' + @value + N' OR v._Double = ' + @value + N')'
                ELSE N'v._String = N''' + @safe_value + N''''
            END + N')';
        RETURN;
    END;
    
    -- Handle $between - value is JSON array [min, max]
    IF @operator = '$between'
    BEGIN
        DECLARE @min_val NVARCHAR(500);
        DECLARE @max_val NVARCHAR(500);
        SELECT @min_val = [value] FROM OPENJSON(@value) WHERE [key] = '0';
        SELECT @max_val = [value] FROM OPENJSON(@value) WHERE [key] = '1';
        
        IF @min_val IS NOT NULL AND @max_val IS NOT NULL
        BEGIN
            SET @result = N'EXISTS (SELECT 1 FROM _values v WHERE v._id_object = ' + @table_alias + N'._id ' +
                N'AND v._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' ' +
                N'AND v._array_index IS NULL AND (' +
                CASE 
                    WHEN @min_val LIKE '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]%' 
                    THEN N'v._DateTimeOffset BETWEEN CONVERT(datetimeoffset, ''' + REPLACE(@min_val, '''', '''''') + N''', 127) AND CONVERT(datetimeoffset, ''' + REPLACE(@max_val, '''', '''''') + N''', 127)'
                    WHEN @min_val LIKE '[0-9]%' OR @min_val LIKE '-[0-9]%'
                    THEN N'(v._Long BETWEEN ' + @min_val + N' AND ' + @max_val + N') OR (v._Double BETWEEN ' + @min_val + N' AND ' + @max_val + N')'
                    ELSE N'v._String BETWEEN N''' + REPLACE(@min_val, '''', '''''') + N''' AND N''' + REPLACE(@max_val, '''', '''''') + N''''
                END + N'))';
        END
        ELSE
            SET @result = N'1=1';
        RETURN;
    END;
    
    -- Handle array aggregation operators
    IF @operator IN ('$arraySum', '$arrayAvg', '$arrayMin', '$arrayMax')
    BEGIN
        DECLARE @agg_func NVARCHAR(10) = CASE @operator
            WHEN '$arraySum' THEN 'SUM'
            WHEN '$arrayAvg' THEN 'AVG'
            WHEN '$arrayMin' THEN 'MIN'
            WHEN '$arrayMax' THEN 'MAX'
        END;
        SET @result = N'(SELECT ' + @agg_func + N'(COALESCE(v._Long, v._Double, v._Numeric)) ' +
                      N'FROM _values v WHERE v._id_object = ' + @table_alias + N'._id ' +
                      N'AND v._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' ' +
                      N'AND v._array_index IS NOT NULL) = ' + @value;
        RETURN;
    END;
    
    -- Handle array aggregation comparison operators ($arraySumGt, $arraySumLt, etc.)
    IF @operator LIKE '$arraySum%' OR @operator LIKE '$arrayAvg%' OR @operator LIKE '$arrayMin%' OR @operator LIKE '$arrayMax%'
    BEGIN
        DECLARE @agg_base NVARCHAR(20);
        DECLARE @agg_cmp NVARCHAR(10);
        
        IF @operator LIKE '%Gt' SET @agg_cmp = '>';
        ELSE IF @operator LIKE '%Gte' SET @agg_cmp = '>=';
        ELSE IF @operator LIKE '%Lt' SET @agg_cmp = '<';
        ELSE IF @operator LIKE '%Lte' SET @agg_cmp = '<=';
        ELSE SET @agg_cmp = '=';
        
        IF @operator LIKE '$arraySum%' SET @agg_base = 'SUM';
        ELSE IF @operator LIKE '$arrayAvg%' SET @agg_base = 'AVG';
        ELSE IF @operator LIKE '$arrayMin%' SET @agg_base = 'MIN';
        ELSE IF @operator LIKE '$arrayMax%' SET @agg_base = 'MAX';
        
        SET @result = N'(SELECT ' + @agg_base + N'(COALESCE(v._Long, v._Double, v._Numeric)) ' +
                      N'FROM _values v WHERE v._id_object = ' + @table_alias + N'._id ' +
                      N'AND v._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' ' +
                      N'AND v._array_index IS NOT NULL) ' + @agg_cmp + N' ' + @value;
        RETURN;
    END;
    
    -- Build final EXISTS clause
    IF @dict_key IS NOT NULL
    BEGIN
        -- Dictionary access: Field[key]
        IF @is_nested = 1 AND @nested_structure_id IS NOT NULL
        BEGIN
            -- Dictionary with nested: Field[key].Prop
            SET @result = N'EXISTS (SELECT 1 FROM _values rv ' +
                N'JOIN _values v ON v._array_parent_id = rv._id AND v._id_structure = ' + CAST(@nested_structure_id AS NVARCHAR(20)) + N' ' +
                N'WHERE rv._id_object = ' + @table_alias + N'._id ' +
                N'AND rv._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' ' +
                N'AND rv._array_index = N''' + REPLACE(@dict_key, '''', '''''') + N''' ' +
                N'AND ' + @condition + N')';
        END
        ELSE
        BEGIN
            -- Simple dictionary: Field[key]
            SET @result = N'EXISTS (SELECT 1 FROM _values v ' +
                N'WHERE v._id_object = ' + @table_alias + N'._id ' +
                N'AND v._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' ' +
                N'AND v._array_index = N''' + REPLACE(@dict_key, '''', '''''') + N''' ' +
                N'AND ' + @condition + N')';
        END;
    END
    ELSE IF @is_array = 1 AND @is_nested = 1 AND @nested_structure_id IS NOT NULL
    BEGIN
        -- Array of Class: Field[].Prop
        SET @result = N'EXISTS (SELECT 1 FROM _values rv ' +
            N'JOIN _values v ON v._array_parent_id = rv._id AND v._id_structure = ' + CAST(@nested_structure_id AS NVARCHAR(20)) + N' ' +
            N'WHERE rv._id_object = ' + @table_alias + N'._id ' +
            N'AND rv._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' ' +
            N'AND rv._array_index IS NOT NULL ' +
            N'AND ' + @condition + N')';
    END
    ELSE IF @is_array = 1 AND (@type_semantic IS NULL OR @type_semantic <> '_RListItem' OR @is_nested = 0)
    BEGIN
        -- Simple array: Field[] (not ListItem array with nested field)
        IF @operator = '$arrayEmpty'
        BEGIN
            SET @result = N'NOT EXISTS (SELECT 1 FROM _values v ' +
                N'WHERE v._id_object = ' + @table_alias + N'._id ' +
                N'AND v._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' ' +
                N'AND v._array_index IS NOT NULL)';
        END
        ELSE
        BEGIN
            SET @result = N'EXISTS (SELECT 1 FROM _values v ' +
                N'WHERE v._id_object = ' + @table_alias + N'._id ' +
                N'AND v._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' ' +
                N'AND v._array_index IS NOT NULL ' +
                N'AND ' + @condition + N')';
        END;
    END
    ELSE IF @is_nested = 1 AND @nested_structure_id IS NOT NULL
    BEGIN
        -- Class field: Field.Prop
        SET @result = N'EXISTS (SELECT 1 FROM _values rv ' +
            N'JOIN _values v ON v._id_object = rv._id_object AND v._id_structure = ' + CAST(@nested_structure_id AS NVARCHAR(20)) + N' ' +
            N'WHERE rv._id_object = ' + @table_alias + N'._id ' +
            N'AND rv._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' ' +
            N'AND rv._array_index IS NULL ' +
            N'AND v._array_index IS NULL ' +
            N'AND ' + @condition + N')';
    END
    -- ListItem field: Status.Value, Status.Alias, Roles[].Value
    ELSE IF @is_nested = 1 AND @type_semantic = '_RListItem' AND @nested_field IN ('Value', 'Alias', 'Id', 'IdList')
    BEGIN
        DECLARE @li_column NVARCHAR(20) = CASE @nested_field
            WHEN 'Value' THEN '_value'
            WHEN 'Alias' THEN '_alias'
            WHEN 'Id' THEN '_id'
            WHEN 'IdList' THEN '_id_list'
        END;
        DECLARE @li_condition NVARCHAR(MAX) = REPLACE(@condition, N'v._String', N'li.' + @li_column);
        SET @li_condition = REPLACE(@li_condition, N'v._Long', N'li.' + @li_column);
        
        IF @is_array = 1
        BEGIN
            -- ListItem array: Roles[].Value
            SET @result = N'EXISTS (SELECT 1 FROM _values v ' +
                N'JOIN _list_items li ON li._id = v._listitem ' +
                N'WHERE v._id_object = ' + @table_alias + N'._id ' +
                N'AND v._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' ' +
                N'AND v._array_index IS NOT NULL ' +
                N'AND ' + @li_condition + N')';
        END
        ELSE
        BEGIN
            -- Simple ListItem: Status.Value
            SET @result = N'EXISTS (SELECT 1 FROM _values v ' +
                N'JOIN _list_items li ON li._id = v._listitem ' +
                N'WHERE v._id_object = ' + @table_alias + N'._id ' +
                N'AND v._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' ' +
                N'AND v._array_index IS NULL ' +
                N'AND ' + @li_condition + N')';
        END;
    END
    -- ListItem direct: search by ID (Status = 123)
    ELSE IF @type_semantic = '_RListItem' AND @is_nested = 0
    BEGIN
        SET @result = N'EXISTS (SELECT 1 FROM _values v ' +
            N'WHERE v._id_object = ' + @table_alias + N'._id ' +
            N'AND v._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' ' +
            N'AND v._array_index IS NULL ' +
            N'AND v._listitem = ' + @value + N')';
    END
    -- Dictionary existence check: PhoneBook != null or PhoneBook.$exists
    -- For Dictionary, values are stored with _array_index = key (not NULL)
    ELSE IF @collection_type IS NOT NULL AND @dict_key IS NULL AND (@operator = '$exists' OR (@operator = '$ne' AND (@value IS NULL OR @value = 'null')))
    BEGIN
        -- Check if any dictionary entry exists (any _array_index value)
        IF @operator = '$exists' AND @value = 'true'
        BEGIN
            SET @result = N'EXISTS (SELECT 1 FROM _values v ' +
                N'WHERE v._id_object = ' + @table_alias + N'._id ' +
                N'AND v._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' ' +
                N'AND v._array_index IS NOT NULL)';
        END
        ELSE IF @operator = '$exists' AND @value = 'false'
        BEGIN
            SET @result = N'NOT EXISTS (SELECT 1 FROM _values v ' +
                N'WHERE v._id_object = ' + @table_alias + N'._id ' +
                N'AND v._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' ' +
                N'AND v._array_index IS NOT NULL)';
        END
        ELSE IF @operator = '$ne' AND (@value IS NULL OR @value = 'null')
        BEGIN
            -- PhoneBook != null means dictionary has at least one entry
            SET @result = N'EXISTS (SELECT 1 FROM _values v ' +
                N'WHERE v._id_object = ' + @table_alias + N'._id ' +
                N'AND v._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' ' +
                N'AND v._array_index IS NOT NULL)';
        END
        ELSE
        BEGIN
            SET @result = N'1=1';
        END;
    END
    -- Property function: $length (string length) or $count (array count)
    ELSE IF @property_function IS NOT NULL
    BEGIN
        DECLARE @func_compare_op NVARCHAR(10) = CASE @operator
            WHEN '$eq' THEN '='
            WHEN '$ne' THEN '<>'
            WHEN '$gt' THEN '>'
            WHEN '$gte' THEN '>='
            WHEN '$lt' THEN '<'
            WHEN '$lte' THEN '<='
            ELSE '='
        END;
        
        IF @property_function = '$length'
        BEGIN
            -- String length: LEN(v._String)
            SET @result = N'EXISTS (SELECT 1 FROM _values v ' +
                N'WHERE v._id_object = ' + @table_alias + N'._id ' +
                N'AND v._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' ' +
                N'AND v._array_index IS NULL ' +
                N'AND LEN(v._String) ' + @func_compare_op + N' ' + @value + N')';
        END
        ELSE IF @property_function = '$count'
        BEGIN
            -- Array count: COUNT of array elements
            SET @result = N'(SELECT COUNT(*) FROM _values v ' +
                N'WHERE v._id_object = ' + @table_alias + N'._id ' +
                N'AND v._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' ' +
                N'AND v._array_index IS NOT NULL) ' + @func_compare_op + N' ' + @value;
        END
        ELSE
        BEGIN
            SET @result = N'1=1'; -- Unknown function, skip
        END;
    END
    ELSE
    BEGIN
        -- Simple field
        IF @operator = '$ne' AND @value IS NOT NULL AND @value <> 'null'
        BEGIN
            -- $ne needs NOT EXISTS for proper null handling in EAV
            -- Use numeric format for numeric values, string format for strings
            DECLARE @ne_value_expr NVARCHAR(100);
            IF @is_numeric = 1
                SET @ne_value_expr = @value;  -- Numeric: just the value
            ELSE
                SET @ne_value_expr = N'N''' + @safe_value + N'''';  -- String: N'value'
            
            SET @result = N'NOT EXISTS (SELECT 1 FROM _values v ' +
                N'WHERE v._id_object = ' + @table_alias + N'._id ' +
                N'AND v._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' ' +
                N'AND v._array_index IS NULL ' +
                N'AND v.' + @value_column + N' = ' + @ne_value_expr + N')';
        END
        ELSE
        BEGIN
            SET @result = N'EXISTS (SELECT 1 FROM _values v ' +
                N'WHERE v._id_object = ' + @table_alias + N'._id ' +
                N'AND v._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' ' +
                N'AND v._array_index IS NULL ' +
                N'AND ' + @condition + N')';
        END;
    END;
END;
GO

-- =====================================================
-- INTERNAL: Recursive filter parser
-- Parses JSON filters and builds WHERE clause
-- =====================================================
CREATE PROCEDURE dbo.internal_parse_filters
    @scheme_id BIGINT,
    @json NVARCHAR(MAX),
    @table_alias NVARCHAR(10),
    @logical_op NVARCHAR(10),  -- 'AND' or 'OR'
    @negate BIT,               -- For $not
    @result NVARCHAR(MAX) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Handle NULL, empty, '{}' and string 'null' from C#
    IF @json IS NULL OR @json = N'' OR @json = N'{}' OR @json = N'null'
    BEGIN
        SET @result = N'';
        RETURN;
    END;
    
    -- All variable declarations at the top (MSSQL requirement)
    DECLARE @conditions TABLE (idx INT IDENTITY(1,1), condition NVARCHAR(MAX));
    DECLARE @key NVARCHAR(500);
    DECLARE @value NVARCHAR(MAX);
    DECLARE @type INT;
    DECLARE @exists_sql NVARCHAR(MAX);
    DECLARE @sub_result NVARCHAR(MAX);
    
    -- Variables for $and
    DECLARE @and_conditions TABLE (idx INT IDENTITY(1,1), cond NVARCHAR(MAX));
    DECLARE @and_item NVARCHAR(MAX);
    DECLARE @and_result NVARCHAR(MAX);
    
    -- Variables for $or
    DECLARE @or_conditions TABLE (idx INT IDENTITY(1,1), cond NVARCHAR(MAX));
    DECLARE @or_item NVARCHAR(MAX);
    DECLARE @or_result NVARCHAR(MAX);
    
    -- Variables for $hasAncestor
    DECLARE @anc_condition NVARCHAR(MAX);
    DECLARE @anc_scheme_id BIGINT;
    DECLARE @anc_max_depth INT;
    DECLARE @anc_temp_json NVARCHAR(MAX);
    DECLARE @anc_built_condition NVARCHAR(MAX);
    
    -- Variables for $hasDescendant
    DECLARE @desc_condition NVARCHAR(MAX);
    DECLARE @desc_scheme_id BIGINT;
    DECLARE @desc_max_depth INT;
    DECLARE @desc_temp_json NVARCHAR(MAX);
    DECLARE @desc_built_condition NVARCHAR(MAX);
    
    -- Variables for $level
    DECLARE @level_op NVARCHAR(50);
    DECLARE @level_val NVARCHAR(50);
    DECLARE @level_cmp NVARCHAR(10);
    
    -- Variables for base fields (0$:)
    DECLARE @base_field NVARCHAR(100);
    DECLARE @base_col NVARCHAR(100);
    DECLARE @base_op NVARCHAR(50);
    DECLARE @base_val NVARCHAR(MAX);
    DECLARE @base_type INT;
    DECLARE @base_cond NVARCHAR(MAX);
    DECLARE @is_datetime_col BIT;
    DECLARE @safe_base_val NVARCHAR(MAX);
    DECLARE @is_datetime_simple BIT;
    DECLARE @safe_value_simple NVARCHAR(MAX);
    
    -- Variables for field operators
    DECLARE @op_name NVARCHAR(50);
    DECLARE @op_value NVARCHAR(MAX);
    DECLARE @op_type INT;
    DECLARE @op_value_type NVARCHAR(20);
    
    -- Variable for STRING_AGG separator
    DECLARE @separator NVARCHAR(10);
    
    -- Parse JSON object
    DECLARE json_cursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT [key], [value], [type] FROM OPENJSON(@json);
    
    OPEN json_cursor;
    FETCH NEXT FROM json_cursor INTO @key, @value, @type;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Handle logical operators
        IF @key = '$and'
        BEGIN
            -- $and: array of conditions, all must match
            DELETE FROM @and_conditions;
            
            DECLARE and_cursor CURSOR LOCAL FAST_FORWARD FOR
                SELECT [value] FROM OPENJSON(@value);
            
            OPEN and_cursor;
            FETCH NEXT FROM and_cursor INTO @and_item;
            
            WHILE @@FETCH_STATUS = 0
            BEGIN
                EXEC dbo.internal_parse_filters @scheme_id, @and_item, @table_alias, N'AND', 0, @and_result OUTPUT;
                IF @and_result IS NOT NULL AND @and_result <> N''
                    INSERT INTO @and_conditions (cond) VALUES (@and_result);
                FETCH NEXT FROM and_cursor INTO @and_item;
            END;
            
            CLOSE and_cursor;
            DEALLOCATE and_cursor;
            
            SELECT @sub_result = N'(' + STRING_AGG(cond, N' AND ') + N')'
            FROM @and_conditions;
            
            IF @sub_result IS NOT NULL
                INSERT INTO @conditions (condition) VALUES (@sub_result);
            
            DELETE FROM @and_conditions;
        END
        ELSE IF @key = '$or'
        BEGIN
            -- $or: array of conditions, any must match
            DELETE FROM @or_conditions;
            
            DECLARE or_cursor CURSOR LOCAL FAST_FORWARD FOR
                SELECT [value] FROM OPENJSON(@value);
            
            OPEN or_cursor;
            FETCH NEXT FROM or_cursor INTO @or_item;
            
            WHILE @@FETCH_STATUS = 0
            BEGIN
                EXEC dbo.internal_parse_filters @scheme_id, @or_item, @table_alias, N'AND', 0, @or_result OUTPUT;
                IF @or_result IS NOT NULL AND @or_result <> N''
                    INSERT INTO @or_conditions (cond) VALUES (@or_result);
                FETCH NEXT FROM or_cursor INTO @or_item;
            END;
            
            CLOSE or_cursor;
            DEALLOCATE or_cursor;
            
            SELECT @sub_result = N'(' + STRING_AGG(cond, N' OR ') + N')'
            FROM @or_conditions;
            
            IF @sub_result IS NOT NULL
                INSERT INTO @conditions (condition) VALUES (@sub_result);
            
            DELETE FROM @or_conditions;
        END
        ELSE IF @key = '$not'
        BEGIN
            -- $not: negate the inner condition
            EXEC dbo.internal_parse_filters @scheme_id, @value, @table_alias, N'AND', 1, @sub_result OUTPUT;
            IF @sub_result IS NOT NULL AND @sub_result <> N''
                INSERT INTO @conditions (condition) VALUES (N'NOT (' + @sub_result + N')');
        END
        -- Hierarchical operators
        ELSE IF @key = '$isRoot'
        BEGIN
            IF @value = 'true'
                INSERT INTO @conditions (condition) VALUES (@table_alias + N'._id_parent IS NULL');
            ELSE
                INSERT INTO @conditions (condition) VALUES (@table_alias + N'._id_parent IS NOT NULL');
        END
        ELSE IF @key = '$isLeaf'
        BEGIN
            IF @value = 'true'
                INSERT INTO @conditions (condition) VALUES (N'NOT EXISTS (SELECT 1 FROM _objects c WHERE c._id_parent = ' + @table_alias + N'._id)');
            ELSE
                INSERT INTO @conditions (condition) VALUES (N'EXISTS (SELECT 1 FROM _objects c WHERE c._id_parent = ' + @table_alias + N'._id)');
        END
        ELSE IF @key = '$childrenOf'
        BEGIN
            -- $childrenOf: direct children of specified parent
            INSERT INTO @conditions (condition) VALUES (@table_alias + N'._id_parent = ' + @value);
        END
        ELSE IF @key = '$hasAncestor'
        BEGIN
            -- $hasAncestor: check if object has ancestor matching condition
            SET @anc_condition = N'';
            SET @anc_scheme_id = NULL;
            SET @anc_max_depth = 50;
            
            IF @type = 5  -- object
            BEGIN
                SET @anc_scheme_id = JSON_VALUE(@value, '$.scheme_id');
                SET @anc_max_depth = ISNULL(CAST(JSON_VALUE(@value, '$.max_depth') AS INT), 50);
                
                SET @anc_temp_json = JSON_QUERY(@value, '$.condition');
                IF @anc_temp_json IS NOT NULL
                BEGIN
                    EXEC dbo.internal_parse_filters @anc_scheme_id, @anc_temp_json, N'anc', N'AND', 0, @anc_condition OUTPUT;
                END;
            END;
            
            -- Build condition string separately (CASE not allowed in VALUES)
            SET @anc_built_condition = N'(SELECT 1 WHERE EXISTS (SELECT 1 FROM _objects anc WHERE anc._id IN (' +
                N'SELECT p1._id FROM _objects p1 WHERE p1._id = ' + @table_alias + N'._id_parent ' +
                N'UNION SELECT p2._id FROM _objects p2 JOIN _objects p1 ON p2._id = p1._id_parent WHERE p1._id = ' + @table_alias + N'._id_parent ' +
                N'UNION SELECT p3._id FROM _objects p3 JOIN _objects p2 ON p3._id = p2._id_parent JOIN _objects p1 ON p2._id = p1._id_parent WHERE p1._id = ' + @table_alias + N'._id_parent ' +
                N'UNION SELECT p4._id FROM _objects p4 JOIN _objects p3 ON p4._id = p3._id_parent JOIN _objects p2 ON p3._id = p2._id_parent JOIN _objects p1 ON p2._id = p1._id_parent WHERE p1._id = ' + @table_alias + N'._id_parent ' +
                N'UNION SELECT p5._id FROM _objects p5 JOIN _objects p4 ON p5._id = p4._id_parent JOIN _objects p3 ON p4._id = p3._id_parent JOIN _objects p2 ON p3._id = p2._id_parent JOIN _objects p1 ON p2._id = p1._id_parent WHERE p1._id = ' + @table_alias + N'._id_parent ' +
                N')';
            
            IF @anc_scheme_id IS NOT NULL
                SET @anc_built_condition = @anc_built_condition + N' AND anc._id_scheme = ' + CAST(@anc_scheme_id AS NVARCHAR(20));
            IF @anc_condition IS NOT NULL AND @anc_condition <> N''
                SET @anc_built_condition = @anc_built_condition + N' AND ' + @anc_condition;
            
            SET @anc_built_condition = @anc_built_condition + N')) = 1';
            
            INSERT INTO @conditions (condition) VALUES (@anc_built_condition);
        END
        ELSE IF @key = '$hasDescendant'
        BEGIN
            -- $hasDescendant: check if object has descendant matching condition
            SET @desc_condition = N'';
            SET @desc_scheme_id = NULL;
            SET @desc_max_depth = 50;
            
            IF @type = 5  -- object
            BEGIN
                SET @desc_scheme_id = JSON_VALUE(@value, '$.scheme_id');
                SET @desc_max_depth = ISNULL(CAST(JSON_VALUE(@value, '$.max_depth') AS INT), 50);
                
                SET @desc_temp_json = JSON_QUERY(@value, '$.condition');
                IF @desc_temp_json IS NOT NULL
                BEGIN
                    EXEC dbo.internal_parse_filters @desc_scheme_id, @desc_temp_json, N'desc_obj', N'AND', 0, @desc_condition OUTPUT;
                END;
            END;
            
            -- Build condition string separately (CASE not allowed in VALUES)
            SET @desc_built_condition = N'EXISTS (SELECT 1 FROM _objects desc_obj WHERE desc_obj._id IN (' +
                N'SELECT c1._id FROM _objects c1 WHERE c1._id_parent = ' + @table_alias + N'._id ' +
                N'UNION SELECT c2._id FROM _objects c2 JOIN _objects c1 ON c2._id_parent = c1._id WHERE c1._id_parent = ' + @table_alias + N'._id ' +
                N'UNION SELECT c3._id FROM _objects c3 JOIN _objects c2 ON c3._id_parent = c2._id JOIN _objects c1 ON c2._id_parent = c1._id WHERE c1._id_parent = ' + @table_alias + N'._id ' +
                N'UNION SELECT c4._id FROM _objects c4 JOIN _objects c3 ON c4._id_parent = c3._id JOIN _objects c2 ON c3._id_parent = c2._id JOIN _objects c1 ON c2._id_parent = c1._id WHERE c1._id_parent = ' + @table_alias + N'._id ' +
                N'UNION SELECT c5._id FROM _objects c5 JOIN _objects c4 ON c5._id_parent = c4._id JOIN _objects c3 ON c4._id_parent = c3._id JOIN _objects c2 ON c3._id_parent = c2._id JOIN _objects c1 ON c2._id_parent = c1._id WHERE c1._id_parent = ' + @table_alias + N'._id' +
                N')';
            
            IF @desc_scheme_id IS NOT NULL
                SET @desc_built_condition = @desc_built_condition + N' AND desc_obj._id_scheme = ' + CAST(@desc_scheme_id AS NVARCHAR(20));
            IF @desc_condition IS NOT NULL AND @desc_condition <> N''
                SET @desc_built_condition = @desc_built_condition + N' AND ' + @desc_condition;
            
            SET @desc_built_condition = @desc_built_condition + N')';
            
            INSERT INTO @conditions (condition) VALUES (@desc_built_condition);
        END
        ELSE IF @key = '$level'
        BEGIN
            -- $level: use parent chain counting
            IF @type = 2  -- number (direct value like "$level": 2)
            BEGIN
                -- Simple level check via counting parent chain
                IF @value = '0'
                BEGIN
                    INSERT INTO @conditions (condition) VALUES (@table_alias + N'._id_parent IS NULL');
                END
                ELSE
                BEGIN
                    INSERT INTO @conditions (condition) VALUES (
                        N'(SELECT COUNT(*) FROM (' +
                        N'SELECT p1._id FROM _objects p1 WHERE p1._id = ' + @table_alias + N'._id_parent ' +
                        N'UNION ALL SELECT p2._id FROM _objects p2 JOIN _objects p1 ON p2._id = p1._id_parent WHERE p1._id = ' + @table_alias + N'._id_parent ' +
                        N'UNION ALL SELECT p3._id FROM _objects p3 JOIN _objects p2 ON p3._id = p2._id_parent JOIN _objects p1 ON p2._id = p1._id_parent WHERE p1._id = ' + @table_alias + N'._id_parent ' +
                        N'UNION ALL SELECT p4._id FROM _objects p4 JOIN _objects p3 ON p4._id = p3._id_parent JOIN _objects p2 ON p3._id = p2._id_parent JOIN _objects p1 ON p2._id = p1._id_parent WHERE p1._id = ' + @table_alias + N'._id_parent ' +
                        N'UNION ALL SELECT p5._id FROM _objects p5 JOIN _objects p4 ON p5._id = p4._id_parent JOIN _objects p3 ON p4._id = p3._id_parent JOIN _objects p2 ON p3._id = p2._id_parent JOIN _objects p1 ON p2._id = p1._id_parent WHERE p1._id = ' + @table_alias + N'._id_parent ' +
                        N') AS parents) = ' + @value
                    );
                END;
            END
            ELSE IF @type = 5  -- object with operators like "$level": {"$gt": 2}
            BEGIN
                DECLARE level_cursor CURSOR LOCAL FAST_FORWARD FOR
                    SELECT [key], [value] FROM OPENJSON(@value);
                
                OPEN level_cursor;
                FETCH NEXT FROM level_cursor INTO @level_op, @level_val;
                
                WHILE @@FETCH_STATUS = 0
                BEGIN
                    SET @level_cmp = CASE @level_op
                        WHEN '$gt' THEN '>'
                        WHEN '$gte' THEN '>='
                        WHEN '$lt' THEN '<'
                        WHEN '$lte' THEN '<='
                        WHEN '$eq' THEN '='
                        WHEN '$ne' THEN '<>'
                        ELSE '='
                    END;
                    
                    -- Optimized checks for common cases
                    IF @level_val = '0' AND @level_cmp = '='
                    BEGIN
                        INSERT INTO @conditions (condition) VALUES (@table_alias + N'._id_parent IS NULL');
                    END
                    ELSE IF @level_val = '0' AND @level_cmp = '>'
                    BEGIN
                        INSERT INTO @conditions (condition) VALUES (@table_alias + N'._id_parent IS NOT NULL');
                    END
                    ELSE IF @level_val = '0' AND @level_cmp = '>='
                    BEGIN
                        INSERT INTO @conditions (condition) VALUES (N'1=1'); -- all objects have level >= 0
                    END
                    ELSE IF @level_val = '1' AND @level_cmp = '='
                    BEGIN
                        INSERT INTO @conditions (condition) VALUES (
                            @table_alias + N'._id_parent IS NOT NULL AND EXISTS (SELECT 1 FROM _objects p WHERE p._id = ' + @table_alias + N'._id_parent AND p._id_parent IS NULL)'
                        );
                    END
                    ELSE
                    BEGIN
                        -- General case: count parent chain (supports up to 10 levels)
                        INSERT INTO @conditions (condition) VALUES (
                            N'(SELECT COUNT(*) FROM (' +
                            N'SELECT p1._id FROM _objects p1 WHERE p1._id = ' + @table_alias + N'._id_parent ' +
                            N'UNION ALL SELECT p2._id FROM _objects p2 JOIN _objects p1 ON p2._id = p1._id_parent WHERE p1._id = ' + @table_alias + N'._id_parent ' +
                            N'UNION ALL SELECT p3._id FROM _objects p3 JOIN _objects p2 ON p3._id = p2._id_parent JOIN _objects p1 ON p2._id = p1._id_parent WHERE p1._id = ' + @table_alias + N'._id_parent ' +
                            N'UNION ALL SELECT p4._id FROM _objects p4 JOIN _objects p3 ON p4._id = p3._id_parent JOIN _objects p2 ON p3._id = p2._id_parent JOIN _objects p1 ON p2._id = p1._id_parent WHERE p1._id = ' + @table_alias + N'._id_parent ' +
                            N'UNION ALL SELECT p5._id FROM _objects p5 JOIN _objects p4 ON p5._id = p4._id_parent JOIN _objects p3 ON p4._id = p3._id_parent JOIN _objects p2 ON p3._id = p2._id_parent JOIN _objects p1 ON p2._id = p1._id_parent WHERE p1._id = ' + @table_alias + N'._id_parent ' +
                            N'UNION ALL SELECT p6._id FROM _objects p6 JOIN _objects p5 ON p6._id = p5._id_parent JOIN _objects p4 ON p5._id = p4._id_parent JOIN _objects p3 ON p4._id = p3._id_parent JOIN _objects p2 ON p3._id = p2._id_parent JOIN _objects p1 ON p2._id = p1._id_parent WHERE p1._id = ' + @table_alias + N'._id_parent ' +
                            N'UNION ALL SELECT p7._id FROM _objects p7 JOIN _objects p6 ON p7._id = p6._id_parent JOIN _objects p5 ON p6._id = p5._id_parent JOIN _objects p4 ON p5._id = p4._id_parent JOIN _objects p3 ON p4._id = p3._id_parent JOIN _objects p2 ON p3._id = p2._id_parent JOIN _objects p1 ON p2._id = p1._id_parent WHERE p1._id = ' + @table_alias + N'._id_parent ' +
                            N'UNION ALL SELECT p8._id FROM _objects p8 JOIN _objects p7 ON p8._id = p7._id_parent JOIN _objects p6 ON p7._id = p6._id_parent JOIN _objects p5 ON p6._id = p5._id_parent JOIN _objects p4 ON p5._id = p4._id_parent JOIN _objects p3 ON p4._id = p3._id_parent JOIN _objects p2 ON p3._id = p2._id_parent JOIN _objects p1 ON p2._id = p1._id_parent WHERE p1._id = ' + @table_alias + N'._id_parent ' +
                            N'UNION ALL SELECT p9._id FROM _objects p9 JOIN _objects p8 ON p9._id = p8._id_parent JOIN _objects p7 ON p8._id = p7._id_parent JOIN _objects p6 ON p7._id = p6._id_parent JOIN _objects p5 ON p6._id = p5._id_parent JOIN _objects p4 ON p5._id = p4._id_parent JOIN _objects p3 ON p4._id = p3._id_parent JOIN _objects p2 ON p3._id = p2._id_parent JOIN _objects p1 ON p2._id = p1._id_parent WHERE p1._id = ' + @table_alias + N'._id_parent ' +
                            N'UNION ALL SELECT p10._id FROM _objects p10 JOIN _objects p9 ON p10._id = p9._id_parent JOIN _objects p8 ON p9._id = p8._id_parent JOIN _objects p7 ON p8._id = p7._id_parent JOIN _objects p6 ON p7._id = p6._id_parent JOIN _objects p5 ON p6._id = p5._id_parent JOIN _objects p4 ON p5._id = p4._id_parent JOIN _objects p3 ON p4._id = p3._id_parent JOIN _objects p2 ON p3._id = p2._id_parent JOIN _objects p1 ON p2._id = p1._id_parent WHERE p1._id = ' + @table_alias + N'._id_parent ' +
                            N') AS parents) ' + @level_cmp + N' ' + @level_val
                        );
                    END;
                    
                    FETCH NEXT FROM level_cursor INTO @level_op, @level_val;
                END;
                
                CLOSE level_cursor;
                DEALLOCATE level_cursor;
            END;
        END
        -- Base field (0$: prefix)
        ELSE IF @key LIKE '0$:%'
        BEGIN
            SET @base_field = SUBSTRING(@key, 4, LEN(@key));
            SET @base_col = dbo.normalize_base_field_name(@base_field);
            
            IF @base_col IS NOT NULL
            BEGIN
                IF @type = 5  -- object with operators
                BEGIN
                    DECLARE base_cursor CURSOR LOCAL FAST_FORWARD FOR
                        SELECT [key], [value], [type] FROM OPENJSON(@value);
                    
                    OPEN base_cursor;
                    FETCH NEXT FROM base_cursor INTO @base_op, @base_val, @base_type;
                    
                    WHILE @@FETCH_STATUS = 0
                    BEGIN
                        -- Detect datetime base columns for proper CONVERT handling
                        SET @is_datetime_col = CASE WHEN @base_col IN ('_date_create', '_date_modify', '_date_begin', '_date_complete') THEN 1 ELSE 0 END;
                        SET @safe_base_val = REPLACE(@base_val, '''', '''''');
                        
                        SET @base_cond = CASE @base_op
                            WHEN '$eq' THEN 
                                CASE WHEN @is_datetime_col = 1 
                                    THEN @table_alias + N'.' + @base_col + N' = CONVERT(datetimeoffset, ''' + @safe_base_val + N''', 127)'
                                    ELSE @table_alias + N'.' + @base_col + N' = N''' + @safe_base_val + N''''
                                END
                            WHEN '$ne' THEN 
                                CASE WHEN @is_datetime_col = 1 
                                    THEN @table_alias + N'.' + @base_col + N' <> CONVERT(datetimeoffset, ''' + @safe_base_val + N''', 127)'
                                    ELSE @table_alias + N'.' + @base_col + N' <> N''' + @safe_base_val + N''''
                                END
                            WHEN '$gt' THEN 
                                CASE WHEN @is_datetime_col = 1 
                                    THEN @table_alias + N'.' + @base_col + N' > CONVERT(datetimeoffset, ''' + @safe_base_val + N''', 127)'
                                    ELSE @table_alias + N'.' + @base_col + N' > ' + @base_val
                                END
                            WHEN '$gte' THEN 
                                CASE WHEN @is_datetime_col = 1 
                                    THEN @table_alias + N'.' + @base_col + N' >= CONVERT(datetimeoffset, ''' + @safe_base_val + N''', 127)'
                                    ELSE @table_alias + N'.' + @base_col + N' >= ' + @base_val
                                END
                            WHEN '$lt' THEN 
                                CASE WHEN @is_datetime_col = 1 
                                    THEN @table_alias + N'.' + @base_col + N' < CONVERT(datetimeoffset, ''' + @safe_base_val + N''', 127)'
                                    ELSE @table_alias + N'.' + @base_col + N' < ' + @base_val
                                END
                            WHEN '$lte' THEN 
                                CASE WHEN @is_datetime_col = 1 
                                    THEN @table_alias + N'.' + @base_col + N' <= CONVERT(datetimeoffset, ''' + @safe_base_val + N''', 127)'
                                    ELSE @table_alias + N'.' + @base_col + N' <= ' + @base_val
                                END
                            WHEN '$contains' THEN @table_alias + N'.' + @base_col + N' LIKE N''%' + @safe_base_val + N'%'''
                            WHEN '$startsWith' THEN @table_alias + N'.' + @base_col + N' LIKE N''' + @safe_base_val + N'%'''
                            WHEN '$endsWith' THEN @table_alias + N'.' + @base_col + N' LIKE N''%' + @safe_base_val + N''''
                            WHEN '$in' THEN @table_alias + N'.' + @base_col + N' IN (SELECT [value] FROM OPENJSON(N''' + @safe_base_val + N'''))'
                            WHEN '$exists' THEN CASE WHEN @base_val = 'true' THEN @table_alias + N'.' + @base_col + N' IS NOT NULL' ELSE @table_alias + N'.' + @base_col + N' IS NULL' END
                            ELSE 
                                CASE WHEN @is_datetime_col = 1 
                                    THEN @table_alias + N'.' + @base_col + N' = CONVERT(datetimeoffset, ''' + @safe_base_val + N''', 127)'
                                    ELSE @table_alias + N'.' + @base_col + N' = N''' + @safe_base_val + N''''
                                END
                        END;
                        
                        IF @base_cond IS NOT NULL
                            INSERT INTO @conditions (condition) VALUES (@base_cond);
                        
                        FETCH NEXT FROM base_cursor INTO @base_op, @base_val, @base_type;
                    END;
                    
                    CLOSE base_cursor;
                    DEALLOCATE base_cursor;
                END
                ELSE
                BEGIN
                    -- Simple value - detect datetime columns for proper CONVERT
                    SET @is_datetime_simple = CASE WHEN @base_col IN ('_date_create', '_date_modify', '_date_begin', '_date_complete') THEN 1 ELSE 0 END;
                    SET @safe_value_simple = REPLACE(@value, '''', '''''');
                    
                    IF @is_datetime_simple = 1
                        INSERT INTO @conditions (condition) VALUES (
                            @table_alias + N'.' + @base_col + N' = CONVERT(datetimeoffset, ''' + @safe_value_simple + N''', 127)'
                        );
                    ELSE
                        INSERT INTO @conditions (condition) VALUES (
                            @table_alias + N'.' + @base_col + N' = N''' + @safe_value_simple + N''''
                        );
                END;
            END;
        END
        -- Dictionary indexer: FieldName[key] (e.g.: "PhoneBook[home]": {"$eq": "+7-999..."})
        -- Uses db_type from metadata cache for correct column selection
        ELSE IF @key LIKE '%[%]%' AND @key NOT LIKE '$%'
        BEGIN
            DECLARE @dict_idx_field_name NVARCHAR(450);
            DECLARE @dict_idx_key NVARCHAR(450);
            DECLARE @dict_idx_structure_id BIGINT;
            DECLARE @dict_idx_db_type NVARCHAR(50);
            DECLARE @dict_idx_op NVARCHAR(50);
            DECLARE @dict_idx_val NVARCHAR(MAX);
            DECLARE @dict_idx_val_type INT;
            DECLARE @dict_idx_condition NVARCHAR(MAX);
            DECLARE @dict_idx_value_cond NVARCHAR(MAX);
            DECLARE @dict_idx_safe_val NVARCHAR(MAX);
            
            -- Parse: "PhoneBook[home]" -> field="PhoneBook", key="home"
            SET @dict_idx_field_name = LEFT(@key, CHARINDEX('[', @key) - 1);
            SET @dict_idx_key = SUBSTRING(@key, CHARINDEX('[', @key) + 1, CHARINDEX(']', @key) - CHARINDEX('[', @key) - 1);
            
            -- Find structure and db_type for dictionary field from metadata cache
            SELECT @dict_idx_structure_id = c._structure_id, @dict_idx_db_type = c.db_type
            FROM _scheme_metadata_cache c
            WHERE c._scheme_id = @scheme_id 
              AND c._name = @dict_idx_field_name
              AND c._parent_structure_id IS NULL;
            
            IF @dict_idx_structure_id IS NOT NULL
            BEGIN
                IF @type = 5  -- object with operators like {"$eq": "value"}
                BEGIN
                    DECLARE dict_idx_cursor CURSOR LOCAL FAST_FORWARD FOR
                        SELECT [key], [value], [type] FROM OPENJSON(@value);
                    
                    OPEN dict_idx_cursor;
                    FETCH NEXT FROM dict_idx_cursor INTO @dict_idx_op, @dict_idx_val, @dict_idx_val_type;
                    
                    WHILE @@FETCH_STATUS = 0
                    BEGIN
                        SET @dict_idx_safe_val = REPLACE(@dict_idx_val, '''', '''''');
                        
                        -- Build value condition based on operator AND db_type
                        IF @dict_idx_op IN ('$gt', '$gte', '$lt', '$lte')
                        BEGIN
                            -- Numeric comparisons - use db_type for correct column
                            SET @dict_idx_value_cond = CASE @dict_idx_db_type
                                WHEN 'Numeric' THEN N'dv._Numeric ' + 
                                    CASE @dict_idx_op WHEN '$gt' THEN '>' WHEN '$gte' THEN '>=' WHEN '$lt' THEN '<' WHEN '$lte' THEN '<=' END +
                                    N' ' + @dict_idx_val
                                WHEN 'Double' THEN N'dv.[_Double] ' + 
                                    CASE @dict_idx_op WHEN '$gt' THEN '>' WHEN '$gte' THEN '>=' WHEN '$lt' THEN '<' WHEN '$lte' THEN '<=' END +
                                    N' ' + @dict_idx_val
                                ELSE N'dv._Long ' + 
                                    CASE @dict_idx_op WHEN '$gt' THEN '>' WHEN '$gte' THEN '>=' WHEN '$lt' THEN '<' WHEN '$lte' THEN '<=' END +
                                    N' ' + @dict_idx_val
                            END;
                        END
                        ELSE IF @dict_idx_op = '$in'
                        BEGIN
                            -- $in operator - use db_type for correct column
                            SET @dict_idx_value_cond = CASE @dict_idx_db_type
                                WHEN 'Long' THEN N'dv._Long IN (SELECT CAST([value] AS BIGINT) FROM OPENJSON(N''' + @dict_idx_safe_val + N'''))'
                                WHEN 'Numeric' THEN N'dv._Numeric IN (SELECT CAST([value] AS DECIMAL(38,18)) FROM OPENJSON(N''' + @dict_idx_safe_val + N'''))'
                                WHEN 'Double' THEN N'dv.[_Double] IN (SELECT CAST([value] AS FLOAT) FROM OPENJSON(N''' + @dict_idx_safe_val + N'''))'
                                ELSE N'dv._String IN (SELECT [value] FROM OPENJSON(N''' + @dict_idx_safe_val + N'''))'
                            END;
                        END
                        ELSE IF @dict_idx_op = '$nin'
                        BEGIN
                            -- $nin operator - use db_type for correct column
                            SET @dict_idx_value_cond = CASE @dict_idx_db_type
                                WHEN 'Long' THEN N'dv._Long NOT IN (SELECT CAST([value] AS BIGINT) FROM OPENJSON(N''' + @dict_idx_safe_val + N'''))'
                                WHEN 'Numeric' THEN N'dv._Numeric NOT IN (SELECT CAST([value] AS DECIMAL(38,18)) FROM OPENJSON(N''' + @dict_idx_safe_val + N'''))'
                                WHEN 'Double' THEN N'dv.[_Double] NOT IN (SELECT CAST([value] AS FLOAT) FROM OPENJSON(N''' + @dict_idx_safe_val + N'''))'
                                ELSE N'dv._String NOT IN (SELECT [value] FROM OPENJSON(N''' + @dict_idx_safe_val + N'''))'
                            END;
                        END
                        ELSE
                        BEGIN
                            -- Other operators
                            SET @dict_idx_value_cond = CASE @dict_idx_op
                                -- Equality/Inequality - use db_type for correct column
                                WHEN '$eq' THEN CASE @dict_idx_db_type
                                    WHEN 'Long' THEN N'dv._Long = ' + @dict_idx_val
                                    WHEN 'Numeric' THEN N'dv._Numeric = ' + @dict_idx_val
                                    WHEN 'Double' THEN N'dv.[_Double] = ' + @dict_idx_val
                                    ELSE N'dv._String = N''' + @dict_idx_safe_val + N''''
                                END
                                WHEN '$ne' THEN CASE @dict_idx_db_type
                                    WHEN 'Long' THEN N'dv._Long <> ' + @dict_idx_val
                                    WHEN 'Numeric' THEN N'dv._Numeric <> ' + @dict_idx_val
                                    WHEN 'Double' THEN N'dv.[_Double] <> ' + @dict_idx_val
                                    ELSE N'dv._String <> N''' + @dict_idx_safe_val + N''''
                                END
                                -- String operations (always use _String)
                                WHEN '$contains' THEN N'dv._String LIKE N''%' + @dict_idx_safe_val + N'%'''
                                WHEN '$startsWith' THEN N'dv._String LIKE N''' + @dict_idx_safe_val + N'%'''
                                WHEN '$endsWith' THEN N'dv._String LIKE N''%' + @dict_idx_safe_val + N''''
                                WHEN '$containsIgnoreCase' THEN N'dv._String LIKE N''%' + @dict_idx_safe_val + N'%'' COLLATE Latin1_General_CI_AS'
                                WHEN '$startsWithIgnoreCase' THEN N'dv._String LIKE N''' + @dict_idx_safe_val + N'%'' COLLATE Latin1_General_CI_AS'
                                WHEN '$endsWithIgnoreCase' THEN N'dv._String LIKE N''%' + @dict_idx_safe_val + N''' COLLATE Latin1_General_CI_AS'
                                ELSE CASE @dict_idx_db_type
                                    WHEN 'Long' THEN N'dv._Long = ' + @dict_idx_val
                                    WHEN 'Numeric' THEN N'dv._Numeric = ' + @dict_idx_val
                                    WHEN 'Double' THEN N'dv.[_Double] = ' + @dict_idx_val
                                    ELSE N'dv._String = N''' + @dict_idx_safe_val + N''''
                                END
                            END;
                        END;
                        
                        SET @dict_idx_condition = 
                            N'EXISTS (SELECT 1 FROM _values dv WHERE dv._id_object = ' + @table_alias + N'._id ' +
                            N'AND dv._id_structure = ' + CAST(@dict_idx_structure_id AS NVARCHAR(20)) + N' ' +
                            N'AND dv._array_index = N''' + REPLACE(@dict_idx_key, '''', '''''') + N''' ' +
                            N'AND ' + @dict_idx_value_cond + N')';
                        
                        INSERT INTO @conditions (condition) VALUES (@dict_idx_condition);
                        
                        FETCH NEXT FROM dict_idx_cursor INTO @dict_idx_op, @dict_idx_val, @dict_idx_val_type;
                    END;
                    
                    CLOSE dict_idx_cursor;
                    DEALLOCATE dict_idx_cursor;
                END
                ELSE
                BEGIN
                    -- Simple value: direct equality (use db_type for correct column)
                    SET @dict_idx_safe_val = REPLACE(@value, '''', '''''');
                    SET @dict_idx_condition = 
                        N'EXISTS (SELECT 1 FROM _values dv WHERE dv._id_object = ' + @table_alias + N'._id ' +
                        N'AND dv._id_structure = ' + CAST(@dict_idx_structure_id AS NVARCHAR(20)) + N' ' +
                        N'AND dv._array_index = N''' + REPLACE(@dict_idx_key, '''', '''''') + N''' ' +
                        N'AND ' + CASE @dict_idx_db_type
                            WHEN 'Long' THEN N'dv._Long = ' + @value
                            WHEN 'Numeric' THEN N'dv._Numeric = ' + @value
                            WHEN 'Double' THEN N'dv.[_Double] = ' + @value
                            ELSE N'dv._String = N''' + @dict_idx_safe_val + N''''
                        END + N')';
                    
                    INSERT INTO @conditions (condition) VALUES (@dict_idx_condition);
                END;
            END;
        END
        -- Dictionary ContainsKey: FieldName.ContainsKey (e.g.: "PhoneBook.ContainsKey": "home")
        ELSE IF @key LIKE '%.ContainsKey'
        BEGIN
            DECLARE @dict_field_name NVARCHAR(450) = LEFT(@key, LEN(@key) - LEN('.ContainsKey'));
            DECLARE @dict_key NVARCHAR(450);
            DECLARE @dict_structure_id BIGINT;
            
            -- Get the key value (handle both {"$eq": "key"} and "key")
            IF @type = 5 AND JSON_VALUE(@value, '$."$eq"') IS NOT NULL
                SET @dict_key = JSON_VALUE(@value, '$."$eq"');
            ELSE
                SET @dict_key = @value;
            
            -- Remove quotes from string value
            SET @dict_key = REPLACE(REPLACE(@dict_key, '"', ''), '''', '');
            
            -- Find structure for dictionary field
            SELECT @dict_structure_id = _id
            FROM _structures 
            WHERE _id_scheme = @scheme_id 
              AND _name = @dict_field_name
              AND _id_parent IS NULL;
            
            IF @dict_structure_id IS NOT NULL
            BEGIN
                -- Build EXISTS condition
                -- Check BOTH cases:
                --   1) Dictionary<K, primitive>: records in the structure itself (_id_structure = dict_id)
                --   2) Dictionary<K, Class>: records in child structures (_id_parent = dict_id)
                INSERT INTO @conditions (condition) VALUES (
                    N'EXISTS (SELECT 1 FROM _values dv JOIN _structures s ON dv._id_structure = s._id ' +
                    N'WHERE dv._id_object = ' + @table_alias + N'._id ' +
                    N'AND (s._id = ' + CAST(@dict_structure_id AS NVARCHAR(20)) + N' OR s._id_parent = ' + CAST(@dict_structure_id AS NVARCHAR(20)) + N') ' +
                    N'AND dv._array_index = N''' + REPLACE(@dict_key, '''', '''''') + N''')'
                );
            END;
        END
        -- Props field
        ELSE IF @key NOT LIKE '$%'
        BEGIN
            IF @type = 5  -- object with operators
            BEGIN
                DECLARE op_cursor CURSOR LOCAL FAST_FORWARD FOR
                    SELECT [key], [value], [type] FROM OPENJSON(@value);
                
                OPEN op_cursor;
                FETCH NEXT FROM op_cursor INTO @op_name, @op_value, @op_type;
                
                WHILE @@FETCH_STATUS = 0
                BEGIN
                    SET @op_value_type = CASE @op_type WHEN 1 THEN 'string' WHEN 2 THEN 'number' WHEN 3 THEN 'boolean' WHEN 4 THEN 'array' ELSE 'string' END;
                    EXEC dbo.internal_build_exists @scheme_id, @key, @op_name, @op_value, @op_value_type, @table_alias, @exists_sql OUTPUT;
                    
                    IF @exists_sql IS NOT NULL AND @exists_sql <> N'' AND @exists_sql <> N'1=1'
                        INSERT INTO @conditions (condition) VALUES (@exists_sql);
                    
                    FETCH NEXT FROM op_cursor INTO @op_name, @op_value, @op_type;
                END;
                
                CLOSE op_cursor;
                DEALLOCATE op_cursor;
            END
            ELSE IF @type = 4  -- array - treat as $in
            BEGIN
                EXEC dbo.internal_build_exists @scheme_id, @key, '$in', @value, 'array', @table_alias, @exists_sql OUTPUT;
                IF @exists_sql IS NOT NULL AND @exists_sql <> N'' AND @exists_sql <> N'1=1'
                    INSERT INTO @conditions (condition) VALUES (@exists_sql);
            END
            ELSE
            BEGIN
                -- Simple value - treat as $eq
                SET @op_value_type = CASE @type WHEN 1 THEN 'string' WHEN 2 THEN 'number' WHEN 3 THEN 'boolean' ELSE 'string' END;
                EXEC dbo.internal_build_exists @scheme_id, @key, '$eq', @value, @op_value_type, @table_alias, @exists_sql OUTPUT;
                IF @exists_sql IS NOT NULL AND @exists_sql <> N'' AND @exists_sql <> N'1=1'
                    INSERT INTO @conditions (condition) VALUES (@exists_sql);
            END;
        END;
        
        FETCH NEXT FROM json_cursor INTO @key, @value, @type;
    END;
    
    CLOSE json_cursor;
    DEALLOCATE json_cursor;
    
    -- Combine all conditions (separator must be variable, not expression)
    SET @separator = N' ' + @logical_op + N' ';
    SELECT @result = STRING_AGG(condition, @separator)
    FROM @conditions;
    
    IF @result IS NULL SET @result = N'';
END;
GO

-- =====================================================
-- HELPER: build_facet_field_path
-- Returns full field path including parent structures
-- =====================================================
CREATE FUNCTION dbo.build_facet_field_path(
    @structure_id BIGINT,
    @scheme_id BIGINT
)
RETURNS NVARCHAR(500)
AS
BEGIN
    DECLARE @path NVARCHAR(500) = N'';
    DECLARE @current_id BIGINT = @structure_id;
    DECLARE @current_name NVARCHAR(450);
    DECLARE @parent_id BIGINT;
    DECLARE @collection_type BIGINT;
    DECLARE @depth INT = 0;
    
    WHILE @current_id IS NOT NULL AND @depth < 10
    BEGIN
        SELECT 
            @current_name = _name,
            @parent_id = _parent_structure_id,
            @collection_type = _collection_type
        FROM _scheme_metadata_cache
        WHERE _structure_id = @current_id AND _scheme_id = @scheme_id;
        
        IF @current_name IS NOT NULL
        BEGIN
            IF @path = N''
                SET @path = @current_name + CASE WHEN @collection_type IS NOT NULL THEN N'[]' ELSE N'' END;
            ELSE
                SET @path = @current_name + CASE WHEN @collection_type IS NOT NULL THEN N'[]' ELSE N'' END + N'.' + @path;
        END;
        
        SET @current_id = @parent_id;
        SET @depth = @depth + 1;
    END;
    
    RETURN @path;
END;
GO

-- =====================================================
-- get_facets: Get facet values for a scheme
-- Basic version for backward compatibility
-- =====================================================
CREATE PROCEDURE dbo.get_facets
    @scheme_id BIGINT,
    @result NVARCHAR(MAX) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Ensure metadata cache is populated
    IF NOT EXISTS (SELECT 1 FROM _scheme_metadata_cache WHERE _scheme_id = @scheme_id)
    BEGIN
        EXEC dbo.sync_metadata_cache_for_scheme @scheme_id;
    END;
    
    DECLARE @facets TABLE (field_name NVARCHAR(450), field_values NVARCHAR(MAX));
    
    INSERT INTO @facets (field_name, field_values)
    SELECT 
        c._name,
        N'[' + ISNULL(STRING_AGG(
            CASE 
                WHEN c.db_type = 'String' THEN N'"' + REPLACE(REPLACE(v._String, N'\', N'\\'), N'"', N'\"') + N'"'
                WHEN c.db_type = 'Long' AND c.type_semantic <> '_RObject' THEN CAST(v._Long AS NVARCHAR(30))
                WHEN c.db_type = 'Boolean' THEN CASE WHEN v._Boolean = 1 THEN 'true' ELSE 'false' END
                WHEN c.db_type = 'Double' THEN FORMAT(v._Double, 'G', 'en-US')
                ELSE N'"' + REPLACE(REPLACE(ISNULL(v._String, ''), N'\', N'\\'), N'"', N'\"') + N'"'
            END
        , N','), N'') + N']'
    FROM _scheme_metadata_cache c
    LEFT JOIN (
        SELECT DISTINCT v._id_structure, v._String, v._Long, v._Boolean, v._Double
        FROM _values v
        JOIN _objects o ON o._id = v._id_object
        WHERE o._id_scheme = @scheme_id
          AND v._array_index IS NULL
    ) v ON v._id_structure = c._structure_id
    WHERE c._scheme_id = @scheme_id
      AND c._parent_structure_id IS NULL
      AND c.db_type NOT IN ('Guid', 'ByteArray')
    GROUP BY c._name
    HAVING COUNT(DISTINCT ISNULL(v._String, CAST(v._Long AS NVARCHAR(30)))) <= 100;
    
    SELECT @result = N'{' + ISNULL(STRING_AGG(N'"' + field_name + N'":' + field_values, N','), N'') + N'}'
    FROM @facets;
    
    IF @result IS NULL SET @result = N'{}';
END;
GO

-- =====================================================
-- get_facets_extended: Get facet values with options
-- Supports: filtering, limiting, specific fields, nested fields
-- =====================================================
IF OBJECT_ID('dbo.get_facets_extended', 'P') IS NOT NULL DROP PROCEDURE dbo.get_facets_extended;
GO

CREATE PROCEDURE dbo.get_facets_extended
    @scheme_id BIGINT,
    @fields NVARCHAR(MAX) = NULL,         -- JSON array of field names to include, NULL = all
    @max_values_per_field INT = 100,       -- Max unique values per facet
    @include_counts BIT = 0,               -- Include count of objects per value
    @include_nested BIT = 0,               -- Include nested Class fields
    @filter_prefix NVARCHAR(100) = NULL,   -- Filter fields by prefix
    @result NVARCHAR(MAX) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Ensure metadata cache is populated
    IF NOT EXISTS (SELECT 1 FROM _scheme_metadata_cache WHERE _scheme_id = @scheme_id)
    BEGIN
        EXEC dbo.sync_metadata_cache_for_scheme @scheme_id;
    END;
    
    DECLARE @facets TABLE (
        field_path NVARCHAR(500), 
        field_values NVARCHAR(MAX),
        total_count INT
    );
    
    -- Build field list
    DECLARE @field_filter TABLE (field_name NVARCHAR(450));
    IF @fields IS NOT NULL AND @fields <> N'' AND @fields <> N'[]'
    BEGIN
        INSERT INTO @field_filter (field_name)
        SELECT [value] FROM OPENJSON(@fields);
    END;
    
    -- Get facets for root fields
    INSERT INTO @facets (field_path, field_values, total_count)
    SELECT 
        c._name + CASE WHEN c._collection_type IS NOT NULL THEN N'[]' ELSE N'' END,
        N'[' + ISNULL(STRING_AGG(
            CASE 
                WHEN c.db_type = 'String' THEN N'"' + REPLACE(REPLACE(v._String, N'\', N'\\'), N'"', N'\"') + N'"'
                WHEN c.db_type = 'Long' AND c.type_semantic <> '_RObject' THEN CAST(v._Long AS NVARCHAR(30))
                WHEN c.db_type = 'Boolean' THEN CASE WHEN v._Boolean = 1 THEN 'true' ELSE 'false' END
                WHEN c.db_type = 'Double' THEN FORMAT(v._Double, 'G', 'en-US')
                WHEN c.db_type = 'ListItem' THEN N'"' + REPLACE(REPLACE(ISNULL(li._value, ''), N'\', N'\\'), N'"', N'\"') + N'"'
                ELSE N'"' + REPLACE(REPLACE(ISNULL(v._String, ''), N'\', N'\\'), N'"', N'\"') + N'"'
            END
        , N','), N'') + N']',
        COUNT(DISTINCT v._id_object)
    FROM _scheme_metadata_cache c
    LEFT JOIN (
        SELECT DISTINCT v._id_structure, v._id_object, v._String, v._Long, v._Boolean, v._Double, v._listitem
        FROM _values v
        JOIN _objects o ON o._id = v._id_object
        WHERE o._id_scheme = @scheme_id
    ) v ON v._id_structure = c._structure_id
    LEFT JOIN _list_items li ON c.type_semantic = '_RListItem' AND li._id = v._listitem
    WHERE c._scheme_id = @scheme_id
      AND (@include_nested = 1 OR c._parent_structure_id IS NULL)
      AND c.db_type NOT IN ('Guid', 'ByteArray')
      AND (@filter_prefix IS NULL OR c._name LIKE @filter_prefix + N'%')
      AND (NOT EXISTS (SELECT 1 FROM @field_filter) OR c._name IN (SELECT field_name FROM @field_filter))
    GROUP BY c._name, c._collection_type, c.db_type, c.type_semantic
    HAVING COUNT(DISTINCT COALESCE(v._String, CAST(v._Long AS NVARCHAR(30)), li._value)) <= @max_values_per_field;
    
    -- Build result JSON
    IF @include_counts = 1
    BEGIN
        SELECT @result = N'{' + ISNULL(STRING_AGG(
            N'"' + field_path + N'":{"values":' + field_values + N',"count":' + CAST(total_count AS NVARCHAR(20)) + N'}'
        , N','), N'') + N'}'
        FROM @facets;
    END
    ELSE
    BEGIN
        SELECT @result = N'{' + ISNULL(STRING_AGG(N'"' + field_path + N'":' + field_values, N','), N'') + N'}'
        FROM @facets;
    END;
    
    IF @result IS NULL SET @result = N'{}';
END;
GO

-- =====================================================
-- MAIN: search_objects_with_facets
-- Full featured search with all operators
-- =====================================================
CREATE PROCEDURE dbo.search_objects_with_facets
    @scheme_id BIGINT,
    @facet_filters NVARCHAR(MAX) = NULL,
    @limit_count INT = NULL,
    @offset_count INT = 0,
    @order_by NVARCHAR(MAX) = NULL,
    @max_recursion_depth INT = 10,
    @include_facets BIT = 0,
    @distinct_hash BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Ensure metadata cache is populated
    IF NOT EXISTS (SELECT 1 FROM _scheme_metadata_cache WHERE _scheme_id = @scheme_id)
    BEGIN
        EXEC dbo.sync_metadata_cache_for_scheme @scheme_id;
    END;
    
    -- Parse filters
    DECLARE @where_clause NVARCHAR(MAX) = N'';
    IF @facet_filters IS NOT NULL AND @facet_filters <> N'' AND @facet_filters <> N'{}' AND @facet_filters <> N'null'
    BEGIN
        EXEC dbo.internal_parse_filters @scheme_id, @facet_filters, N'o', N'AND', 0, @where_clause OUTPUT;
    END;
    
    -- Build ORDER BY clause using unified function
    -- Format: [{"field": "Name", "direction": "ASC"}] - extended format from C#
    DECLARE @order_clause NVARCHAR(MAX) = N'ORDER BY o._id';
    
    IF @order_by IS NOT NULL AND @order_by <> N'' AND @order_by <> N'[]' AND @order_by <> N'null'
    BEGIN
        SET @order_clause = dbo.build_order_by_clause(@order_by, N'o');
        IF @order_clause IS NULL OR @order_clause = N''
            SET @order_clause = N'ORDER BY o._id';
    END;
    
    -- Build and execute query
    DECLARE @sql NVARCHAR(MAX);
    DECLARE @count_sql NVARCHAR(MAX);
    DECLARE @total_count INT;
    
    SET @sql = N'INSERT INTO #temp_ids (_id) SELECT ' + CASE WHEN @distinct_hash = 1 THEN 'DISTINCT' ELSE '' END + N' o._id
        FROM _objects o
        WHERE o._id_scheme = @p_scheme_id
' +
        CASE WHEN @where_clause <> N'' THEN N' AND ' + @where_clause ELSE N'' END + N' ' +
        @order_clause;
    
    IF @limit_count IS NOT NULL AND @limit_count > 0
        SET @sql = @sql + N' OFFSET ' + CAST(@offset_count AS NVARCHAR(20)) + 
                         N' ROWS FETCH NEXT ' + CAST(@limit_count AS NVARCHAR(20)) + N' ROWS ONLY';
    ELSE
        SET @sql = @sql + N' OFFSET 0 ROWS';  -- Allow ORDER BY without FETCH for count queries
    
    SET @sql = @sql + N'; SELECT @p_count = @@ROWCOUNT;';
    
    -- Count query
    SET @count_sql = N'SELECT @p_total = COUNT(' + CASE WHEN @distinct_hash = 1 THEN 'DISTINCT o._hash' ELSE '*' END + N')
        FROM _objects o
        WHERE o._id_scheme = @p_scheme_id
' +
        CASE WHEN @where_clause <> N'' THEN N' AND ' + @where_clause ELSE N'' END;
    
    -- Execute (use _sort_order to preserve ORDER BY)
    CREATE TABLE #temp_ids (_sort_order INT IDENTITY(1,1), _id BIGINT);
    DECLARE @row_count INT;
    
    -- OPTIMIZATION: For CountAsync (limit = 0), only execute count query, skip temp table creation
    IF @limit_count IS NULL OR @limit_count > 0
    BEGIN
        EXEC sp_executesql @sql, N'@p_scheme_id BIGINT, @p_count INT OUTPUT', 
            @p_scheme_id = @scheme_id, @p_count = @row_count OUTPUT;
    END;
    
    EXEC sp_executesql @count_sql, N'@p_scheme_id BIGINT, @p_total INT OUTPUT', 
        @p_scheme_id = @scheme_id, @p_total = @total_count OUTPUT;
    
    -- Build result JSON using function (no cursor - much faster!)
    DECLARE @objects_json NVARCHAR(MAX) = N'[]';
    DECLARE @facets_json NVARCHAR(MAX) = N'{}';
    
    -- OPTIMIZATION: Skip loading objects if limit = 0 (CountAsync case)
    IF @limit_count IS NULL OR @limit_count > 0
    BEGIN
        -- STRING_AGG with WITHIN GROUP to preserve ORDER BY from INSERT
        SELECT @objects_json = N'[' + ISNULL(STRING_AGG(
            dbo.get_object_json(t._id, @max_recursion_depth), N','
        ) WITHIN GROUP (ORDER BY t._sort_order), N'') + N']'
        FROM #temp_ids t;
    END;
    
    IF @include_facets = 1
        EXEC dbo.get_facets @scheme_id, @facets_json OUTPUT;
    
    -- Return result with NULL protection
    SELECT N'{"objects":' + ISNULL(@objects_json, N'[]') + 
           N',"total_count":' + CAST(ISNULL(@total_count, 0) AS NVARCHAR(20)) + 
           N',"limit":' + ISNULL(CAST(@limit_count AS NVARCHAR(20)), N'null') + 
           N',"offset":' + CAST(ISNULL(@offset_count, 0) AS NVARCHAR(20)) + 
           N',"facets":' + ISNULL(@facets_json, N'[]') + N'}' AS result;
    
    DROP TABLE #temp_ids;
END;
GO

-- =====================================================
-- search_tree_objects_with_facets
-- Search children/descendants with filters
-- Supports MULTIPLE parent_ids (comma-separated string)
-- =====================================================
CREATE PROCEDURE dbo.search_tree_objects_with_facets
    @scheme_id BIGINT,
    @parent_ids NVARCHAR(MAX),  -- Comma-separated list of parent IDs (e.g. '123,456,789') or NULL/empty for global search
    @facet_filters NVARCHAR(MAX) = NULL,
    @limit_count INT = NULL,
    @offset_count INT = 0,
    @order_by NVARCHAR(MAX) = NULL,
    @max_depth INT = 10,
    @max_recursion_depth INT = 10,
    @include_facets BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    
    IF NOT EXISTS (SELECT 1 FROM _scheme_metadata_cache WHERE _scheme_id = @scheme_id)
        EXEC dbo.sync_metadata_cache_for_scheme @scheme_id;
    
    DECLARE @where_clause NVARCHAR(MAX) = N'';
    IF @facet_filters IS NOT NULL AND @facet_filters <> N'' AND @facet_filters <> N'{}' AND @facet_filters <> N'null'
        EXEC dbo.internal_parse_filters @scheme_id, @facet_filters, N'd', N'AND', 0, @where_clause OUTPUT;
    
    -- Build ORDER BY clause from @order_by parameter (use 'd' alias)
    DECLARE @order_clause NVARCHAR(MAX) = N'ORDER BY d._id';
    IF @order_by IS NOT NULL AND @order_by <> N'' AND @order_by <> N'[]' AND @order_by <> N'null'
    BEGIN
        SET @order_clause = dbo.build_order_by_clause(@order_by, N'd');
        IF @order_clause IS NULL OR @order_clause = N''
            SET @order_clause = N'ORDER BY d._id';
    END;
    
    DECLARE @sql NVARCHAR(MAX);
    DECLARE @total_count INT;
    DECLARE @objects_json NVARCHAR(MAX);
    DECLARE @facets_json NVARCHAR(MAX) = N'{}';
    
    -- Use temp table with sort_order to preserve ORDER BY
    CREATE TABLE #tree_ids (_sort_order INT IDENTITY(1,1), _id BIGINT);
    
    -- Check if parent_ids is empty or NULL - global search mode
    DECLARE @has_parents BIT = CASE 
        WHEN @parent_ids IS NULL OR LTRIM(RTRIM(@parent_ids)) = N'' THEN 0 
        ELSE 1 
    END;
    
    -- CASE 1: Global search (no parent_ids) - search entire scheme with hierarchical filters
    IF @has_parents = 0
    BEGIN
        SET @sql = N'INSERT INTO #tree_ids (_id) SELECT d._id FROM _objects d
            WHERE d._id_scheme = @p_scheme
' +
            CASE WHEN @where_clause <> N'' THEN N' AND ' + @where_clause ELSE N'' END +
            N' ' + @order_clause;
        
        IF @limit_count IS NOT NULL AND @limit_count > 0
            SET @sql = @sql + N' OFFSET ' + CAST(@offset_count AS NVARCHAR(20)) + 
                             N' ROWS FETCH NEXT ' + CAST(@limit_count AS NVARCHAR(20)) + N' ROWS ONLY';
        ELSE
            SET @sql = @sql + N' OFFSET 0 ROWS';
    END
    -- CASE 2: Direct children only (max_depth = 1)
    ELSE IF @max_depth = 1
    BEGIN
        SET @sql = N'INSERT INTO #tree_ids (_id) SELECT d._id FROM _objects d
            WHERE d._id_scheme = @p_scheme 
              AND d._id_parent IN (SELECT CAST(value AS BIGINT) FROM STRING_SPLIT(@p_parents, N'','') WHERE LTRIM(RTRIM(value)) <> N'''')
' +
            CASE WHEN @where_clause <> N'' THEN N' AND ' + @where_clause ELSE N'' END +
            N' ' + @order_clause;
        
        IF @limit_count IS NOT NULL AND @limit_count > 0
            SET @sql = @sql + N' OFFSET ' + CAST(@offset_count AS NVARCHAR(20)) + 
                             N' ROWS FETCH NEXT ' + CAST(@limit_count AS NVARCHAR(20)) + N' ROWS ONLY';
        ELSE
            SET @sql = @sql + N' OFFSET 0 ROWS';
    END
    -- CASE 3: Recursive descendants from multiple parents
    ELSE
    BEGIN
        SET @sql = N';WITH descendants AS (
            SELECT CAST(value AS BIGINT) AS _id, 0 AS depth 
            FROM STRING_SPLIT(@p_parents, N'','') 
            WHERE LTRIM(RTRIM(value)) <> N''''
            UNION ALL
            SELECT o._id, d.depth + 1 FROM _objects o JOIN descendants d ON o._id_parent = d._id WHERE d.depth < ' + CAST(@max_depth AS NVARCHAR(10)) + N'
        )
        INSERT INTO #tree_ids (_id) SELECT d._id FROM descendants dt
        JOIN _objects d ON dt._id = d._id
        WHERE dt.depth > 0 AND d._id_scheme = @p_scheme
' +
        CASE WHEN @where_clause <> N'' THEN N' AND ' + @where_clause ELSE N'' END +
        N' ' + @order_clause;
        
        IF @limit_count IS NOT NULL AND @limit_count > 0
            SET @sql = @sql + N' OFFSET ' + CAST(@offset_count AS NVARCHAR(20)) + 
                             N' ROWS FETCH NEXT ' + CAST(@limit_count AS NVARCHAR(20)) + N' ROWS ONLY';
        ELSE
            SET @sql = @sql + N' OFFSET 0 ROWS';
    END;
    
    -- Execute the query
    EXEC sp_executesql @sql, N'@p_scheme BIGINT, @p_parents NVARCHAR(MAX)', @p_scheme = @scheme_id, @p_parents = @parent_ids;
    SELECT @total_count = COUNT(*) FROM #tree_ids;
    
    -- Build JSON using function with ORDER BY to preserve sort order!
    SET @objects_json = N'[]';
    
    -- OPTIMIZATION: Skip loading objects if limit = 0 (CountAsync case)
    IF @limit_count IS NULL OR @limit_count > 0
    BEGIN
        -- STRING_AGG with WITHIN GROUP to preserve order from INSERT
        SELECT @objects_json = N'[' + ISNULL(STRING_AGG(
            dbo.get_object_json(t._id, @max_recursion_depth), N','
        ) WITHIN GROUP (ORDER BY t._sort_order), N'') + N']'
        FROM #tree_ids t;
    END;
    
    IF @include_facets = 1
        EXEC dbo.get_facets @scheme_id, @facets_json OUTPUT;
    
    -- Return result with NULL protection
    SELECT N'{"objects":' + ISNULL(@objects_json, N'[]') + 
           N',"total_count":' + CAST(ISNULL(@total_count, 0) AS NVARCHAR(20)) + 
           N',"limit":' + ISNULL(CAST(@limit_count AS NVARCHAR(20)), N'null') + 
           N',"offset":' + CAST(ISNULL(@offset_count, 0) AS NVARCHAR(20)) + 
           N',"parent_ids":"' + ISNULL(@parent_ids, N'') + N'"' +
           N',"max_depth":' + CAST(ISNULL(@max_depth, 10) AS NVARCHAR(10)) + 
           N',"facets":' + ISNULL(@facets_json, N'[]') + N'}' AS result;
    
    DROP TABLE #tree_ids;
END;
GO

PRINT '============================================================='
PRINT 'FULL Facets Search Module Created!'
PRINT ''
PRINT 'MAIN PROCEDURES:'
PRINT '  search_objects_with_facets     - Search with JSON filters'
PRINT '  search_tree_objects_with_facets - Search in hierarchy'
PRINT '  get_facets                     - Get facet values for scheme'
PRINT ''
PRINT 'HELPER FUNCTIONS:'
PRINT '  get_object_level(object_id)         - Get hierarchy level'
PRINT '  is_ancestor_of(object_id, anc_id)   - Check ancestor'
PRINT '  is_descendant_of(object_id, desc_id) - Check descendant'
PRINT '  normalize_base_field_name(name)     - Map field names'
PRINT '  get_value_column_by_type(type,sem)  - Get value column'
PRINT ''
PRINT 'OPERATORS SUPPORTED:'
PRINT '  Comparison: $eq, $ne, $gt, $gte, $lt, $lte'
PRINT '  String: $contains, $startsWith, $endsWith (+ IgnoreCase)'
PRINT '  List: $in, $exists'
PRINT '  Logical: $and, $or, $not'
PRINT '  Array: $arrayContains, $arrayAny, $arrayEmpty, $arrayCount*'
PRINT '  Hierarchy: $isRoot, $isLeaf, $hasAncestor, $hasDescendant, $level'
PRINT ''
PRINT 'FIELD TYPES SUPPORTED:'
PRINT '  Base fields: 0$:name, 0$:date_create, etc.'
PRINT '  Simple fields: Status, Name'
PRINT '  Class fields: Contact.Name, Address.City'
PRINT '  Arrays: Tags[], Contacts[].Email'
PRINT '  Dictionary: PhoneBook[home], Settings[theme]'
PRINT ''
PRINT 'USAGE EXAMPLE:'
PRINT '  EXEC search_objects_with_facets '
PRINT '    @scheme_id = 1,'
PRINT '    @facet_filters = N''{"Status":{"$eq":"Active"},"$and":[{"Age":{"$gte":18}}]}'''
PRINT '  preview_facet_query            - Show generated SQL (debug)'
PRINT '============================================================='
GO

-- =====================================================
-- DEBUG: preview_facet_query
-- Shows generated SQL without execution (for debugging)
-- =====================================================
CREATE PROCEDURE dbo.preview_facet_query
    @scheme_id BIGINT,
    @facet_filters NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Diagnostics
    DECLARE @cache_count INT;
    DECLARE @scheme_exists BIT = 0;
    DECLARE @cache_populated BIT = 0;
    
    -- Check if scheme exists
    IF EXISTS (SELECT 1 FROM _schemes WHERE _id = @scheme_id)
        SET @scheme_exists = 1;
    
    -- Check cache before sync
    SELECT @cache_count = COUNT(*) FROM _scheme_metadata_cache WHERE _scheme_id = @scheme_id;
    
    -- Ensure metadata cache is populated
    IF @cache_count = 0
    BEGIN
        EXEC dbo.sync_metadata_cache_for_scheme @scheme_id;
        SELECT @cache_count = COUNT(*) FROM _scheme_metadata_cache WHERE _scheme_id = @scheme_id;
        IF @cache_count > 0
            SET @cache_populated = 1;
    END
    ELSE
        SET @cache_populated = 1;
    
    -- Parse filters
    DECLARE @where_clause NVARCHAR(MAX) = N'';
    IF @facet_filters IS NOT NULL AND @facet_filters <> N'' AND @facet_filters <> N'{}' AND @facet_filters <> N'null'
    BEGIN
        EXEC dbo.internal_parse_filters @scheme_id, @facet_filters, N'o', N'AND', 0, @where_clause OUTPUT;
    END;
    
    -- Build preview SQL
    DECLARE @preview_sql NVARCHAR(MAX);
    SET @preview_sql = N'SELECT o._id, o._name, o._id_scheme
FROM _objects o
WHERE o._id_scheme = ' + CAST(@scheme_id AS NVARCHAR(20)) + N'
';
    
    IF @where_clause IS NOT NULL AND @where_clause <> N''
        SET @preview_sql = @preview_sql + N'
  AND ' + @where_clause;
    
    SET @preview_sql = @preview_sql + N'
ORDER BY o._id';
    
    -- Return preview with diagnostics
    SELECT 
        @scheme_id AS scheme_id,
        @scheme_exists AS scheme_exists,
        @cache_count AS metadata_cache_entries,
        @cache_populated AS cache_ok,
        @facet_filters AS input_filters,
        @where_clause AS generated_where_clause,
        @preview_sql AS full_sql_preview;
    
    -- Also return available fields in cache for debugging
    SELECT 
        _name AS field_name,
        db_type,
        type_semantic,
        CASE WHEN _collection_type IS NOT NULL THEN 'Array/Dict' ELSE 'Simple' END AS field_type,
        CASE WHEN _parent_structure_id IS NULL THEN 'Root' ELSE 'Nested' END AS level
    FROM _scheme_metadata_cache
    WHERE _scheme_id = @scheme_id
    ORDER BY CASE WHEN _parent_structure_id IS NULL THEN 0 ELSE 1 END, _name;
END;
GO

PRINT 'preview_facet_query procedure created!'
GO

-- =====================================================
-- DEBUG: preview_tree_facet_query
-- Shows generated SQL for tree search (for debugging)
-- =====================================================
IF OBJECT_ID('dbo.preview_tree_facet_query', 'P') IS NOT NULL DROP PROCEDURE dbo.preview_tree_facet_query;
GO

CREATE PROCEDURE dbo.preview_tree_facet_query
    @scheme_id BIGINT,
    @parent_id BIGINT,
    @facet_filters NVARCHAR(MAX) = NULL,
    @max_depth INT = 10
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Diagnostics
    DECLARE @cache_count INT;
    DECLARE @scheme_exists BIT = 0;
    DECLARE @parent_exists BIT = 0;
    DECLARE @cache_populated BIT = 0;
    
    -- Check if scheme exists
    IF EXISTS (SELECT 1 FROM _schemes WHERE _id = @scheme_id)
        SET @scheme_exists = 1;
    
    -- Check if parent object exists (NULL = global search, always valid)
    IF @parent_id IS NULL
        SET @parent_exists = 1;
    ELSE IF EXISTS (SELECT 1 FROM _objects WHERE _id = @parent_id)
        SET @parent_exists = 1;
    
    -- Check cache
    SELECT @cache_count = COUNT(*) FROM _scheme_metadata_cache WHERE _scheme_id = @scheme_id;
    
    IF @cache_count = 0
    BEGIN
        EXEC dbo.sync_metadata_cache_for_scheme @scheme_id;
        SELECT @cache_count = COUNT(*) FROM _scheme_metadata_cache WHERE _scheme_id = @scheme_id;
        IF @cache_count > 0
            SET @cache_populated = 1;
    END
    ELSE
        SET @cache_populated = 1;
    
    -- Parse filters
    DECLARE @where_clause NVARCHAR(MAX) = N'';
    IF @facet_filters IS NOT NULL AND @facet_filters <> N'' AND @facet_filters <> N'{}' AND @facet_filters <> N'null'
    BEGIN
        EXEC dbo.internal_parse_filters @scheme_id, @facet_filters, N'd', N'AND', 0, @where_clause OUTPUT;
    END;
    
    -- Build preview SQL
    DECLARE @preview_sql NVARCHAR(MAX);
    
    -- CASE 1: Global search (no parent_id)
    IF @parent_id IS NULL
    BEGIN
        SET @preview_sql = N'-- Global search (no parent_id)
SELECT d._id, d._name, d._id_parent
FROM _objects d
WHERE d._id_scheme = ' + CAST(@scheme_id AS NVARCHAR(20)) + N'
';
    END
    -- CASE 2: Direct children only
    ELSE IF @max_depth = 1
    BEGIN
        SET @preview_sql = N'SELECT d._id, d._name, d._id_parent
FROM _objects d
WHERE d._id_scheme = ' + CAST(@scheme_id AS NVARCHAR(20)) + N'
  AND d._id_parent = ' + CAST(@parent_id AS NVARCHAR(20)) + N'
';
    END
    -- CASE 3: Recursive descendants with CTE
    ELSE
    BEGIN
        SET @preview_sql = N';WITH descendants AS (
    SELECT ' + CAST(@parent_id AS NVARCHAR(20)) + N' AS _id, 0 AS depth
    UNION ALL
    SELECT o._id, d.depth + 1 
    FROM _objects o 
    JOIN descendants d ON o._id_parent = d._id 
    WHERE d.depth < ' + CAST(@max_depth AS NVARCHAR(10)) + N'
)
SELECT d._id, d._name, d._id_parent, dt.depth
FROM descendants dt
JOIN _objects d ON dt._id = d._id
WHERE dt.depth > 0 
  AND d._id_scheme = ' + CAST(@scheme_id AS NVARCHAR(20)) + N'
';
    END;
    
    IF @where_clause IS NOT NULL AND @where_clause <> N''
        SET @preview_sql = @preview_sql + N'
  AND ' + @where_clause;
    
    SET @preview_sql = @preview_sql + N'
ORDER BY d._id';
    
    -- Return preview with diagnostics
    SELECT 
        @scheme_id AS scheme_id,
        @parent_id AS parent_id,
        @max_depth AS max_depth,
        @scheme_exists AS scheme_exists,
        @parent_exists AS parent_exists,
        @cache_count AS metadata_cache_entries,
        @cache_populated AS cache_ok,
        @facet_filters AS input_filters,
        @where_clause AS generated_where_clause,
        @preview_sql AS full_sql_preview;
    
    -- Return fields in cache
    SELECT 
        _name AS field_name,
        db_type,
        type_semantic,
        CASE WHEN _collection_type IS NOT NULL THEN 'Array/Dict' ELSE 'Simple' END AS field_type,
        CASE WHEN _parent_structure_id IS NULL THEN 'Root' ELSE 'Nested' END AS level
    FROM _scheme_metadata_cache
    WHERE _scheme_id = @scheme_id
    ORDER BY CASE WHEN _parent_structure_id IS NULL THEN 0 ELSE 1 END, _name;
END;
GO

PRINT 'preview_tree_facet_query procedure created!'
GO

-- =====================================================
-- SUMMARY AND USAGE EXAMPLES
-- =====================================================
PRINT '============================================================='
PRINT 'REDB FACETS SEARCH MODULE FOR MSSQL - COMPLETE!'
PRINT ''
PRINT 'MAIN PROCEDURES:'
PRINT '  search_objects_with_facets      - Full featured search with JSON filters'
PRINT '  search_tree_objects_with_facets - Search children/descendants'
PRINT '  get_facets                      - Get unique values for UI filters'
PRINT '  get_facets_extended             - Extended facets with options'
PRINT '  preview_facet_query             - Debug: show generated SQL'
PRINT '  preview_tree_facet_query        - Debug: show tree search SQL'
PRINT ''
PRINT 'HELPER FUNCTIONS:'
PRINT '  normalize_base_field_name       - Map C# names to SQL columns'
PRINT '  get_value_column_by_type        - Get _values column for db_type'
PRINT '  get_object_level                - Get hierarchy level (0=root)'
PRINT '  is_ancestor_of                  - Check if object is ancestor'
PRINT '  is_descendant_of                - Check if object is descendant'
PRINT '  build_facet_field_path          - Build full field path for facets'
PRINT ''
PRINT 'SUPPORTED OPERATORS:'
PRINT '  Comparison: $eq, $ne, $gt, $gte, $lt, $lte'
PRINT '  Lists:      $in, $notIn, $between'
PRINT '  String:     $contains, $startsWith, $endsWith (+IgnoreCase)'
PRINT '  Pattern:    $like, $likeIgnoreCase (MSSQL alternative to $regex)'
PRINT '  Full-Text:  $match, $matchPrefix (requires FTS catalog)'
PRINT '  Logical:    $and, $or, $not'
PRINT '  Array:      $arrayContains, $arrayAny, $arrayEmpty, $arrayCount*'
PRINT '  Array Ext:  $arrayFirst, $arrayLast, $arrayAt, $arrayStartsWith, $arrayEndsWith'
PRINT '  Array Agg:  $arraySum, $arrayAvg, $arrayMin, $arrayMax (+Gt/Gte/Lt/Lte)'
PRINT '  Hierarchy:  $isRoot, $isLeaf, $hasAncestor, $hasDescendant, $level'
PRINT '  Existence:  $exists'
PRINT ''
PRINT 'FIELD TYPES:'
PRINT '  Base fields (0$:): 0$:name, 0$:date_create, 0$:id_parent, etc.'
PRINT '  Simple fields:     Status, Name, Age'
PRINT '  Class fields:      Contact.Name, Address.City'
PRINT '  Arrays:            Tags[], Contacts[].Email'
PRINT '  Dictionary:        PhoneBook[home], Settings[theme]'
PRINT '  ListItem:          Status.Value, Roles[].Alias'
PRINT ''
PRINT 'SMART TYPE DETECTION:'
PRINT '  Numbers -> checks Long, Double, Numeric columns'
PRINT '  Booleans (true/false) -> checks _Boolean column'
PRINT '  Dates (YYYY-MM-DD...) -> checks _DateTimeOffset column'
PRINT '  GUIDs -> checks _Guid column'
PRINT '  Strings -> checks _String column'
PRINT ''
PRINT 'ORDER BY OPTIONS:'
PRINT '  Simple:     [{"field": "asc"}, {"0$:date_create": "desc"}]'
PRINT '  With nulls: [{"field": {"dir": "asc", "nulls": "first"}}]'
PRINT '============================================================='
GO

/*
===== USAGE EXAMPLES =====

-- 1. Basic search with filters:
EXEC search_objects_with_facets 
    @scheme_id = 1,
    @facet_filters = N'{"Status": {"$eq": "Active"}, "Age": {"$gte": 18}}';

-- 2. Complex AND/OR logic:
EXEC search_objects_with_facets 
    @scheme_id = 1,
    @facet_filters = N'{
        "$and": [
            {"Status": {"$eq": "Active"}},
            {"$or": [
                {"City": {"$eq": "Moscow"}},
                {"City": {"$eq": "SPb"}}
            ]}
        ]
    }';

-- 3. Array operators:
EXEC search_objects_with_facets 
    @scheme_id = 1,
    @facet_filters = N'{
        "Tags[]": {"$arrayContains": "vip"},
        "Scores[]": {"$arrayCountGt": 3}
    }';

-- 4. Class fields (nested):
EXEC search_objects_with_facets 
    @scheme_id = 1,
    @facet_filters = N'{
        "Contact.Name": {"$contains": "John"},
        "Address.City": {"$in": ["Moscow", "SPb"]}
    }';

-- 5. Hierarchical search:
EXEC search_objects_with_facets 
    @scheme_id = 1,
    @facet_filters = N'{"$isRoot": true, "$hasDescendant": {"scheme_id": 2}}';

-- 6. Base fields (RedbObject columns):
EXEC search_objects_with_facets 
    @scheme_id = 1,
    @facet_filters = N'{
        "0$:name": {"$startsWith": "Test"},
        "0$:date_create": {"$gte": "2024-01-01"}
    }';

-- 7. Dictionary access:
EXEC search_objects_with_facets 
    @scheme_id = 1,
    @facet_filters = N'{"PhoneBook[home]": {"$eq": "+7-999-123-45-67"}}';

-- 8. With sorting and pagination:
EXEC search_objects_with_facets 
    @scheme_id = 1,
    @facet_filters = N'{"Status": {"$eq": "Active"}}',
    @limit_count = 10,
    @offset_count = 0,
    @order_by = N'[{"0$:date_create": "desc"}]';

-- 9. Debug preview (shows generated SQL):
EXEC preview_facet_query 
    @scheme_id = 1,
    @facet_filters = N'{"Status": {"$eq": "Active"}}';

-- 10. Tree search (children of parent):
EXEC search_tree_objects_with_facets 
    @scheme_id = 1,
    @parent_id = 100,
    @max_depth = 1;  -- direct children only

-- 10a. Global tree search (all objects with hierarchical filters):
EXEC search_tree_objects_with_facets 
    @scheme_id = 1,
    @parent_id = NULL,  -- NULL = search entire scheme
    @facet_filters = N'{"$isRoot": true}';  -- find all root objects

-- 10b. Find all leaf objects (no children):
EXEC search_tree_objects_with_facets 
    @scheme_id = 1,
    @parent_id = NULL,
    @facet_filters = N'{"$isLeaf": true}';

-- 11. Between operator (date range):
EXEC search_objects_with_facets 
    @scheme_id = 1,
    @facet_filters = N'{"0$:date_create": {"$between": ["2024-01-01", "2024-12-31"]}}';

-- 12. Not In operator:
EXEC search_objects_with_facets 
    @scheme_id = 1,
    @facet_filters = N'{"Status": {"$notIn": ["Deleted", "Archived"]}}';

-- 13. Full-Text Search (requires FTS catalog):
EXEC search_objects_with_facets 
    @scheme_id = 1,
    @facet_filters = N'{"Description": {"$match": "important AND urgent"}}';

-- 14. LIKE patterns (alternative to regex):
EXEC search_objects_with_facets 
    @scheme_id = 1,
    @facet_filters = N'{"Email": {"$like": "%@company.com"}}';

-- 15. Sorting with NULLS handling:
EXEC search_objects_with_facets 
    @scheme_id = 1,
    @facet_filters = N'{}',
    @order_by = N'[{"Priority": {"dir": "asc", "nulls": "last"}}, {"0$:date_create": "desc"}]';

-- 16. Extended facets with options:
DECLARE @facets NVARCHAR(MAX);
EXEC get_facets_extended 
    @scheme_id = 1,
    @fields = N'["Status", "City", "Tags"]',
    @max_values_per_field = 50,
    @include_counts = 1,
    @result = @facets OUTPUT;
SELECT @facets;

-- 17. Tree search preview (debugging):
EXEC preview_tree_facet_query 
    @scheme_id = 1,
    @parent_id = 100,
    @max_depth = 3,
    @facet_filters = N'{"Status": {"$eq": "Active"}}';

-- 18. ListItem field search:
EXEC search_objects_with_facets 
    @scheme_id = 1,
    @facet_filters = N'{"Status.Value": {"$eq": "Active"}, "Roles[].Alias": {"$arrayContains": "Admin"}}';
*/


-- ===== redb_grouping.sql =====
-- =====================================================
-- REDB EAV GROUPING FUNCTIONS (MSSQL)
-- GroupBy aggregations for EAV model
-- Ported from PostgreSQL version
-- =====================================================

SET NOCOUNT ON;
GO

-- ===== DROP EXISTING OBJECTS =====
IF OBJECT_ID('dbo.aggregate_grouped', 'P') IS NOT NULL 
    DROP PROCEDURE dbo.aggregate_grouped;
GO

IF OBJECT_ID('dbo.aggregate_grouped_preview', 'P') IS NOT NULL 
    DROP PROCEDURE dbo.aggregate_grouped_preview;
GO

IF OBJECT_ID('dbo.aggregate_array_grouped', 'P') IS NOT NULL 
    DROP PROCEDURE dbo.aggregate_array_grouped;
GO

-- =====================================================
-- aggregate_grouped: GroupBy with aggregations
-- =====================================================
-- Parameters:
--   @scheme_id     - Scheme ID
--   @group_fields  - JSON array of grouping fields:
--                    [{"field":"Category","alias":"Category"}]
--   @aggregations  - JSON array of aggregations:
--                    [{"field":"Stock","func":"SUM","alias":"TotalStock"}]
--   @filter_json   - JSON filter (optional)
--
-- Returns: JSON array of groups
-- =====================================================
CREATE PROCEDURE dbo.aggregate_grouped
    @scheme_id BIGINT,
    @group_fields NVARCHAR(MAX),
    @aggregations NVARCHAR(MAX),
    @filter_json NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @select_parts NVARCHAR(MAX) = N'';
    DECLARE @join_parts NVARCHAR(MAX) = N'';
    DECLARE @group_parts NVARCHAR(MAX) = N'';
    DECLARE @where_clause NVARCHAR(MAX) = N'';
    
    DECLARE @join_idx INT = 0;
    DECLARE @join_alias NVARCHAR(10);
    DECLARE @column_name NVARCHAR(50);
    
    DECLARE @field NVARCHAR(255);
    DECLARE @alias NVARCHAR(255);
    DECLARE @func NVARCHAR(20);
    
    DECLARE @structure_id BIGINT;
    DECLARE @db_type NVARCHAR(50);
    DECLARE @dict_key NVARCHAR(255);
    DECLARE @array_index NVARCHAR(50);
    DECLARE @array_condition NVARCHAR(500);
    
    DECLARE @sql NVARCHAR(MAX);
    
    -- =========================================
    -- 1. Process grouping fields
    -- =========================================
    DECLARE @group_cursor CURSOR;
    SET @group_cursor = CURSOR FOR
        SELECT 
            JSON_VALUE(value, '$.field') AS field,
            ISNULL(JSON_VALUE(value, '$.alias'), JSON_VALUE(value, '$.field')) AS alias
        FROM OPENJSON(@group_fields);
    
    OPEN @group_cursor;
    FETCH NEXT FROM @group_cursor INTO @field, @alias;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @field IS NOT NULL
        BEGIN
            -- Check for base field (0$: prefix)
            IF @field LIKE N'0$:%'
            BEGIN
                DECLARE @raw_field NVARCHAR(100) = SUBSTRING(@field, 4, 100);
                DECLARE @sql_column NVARCHAR(100) = dbo.normalize_base_field_name(@raw_field);
                
                IF @sql_column IS NOT NULL
                BEGIN
                    IF @select_parts <> N'' SET @select_parts = @select_parts + N', ';
                    SET @select_parts = @select_parts + N'o.' + @sql_column + N' AS [' + REPLACE(@alias, '''', '''''') + N']';
                    
                    IF @group_parts <> N'' SET @group_parts = @group_parts + N', ';
                    SET @group_parts = @group_parts + N'o.' + @sql_column;
                END
            END
            ELSE
            BEGIN
                -- EAV field
                EXEC resolve_field_path @scheme_id, @field,
                    @structure_id OUTPUT, @db_type OUTPUT, @dict_key OUTPUT, @array_index OUTPUT;
                
                IF @structure_id IS NOT NULL
                BEGIN
                    SET @join_idx = @join_idx + 1;
                    SET @join_alias = N'g' + CAST(@join_idx AS NVARCHAR(10));
                    
                    SET @column_name = CASE @db_type
                        WHEN N'Long' THEN N'_Long'
                        WHEN N'Double' THEN N'_Double'
                        WHEN N'Numeric' THEN N'_Numeric'
                        WHEN N'String' THEN N'_String'
                        WHEN N'Bool' THEN N'_Boolean'
                        WHEN N'ListItem' THEN N'_ListItem'
                        ELSE N'_String'
                    END;
                    
                    IF @select_parts <> N'' SET @select_parts = @select_parts + N', ';
                    SET @select_parts = @select_parts + @join_alias + N'.' + @column_name + N' AS [' + REPLACE(@alias, '''', '''''') + N']';
                    
                    -- Build JOIN (with SQL injection protection)
                    IF @dict_key IS NOT NULL
                        SET @join_parts = @join_parts + N' JOIN _values ' + @join_alias + 
                            N' ON ' + @join_alias + N'._id_object = o._id AND ' + 
                            @join_alias + N'._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) +
                            N' AND ' + @join_alias + N'._array_index = N''' + REPLACE(@dict_key, '''', '''''') + N'''';
                    ELSE IF @array_index IS NOT NULL
                        SET @join_parts = @join_parts + N' JOIN _values ' + @join_alias + 
                            N' ON ' + @join_alias + N'._id_object = o._id AND ' + 
                            @join_alias + N'._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) +
                            N' AND ' + @join_alias + N'._array_index = N''' + REPLACE(@array_index, '''', '''''') + N'''';
                    ELSE
                        SET @join_parts = @join_parts + N' JOIN _values ' + @join_alias + 
                            N' ON ' + @join_alias + N'._id_object = o._id AND ' + 
                            @join_alias + N'._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) +
                            N' AND ' + @join_alias + N'._array_index IS NULL';
                    
                    IF @group_parts <> N'' SET @group_parts = @group_parts + N', ';
                    SET @group_parts = @group_parts + @join_alias + N'.' + @column_name;
                END
            END
        END
        
        FETCH NEXT FROM @group_cursor INTO @field, @alias;
    END
    
    CLOSE @group_cursor;
    DEALLOCATE @group_cursor;
    
    -- =========================================
    -- 2. Process aggregations
    -- =========================================
    DECLARE @agg_cursor CURSOR;
    SET @agg_cursor = CURSOR FOR
        SELECT 
            JSON_VALUE(value, '$.field') AS field,
            UPPER(JSON_VALUE(value, '$.func')) AS func,
            ISNULL(JSON_VALUE(value, '$.alias'), UPPER(JSON_VALUE(value, '$.func'))) AS alias
        FROM OPENJSON(@aggregations);
    
    OPEN @agg_cursor;
    FETCH NEXT FROM @agg_cursor INTO @field, @func, @alias;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- COUNT(*) special case
        IF @func = N'COUNT' AND (@field IS NULL OR @field = N'*' OR @field = N'')
        BEGIN
            IF @select_parts <> N'' SET @select_parts = @select_parts + N', ';
            SET @select_parts = @select_parts + N'COUNT(DISTINCT o._id) AS [' + REPLACE(@alias, '''', '''''') + N']';
            FETCH NEXT FROM @agg_cursor INTO @field, @func, @alias;
            CONTINUE;
        END
        
        IF @field IS NOT NULL
        BEGIN
            -- Check for base field (0$: prefix)
            IF @field LIKE N'0$:%'
            BEGIN
                SET @raw_field = SUBSTRING(@field, 4, 100);
                SET @sql_column = dbo.normalize_base_field_name(@raw_field);
                
                IF @sql_column IS NOT NULL
                BEGIN
                    IF @select_parts <> N'' SET @select_parts = @select_parts + N', ';
                    
                    IF @func IN (N'SUM', N'AVG')
                        SET @select_parts = @select_parts + @func + N'(CAST(o.' + @sql_column + N' AS DECIMAL(38,10))) AS [' + REPLACE(@alias, '''', '''''') + N']';
                    ELSE
                        SET @select_parts = @select_parts + @func + N'(o.' + @sql_column + N') AS [' + REPLACE(@alias, '''', '''''') + N']';
                END
            END
            ELSE
            BEGIN
                -- EAV field
                EXEC resolve_field_path @scheme_id, @field,
                    @structure_id OUTPUT, @db_type OUTPUT, @dict_key OUTPUT, @array_index OUTPUT;
                
                IF @structure_id IS NOT NULL
                BEGIN
                    SET @join_idx = @join_idx + 1;
                    SET @join_alias = N'a' + CAST(@join_idx AS NVARCHAR(10));
                    
                    SET @column_name = CASE @db_type
                        WHEN N'Long' THEN N'_Long'
                        WHEN N'Double' THEN N'_Double'
                        WHEN N'Numeric' THEN N'_Numeric'
                        ELSE N'_Long'
                    END;
                    
                    -- Build array condition
                    IF @dict_key IS NOT NULL
                        SET @array_condition = N' AND ' + @join_alias + N'._array_index = N''' + REPLACE(@dict_key, '''', '''''') + N'''';
                    ELSE IF @array_index IS NOT NULL
                        SET @array_condition = N' AND ' + @join_alias + N'._array_index = N''' + REPLACE(@array_index, '''', '''''') + N'''';
                    ELSE IF @field NOT LIKE N'%[]%'
                        SET @array_condition = N' AND ' + @join_alias + N'._array_index IS NULL';
                    ELSE
                        SET @array_condition = N'';
                    
                    SET @join_parts = @join_parts + N' LEFT JOIN _values ' + @join_alias + 
                        N' ON ' + @join_alias + N'._id_object = o._id AND ' + 
                        @join_alias + N'._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + @array_condition;
                    
                    IF @select_parts <> N'' SET @select_parts = @select_parts + N', ';
                    SET @select_parts = @select_parts + @func + N'(CAST(' + @join_alias + N'.' + @column_name + N' AS DECIMAL(38,10))) AS [' + REPLACE(@alias, '''', '''''') + N']';
                END
            END
        END
        
        FETCH NEXT FROM @agg_cursor INTO @field, @func, @alias;
    END
    
    CLOSE @agg_cursor;
    DEALLOCATE @agg_cursor;
    
    -- =========================================
    -- 3. Process filter
    -- =========================================
    IF @filter_json IS NOT NULL AND @filter_json <> N'' AND @filter_json <> N'null'
    BEGIN
        CREATE TABLE #filtered_ids (_id BIGINT PRIMARY KEY);
        
        INSERT INTO #filtered_ids
        EXEC get_filtered_object_ids @scheme_id, @filter_json, 10;
        
        IF NOT EXISTS (SELECT 1 FROM #filtered_ids)
        BEGIN
            SELECT N'[]' AS result;
            DROP TABLE #filtered_ids;
            RETURN;
        END
        
        SET @where_clause = N' AND o._id IN (SELECT _id FROM #filtered_ids)';
    END
    
    -- =========================================
    -- 4. Build and execute SQL
    -- =========================================
    DECLARE @result NVARCHAR(MAX);
    
    IF @group_parts = N'' OR @group_parts IS NULL
    BEGIN
        -- No grouping (simple aggregation) - still return array for consistency
        SET @sql = N'
            SELECT @result = (
                SELECT ' + @select_parts + N'
                FROM _objects o' + @join_parts + N'
                WHERE o._id_scheme = ' + CAST(@scheme_id AS NVARCHAR(20)) + @where_clause + N'
                FOR JSON PATH
            )';
    END
    ELSE
    BEGIN
        -- With grouping
        SET @sql = N'
            SELECT @result = (
                SELECT ' + @select_parts + N'
                FROM _objects o' + @join_parts + N'
                WHERE o._id_scheme = ' + CAST(@scheme_id AS NVARCHAR(20)) + @where_clause + N'
                GROUP BY ' + @group_parts + N'
                ORDER BY ' + @group_parts + N'
                FOR JSON PATH
            )';
    END
    
    EXEC sp_executesql @sql, N'@result NVARCHAR(MAX) OUTPUT', @result = @result OUTPUT;
    
    -- Return with 'result' column name for C# mapping
    SELECT @result AS result;
    
    IF OBJECT_ID('tempdb..#filtered_ids') IS NOT NULL
        DROP TABLE #filtered_ids;
END;
GO

-- =====================================================
-- aggregate_grouped_preview: SQL preview for debugging
-- =====================================================
CREATE PROCEDURE dbo.aggregate_grouped_preview
    @scheme_id BIGINT,
    @group_fields NVARCHAR(MAX),
    @aggregations NVARCHAR(MAX),
    @filter_json NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @select_parts NVARCHAR(MAX) = N'';
    DECLARE @join_parts NVARCHAR(MAX) = N'';
    DECLARE @group_parts NVARCHAR(MAX) = N'';
    
    DECLARE @join_idx INT = 0;
    DECLARE @join_alias NVARCHAR(10);
    DECLARE @column_name NVARCHAR(50);
    
    DECLARE @field NVARCHAR(255);
    DECLARE @alias NVARCHAR(255);
    DECLARE @func NVARCHAR(20);
    
    DECLARE @structure_id BIGINT;
    DECLARE @db_type NVARCHAR(50);
    DECLARE @dict_key NVARCHAR(255);
    DECLARE @array_index NVARCHAR(50);
    
    DECLARE @sql NVARCHAR(MAX);
    
    -- Process grouping fields
    DECLARE @group_cursor CURSOR;
    SET @group_cursor = CURSOR FOR
        SELECT 
            JSON_VALUE(value, '$.field') AS field,
            ISNULL(JSON_VALUE(value, '$.alias'), JSON_VALUE(value, '$.field')) AS alias
        FROM OPENJSON(@group_fields);
    
    OPEN @group_cursor;
    FETCH NEXT FROM @group_cursor INTO @field, @alias;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        EXEC resolve_field_path @scheme_id, @field,
            @structure_id OUTPUT, @db_type OUTPUT, @dict_key OUTPUT, @array_index OUTPUT;
        
        IF @structure_id IS NOT NULL
        BEGIN
            SET @join_idx = @join_idx + 1;
            SET @join_alias = N'g' + CAST(@join_idx AS NVARCHAR(10));
            SET @column_name = CASE @db_type WHEN N'Long' THEN N'_Long' WHEN N'String' THEN N'_String' WHEN N'ListItem' THEN N'_ListItem' ELSE N'_String' END;
            
            IF @select_parts <> N'' SET @select_parts = @select_parts + N', ';
            SET @select_parts = @select_parts + @join_alias + N'.' + @column_name + N' AS [' + REPLACE(@alias, '''', '''''') + N']';
            
            IF @dict_key IS NOT NULL
                SET @join_parts = @join_parts + N'
JOIN _values ' + @join_alias + N' ON ' + @join_alias + N'._id_object = o._id AND ' + @join_alias + N'._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' AND ' + @join_alias + N'._array_index = N''' + REPLACE(@dict_key, '''', '''''') + N'''';
            ELSE IF @array_index IS NOT NULL
                SET @join_parts = @join_parts + N'
JOIN _values ' + @join_alias + N' ON ' + @join_alias + N'._id_object = o._id AND ' + @join_alias + N'._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' AND ' + @join_alias + N'._array_index = N''' + REPLACE(@array_index, '''', '''''') + N'''';
            ELSE
                SET @join_parts = @join_parts + N'
JOIN _values ' + @join_alias + N' ON ' + @join_alias + N'._id_object = o._id AND ' + @join_alias + N'._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' AND ' + @join_alias + N'._array_index IS NULL';
            
            IF @group_parts <> N'' SET @group_parts = @group_parts + N', ';
            SET @group_parts = @group_parts + @join_alias + N'.' + @column_name;
        END
        
        FETCH NEXT FROM @group_cursor INTO @field, @alias;
    END
    
    CLOSE @group_cursor;
    DEALLOCATE @group_cursor;
    
    -- Process aggregations
    DECLARE @agg_cursor CURSOR;
    SET @agg_cursor = CURSOR FOR
        SELECT 
            JSON_VALUE(value, '$.field') AS field,
            UPPER(JSON_VALUE(value, '$.func')) AS func,
            ISNULL(JSON_VALUE(value, '$.alias'), UPPER(JSON_VALUE(value, '$.func'))) AS alias
        FROM OPENJSON(@aggregations);
    
    OPEN @agg_cursor;
    FETCH NEXT FROM @agg_cursor INTO @field, @func, @alias;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @func = N'COUNT' AND (@field IS NULL OR @field = N'*')
        BEGIN
            IF @select_parts <> N'' SET @select_parts = @select_parts + N', ';
            SET @select_parts = @select_parts + N'COUNT(DISTINCT o._id) AS [' + REPLACE(@alias, '''', '''''') + N']';
        END
        ELSE IF @field IS NOT NULL
        BEGIN
            EXEC resolve_field_path @scheme_id, @field,
                @structure_id OUTPUT, @db_type OUTPUT, @dict_key OUTPUT, @array_index OUTPUT;
            
            IF @structure_id IS NOT NULL
            BEGIN
                SET @join_idx = @join_idx + 1;
                SET @join_alias = N'a' + CAST(@join_idx AS NVARCHAR(10));
                SET @column_name = CASE @db_type WHEN N'Long' THEN N'_Long' ELSE N'_Numeric' END;
                
                SET @join_parts = @join_parts + N'
LEFT JOIN _values ' + @join_alias + N' ON ' + @join_alias + N'._id_object = o._id AND ' + @join_alias + N'._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) + N' AND ' + @join_alias + N'._array_index IS NULL';
                
                IF @select_parts <> N'' SET @select_parts = @select_parts + N', ';
                SET @select_parts = @select_parts + @func + N'(' + @join_alias + N'.' + @column_name + N') AS [' + REPLACE(@alias, '''', '''''') + N']';
            END
        END
        
        FETCH NEXT FROM @agg_cursor INTO @field, @func, @alias;
    END
    
    CLOSE @agg_cursor;
    DEALLOCATE @agg_cursor;
    
    SET @sql = N'SELECT ' + @select_parts + N'
FROM _objects o' + @join_parts + N'
WHERE o._id_scheme = ' + CAST(@scheme_id AS NVARCHAR(20)) + N'
GROUP BY ' + @group_parts;
    
    SELECT @sql AS sql_preview;
END;
GO

-- =====================================================
-- aggregate_array_grouped: GroupBy by array elements
-- =====================================================
CREATE PROCEDURE dbo.aggregate_array_grouped
    @scheme_id BIGINT,
    @array_path NVARCHAR(255),
    @group_fields NVARCHAR(MAX),
    @aggregations NVARCHAR(MAX),
    @filter_json NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @array_struct_id BIGINT;
    DECLARE @structure_id BIGINT;
    DECLARE @db_type NVARCHAR(50);
    DECLARE @dict_key NVARCHAR(255);
    DECLARE @array_index NVARCHAR(50);
    
    DECLARE @select_parts NVARCHAR(MAX) = N'';
    DECLARE @group_parts NVARCHAR(MAX) = N'';
    DECLARE @join_parts NVARCHAR(MAX) = N'';
    
    DECLARE @join_idx INT = 0;
    DECLARE @join_alias NVARCHAR(10);
    DECLARE @column_name NVARCHAR(50);
    
    DECLARE @field NVARCHAR(255);
    DECLARE @alias NVARCHAR(255);
    DECLARE @func NVARCHAR(20);
    DECLARE @full_path NVARCHAR(500);
    
    DECLARE @sql NVARCHAR(MAX);
    
    -- 1. Get array structure_id
    EXEC resolve_field_path @scheme_id, @array_path,
        @array_struct_id OUTPUT, @db_type OUTPUT, @dict_key OUTPUT, @array_index OUTPUT;
    
    IF @array_struct_id IS NULL
    BEGIN
        RAISERROR('Array "%s" not found in scheme %d', 16, 1, @array_path, @scheme_id);
        RETURN;
    END
    
    -- 2. Process grouping fields
    DECLARE @group_cursor CURSOR;
    SET @group_cursor = CURSOR FOR
        SELECT 
            JSON_VALUE(value, '$.field') AS field,
            ISNULL(JSON_VALUE(value, '$.alias'), JSON_VALUE(value, '$.field')) AS alias
        FROM OPENJSON(@group_fields);
    
    OPEN @group_cursor;
    FETCH NEXT FROM @group_cursor INTO @field, @alias;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @full_path = @array_path + N'[].' + @field;
        
        EXEC resolve_field_path @scheme_id, @full_path,
            @structure_id OUTPUT, @db_type OUTPUT, @dict_key OUTPUT, @array_index OUTPUT;
        
        IF @structure_id IS NOT NULL
        BEGIN
            SET @join_idx = @join_idx + 1;
            SET @join_alias = N'g' + CAST(@join_idx AS NVARCHAR(10));
            SET @column_name = CASE @db_type WHEN N'Long' THEN N'_Long' WHEN N'String' THEN N'_String' WHEN N'ListItem' THEN N'_ListItem' ELSE N'_String' END;
            
            SET @join_parts = @join_parts + N' LEFT JOIN _values ' + @join_alias + 
                N' ON ' + @join_alias + N'._id_object = arr._id_object AND ' + 
                @join_alias + N'._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) +
                N' AND ' + @join_alias + N'._array_parent_id = arr._id';
            
            IF @select_parts <> N'' SET @select_parts = @select_parts + N', ';
            SET @select_parts = @select_parts + @join_alias + N'.' + @column_name + N' AS [' + REPLACE(@alias, '''', '''''') + N']';
            
            IF @group_parts <> N'' SET @group_parts = @group_parts + N', ';
            SET @group_parts = @group_parts + @join_alias + N'.' + @column_name;
        END
        
        FETCH NEXT FROM @group_cursor INTO @field, @alias;
    END
    
    CLOSE @group_cursor;
    DEALLOCATE @group_cursor;
    
    -- 3. Process aggregations
    DECLARE @agg_cursor CURSOR;
    SET @agg_cursor = CURSOR FOR
        SELECT 
            JSON_VALUE(value, '$.field') AS field,
            UPPER(JSON_VALUE(value, '$.func')) AS func,
            ISNULL(JSON_VALUE(value, '$.alias'), UPPER(JSON_VALUE(value, '$.func'))) AS alias
        FROM OPENJSON(@aggregations);
    
    OPEN @agg_cursor;
    FETCH NEXT FROM @agg_cursor INTO @field, @func, @alias;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @func = N'COUNT' AND (@field IS NULL OR @field = N'*')
        BEGIN
            IF @select_parts <> N'' SET @select_parts = @select_parts + N', ';
            SET @select_parts = @select_parts + N'COUNT(*) AS [' + REPLACE(@alias, '''', '''''') + N']';
        END
        ELSE IF @field IS NOT NULL
        BEGIN
            SET @full_path = @array_path + N'[].' + @field;
            
            EXEC resolve_field_path @scheme_id, @full_path,
                @structure_id OUTPUT, @db_type OUTPUT, @dict_key OUTPUT, @array_index OUTPUT;
            
            IF @structure_id IS NOT NULL
            BEGIN
                SET @join_idx = @join_idx + 1;
                SET @join_alias = N'a' + CAST(@join_idx AS NVARCHAR(10));
                SET @column_name = CASE @db_type WHEN N'Long' THEN N'_Long' WHEN N'Double' THEN N'_Double' ELSE N'_Long' END;
                
                SET @join_parts = @join_parts + N' LEFT JOIN _values ' + @join_alias + 
                    N' ON ' + @join_alias + N'._id_object = arr._id_object AND ' + 
                    @join_alias + N'._id_structure = ' + CAST(@structure_id AS NVARCHAR(20)) +
                    N' AND ' + @join_alias + N'._array_parent_id = arr._id';
                
                IF @select_parts <> N'' SET @select_parts = @select_parts + N', ';
                SET @select_parts = @select_parts + @func + N'(' + @join_alias + N'.' + @column_name + N') AS [' + REPLACE(@alias, '''', '''''') + N']';
            END
        END
        
        FETCH NEXT FROM @agg_cursor INTO @field, @func, @alias;
    END
    
    CLOSE @agg_cursor;
    DEALLOCATE @agg_cursor;
    
    -- 4. Build and execute SQL
    DECLARE @result NVARCHAR(MAX);
    
    SET @sql = N'
        SELECT @result = (
            SELECT ' + @select_parts + N'
            FROM _values arr
            JOIN _objects o ON o._id = arr._id_object' + @join_parts + N'
            WHERE o._id_scheme = ' + CAST(@scheme_id AS NVARCHAR(20)) + N'
              AND arr._id_structure = ' + CAST(@array_struct_id AS NVARCHAR(20)) + N'
              AND arr._array_index IS NOT NULL
            GROUP BY ' + @group_parts + N'
            FOR JSON PATH
        )';
    
    EXEC sp_executesql @sql, N'@result NVARCHAR(MAX) OUTPUT', @result = @result OUTPUT;
    
    -- Return with 'result' column name for C# mapping
    SELECT @result AS result;
END;
GO

PRINT N'=========================================';
PRINT N'Grouping procedures created!';
PRINT N'';
PRINT N'PROCEDURES:';
PRINT N'  aggregate_grouped - GroupBy with aggregations';
PRINT N'  aggregate_grouped_preview - SQL preview for debugging';
PRINT N'  aggregate_array_grouped - GroupBy by array elements';
PRINT N'';
PRINT N'FEATURES:';
PRINT N'  - Base fields with 0$: prefix';
PRINT N'  - EAV fields from _values';
PRINT N'  - Nested paths: Address.City';
PRINT N'  - Array support: Items[].Price, Items[2].Price';
PRINT N'  - Dictionary support: PhoneBook[home]';
PRINT N'  - Multiple grouping keys';
PRINT N'  - Filters via get_filtered_object_ids';
PRINT N'=========================================';
GO



-- ===== redb_json_objects.sql =====
-- ====================================================================================================
-- JSON OBJECT FUNCTION (not procedure!)
-- MS SQL Server version - OPTIMIZED for use with SELECT + STRING_AGG
-- ====================================================================================================
-- Key difference from get_object_json_internal (procedure):
--   - This is a FUNCTION that can be called in SELECT statements
--   - Reads directly from _values table (no temp tables)
--   - Uses recursive function calls instead of cursors where possible
--   - Designed for batch processing: SELECT dbo.get_object_json(id, 10) FROM ...
-- ====================================================================================================


-- =====================================================
-- DROP EXISTING FUNCTION (if exists)
-- =====================================================
IF OBJECT_ID('dbo.get_object_json', 'FN') IS NOT NULL
    DROP FUNCTION dbo.get_object_json
GO

IF OBJECT_ID('dbo.build_properties', 'FN') IS NOT NULL
    DROP FUNCTION dbo.build_properties
GO

IF OBJECT_ID('dbo.build_field_json', 'FN') IS NOT NULL
    DROP FUNCTION dbo.build_field_json
GO

IF OBJECT_ID('dbo.escape_json_string', 'FN') IS NOT NULL
    DROP FUNCTION dbo.escape_json_string
GO

IF OBJECT_ID('dbo.build_listitem_json', 'FN') IS NOT NULL
    DROP FUNCTION dbo.build_listitem_json
GO

-- =====================================================
-- HELPER: Escape string for JSON
-- =====================================================
CREATE FUNCTION dbo.escape_json_string(@input NVARCHAR(MAX))
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @input IS NULL RETURN NULL;
    
    RETURN REPLACE(
        REPLACE(
            REPLACE(
                REPLACE(
                    REPLACE(@input, N'\', N'\\'),  -- Backslash first!
                    N'"', N'\"'),
                CHAR(13), N'\r'),
            CHAR(10), N'\n'),
        CHAR(9), N'\t');
END
GO

-- =====================================================
-- HELPER: Build ListItem JSON (DRY - used in multiple places)
-- =====================================================
CREATE FUNCTION dbo.build_listitem_json(@listitem_id BIGINT)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @listitem_id IS NULL RETURN NULL;
    
    RETURN (SELECT N'{"id":' + CAST(li._id AS NVARCHAR(20)) + 
                   N',"idList":' + CAST(li._id_list AS NVARCHAR(20)) + 
                   N',"value":' + CASE WHEN li._value IS NULL THEN N'null' 
                                       ELSE N'"' + dbo.escape_json_string(li._value) + N'"' END +
                   N',"alias":' + CASE WHEN li._alias IS NULL THEN N'null' 
                                       ELSE N'"' + dbo.escape_json_string(li._alias) + N'"' END +
                   N'}'
            FROM _list_items li WHERE li._id = @listitem_id);
END
GO

-- =====================================================
-- HELPER: Build single field value as JSON
-- =====================================================
CREATE FUNCTION dbo.build_field_json(
    @object_id BIGINT,
    @structure_id BIGINT,
    @scheme_id BIGINT,
    @parent_structure_id BIGINT,
    @field_name NVARCHAR(450),
    @db_type NVARCHAR(50),
    @type_semantic NVARCHAR(50),
    @collection_type BIGINT,
    @max_depth INT,
    @array_index NVARCHAR(430),
    @parent_value_id BIGINT
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    DECLARE @result NVARCHAR(MAX);
    DECLARE @is_array BIT = CASE WHEN @collection_type = -9223372036854775668 THEN 1 ELSE 0 END;
    DECLARE @is_dictionary BIT = CASE WHEN @collection_type = -9223372036854775667 THEN 1 ELSE 0 END;
    
    -- Get value for this field
    DECLARE @val_id BIGINT, @val_String NVARCHAR(MAX), @val_Long BIGINT, @val_Guid UNIQUEIDENTIFIER;
    DECLARE @val_Double FLOAT, @val_Numeric DECIMAL(38,18), @val_DateTimeOffset DATETIMEOFFSET;
    DECLARE @val_Boolean BIT, @val_ByteArray VARBINARY(MAX), @val_ListItem BIGINT, @val_Object BIGINT;
    
    -- Find the value record
    IF @parent_value_id IS NOT NULL
    BEGIN
        SELECT TOP 1 
            @val_id = _id, @val_String = _String, @val_Long = _Long, @val_Guid = _Guid,
            @val_Double = _Double, @val_Numeric = _Numeric, @val_DateTimeOffset = _DateTimeOffset,
            @val_Boolean = _Boolean, @val_ByteArray = _ByteArray, @val_ListItem = _ListItem, @val_Object = _Object
        FROM _values
        WHERE _id_object = @object_id AND _id_structure = @structure_id AND _array_parent_id = @parent_value_id;
    END
    ELSE IF @array_index IS NOT NULL
    BEGIN
        SELECT TOP 1 
            @val_id = _id, @val_String = _String, @val_Long = _Long, @val_Guid = _Guid,
            @val_Double = _Double, @val_Numeric = _Numeric, @val_DateTimeOffset = _DateTimeOffset,
            @val_Boolean = _Boolean, @val_ByteArray = _ByteArray, @val_ListItem = _ListItem, @val_Object = _Object
        FROM _values
        WHERE _id_object = @object_id AND _id_structure = @structure_id AND _array_index = @array_index;
    END
    ELSE
    BEGIN
        SELECT TOP 1 
            @val_id = _id, @val_String = _String, @val_Long = _Long, @val_Guid = _Guid,
            @val_Double = _Double, @val_Numeric = _Numeric, @val_DateTimeOffset = _DateTimeOffset,
            @val_Boolean = _Boolean, @val_ByteArray = _ByteArray, @val_ListItem = _ListItem, @val_Object = _Object
        FROM _values
        WHERE _id_object = @object_id AND _id_structure = @structure_id AND _array_index IS NULL AND _array_parent_id IS NULL;
    END
    
    -- =====================================================
    -- ARRAYS
    -- =====================================================
    IF @is_array = 1
    BEGIN
        DECLARE @base_value_id BIGINT;
        -- For nested arrays inside array/dict elements, base record has _array_parent_id = @parent_value_id
        IF @parent_value_id IS NOT NULL
            SELECT TOP 1 @base_value_id = _id FROM _values 
            WHERE _id_object = @object_id AND _id_structure = @structure_id 
              AND _array_index IS NULL AND _array_parent_id = @parent_value_id;
        ELSE
            SELECT TOP 1 @base_value_id = _id FROM _values 
            WHERE _id_object = @object_id AND _id_structure = @structure_id 
              AND _array_index IS NULL AND _array_parent_id IS NULL;
        
        IF @type_semantic = '_RObject'
        BEGIN
            -- Array of Object references - recursive call for each
            SELECT @result = N'[' + ISNULL(STRING_AGG(
                CASE WHEN v._Object IS NOT NULL AND @max_depth > 0 
                     THEN dbo.get_object_json(v._Object, @max_depth - 1)
                     ELSE N'null' END
            , N',') WITHIN GROUP (ORDER BY 
                CASE WHEN v._array_index LIKE '[0-9]%' AND ISNUMERIC(v._array_index) = 1 
                     THEN CAST(v._array_index AS INT) ELSE 2147483647 END, v._array_index
            ), N'') + N']'
            FROM _values v
            WHERE v._id_object = @object_id AND v._id_structure = @structure_id
              AND v._array_index IS NOT NULL
              AND (@base_value_id IS NULL OR v._array_parent_id = @base_value_id);
        END
        ELSE IF @type_semantic = 'Object'
        BEGIN
            -- Array of Class - recursive properties for each element
            SELECT @result = N'[' + ISNULL(STRING_AGG(
                dbo.build_properties(@object_id, @scheme_id, @max_depth, @structure_id, v._array_index, v._id)
            , N',') WITHIN GROUP (ORDER BY 
                CASE WHEN v._array_index LIKE '[0-9]%' AND ISNUMERIC(v._array_index) = 1 
                     THEN CAST(v._array_index AS INT) ELSE 2147483647 END, v._array_index
            ), N'') + N']'
            FROM _values v
            WHERE v._id_object = @object_id AND v._id_structure = @structure_id
              AND v._array_index IS NOT NULL
              AND (@base_value_id IS NULL OR v._array_parent_id = @base_value_id);
        END
        ELSE
        BEGIN
            -- Array of primitives (including ListItem)
            SELECT @result = N'[' + ISNULL(STRING_AGG(
                CASE 
                    WHEN @db_type = 'String' THEN 
                        CASE WHEN v._String IS NULL THEN N'null' ELSE N'"' + dbo.escape_json_string(v._String) + N'"' END
                    WHEN @db_type = 'Long' THEN 
                        CASE WHEN v._Long IS NULL THEN N'null' ELSE CAST(v._Long AS NVARCHAR(30)) END
                    WHEN @db_type = 'Guid' THEN 
                        CASE WHEN v._Guid IS NULL THEN N'null' ELSE N'"' + CAST(v._Guid AS NVARCHAR(50)) + N'"' END
                    WHEN @db_type = 'Double' THEN 
                        CASE WHEN v._Double IS NULL THEN N'null' ELSE FORMAT(v._Double, 'G', 'en-US') END
                    WHEN @db_type = 'Numeric' THEN 
                        CASE WHEN v._Numeric IS NULL THEN N'null' ELSE REPLACE(CAST(v._Numeric AS NVARCHAR(50)), N',', N'.') END
                    WHEN @db_type = 'DateTimeOffset' THEN 
                        CASE WHEN v._DateTimeOffset IS NULL THEN N'null' ELSE N'"' + CONVERT(NVARCHAR(50), v._DateTimeOffset, 127) + N'"' END
                    WHEN @db_type = 'Boolean' THEN 
                        CASE WHEN v._Boolean IS NULL THEN N'null' WHEN v._Boolean = 1 THEN N'true' ELSE N'false' END
                    WHEN @db_type = 'ListItem' THEN 
                        CASE WHEN v._ListItem IS NULL THEN N'null'
                             ELSE dbo.build_listitem_json(v._ListItem)
                        END
                    ELSE N'null'
                END
            , N',') WITHIN GROUP (ORDER BY 
                CASE WHEN v._array_index LIKE '[0-9]%' AND ISNUMERIC(v._array_index) = 1 
                     THEN CAST(v._array_index AS INT) ELSE 2147483647 END, v._array_index
            ), N'') + N']'
            FROM _values v
            WHERE v._id_object = @object_id AND v._id_structure = @structure_id
              AND v._array_index IS NOT NULL
              AND (@base_value_id IS NULL OR v._array_parent_id = @base_value_id);
        END
        
        RETURN @result;
    END
    
    -- =====================================================
    -- DICTIONARIES
    -- =====================================================
    IF @is_dictionary = 1
    BEGIN
        DECLARE @dict_base_id BIGINT;
        -- For nested dicts inside array/dict elements, base record has _array_parent_id = @parent_value_id
        IF @parent_value_id IS NOT NULL
            SELECT TOP 1 @dict_base_id = _id FROM _values 
            WHERE _id_object = @object_id AND _id_structure = @structure_id 
              AND _array_index IS NULL AND _array_parent_id = @parent_value_id;
        ELSE
            SELECT TOP 1 @dict_base_id = _id FROM _values 
            WHERE _id_object = @object_id AND _id_structure = @structure_id 
              AND _array_index IS NULL AND _array_parent_id IS NULL;
        
        IF @type_semantic = '_RObject'
        BEGIN
            -- Dictionary of Object references
            SELECT @result = N'{' + ISNULL(STRING_AGG(
                N'"' + dbo.escape_json_string(v._array_index) + N'":' +
                CASE WHEN v._Object IS NOT NULL AND @max_depth > 0 
                     THEN dbo.get_object_json(v._Object, @max_depth - 1)
                     ELSE N'null' END
            , N','), N'') + N'}'
            FROM _values v
            WHERE v._id_object = @object_id AND v._id_structure = @structure_id
              AND v._array_index IS NOT NULL
              AND (@dict_base_id IS NULL OR v._array_parent_id = @dict_base_id);
        END
        ELSE IF @type_semantic = 'Object'
        BEGIN
            -- Dictionary of Class
            SELECT @result = N'{' + ISNULL(STRING_AGG(
                N'"' + dbo.escape_json_string(v._array_index) + N'":' +
                dbo.build_properties(@object_id, @scheme_id, @max_depth, @structure_id, NULL, v._id)
            , N','), N'') + N'}'
            FROM _values v
            WHERE v._id_object = @object_id AND v._id_structure = @structure_id
              AND v._array_index IS NOT NULL
              AND (@dict_base_id IS NULL OR v._array_parent_id = @dict_base_id);
        END
        ELSE
        BEGIN
            -- Dictionary of primitives
            SELECT @result = N'{' + ISNULL(STRING_AGG(
                N'"' + dbo.escape_json_string(v._array_index) + N'":' +
                CASE 
                    WHEN @db_type = 'String' THEN 
                        CASE WHEN v._String IS NULL THEN N'null' ELSE N'"' + dbo.escape_json_string(v._String) + N'"' END
                    WHEN @db_type = 'Long' THEN 
                        CASE WHEN v._Long IS NULL THEN N'null' ELSE CAST(v._Long AS NVARCHAR(30)) END
                    WHEN @db_type = 'Guid' THEN 
                        CASE WHEN v._Guid IS NULL THEN N'null' ELSE N'"' + CAST(v._Guid AS NVARCHAR(50)) + N'"' END
                    WHEN @db_type = 'Double' THEN 
                        CASE WHEN v._Double IS NULL THEN N'null' ELSE FORMAT(v._Double, 'G', 'en-US') END
                    WHEN @db_type = 'Numeric' THEN 
                        CASE WHEN v._Numeric IS NULL THEN N'null' ELSE REPLACE(CAST(v._Numeric AS NVARCHAR(50)), N',', N'.') END
                    WHEN @db_type = 'DateTimeOffset' THEN 
                        CASE WHEN v._DateTimeOffset IS NULL THEN N'null' ELSE N'"' + CONVERT(NVARCHAR(50), v._DateTimeOffset, 127) + N'"' END
                    WHEN @db_type = 'Boolean' THEN 
                        CASE WHEN v._Boolean IS NULL THEN N'null' WHEN v._Boolean = 1 THEN N'true' ELSE N'false' END
                    ELSE N'null'
                END
            , N','), N'') + N'}'
            FROM _values v
            WHERE v._id_object = @object_id AND v._id_structure = @structure_id
              AND v._array_index IS NOT NULL
              AND (@dict_base_id IS NULL OR v._array_parent_id = @dict_base_id);
        END
        
        RETURN @result;
    END
    
    -- =====================================================
    -- _RObject reference (single)
    -- =====================================================
    IF @type_semantic = '_RObject'
    BEGIN
        IF @val_Object IS NOT NULL AND @max_depth > 0
            RETURN dbo.get_object_json(@val_Object, @max_depth - 1);
        RETURN NULL;
    END
    
    -- =====================================================
    -- Nested Class (hierarchical)
    -- =====================================================
    IF @type_semantic = 'Object'
    BEGIN
        IF @val_Guid IS NOT NULL
            RETURN dbo.build_properties(@object_id, @scheme_id, @max_depth, @structure_id, NULL, @val_id);
        RETURN NULL;
    END
    
    -- =====================================================
    -- PRIMITIVES
    -- =====================================================
    IF @val_id IS NULL RETURN NULL;
    
    SET @result = CASE 
        WHEN @db_type = 'String' THEN 
            CASE WHEN @val_String IS NULL THEN NULL ELSE N'"' + dbo.escape_json_string(@val_String) + N'"' END
        WHEN @db_type = 'Long' THEN 
            CASE 
                WHEN @val_ListItem IS NOT NULL THEN dbo.build_listitem_json(@val_ListItem)
                WHEN @val_Long IS NULL THEN NULL 
                ELSE CAST(@val_Long AS NVARCHAR(30)) 
            END
        WHEN @db_type = 'Guid' THEN 
            CASE WHEN @val_Guid IS NULL THEN NULL ELSE N'"' + CAST(@val_Guid AS NVARCHAR(50)) + N'"' END
        WHEN @db_type = 'Double' THEN 
            CASE WHEN @val_Double IS NULL THEN NULL ELSE FORMAT(@val_Double, 'G', 'en-US') END
        WHEN @db_type = 'Numeric' THEN 
            CASE WHEN @val_Numeric IS NULL THEN NULL ELSE REPLACE(CAST(@val_Numeric AS NVARCHAR(50)), N',', N'.') END
        WHEN @db_type = 'DateTimeOffset' THEN 
            CASE WHEN @val_DateTimeOffset IS NULL THEN NULL ELSE N'"' + CONVERT(NVARCHAR(50), @val_DateTimeOffset, 127) + N'"' END
        WHEN @db_type = 'Boolean' THEN 
            CASE WHEN @val_Boolean IS NULL THEN NULL WHEN @val_Boolean = 1 THEN N'true' ELSE N'false' END
        WHEN @db_type = 'ListItem' THEN dbo.build_listitem_json(@val_ListItem)
        ELSE NULL
    END;
    
    RETURN @result;
END
GO

-- =====================================================
-- HELPER: Build properties JSON for an object
-- =====================================================
CREATE FUNCTION dbo.build_properties(
    @object_id BIGINT,
    @scheme_id BIGINT,
    @max_depth INT,
    @parent_structure_id BIGINT,
    @array_index NVARCHAR(430),
    @parent_value_id BIGINT
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    DECLARE @result NVARCHAR(MAX);
    
    -- Protection against infinite recursion
    IF @max_depth < -100 RETURN N'{"error":"Max recursion depth reached"}';
    
    -- Build properties object from metadata cache
    -- Filter out NULL values to reduce JSON size (like PostgreSQL behavior)
    SELECT @result = N'{' + ISNULL((
        SELECT STRING_AGG(N'"' + dbo.escape_json_string(_name) + N'":' + field_value, N',') 
               WITHIN GROUP (ORDER BY _order, _structure_id)
        FROM (
            SELECT 
                c._order,
                c._structure_id,
                c._name,
                dbo.build_field_json(
                    @object_id, c._structure_id, @scheme_id, @parent_structure_id,
                    c._name, c.db_type, c.type_semantic, c._collection_type,
                    @max_depth, @array_index, @parent_value_id
                ) AS field_value
            FROM _scheme_metadata_cache c
            WHERE c._scheme_id = @scheme_id
              AND ((@parent_structure_id IS NULL AND c._parent_structure_id IS NULL) 
                   OR (@parent_structure_id IS NOT NULL AND c._parent_structure_id = @parent_structure_id))
        ) sub
        WHERE field_value IS NOT NULL
    ), N'') + N'}';
    
    RETURN ISNULL(@result, N'{}');
END
GO

-- =====================================================
-- MAIN FUNCTION: Get object as JSON
-- =====================================================
CREATE FUNCTION dbo.get_object_json(
    @object_id BIGINT,
    @max_depth INT = 10
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    DECLARE @result NVARCHAR(MAX);
    DECLARE @scheme_id BIGINT;
    DECLARE @base_json NVARCHAR(MAX);
    DECLARE @properties_json NVARCHAR(MAX);
    
    -- Check if object exists
    IF NOT EXISTS(SELECT 1 FROM _objects WHERE _id = @object_id)
        RETURN NULL;
    
    -- Get scheme_id and base fields
    SELECT 
        @scheme_id = o._id_scheme,
        @base_json = N'{' +
            N'"id":' + CAST(o._id AS NVARCHAR(20)) +
            N',"name":' + CASE WHEN o._name IS NULL THEN N'null' ELSE N'"' + dbo.escape_json_string(o._name) + N'"' END +
            N',"scheme_id":' + CAST(o._id_scheme AS NVARCHAR(20)) +
            N',"scheme_name":"' + dbo.escape_json_string(s._name) + N'"' +
            N',"parent_id":' + CASE WHEN o._id_parent IS NULL THEN N'null' ELSE CAST(o._id_parent AS NVARCHAR(20)) END +
            N',"owner_id":' + CAST(o._id_owner AS NVARCHAR(20)) +
            N',"who_change_id":' + CAST(o._id_who_change AS NVARCHAR(20)) +
            N',"date_create":"' + CONVERT(NVARCHAR(50), o._date_create, 127) + N'"' +
            N',"date_modify":"' + CONVERT(NVARCHAR(50), o._date_modify, 127) + N'"' +
            N',"date_begin":' + CASE WHEN o._date_begin IS NULL THEN N'null' ELSE N'"' + CONVERT(NVARCHAR(50), o._date_begin, 127) + N'"' END +
            N',"date_complete":' + CASE WHEN o._date_complete IS NULL THEN N'null' ELSE N'"' + CONVERT(NVARCHAR(50), o._date_complete, 127) + N'"' END +
            N',"key":' + CASE WHEN o._key IS NULL THEN N'null' ELSE CAST(o._key AS NVARCHAR(20)) END +
            N',"value_long":' + CASE WHEN o._value_long IS NULL THEN N'null' ELSE CAST(o._value_long AS NVARCHAR(20)) END +
            N',"value_string":' + CASE WHEN o._value_string IS NULL THEN N'null' ELSE N'"' + dbo.escape_json_string(o._value_string) + N'"' END +
            N',"value_guid":' + CASE WHEN o._value_guid IS NULL THEN N'null' ELSE N'"' + CAST(o._value_guid AS NVARCHAR(50)) + N'"' END +
            N',"note":' + CASE WHEN o._note IS NULL THEN N'null' ELSE N'"' + dbo.escape_json_string(o._note) + N'"' END +
            N',"value_bool":' + CASE WHEN o._value_bool IS NULL THEN N'null' WHEN o._value_bool = 1 THEN N'true' ELSE N'false' END +
            N',"value_double":' + CASE WHEN o._value_double IS NULL THEN N'null' ELSE FORMAT(o._value_double, 'G', 'en-US') END +
            N',"value_numeric":' + CASE WHEN o._value_numeric IS NULL THEN N'null' ELSE REPLACE(CAST(o._value_numeric AS NVARCHAR(50)), N',', N'.') END +
            N',"value_datetime":' + CASE WHEN o._value_datetime IS NULL THEN N'null' ELSE N'"' + CONVERT(NVARCHAR(50), o._value_datetime, 127) + N'"' END +
            N',"hash":' + CASE WHEN o._hash IS NULL THEN N'null' ELSE N'"' + CAST(o._hash AS NVARCHAR(50)) + N'"' END
    FROM _objects o
    INNER JOIN _schemes s ON s._id = o._id_scheme
    WHERE o._id = @object_id;
    
    -- If max_depth = 0, return only base fields
    IF @max_depth <= 0
        RETURN @base_json + N'}';
    
    -- Build properties
    SET @properties_json = dbo.build_properties(@object_id, @scheme_id, @max_depth, NULL, NULL, NULL);
    
    -- Combine base + properties
    RETURN @base_json + N',"properties":' + ISNULL(@properties_json, N'{}') + N'}';
END
GO

-- =====================================================
-- USAGE EXAMPLES
-- =====================================================
-- 
-- Single object:
--   SELECT dbo.get_object_json(123, 10);
--
-- Multiple objects (batch - FAST!):
--   SELECT dbo.get_object_json(o._id, 10) as json_object
--   FROM _objects o
--   WHERE o._id IN (123, 456, 789);
--
-- With aggregation:
--   SELECT '[' + STRING_AGG(dbo.get_object_json(t._id, 10), ',') + ']'
--   FROM #temp_ids t;
-- =====================================================

PRINT '=========================================';
PRINT 'JSON object FUNCTION created!';
PRINT '';
PRINT 'FUNCTION: dbo.get_object_json(@object_id, @max_depth)';
PRINT '  - Can be used in SELECT statements';
PRINT '  - Reads directly from _values (no temp tables)';
PRINT '  - Recursive for nested objects';
PRINT '  - Use with STRING_AGG for batch processing';
PRINT '=========================================';
GO



-- ===== redb_lazy_loading_search.sql =====
-- ===== LAZY LOADING SUPPORT FOR FACET SEARCH (MSSQL) =====
-- Functions for returning base objects WITHOUT Props
-- Ported from PostgreSQL version
-- Original functions (search_objects_with_facets, search_tree_objects_with_facets) remain unchanged

SET NOCOUNT ON;
GO

-- ===== DROP EXISTING OBJECTS =====
IF OBJECT_ID('dbo.get_object_base_fields', 'FN') IS NOT NULL 
    DROP FUNCTION dbo.get_object_base_fields;
GO

IF OBJECT_ID('dbo.execute_objects_query_base', 'P') IS NOT NULL 
    DROP PROCEDURE dbo.execute_objects_query_base;
GO

IF OBJECT_ID('dbo.search_objects_with_facets_base', 'P') IS NOT NULL 
    DROP PROCEDURE dbo.search_objects_with_facets_base;
GO

IF OBJECT_ID('dbo.get_filtered_object_ids', 'P') IS NOT NULL 
    DROP PROCEDURE dbo.get_filtered_object_ids;
GO

IF OBJECT_ID('dbo.search_tree_objects_with_facets_base', 'P') IS NOT NULL 
    DROP PROCEDURE dbo.search_tree_objects_with_facets_base;
GO

IF OBJECT_ID('dbo.get_search_sql_preview_base', 'P') IS NOT NULL 
    DROP PROCEDURE dbo.get_search_sql_preview_base;
GO

IF OBJECT_ID('dbo.get_search_tree_sql_preview_base', 'P') IS NOT NULL 
    DROP PROCEDURE dbo.get_search_tree_sql_preview_base;
GO

-- ========== FUNCTION 1: Return base fields WITHOUT Props ==========
-- Returns JSON object with base fields only (no properties from _values)
CREATE FUNCTION dbo.get_object_base_fields(@object_id BIGINT)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    DECLARE @result NVARCHAR(MAX);
    
    SELECT @result = (
        SELECT 
            o._id AS [id],
            o._name AS [name],
            o._id_scheme AS [scheme_id],
            o._id_parent AS [parent_id],
            o._id_owner AS [owner_id],
            o._id_who_change AS [who_change_id],
            o._date_create AS [date_create],
            o._date_modify AS [date_modify],
            o._date_begin AS [date_begin],
            o._date_complete AS [date_complete],
            o._key AS [key],
            o._value_long AS [value_long],
            o._value_string AS [value_string],
            o._value_guid AS [value_guid],
            o._note AS [note],
            o._value_bool AS [value_bool],
            o._value_double AS [value_double],
            o._value_numeric AS [value_numeric],
            o._value_datetime AS [value_datetime],
            o._value_bytes AS [value_bytes],
            o._hash AS [hash]
        FROM _objects o
        WHERE o._id = @object_id
        FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    );
    
    RETURN @result;
END;
GO

-- ========== PROCEDURE 2: Execute query with base fields ==========
-- Returns search results with base objects (no Props)
CREATE PROCEDURE dbo.execute_objects_query_base
    @scheme_id BIGINT,
    @base_conditions NVARCHAR(MAX),
    @hierarchical_conditions NVARCHAR(MAX),
    @order_conditions NVARCHAR(MAX),
    @limit_count INT = NULL,
    @offset_count INT = 0,
    @distinct_hash BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @sql NVARCHAR(MAX);
    DECLARE @count_sql NVARCHAR(MAX);
    DECLARE @final_where NVARCHAR(MAX);
    DECLARE @objects_json NVARCHAR(MAX);
    DECLARE @total_count INT;
    DECLARE @order_clause NVARCHAR(MAX);
    
    -- Build WHERE clause
    SET @final_where = N'WHERE obj._id_scheme = ' + CAST(@scheme_id AS NVARCHAR(20));
    IF @base_conditions IS NOT NULL AND @base_conditions <> N''
        SET @final_where = @final_where + N' AND ' + @base_conditions;
    IF @hierarchical_conditions IS NOT NULL AND @hierarchical_conditions <> N''
        SET @final_where = @final_where + N' AND ' + @hierarchical_conditions;
    
    -- Build ORDER clause
    IF @order_conditions IS NOT NULL AND @order_conditions <> N''
        SET @order_clause = @order_conditions;
    ELSE
        SET @order_clause = N'ORDER BY obj._id';
    
    -- Handle DISTINCT by hash using ROW_NUMBER
    IF @distinct_hash = 1
    BEGIN
        -- DISTINCT ON equivalent: use ROW_NUMBER to get first row per hash
        SET @sql = N'
            SELECT @result = (
                SELECT 
                    o._id AS [id],
                    o._name AS [name],
                    o._id_scheme AS [scheme_id],
                    o._id_parent AS [parent_id],
                    o._id_owner AS [owner_id],
                    o._id_who_change AS [who_change_id],
                    o._date_create AS [date_create],
                    o._date_modify AS [date_modify],
                    o._date_begin AS [date_begin],
                    o._date_complete AS [date_complete],
                    o._key AS [key],
                    o._value_long AS [value_long],
                    o._value_string AS [value_string],
                    o._value_guid AS [value_guid],
                    o._note AS [note],
                    o._value_bool AS [value_bool],
                    o._value_double AS [value_double],
                    o._value_numeric AS [value_numeric],
                    o._value_datetime AS [value_datetime],
                    o._value_bytes AS [value_bytes],
                    o._hash AS [hash]
                FROM (
                    SELECT obj._id, obj._hash,
                           ROW_NUMBER() OVER (PARTITION BY obj._hash ORDER BY obj._id) AS rn
                    FROM _objects obj
                    ' + @final_where + N'
                ) ranked
                JOIN _objects o ON o._id = ranked._id
                WHERE ranked.rn = 1';
        
        -- Add ORDER BY for outer query (must use 'o' alias, not 'obj')
        IF @order_conditions IS NOT NULL AND @order_conditions <> N''
            SET @sql = @sql + N' ' + REPLACE(REPLACE(@order_conditions, N'obj.', N'o.'), N'obj._', N'o._');
        ELSE
            SET @sql = @sql + N' ORDER BY o._id';
        
        -- Add pagination
        IF @limit_count IS NOT NULL AND @limit_count > 0
            SET @sql = @sql + N' OFFSET ' + CAST(@offset_count AS NVARCHAR(20)) + 
                              N' ROWS FETCH NEXT ' + CAST(@limit_count AS NVARCHAR(20)) + N' ROWS ONLY';
        ELSE
            SET @sql = @sql + N' OFFSET 0 ROWS';  -- Allow ORDER BY without FETCH for count queries
        
        SET @sql = @sql + N' FOR JSON PATH)';
        
        -- Count query for distinct
        SET @count_sql = N'SELECT @cnt = COUNT(DISTINCT obj._hash) FROM _objects obj ' + @final_where;
    END
    ELSE
    BEGIN
        -- Standard query without distinct
        SET @sql = N'
            SELECT @result = (
                SELECT 
                    o._id AS [id],
                    o._name AS [name],
                    o._id_scheme AS [scheme_id],
                    o._id_parent AS [parent_id],
                    o._id_owner AS [owner_id],
                    o._id_who_change AS [who_change_id],
                    o._date_create AS [date_create],
                    o._date_modify AS [date_modify],
                    o._date_begin AS [date_begin],
                    o._date_complete AS [date_complete],
                    o._key AS [key],
                    o._value_long AS [value_long],
                    o._value_string AS [value_string],
                    o._value_guid AS [value_guid],
                    o._note AS [note],
                    o._value_bool AS [value_bool],
                    o._value_double AS [value_double],
                    o._value_numeric AS [value_numeric],
                    o._value_datetime AS [value_datetime],
                    o._value_bytes AS [value_bytes],
                    o._hash AS [hash]
                FROM _objects o
                JOIN (
                    SELECT obj._id
                    FROM _objects obj
                    ' + @final_where + N'
                    ' + @order_clause;
        
        -- Add pagination
        IF @limit_count IS NOT NULL AND @limit_count > 0
            SET @sql = @sql + N' OFFSET ' + CAST(@offset_count AS NVARCHAR(20)) + 
                              N' ROWS FETCH NEXT ' + CAST(@limit_count AS NVARCHAR(20)) + N' ROWS ONLY';
        ELSE
            SET @sql = @sql + N' OFFSET 0 ROWS';  -- Allow ORDER BY without FETCH for count queries
        
        SET @sql = @sql + N'
                ) sub ON sub._id = o._id
                FOR JSON PATH)';
        
        -- Count query
        SET @count_sql = N'SELECT @cnt = COUNT(*) FROM _objects obj ' + @final_where;
    END
    
    -- Execute count query first
    EXEC sp_executesql @count_sql, N'@cnt INT OUTPUT', @cnt = @total_count OUTPUT;
    
    -- OPTIMIZATION: For CountAsync (limit = 0), skip objects loading
    IF @limit_count IS NULL OR @limit_count > 0
    BEGIN
        EXEC sp_executesql @sql, N'@result NVARCHAR(MAX) OUTPUT', @result = @objects_json OUTPUT;
    END;
    
    -- Return result as JSON with 'result' column for C# mapping
    -- Use JSON_QUERY to embed JSON arrays properly (not as escaped strings)
    SELECT ISNULL((
        SELECT 
            JSON_QUERY(ISNULL(@objects_json, N'[]')) AS objects,
            ISNULL(@total_count, 0) AS total_count,
            ISNULL(@limit_count, 2147483647) AS [limit],
            ISNULL(@offset_count, 0) AS [offset],
            JSON_QUERY(N'[]') AS facets
        FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    ), N'{"objects":[],"total_count":0,"limit":2147483647,"offset":0,"facets":[]}') AS result;
END;
GO

-- ========== PROCEDURE 3: Search with facets (base objects) ==========
-- Faceted search returning base objects WITHOUT Props (7 parameters - for lazy loading)
CREATE PROCEDURE dbo.search_objects_with_facets_base
    @scheme_id BIGINT,
    @facet_filters NVARCHAR(MAX) = NULL,
    @limit_count INT = NULL,
    @offset_count INT = 0,
    @order_by NVARCHAR(MAX) = NULL,
    @max_recursion_depth INT = 10,
    @distinct_hash BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @base_conditions NVARCHAR(MAX);
    DECLARE @hierarchical_conditions NVARCHAR(MAX);
    DECLARE @order_conditions NVARCHAR(MAX);
    
    -- Build conditions using existing procedures from redb_facets_search.sql
    EXEC dbo.internal_parse_filters @scheme_id, @facet_filters, N'obj', N'AND', 0, @base_conditions OUTPUT;
    
    -- Build order conditions
    SET @order_conditions = dbo.build_order_by_clause(@order_by, N'obj');
    
    -- Execute base query
    EXEC execute_objects_query_base 
        @scheme_id,
        @base_conditions,
        NULL, -- hierarchical_conditions not implemented yet
        @order_conditions,
        @limit_count,
        @offset_count,
        @distinct_hash;
END;
GO

-- NOTE: search_objects_with_facets (8 params) already exists in redb_facets_search.sql
-- We only define search_objects_with_facets_base (7 params) here for lazy loading

-- ========== PROCEDURE 4: Get filtered object IDs only ==========
-- Optimized for aggregations - returns only IDs without JSON overhead
CREATE PROCEDURE dbo.get_filtered_object_ids
    @scheme_id BIGINT,
    @filter_json NVARCHAR(MAX) = NULL,
    @max_recursion_depth INT = 10
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @base_conditions NVARCHAR(MAX);
    DECLARE @sql NVARCHAR(MAX);
    DECLARE @final_where NVARCHAR(MAX);
    
    -- Build conditions
    EXEC dbo.internal_parse_filters @scheme_id, @filter_json, N'obj', N'AND', 0, @base_conditions OUTPUT;
    
    -- Build WHERE clause
    SET @final_where = N'WHERE obj._id_scheme = ' + CAST(@scheme_id AS NVARCHAR(20));
    IF @base_conditions IS NOT NULL AND @base_conditions <> N''
        SET @final_where = @final_where + N' AND ' + @base_conditions;
    
    -- Return only IDs
    SET @sql = N'SELECT obj._id FROM _objects obj ' + @final_where;
    
    EXEC sp_executesql @sql;
END;
GO

-- ========== PROCEDURE 5: Tree search (base objects) ==========
-- Tree search returning base objects WITHOUT Props
CREATE PROCEDURE dbo.search_tree_objects_with_facets_base
    @scheme_id BIGINT,
    @parent_ids NVARCHAR(MAX), -- Comma-separated list of parent IDs
    @facet_filters NVARCHAR(MAX) = NULL,
    @limit_count INT = NULL,
    @offset_count INT = 0,
    @order_by NVARCHAR(MAX) = NULL,
    @max_depth INT = 10,
    @max_recursion_depth INT = 10
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @sql NVARCHAR(MAX);
    DECLARE @count_sql NVARCHAR(MAX);
    DECLARE @base_conditions NVARCHAR(MAX);
    DECLARE @order_conditions NVARCHAR(MAX);
    DECLARE @objects_json NVARCHAR(MAX);
    DECLARE @total_count INT;
    
    -- Ensure metadata cache is populated
    IF NOT EXISTS (SELECT 1 FROM _scheme_metadata_cache WHERE _scheme_id = @scheme_id)
    BEGIN
        EXEC sync_metadata_cache_for_scheme @scheme_id;
    END
    
    -- Build conditions (use 'o' alias - all conditions on outer _objects table)
    EXEC dbo.internal_parse_filters @scheme_id, @facet_filters, N'o', N'AND', 0, @base_conditions OUTPUT;
    
    -- Build order conditions (use 'o' alias - ORDER BY is on outer level)
    SET @order_conditions = dbo.build_order_by_clause(@order_by, N'o');
    IF @order_conditions IS NULL OR @order_conditions = N''
        SET @order_conditions = N'ORDER BY o._id';
    
    -- Direct children only (max_depth = 1)
    IF @max_depth = 1
    BEGIN
        SET @sql = N'
            SELECT @result = (
                SELECT 
                    o._id AS [id],
                    o._name AS [name],
                    o._id_scheme AS [scheme_id],
                    o._id_parent AS [parent_id],
                    o._id_owner AS [owner_id],
                    o._id_who_change AS [who_change_id],
                    o._date_create AS [date_create],
                    o._date_modify AS [date_modify],
                    o._date_begin AS [date_begin],
                    o._date_complete AS [date_complete],
                    o._key AS [key],
                    o._value_long AS [value_long],
                    o._value_string AS [value_string],
                    o._value_guid AS [value_guid],
                    o._note AS [note],
                    o._value_bool AS [value_bool],
                    o._value_double AS [value_double],
                    o._value_numeric AS [value_numeric],
                    o._value_datetime AS [value_datetime],
                    o._value_bytes AS [value_bytes],
                    o._hash AS [hash]
                FROM _objects o
                WHERE o._id_scheme = ' + CAST(@scheme_id AS NVARCHAR(20)) + N'
                  AND o._id_parent IN (SELECT CAST(value AS BIGINT) FROM STRING_SPLIT(@parents, '',''))';
        
        IF @base_conditions IS NOT NULL AND @base_conditions <> N''
            SET @sql = @sql + N' AND ' + @base_conditions;
        
        -- ORDER BY on clean SELECT
        SET @sql = @sql + N'
                ' + @order_conditions;
        
        IF @limit_count IS NOT NULL AND @limit_count > 0
            SET @sql = @sql + N' OFFSET ' + CAST(@offset_count AS NVARCHAR(20)) + 
                              N' ROWS FETCH NEXT ' + CAST(@limit_count AS NVARCHAR(20)) + N' ROWS ONLY';
        ELSE
            SET @sql = @sql + N' OFFSET 0 ROWS';  -- Allow ORDER BY without FETCH for count queries
        
        SET @sql = @sql + N'
                FOR JSON PATH)';
        
        -- Count query (use alias 'o' to match base_conditions)
        SET @count_sql = N'
            SELECT @cnt = COUNT(*)
            FROM _objects o
            WHERE o._id_scheme = ' + CAST(@scheme_id AS NVARCHAR(20)) + N'
              AND o._id_parent IN (SELECT CAST(value AS BIGINT) FROM STRING_SPLIT(@parents, '',''))';
        
        IF @base_conditions IS NOT NULL AND @base_conditions <> N''
            SET @count_sql = @count_sql + N' AND ' + @base_conditions;
    END
    ELSE
    BEGIN
        -- Recursive search for descendants
        -- Use WHERE ... IN instead of JOIN with DISTINCT to avoid MSSQL ORDER BY issue
        SET @sql = N'
            ;WITH descendants AS (
                SELECT CAST(value AS BIGINT) AS _id, 0 AS depth
                FROM STRING_SPLIT(@parents, '','')
                UNION ALL
                SELECT obj._id, d.depth + 1
                FROM _objects obj
                JOIN descendants d ON obj._id_parent = d._id
                WHERE d.depth < ' + CAST(@max_depth AS NVARCHAR(10)) + N'
            )
            SELECT @result = (
                SELECT 
                    o._id AS [id],
                    o._name AS [name],
                    o._id_scheme AS [scheme_id],
                    o._id_parent AS [parent_id],
                    o._id_owner AS [owner_id],
                    o._id_who_change AS [who_change_id],
                    o._date_create AS [date_create],
                    o._date_modify AS [date_modify],
                    o._date_begin AS [date_begin],
                    o._date_complete AS [date_complete],
                    o._key AS [key],
                    o._value_long AS [value_long],
                    o._value_string AS [value_string],
                    o._value_guid AS [value_guid],
                    o._note AS [note],
                    o._value_bool AS [value_bool],
                    o._value_double AS [value_double],
                    o._value_numeric AS [value_numeric],
                    o._value_datetime AS [value_datetime],
                    o._value_bytes AS [value_bytes],
                    o._hash AS [hash]
                FROM _objects o
                WHERE o._id_scheme = ' + CAST(@scheme_id AS NVARCHAR(20)) + N'
                  AND EXISTS (
                      SELECT 1 FROM descendants dt
                      WHERE dt._id = o._id AND dt.depth > 0
                  )';
        
        IF @base_conditions IS NOT NULL AND @base_conditions <> N''
            SET @sql = @sql + N' AND ' + @base_conditions;
        
        -- ORDER BY on clean SELECT (no JOIN with DISTINCT)
        SET @sql = @sql + N'
                ' + @order_conditions;
        
        IF @limit_count IS NOT NULL AND @limit_count > 0
            SET @sql = @sql + N' OFFSET ' + CAST(@offset_count AS NVARCHAR(20)) + 
                              N' ROWS FETCH NEXT ' + CAST(@limit_count AS NVARCHAR(20)) + N' ROWS ONLY';
        ELSE
            SET @sql = @sql + N' OFFSET 0 ROWS';  -- Allow ORDER BY without FETCH for count queries
        
        SET @sql = @sql + N'
                FOR JSON PATH)';
        
        -- Count query (use alias 'o' and WHERE IN to match main query)
        SET @count_sql = N'
            ;WITH descendants AS (
                SELECT CAST(value AS BIGINT) AS _id, 0 AS depth
                FROM STRING_SPLIT(@parents, '','')
                UNION ALL
                SELECT obj._id, d.depth + 1
                FROM _objects obj
                JOIN descendants d ON obj._id_parent = d._id
                WHERE d.depth < ' + CAST(@max_depth AS NVARCHAR(10)) + N'
            )
            SELECT @cnt = COUNT(*)
            FROM _objects o
            WHERE o._id_scheme = ' + CAST(@scheme_id AS NVARCHAR(20)) + N'
              AND EXISTS (
                  SELECT 1 FROM descendants dt
                  WHERE dt._id = o._id AND dt.depth > 0
              )';
        
        IF @base_conditions IS NOT NULL AND @base_conditions <> N''
            SET @count_sql = @count_sql + N' AND ' + @base_conditions;
    END
    
    -- Execute queries
    EXEC sp_executesql @sql, 
        N'@parents NVARCHAR(MAX), @result NVARCHAR(MAX) OUTPUT', 
        @parents = @parent_ids, 
        @result = @objects_json OUTPUT;
    
    EXEC sp_executesql @count_sql, 
        N'@parents NVARCHAR(MAX), @cnt INT OUTPUT', 
        @parents = @parent_ids, 
        @cnt = @total_count OUTPUT;
    
    -- Return result as JSON with 'result' column for C# mapping
    -- Use JSON_QUERY to embed JSON arrays properly (not as escaped strings)
    SELECT ISNULL((
        SELECT 
            JSON_QUERY(ISNULL(@objects_json, N'[]')) AS objects,
            ISNULL(@total_count, 0) AS total_count,
            ISNULL(@limit_count, 2147483647) AS [limit],
            ISNULL(@offset_count, 0) AS [offset],
            N'[' + ISNULL(@parent_ids, N'') + N']' AS parent_ids,
            ISNULL(@max_depth, 10) AS max_depth,
            JSON_QUERY(N'[]') AS facets
        FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    ), N'{"objects":[],"total_count":0,"limit":2147483647,"offset":0,"parent_ids":[],"max_depth":10,"facets":[]}') AS result;
END;
GO

-- ========== PROCEDURE 6: SQL Preview for base search ==========
-- Returns generated SQL for debugging
CREATE PROCEDURE dbo.get_search_sql_preview_base
    @scheme_id BIGINT,
    @facet_filters NVARCHAR(MAX) = NULL,
    @limit_count INT = NULL,
    @offset_count INT = 0,
    @order_by NVARCHAR(MAX) = NULL,
    @max_recursion_depth INT = 10,
    @distinct_hash BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @base_conditions NVARCHAR(MAX);
    DECLARE @order_conditions NVARCHAR(MAX);
    DECLARE @sql NVARCHAR(MAX);
    DECLARE @final_where NVARCHAR(MAX);
    
    -- Build conditions
    EXEC dbo.internal_parse_filters @scheme_id, @facet_filters, N'obj', N'AND', 0, @base_conditions OUTPUT;
    
    -- Build order conditions
    SET @order_conditions = dbo.build_order_by_clause(@order_by, N'obj');
    IF @order_conditions IS NULL OR @order_conditions = N''
        SET @order_conditions = N'ORDER BY obj._id';
    
    -- Build WHERE clause
    SET @final_where = N'WHERE obj._id_scheme = ' + CAST(@scheme_id AS NVARCHAR(20));
    IF @base_conditions IS NOT NULL AND @base_conditions <> N''
        SET @final_where = @final_where + N' AND ' + @base_conditions;
    
    -- Build preview SQL
    IF @distinct_hash = 1
    BEGIN
        SET @sql = N'
SELECT 
    o._id AS [id], o._name AS [name], o._id_scheme AS [scheme_id],
    o._id_parent AS [parent_id], o._hash AS [hash], ...
FROM (
    SELECT obj._id, obj._hash,
           ROW_NUMBER() OVER (PARTITION BY obj._hash ORDER BY obj._id) AS rn
    FROM _objects obj
    ' + @final_where + N'
) ranked
JOIN _objects o ON o._id = ranked._id
WHERE ranked.rn = 1
' + CASE WHEN @order_conditions IS NOT NULL AND @order_conditions <> N'' 
         THEN REPLACE(REPLACE(@order_conditions, N'obj.', N'o.'), N'obj._', N'o._')
         ELSE N'ORDER BY o._id' END;
    END
    ELSE
    BEGIN
        SET @sql = N'
SELECT 
    o._id AS [id], o._name AS [name], o._id_scheme AS [scheme_id],
    o._id_parent AS [parent_id], o._hash AS [hash], ...
FROM _objects o
JOIN (
    SELECT obj._id
    FROM _objects obj
    ' + @final_where + N'
    ' + @order_conditions;
    END
    
    IF @limit_count IS NOT NULL AND @limit_count > 0
        SET @sql = @sql + N'
    OFFSET ' + CAST(@offset_count AS NVARCHAR(20)) + N' ROWS 
    FETCH NEXT ' + CAST(@limit_count AS NVARCHAR(20)) + N' ROWS ONLY';
    ELSE
        SET @sql = @sql + N' OFFSET 0 ROWS';  -- Allow ORDER BY without FETCH for count queries
    
    IF @distinct_hash = 0
        SET @sql = @sql + N'
) sub ON sub._id = o._id';
    
    SET @sql = @sql + N'
FOR JSON PATH';
    
    SELECT @sql AS sql_preview;
END;
GO

-- ========== PROCEDURE 7: SQL Preview for tree search ==========
-- Returns generated SQL for tree search debugging
CREATE PROCEDURE dbo.get_search_tree_sql_preview_base
    @scheme_id BIGINT,
    @parent_ids NVARCHAR(MAX),
    @facet_filters NVARCHAR(MAX) = NULL,
    @limit_count INT = NULL,
    @offset_count INT = 0,
    @order_by NVARCHAR(MAX) = NULL,
    @max_depth INT = 10,
    @max_recursion_depth INT = 10
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @base_conditions NVARCHAR(MAX);
    DECLARE @order_conditions NVARCHAR(MAX);
    DECLARE @sql NVARCHAR(MAX);
    
    -- Build conditions (use 'o' alias - all conditions on outer _objects table)
    EXEC dbo.internal_parse_filters @scheme_id, @facet_filters, N'o', N'AND', 0, @base_conditions OUTPUT;
    
    -- Build order conditions (use 'o' alias)
    SET @order_conditions = dbo.build_order_by_clause(@order_by, N'o');
    IF @order_conditions IS NULL OR @order_conditions = N''
        SET @order_conditions = N'ORDER BY o._id';
    
    IF @max_depth = 1
    BEGIN
        SET @sql = N'
-- Direct children only (WHERE ... IN structure)
SELECT o._id, o._name, o._hash, ...
FROM _objects o
WHERE o._id_scheme = ' + CAST(@scheme_id AS NVARCHAR(20)) + N'
  AND o._id_parent IN (SELECT CAST(value AS BIGINT) FROM STRING_SPLIT(''' + @parent_ids + N''', '',''))';
        
        IF @base_conditions IS NOT NULL AND @base_conditions <> N''
            SET @sql = @sql + N'
  AND ' + @base_conditions;
        
        -- ORDER BY on clean SELECT
        SET @sql = @sql + N'
' + @order_conditions;
    END
    ELSE
    BEGIN
        SET @sql = N'
-- Recursive descendants search (WHERE ... IN structure)
;WITH descendants AS (
    SELECT CAST(value AS BIGINT) AS _id, 0 AS depth
    FROM STRING_SPLIT(''' + @parent_ids + N''', '','')
    UNION ALL
    SELECT obj._id, d.depth + 1
    FROM _objects obj
    JOIN descendants d ON obj._id_parent = d._id
    WHERE d.depth < ' + CAST(@max_depth AS NVARCHAR(10)) + N'
)
SELECT o._id, o._name, o._hash, ...
FROM _objects o
WHERE o._id_scheme = ' + CAST(@scheme_id AS NVARCHAR(20)) + N'
  AND EXISTS (
      SELECT 1 FROM descendants dt
      WHERE dt._id = o._id AND dt.depth > 0
  )';
        
        IF @base_conditions IS NOT NULL AND @base_conditions <> N''
            SET @sql = @sql + N'
  AND ' + @base_conditions;
        
        -- ORDER BY on clean SELECT
        SET @sql = @sql + N'
' + @order_conditions;
    END
    
    IF @limit_count IS NOT NULL AND @limit_count > 0
        SET @sql = @sql + N'
    OFFSET ' + CAST(@offset_count AS NVARCHAR(20)) + N' ROWS 
    FETCH NEXT ' + CAST(@limit_count AS NVARCHAR(20)) + N' ROWS ONLY';
    ELSE
        SET @sql = @sql + N' OFFSET 0 ROWS';  -- Allow ORDER BY without FETCH for count queries
    
    SET @sql = @sql + N'
FOR JSON PATH';
    
    SELECT @sql AS sql_preview;
END;
GO

-- ========== HELPER: Build ORDER BY clause ==========
-- Creates ORDER BY clause from JSON specification
IF OBJECT_ID('dbo.build_order_by_clause', 'FN') IS NOT NULL 
    DROP FUNCTION dbo.build_order_by_clause;
GO

CREATE FUNCTION dbo.build_order_by_clause(
    @order_by NVARCHAR(MAX),
    @table_alias NVARCHAR(10)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    DECLARE @result NVARCHAR(MAX) = N'';
    
    -- If no order specified, return empty (including string 'null' from C#)
    IF @order_by IS NULL OR @order_by = N'' OR @order_by = N'[]' OR @order_by = N'null'
        RETURN @result;
    
    -- Parse JSON array of order items
    -- Expected format: [{"field": "Name", "direction": "ASC"}]
    SELECT @result = STRING_AGG(order_item, N', ')
    FROM (
        SELECT 
            CASE 
                -- Base field with 0$: prefix
                WHEN JSON_VALUE(arr.value, '$.field') LIKE N'0$:%' THEN
                    @table_alias + N'.' + dbo.normalize_base_field_name(
                        SUBSTRING(JSON_VALUE(arr.value, '$.field'), 4, 100)
                    ) + N' ' + 
                    UPPER(ISNULL(JSON_VALUE(arr.value, '$.direction'), N'ASC'))
                -- Regular field - sort via subquery to _values
                ELSE
                    N'(SELECT TOP 1 COALESCE(v._String, CAST(v._Long AS NVARCHAR(50)), FORMAT(v._Double, ''G'', ''en-US''))
                       FROM _values v 
                       JOIN _scheme_metadata_cache c ON c._structure_id = v._id_structure
                       WHERE v._id_object = ' + @table_alias + N'._id 
                         AND c._name = N''' + JSON_VALUE(arr.value, '$.field') + N'''
                         AND v._array_index IS NULL) ' +
                    UPPER(ISNULL(JSON_VALUE(arr.value, '$.direction'), N'ASC'))
            END AS order_item
        FROM OPENJSON(@order_by) arr
        WHERE JSON_VALUE(arr.value, '$.field') IS NOT NULL
    ) items;
    
    IF @result IS NOT NULL AND @result <> N''
        SET @result = N'ORDER BY ' + @result;
    
    RETURN @result;
END;
GO

PRINT N'=========================================';
PRINT N'Lazy Loading Search procedures created!';
PRINT N'';
PRINT N'PROCEDURES:';
PRINT N'  search_objects_with_facets_base - Search without Props';
PRINT N'  search_tree_objects_with_facets_base - Tree search without Props';
PRINT N'  get_filtered_object_ids - Get only IDs for aggregations';
PRINT N'  execute_objects_query_base - Execute query with base fields';
PRINT N'  get_search_sql_preview_base - SQL preview for debugging';
PRINT N'  get_search_tree_sql_preview_base - Tree SQL preview';
PRINT N'';
PRINT N'FUNCTIONS:';
PRINT N'  get_object_base_fields - Get single object base fields';
PRINT N'  build_order_by_clause - Build ORDER BY from JSON';
PRINT N'=========================================';
GO



-- ===== redb_migrations.sql =====
-- =====================================================
-- REDB Pro: Таблица истории миграций (SQL Server)
-- =====================================================

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = '_migrations')
BEGIN
    CREATE TABLE _migrations (
        _id BIGINT IDENTITY(1,1) PRIMARY KEY,
        _migration_id NVARCHAR(500) NOT NULL,                   -- уникальный ID миграции "OrderProps_TotalPrice_v1"
        _scheme_id BIGINT NOT NULL REFERENCES _schemes(_id) ON DELETE CASCADE,
        _structure_id BIGINT REFERENCES _structures(_id),       -- NULL = вся схема (ON DELETE SET NULL not supported with CASCADE on same table)
        _property_name NVARCHAR(500),                           -- имя свойства (для логов)
        _expression_hash NVARCHAR(500),                         -- MD5 от Expression для детекции изменений
        _migration_type NVARCHAR(200) NOT NULL,                 -- ComputedFrom, TypeChange, DefaultValue, Transform
        _applied_at DATETIMEOFFSET NOT NULL DEFAULT SYSDATETIMEOFFSET(),
        _applied_by NVARCHAR(500),                              -- кто применил (user/system)
        _sql_executed NVARCHAR(MAX),                            -- SQL который был выполнен (для аудита)
        _affected_rows INT,                                     -- сколько записей затронуто
        _duration_ms INT,                                       -- время выполнения
        _dry_run BIT NOT NULL DEFAULT 0,                        -- это был dry-run?
        
        CONSTRAINT uq_migration_scheme UNIQUE(_scheme_id, _migration_id)
    );
END;

-- Индексы для быстрого поиска
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_migrations_scheme')
    CREATE INDEX idx_migrations_scheme ON _migrations(_scheme_id);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_migrations_applied')
    CREATE INDEX idx_migrations_applied ON _migrations(_applied_at DESC);
GO

-- ===== redb_permissions.sql =====
-- =============================================================================
-- REDB Permissions Functions for MS SQL Server
-- Compatible with SQL Server 2016+ (uses JSON functions, window functions, CTEs)
-- =============================================================================

-- Drop existing function if exists
IF OBJECT_ID('dbo.get_user_permissions_for_object', 'TF') IS NOT NULL
    DROP FUNCTION dbo.get_user_permissions_for_object;
GO

-- =============================================================================
-- Function: get_user_permissions_for_object
-- Purpose: Returns effective permissions for a specific object considering 
--          hierarchical inheritance and priorities (user > role).
--          If @user_id = NULL, returns the first found permission without 
--          filtering by user (for use in triggers).
--
-- Parameters:
--   @object_id BIGINT - Target object ID
--   @user_id BIGINT = NULL - User ID (optional for trigger usage)
--
-- Returns: Table with permission details
--
-- Usage:
--   SELECT * FROM dbo.get_user_permissions_for_object(12345, 100);
--   SELECT * FROM dbo.get_user_permissions_for_object(12345, NULL); -- for triggers
-- =============================================================================
CREATE FUNCTION dbo.get_user_permissions_for_object
(
    @object_id BIGINT,
    @user_id BIGINT = NULL
)
RETURNS @result TABLE
(
    object_id BIGINT,
    user_id BIGINT,
    permission_source_id BIGINT,
    permission_type NVARCHAR(50),
    _id_role BIGINT,
    _id_user BIGINT,
    can_select BIT,
    can_insert BIT,
    can_update BIT,
    can_delete BIT
)
AS
BEGIN
    -- System user (id=0) has full permissions on everything
    IF @user_id = 0
    BEGIN
        INSERT INTO @result
        SELECT 
            @object_id AS object_id,
            0 AS user_id,
            0 AS permission_source_id,
            N'system' AS permission_type,
            NULL AS _id_role,
            0 AS _id_user,
            1 AS can_select,
            1 AS can_insert,
            1 AS can_update,
            1 AS can_delete;
        RETURN;
    END;

    -- Use CTE to find permissions with hierarchical search
    ;WITH permission_search AS (
        -- Step 1: Start from target object
        SELECT 
            @object_id AS object_id,
            @object_id AS current_search_id,
            o._id_parent,
            0 AS level,
            CASE WHEN EXISTS(SELECT 1 FROM _permissions WHERE _id_ref = @object_id) 
                 THEN 1 ELSE 0 END AS has_permission
        FROM _objects o
        WHERE o._id = @object_id
        
        UNION ALL
        
        -- Step 2: If NO permission - go to parent
        SELECT 
            ps.object_id,
            o._id AS current_search_id,
            o._id_parent,
            ps.level + 1,
            CASE WHEN EXISTS(SELECT 1 FROM _permissions WHERE _id_ref = o._id) 
                 THEN 1 ELSE 0 END AS has_permission
        FROM _objects o
        INNER JOIN permission_search ps ON o._id = ps._id_parent
        WHERE ps.level < 50
          AND ps.has_permission = 0  -- continue only if NO permission
    ),
    -- Get first found permission for object using ROW_NUMBER instead of DISTINCT ON
    object_permission_ranked AS (
        SELECT 
            ps.object_id,
            p._id AS permission_id,
            p._id_user,
            p._id_role,
            p._select,
            p._insert,
            p._update,
            p._delete,
            ps.level,
            ps.current_search_id AS permission_source_id,
            ROW_NUMBER() OVER (PARTITION BY ps.object_id ORDER BY ps.level) AS rn
        FROM permission_search ps
        INNER JOIN _permissions p ON p._id_ref = ps.current_search_id
        WHERE ps.has_permission = 1
    ),
    object_permission AS (
        SELECT 
            object_id,
            permission_id,
            _id_user,
            _id_role,
            _select,
            _insert,
            _update,
            _delete,
            level,
            permission_source_id
        FROM object_permission_ranked
        WHERE rn = 1
    ),
    -- Add global permissions as fallback (_id_ref = 0)
    global_permission AS (
        SELECT 
            @object_id AS object_id,
            p._id AS permission_id,
            p._id_user,
            p._id_role,
            p._select,
            p._insert,
            p._update,
            p._delete,
            999 AS level,  -- low priority
            CAST(0 AS BIGINT) AS permission_source_id
        FROM _permissions p
        WHERE p._id_ref = 0
    ),
    -- Combine specific and global permissions
    all_permissions AS (
        SELECT * FROM object_permission
        UNION ALL
        SELECT * FROM global_permission
    ),
    -- Get first by priority (specific > global) using ROW_NUMBER
    final_permission_ranked AS (
        SELECT 
            *,
            ROW_NUMBER() OVER (PARTITION BY object_id ORDER BY level) AS rn
        FROM all_permissions
    ),
    final_permission AS (
        SELECT 
            object_id,
            permission_id,
            _id_user,
            _id_role,
            _select,
            _insert,
            _update,
            _delete,
            level,
            permission_source_id
        FROM final_permission_ranked
        WHERE rn = 1
    )
    -- Result: for user permissions - direct, for role - through users_roles
    INSERT INTO @result
    SELECT 
        fp.object_id,
        CASE 
            WHEN @user_id IS NULL THEN NULL  -- if user_id not passed for trigger
            WHEN fp._id_user IS NOT NULL THEN fp._id_user  -- direct user permission
            ELSE ur._id_user  -- through role
        END AS user_id,
        fp.permission_source_id,
        CASE 
            WHEN fp._id_user IS NOT NULL THEN N'user'
            ELSE N'role'
        END AS permission_type,
        fp._id_role,
        fp._id_user,
        fp._select AS can_select,
        fp._insert AS can_insert,
        fp._update AS can_update,
        fp._delete AS can_delete
    FROM final_permission fp
    LEFT JOIN _users_roles ur ON ur._id_role = fp._id_role  -- only for role permissions
    WHERE @user_id IS NULL 
       OR (fp._id_user = @user_id OR ur._id_user = @user_id);  -- if user_id NULL - all permissions, else filter

    RETURN;
END;
GO

-- =============================================================================
-- Trigger: auto_create_node_permissions
-- Purpose: Automatically creates permissions when creating node objects.
--          If parent has no direct permission, finds inherited permission
--          and creates copy for the new object.
--
-- Note: This trigger is for reference. Adjust based on your actual requirements.
-- =============================================================================

-- Drop existing trigger if exists
IF OBJECT_ID('dbo.tr_auto_create_node_permissions', 'TR') IS NOT NULL
    DROP TRIGGER dbo.tr_auto_create_node_permissions;
GO

CREATE TRIGGER dbo.tr_auto_create_node_permissions
ON _objects
AFTER INSERT
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @new_id BIGINT;
    DECLARE @parent_id BIGINT;
    DECLARE @source_permission_id BIGINT;
    DECLARE @source_user_id BIGINT;
    DECLARE @source_role_id BIGINT;
    DECLARE @source_select BIT;
    DECLARE @source_insert BIT;
    DECLARE @source_update BIT;
    DECLARE @source_delete BIT;
    DECLARE @next_id BIGINT;
    
    -- Process each inserted object with parent
    DECLARE insert_cursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT i._id, i._id_parent
        FROM inserted i
        WHERE i._id_parent IS NOT NULL;
    
    OPEN insert_cursor;
    FETCH NEXT FROM insert_cursor INTO @new_id, @parent_id;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Check if parent already has direct permission
        IF NOT EXISTS(SELECT 1 FROM _permissions WHERE _id_ref = @parent_id)
        BEGIN
            -- Use function without user_id to find source permission
            SELECT TOP 1
                @source_permission_id = permission_source_id,
                @source_user_id = _id_user,
                @source_role_id = _id_role,
                @source_select = can_select,
                @source_insert = can_insert,
                @source_update = can_update,
                @source_delete = can_delete
            FROM dbo.get_user_permissions_for_object(@parent_id, NULL);
            
            -- If source permission found, create copy for parent
            IF @source_permission_id IS NOT NULL
            BEGIN
                -- Get next ID from sequence
                SELECT @next_id = NEXT VALUE FOR global_identity;
                
                INSERT INTO _permissions (
                    _id, _id_ref, _id_user, _id_role,
                    _select, _insert, _update, _delete
                )
                VALUES (
                    @next_id, @parent_id, @source_user_id, @source_role_id,
                    @source_select, @source_insert, @source_update, @source_delete
                );
            END;
        END;
        
        FETCH NEXT FROM insert_cursor INTO @new_id, @parent_id;
    END;
    
    CLOSE insert_cursor;
    DEALLOCATE insert_cursor;
END;
GO

-- =============================================================================
-- Function: check_user_permission
-- Purpose: Quick check if user has specific permission on object.
--          Returns 1 if has permission, 0 otherwise.
--
-- Parameters:
--   @object_id BIGINT - Target object ID
--   @user_id BIGINT - User ID
--   @permission_type NVARCHAR(10) - 'select', 'insert', 'update', 'delete'
--
-- Usage:
--   IF dbo.check_user_permission(12345, 100, 'update') = 1 
--     PRINT 'User can update';
-- =============================================================================
IF OBJECT_ID('dbo.check_user_permission', 'FN') IS NOT NULL
    DROP FUNCTION dbo.check_user_permission;
GO

CREATE FUNCTION dbo.check_user_permission
(
    @object_id BIGINT,
    @user_id BIGINT,
    @permission_type NVARCHAR(10)
)
RETURNS BIT
AS
BEGIN
    DECLARE @result BIT = 0;
    
    -- System user always has permission
    IF @user_id = 0
        RETURN 1;
    
    SELECT @result = 
        CASE @permission_type
            WHEN N'select' THEN can_select
            WHEN N'insert' THEN can_insert
            WHEN N'update' THEN can_update
            WHEN N'delete' THEN can_delete
            ELSE 0
        END
    FROM dbo.get_user_permissions_for_object(@object_id, @user_id);
    
    RETURN ISNULL(@result, 0);
END;
GO

-- =============================================================================
-- Function: get_user_accessible_objects
-- Purpose: Returns list of object IDs that user can access with given permission.
--          Uses hierarchical permission inheritance.
--
-- Parameters:
--   @user_id BIGINT - User ID
--   @permission_type NVARCHAR(10) - 'select', 'insert', 'update', 'delete'
--   @scheme_id BIGINT = NULL - Optional filter by scheme
--
-- Usage:
--   SELECT * FROM dbo.get_user_accessible_objects(100, 'select', NULL);
-- =============================================================================
IF OBJECT_ID('dbo.get_user_accessible_objects', 'TF') IS NOT NULL
    DROP FUNCTION dbo.get_user_accessible_objects;
GO

CREATE FUNCTION dbo.get_user_accessible_objects
(
    @user_id BIGINT,
    @permission_type NVARCHAR(10),
    @scheme_id BIGINT = NULL
)
RETURNS @result TABLE
(
    object_id BIGINT PRIMARY KEY
)
AS
BEGIN
    -- System user sees everything
    IF @user_id = 0
    BEGIN
        INSERT INTO @result
        SELECT _id FROM _objects
        WHERE @scheme_id IS NULL OR _id_scheme = @scheme_id;
        RETURN;
    END;
    
    -- Get user's roles
    DECLARE @user_roles TABLE (role_id BIGINT PRIMARY KEY);
    INSERT INTO @user_roles
    SELECT _id_role FROM _users_roles WHERE _id_user = @user_id;
    
    -- Find all permissions applicable to user (direct or via role)
    ;WITH applicable_permissions AS (
        SELECT 
            p._id_ref AS object_id,
            CASE @permission_type
                WHEN N'select' THEN p._select
                WHEN N'insert' THEN p._insert
                WHEN N'update' THEN p._update
                WHEN N'delete' THEN p._delete
                ELSE 0
            END AS has_permission
        FROM _permissions p
        WHERE p._id_user = @user_id
           OR p._id_role IN (SELECT role_id FROM @user_roles)
    ),
    -- Objects with direct permissions
    permitted_roots AS (
        SELECT object_id
        FROM applicable_permissions
        WHERE has_permission = 1
    ),
    -- Recursively find all descendants of permitted objects
    all_accessible AS (
        SELECT o._id AS object_id
        FROM _objects o
        WHERE o._id IN (SELECT object_id FROM permitted_roots)
        
        UNION ALL
        
        SELECT o._id
        FROM _objects o
        INNER JOIN all_accessible a ON o._id_parent = a.object_id
    )
    INSERT INTO @result
    SELECT DISTINCT aa.object_id
    FROM all_accessible aa
    INNER JOIN _objects o ON o._id = aa.object_id
    WHERE @scheme_id IS NULL OR o._id_scheme = @scheme_id;
    
    RETURN;
END;
GO

-- =============================================================================
-- Inline Function: fn_can_user_edit_object
-- Purpose: Optimized scalar check for edit (update) permission.
--          Returns 1 if user can edit, 0 otherwise.
--          Inline version for better query plan optimization.
-- =============================================================================
IF OBJECT_ID('dbo.fn_can_user_edit_object', 'IF') IS NOT NULL
    DROP FUNCTION dbo.fn_can_user_edit_object;
GO

CREATE FUNCTION dbo.fn_can_user_edit_object
(
    @object_id BIGINT,
    @user_id BIGINT
)
RETURNS TABLE
AS
RETURN
(
    SELECT 
        CASE 
            WHEN @user_id = 0 THEN 1  -- system user
            WHEN EXISTS(
                SELECT 1 FROM dbo.get_user_permissions_for_object(@object_id, @user_id)
                WHERE can_update = 1
            ) THEN 1
            ELSE 0
        END AS can_edit
);
GO

-- =============================================================================
-- Inline Function: fn_can_user_select_object
-- Purpose: Optimized scalar check for select permission.
-- =============================================================================
IF OBJECT_ID('dbo.fn_can_user_select_object', 'IF') IS NOT NULL
    DROP FUNCTION dbo.fn_can_user_select_object;
GO

CREATE FUNCTION dbo.fn_can_user_select_object
(
    @object_id BIGINT,
    @user_id BIGINT
)
RETURNS TABLE
AS
RETURN
(
    SELECT 
        CASE 
            WHEN @user_id = 0 THEN 1  -- system user
            WHEN EXISTS(
                SELECT 1 FROM dbo.get_user_permissions_for_object(@object_id, @user_id)
                WHERE can_select = 1
            ) THEN 1
            ELSE 0
        END AS can_select
);
GO

-- =============================================================================
-- Inline Function: fn_can_user_delete_object
-- Purpose: Optimized scalar check for delete permission.
-- =============================================================================
IF OBJECT_ID('dbo.fn_can_user_delete_object', 'IF') IS NOT NULL
    DROP FUNCTION dbo.fn_can_user_delete_object;
GO

CREATE FUNCTION dbo.fn_can_user_delete_object
(
    @object_id BIGINT,
    @user_id BIGINT
)
RETURNS TABLE
AS
RETURN
(
    SELECT 
        CASE 
            WHEN @user_id = 0 THEN 1  -- system user
            WHEN EXISTS(
                SELECT 1 FROM dbo.get_user_permissions_for_object(@object_id, @user_id)
                WHERE can_delete = 1
            ) THEN 1
            ELSE 0
        END AS can_delete
);
GO

-- =============================================================================
-- Inline Function: fn_can_user_insert_scheme
-- Purpose: Check if user can insert objects of specific scheme.
--          Looks for global permission (on _id_ref = 0 or scheme itself).
-- =============================================================================
IF OBJECT_ID('dbo.fn_can_user_insert_scheme', 'IF') IS NOT NULL
    DROP FUNCTION dbo.fn_can_user_insert_scheme;
GO

CREATE FUNCTION dbo.fn_can_user_insert_scheme
(
    @scheme_id BIGINT,
    @user_id BIGINT
)
RETURNS TABLE
AS
RETURN
(
    WITH user_roles AS (
        SELECT _id_role AS role_id FROM _users_roles WHERE _id_user = @user_id
    )
    SELECT 
        CASE 
            WHEN @user_id = 0 THEN 1  -- system user
            WHEN EXISTS(
                SELECT 1 FROM _permissions p
                WHERE (p._id_ref = 0 OR p._id_ref = @scheme_id)
                  AND p._insert = 1
                  AND (p._id_user = @user_id OR p._id_role IN (SELECT role_id FROM user_roles))
            ) THEN 1
            ELSE 0
        END AS can_insert
);
GO

PRINT 'REDB Permissions functions created successfully';
GO



-- ===== redb_projection.sql =====
-- ============================================================
-- PROJECTION FUNCTIONS (MSSQL): Optimized loading of specific fields
-- ============================================================
-- Result format:
--   - Class fields -> flat with paths: "Contact.Name": "John"
--   - Arrays -> flat with indexes: "Items[0].Price": 100
--   - _RObject -> NESTED object: "Author": { "Name": "Pushkin", ... }
-- OPTIMIZED: Set-based approach without cursors (like PostgreSQL)
-- ============================================================

SET NOCOUNT ON;
GO

-- ===== DROP EXISTING OBJECTS =====
IF OBJECT_ID('dbo.build_flat_projection', 'P') IS NOT NULL 
    DROP PROCEDURE dbo.build_flat_projection;
GO

IF OBJECT_ID('dbo.search_objects_with_projection', 'P') IS NOT NULL 
    DROP PROCEDURE dbo.search_objects_with_projection;
GO

IF OBJECT_ID('dbo.get_object_with_projection', 'P') IS NOT NULL 
    DROP PROCEDURE dbo.get_object_with_projection;
GO

IF OBJECT_ID('dbo.search_objects_with_projection_by_paths', 'P') IS NOT NULL 
    DROP PROCEDURE dbo.search_objects_with_projection_by_paths;
GO

IF OBJECT_ID('dbo.search_objects_with_projection_by_ids', 'P') IS NOT NULL 
    DROP PROCEDURE dbo.search_objects_with_projection_by_ids;
GO

IF OBJECT_ID('dbo._build_field_path', 'FN') IS NOT NULL 
    DROP FUNCTION dbo._build_field_path;
GO

-- ============================================================
-- Helper: _build_field_path - Build field path from structure_id
-- ============================================================
CREATE FUNCTION dbo._build_field_path(
    @structure_id BIGINT,
    @scheme_id BIGINT
)
RETURNS NVARCHAR(500)
AS
BEGIN
    DECLARE @path NVARCHAR(500) = N'';
    DECLARE @current_id BIGINT = @structure_id;
    DECLARE @name NVARCHAR(255);
    DECLARE @parent_id BIGINT;
    DECLARE @depth INT = 0;
    
    WHILE @depth < 20
    BEGIN
        SELECT @name = _name, @parent_id = _parent_structure_id
        FROM _scheme_metadata_cache
        WHERE _scheme_id = @scheme_id AND _structure_id = @current_id;
        
        IF @name IS NULL
            BREAK;
        
        IF @path = N''
            SET @path = @name;
        ELSE
            SET @path = @name + N'.' + @path;
        
        IF @parent_id IS NULL
            BREAK;
        
        SET @current_id = @parent_id;
        SET @depth = @depth + 1;
    END
    
    RETURN @path;
END;
GO

-- ============================================================
-- get_object_with_projection: Get single object with projection
-- OPTIMIZED: Single query approach
-- ============================================================
CREATE PROCEDURE dbo.get_object_with_projection
    @object_id BIGINT,
    @projection_paths NVARCHAR(MAX),
    @max_depth INT = 10
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @scheme_id BIGINT;
    DECLARE @sql NVARCHAR(MAX);
    
    -- Get scheme_id
    SELECT @scheme_id = _id_scheme FROM _objects WHERE _id = @object_id;
    IF @scheme_id IS NULL
    BEGIN
        SELECT NULL AS result;
        RETURN;
    END
    
    -- Parse projection paths into table (no cursor!)
    DECLARE @projections TABLE (
        path NVARCHAR(255),
        structure_id BIGINT,
        db_type NVARCHAR(50),
        column_name NVARCHAR(50)
    );
    
    INSERT INTO @projections (path, structure_id)
    SELECT 
        JSON_VALUE(value, '$.path'),
        CAST(JSON_VALUE(value, '$.structure_id') AS BIGINT)
    FROM OPENJSON(@projection_paths)
    WHERE JSON_VALUE(value, '$.structure_id') IS NOT NULL;
    
    -- Get db_type for each structure (single query)
    UPDATE p
    SET p.db_type = c.db_type,
        p.column_name = CASE c.db_type
            WHEN N'String' THEN N'_String'
            WHEN N'Long' THEN N'_Long'
            WHEN N'Double' THEN N'_Double'
            WHEN N'Numeric' THEN N'_Numeric'
            WHEN N'Boolean' THEN N'_Boolean'
            WHEN N'Guid' THEN N'_Guid'
            WHEN N'DateTimeOffset' THEN N'_DateTimeOffset'
            ELSE N'_String'
        END
    FROM @projections p
    JOIN _scheme_metadata_cache c ON c._scheme_id = @scheme_id AND c._structure_id = p.structure_id;
    
    -- Remove Object (Class) type fields without nested paths (like PostgreSQL)
    DELETE FROM @projections 
    WHERE EXISTS (
        SELECT 1 FROM _scheme_metadata_cache c 
        WHERE c._structure_id = [@projections].structure_id 
        AND c._scheme_id = @scheme_id 
        AND c.type_semantic = N'Object'
    );
    
    -- Build dynamic PVT query
    DECLARE @pvt_columns NVARCHAR(MAX) = N'';
    DECLARE @select_columns NVARCHAR(MAX) = N'';
    
    SELECT 
        @pvt_columns = @pvt_columns + N',' + QUOTENAME(structure_id),
        @select_columns = @select_columns + N',
            JSON_VALUE(pvt.' + QUOTENAME(structure_id) + N', ''$.' + column_name + N''') AS [properties.' + 
            REPLACE(REPLACE(path, N'[', N'.'), N']', N'') + N']'
    FROM @projections;
    
    IF @pvt_columns = N''
    BEGIN
        -- No projection fields - return basic object
        SELECT (
            SELECT 
                o._id AS [id],
                o._name AS [name],
                o._id_scheme AS [scheme_id],
                o._hash AS [hash],
                o._date_modify AS [date_modify]
            FROM _objects o
            WHERE o._id = @object_id
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        ) AS result;
        RETURN;
    END
    
    SET @pvt_columns = STUFF(@pvt_columns, 1, 1, N'');
    
    SET @sql = N'
    WITH values_json AS (
        SELECT 
            v._id_object,
            v._id_structure,
            (SELECT v._String AS _String, v._Long AS _Long, v._Double AS _Double,
                    v._Numeric AS _Numeric, v._Boolean AS _Boolean, v._Guid AS _Guid,
                    v._DateTimeOffset AS _DateTimeOffset FOR JSON PATH, WITHOUT_ARRAY_WRAPPER) AS val_json
        FROM _values v
        WHERE v._id_object = @obj_id 
          AND v._id_structure IN (SELECT structure_id FROM @proj)
          AND v._array_index IS NULL
    ),
    pvted AS (
        SELECT _id_object, ' + @pvt_columns + N'
        FROM values_json
        PVT (MAX(val_json) FOR _id_structure IN (' + @pvt_columns + N')) AS pvt
    )
    SELECT (
        SELECT 
            o._id AS [id],
            o._name AS [name],
            o._id_scheme AS [scheme_id],
            o._hash AS [hash],
            o._date_modify AS [date_modify]' + @select_columns + N'
        FROM _objects o
        LEFT JOIN pvted pvt ON pvt._id_object = o._id
        WHERE o._id = @obj_id
        FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    ) AS result';
    
    EXEC sp_executesql @sql, 
        N'@obj_id BIGINT, @proj AS dbo.BigIntTable READONLY', 
        @obj_id = @object_id;
END;
GO

-- ============================================================
-- search_objects_with_projection_by_ids: Search with projection by structure IDs
-- OPTIMIZED: No cursors, single PVT query
-- ============================================================
CREATE PROCEDURE dbo.search_objects_with_projection_by_ids
    @scheme_id BIGINT,
    @filter_json NVARCHAR(MAX) = NULL,
    @structure_ids NVARCHAR(MAX), -- Comma-separated structure IDs
    @limit INT = NULL,
    @offset INT = 0,
    @order_by NVARCHAR(MAX) = NULL,
    @max_depth INT = 10
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @sql NVARCHAR(MAX);
    DECLARE @where_clause NVARCHAR(MAX) = N'';
    DECLARE @order_clause NVARCHAR(MAX);
    DECLARE @objects_json NVARCHAR(MAX);
    DECLARE @total_count INT;
    
    -- Parse structure_ids into table (no cursor!)
    CREATE TABLE #struct_ids (structure_id BIGINT PRIMARY KEY);
    INSERT INTO #struct_ids (structure_id)
    SELECT DISTINCT CAST(value AS BIGINT) FROM STRING_SPLIT(@structure_ids, N',')
    WHERE value IS NOT NULL AND value <> N'';
    
    -- Get field info for all structures at once
    CREATE TABLE #projections (
        structure_id BIGINT PRIMARY KEY,
        field_name NVARCHAR(255),
        db_type NVARCHAR(50),
        column_name NVARCHAR(50)
    );
    
    INSERT INTO #projections (structure_id, field_name, db_type, column_name)
    SELECT 
        c._structure_id,
        c._name,
        c.db_type,
        CASE c.db_type
            WHEN N'String' THEN N'_String'
            WHEN N'Long' THEN N'_Long'
            WHEN N'Double' THEN N'_Double'
            WHEN N'Numeric' THEN N'_Numeric'
            WHEN N'Boolean' THEN N'_Boolean'
            WHEN N'Guid' THEN N'_Guid'
            WHEN N'DateTimeOffset' THEN N'_DateTimeOffset'
            ELSE N'_String'
        END
    FROM _scheme_metadata_cache c
    JOIN #struct_ids s ON s.structure_id = c._structure_id
    WHERE c._scheme_id = @scheme_id;
    
    -- Remove Object (Class) type fields without nested paths (like PostgreSQL)
    DELETE p FROM #projections p
    JOIN _scheme_metadata_cache c ON c._structure_id = p.structure_id AND c._scheme_id = @scheme_id
    WHERE c.type_semantic = N'Object';
    
    -- Build PVT columns dynamically
    DECLARE @pvt_columns NVARCHAR(MAX) = N'';
    DECLARE @select_columns NVARCHAR(MAX) = N'';
    
    SELECT 
        @pvt_columns = @pvt_columns + N',' + QUOTENAME(structure_id),
        @select_columns = @select_columns + N',
            pvt.[' + CAST(structure_id AS NVARCHAR(20)) + N'_val] AS [properties.' + 
            REPLACE(REPLACE(field_name, N'[', N'.'), N']', N'') + N']'
    FROM #projections
    ORDER BY structure_id;
    
    -- Build filter conditions
    IF @filter_json IS NOT NULL AND @filter_json <> N'' AND @filter_json <> N'null'
    BEGIN
        EXEC dbo.internal_parse_filters @scheme_id, @filter_json, N'o', N'AND', 0, @where_clause OUTPUT;
    END
    
    IF @where_clause IS NOT NULL AND @where_clause <> N''
        SET @where_clause = N' AND ' + @where_clause;
    
    -- Build order clause
    SET @order_clause = dbo.build_order_by_clause(@order_by, N'o');
    IF @order_clause IS NULL OR @order_clause = N''
        SET @order_clause = N'ORDER BY o._id';
    
    -- Count query
    SET @sql = N'
        SELECT @cnt = COUNT(*)
        FROM _objects o
        WHERE o._id_scheme = @sch_id' + @where_clause;
    
    EXEC sp_executesql @sql, N'@cnt INT OUTPUT, @sch_id BIGINT', 
        @cnt = @total_count OUTPUT, @sch_id = @scheme_id;
    
    -- Build main query with PVT approach
    IF @pvt_columns = N''
    BEGIN
        -- No projection fields - return basic objects
        SET @sql = N'
        SELECT @result = (
            SELECT 
                o._id AS [id],
                o._name AS [name],
                o._id_scheme AS [scheme_id],
                o._id_parent AS [parent_id],
                o._id_owner AS [owner_id],
                o._date_create AS [date_create],
                o._date_modify AS [date_modify],
                o._key AS [key],
                o._value_long AS [value_long],
                o._value_string AS [value_string],
                o._value_guid AS [value_guid],
                o._hash AS [hash]
            FROM _objects o
            WHERE o._id_scheme = @sch_id' + @where_clause + N'
            ' + @order_clause;
        
        IF @limit IS NOT NULL
            SET @sql = @sql + N'
            OFFSET @off ROWS FETCH NEXT @lim ROWS ONLY';
        
        SET @sql = @sql + N'
            FOR JSON PATH
        )';
        
        EXEC sp_executesql @sql, 
            N'@result NVARCHAR(MAX) OUTPUT, @sch_id BIGINT, @off INT, @lim INT',
            @result = @objects_json OUTPUT, @sch_id = @scheme_id, @off = @offset, @lim = @limit;
    END
    ELSE
    BEGIN
        SET @pvt_columns = STUFF(@pvt_columns, 1, 1, N'');
        
        -- Build value extraction for each column
        DECLARE @value_extracts NVARCHAR(MAX) = N'';
        SELECT @value_extracts = @value_extracts + N',
            CASE WHEN v._id_structure = ' + CAST(structure_id AS NVARCHAR(20)) + N' THEN v.' + column_name + N' END AS [' + CAST(structure_id AS NVARCHAR(20)) + N'_val]'
        FROM #projections;
        SET @value_extracts = STUFF(@value_extracts, 1, 1, N'');
        
        -- Aggregate columns
        DECLARE @agg_columns NVARCHAR(MAX) = N'';
        SELECT @agg_columns = @agg_columns + N',
            MAX([' + CAST(structure_id AS NVARCHAR(20)) + N'_val]) AS [' + CAST(structure_id AS NVARCHAR(20)) + N'_val]'
        FROM #projections;
        SET @agg_columns = STUFF(@agg_columns, 1, 1, N'');
        
        -- Use temp table instead of CTE to materialize filtered IDs (avoids re-computation)
        CREATE TABLE #filtered_ids (_id BIGINT PRIMARY KEY);
        
        SET @sql = N'
        INSERT INTO #filtered_ids (_id)
        SELECT o._id
        FROM _objects o
        WHERE o._id_scheme = @sch_id' + @where_clause + N'
        ' + @order_clause;
        
        IF @limit IS NOT NULL
            SET @sql = @sql + N'
        OFFSET @off ROWS FETCH NEXT @lim ROWS ONLY';
        
        EXEC sp_executesql @sql, 
            N'@sch_id BIGINT, @off INT, @lim INT',
            @sch_id = @scheme_id, @off = @offset, @lim = @limit;
        
        SET @sql = N'
        ;WITH val_expanded AS (
            SELECT 
                v._id_object,
                ' + @value_extracts + N'
            FROM #filtered_ids f
            JOIN _values v ON v._id_object = f._id
            WHERE v._id_structure IN (SELECT structure_id FROM #projections)
              AND v._array_index IS NULL
        ),
        pvt AS (
            SELECT 
                _id_object,
                ' + @agg_columns + N'
            FROM val_expanded
            GROUP BY _id_object
        )
        SELECT @result = (
            SELECT 
                o._id AS [id],
                o._name AS [name],
                o._id_scheme AS [scheme_id],
                o._id_parent AS [parent_id],
                o._id_owner AS [owner_id],
                o._date_create AS [date_create],
                o._date_modify AS [date_modify],
                o._key AS [key],
                o._value_long AS [value_long],
                o._value_string AS [value_string],
                o._value_guid AS [value_guid],
                o._hash AS [hash]' + @select_columns + N'
            FROM #filtered_ids f
            JOIN _objects o ON o._id = f._id
            LEFT JOIN pvt ON pvt._id_object = o._id
            ORDER BY o._id
            FOR JSON PATH
        )';
        
        EXEC sp_executesql @sql, 
            N'@result NVARCHAR(MAX) OUTPUT',
            @result = @objects_json OUTPUT;
        
        DROP TABLE #filtered_ids;
    END
    
    -- Return result
    SELECT (
        SELECT 
            JSON_QUERY(ISNULL(@objects_json, N'[]')) AS objects,
            @total_count AS total_count,
            @limit AS [limit],
            @offset AS [offset],
            JSON_QUERY(N'[]') AS facets
        FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    ) AS result;
    
    DROP TABLE #struct_ids;
    DROP TABLE #projections;
END;
GO

-- ============================================================
-- search_objects_with_projection_by_paths: Search with projection by text paths
-- OPTIMIZED: No cursors, batch processing
-- ============================================================
CREATE PROCEDURE dbo.search_objects_with_projection_by_paths
    @scheme_id BIGINT,
    @filter_json NVARCHAR(MAX) = NULL,
    @field_paths NVARCHAR(MAX), -- JSON array of paths: ["Name", "Address.City"]
    @limit INT = NULL,
    @offset INT = 0,
    @order_by NVARCHAR(MAX) = NULL,
    @max_depth INT = 10
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @sql NVARCHAR(MAX);
    DECLARE @where_clause NVARCHAR(MAX) = N'';
    DECLARE @order_clause NVARCHAR(MAX);
    DECLARE @objects_json NVARCHAR(MAX);
    DECLARE @total_count INT;
    
    -- Parse field paths into table
    CREATE TABLE #paths (
        idx INT IDENTITY(1,1) PRIMARY KEY,
        path NVARCHAR(255),
        structure_id BIGINT,
        db_type NVARCHAR(50),
        dict_key NVARCHAR(255),
        array_index NVARCHAR(50),
        column_name NVARCHAR(50),
        parent_structure_id BIGINT,        -- Parent structure for nested dict paths (AddressBook for AddressBook[home].City)
        is_nested_dict BIT DEFAULT 0       -- Flag for nested dictionary paths
    );
    
    -- Insert paths from JSON array or comma-separated
    IF LEFT(@field_paths, 1) = N'['
        INSERT INTO #paths (path)
        SELECT REPLACE(value, N'"', N'') FROM OPENJSON(@field_paths);
    ELSE
        INSERT INTO #paths (path)
        SELECT value FROM STRING_SPLIT(@field_paths, N',');
    
    -- Resolve all paths at once using resolve_field_path
    DECLARE @path NVARCHAR(255);
    DECLARE @structure_id BIGINT;
    DECLARE @db_type NVARCHAR(50);
    DECLARE @dict_key NVARCHAR(255);
    DECLARE @array_index NVARCHAR(50);
    DECLARE @idx INT;
    DECLARE @parent_path NVARCHAR(255);
    DECLARE @parent_structure_id BIGINT;
    DECLARE @parent_db_type NVARCHAR(50);
    DECLARE @parent_dict_key NVARCHAR(255);
    DECLARE @parent_array_index NVARCHAR(50);
    DECLARE @is_nested_dict BIT;
    DECLARE @bracket_pos INT;
    DECLARE @dot_after_bracket INT;
    
    DECLARE path_cursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT idx, path FROM #paths WHERE path IS NOT NULL AND path <> N'';
    
    OPEN path_cursor;
    FETCH NEXT FROM path_cursor INTO @idx, @path;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @is_nested_dict = 0;
        SET @parent_structure_id = NULL;
        
        -- Check if this is a nested dictionary path: AddressBook[home].City
        SET @bracket_pos = CHARINDEX(N'[', @path);
        IF @bracket_pos > 0
        BEGIN
            SET @dot_after_bracket = CHARINDEX(N'].', @path);
            IF @dot_after_bracket > 0
            BEGIN
                -- This is a nested dictionary path
                SET @is_nested_dict = 1;
                -- Get parent path: AddressBook[home]
                SET @parent_path = LEFT(@path, @dot_after_bracket);
                
                -- Resolve parent structure (AddressBook with dict_key)
                EXEC resolve_field_path @scheme_id, @parent_path,
                    @parent_structure_id OUTPUT, @parent_db_type OUTPUT, @parent_dict_key OUTPUT, @parent_array_index OUTPUT;
            END
        END
        
        -- Resolve the field path
        EXEC resolve_field_path @scheme_id, @path,
            @structure_id OUTPUT, @db_type OUTPUT, @dict_key OUTPUT, @array_index OUTPUT;
        
        UPDATE #paths 
        SET structure_id = @structure_id,
            db_type = @db_type,
            dict_key = @dict_key,
            array_index = @array_index,
            column_name = CASE @db_type
                WHEN N'String' THEN N'_String'
                WHEN N'Long' THEN N'_Long'
                WHEN N'Double' THEN N'_Double'
                WHEN N'Numeric' THEN N'_Numeric'
                WHEN N'Boolean' THEN N'_Boolean'
                WHEN N'Guid' THEN N'_Guid'
                WHEN N'DateTimeOffset' THEN N'_DateTimeOffset'
                ELSE N'_String'
            END,
            parent_structure_id = @parent_structure_id,
            is_nested_dict = @is_nested_dict
        WHERE idx = @idx;
        
        FETCH NEXT FROM path_cursor INTO @idx, @path;
    END
    
    CLOSE path_cursor;
    DEALLOCATE path_cursor;
    
    -- Remove unresolved paths
    DELETE FROM #paths WHERE structure_id IS NULL;
    
    -- Remove Object (Class) type fields without nested paths (like PostgreSQL)
    -- When user requests Address1 without specifying Address1.City, Address1.Street, etc.
    -- we should skip it (Object fields need nested field specification)
    DELETE p FROM #paths p
    JOIN _scheme_metadata_cache c ON c._structure_id = p.structure_id AND c._scheme_id = @scheme_id
    WHERE c.type_semantic = N'Object';
    
    -- Build filter conditions
    IF @filter_json IS NOT NULL AND @filter_json <> N'' AND @filter_json <> N'null'
    BEGIN
        EXEC dbo.internal_parse_filters @scheme_id, @filter_json, N'o', N'AND', 0, @where_clause OUTPUT;
    END
    
    IF @where_clause IS NOT NULL AND @where_clause <> N''
        SET @where_clause = N' AND ' + @where_clause;
    
    -- Build order clause
    SET @order_clause = dbo.build_order_by_clause(@order_by, N'o');
    IF @order_clause IS NULL OR @order_clause = N''
        SET @order_clause = N'ORDER BY o._id';
    
    -- Count query
    SET @sql = N'
        SELECT @cnt = COUNT(*)
        FROM _objects o
        WHERE o._id_scheme = @sch_id' + @where_clause;
    
    EXEC sp_executesql @sql, N'@cnt INT OUTPUT, @sch_id BIGINT', 
        @cnt = @total_count OUTPUT, @sch_id = @scheme_id;
    
    -- Build value extraction columns
    DECLARE @value_extracts NVARCHAR(MAX) = N'';
    DECLARE @agg_columns NVARCHAR(MAX) = N'';
    DECLARE @select_columns NVARCHAR(MAX) = N'';
    DECLARE @nested_dict_joins NVARCHAR(MAX) = N'';
    DECLARE @nested_dict_selects NVARCHAR(MAX) = N'';
    
    -- Process non-nested-dict paths (simple fields, simple dict keys like PhoneBook[home])
    SELECT 
        @value_extracts = @value_extracts + N',
            CASE WHEN v._id_structure = ' + CAST(structure_id AS NVARCHAR(20)) + 
            CASE 
                WHEN dict_key IS NOT NULL AND is_nested_dict = 0 THEN N' AND v._array_index = N''' + dict_key + N''''
                WHEN array_index IS NOT NULL THEN N' AND v._array_index = N''' + array_index + N''''
                ELSE N' AND v._array_index IS NULL'
            END +
            N' THEN v.' + column_name + N' END AS [col_' + CAST(idx AS NVARCHAR(10)) + N']',
        @agg_columns = @agg_columns + N',
            MAX([col_' + CAST(idx AS NVARCHAR(10)) + N']) AS [col_' + CAST(idx AS NVARCHAR(10)) + N']',
        @select_columns = @select_columns + N',
            pvt.[col_' + CAST(idx AS NVARCHAR(10)) + N'] AS [properties.' + 
            -- FOR JSON PATH uses dots for hierarchy: PhoneBook[home] → properties.PhoneBook.home
            REPLACE(REPLACE(path, N'[', N'.'), N']', N'') + N']'
    FROM #paths
    WHERE is_nested_dict = 0
    ORDER BY idx;
    
    -- Process nested dictionary paths (AddressBook[home].City) - need JOIN through _array_parent_id
    -- FOR JSON PATH uses dots for hierarchy: AddressBook[home].City → properties.AddressBook.home.City
    SELECT 
        @nested_dict_joins = @nested_dict_joins + N'
        LEFT JOIN (
            SELECT v_child._id_object, v_child.' + column_name + N' AS val
            FROM _values v_parent
            JOIN _values v_child ON v_child._array_parent_id = v_parent._id
            WHERE v_parent._id_structure = ' + CAST(parent_structure_id AS NVARCHAR(20)) + N'
              AND v_parent._array_index = N''' + dict_key + N'''
              AND v_child._id_structure = ' + CAST(structure_id AS NVARCHAR(20)) + N'
        ) nd_' + CAST(idx AS NVARCHAR(10)) + N' ON nd_' + CAST(idx AS NVARCHAR(10)) + N'._id_object = o._id',
        @nested_dict_selects = @nested_dict_selects + N',
            nd_' + CAST(idx AS NVARCHAR(10)) + N'.val AS [properties.' + 
            -- FOR JSON PATH uses dots for hierarchy: AddressBook[home].City → properties.AddressBook.home.City
            REPLACE(REPLACE(path, N'[', N'.'), N']', N'') + N']'
    FROM #paths
    WHERE is_nested_dict = 1
    ORDER BY idx;
    
    -- Create temp table for filtered IDs once (used in projection branches)
    CREATE TABLE #filtered_ids (_id BIGINT PRIMARY KEY);
    
    -- Check if we have any projections at all
    IF @value_extracts = N'' AND @nested_dict_joins = N''
    BEGIN
        -- No projection fields - return basic objects
        SET @sql = N'
        SELECT @result = (
            SELECT 
                o._id AS [id],
                o._name AS [name],
                o._id_scheme AS [scheme_id],
                o._id_parent AS [parent_id],
                o._id_owner AS [owner_id],
                o._date_create AS [date_create],
                o._date_modify AS [date_modify],
                o._key AS [key],
                o._value_long AS [value_long],
                o._value_string AS [value_string],
                o._value_guid AS [value_guid],
                o._hash AS [hash]
            FROM _objects o
            WHERE o._id_scheme = @sch_id' + @where_clause + N'
            ' + @order_clause;
        
        IF @limit IS NOT NULL
            SET @sql = @sql + N'
            OFFSET @off ROWS FETCH NEXT @lim ROWS ONLY';
        
        SET @sql = @sql + N'
            FOR JSON PATH
        )';
        
        EXEC sp_executesql @sql, 
            N'@result NVARCHAR(MAX) OUTPUT, @sch_id BIGINT, @off INT, @lim INT',
            @result = @objects_json OUTPUT, @sch_id = @scheme_id, @off = @offset, @lim = @limit;
    END
    ELSE IF @value_extracts = N''
    BEGIN
        -- Only nested dict fields (no simple projections)
        SET @sql = N'
        INSERT INTO #filtered_ids (_id)
        SELECT o._id
        FROM _objects o
        WHERE o._id_scheme = @sch_id' + @where_clause + N'
        ' + @order_clause;
        
        IF @limit IS NOT NULL
            SET @sql = @sql + N'
        OFFSET @off ROWS FETCH NEXT @lim ROWS ONLY';
        
        EXEC sp_executesql @sql, 
            N'@sch_id BIGINT, @off INT, @lim INT',
            @sch_id = @scheme_id, @off = @offset, @lim = @limit;
        
        SET @sql = N'
        SELECT @result = (
            SELECT 
                o._id AS [id],
                o._name AS [name],
                o._id_scheme AS [scheme_id],
                o._id_parent AS [parent_id],
                o._id_owner AS [owner_id],
                o._date_create AS [date_create],
                o._date_modify AS [date_modify],
                o._key AS [key],
                o._value_long AS [value_long],
                o._value_string AS [value_string],
                o._value_guid AS [value_guid],
                o._hash AS [hash]' + @nested_dict_selects + N'
            FROM #filtered_ids f
            JOIN _objects o ON o._id = f._id' + @nested_dict_joins + N'
            ORDER BY o._id
            FOR JSON PATH
        )';
        
        EXEC sp_executesql @sql, 
            N'@result NVARCHAR(MAX) OUTPUT',
            @result = @objects_json OUTPUT;
    END
    ELSE
    BEGIN
        SET @value_extracts = STUFF(@value_extracts, 1, 1, N'');
        SET @agg_columns = STUFF(@agg_columns, 1, 1, N'');
        
        -- Get distinct structure_ids for filter (only non-nested-dict)
        DECLARE @struct_ids NVARCHAR(MAX);
        SELECT @struct_ids = STRING_AGG(CAST(structure_id AS NVARCHAR(20)), N',') 
        FROM #paths WHERE is_nested_dict = 0;
        
        SET @sql = N'
        INSERT INTO #filtered_ids (_id)
        SELECT o._id
        FROM _objects o
        WHERE o._id_scheme = @sch_id' + @where_clause + N'
        ' + @order_clause;
        
        IF @limit IS NOT NULL
            SET @sql = @sql + N'
        OFFSET @off ROWS FETCH NEXT @lim ROWS ONLY';
        
        EXEC sp_executesql @sql, 
            N'@sch_id BIGINT, @off INT, @lim INT',
            @sch_id = @scheme_id, @off = @offset, @lim = @limit;
        
        -- Now query with materialized IDs - JOIN instead of IN for better performance
        SET @sql = N'
        ;WITH val_expanded AS (
            SELECT 
                v._id_object,
                ' + @value_extracts + N'
            FROM #filtered_ids f
            JOIN _values v ON v._id_object = f._id
            WHERE v._id_structure IN (' + @struct_ids + N')
        ),
        pvt AS (
            SELECT 
                _id_object,
                ' + @agg_columns + N'
            FROM val_expanded
            GROUP BY _id_object
        )
        SELECT @result = (
            SELECT 
                o._id AS [id],
                o._name AS [name],
                o._id_scheme AS [scheme_id],
                o._id_parent AS [parent_id],
                o._id_owner AS [owner_id],
                o._date_create AS [date_create],
                o._date_modify AS [date_modify],
                o._key AS [key],
                o._value_long AS [value_long],
                o._value_string AS [value_string],
                o._value_guid AS [value_guid],
                o._hash AS [hash]' + @select_columns + @nested_dict_selects + N'
            FROM #filtered_ids f
            JOIN _objects o ON o._id = f._id
            LEFT JOIN pvt ON pvt._id_object = o._id' + @nested_dict_joins + N'
            ORDER BY o._id
            FOR JSON PATH
        )';
        
        EXEC sp_executesql @sql, 
            N'@result NVARCHAR(MAX) OUTPUT',
            @result = @objects_json OUTPUT;
    END
    
    -- Return result
    SELECT (
        SELECT 
            JSON_QUERY(ISNULL(@objects_json, N'[]')) AS objects,
            @total_count AS total_count,
            @limit AS [limit],
            @offset AS [offset],
            JSON_QUERY(N'[]') AS facets
        FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    ) AS result;
    
    DROP TABLE #filtered_ids;
    DROP TABLE #paths;
END;
GO

-- ============================================================
-- search_objects_with_projection: Search with projection (JSONB paths)
-- OPTIMIZED: Set-based approach
-- ============================================================
CREATE PROCEDURE dbo.search_objects_with_projection
    @scheme_id BIGINT,
    @filter_json NVARCHAR(MAX) = NULL,
    @projection_paths NVARCHAR(MAX) = N'[]',
    @limit INT = NULL,
    @offset INT = 0,
    @order_by NVARCHAR(MAX) = NULL,
    @max_depth INT = 10
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @sql NVARCHAR(MAX);
    DECLARE @where_clause NVARCHAR(MAX) = N'';
    DECLARE @order_clause NVARCHAR(MAX);
    DECLARE @objects_json NVARCHAR(MAX);
    DECLARE @total_count INT;
    
    -- Parse projection paths into table
    CREATE TABLE #projections (
        idx INT IDENTITY(1,1) PRIMARY KEY,
        path NVARCHAR(255),
        structure_id BIGINT,
        db_type NVARCHAR(50),
        column_name NVARCHAR(50)
    );
    
    INSERT INTO #projections (path, structure_id)
    SELECT 
        JSON_VALUE(value, '$.path'),
        CAST(JSON_VALUE(value, '$.structure_id') AS BIGINT)
    FROM OPENJSON(@projection_paths)
    WHERE JSON_VALUE(value, '$.structure_id') IS NOT NULL;
    
    -- Get db_type for each structure (single query)
    UPDATE p
    SET p.db_type = c.db_type,
        p.column_name = CASE c.db_type
            WHEN N'String' THEN N'_String'
            WHEN N'Long' THEN N'_Long'
            WHEN N'Double' THEN N'_Double'
            WHEN N'Numeric' THEN N'_Numeric'
            WHEN N'Boolean' THEN N'_Boolean'
            WHEN N'Guid' THEN N'_Guid'
            WHEN N'DateTimeOffset' THEN N'_DateTimeOffset'
            ELSE N'_String'
        END
    FROM #projections p
    JOIN _scheme_metadata_cache c ON c._scheme_id = @scheme_id AND c._structure_id = p.structure_id;
    
    -- Remove rows with no match
    DELETE FROM #projections WHERE db_type IS NULL;
    
    -- Remove Object (Class) type fields without nested paths (like PostgreSQL)
    DELETE p FROM #projections p
    JOIN _scheme_metadata_cache c ON c._structure_id = p.structure_id AND c._scheme_id = @scheme_id
    WHERE c.type_semantic = N'Object';
    
    -- Build filter conditions
    IF @filter_json IS NOT NULL AND @filter_json <> N'' AND @filter_json <> N'null'
    BEGIN
        EXEC dbo.internal_parse_filters @scheme_id, @filter_json, N'o', N'AND', 0, @where_clause OUTPUT;
    END
    
    IF @where_clause IS NOT NULL AND @where_clause <> N''
        SET @where_clause = N' AND ' + @where_clause;
    
    -- Build order clause
    SET @order_clause = dbo.build_order_by_clause(@order_by, N'o');
    IF @order_clause IS NULL OR @order_clause = N''
        SET @order_clause = N'ORDER BY o._id';
    
    -- Count query
    SET @sql = N'
        SELECT @cnt = COUNT(*)
        FROM _objects o
        WHERE o._id_scheme = @sch_id' + @where_clause;
    
    EXEC sp_executesql @sql, N'@cnt INT OUTPUT, @sch_id BIGINT', 
        @cnt = @total_count OUTPUT, @sch_id = @scheme_id;
    
    -- Build value extraction columns
    DECLARE @value_extracts NVARCHAR(MAX) = N'';
    DECLARE @agg_columns NVARCHAR(MAX) = N'';
    DECLARE @select_columns NVARCHAR(MAX) = N'';
    
    SELECT 
        @value_extracts = @value_extracts + N',
            CASE WHEN v._id_structure = ' + CAST(structure_id AS NVARCHAR(20)) + 
            N' AND v._array_index IS NULL THEN v.' + column_name + N' END AS [col_' + CAST(idx AS NVARCHAR(10)) + N']',
        @agg_columns = @agg_columns + N',
            MAX([col_' + CAST(idx AS NVARCHAR(10)) + N']) AS [col_' + CAST(idx AS NVARCHAR(10)) + N']',
        @select_columns = @select_columns + N',
            pvt.[col_' + CAST(idx AS NVARCHAR(10)) + N'] AS [properties.' + 
            REPLACE(REPLACE(path, N'[', N'.'), N']', N'') + N']'
    FROM #projections
    ORDER BY idx;
    
    IF @value_extracts = N''
    BEGIN
        -- No projection fields - return basic objects
        SET @sql = N'
        SELECT @result = (
            SELECT 
                o._id AS [id],
                o._name AS [name],
                o._id_scheme AS [scheme_id],
                o._id_parent AS [parent_id],
                o._hash AS [hash]
            FROM _objects o
            WHERE o._id_scheme = @sch_id' + @where_clause + N'
            ' + @order_clause;
        
        IF @limit IS NOT NULL
            SET @sql = @sql + N'
            OFFSET @off ROWS FETCH NEXT @lim ROWS ONLY';
        
        SET @sql = @sql + N'
            FOR JSON PATH
        )';
        
        EXEC sp_executesql @sql, 
            N'@result NVARCHAR(MAX) OUTPUT, @sch_id BIGINT, @off INT, @lim INT',
            @result = @objects_json OUTPUT, @sch_id = @scheme_id, @off = @offset, @lim = @limit;
    END
    ELSE
    BEGIN
        SET @value_extracts = STUFF(@value_extracts, 1, 1, N'');
        SET @agg_columns = STUFF(@agg_columns, 1, 1, N'');
        
        -- Get distinct structure_ids for filter
        DECLARE @struct_ids NVARCHAR(MAX);
        SELECT @struct_ids = STRING_AGG(CAST(structure_id AS NVARCHAR(20)), N',') FROM #projections;
        
        -- Use temp table instead of CTE to materialize filtered IDs (avoids re-computation)
        CREATE TABLE #filtered_ids (_id BIGINT PRIMARY KEY);
        
        SET @sql = N'
        INSERT INTO #filtered_ids (_id)
        SELECT o._id
        FROM _objects o
        WHERE o._id_scheme = @sch_id' + @where_clause + N'
        ' + @order_clause;
        
        IF @limit IS NOT NULL
            SET @sql = @sql + N'
        OFFSET @off ROWS FETCH NEXT @lim ROWS ONLY';
        
        EXEC sp_executesql @sql, 
            N'@sch_id BIGINT, @off INT, @lim INT',
            @sch_id = @scheme_id, @off = @offset, @lim = @limit;
        
        SET @sql = N'
        ;WITH val_expanded AS (
            SELECT 
                v._id_object,
                ' + @value_extracts + N'
            FROM #filtered_ids f
            JOIN _values v ON v._id_object = f._id
            WHERE v._id_structure IN (' + @struct_ids + N')
        ),
        pvt AS (
            SELECT 
                _id_object,
                ' + @agg_columns + N'
            FROM val_expanded
            GROUP BY _id_object
        )
        SELECT @result = (
            SELECT 
                o._id AS [id],
                o._name AS [name],
                o._id_scheme AS [scheme_id],
                o._id_parent AS [parent_id],
                o._hash AS [hash]' + @select_columns + N'
            FROM #filtered_ids f2
            JOIN _objects o ON o._id = f2._id
            LEFT JOIN pvt ON pvt._id_object = o._id
            ORDER BY o._id
            FOR JSON PATH
        )';
        
        EXEC sp_executesql @sql, 
            N'@result NVARCHAR(MAX) OUTPUT',
            @result = @objects_json OUTPUT;
        
        DROP TABLE #filtered_ids;
    END
    
    -- Return result
    SELECT (
        SELECT 
            JSON_QUERY(ISNULL(@objects_json, N'[]')) AS objects,
            @total_count AS total_count,
            @limit AS [limit],
            @offset AS [offset]
        FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    ) AS result;
    
    DROP TABLE #projections;
END;
GO

PRINT N'=========================================';
PRINT N'OPTIMIZED Projection procedures created!';
PRINT N'';
PRINT N'KEY OPTIMIZATIONS:';
PRINT N'  - Replaced cursors with table variables';
PRINT N'  - Temp table #filtered_ids instead of CTE filtered';
PRINT N'  - Materialized IDs avoid CTE re-computation in subqueries';
PRINT N'  - CASE expressions instead of N LEFT JOINs';
PRINT N'  - GROUP BY aggregation instead of PVT';
PRINT N'';
PRINT N'PROCEDURES:';
PRINT N'  get_object_with_projection';
PRINT N'  search_objects_with_projection';
PRINT N'  search_objects_with_projection_by_paths';
PRINT N'  search_objects_with_projection_by_ids';
PRINT N'';
PRINT N'Expected speedup: 5-50x for multi-field projections';
PRINT N'=========================================';
GO


-- ===== redb_soft_delete.sql =====
-- =====================================================
-- SOFT DELETE PROCEDURES FOR MSSQL
-- Part of Background Deletion System
-- =====================================================

-- Drop existing procedures if any
IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'sp_mark_for_deletion')
DROP PROCEDURE [dbo].[sp_mark_for_deletion]
GO

IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'sp_purge_trash')
DROP PROCEDURE [dbo].[sp_purge_trash]
GO

-- =====================================================
-- PROCEDURE: sp_mark_for_deletion
-- Marks objects for deletion by moving them under a trash container
-- Creates trash container, finds all descendants via CTE, updates parent and scheme
-- All operations in single transaction (atomic)
-- @trash_parent_id: optional parent for trash container (NULL = root level)
-- =====================================================
CREATE PROCEDURE [dbo].[sp_mark_for_deletion]
    @object_ids NVARCHAR(MAX),  -- Comma-separated list of object IDs
    @user_id BIGINT,
    @trash_parent_id BIGINT = NULL,  -- Optional parent for trash container
    @trash_id BIGINT OUTPUT,
    @marked_count BIGINT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRANSACTION;
    
    BEGIN TRY
        -- 1. Create Trash container object with @@__deleted scheme
        -- Progress fields: _value_long=total, _key=deleted, _value_string=status
        SET @trash_id = NEXT VALUE FOR [dbo].[global_identity];
        
        INSERT INTO [dbo].[_objects] (
            [_id], [_id_scheme], [_id_parent], [_id_owner], [_id_who_change],
            [_name], [_date_create], [_date_modify],
            [_value_long], [_key], [_value_string]
        ) VALUES (
            @trash_id, 
            -10,  -- @@__deleted scheme
            @trash_parent_id,  -- user-specified parent or NULL
            @user_id, 
            @user_id,
            '__TRASH__' + CAST(@user_id AS NVARCHAR(20)) + '_' + CAST(DATEDIFF(SECOND, '1970-01-01', GETUTCDATE()) AS NVARCHAR(20)),
            SYSDATETIMEOFFSET(), 
            SYSDATETIMEOFFSET(),
            0,          -- _value_long = total (will be updated after count)
            0,          -- _key = deleted
            'pending'   -- _value_string = status
        );
        
        -- 2. Create temp table for objects to process
        CREATE TABLE #objects_to_delete (_id BIGINT);
        
        -- 3. CTE: find all objects and their descendants recursively
        ;WITH all_descendants AS (
            -- Start with requested objects
            SELECT o._id 
            FROM [dbo].[_objects] o
            WHERE o._id IN (SELECT CAST(value AS BIGINT) FROM STRING_SPLIT(@object_ids, ','))
              AND o._id_scheme != -10  -- skip already deleted
            
            UNION ALL
            
            -- Recursively find children
            SELECT o._id 
            FROM [dbo].[_objects] o
            INNER JOIN all_descendants d ON o._id_parent = d._id
            WHERE o._id_scheme != -10  -- skip already deleted
        )
        INSERT INTO #objects_to_delete
        SELECT _id FROM all_descendants;
        
        -- 4. UPDATE: move all found objects under Trash container and change scheme
        UPDATE [dbo].[_objects] 
        SET [_id_parent] = @trash_id,
            [_id_scheme] = -10,
            [_date_modify] = SYSDATETIMEOFFSET()
        WHERE [_id] IN (SELECT _id FROM #objects_to_delete);
        
        SET @marked_count = @@ROWCOUNT;
        
        -- 5. Update trash container with total count
        UPDATE [dbo].[_objects] 
        SET [_value_long] = @marked_count
        WHERE [_id] = @trash_id;
        
        DROP TABLE #objects_to_delete;
        
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END
GO

-- =====================================================
-- PROCEDURE: sp_purge_trash
-- Physically deletes objects from a trash container in batches
-- TR__objects__cascade_values trigger handles _values deletion
-- Updates progress in trash container (_key=deleted, _value_string=status)
-- After all children deleted, removes the trash container itself
-- =====================================================
CREATE PROCEDURE [dbo].[sp_purge_trash]
    @trash_id BIGINT,
    @batch_size INT = 10,
    @deleted_count BIGINT OUTPUT,
    @remaining_count BIGINT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Update status to 'running' if it was 'pending'
    UPDATE [dbo].[_objects] 
    SET [_value_string] = 'running',
        [_date_modify] = SYSDATETIMEOFFSET()
    WHERE [_id] = @trash_id AND [_value_string] = 'pending';
    
    -- Delete a batch of objects (trigger handles _values cascade)
    DELETE TOP (@batch_size) FROM [dbo].[_objects]
    WHERE [_id_parent] = @trash_id;
    
    SET @deleted_count = @@ROWCOUNT;
    
    -- Count remaining objects in this trash
    SELECT @remaining_count = COUNT(*) 
    FROM [dbo].[_objects] 
    WHERE [_id_parent] = @trash_id;
    
    -- Update progress in trash container
    UPDATE [dbo].[_objects] 
    SET [_key] = [_key] + @deleted_count,
        [_value_string] = CASE WHEN @remaining_count = 0 THEN 'completed' ELSE 'running' END,
        [_date_modify] = SYSDATETIMEOFFSET()
    WHERE [_id] = @trash_id;
    
    -- If no more children, delete the trash container itself
    IF @remaining_count = 0
    BEGIN
        DELETE FROM [dbo].[_objects] WHERE [_id] = @trash_id;
    END
END
GO



-- ===== redb_structure_tree.sql =====
-- ====================================================================================================
-- FUNCTIONS FOR SCHEME STRUCTURE TREE
-- MS SQL Server version
-- ====================================================================================================
-- Supports hierarchical navigation: parent -> children -> descendants
-- Solves flat structure search issues in SaveAsync
-- ====================================================================================================



-- =====================================================
-- DROP EXISTING OBJECTS
-- =====================================================

IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'get_scheme_structure_tree')
    DROP PROCEDURE [dbo].[get_scheme_structure_tree]
GO

IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'get_structure_children')
    DROP PROCEDURE [dbo].[get_structure_children]
GO

IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'validate_structure_tree')
    DROP PROCEDURE [dbo].[validate_structure_tree]
GO

IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'get_structure_descendants')
    DROP PROCEDURE [dbo].[get_structure_descendants]
GO

-- =====================================================
-- 1. MAIN PROCEDURE: Build scheme structure tree
-- =====================================================

CREATE PROCEDURE [dbo].[get_scheme_structure_tree]
    @scheme_id BIGINT,
    @parent_id BIGINT = NULL,
    @max_depth INT = 10,
    @result NVARCHAR(MAX) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @structure_id BIGINT;
    DECLARE @name NVARCHAR(450);
    DECLARE @order BIGINT;
    DECLARE @is_array BIT;
    DECLARE @collection_type BIGINT;
    DECLARE @store_null BIT;
    DECLARE @allow_not_null BIT;
    DECLARE @type_name NVARCHAR(450);
    DECLARE @db_type NVARCHAR(450);
    DECLARE @type_semantic NVARCHAR(450);
    DECLARE @children_json NVARCHAR(MAX);
    DECLARE @item_json NVARCHAR(MAX);
    DECLARE @next_depth INT;
    
    SET @result = N'[]';
    
    -- Protection from infinite recursion
    IF @max_depth <= 0
    BEGIN
        SET @result = N'[{"error":"Max recursion depth reached"}]';
        RETURN;
    END
    
    -- Check if scheme exists
    IF NOT EXISTS(SELECT 1 FROM [dbo].[_schemes] WHERE [_id] = @scheme_id)
    BEGIN
        SET @result = N'[{"error":"Scheme not found"}]';
        RETURN;
    END
    
    -- Auto-fill cache if empty
    IF NOT EXISTS(SELECT 1 FROM [dbo].[_scheme_metadata_cache] WHERE [_scheme_id] = @scheme_id)
    BEGIN
        EXEC [dbo].[sync_metadata_cache_for_scheme] @scheme_id;
    END
    
    -- Create temp table for results
    DECLARE @items TABLE (
        [structure_id] BIGINT,
        [name] NVARCHAR(450),
        [order] BIGINT,
        [is_array] BIT,
        [collection_type] BIGINT,
        [store_null] BIT,
        [allow_not_null] BIT,
        [type_name] NVARCHAR(450),
        [db_type] NVARCHAR(450),
        [type_semantic] NVARCHAR(450),
        [children] NVARCHAR(MAX)
    );
    
    -- Get structures for current level from cache
    DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
        SELECT 
            c.[_structure_id],
            c.[_name],
            c.[_order],
            CASE WHEN c.[_collection_type] IS NOT NULL THEN 1 ELSE 0 END,
            c.[_collection_type],
            c.[_store_null],
            c.[_allow_not_null],
            c.[type_name],
            c.[db_type],
            c.[type_semantic]
        FROM [dbo].[_scheme_metadata_cache] c
        WHERE c.[_scheme_id] = @scheme_id
          AND ((@parent_id IS NULL AND c.[_parent_structure_id] IS NULL) 
               OR (@parent_id IS NOT NULL AND c.[_parent_structure_id] = @parent_id))
        ORDER BY c.[_order], c.[_structure_id];
    
    OPEN cur;
    FETCH NEXT FROM cur INTO @structure_id, @name, @order, @is_array, @collection_type,
                              @store_null, @allow_not_null, @type_name, @db_type, @type_semantic;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Check if has children
        IF EXISTS(SELECT 1 FROM [dbo].[_structures] 
                  WHERE [_id_scheme] = @scheme_id AND [_id_parent] = @structure_id)
        BEGIN
            -- Recursively get children
            SET @next_depth = @max_depth - 1;
            EXEC [dbo].[get_scheme_structure_tree] 
                @scheme_id, 
                @structure_id, 
                @next_depth, 
                @children_json OUTPUT;
        END
        ELSE
        BEGIN
            SET @children_json = N'[]';
        END
        
        -- Insert into temp table
        INSERT INTO @items VALUES (
            @structure_id, @name, @order, @is_array, @collection_type,
            @store_null, @allow_not_null, @type_name, @db_type, @type_semantic,
            @children_json
        );
        
        FETCH NEXT FROM cur INTO @structure_id, @name, @order, @is_array, @collection_type,
                                  @store_null, @allow_not_null, @type_name, @db_type, @type_semantic;
    END
    
    CLOSE cur;
    DEALLOCATE cur;
    
    -- Build JSON result
    SELECT @result = N'[' + ISNULL(STRING_AGG(
        N'{"structure_id":' + CAST([structure_id] AS NVARCHAR(20)) +
        N',"name":' + QUOTENAME([name], '"') +
        N',"order":' + ISNULL(CAST([order] AS NVARCHAR(20)), 'null') +
        N',"is_array":' + CASE WHEN [is_array] = 1 THEN 'true' ELSE 'false' END +
        N',"collection_type":' + ISNULL(CAST([collection_type] AS NVARCHAR(20)), 'null') +
        N',"store_null":' + CASE WHEN [store_null] = 1 THEN 'true' WHEN [store_null] = 0 THEN 'false' ELSE 'null' END +
        N',"allow_not_null":' + CASE WHEN [allow_not_null] = 1 THEN 'true' WHEN [allow_not_null] = 0 THEN 'false' ELSE 'null' END +
        N',"type_name":' + QUOTENAME([type_name], '"') +
        N',"db_type":' + QUOTENAME([db_type], '"') +
        N',"type_semantic":' + QUOTENAME([type_semantic], '"') +
        N',"children":' + [children] + N'}'
    , ','), '') + N']'
    FROM @items;
END
GO

-- =====================================================
-- 2. HELPER: Get direct children only
-- =====================================================

CREATE PROCEDURE [dbo].[get_structure_children]
    @scheme_id BIGINT,
    @parent_id BIGINT,
    @result NVARCHAR(MAX) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Auto-fill cache if empty
    IF NOT EXISTS(SELECT 1 FROM [dbo].[_scheme_metadata_cache] WHERE [_scheme_id] = @scheme_id)
    BEGIN
        EXEC [dbo].[sync_metadata_cache_for_scheme] @scheme_id;
    END
    
    SELECT @result = N'[' + ISNULL(STRING_AGG(json_item, ',') WITHIN GROUP (ORDER BY [_order], [_structure_id]), '') + N']'
    FROM (
        SELECT 
            c.[_structure_id],
            c.[_order],
            N'{"structure_id":' + CAST(c.[_structure_id] AS NVARCHAR(20)) +
            N',"name":' + QUOTENAME(c.[_name], '"') +
            N',"order":' + ISNULL(CAST(c.[_order] AS NVARCHAR(20)), 'null') +
            N',"is_array":' + CASE WHEN c.[_collection_type] IS NOT NULL THEN 'true' ELSE 'false' END +
            N',"collection_type":' + ISNULL(CAST(c.[_collection_type] AS NVARCHAR(20)), 'null') +
            N',"type_name":' + QUOTENAME(c.[type_name], '"') +
            N',"db_type":' + QUOTENAME(c.[db_type], '"') +
            N',"type_semantic":' + QUOTENAME(c.[type_semantic], '"') + N'}' AS json_item
        FROM [dbo].[_scheme_metadata_cache] c
        WHERE c.[_scheme_id] = @scheme_id
          AND c.[_parent_structure_id] = @parent_id
    ) sub;
END
GO

-- =====================================================
-- 3. DIAGNOSTIC: Validate structure tree
-- =====================================================

CREATE PROCEDURE [dbo].[validate_structure_tree]
    @scheme_id BIGINT,
    @result NVARCHAR(MAX) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @excessive_json NVARCHAR(MAX);
    DECLARE @orphaned_json NVARCHAR(MAX);
    DECLARE @circular_json NVARCHAR(MAX);
    DECLARE @total_count INT;
    DECLARE @is_valid BIT;
    
    -- 1. Find excessive structures (no values linked)
    SELECT @excessive_json = N'[' + ISNULL(STRING_AGG(
        N'{"structure_id":' + CAST(s.[_id] AS NVARCHAR(20)) +
        N',"name":' + QUOTENAME(s.[_name], '"') +
        N',"parent_name":' + ISNULL(QUOTENAME(parent_s.[_name], '"'), 'null') +
        N',"issue":"No values found - possibly excessive structure"}'
    , ','), '') + N']'
    FROM [dbo].[_structures] s
    LEFT JOIN [dbo].[_structures] parent_s ON parent_s.[_id] = s.[_id_parent]
    LEFT JOIN [dbo].[_values] v ON v.[_id_structure] = s.[_id]
    WHERE s.[_id_scheme] = @scheme_id
      AND v.[_id] IS NULL
      AND s.[_id_parent] IS NOT NULL;
    
    -- 2. Find orphaned structures (parent does not exist)
    SELECT @orphaned_json = N'[' + ISNULL(STRING_AGG(
        N'{"structure_id":' + CAST(s.[_id] AS NVARCHAR(20)) +
        N',"name":' + QUOTENAME(s.[_name], '"') +
        N',"parent_id":' + CAST(s.[_id_parent] AS NVARCHAR(20)) +
        N',"issue":"Parent structure does not exist"}'
    , ','), '') + N']'
    FROM [dbo].[_structures] s
    WHERE s.[_id_scheme] = @scheme_id
      AND s.[_id_parent] IS NOT NULL
      AND NOT EXISTS(SELECT 1 FROM [dbo].[_structures] parent_s WHERE parent_s.[_id] = s.[_id_parent]);
    
    -- 3. Check for circular references (using path string)
    ;WITH cycle_check AS (
        SELECT 
            [_id], 
            [_id_parent], 
            CAST([_id] AS NVARCHAR(MAX)) AS [path],
            0 AS has_cycle
        FROM [dbo].[_structures] 
        WHERE [_id_scheme] = @scheme_id AND [_id_parent] IS NOT NULL
        
        UNION ALL
        
        SELECT 
            s.[_id], 
            s.[_id_parent], 
            cc.[path] + ',' + CAST(s.[_id] AS NVARCHAR(20)),
            CASE WHEN CHARINDEX(',' + CAST(s.[_id] AS NVARCHAR(20)) + ',', ',' + cc.[path] + ',') > 0 THEN 1 ELSE 0 END
        FROM [dbo].[_structures] s
        INNER JOIN cycle_check cc ON cc.[_id_parent] = s.[_id]
        WHERE cc.has_cycle = 0 AND LEN(cc.[path]) < 1000
    )
    SELECT @circular_json = N'[' + ISNULL(STRING_AGG(
        N'{"structure_id":' + CAST([_id] AS NVARCHAR(20)) +
        N',"path":"' + [path] +
        N'","issue":"Circular reference detected"}'
    , ','), '') + N']'
    FROM cycle_check 
    WHERE has_cycle = 1;
    
    -- Get total count
    SELECT @total_count = COUNT(*) FROM [dbo].[_structures] WHERE [_id_scheme] = @scheme_id;
    
    -- Determine if valid
    SET @is_valid = CASE 
        WHEN @excessive_json = N'[]' AND @orphaned_json = N'[]' AND @circular_json = N'[]' 
        THEN 1 ELSE 0 
    END;
    
    -- Build result
    SET @result = N'{' +
        N'"scheme_id":' + CAST(@scheme_id AS NVARCHAR(20)) +
        N',"validation_date":"' + CONVERT(NVARCHAR(30), GETDATE(), 127) + '"' +
        N',"excessive_structures":' + ISNULL(@excessive_json, N'[]') +
        N',"orphaned_structures":' + ISNULL(@orphaned_json, N'[]') +
        N',"circular_references":' + ISNULL(@circular_json, N'[]') +
        N',"total_structures":' + CAST(@total_count AS NVARCHAR(10)) +
        N',"is_valid":' + CASE WHEN @is_valid = 1 THEN 'true' ELSE 'false' END +
    N'}';
END
GO

-- =====================================================
-- 4. HELPER: Get all descendants (flat list)
-- =====================================================

CREATE PROCEDURE [dbo].[get_structure_descendants]
    @scheme_id BIGINT,
    @parent_id BIGINT,
    @result NVARCHAR(MAX) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    ;WITH descendants AS (
        -- Direct children
        SELECT [_id], [_name], [_id_parent], 0 AS [level]
        FROM [dbo].[_structures] 
        WHERE [_id_scheme] = @scheme_id AND [_id_parent] = @parent_id
        
        UNION ALL
        
        -- Recursive descendants
        SELECT s.[_id], s.[_name], s.[_id_parent], d.[level] + 1
        FROM [dbo].[_structures] s
        INNER JOIN descendants d ON d.[_id] = s.[_id_parent]
        WHERE s.[_id_scheme] = @scheme_id AND d.[level] < 10
    )
    SELECT @result = N'[' + ISNULL(STRING_AGG(json_item, ',') WITHIN GROUP (ORDER BY [level], [_id]), '') + N']'
    FROM (
        SELECT 
            [_id],
            [level],
            N'{"structure_id":' + CAST([_id] AS NVARCHAR(20)) +
            N',"name":' + QUOTENAME([_name], '"') +
            N',"parent_id":' + ISNULL(CAST([_id_parent] AS NVARCHAR(20)), 'null') +
            N',"level":' + CAST([level] AS NVARCHAR(10)) + N'}' AS json_item
        FROM descendants
    ) sub;
END
GO

-- =====================================================
-- USAGE EXAMPLES
-- =====================================================
-- 
-- Get full structure tree:
--   DECLARE @json NVARCHAR(MAX);
--   EXEC get_scheme_structure_tree @scheme_id = 123, @result = @json OUTPUT;
--   SELECT @json;
--
-- Get direct children:
--   DECLARE @json NVARCHAR(MAX);
--   EXEC get_structure_children @scheme_id = 123, @parent_id = 456, @result = @json OUTPUT;
--   SELECT @json;
--
-- Validate tree:
--   DECLARE @json NVARCHAR(MAX);
--   EXEC validate_structure_tree @scheme_id = 123, @result = @json OUTPUT;
--   SELECT @json;
--
-- Get all descendants:
--   DECLARE @json NVARCHAR(MAX);
--   EXEC get_structure_descendants @scheme_id = 123, @parent_id = 456, @result = @json OUTPUT;
--   SELECT @json;
-- =====================================================

PRINT '========================================='
PRINT 'Structure tree procedures created!'
PRINT '========================================='
GO



-- ===== sp_redb_json_objects.sql =====
-- ====================================================================================================
-- JSON OBJECT FUNCTIONS
-- MS SQL Server version
-- ====================================================================================================
-- Builds JSON representation of objects from EAV model
-- Supports: hierarchical Class fields, arrays, dictionaries, Object references
-- ====================================================================================================


-- =====================================================
-- DROP EXISTING OBJECTS
-- =====================================================

IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'sp_get_object_json')
    DROP PROCEDURE [dbo].[sp_get_object_json]
GO

IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'sp_get_object_json_internal')
    DROP PROCEDURE [dbo].[sp_get_object_json_internal]
GO

IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'sp_build_hierarchical_properties')
    DROP PROCEDURE [dbo].[sp_build_hierarchical_properties]
GO

-- Drop temp table type if exists
IF EXISTS (SELECT * FROM sys.types WHERE name = 'ValuesTableType')
    DROP TYPE [dbo].[ValuesTableType]
GO

-- =====================================================
-- CREATE TABLE TYPE FOR VALUES (used in temp table)
-- =====================================================

-- Note: We use #all_values temp table instead of TVP for recursion support

-- =====================================================
-- HELPER: Build hierarchical properties from #all_values
-- =====================================================
-- IMPORTANT: Requires #all_values temp table to exist in session!
-- Called recursively by get_object_json

CREATE PROCEDURE [dbo].[sp_build_hierarchical_properties]
    @object_id BIGINT,
    @parent_structure_id BIGINT = NULL,
    @object_scheme_id BIGINT,
    @max_depth INT = 10,
    @array_index NVARCHAR(430) = NULL,
    @parent_value_id BIGINT = NULL,
    @result NVARCHAR(MAX) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @field_results TABLE (
        [field_name] NVARCHAR(450),
        [field_order] BIGINT,
        [field_value] NVARCHAR(MAX)
    );
    
    -- Variables for cursor
    DECLARE @structure_id BIGINT;
    DECLARE @field_name NVARCHAR(450);
    DECLARE @field_order BIGINT;
    DECLARE @collection_type BIGINT;
    DECLARE @is_array BIT;
    DECLARE @is_dictionary BIT;
    DECLARE @type_name NVARCHAR(450);
    DECLARE @db_type NVARCHAR(450);
    DECLARE @type_semantic NVARCHAR(450);
    
    -- Variables for current value
    DECLARE @current_value_id BIGINT;
    DECLARE @current_String NVARCHAR(MAX);
    DECLARE @current_Long BIGINT;
    DECLARE @current_Guid UNIQUEIDENTIFIER;
    DECLARE @current_Double FLOAT;
    DECLARE @current_Numeric DECIMAL(38,18);
    DECLARE @current_DateTimeOffset DATETIMEOFFSET;
    DECLARE @current_Boolean BIT;
    DECLARE @current_ByteArray VARBINARY(MAX);
    DECLARE @current_ListItem BIGINT;
    DECLARE @current_Object BIGINT;
    
    DECLARE @field_value NVARCHAR(MAX);
    DECLARE @base_array_value_id BIGINT;
    DECLARE @children_json NVARCHAR(MAX);
    DECLARE @temp_json NVARCHAR(MAX);
    DECLARE @next_depth INT;
    
    -- Variables for array/dictionary handling (must be declared at procedure level)
    DECLARE @elem_array_index NVARCHAR(430);
    DECLARE @elem_value_id BIGINT;
    DECLARE @ref_object_id BIGINT;
    DECLARE @dict_key NVARCHAR(430);
    DECLARE @dict_value_id BIGINT;
    DECLARE @dict_obj_key NVARCHAR(430);
    DECLARE @dict_obj_id BIGINT;
    
    -- Table variables for collecting results (must be declared at procedure level)
    DECLARE @array_elements TABLE ([idx] INT IDENTITY, [element_json] NVARCHAR(MAX));
    DECLARE @obj_array TABLE ([idx] INT IDENTITY, [obj_json] NVARCHAR(MAX));
    DECLARE @dict_elements TABLE ([key] NVARCHAR(430), [element_json] NVARCHAR(MAX));
    DECLARE @dict_obj TABLE ([key] NVARCHAR(430), [obj_json] NVARCHAR(MAX));
    
    -- Protection against infinite recursion
    IF @max_depth < -100
    BEGIN
        SET @result = N'{"error":"Max recursion depth reached"}';
        RETURN;
    END
    
    -- Auto-fill cache if empty
    IF NOT EXISTS(SELECT 1 FROM [dbo].[_scheme_metadata_cache] WHERE [_scheme_id] = @object_scheme_id)
    BEGIN
        EXEC [dbo].[sync_metadata_cache_for_scheme] @object_scheme_id;
    END
    
    -- Iterate through structures for current level
    DECLARE structure_cursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT 
            c.[_structure_id],
            c.[_name],
            c.[_order],
            c.[_collection_type],
            CASE WHEN c.[_collection_type] = -9223372036854775668 THEN 1 ELSE 0 END, -- Array type ID
            CASE WHEN c.[_collection_type] = -9223372036854775667 THEN 1 ELSE 0 END, -- Dictionary type ID
            c.[type_name],
            c.[db_type],
            c.[type_semantic]
        FROM [dbo].[_scheme_metadata_cache] c
        WHERE c.[_scheme_id] = @object_scheme_id
          AND ((@parent_structure_id IS NULL AND c.[_parent_structure_id] IS NULL) 
               OR (@parent_structure_id IS NOT NULL AND c.[_parent_structure_id] = @parent_structure_id))
        ORDER BY c.[_order], c.[_structure_id];
    
    OPEN structure_cursor;
    FETCH NEXT FROM structure_cursor INTO @structure_id, @field_name, @field_order, @collection_type,
                                          @is_array, @is_dictionary, @type_name, @db_type, @type_semantic;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @field_value = NULL;
        SET @current_value_id = NULL;
        SET @base_array_value_id = NULL;
        
        -- Find current value from #all_values
        IF @parent_value_id IS NOT NULL
        BEGIN
            -- For nested fields inside array/dictionary elements
            SELECT TOP 1
                @current_value_id = [_id],
                @current_String = [_String],
                @current_Long = [_Long],
                @current_Guid = [_Guid],
                @current_Double = [_Double],
                @current_Numeric = [_Numeric],
                @current_DateTimeOffset = [_DateTimeOffset],
                @current_Boolean = [_Boolean],
                @current_ByteArray = [_ByteArray],
                @current_ListItem = [_ListItem],
                @current_Object = [_Object]
            FROM #all_values
            WHERE [_id_object] = @object_id
              AND [_id_structure] = @structure_id
              AND [_array_parent_id] = @parent_value_id;
        END
        ELSE IF @array_index IS NOT NULL
        BEGIN
            -- For array/dictionary elements
            SELECT TOP 1
                @current_value_id = [_id],
                @current_String = [_String],
                @current_Long = [_Long],
                @current_Guid = [_Guid],
                @current_Double = [_Double],
                @current_Numeric = [_Numeric],
                @current_DateTimeOffset = [_DateTimeOffset],
                @current_Boolean = [_Boolean],
                @current_ByteArray = [_ByteArray],
                @current_ListItem = [_ListItem],
                @current_Object = [_Object]
            FROM #all_values
            WHERE [_id_object] = @object_id
              AND [_id_structure] = @structure_id
              AND [_array_index] = @array_index;
        END
        ELSE
        BEGIN
            -- For regular fields
            SELECT TOP 1
                @current_value_id = [_id],
                @current_String = [_String],
                @current_Long = [_Long],
                @current_Guid = [_Guid],
                @current_Double = [_Double],
                @current_Numeric = [_Numeric],
                @current_DateTimeOffset = [_DateTimeOffset],
                @current_Boolean = [_Boolean],
                @current_ByteArray = [_ByteArray],
                @current_ListItem = [_ListItem],
                @current_Object = [_Object]
            FROM #all_values
            WHERE [_id_object] = @object_id
              AND [_id_structure] = @structure_id
              AND [_array_index] IS NULL;
        END
        
        -- Get base array/dictionary record ID for collections
        IF @is_array = 1 OR @is_dictionary = 1
        BEGIN
            IF @parent_value_id IS NULL
            BEGIN
                SELECT TOP 1 @base_array_value_id = [_id]
                FROM #all_values
                WHERE [_id_object] = @object_id
                  AND [_id_structure] = @structure_id
                  AND [_array_index] IS NULL
                  AND [_array_parent_id] IS NULL;
            END
            ELSE
            BEGIN
                SELECT TOP 1 @base_array_value_id = [_id]
                FROM #all_values
                WHERE [_id_object] = @object_id
                  AND [_id_structure] = @structure_id
                  AND [_array_index] IS NULL
                  AND [_array_parent_id] = @parent_value_id;
            END
        END
        
        -- =====================================================
        -- ARRAYS
        -- =====================================================
        IF @is_array = 1
        BEGIN
            IF @type_semantic = 'Object' -- Array of Class fields
            BEGIN
                -- Build array of Class objects recursively
                DELETE FROM @array_elements;
                
                DECLARE elem_cursor CURSOR LOCAL FAST_FORWARD FOR
                    SELECT [_array_index], [_id]
                    FROM #all_values
                    WHERE [_id_object] = @object_id
                      AND [_id_structure] = @structure_id
                      AND [_array_index] IS NOT NULL
                      AND (@base_array_value_id IS NULL OR [_array_parent_id] = @base_array_value_id)
                    ORDER BY CASE WHEN [_array_index] LIKE '[0-9]%' AND ISNUMERIC([_array_index]) = 1 
                                  THEN CAST([_array_index] AS INT) ELSE 0 END, [_array_index];
                
                OPEN elem_cursor;
                FETCH NEXT FROM elem_cursor INTO @elem_array_index, @elem_value_id;
                
                WHILE @@FETCH_STATUS = 0
                BEGIN
                    EXEC [dbo].[sp_build_hierarchical_properties]
                        @object_id,
                        @structure_id,
                        @object_scheme_id,
                        @max_depth,
                        @elem_array_index,
                        @elem_value_id,
                        @temp_json OUTPUT;
                    
                    INSERT INTO @array_elements ([element_json]) VALUES (@temp_json);
                    FETCH NEXT FROM elem_cursor INTO @elem_array_index, @elem_value_id;
                END
                
                CLOSE elem_cursor;
                DEALLOCATE elem_cursor;
                
                -- Build JSON array
                SELECT @field_value = N'[' + ISNULL(STRING_AGG([element_json], N',') WITHIN GROUP (ORDER BY [idx]), N'') + N']'
                FROM @array_elements;
            END
            ELSE -- Array of primitives or _RObject references
            BEGIN
                -- Handle _RObject arrays with recursion (cannot use subquery in STRING_AGG)
                IF @type_semantic = '_RObject'
                BEGIN
                    DELETE FROM @obj_array;
                    
                    DECLARE obj_cursor CURSOR LOCAL FAST_FORWARD FOR
                        SELECT v.[_Object]
                        FROM #all_values v
                        WHERE v.[_id_object] = @object_id
                          AND v.[_id_structure] = @structure_id
                          AND v.[_array_index] IS NOT NULL
                          AND v.[_Object] IS NOT NULL
                          AND (@base_array_value_id IS NULL OR v.[_array_parent_id] = @base_array_value_id)
                        ORDER BY CASE WHEN v.[_array_index] LIKE '[0-9]%' AND ISNUMERIC(v.[_array_index]) = 1 
                                      THEN CAST(v.[_array_index] AS INT) ELSE 0 END, v.[_array_index];
                    
                    OPEN obj_cursor;
                    FETCH NEXT FROM obj_cursor INTO @ref_object_id;
                    
                    WHILE @@FETCH_STATUS = 0
                    BEGIN
                        SET @next_depth = @max_depth - 1;
                        EXEC [dbo].[sp_get_object_json_internal] @ref_object_id, @next_depth, @temp_json OUTPUT;
                        INSERT INTO @obj_array ([obj_json]) VALUES (@temp_json);
                        FETCH NEXT FROM obj_cursor INTO @ref_object_id;
                    END
                    
                    CLOSE obj_cursor;
                    DEALLOCATE obj_cursor;
                    
                    SELECT @field_value = N'[' + ISNULL(STRING_AGG([obj_json], N',') WITHIN GROUP (ORDER BY [idx]), N'') + N']'
                    FROM @obj_array;
                END
                ELSE
                BEGIN
                    -- Array of primitives (String, Long, Guid, Double, ListItem, etc.)
                    SELECT @field_value = N'[' + ISNULL(STRING_AGG(
                        CASE 
                            WHEN @db_type = 'String' THEN 
                                CASE WHEN v.[_String] IS NULL THEN N'null' ELSE N'"' + REPLACE(REPLACE(v.[_String], N'\', N'\\'), N'"', N'\"') + N'"' END
                            WHEN @db_type = 'Long' THEN 
                                CASE WHEN v.[_Long] IS NULL THEN N'null' ELSE CAST(v.[_Long] AS NVARCHAR(30)) END
                            WHEN @db_type = 'Guid' THEN 
                                CASE WHEN v.[_Guid] IS NULL THEN N'null' ELSE N'"' + CAST(v.[_Guid] AS NVARCHAR(50)) + N'"' END
                            WHEN @db_type = 'Double' THEN 
                                CASE WHEN v.[_Double] IS NULL THEN N'null' ELSE FORMAT(v.[_Double], 'G', 'en-US') END
                            WHEN @db_type = 'Numeric' THEN 
                                CASE WHEN v.[_Numeric] IS NULL THEN N'null' ELSE REPLACE(CAST(v.[_Numeric] AS NVARCHAR(50)), N',', N'.') END
                            WHEN @db_type = 'DateTimeOffset' THEN 
                                CASE WHEN v.[_DateTimeOffset] IS NULL THEN N'null' ELSE N'"' + CONVERT(NVARCHAR(50), v.[_DateTimeOffset], 127) + N'"' END
                            WHEN @db_type = 'Boolean' THEN 
                                CASE WHEN v.[_Boolean] IS NULL THEN N'null' WHEN v.[_Boolean] = 1 THEN N'true' ELSE N'false' END
                            WHEN @db_type = 'ListItem' THEN 
                                CASE WHEN v.[_ListItem] IS NULL THEN N'null'
                                     ELSE dbo.build_listitem_json(v.[_ListItem])
                                END
                            ELSE N'null'
                        END
                    , N',') WITHIN GROUP (ORDER BY 
                        CASE WHEN v.[_array_index] LIKE '[0-9]%' AND ISNUMERIC(v.[_array_index]) = 1 
                             THEN CAST(v.[_array_index] AS INT) ELSE 0 END, 
                        v.[_array_index]
                    ), N'') + N']'
                    FROM #all_values v
                    WHERE v.[_id_object] = @object_id
                      AND v.[_id_structure] = @structure_id
                      AND v.[_array_index] IS NOT NULL
                      AND (@base_array_value_id IS NULL OR v.[_array_parent_id] = @base_array_value_id);
                END
            END
        END
        -- =====================================================
        -- DICTIONARIES
        -- =====================================================
        ELSE IF @is_dictionary = 1
        BEGIN
            IF @type_semantic = 'Object' -- Dictionary of Class fields
            BEGIN
                DELETE FROM @dict_elements;
                
                DECLARE dict_cursor CURSOR LOCAL FAST_FORWARD FOR
                    SELECT [_array_index], [_id]
                    FROM #all_values
                    WHERE [_id_object] = @object_id
                      AND [_id_structure] = @structure_id
                      AND [_array_index] IS NOT NULL
                      AND (@base_array_value_id IS NULL OR [_array_parent_id] = @base_array_value_id);
                
                OPEN dict_cursor;
                FETCH NEXT FROM dict_cursor INTO @dict_key, @dict_value_id;
                
                WHILE @@FETCH_STATUS = 0
                BEGIN
                    EXEC [dbo].[sp_build_hierarchical_properties]
                        @object_id,
                        @structure_id,
                        @object_scheme_id,
                        @max_depth,
                        NULL,
                        @dict_value_id,
                        @temp_json OUTPUT;
                    
                    INSERT INTO @dict_elements ([key], [element_json]) VALUES (@dict_key, @temp_json);
                    FETCH NEXT FROM dict_cursor INTO @dict_key, @dict_value_id;
                END
                
                CLOSE dict_cursor;
                DEALLOCATE dict_cursor;
                
                -- Build JSON object
                SELECT @field_value = N'{' + ISNULL(STRING_AGG(
                    N'"' + REPLACE(REPLACE([key], N'\', N'\\'), N'"', N'\"') + N'":' + [element_json]
                , N','), N'') + N'}'
                FROM @dict_elements;
            END
            ELSE IF @type_semantic = '_RObject' -- Dictionary of Object references
            BEGIN
                DELETE FROM @dict_obj;
                
                DECLARE dict_obj_cursor CURSOR LOCAL FAST_FORWARD FOR
                    SELECT v.[_array_index], v.[_Object]
                    FROM #all_values v
                    WHERE v.[_id_object] = @object_id
                      AND v.[_id_structure] = @structure_id
                      AND v.[_array_index] IS NOT NULL
                      AND v.[_Object] IS NOT NULL
                      AND (@base_array_value_id IS NULL OR v.[_array_parent_id] = @base_array_value_id);
                
                OPEN dict_obj_cursor;
                FETCH NEXT FROM dict_obj_cursor INTO @dict_obj_key, @dict_obj_id;
                
                WHILE @@FETCH_STATUS = 0
                BEGIN
                    SET @next_depth = @max_depth - 1;
                    EXEC [dbo].[sp_get_object_json_internal] @dict_obj_id, @next_depth, @temp_json OUTPUT;
                    INSERT INTO @dict_obj ([key], [obj_json]) VALUES (@dict_obj_key, @temp_json);
                    FETCH NEXT FROM dict_obj_cursor INTO @dict_obj_key, @dict_obj_id;
                END
                
                CLOSE dict_obj_cursor;
                DEALLOCATE dict_obj_cursor;
                
                SELECT @field_value = N'{' + ISNULL(STRING_AGG(
                    N'"' + REPLACE(REPLACE([key], N'\', N'\\'), N'"', N'\"') + N'":' + [obj_json]
                , N','), N'') + N'}'
                FROM @dict_obj;
            END
            ELSE -- Dictionary of primitives
            BEGIN
                SELECT @field_value = N'{' + ISNULL(STRING_AGG(
                    N'"' + REPLACE(REPLACE(v.[_array_index], N'\', N'\\'), N'"', N'\"') + N'":' +
                    CASE 
                        WHEN @db_type = 'String' THEN 
                            CASE WHEN v.[_String] IS NULL THEN N'null' ELSE N'"' + REPLACE(REPLACE(v.[_String], N'\', N'\\'), N'"', N'\"') + N'"' END
                        WHEN @db_type = 'Long' THEN 
                            CASE WHEN v.[_Long] IS NULL THEN N'null' ELSE CAST(v.[_Long] AS NVARCHAR(30)) END
                        WHEN @db_type = 'Guid' THEN 
                            CASE WHEN v.[_Guid] IS NULL THEN N'null' ELSE N'"' + CAST(v.[_Guid] AS NVARCHAR(50)) + N'"' END
                        WHEN @db_type = 'Double' THEN 
                            CASE WHEN v.[_Double] IS NULL THEN N'null' ELSE FORMAT(v.[_Double], 'G', 'en-US') END
                        WHEN @db_type = 'Numeric' THEN 
                            CASE WHEN v.[_Numeric] IS NULL THEN N'null' ELSE REPLACE(CAST(v.[_Numeric] AS NVARCHAR(50)), N',', N'.') END
                        WHEN @db_type = 'DateTimeOffset' THEN 
                            CASE WHEN v.[_DateTimeOffset] IS NULL THEN N'null' ELSE N'"' + CONVERT(NVARCHAR(50), v.[_DateTimeOffset], 127) + N'"' END
                        WHEN @db_type = 'Boolean' THEN 
                            CASE WHEN v.[_Boolean] IS NULL THEN N'null' WHEN v.[_Boolean] = 1 THEN N'true' ELSE N'false' END
                        ELSE N'null'
                    END
                , N','), N'') + N'}'
                FROM #all_values v
                WHERE v.[_id_object] = @object_id
                  AND v.[_id_structure] = @structure_id
                  AND v.[_array_index] IS NOT NULL
                  AND (@base_array_value_id IS NULL OR v.[_array_parent_id] = @base_array_value_id);
            END
        END
        -- =====================================================
        -- OBJECT REFERENCE (_RObject)
        -- =====================================================
        ELSE IF @type_name = 'Object' AND @type_semantic = '_RObject'
        BEGIN
            IF @current_Object IS NOT NULL
            BEGIN
                SET @next_depth = @max_depth - 1;
                EXEC [dbo].[sp_get_object_json_internal] @current_Object, @next_depth, @field_value OUTPUT;
            END
        END
        -- =====================================================
        -- CLASS FIELD (hierarchical)
        -- =====================================================
        ELSE IF @type_semantic = 'Object'
        BEGIN
            IF @current_Guid IS NOT NULL
            BEGIN
                EXEC [dbo].[sp_build_hierarchical_properties]
                    @object_id,
                    @structure_id,
                    @object_scheme_id,
                    @max_depth,
                    NULL,
                    @current_value_id,
                    @field_value OUTPUT;
            END
        END
        -- =====================================================
        -- PRIMITIVE TYPES
        -- =====================================================
        ELSE IF @current_value_id IS NOT NULL
        BEGIN
            SET @field_value = CASE 
                WHEN @db_type = 'String' THEN 
                    CASE WHEN @current_String IS NULL THEN NULL 
                         ELSE N'"' + REPLACE(REPLACE(@current_String, N'\', N'\\'), N'"', N'\"') + N'"' END
                WHEN @db_type = 'Long' THEN 
                    CASE 
                        WHEN @current_ListItem IS NOT NULL THEN dbo.build_listitem_json(@current_ListItem)
                        WHEN @current_Long IS NULL THEN NULL 
                        ELSE CAST(@current_Long AS NVARCHAR(30)) 
                    END
                WHEN @db_type = 'Guid' THEN 
                    CASE WHEN @current_Guid IS NULL THEN NULL 
                         ELSE N'"' + CAST(@current_Guid AS NVARCHAR(50)) + N'"' END
                WHEN @db_type = 'Double' THEN 
                    CASE WHEN @current_Double IS NULL THEN NULL 
                         ELSE FORMAT(@current_Double, 'G', 'en-US') END
                WHEN @db_type = 'Numeric' THEN 
                    CASE WHEN @current_Numeric IS NULL THEN NULL 
                         ELSE REPLACE(CAST(@current_Numeric AS NVARCHAR(50)), N',', N'.') END
                WHEN @db_type = 'DateTimeOffset' THEN 
                    CASE WHEN @current_DateTimeOffset IS NULL THEN NULL 
                         ELSE N'"' + CONVERT(NVARCHAR(50), @current_DateTimeOffset, 127) + N'"' END
                WHEN @db_type = 'Boolean' THEN 
                    CASE WHEN @current_Boolean IS NULL THEN NULL 
                         WHEN @current_Boolean = 1 THEN N'true' ELSE N'false' END
                WHEN @db_type = 'ListItem' THEN dbo.build_listitem_json(@current_ListItem)
                WHEN @db_type = 'ByteArray' THEN
                    CASE WHEN @current_ByteArray IS NULL THEN NULL
                         ELSE N'"' + CAST(N'' AS XML).value('xs:base64Binary(sql:variable("@current_ByteArray"))', 'NVARCHAR(MAX)') + N'"'
                    END
                ELSE NULL
            END;
        END
        
        -- Add to results if not null
        IF @field_value IS NOT NULL
        BEGIN
            INSERT INTO @field_results ([field_name], [field_order], [field_value])
            VALUES (@field_name, @field_order, @field_value);
        END
        
        FETCH NEXT FROM structure_cursor INTO @structure_id, @field_name, @field_order, @collection_type,
                                              @is_array, @is_dictionary, @type_name, @db_type, @type_semantic;
    END
    
    CLOSE structure_cursor;
    DEALLOCATE structure_cursor;
    
    -- Build final JSON object
    SELECT @result = N'{' + ISNULL(STRING_AGG(
        N'"' + REPLACE(REPLACE([field_name], N'\', N'\\'), N'"', N'\"') + N'":' + [field_value]
    , N',') WITHIN GROUP (ORDER BY [field_order], [field_name]), N'') + N'}'
    FROM @field_results;
    
    IF @result IS NULL OR @result = N''
        SET @result = N'{}';
END
GO

-- =====================================================
-- MAIN: Get object as JSON (with OUTPUT for recursive calls)
-- =====================================================

CREATE PROCEDURE [dbo].[sp_get_object_json_internal]
    @object_id BIGINT,
    @max_depth INT = 10,
    @result NVARCHAR(MAX) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @object_scheme_id BIGINT;
    DECLARE @base_json NVARCHAR(MAX);
    DECLARE @properties_json NVARCHAR(MAX);
    DECLARE @is_root_call BIT = 0;
    
    -- Check if object exists - return NULL if not found (consistent with PostgreSQL)
    IF NOT EXISTS(SELECT 1 FROM [dbo].[_objects] WHERE [_id] = @object_id)
    BEGIN
        SET @result = NULL;
        RETURN;
    END
    
    -- Check if this is root call (need to create temp table)
    IF OBJECT_ID('tempdb..#all_values') IS NULL
    BEGIN
        SET @is_root_call = 1;
        
        -- Create temp table for all values
        CREATE TABLE #all_values (
            [_id] BIGINT,
            [_id_structure] BIGINT,
            [_id_object] BIGINT,
            [_String] NVARCHAR(MAX),
            [_Long] BIGINT,
            [_Guid] UNIQUEIDENTIFIER,
            [_Double] FLOAT,
            [_DateTimeOffset] DATETIMEOFFSET,
            [_Boolean] BIT,
            [_ByteArray] VARBINARY(MAX),
            [_Numeric] DECIMAL(38,18),
            [_ListItem] BIGINT,
            [_Object] BIGINT,
            [_array_parent_id] BIGINT,
            [_array_index] NVARCHAR(430)
        );
    END
    
    -- Load values for this object (always, for both root and recursive calls)
    -- Check if values for this object are already loaded
    IF NOT EXISTS(SELECT 1 FROM #all_values WHERE [_id_object] = @object_id)
    BEGIN
        INSERT INTO #all_values
        SELECT [_id], [_id_structure], [_id_object], [_String], [_Long], [_Guid], [_Double],
               [_DateTimeOffset], [_Boolean], [_ByteArray], [_Numeric], [_ListItem], [_Object],
               [_array_parent_id], [_array_index]
        FROM [dbo].[_values]
        WHERE [_id_object] = @object_id;
    END
    
    -- Get scheme_id
    SELECT @object_scheme_id = [_id_scheme]
    FROM [dbo].[_objects]
    WHERE [_id] = @object_id;
    
    -- Build base object JSON
    SELECT @base_json = N'{' +
        N'"id":' + CAST(o.[_id] AS NVARCHAR(20)) +
        N',"name":' + CASE WHEN o.[_name] IS NULL THEN N'null' ELSE N'"' + REPLACE(REPLACE(o.[_name], N'\', N'\\'), N'"', N'\"') + N'"' END +
        N',"scheme_id":' + CAST(o.[_id_scheme] AS NVARCHAR(20)) +
        N',"scheme_name":"' + REPLACE(REPLACE(s.[_name], N'\', N'\\'), N'"', N'\"') + N'"' +
        N',"parent_id":' + CASE WHEN o.[_id_parent] IS NULL THEN N'null' ELSE CAST(o.[_id_parent] AS NVARCHAR(20)) END +
        N',"owner_id":' + CAST(o.[_id_owner] AS NVARCHAR(20)) +
        N',"who_change_id":' + CAST(o.[_id_who_change] AS NVARCHAR(20)) +
        N',"date_create":"' + CONVERT(NVARCHAR(50), o.[_date_create], 127) + N'"' +
        N',"date_modify":"' + CONVERT(NVARCHAR(50), o.[_date_modify], 127) + N'"' +
        N',"date_begin":' + CASE WHEN o.[_date_begin] IS NULL THEN N'null' ELSE N'"' + CONVERT(NVARCHAR(50), o.[_date_begin], 127) + N'"' END +
        N',"date_complete":' + CASE WHEN o.[_date_complete] IS NULL THEN N'null' ELSE N'"' + CONVERT(NVARCHAR(50), o.[_date_complete], 127) + N'"' END +
        N',"key":' + CASE WHEN o.[_key] IS NULL THEN N'null' ELSE CAST(o.[_key] AS NVARCHAR(20)) END +
        N',"value_long":' + CASE WHEN o.[_value_long] IS NULL THEN N'null' ELSE CAST(o.[_value_long] AS NVARCHAR(20)) END +
        N',"value_string":' + CASE WHEN o.[_value_string] IS NULL THEN N'null' ELSE N'"' + REPLACE(REPLACE(o.[_value_string], N'\', N'\\'), N'"', N'\"') + N'"' END +
        N',"value_guid":' + CASE WHEN o.[_value_guid] IS NULL THEN N'null' ELSE N'"' + CAST(o.[_value_guid] AS NVARCHAR(50)) + N'"' END +
        N',"note":' + CASE WHEN o.[_note] IS NULL THEN N'null' ELSE N'"' + REPLACE(REPLACE(o.[_note], N'\', N'\\'), N'"', N'\"') + N'"' END +
        N',"value_bool":' + CASE WHEN o.[_value_bool] IS NULL THEN N'null' WHEN o.[_value_bool] = 1 THEN N'true' ELSE N'false' END +
        N',"value_double":' + CASE WHEN o.[_value_double] IS NULL THEN N'null' ELSE FORMAT(o.[_value_double], 'G', 'en-US') END +
        N',"value_numeric":' + CASE WHEN o.[_value_numeric] IS NULL THEN N'null' ELSE REPLACE(CAST(o.[_value_numeric] AS NVARCHAR(50)), N',', N'.') END +
        N',"value_datetime":' + CASE WHEN o.[_value_datetime] IS NULL THEN N'null' ELSE N'"' + CONVERT(NVARCHAR(50), o.[_value_datetime], 127) + N'"' END +
        N',"value_bytes":' + CASE WHEN o.[_value_bytes] IS NULL THEN N'null' ELSE N'"' + CAST(N'' AS XML).value('xs:base64Binary(sql:column("o.[_value_bytes]"))', 'NVARCHAR(MAX)') + N'"' END +
        N',"hash":' + CASE WHEN o.[_hash] IS NULL THEN N'null' ELSE N'"' + CAST(o.[_hash] AS NVARCHAR(50)) + N'"' END
    FROM [dbo].[_objects] o
    INNER JOIN [dbo].[_schemes] s ON s.[_id] = o.[_id_scheme]
    WHERE o.[_id] = @object_id;
    
    -- Check max_depth
    IF @max_depth <= 0
    BEGIN
        SET @result = @base_json + N'}';
        IF @is_root_call = 1 DROP TABLE #all_values;
        RETURN;
    END
    
    -- Build properties
    EXEC [dbo].[sp_build_hierarchical_properties]
        @object_id,
        NULL,
        @object_scheme_id,
        @max_depth,
        NULL,
        NULL,
        @properties_json OUTPUT;
    
    -- Combine base + properties
    SET @result = @base_json + N',"properties":' + ISNULL(@properties_json, N'{}') + N'}';
    
    -- Cleanup temp table if this was root call
    IF @is_root_call = 1
        DROP TABLE #all_values;
END
GO

-- =====================================================
-- PUBLIC WRAPPER: Get object as JSON with SELECT result
-- More efficient for C# - just ExecuteScalarAsync
-- =====================================================

CREATE PROCEDURE [dbo].[sp_get_object_json]
    @object_id BIGINT,
    @max_depth INT = 10
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @result NVARCHAR(MAX);
    EXEC [dbo].[sp_get_object_json_internal] @object_id, @max_depth, @result OUTPUT;
    SELECT @result AS json_result;
END
GO

-- =====================================================
-- USAGE EXAMPLES
-- =====================================================
-- 
-- Get object as JSON (simple - SELECT result):
--   EXEC get_object_json @object_id = 123;
--   EXEC get_object_json @object_id = 123, @max_depth = 2;
--
-- For recursive calls (internal use with OUTPUT):
--   DECLARE @json NVARCHAR(MAX);
--   EXEC sp_get_object_json_internal @object_id = 123, @result = @json OUTPUT;
-- =====================================================

PRINT '========================================='
PRINT 'JSON object functions created!'
PRINT '========================================='
GO



