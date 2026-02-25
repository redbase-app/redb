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
