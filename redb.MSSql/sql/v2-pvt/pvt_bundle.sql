-- ==========================================================
-- REDB: Combined schema initialization script (auto-generated)
-- DO NOT EDIT — this file is overwritten on every build.
-- ==========================================================

-- ===== 00_module_init.sql =====
-- =====================================================================
-- v2-pvt module init (MSSql)
-- =====================================================================
-- Purpose: PVT-based search engine for REDB free (SQL Server).
-- Owner  : redb core team. Mirrors redb.Postgres/sql/v2-pvt/.
-- Version: see dbo.pvt_module_version() at the bottom of this file.
--
-- This file must be applied FIRST. It performs three things:
--   1. Verifies that system infrastructure of REDB is in place
--      (core tables and dbo.get_object_json).
--   2. Drops every function this module owns so the module can be
--      redeployed cleanly.
--   3. Creates dbo.pvt_module_version() -- used by the C# client to
--      verify compatibility on InitializeAsync(). No runtime fallback.
-- =====================================================================
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

-- ---------- 1. System infrastructure check ------------------------------
IF OBJECT_ID(N'dbo.get_object_json', N'FN') IS NULL
   AND OBJECT_ID(N'dbo.get_object_json', N'IF') IS NULL
   AND OBJECT_ID(N'dbo.get_object_json', N'TF') IS NULL
BEGIN
    THROW 50000,
        N'v2-pvt: required system function dbo.get_object_json is missing. Deploy the REDB core schema first (redbMSSQL.sql / generated redb_init.sql).',
        1;
END;

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
    -- 0.1.0 - skeleton: module bootstrap, drop-all, version function.
    --         Builder functions (pvt_build_query_sql etc.) not implemented yet.
    RETURN N'0.1.2';
END;
GO

-- ---------- 4. Smoke -----------------------------------------------------
DECLARE @v nvarchar(50) = dbo.pvt_module_version();
PRINT N'v2-pvt module init OK, version: ' + @v;
GO


-- ===== 01_pvt_field_path.sql =====
-- =====================================================================
-- 01_pvt_field_path.sql (MSSql)
-- ---------------------------------------------------------------------
-- Field-path normalization & parsing primitives. Direct port of
-- redb.Postgres/sql/v2-pvt/01_pvt_field_path.sql to T-SQL.
--
-- All functions are scalar UDFs returning NVARCHAR(MAX); structured
-- results (table-style in PG) are encoded as JSON strings and consumed
-- via OPENJSON/JSON_VALUE on the caller side. Architecture per
-- docs/MsSqlPvtQuery/PLAN.md §2.
--
-- Functions:
--   dbo.pvt_normalize_base_field_name(@field NVARCHAR(200))
--       -> NVARCHAR(64) | NULL
--   dbo.pvt_parse_field_path(@field_path NVARCHAR(400))
--       -> NVARCHAR(MAX) JSON with { root_field, nested_field, is_array,
--                                    is_nested, dict_key }
--   dbo.pvt_normalize_field_name(@path NVARCHAR(400), @op_value NVARCHAR(400))
--       -> NVARCHAR(400)
--   dbo.pvt_peek_contains_key_value(@op NVARCHAR(MAX))
--       -> NVARCHAR(400) | NULL
-- =====================================================================
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

-- ---------- pvt_json_string_or_null ---------------------------------------
-- Tiny helper used by builders to serialize a NVARCHAR value as a JSON
-- string with proper escaping, or the literal `null` if NULL. Required
-- because T-SQL has no `quote_literal`-equivalent for JSON text.
-- Defined first because subsequent builders reference it.
CREATE FUNCTION dbo.pvt_json_string_or_null(@v NVARCHAR(MAX))
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @v IS NULL
        RETURN N'null';
    -- STRING_ESCAPE(.., 'json') is SQL Server 2016+.
    RETURN N'"' + STRING_ESCAPE(@v, N'json') + N'"';
END;
GO

-- ---------- pvt_normalize_base_field_name --------------------------------
-- Maps C# base field names to _objects columns. Returns NULL if the field
-- is not a base column. Bare names (without `0$:` prefix and without `_`
-- prefix) are NOT resolved to base columns to avoid colliding with
-- legitimate user Props named "Name", "Id", etc.
CREATE FUNCTION dbo.pvt_normalize_base_field_name(@field NVARCHAR(200))
RETURNS NVARCHAR(64)
AS
BEGIN
    DECLARE @had_prefix bit = 0;
    DECLARE @input NVARCHAR(200) = @field;
    DECLARE @result NVARCHAR(64) = NULL;

    IF @input LIKE N'0$:%'
    BEGIN
        SET @had_prefix = 1;
        SET @input = SUBSTRING(@input, 4, 200);
    END;

    SET @result = CASE @input
        WHEN N'id' THEN N'_id'
        WHEN N'Id' THEN N'_id'
        WHEN N'_id' THEN N'_id'
        WHEN N'parent_id' THEN N'_id_parent'
        WHEN N'ParentId' THEN N'_id_parent'
        WHEN N'id_parent' THEN N'_id_parent'
        WHEN N'_id_parent' THEN N'_id_parent'
        WHEN N'scheme_id' THEN N'_id_scheme'
        WHEN N'SchemeId' THEN N'_id_scheme'
        WHEN N'id_scheme' THEN N'_id_scheme'
        WHEN N'_id_scheme' THEN N'_id_scheme'
        WHEN N'owner_id' THEN N'_id_owner'
        WHEN N'OwnerId' THEN N'_id_owner'
        WHEN N'_id_owner' THEN N'_id_owner'
        WHEN N'who_change_id' THEN N'_id_who_change'
        WHEN N'WhoChangeId' THEN N'_id_who_change'
        WHEN N'_id_who_change' THEN N'_id_who_change'
        WHEN N'value_long' THEN N'_value_long'
        WHEN N'ValueLong' THEN N'_value_long'
        WHEN N'_value_long' THEN N'_value_long'
        WHEN N'value_string' THEN N'_value_string'
        WHEN N'ValueString' THEN N'_value_string'
        WHEN N'_value_string' THEN N'_value_string'
        WHEN N'value_guid' THEN N'_value_guid'
        WHEN N'ValueGuid' THEN N'_value_guid'
        WHEN N'_value_guid' THEN N'_value_guid'
        WHEN N'key' THEN N'_key'
        WHEN N'Key' THEN N'_key'
        WHEN N'_key' THEN N'_key'
        WHEN N'name' THEN N'_name'
        WHEN N'Name' THEN N'_name'
        WHEN N'_name' THEN N'_name'
        WHEN N'note' THEN N'_note'
        WHEN N'Note' THEN N'_note'
        WHEN N'_note' THEN N'_note'
        WHEN N'value_bool' THEN N'_value_bool'
        WHEN N'ValueBool' THEN N'_value_bool'
        WHEN N'_value_bool' THEN N'_value_bool'
        WHEN N'value_double' THEN N'_value_double'
        WHEN N'ValueDouble' THEN N'_value_double'
        WHEN N'_value_double' THEN N'_value_double'
        WHEN N'value_numeric' THEN N'_value_numeric'
        WHEN N'ValueNumeric' THEN N'_value_numeric'
        WHEN N'_value_numeric' THEN N'_value_numeric'
        WHEN N'value_datetime' THEN N'_value_datetime'
        WHEN N'ValueDatetime' THEN N'_value_datetime'
        WHEN N'_value_datetime' THEN N'_value_datetime'
        WHEN N'value_bytes' THEN N'_value_bytes'
        WHEN N'ValueBytes' THEN N'_value_bytes'
        WHEN N'_value_bytes' THEN N'_value_bytes'
        WHEN N'hash' THEN N'_hash'
        WHEN N'Hash' THEN N'_hash'
        WHEN N'_hash' THEN N'_hash'
        WHEN N'date_create' THEN N'_date_create'
        WHEN N'DateCreate' THEN N'_date_create'
        WHEN N'_date_create' THEN N'_date_create'
        WHEN N'date_modify' THEN N'_date_modify'
        WHEN N'DateModify' THEN N'_date_modify'
        WHEN N'_date_modify' THEN N'_date_modify'
        WHEN N'date_begin' THEN N'_date_begin'
        WHEN N'DateBegin' THEN N'_date_begin'
        WHEN N'_date_begin' THEN N'_date_begin'
        WHEN N'date_complete' THEN N'_date_complete'
        WHEN N'DateComplete' THEN N'_date_complete'
        WHEN N'_date_complete' THEN N'_date_complete'
        ELSE NULL
    END;

    -- Disambiguation: bare names like `Name`, `Id`, `Key` collide with
    -- legitimate user-defined Props fields. Only honor the mapping if
    -- caller explicitly opted in via `0$:` or the input already starts
    -- with an underscore (system column convention).
    IF @result IS NOT NULL
       AND @had_prefix = 0
       AND LEFT(@input, 1) <> N'_'
        RETURN NULL;

    RETURN @result;
END;
GO

-- ---------- pvt_parse_field_path -----------------------------------------
-- Parses field path into JSON components. Replaces PG `RETURNS TABLE`
-- with `RETURNS NVARCHAR(MAX)` JSON. Output keys mirror PG names so the
-- caller can JSON_VALUE($.root_field) etc.
--
-- Examples:
--   'Name'                -> {"root_field":"Name","nested_field":null,
--                             "is_array":false,"is_nested":false,
--                             "dict_key":null}
--   'Contact.Name'        -> {"root_field":"Contact","nested_field":"Name",
--                             "is_array":false,"is_nested":true,
--                             "dict_key":null}
--   'Tags[]'              -> {"root_field":"Tags","nested_field":null,
--                             "is_array":true,"is_nested":false,
--                             "dict_key":null}
--   'Contacts[].Email'    -> {"root_field":"Contacts",
--                             "nested_field":"Email","is_array":true,
--                             "is_nested":true,"dict_key":null}
--   'PhoneBook[home]'     -> {"root_field":"PhoneBook","nested_field":null,
--                             "is_array":false,"is_nested":false,
--                             "dict_key":"home"}
--   'AddressBook[h].City' -> {"root_field":"AddressBook",
--                             "nested_field":"City","is_array":false,
--                             "is_nested":true,"dict_key":"h"}
CREATE FUNCTION dbo.pvt_parse_field_path(@field_path NVARCHAR(400))
RETURNS NVARCHAR(MAX)
AS
BEGIN
    DECLARE @root_field   NVARCHAR(200) = NULL;
    DECLARE @nested_field NVARCHAR(200) = NULL;
    DECLARE @is_array     bit = 0;
    DECLARE @is_nested    bit = 0;
    DECLARE @dict_key     NVARCHAR(200) = NULL;

    DECLARE @bracket_pos int;
    DECLARE @key_end     int;
    DECLARE @dot_after   int;
    DECLARE @bracketed   NVARCHAR(200);

    -- Detect plain array `[]` (no key inside)
    IF CHARINDEX(N'[]', @field_path) > 0
        SET @is_array = 1;

    -- Detect Dictionary path: `Name[key]` or `Name[key].Child` (non-empty key).
    -- Plain `[]` array marker already handled above via @is_array.
    IF @is_array = 0 AND CHARINDEX(N'[', @field_path) > 0
    BEGIN
        SET @bracket_pos = CHARINDEX(N'[', @field_path);
        SET @key_end     = CHARINDEX(N']', @field_path);
        IF @bracket_pos > 0 AND @key_end > @bracket_pos + 1
        BEGIN
            SET @root_field = SUBSTRING(@field_path, 1, @bracket_pos - 1);
            SET @dict_key   = SUBSTRING(@field_path, @bracket_pos + 1, @key_end - @bracket_pos - 1);

            -- Look for `.<child>` after `]`
            DECLARE @after_bracket NVARCHAR(400) = SUBSTRING(@field_path, @key_end + 1, 400);
            IF LEN(@after_bracket) > 0 AND LEFT(@after_bracket, 1) = N'.'
            BEGIN
                SET @nested_field = SUBSTRING(@after_bracket, 2, 400);
                SET @is_nested = 1;
            END;

            RETURN N'{"root_field":'   + dbo.pvt_json_string_or_null(@root_field)
                + N',"nested_field":' + dbo.pvt_json_string_or_null(@nested_field)
                + N',"is_array":false,"is_nested":'
                + CASE WHEN @is_nested = 1 THEN N'true' ELSE N'false' END
                + N',"dict_key":' + dbo.pvt_json_string_or_null(@dict_key)
                + N'}';
        END;
    END;

    -- Nested? (contains `.`)
    IF CHARINDEX(N'.', @field_path) > 0
        SET @is_nested = 1;

    IF @is_nested = 1
    BEGIN
        DECLARE @stripped NVARCHAR(400) = REPLACE(@field_path, N'[]', N'');
        DECLARE @dot int = CHARINDEX(N'.', @stripped);
        SET @root_field   = SUBSTRING(@stripped, 1, @dot - 1);
        SET @nested_field = SUBSTRING(@stripped, @dot + 1, 400);
    END
    ELSE
    BEGIN
        IF @is_array = 1
            SET @root_field = REPLACE(@field_path, N'[]', N'');
        ELSE
            SET @root_field = @field_path;
    END;

    RETURN N'{"root_field":'   + dbo.pvt_json_string_or_null(@root_field)
        + N',"nested_field":' + dbo.pvt_json_string_or_null(@nested_field)
        + N',"is_array":'  + CASE WHEN @is_array  = 1 THEN N'true' ELSE N'false' END
        + N',"is_nested":' + CASE WHEN @is_nested = 1 THEN N'true' ELSE N'false' END
        + N',"dict_key":' + dbo.pvt_json_string_or_null(@dict_key)
        + N'}';
END;
GO

-- ---------- pvt_normalize_field_name --------------------------------------
-- If `<Dict>.ContainsKey` is the field and an operand string is provided,
-- rewrites to `<Dict>[<key>]` so dict-pivot path resolution kicks in.
-- Mirrors Pro.ProSqlBuilderBase.NormalizeDictionaryFieldName.
CREATE FUNCTION dbo.pvt_normalize_field_name(
    @path     NVARCHAR(400),
    @op_value NVARCHAR(400)
)
RETURNS NVARCHAR(400)
AS
BEGIN
    IF @path IS NULL
        RETURN NULL;
    IF RIGHT(@path, 12) <> N'.ContainsKey'
        RETURN @path;
    IF @op_value IS NULL OR @op_value = N''
        RETURN @path;

    DECLARE @base NVARCHAR(400) = LEFT(@path, LEN(@path) - 12);
    RETURN @base + N'[' + @op_value + N']';
END;
GO

-- ---------- pvt_peek_contains_key_value -----------------------------------
-- The operand of `Dict.ContainsKey(...)` can arrive either as a bare JSON
-- string (shorthand $eq) or as an object `{ "$eq": "<key>" }`. Returns
-- the underlying text, or NULL if the operand is not a string.
CREATE FUNCTION dbo.pvt_peek_contains_key_value(@op NVARCHAR(MAX))
RETURNS NVARCHAR(400)
AS
BEGIN
    IF @op IS NULL
        RETURN NULL;

    -- Object operand: { "$eq": "<key>" }
    IF ISJSON(@op) = 1
    BEGIN
        DECLARE @eq NVARCHAR(400) = JSON_VALUE(@op, N'$."$eq"');
        IF @eq IS NOT NULL
            RETURN @eq;
    END;

    -- Bare JSON string: `"<key>"`. JSON_VALUE on root needs a wrapper:
    -- wrap in single-element array and read [0]. Works for both `"foo"`
    -- and bare scalars; for non-string scalars JSON_VALUE returns the
    -- text form which is acceptable for the caller (it only treats
    -- non-null strings as dict keys).
    DECLARE @arr NVARCHAR(MAX) = N'[' + @op + N']';
    IF ISJSON(@arr) = 1
        RETURN JSON_VALUE(@arr, N'$[0]');

    RETURN NULL;
END;
GO


-- ===== 02_pvt_type_info.sql =====
-- =====================================================================
-- 02_pvt_type_info.sql (MSSql)
-- ---------------------------------------------------------------------
-- ListItem type-info resolver. Mirrors PG v2-pvt/02_pvt_type_info.sql.
-- Returns NVARCHAR(MAX) JSON with { db_type, type_semantic, is_array }
-- or NULL if the ListItem accessor is not recognized.
-- =====================================================================
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

CREATE FUNCTION dbo.pvt_get_listitem_field_type_info(@field_name NVARCHAR(100))
RETURNS NVARCHAR(MAX)
AS
BEGIN
    RETURN CASE @field_name
        WHEN N'Value'    THEN N'{"db_type":"String","type_semantic":"String","is_array":false}'
        WHEN N'Alias'    THEN N'{"db_type":"String","type_semantic":"String","is_array":false}'
        WHEN N'IdObject' THEN N'{"db_type":"Long","type_semantic":"Long","is_array":false}'
        WHEN N'IdList'   THEN N'{"db_type":"Long","type_semantic":"Long","is_array":false}'
        WHEN N'Id'       THEN N'{"db_type":"Long","type_semantic":"Long","is_array":false}'
        ELSE NULL
    END;
END;
GO


-- ===== 03_pvt_structure_info.sql =====
-- =====================================================================
-- 03_pvt_structure_info.sql (MSSql)
-- ---------------------------------------------------------------------
-- Lookup structure metadata by field path. Mirrors PG
-- v2-pvt/03_pvt_structure_info.sql.
--
-- PG returns RETURNS TABLE; here we return NVARCHAR(MAX) JSON with:
--   { root_structure_id, nested_structure_id, root_type_info,
--     nested_type_info }
-- root_structure_id / nested_structure_id may be null (JSON nulls).
-- type_info objects share the shape from pvt_get_listitem_field_type_info:
--   { type_name, db_type, type_semantic, is_array }
--
-- Reads from _scheme_metadata_cache directly (faster than parsing the
-- full get_scheme_definition JSON). MSSql free reuses the same cache
-- table as Pro.
-- =====================================================================
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

CREATE FUNCTION dbo.pvt_find_structure_info(
    @scheme_id    bigint,
    @root_field   NVARCHAR(200),
    @nested_field NVARCHAR(200)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    DECLARE @root_sid       bigint = NULL;
    DECLARE @root_type_name NVARCHAR(200) = NULL;
    DECLARE @root_db_type   NVARCHAR(64)  = NULL;
    DECLARE @root_dotnet    NVARCHAR(200) = NULL;
    DECLARE @root_is_arr    bit = 0;

    SELECT TOP 1
        @root_sid       = c._structure_id,
        @root_db_type   = c.db_type,
        @root_is_arr    = CASE WHEN c._collection_type IS NOT NULL THEN 1 ELSE 0 END,
        @root_type_name = c.type_name,
        @root_dotnet    = c.type_semantic
    FROM dbo._scheme_metadata_cache c
    WHERE c._scheme_id = @scheme_id
      AND c._name = @root_field
      AND c._parent_structure_id IS NULL;

    DECLARE @nested_sid       bigint = NULL;
    DECLARE @nested_type_name NVARCHAR(200) = NULL;
    DECLARE @nested_db_type   NVARCHAR(64)  = NULL;
    DECLARE @nested_dotnet    NVARCHAR(200) = NULL;
    DECLARE @nested_is_arr    bit = 0;
    DECLARE @nested_json      NVARCHAR(MAX) = NULL;

    IF @nested_field IS NOT NULL AND @root_sid IS NOT NULL
    BEGIN
        -- Special case: ListItem nested accessors (Value/Alias/Id...)
        -- are stored as columns of _list_items, not as child structures.
        IF @root_dotnet = N'_RListItem'
        BEGIN
            SET @nested_json = dbo.pvt_get_listitem_field_type_info(@nested_field);
        END
        ELSE
        BEGIN
            SELECT TOP 1
                @nested_sid       = c._structure_id,
                @nested_db_type   = c.db_type,
                @nested_is_arr    = CASE WHEN c._collection_type IS NOT NULL THEN 1 ELSE 0 END,
                @nested_type_name = c.type_name,
                @nested_dotnet    = c.type_semantic
            FROM dbo._scheme_metadata_cache c
            WHERE c._scheme_id = @scheme_id
              AND c._name = @nested_field
              AND c._parent_structure_id = @root_sid;
        END;
    END;

    -- Build root_type_info JSON or `null` if root not found.
    DECLARE @root_ti NVARCHAR(MAX) = CASE
        WHEN @root_sid IS NULL THEN N'null'
        ELSE N'{"type_name":'  + dbo.pvt_json_string_or_null(@root_type_name)
           + N',"db_type":'    + dbo.pvt_json_string_or_null(@root_db_type)
           + N',"type_semantic":' + dbo.pvt_json_string_or_null(@root_dotnet)
           + N',"is_array":'   + CASE WHEN @root_is_arr = 1 THEN N'true' ELSE N'false' END
           + N'}'
    END;

    DECLARE @nested_ti NVARCHAR(MAX);
    IF @nested_json IS NOT NULL
        SET @nested_ti = @nested_json;
    ELSE IF @nested_sid IS NULL
        SET @nested_ti = N'null';
    ELSE
        SET @nested_ti = N'{"type_name":'  + dbo.pvt_json_string_or_null(@nested_type_name)
                       + N',"db_type":'    + dbo.pvt_json_string_or_null(@nested_db_type)
                       + N',"type_semantic":' + dbo.pvt_json_string_or_null(@nested_dotnet)
                       + N',"is_array":'   + CASE WHEN @nested_is_arr = 1 THEN N'true' ELSE N'false' END
                       + N'}';

    RETURN N'{"root_structure_id":'  + ISNULL(CAST(@root_sid   AS NVARCHAR(32)), N'null')
        + N',"nested_structure_id":' + ISNULL(CAST(@nested_sid AS NVARCHAR(32)), N'null')
        + N',"root_type_info":'      + @root_ti
        + N',"nested_type_info":'    + @nested_ti
        + N'}';
END;
GO


-- ===== 04_pvt_tree_helpers.sql =====
-- =====================================================================
-- 04_pvt_tree_helpers.sql  (MSSql v2-pvt) — iterative tree traversal
-- ---------------------------------------------------------------------
-- Functions:
--   dbo.pvt_object_depth(@id)
--       Returns the depth of an object in the tree (root = 0).
--       Uses an iterative WHILE loop (max 200 levels) because scalar
--       UDFs in SQL Server cannot contain recursive CTEs.
--
--   dbo.pvt_is_descendant_of(@id, @ancestor_id)
--       Returns 1 if @id is a direct or indirect descendant of
--       @ancestor_id, 0 otherwise. Also iterative.
--
-- Used by pvt_build_where_from_json to emit $level, $hasAncestor, and
-- $hasDescendant predicates as correlated scalar UDF calls.
-- =====================================================================

SET QUOTED_IDENTIFIER ON;
GO

-- ---------- pvt_object_depth -----------------------------------------
CREATE OR ALTER FUNCTION dbo.pvt_object_depth(@id BIGINT)
RETURNS INT
AS
BEGIN
    DECLARE @depth  INT    = 0;
    DECLARE @cur_id BIGINT = @id;
    DECLARE @par_id BIGINT;
    DECLARE @iter   INT    = 200;

    WHILE @iter > 0
    BEGIN
        SELECT @par_id = [_id_parent]
        FROM   dbo._objects
        WHERE  [_id] = @cur_id;

        IF @par_id IS NULL
            RETURN @depth;

        SET @depth  += 1;
        SET @cur_id  = @par_id;
        SET @iter   -= 1;
    END;

    RETURN @depth;
END;
GO

-- ---------- pvt_is_descendant_of -------------------------------------
-- Returns 1 when @id is a (direct or indirect) descendant of @ancestor_id.
CREATE OR ALTER FUNCTION dbo.pvt_is_descendant_of(
    @id          BIGINT,
    @ancestor_id BIGINT
)
RETURNS BIT
AS
BEGIN
    IF @id IS NULL OR @ancestor_id IS NULL
        RETURN 0;

    DECLARE @cur_id BIGINT = @id;
    DECLARE @par_id BIGINT;
    DECLARE @iter   INT    = 200;

    WHILE @iter > 0
    BEGIN
        SELECT @par_id = [_id_parent]
        FROM   dbo._objects
        WHERE  [_id] = @cur_id;

        IF @par_id IS NULL
            RETURN 0;
        IF @par_id = @ancestor_id
            RETURN 1;

        SET @cur_id = @par_id;
        SET @iter  -= 1;
    END;

    RETURN 0;
END;
GO


-- ===== 05_pvt_single_facet.sql =====
-- =====================================================================
-- 05_pvt_single_facet.sql  (MSSql v2-pvt)
-- ---------------------------------------------------------------------
-- Legacy facet condition builder — MSSql thin wrapper.
-- In MSSql v2-pvt, pvt_build_where_from_json (14_pvt_where.sql) handles
-- all filter cases inline, including property functions, hierarchical
-- operators, and 0$: base-field prefixes. This function delegates to
-- that engine via pvt_collect_fields + pvt_build_where_from_json.
--
-- Functions:
--   dbo.pvt_build_single_facet_condition(
--       @facet_condition NVARCHAR(MAX),
--       @scheme_id       BIGINT,
--       @table_alias     NVARCHAR(50),   -- default 'o'
--       @max_depth       INT             -- ignored (not used in T-SQL engine)
--   ) -> NVARCHAR(MAX)
--
-- Returns the AND-joined SQL predicate string (no leading ' AND ').
-- Returns '1=1' for empty / null input.
--
-- Depends on:
--   dbo.pvt_collect_fields     (10_pvt_field_collection.sql)
--   dbo.pvt_build_where_from_json (14_pvt_where.sql)
-- =====================================================================

SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER FUNCTION dbo.pvt_build_single_facet_condition(
    @facet_condition NVARCHAR(MAX),
    @scheme_id       BIGINT,
    @table_alias     NVARCHAR(50),
    @max_depth       INT
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @facet_condition IS NULL OR ISJSON(@facet_condition) = 0
        RETURN N'1=1';

    DECLARE @prefix NVARCHAR(60) = ISNULL(@table_alias, N'o') + N'.';
    DECLARE @fields NVARCHAR(MAX) = dbo.pvt_collect_fields(@scheme_id, @facet_condition, NULL);
    RETURN dbo.pvt_build_where_from_json(@facet_condition, @fields, @prefix);
END;
GO


-- ===== 06a_pvt_legacy_helpers.sql =====
-- =====================================================================
-- 06a_pvt_legacy_helpers.sql  (MSSql v2-pvt)
-- ---------------------------------------------------------------------
-- Level-condition helpers required by pvt_build_hierarchical_conditions.
-- Mirrors pvt_build_level_condition / pvt_build_level_condition_with_operators
-- from PG v2-pvt/06a_pvt_legacy_helpers.sql.
--
-- Functions:
--   dbo.pvt_build_level_condition(@target_level INT, @table_alias NVARCHAR(50))
--       -> NVARCHAR(MAX)  SQL fragment: ' AND dbo.pvt_object_depth(alias.[_id]) = N'
--   dbo.pvt_build_level_condition_with_operators(@level_operators NVARCHAR(MAX),
--                                                @table_alias     NVARCHAR(50))
--       -> NVARCHAR(MAX)  SQL fragment: ' AND (depth op N [AND ...])'
--
-- Depends on: dbo.pvt_object_depth (04_pvt_tree_helpers.sql)
-- =====================================================================

SET QUOTED_IDENTIFIER ON;
GO

-- ---------- pvt_build_level_condition ---------------------------------
-- Returns a T-SQL predicate fragment for exact tree-depth equality.
-- Level 0 = root (no parent). Level 1 = direct child of root, etc.
CREATE OR ALTER FUNCTION dbo.pvt_build_level_condition(
    @target_level INT,
    @table_alias  NVARCHAR(50)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    DECLARE @alias NVARCHAR(60) = ISNULL(@table_alias, N'o');
    RETURN N' AND dbo.pvt_object_depth(' + @alias + N'.[_id]) = '
         + CAST(@target_level AS NVARCHAR(10));
END;
GO

-- ---------- pvt_build_level_condition_with_operators ------------------
-- Parses a JSON object of comparison operators ({"$gt":2,"$lt":5})
-- and builds an AND-joined depth predicate fragment.
-- Returns empty string when no recognized operators are found.
CREATE OR ALTER FUNCTION dbo.pvt_build_level_condition_with_operators(
    @level_operators NVARCHAR(MAX),
    @table_alias     NVARCHAR(50)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @level_operators IS NULL OR ISJSON(@level_operators) = 0
        RETURN N'';

    DECLARE @alias NVARCHAR(60) = ISNULL(@table_alias, N'o');
    DECLARE @depth_expr NVARCHAR(200) = N'dbo.pvt_object_depth(' + @alias + N'.[_id])';
    DECLARE @parts NVARCHAR(MAX) = N'';

    DECLARE c CURSOR LOCAL FAST_FORWARD FOR
        SELECT [key], [value] FROM OPENJSON(@level_operators);
    DECLARE @opk NVARCHAR(20), @opv NVARCHAR(50);
    OPEN c;
    FETCH NEXT FROM c INTO @opk, @opv;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        DECLARE @sym NVARCHAR(4) = CASE @opk
            WHEN N'$gt'  THEN N'>'
            WHEN N'$gte' THEN N'>='
            WHEN N'$lt'  THEN N'<'
            WHEN N'$lte' THEN N'<='
            WHEN N'$eq'  THEN N'='
            WHEN N'$ne'  THEN N'<>'
            ELSE NULL
        END;
        IF @sym IS NOT NULL AND TRY_CAST(@opv AS INT) IS NOT NULL
        BEGIN
            IF @parts <> N'' SET @parts += N' AND ';
            SET @parts += @depth_expr + N' ' + @sym + N' ' + CAST(CAST(@opv AS INT) AS NVARCHAR(10));
        END;
        FETCH NEXT FROM c INTO @opk, @opv;
    END;
    CLOSE c; DEALLOCATE c;

    IF @parts = N'' RETURN N'';
    RETURN N' AND (' + @parts + N')';
END;
GO


-- ===== 06_pvt_hierarchical.sql =====
-- =====================================================================
-- 06_pvt_hierarchical.sql  (MSSql v2-pvt)
-- ---------------------------------------------------------------------
-- Tree predicate builder for $hasAncestor, $hasDescendant, $level,
-- $isRoot, $isLeaf, $childrenOf. Mirrors PG pvt_build_hierarchical_conditions.
--
-- Note: The core WHERE builder (14_pvt_where.sql) handles all these
-- operators inline. This function is a focused variant that extracts
-- only the hierarchical keys from a filter JSON and builds a WHERE
-- fragment — useful when callers want to separate tree conditions from
-- field conditions.
--
-- Functions:
--   dbo.pvt_build_hierarchical_conditions(@facet_filters NVARCHAR(MAX),
--                                          @table_alias  NVARCHAR(50))
--       -> NVARCHAR(MAX)  SQL fragment, leading ' AND ...' or ''
--
-- Depends on:
--   dbo.pvt_object_depth          (04_pvt_tree_helpers.sql)
--   dbo.pvt_is_descendant_of      (04_pvt_tree_helpers.sql)
--   dbo.pvt_build_level_condition (06a_pvt_legacy_helpers.sql)
--   dbo.pvt_build_level_condition_with_operators (06a)
-- =====================================================================

SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER FUNCTION dbo.pvt_build_hierarchical_conditions(
    @facet_filters NVARCHAR(MAX),
    @table_alias   NVARCHAR(50)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @facet_filters IS NULL OR ISJSON(@facet_filters) = 0
        RETURN N'';

    DECLARE @alias     NVARCHAR(60) = ISNULL(@table_alias, N'o');
    DECLARE @result    NVARCHAR(MAX) = N'';

    -- $isRoot
    DECLARE @is_root_raw NVARCHAR(10) = JSON_VALUE(@facet_filters, N'$."$isRoot"');
    IF @is_root_raw IS NOT NULL
    BEGIN
        IF LOWER(@is_root_raw) = N'true'
            SET @result += N' AND ' + @alias + N'.[_id_parent] IS NULL';
        ELSE
            SET @result += N' AND ' + @alias + N'.[_id_parent] IS NOT NULL';
    END;

    -- $isLeaf
    DECLARE @is_leaf_raw NVARCHAR(10) = JSON_VALUE(@facet_filters, N'$."$isLeaf"');
    IF @is_leaf_raw IS NOT NULL
    BEGIN
        IF LOWER(@is_leaf_raw) = N'true'
            SET @result += N' AND NOT EXISTS (SELECT 1 FROM dbo._objects _ch WHERE _ch.[_id_parent] = ' + @alias + N'.[_id])';
        ELSE
            SET @result += N' AND EXISTS (SELECT 1 FROM dbo._objects _ch WHERE _ch.[_id_parent] = ' + @alias + N'.[_id])';
    END;

    -- $level: integer (exact) or object (operators)
    DECLARE @level_raw NVARCHAR(MAX) = JSON_QUERY(@facet_filters, N'$."$level"');
    IF @level_raw IS NOT NULL
    BEGIN
        IF ISJSON(@level_raw) = 1
            -- Operator object: {"$gt":2,"$lt":5}
            SET @result += dbo.pvt_build_level_condition_with_operators(@level_raw, @alias);
        ELSE
        BEGIN
            DECLARE @lvl_val NVARCHAR(20) = JSON_VALUE(@facet_filters, N'$."$level"');
            DECLARE @lvl_int INT = TRY_CAST(@lvl_val AS INT);
            IF @lvl_int IS NOT NULL
                SET @result += dbo.pvt_build_level_condition(@lvl_int, @alias);
        END;
    END;

    -- $hasAncestor: object is a descendant of the given ancestor id
    DECLARE @ha_raw NVARCHAR(40) = JSON_VALUE(@facet_filters, N'$."$hasAncestor"');
    IF @ha_raw IS NOT NULL
    BEGIN
        DECLARE @ha_id BIGINT = TRY_CAST(@ha_raw AS BIGINT);
        IF @ha_id IS NOT NULL
            SET @result += N' AND dbo.pvt_is_descendant_of(' + @alias + N'.[_id], '
                         + CAST(@ha_id AS NVARCHAR(20)) + N') = 1';
    END;

    -- $hasDescendant: given id is a descendant of this object
    DECLARE @hd_raw NVARCHAR(40) = JSON_VALUE(@facet_filters, N'$."$hasDescendant"');
    IF @hd_raw IS NOT NULL
    BEGIN
        DECLARE @hd_id BIGINT = TRY_CAST(@hd_raw AS BIGINT);
        IF @hd_id IS NOT NULL
            SET @result += N' AND dbo.pvt_is_descendant_of('
                         + CAST(@hd_id AS NVARCHAR(20)) + N', ' + @alias + N'.[_id]) = 1';
    END;

    -- $childrenOf: direct children of the given parent id
    DECLARE @co_raw NVARCHAR(40) = JSON_VALUE(@facet_filters, N'$."$childrenOf"');
    IF @co_raw IS NOT NULL
    BEGIN
        DECLARE @co_id BIGINT = TRY_CAST(@co_raw AS BIGINT);
        IF @co_id IS NOT NULL
            SET @result += N' AND ' + @alias + N'.[_id_parent] = ' + CAST(@co_id AS NVARCHAR(20));
    END;

    RETURN @result;
END;
GO


-- ===== 07_pvt_base_fields.sql =====
-- =====================================================================
-- 07_pvt_base_fields.sql  (MSSql v2-pvt)
-- ---------------------------------------------------------------------
-- Return a JSON object containing all base (_objects) fields of a
-- single redb object (no Props). Mirrors PG pvt_get_object_base_fields.
--
-- Functions:
--   dbo.pvt_get_object_base_fields(@object_id BIGINT)
--       -> NVARCHAR(MAX)  JSON object with all base-table columns
--
-- Notes:
--   - Includes [_hash] which is critical for cache validation.
--   - FOR JSON PATH, WITHOUT_ARRAY_WRAPPER collapses the single row.
--   - Returns NULL when no matching object is found.
-- =====================================================================

SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER FUNCTION dbo.pvt_get_object_base_fields(@object_id BIGINT)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    DECLARE @json NVARCHAR(MAX);
    SELECT @json = (
        SELECT
            o.[_id]               AS [id],
            o.[_name]             AS [name],
            o.[_id_scheme]        AS [scheme_id],
            o.[_id_parent]        AS [parent_id],
            o.[_id_owner]         AS [owner_id],
            o.[_id_who_change]    AS [who_change_id],
            o.[_date_create]      AS [date_create],
            o.[_date_modify]      AS [date_modify],
            o.[_date_begin]       AS [date_begin],
            o.[_date_complete]    AS [date_complete],
            o.[_key]              AS [key],
            o.[_value_long]       AS [value_long],
            o.[_value_string]     AS [value_string],
            o.[_value_guid]       AS [value_guid],
            o.[_note]             AS [note],
            o.[_value_bool]       AS [value_bool],
            o.[_value_double]     AS [value_double],
            o.[_value_numeric]    AS [value_numeric],
            o.[_value_datetime]   AS [value_datetime],
            o.[_value_bytes]      AS [value_bytes],
            o.[_hash]             AS [hash]
        FROM dbo._objects o
        WHERE o.[_id] = @object_id
        FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    );
    RETURN @json;
END;
GO


-- ===== 08_pvt_tree_functions.sql =====
-- ======================================================================
-- 08_pvt_tree_functions.sql  (MSSql v2-pvt — Stage 2a)
-- ----------------------------------------------------------------------
-- Multi-statement table-valued functions for the 5 tree-walk modes
-- consumed by pvt_build_cte_sql. PG uses inline recursive CTEs in the
-- generated SQL; T-SQL forbids `WITH` inside a subquery, so we wrap
-- the recursive walk in TVFs and let 12_pvt_cte_builder emit
--
--     o.[_id] IN (SELECT _id FROM dbo.pvt_tree_<mode>(...))
--
-- as an ordinary predicate. This keeps the contract of file 12 (a bare
-- SELECT body) intact and avoids touching the 5 callers.
--
-- Modes:
--   pvt_tree_descendants : recursive walk DOWN from seed ids.
--                          depth=0 at seed; descendants depth>0.
--                          @include_seed=0 strips depth=0 rows.
--                          @max_depth NULL = unbounded.
--                          @polymorphic=0 restricts the recursive
--                          step to o._id_scheme = @scheme_id.
--   pvt_tree_children    : non-recursive — direct children of seed ids.
--   pvt_tree_roots       : non-recursive — scheme objects with
--                          _id_parent IS NULL. Optional seed_ids
--                          restricts to that root subset.
--   pvt_tree_leaves      : non-recursive — scheme objects with no
--                          children (NOT EXISTS child._id_parent = o._id).
--                          Optional seed_ids restricts the leaf subset.
--   pvt_tree_ancestors   : recursive walk UP from seed ids via _id_parent.
--                          Seed depth=1 = direct parent of seed objects.
--                          The seeds themselves are NEVER in the output.
--
-- Seed input: NVARCHAR(MAX) JSON array of bigints, e.g. N'[1,42,77]'.
-- NULL or N'[]' = no seeds (only valid for roots/leaves).
--
-- All functions return TABLE(_id BIGINT PRIMARY KEY, depth INT NULL).
-- ======================================================================
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

-- ---------------------------------------------------------------- DROPS
IF OBJECT_ID('dbo.pvt_tree_descendants', 'TF') IS NOT NULL DROP FUNCTION dbo.pvt_tree_descendants;
GO
IF OBJECT_ID('dbo.pvt_tree_children', 'TF') IS NOT NULL DROP FUNCTION dbo.pvt_tree_children;
GO
IF OBJECT_ID('dbo.pvt_tree_roots', 'TF') IS NOT NULL DROP FUNCTION dbo.pvt_tree_roots;
GO
IF OBJECT_ID('dbo.pvt_tree_leaves', 'TF') IS NOT NULL DROP FUNCTION dbo.pvt_tree_leaves;
GO
IF OBJECT_ID('dbo.pvt_tree_ancestors', 'TF') IS NOT NULL DROP FUNCTION dbo.pvt_tree_ancestors;
GO

-- ====================================================================
-- pvt_tree_descendants
-- ====================================================================
CREATE FUNCTION dbo.pvt_tree_descendants(
    @scheme_id    BIGINT,
    @seed_ids     NVARCHAR(MAX),     -- JSON array of bigints
    @max_depth    INT          = NULL,
    @polymorphic  BIT          = 1,
    @include_seed BIT          = 1
)
RETURNS @T TABLE (_id BIGINT PRIMARY KEY, depth INT NULL)
AS
BEGIN
    IF @seed_ids IS NULL RETURN;

    ;WITH seeds(_id) AS (
        SELECT CAST([value] AS BIGINT) FROM OPENJSON(@seed_ids)
    ),
    walk(_id, depth) AS (
        SELECT s._id, 0 FROM seeds s
        UNION ALL
        SELECT o.[_id], w.depth + 1
        FROM dbo._objects o
        JOIN walk w ON o.[_id_parent] = w._id
        WHERE (@max_depth IS NULL OR w.depth < @max_depth)
          AND (@polymorphic = 1 OR o.[_id_scheme] = @scheme_id)
    )
    INSERT INTO @T(_id, depth)
    SELECT _id, MIN(depth)
    FROM walk
    WHERE @include_seed = 1 OR depth > 0
    GROUP BY _id;

    RETURN;
END;
GO

-- ====================================================================
-- pvt_tree_children
-- ====================================================================
CREATE FUNCTION dbo.pvt_tree_children(
    @scheme_id   BIGINT,
    @seed_ids    NVARCHAR(MAX),
    @polymorphic BIT = 1
)
RETURNS @T TABLE (_id BIGINT PRIMARY KEY, depth INT NULL)
AS
BEGIN
    IF @seed_ids IS NULL RETURN;

    ;WITH seeds(_id) AS (
        SELECT CAST([value] AS BIGINT) FROM OPENJSON(@seed_ids)
    )
    INSERT INTO @T(_id, depth)
    SELECT o.[_id], 1
    FROM dbo._objects o
    JOIN seeds s ON o.[_id_parent] = s._id
    WHERE (@polymorphic = 1 OR o.[_id_scheme] = @scheme_id);

    RETURN;
END;
GO

-- ====================================================================
-- pvt_tree_roots
-- ====================================================================
CREATE FUNCTION dbo.pvt_tree_roots(
    @scheme_id BIGINT,
    @seed_ids  NVARCHAR(MAX)         -- optional; NULL = all roots in scheme
)
RETURNS @T TABLE (_id BIGINT PRIMARY KEY, depth INT NULL)
AS
BEGIN
    IF @seed_ids IS NULL
    BEGIN
        INSERT INTO @T(_id, depth)
        SELECT o.[_id], 0
        FROM dbo._objects o
        WHERE o.[_id_parent] IS NULL
          AND o.[_id_scheme] = @scheme_id;
    END
    ELSE
    BEGIN
        ;WITH seeds(_id) AS (
            SELECT CAST([value] AS BIGINT) FROM OPENJSON(@seed_ids)
        )
        INSERT INTO @T(_id, depth)
        SELECT o.[_id], 0
        FROM dbo._objects o
        JOIN seeds s ON s._id = o.[_id]
        WHERE o.[_id_parent] IS NULL
          AND o.[_id_scheme] = @scheme_id;
    END;

    RETURN;
END;
GO

-- ====================================================================
-- pvt_tree_leaves
-- ====================================================================
CREATE FUNCTION dbo.pvt_tree_leaves(
    @scheme_id BIGINT,
    @seed_ids  NVARCHAR(MAX)         -- optional; NULL = all leaves in scheme
)
RETURNS @T TABLE (_id BIGINT PRIMARY KEY, depth INT NULL)
AS
BEGIN
    IF @seed_ids IS NULL
    BEGIN
        INSERT INTO @T(_id, depth)
        SELECT o.[_id], 0
        FROM dbo._objects o
        WHERE o.[_id_scheme] = @scheme_id
          AND NOT EXISTS (
              SELECT 1 FROM dbo._objects c
              WHERE c.[_id_parent] = o.[_id]
          );
    END
    ELSE
    BEGIN
        ;WITH seeds(_id) AS (
            SELECT CAST([value] AS BIGINT) FROM OPENJSON(@seed_ids)
        )
        INSERT INTO @T(_id, depth)
        SELECT o.[_id], 0
        FROM dbo._objects o
        JOIN seeds s ON s._id = o.[_id]
        WHERE o.[_id_scheme] = @scheme_id
          AND NOT EXISTS (
              SELECT 1 FROM dbo._objects c
              WHERE c.[_id_parent] = o.[_id]
          );
    END;

    RETURN;
END;
GO

-- ====================================================================
-- pvt_tree_ancestors
-- ====================================================================
CREATE FUNCTION dbo.pvt_tree_ancestors(
    @scheme_id   BIGINT,
    @seed_ids    NVARCHAR(MAX),
    @max_depth   INT          = NULL,
    @polymorphic BIT          = 1
)
RETURNS @T TABLE (_id BIGINT PRIMARY KEY, depth INT NULL)
AS
BEGIN
    IF @seed_ids IS NULL RETURN;

    ;WITH seeds(_id) AS (
        SELECT CAST([value] AS BIGINT) FROM OPENJSON(@seed_ids)
    ),
    walk(_id, depth) AS (
        -- Seed: direct parents of the input ids (depth=1).
        SELECT seed.[_id_parent], 1
        FROM dbo._objects seed
        JOIN seeds s ON s._id = seed.[_id]
        WHERE seed.[_id_parent] IS NOT NULL

        UNION ALL

        -- Recursive step: grandparents and so on.
        SELECT o.[_id_parent], w.depth + 1
        FROM dbo._objects o
        JOIN walk w ON o.[_id] = w._id
        WHERE o.[_id_parent] IS NOT NULL
          AND (@max_depth IS NULL OR w.depth < @max_depth)
          AND (@polymorphic = 1 OR o.[_id_scheme] = @scheme_id)
    )
    INSERT INTO @T(_id, depth)
    SELECT _id, MIN(depth) FROM walk GROUP BY _id;

    RETURN;
END;
GO


-- ===== 10_pvt_field_collection.sql =====
-- =====================================================================
-- 10_pvt_field_collection.sql  (MSSql v2-pvt)
-- ---------------------------------------------------------------------
-- Field-name collection from filter/order JSON and metadata resolution
-- via _scheme_metadata_cache. Produces the `fields` JSON consumed by
-- pvt_build_column_expr / pvt_build_cte_sql / pvt_build_*_condition.
--
-- Minimal-port functions:
--   dbo.pvt_resolve_field_path(@scheme_id, @path)            -> NVARCHAR(MAX) JSON FieldInfo
--   dbo.pvt_extract_field_pairs(@filter)                     -> NVARCHAR(MAX) JSON array of {path,op_value}
--   dbo.pvt_collect_fields(@scheme_id, @filter, @order)      -> NVARCHAR(MAX) JSON object keyed by field name
--   dbo.pvt_has_absence_check(@filter)                       -> BIT
--
-- Skipped vs PG (v0.1 slice): $expr field walk, $case/$fts/$cast/$dateAdd
-- branches, length/count modifier registration.
-- =====================================================================

SET QUOTED_IDENTIFIER ON;
GO

-- ---------- pvt_resolve_field_path ------------------------------------
CREATE OR ALTER FUNCTION dbo.pvt_resolve_field_path(
    @scheme_id BIGINT,
    @path      NVARCHAR(400)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @scheme_id IS NULL OR @path IS NULL OR @path = N''
        RETURN NULL;

    -- 0. Base?
    DECLARE @base_col NVARCHAR(100) = dbo.pvt_normalize_base_field_name(@path);
    IF @base_col IS NOT NULL
        RETURN N'{"kind":"base","column":"' + @base_col
             + N'","name":' + dbo.pvt_json_string_or_null(@path)
             + N',"is_array":false,"list_item_prop":null,"dict_key":null,"parent_sid":null,"sid":null,"db_type":null,"db_column":null}';

    DECLARE @sid          BIGINT,
            @db_type      NVARCHAR(50),
            @db_col       NVARCHAR(50),
            @is_array     BIT = 0,
            @list_prop    NVARCHAR(20) = NULL,
            @dict_key     NVARCHAR(200) = NULL,
            @parent_sid   BIGINT = NULL,
            @root_name    NVARCHAR(200),
            @child_name   NVARCHAR(200),
            @rest         NVARCHAR(400);

    -- 1. Dictionary path: Name[key] or Name[key].Child
    DECLARE @lb INT = CHARINDEX(N'[', @path);
    DECLARE @rb INT = CHARINDEX(N']', @path);
    IF @lb > 0 AND @rb > @lb + 1
    BEGIN
        SET @root_name = SUBSTRING(@path, 1, @lb - 1);
        SET @dict_key  = SUBSTRING(@path, @lb + 1, @rb - @lb - 1);
        SET @rest      = SUBSTRING(@path, @rb + 1, 400);

        SELECT TOP 1 @sid = c._structure_id, @db_type = c.db_type
          FROM dbo._scheme_metadata_cache c
         WHERE c._scheme_id = @scheme_id AND c._name = @root_name AND c._parent_structure_id IS NULL;
        IF @sid IS NULL
            RETURN NULL;

        IF @rest LIKE N'.%'
        BEGIN
            SET @parent_sid = @sid;
            SET @child_name = SUBSTRING(@rest, 2, 400);
            SELECT TOP 1 @sid = c._structure_id, @db_type = c.db_type,
                         @is_array = CASE WHEN c._collection_type IS NOT NULL THEN 1 ELSE 0 END
              FROM dbo._scheme_metadata_cache c
             WHERE c._scheme_id = @scheme_id AND c._name = @child_name AND c._parent_structure_id = @parent_sid;
            IF @sid IS NULL
                RETURN NULL;
        END;

        SET @db_col = dbo.pvt_db_type_to_value_column(@db_type);
        RETURN N'{"kind":"field","sid":' + CAST(@sid AS NVARCHAR(40))
             + N',"db_type":' + dbo.pvt_json_string_or_null(@db_type)
             + N',"db_column":' + dbo.pvt_json_string_or_null(@db_col)
             + N',"name":' + dbo.pvt_json_string_or_null(@path)
             + N',"is_array":' + CASE WHEN @is_array = 1 THEN N'true' ELSE N'false' END
             + N',"list_item_prop":null'
             + N',"dict_key":' + dbo.pvt_json_string_or_null(@dict_key)
             + N',"parent_sid":' + CASE WHEN @parent_sid IS NULL THEN N'null' ELSE CAST(@parent_sid AS NVARCHAR(40)) END
             + N'}';
    END;

    -- 2. Dotted nested
    IF CHARINDEX(N'.', @path) > 0
    BEGIN
        DECLARE @last NVARCHAR(200), @first NVARCHAR(200);
        SET @last  = REVERSE(LEFT(REVERSE(@path), CHARINDEX(N'.', REVERSE(@path)) - 1));
        SET @first = LEFT(@path, CHARINDEX(N'.', @path) - 1);

        -- 2a. Foo[].Value|Alias|Id  (ListItem array accessors)
        IF @last IN (N'Id', N'Value', N'Alias') AND RIGHT(@first, 2) = N'[]'
           AND CHARINDEX(N'.', @path) = LEN(@first) + 1
        BEGIN
            DECLARE @root_li NVARCHAR(200) = LEFT(@first, LEN(@first) - 2);
            SELECT TOP 1 @sid = c._structure_id, @db_type = c.db_type,
                         @is_array = CASE WHEN c._collection_type IS NOT NULL THEN 1 ELSE 0 END
              FROM dbo._scheme_metadata_cache c
             WHERE c._scheme_id = @scheme_id AND c._name = @root_li AND c._parent_structure_id IS NULL;
            IF @sid IS NOT NULL AND @db_type = N'ListItem' AND @is_array = 1
            BEGIN
                SET @list_prop = @last;
                RETURN N'{"kind":"field","sid":' + CAST(@sid AS NVARCHAR(40))
                     + N',"db_type":"ListItem","db_column":"_ListItem"'
                     + N',"name":' + dbo.pvt_json_string_or_null(@path)
                     + N',"is_array":true'
                     + N',"list_item_prop":' + dbo.pvt_json_string_or_null(@list_prop)
                     + N',"dict_key":null,"parent_sid":null}';
            END;
        END;

        -- 2b. Status.Value|Alias|Id  (scalar ListItem accessors)
        IF @last IN (N'Id', N'Value', N'Alias') AND CHARINDEX(N'.', @path) = LEN(@first) + 1
        BEGIN
            SELECT TOP 1 @sid = c._structure_id, @db_type = c.db_type,
                         @is_array = CASE WHEN c._collection_type IS NOT NULL THEN 1 ELSE 0 END
              FROM dbo._scheme_metadata_cache c
             WHERE c._scheme_id = @scheme_id AND c._name = @first AND c._parent_structure_id IS NULL;
            IF @sid IS NOT NULL AND @db_type = N'ListItem'
            BEGIN
                SET @list_prop = @last;
                RETURN N'{"kind":"field","sid":' + CAST(@sid AS NVARCHAR(40))
                     + N',"db_type":"ListItem","db_column":"_ListItem"'
                     + N',"name":' + dbo.pvt_json_string_or_null(@path)
                     + N',"is_array":' + CASE WHEN @is_array = 1 THEN N'true' ELSE N'false' END
                     + N',"list_item_prop":' + dbo.pvt_json_string_or_null(@list_prop)
                     + N',"dict_key":null,"parent_sid":null}';
            END;
        END;

        -- 2c. Generic nested walk
        DECLARE @cur_sid BIGINT, @segment NVARCHAR(200), @rem NVARCHAR(400), @cut INT;
        SET @rem = @path;
        SET @cut = CHARINDEX(N'.', @rem);
        SET @segment = LEFT(@rem, @cut - 1);
        -- Strip trailing "[]" so element-of-struct-array paths like
        -- "Contacts[].Type" walk into the nested "Type" field of the
        -- Contacts struct-array element.
        IF RIGHT(@segment, 2) = N'[]' SET @segment = LEFT(@segment, LEN(@segment) - 2);
        SELECT TOP 1 @cur_sid = c._structure_id
          FROM dbo._scheme_metadata_cache c
         WHERE c._scheme_id = @scheme_id AND c._name = @segment AND c._parent_structure_id IS NULL;
        IF @cur_sid IS NULL
            RETURN NULL;
        SET @rem = SUBSTRING(@rem, @cut + 1, 400);
        WHILE @rem IS NOT NULL AND @rem <> N''
        BEGIN
            SET @cut = CHARINDEX(N'.', @rem);
            IF @cut = 0
            BEGIN
                SET @segment = @rem;
                SET @rem = N'';
            END
            ELSE
            BEGIN
                SET @segment = LEFT(@rem, @cut - 1);
                SET @rem = SUBSTRING(@rem, @cut + 1, 400);
            END;
            IF RIGHT(@segment, 2) = N'[]' SET @segment = LEFT(@segment, LEN(@segment) - 2);
            SELECT TOP 1 @cur_sid = c._structure_id
              FROM dbo._scheme_metadata_cache c
             WHERE c._scheme_id = @scheme_id AND c._name = @segment AND c._parent_structure_id = @cur_sid;
            IF @cur_sid IS NULL
                RETURN NULL;
        END;

        SELECT TOP 1 @db_type = c.db_type,
                     @is_array = CASE WHEN c._collection_type IS NOT NULL THEN 1 ELSE 0 END
          FROM dbo._scheme_metadata_cache c
         WHERE c._structure_id = @cur_sid;
        SET @db_col = dbo.pvt_db_type_to_value_column(@db_type);
        RETURN N'{"kind":"field","sid":' + CAST(@cur_sid AS NVARCHAR(40))
             + N',"db_type":' + dbo.pvt_json_string_or_null(@db_type)
             + N',"db_column":' + dbo.pvt_json_string_or_null(@db_col)
             + N',"name":' + dbo.pvt_json_string_or_null(@path)
             + N',"is_array":' + CASE WHEN @is_array = 1 THEN N'true' ELSE N'false' END
             + N',"list_item_prop":null,"dict_key":null,"parent_sid":null}';
    END;

    -- 3. Bare root field (possibly Foo[] for arrays)
    DECLARE @base_name NVARCHAR(200) = @path;
    DECLARE @force_array BIT = 0;
    IF RIGHT(@path, 2) = N'[]'
    BEGIN
        SET @base_name = LEFT(@path, LEN(@path) - 2);
        SET @force_array = 1;
    END;

    SELECT TOP 1 @sid = c._structure_id, @db_type = c.db_type,
                 @is_array = CASE WHEN c._collection_type IS NOT NULL THEN 1 ELSE 0 END
      FROM dbo._scheme_metadata_cache c
     WHERE c._scheme_id = @scheme_id AND c._name = @base_name AND c._parent_structure_id IS NULL;
    IF @sid IS NULL
        RETURN NULL;
    IF @force_array = 1
        SET @is_array = 1;

    IF @db_type = N'ListItem'
    BEGIN
        SET @db_col = N'_ListItem';
        SET @list_prop = N'Id';
    END
    ELSE
        SET @db_col = dbo.pvt_db_type_to_value_column(@db_type);

    RETURN N'{"kind":"field","sid":' + CAST(@sid AS NVARCHAR(40))
         + N',"db_type":' + dbo.pvt_json_string_or_null(@db_type)
         + N',"db_column":' + dbo.pvt_json_string_or_null(@db_col)
         + N',"name":' + dbo.pvt_json_string_or_null(@path)
         + N',"is_array":' + CASE WHEN @is_array = 1 THEN N'true' ELSE N'false' END
         + N',"list_item_prop":' + dbo.pvt_json_string_or_null(@list_prop)
         + N',"dict_key":null,"parent_sid":null}';
END;
GO

-- ---------- pvt_extract_field_pairs -----------------------------------
-- Walks a filter JSON recursively and returns JSON array of {path, op_value}.
-- Logical ops ($and/$or/$not) are descended into; their child arrays/objects
-- contribute pairs but the operator keys themselves never appear as paths.
CREATE OR ALTER FUNCTION dbo.pvt_extract_field_pairs(@filter NVARCHAR(MAX))
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @filter IS NULL OR ISJSON(@filter) = 0
        RETURN N'[]';

    DECLARE @out NVARCHAR(MAX) = N'[';
    DECLARE @first BIT = 1;

    DECLARE c_keys CURSOR LOCAL FAST_FORWARD FOR
        SELECT [key], [value], [type] FROM OPENJSON(@filter);
    DECLARE @k NVARCHAR(400), @v NVARCHAR(MAX), @t INT;
    OPEN c_keys;
    FETCH NEXT FROM c_keys INTO @k, @v, @t;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF LEFT(@k, 1) = N'$'
        BEGIN
            -- Logical operator: descend
            IF @t = 4  -- array
            BEGIN
                DECLARE @elem NVARCHAR(MAX);
                DECLARE c_arr CURSOR LOCAL FAST_FORWARD FOR
                    SELECT [value] FROM OPENJSON(@v);
                OPEN c_arr;
                FETCH NEXT FROM c_arr INTO @elem;
                WHILE @@FETCH_STATUS = 0
                BEGIN
                    DECLARE @sub NVARCHAR(MAX) = dbo.pvt_extract_field_pairs(@elem);
                    IF @sub <> N'[]' AND LEN(@sub) > 2
                    BEGIN
                        IF @first = 0 SET @out = @out + N',';
                        SET @out = @out + SUBSTRING(@sub, 2, LEN(@sub) - 2);
                        SET @first = 0;
                    END;
                    FETCH NEXT FROM c_arr INTO @elem;
                END;
                CLOSE c_arr; DEALLOCATE c_arr;
            END
            ELSE IF @t = 5  -- object: descend into it
            BEGIN
                DECLARE @sub2 NVARCHAR(MAX) = dbo.pvt_extract_field_pairs(@v);
                IF @sub2 <> N'[]' AND LEN(@sub2) > 2
                BEGIN
                    IF @first = 0 SET @out = @out + N',';
                    SET @out = @out + SUBSTRING(@sub2, 2, LEN(@sub2) - 2);
                    SET @first = 0;
                END;
            END
            ELSE IF LOWER(@k) = N'$field' AND @t = 1
            BEGIN
                -- B2-expr $field: the string value is the field path to collect
                IF @first = 0 SET @out = @out + N',';
                SET @out = @out
                         + N'{"path":' + dbo.pvt_json_string_or_null(@v)
                         + N',"op_value":null}';
                SET @first = 0;
            END
            ELSE IF LOWER(@k) = N'$case' AND @t = 4
            BEGIN
                -- $case arms: descend into each {when, then, else} object
                DECLARE @ca_arm NVARCHAR(MAX);
                DECLARE c_ca2 CURSOR LOCAL FAST_FORWARD FOR
                    SELECT [value] FROM OPENJSON(@v);
                OPEN c_ca2;
                FETCH NEXT FROM c_ca2 INTO @ca_arm;
                WHILE @@FETCH_STATUS = 0
                BEGIN
                    DECLARE @ca_sub NVARCHAR(MAX);
                    -- descend into 'when', 'then', 'else' branches
                    DECLARE @ca_w NVARCHAR(MAX) = JSON_QUERY(@ca_arm, N'$.when');
                    DECLARE @ca_t NVARCHAR(MAX) = JSON_QUERY(@ca_arm, N'$.then');
                    DECLARE @ca_e NVARCHAR(MAX) = JSON_QUERY(@ca_arm, N'$.else');
                    IF @ca_w IS NOT NULL
                    BEGIN
                        SET @ca_sub = dbo.pvt_extract_field_pairs(@ca_w);
                        IF @ca_sub <> N'[]' AND LEN(@ca_sub) > 2
                        BEGIN
                            IF @first = 0 SET @out = @out + N',';
                            SET @out = @out + SUBSTRING(@ca_sub, 2, LEN(@ca_sub) - 2);
                            SET @first = 0;
                        END;
                    END;
                    IF @ca_t IS NOT NULL
                    BEGIN
                        SET @ca_sub = dbo.pvt_extract_field_pairs(@ca_t);
                        IF @ca_sub <> N'[]' AND LEN(@ca_sub) > 2
                        BEGIN
                            IF @first = 0 SET @out = @out + N',';
                            SET @out = @out + SUBSTRING(@ca_sub, 2, LEN(@ca_sub) - 2);
                            SET @first = 0;
                        END;
                    END;
                    IF @ca_e IS NOT NULL
                    BEGIN
                        SET @ca_sub = dbo.pvt_extract_field_pairs(@ca_e);
                        IF @ca_sub <> N'[]' AND LEN(@ca_sub) > 2
                        BEGIN
                            IF @first = 0 SET @out = @out + N',';
                            SET @out = @out + SUBSTRING(@ca_sub, 2, LEN(@ca_sub) - 2);
                            SET @first = 0;
                        END;
                    END;
                    FETCH NEXT FROM c_ca2 INTO @ca_arm;
                END;
                CLOSE c_ca2; DEALLOCATE c_ca2;
            END;
        END
        ELSE
        BEGIN
            -- Field reference; strip property-function suffixes so the base name
            -- is used for metadata resolution (.$length, .$count, [].$count).
            DECLARE @raw_path NVARCHAR(400) = @k;
            DECLARE @field_path NVARCHAR(400) = @k;
            IF RIGHT(@field_path, 9) = N'[].$count'
                SET @field_path = LEFT(@field_path, LEN(@field_path) - 9);
            ELSE IF RIGHT(@field_path, 8) = N'.$length'
                SET @field_path = LEFT(@field_path, LEN(@field_path) - 8);
            ELSE IF RIGHT(@field_path, 7) = N'.$count'
                SET @field_path = LEFT(@field_path, LEN(@field_path) - 7);
            DECLARE @opv NVARCHAR(MAX) = NULL;
            IF RIGHT(@raw_path, 12) = N'.ContainsKey'
                SET @opv = dbo.pvt_peek_contains_key_value(@v);
            IF @first = 0 SET @out = @out + N',';
            SET @out = @out
                     + N'{"path":' + dbo.pvt_json_string_or_null(@field_path)
                     + N',"op_value":' + dbo.pvt_json_string_or_null(@opv) + N'}';
            SET @first = 0;
        END;
        FETCH NEXT FROM c_keys INTO @k, @v, @t;
    END;
    CLOSE c_keys; DEALLOCATE c_keys;

    SET @out = @out + N']';
    RETURN @out;
END;
GO

-- ---------- pvt_collect_fields ----------------------------------------
-- Resolves metadata for every field referenced in filter/order JSON.
-- Returns JSON object {field_name: <FieldInfo>}.
-- ContainsKey-rewritten entries carry "was_contains_key": true.
CREATE OR ALTER FUNCTION dbo.pvt_collect_fields(
    @scheme_id BIGINT,
    @filter    NVARCHAR(MAX),
    @order     NVARCHAR(MAX)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @scheme_id IS NULL
        RETURN N'{}';

    DECLARE @seen TABLE (name NVARCHAR(400) PRIMARY KEY, meta NVARCHAR(MAX) NOT NULL);

    -- ---- From filter (via extract_field_pairs) ----
    DECLARE @pairs NVARCHAR(MAX) = dbo.pvt_extract_field_pairs(@filter);
    IF @pairs IS NOT NULL AND @pairs <> N'[]'
    BEGIN
        DECLARE c_p CURSOR LOCAL FAST_FORWARD FOR
            SELECT JSON_VALUE([value], '$.path'), JSON_VALUE([value], '$.op_value')
              FROM OPENJSON(@pairs);
        DECLARE @path NVARCHAR(400), @opv NVARCHAR(MAX);
        OPEN c_p;
        FETCH NEXT FROM c_p INTO @path, @opv;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            DECLARE @norm NVARCHAR(400) = dbo.pvt_normalize_field_name(@path, @opv);
            IF NOT EXISTS (SELECT 1 FROM @seen WHERE name = @norm)
            BEGIN
                DECLARE @meta NVARCHAR(MAX) = dbo.pvt_resolve_field_path(@scheme_id, @norm);
                IF @meta IS NOT NULL
                BEGIN
                    IF @norm <> @path
                        SET @meta = LEFT(@meta, LEN(@meta) - 1) + N',"was_contains_key":true}';
                    INSERT INTO @seen(name, meta) VALUES (@norm, @meta);
                END;
            END;
            FETCH NEXT FROM c_p INTO @path, @opv;
        END;
        CLOSE c_p; DEALLOCATE c_p;
    END;

    -- ---- From order (plain {field|field_path}; also walks $expr nodes) ----
    IF @order IS NOT NULL AND ISJSON(@order) = 1
    BEGIN
        DECLARE c_o CURSOR LOCAL FAST_FORWARD FOR
            SELECT COALESCE(JSON_VALUE([value], '$.field'), JSON_VALUE([value], '$.field_path')),
                   JSON_QUERY([value], '$."$expr"')
              FROM OPENJSON(@order);
        DECLARE @ofp NVARCHAR(400), @oexpr NVARCHAR(MAX);
        OPEN c_o;
        FETCH NEXT FROM c_o INTO @ofp, @oexpr;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            IF @ofp IS NOT NULL AND NOT EXISTS (SELECT 1 FROM @seen WHERE name = @ofp)
            BEGIN
                DECLARE @ometa NVARCHAR(MAX) = dbo.pvt_resolve_field_path(@scheme_id, @ofp);
                IF @ometa IS NOT NULL
                    INSERT INTO @seen(name, meta) VALUES (@ofp, @ometa);
            END;
            IF @oexpr IS NOT NULL
            BEGIN
                DECLARE @oepairs NVARCHAR(MAX) = dbo.pvt_extract_field_pairs(@oexpr);
                IF @oepairs IS NOT NULL AND @oepairs <> N'[]'
                BEGIN
                    DECLARE c_oe CURSOR LOCAL FAST_FORWARD FOR
                        SELECT JSON_VALUE([value], '$.path'), JSON_VALUE([value], '$.op_value')
                          FROM OPENJSON(@oepairs);
                    DECLARE @oepath NVARCHAR(400), @oeopv NVARCHAR(MAX);
                    OPEN c_oe;
                    FETCH NEXT FROM c_oe INTO @oepath, @oeopv;
                    WHILE @@FETCH_STATUS = 0
                    BEGIN
                        DECLARE @oenorm NVARCHAR(400) = dbo.pvt_normalize_field_name(@oepath, @oeopv);
                        IF NOT EXISTS (SELECT 1 FROM @seen WHERE name = @oenorm)
                        BEGIN
                            DECLARE @oemeta NVARCHAR(MAX) = dbo.pvt_resolve_field_path(@scheme_id, @oenorm);
                            IF @oemeta IS NOT NULL
                            BEGIN
                                IF @oenorm <> @oepath
                                    SET @oemeta = LEFT(@oemeta, LEN(@oemeta) - 1) + N',"was_contains_key":true}';
                                INSERT INTO @seen(name, meta) VALUES (@oenorm, @oemeta);
                            END;
                        END;
                        FETCH NEXT FROM c_oe INTO @oepath, @oeopv;
                    END;
                    CLOSE c_oe; DEALLOCATE c_oe;
                END;
            END;
            FETCH NEXT FROM c_o INTO @ofp, @oexpr;
        END;
        CLOSE c_o; DEALLOCATE c_o;
    END;

    DECLARE @out NVARCHAR(MAX) = N'{';
    DECLARE @first BIT = 1;
    DECLARE c_s CURSOR LOCAL FAST_FORWARD FOR SELECT name, meta FROM @seen ORDER BY name;
    DECLARE @n NVARCHAR(400), @m NVARCHAR(MAX);
    OPEN c_s;
    FETCH NEXT FROM c_s INTO @n, @m;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @first = 0 SET @out = @out + N',';
        SET @out = @out + dbo.pvt_json_string_or_null(@n) + N':' + @m;
        SET @first = 0;
        FETCH NEXT FROM c_s INTO @n, @m;
    END;
    CLOSE c_s; DEALLOCATE c_s;
    SET @out = @out + N'}';
    RETURN @out;
END;
GO

-- ---------- pvt_has_absence_check -------------------------------------
-- Strict subset of "has null check": returns 1 only for predicates that
-- require detecting absent _values rows ($null/$isNull/$exists/{$eq:null}).
-- $notNull / {$ne:null} are NOT flagged (INNER JOIN already drops absent).
CREATE OR ALTER FUNCTION dbo.pvt_has_absence_check(@filter NVARCHAR(MAX))
RETURNS BIT
AS
BEGIN
    IF @filter IS NULL OR ISJSON(@filter) = 0
        RETURN 0;

    DECLARE c CURSOR LOCAL FAST_FORWARD FOR
        SELECT [key], [value], [type] FROM OPENJSON(@filter);
    DECLARE @k NVARCHAR(400), @v NVARCHAR(MAX), @t INT;
    DECLARE @hit BIT = 0;
    OPEN c;
    FETCH NEXT FROM c INTO @k, @v, @t;
    WHILE @@FETCH_STATUS = 0 AND @hit = 0
    BEGIN
        IF LEFT(@k, 1) = N'$'
        BEGIN
            IF LOWER(@k) IN (N'$null', N'$isnull', N'$exists')
                SET @hit = 1;
            ELSE IF @t = 4  -- array
            BEGIN
                DECLARE c_a CURSOR LOCAL FAST_FORWARD FOR SELECT [value] FROM OPENJSON(@v);
                DECLARE @e NVARCHAR(MAX);
                OPEN c_a;
                FETCH NEXT FROM c_a INTO @e;
                WHILE @@FETCH_STATUS = 0 AND @hit = 0
                BEGIN
                    IF dbo.pvt_has_absence_check(@e) = 1 SET @hit = 1;
                    FETCH NEXT FROM c_a INTO @e;
                END;
                CLOSE c_a; DEALLOCATE c_a;
            END
            ELSE IF @t = 5  -- object
                IF dbo.pvt_has_absence_check(@v) = 1 SET @hit = 1;
        END
        ELSE
        BEGIN
            IF @t = 5  -- object operand
            BEGIN
                DECLARE c_op CURSOR LOCAL FAST_FORWARD FOR
                    SELECT [key], [value], [type] FROM OPENJSON(@v);
                DECLARE @sk NVARCHAR(400), @sv NVARCHAR(MAX), @st INT;
                OPEN c_op;
                FETCH NEXT FROM c_op INTO @sk, @sv, @st;
                WHILE @@FETCH_STATUS = 0 AND @hit = 0
                BEGIN
                    IF LOWER(@sk) IN (N'$null', N'$isnull', N'$exists')
                        SET @hit = 1;
                    ELSE IF LOWER(@sk) = N'$eq' AND @st = 0  -- null literal
                        SET @hit = 1;
                    FETCH NEXT FROM c_op INTO @sk, @sv, @st;
                END;
                CLOSE c_op; DEALLOCATE c_op;
            END
            ELSE IF @t = 0  -- null literal directly
                SET @hit = 1;
        END;
        FETCH NEXT FROM c INTO @k, @v, @t;
    END;
    CLOSE c; DEALLOCATE c;
    RETURN @hit;
END;
GO


-- ===== 11_pvt_column_expr.sql =====
-- =====================================================================
-- 11_pvt_column_expr.sql (MSSql)
-- ---------------------------------------------------------------------
-- Build a single SELECT-list expression for the PVT pivot CTE.
-- Mirrors PG v2-pvt/11_pvt_column_expr.sql.
--
-- T-SQL replaces PG's `(array_agg(v.<col>) FILTER (WHERE ...))[1]`
-- idiom with a correlated subquery that returns the matching value.
-- For arrays we use `(SELECT ... FOR JSON PATH)` to materialize a
-- JSON array of values (caller decides how to parse it).
--
-- Functions:
--   dbo.pvt_db_type_to_value_column(@db_type NVARCHAR(64))
--       -> NVARCHAR(64) | NULL
--   dbo.pvt_build_column_expr(@field_name NVARCHAR(400),
--                             @field_meta NVARCHAR(MAX))   -- FieldInfo JSON
--       -> NVARCHAR(MAX)  -- T-SQL fragment for the outer SELECT list
-- =====================================================================
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

-- ---------- pvt_db_type_to_value_column ----------------------------------
-- Maps a logical db_type to the physical typed column name on _values.
-- Returns NULL if the type is unknown.
CREATE OR ALTER FUNCTION dbo.pvt_db_type_to_value_column(@db_type NVARCHAR(64))
RETURNS NVARCHAR(64)
AS
BEGIN
    RETURN CASE @db_type
        WHEN N'String'         THEN N'_String'
        WHEN N'Long'           THEN N'_Long'
        WHEN N'Double'         THEN N'_Double'
        WHEN N'Numeric'        THEN N'_Numeric'
        WHEN N'Boolean'        THEN N'_Boolean'
        WHEN N'Guid'           THEN N'_Guid'
        WHEN N'DateTimeOffset' THEN N'_DateTimeOffset'
        WHEN N'ByteArray'      THEN N'_ByteArray'
        WHEN N'ListItem'       THEN N'_ListItem'
        WHEN N'Object'         THEN N'_Object'
        WHEN N'TimeSpan'       THEN N'_String'
        WHEN N'DateTime'       THEN N'_DateTimeOffset'
        ELSE NULL
    END;
END;
GO

-- ---------- pvt_build_column_expr ----------------------------------------
-- Build one SELECT-list expression for the PVT inner pivot scan.
--
-- Mirrors PG `pvt_build_column_expr` Pro-shape: scalars become
-- `MAX(CASE WHEN v._id_structure = <sid> AND v._array_index IS NULL
--          THEN v.<col> END) AS [field]` aggregates over a single
-- `LEFT JOIN _values v ON v._id_object = o._id` scan. Arrays remain
-- correlated `(SELECT ... FOR JSON PATH)` subqueries because MAX
-- collapses arrays and loses order.
--
-- Output context: the inner pivot SELECT body emitted by
-- pvt_build_cte_sql, with one or two scans available:
--   * `v`  : dbo._values, the pivot source (always present)
--   * `li` : dbo._list_items, LEFT JOIN'd by pvt_build_cte_sql when
--            any field is ListItem.Value/.Alias
--
-- Output examples (alias = QUOTENAME(field_name)):
--   base:                o.[_id_parent] AS [ParentId]
--   scalar prop:         MAX(CASE WHEN v.[_id_structure] = 42 AND v.[_array_index] IS NULL THEN v.[_Long] END) AS [Age]
--   array prop:          (SELECT v2.[_Long] FROM dbo._values v2
--                          WHERE v2._id_object = v.[_id_object] AND v2._id_structure = 42
--                            AND v2._array_index IS NOT NULL
--                          ORDER BY v2._array_index FOR JSON PATH) AS [Tags]
--   simple dict[home]:   MAX(CASE WHEN v.[_id_structure] = 42 AND v.[_array_index] = N'home' THEN v.[_String] END) AS [PhoneBook[home]]
--   ListItem.Value:      MAX(CASE WHEN v.[_id_structure] = 42 AND v.[_array_index] IS NULL THEN li.[_value] END) AS [Status]
--   ListItem.Id:         MAX(CASE WHEN v.[_id_structure] = 42 AND v.[_array_index] IS NULL THEN v.[_ListItem] END) AS [StatusId]
--
-- @array_index_in_outer (Pro parity): when 1, drop `AND v._array_index IS NULL`
-- from the scalar CASE filter; caller hoists the index predicate to an
-- outer WHERE for inline-pivot shape. Defaults to 0 (CTE-style).
--
-- Nested-dict fields (parent_sid AND dict_key) return NULL: they are
-- handled via a side CTE emitted by pvt_build_cte_sql (Stage 3 of the
-- MSSql port).
CREATE OR ALTER FUNCTION dbo.pvt_build_column_expr(
    @field_name             NVARCHAR(400),
    @field_meta             NVARCHAR(MAX),
    @array_index_in_outer   BIT = 0,
    @row_ref                NVARCHAR(40) = N'o.[_id]'
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @field_meta IS NULL OR ISJSON(@field_meta) = 0
        RETURN N'/* pvt_build_column_expr: meta is NULL or not JSON for field "'
             + ISNULL(@field_name, N'<null>') + N'" */';

    DECLARE @kind       NVARCHAR(32)  = JSON_VALUE(@field_meta, N'$.kind');
    DECLARE @li_prop    NVARCHAR(32)  = JSON_VALUE(@field_meta, N'$.list_item_prop');
    DECLARE @dict_key   NVARCHAR(400) = JSON_VALUE(@field_meta, N'$.dict_key');
    DECLARE @parent_sid NVARCHAR(32)  = JSON_VALUE(@field_meta, N'$.parent_sid');
    DECLARE @is_array_s NVARCHAR(10)  = JSON_VALUE(@field_meta, N'$.is_array');
    DECLARE @is_array   bit = CASE WHEN @is_array_s = N'true' THEN 1 ELSE 0 END;

    DECLARE @alias NVARCHAR(420) = QUOTENAME(@field_name);

    -- ---- Base field: straight projection from _objects. -------------
    IF @kind = N'base'
    BEGIN
        DECLARE @col NVARCHAR(64) = JSON_VALUE(@field_meta, N'$.column');
        RETURN N'o.' + QUOTENAME(@col) + N' AS ' + @alias;
    END;

    DECLARE @sid     NVARCHAR(32) = JSON_VALUE(@field_meta, N'$.sid');
    DECLARE @db_type NVARCHAR(64) = JSON_VALUE(@field_meta, N'$.db_type');

    -- ---- Nested-dict: handled by side CTE (Stage 3); caller skips. --
    IF @parent_sid IS NOT NULL AND @dict_key IS NOT NULL
        RETURN NULL;

    -- Scalar CASE filter prefix. Drop _array_index predicate when
    -- caller hoists it to an outer WHERE (inline-pivot Pro parity).
    DECLARE @scalar_filter NVARCHAR(200) =
        N'v.[_id_structure] = ' + @sid
        + CASE WHEN @array_index_in_outer = 1 THEN N''
               ELSE N' AND v.[_array_index] IS NULL' END;

    -- ---- ListItem.Value (scalar via li join, array via correlated) --
    IF @li_prop = N'Value'
    BEGIN
        IF @is_array = 1
            RETURN N'(SELECT li2.[_value] FROM dbo._values v2'
                 + N' JOIN dbo._list_items li2 ON li2._id = v2.[_ListItem]'
                 + N' WHERE v2._id_object = ' + @row_ref + N' AND v2._id_structure = ' + @sid
                 + N' AND v2._array_index IS NOT NULL'
                 + N' ORDER BY v2._array_index FOR JSON PATH) AS ' + @alias;
        RETURN N'MAX(CASE WHEN ' + @scalar_filter + N' THEN li.[_value] END) AS ' + @alias;
    END;

    -- ---- ListItem.Alias (scalar via li join, array via correlated) --
    IF @li_prop = N'Alias'
    BEGIN
        IF @is_array = 1
            RETURN N'(SELECT li2.[_alias] FROM dbo._values v2'
                 + N' JOIN dbo._list_items li2 ON li2._id = v2.[_ListItem]'
                 + N' WHERE v2._id_object = ' + @row_ref + N' AND v2._id_structure = ' + @sid
                 + N' AND v2._array_index IS NOT NULL'
                 + N' ORDER BY v2._array_index FOR JSON PATH) AS ' + @alias;
        RETURN N'MAX(CASE WHEN ' + @scalar_filter + N' THEN li.[_alias] END) AS ' + @alias;
    END;

    -- ---- Resolve typed column ---------------------------------------
    DECLARE @vcol NVARCHAR(64) = dbo.pvt_db_type_to_value_column(@db_type);
    IF @vcol IS NULL
        RETURN N'/* pvt_build_column_expr: unsupported db_type "'
             + ISNULL(@db_type, N'<null>') + N'" for field "' + @field_name + N'" */';

    -- ListItem.Id: project the foreign key column itself (bigint).
    IF @li_prop = N'Id'
        SET @vcol = N'_ListItem';

    -- ---- Simple dictionary: PhoneBook[home] --> _array_index='<key>'
    IF @dict_key IS NOT NULL AND @parent_sid IS NULL
    BEGIN
        DECLARE @dict_lit NVARCHAR(MAX) =
            N'N''' + REPLACE(@dict_key, N'''', N'''''') + N'''';
        IF @vcol = N'_Boolean'
            RETURN N'CONVERT(BIT, MAX(CASE WHEN v.[_id_structure] = ' + @sid
                 + N' AND v.[_array_index] = ' + @dict_lit
                 + N' THEN CAST(v.[_Boolean] AS TINYINT) END)) AS ' + @alias;
        RETURN N'MAX(CASE WHEN v.[_id_structure] = ' + @sid
             + N' AND v.[_array_index] = ' + @dict_lit
             + N' THEN v.' + QUOTENAME(@vcol) + N' END) AS ' + @alias;
    END;

    -- ---- Array pivot: correlated FOR JSON PATH subquery -------------
    IF @is_array = 1
        RETURN N'(SELECT v2.' + QUOTENAME(@vcol)
             + N' FROM dbo._values v2'
             + N' WHERE v2._id_object = ' + @row_ref + N' AND v2._id_structure = ' + @sid
             + N' AND v2._array_index IS NOT NULL'
             + N' ORDER BY v2._array_index FOR JSON PATH) AS ' + @alias;

    -- ---- Scalar pivot: MAX(CASE WHEN ... THEN v.<col> END) ----------
    IF @vcol = N'_Boolean'
        RETURN N'CONVERT(BIT, MAX(CASE WHEN ' + @scalar_filter
             + N' THEN CAST(v.[_Boolean] AS TINYINT) END)) AS ' + @alias;
    RETURN N'MAX(CASE WHEN ' + @scalar_filter
         + N' THEN v.' + QUOTENAME(@vcol) + N' END) AS ' + @alias;
END;
GO


-- ===== 12_pvt_cte_builder.sql =====
-- =====================================================================
-- 12_pvt_cte_builder.sql  (MSSql v2-pvt) — Pro-shape pivot body
-- ---------------------------------------------------------------------
-- Build the INNER pivot SELECT body (NO `WITH _pvt_cte AS (...)` wrap,
-- NO alias). T-SQL forbids `WITH` inside a subquery, so unlike PG we
-- return ONLY the SELECT body and let every orchestrator wrap it as a
-- derived table: `FROM (<pvt_build_cte_sql>) _pvt_cte`.
--
-- Two pivot shapes (Pro parity, mirrors PG 12):
--
--   WIDE (default, projects every _objects base col):
--     SELECT
--         o.[_id], ... <21 base cols>,
--         MAX(CASE WHEN v._id_structure = <sid> AND v._array_index IS NULL
--                  THEN v.<col> END) AS [Field1],
--         ...
--     FROM dbo._objects o
--     <LEFT|INNER> JOIN dbo._values v ON v._id_object = o._id
--     [LEFT JOIN dbo._list_items li ON li._id = v._ListItem]
--     WHERE o._id_scheme = <scheme_id> [AND <base pushdown>]
--     GROUP BY o.[_id], ... <21 base cols>
--
--   NARROW (Pro-shape, when @narrow=1 AND @force_outer=0 AND there are
--           pivot sids AND no nested-dict groups):
--     SELECT
--         v._id_object,
--         MAX(CASE WHEN ... THEN v.<col> END) AS [Field1],
--         ...
--     FROM dbo._values v [LEFT JOIN dbo._list_items li ...]
--     WHERE v._id_structure IN (<sid1>, <sid2>, ...)
--       AND v._id_object IN (SELECT o.[_id] FROM dbo._objects o
--                             WHERE o.[_id_scheme] = <scheme_id>
--                               [AND <base pushdown>])
--     GROUP BY v._id_object
--
-- Narrow drops the heavy GROUP BY (1 col vs 21), narrows the _values
-- scan via the structure-id index, and lets the planner prune by base
-- predicates BEFORE touching _values. Outer SELECT in the orchestrator
-- MUST `JOIN dbo._objects o ON o._id = _pvt_cte._id_object` to expose
-- base columns when narrow is chosen.
--
-- WARNING (T-SQL, by design): wide-shape GROUP BY includes _objects
-- columns of MAX-types (_note NVARCHAR(MAX), _value_string NVARCHAR(MAX),
-- _value_bytes VARBINARY(MAX)). T-SQL forbids GROUP BY on those, so a
-- scheme that touches one of those base cols via a pivot field will
-- fail at execution time. Mirrors PG by design — caller's responsibility.
-- Use narrow shape to dodge entirely (groups by v._id_object only).
--
-- Tree modes (tree / tree_descendants / tree_children / tree_roots /
-- tree_leaves / tree_ancestors) and nested-dict side CTEs are deferred
-- to Stage 2 and Stage 3 of the MSSql port. Caller passing
-- @source_mode <> 'flat' (or schemes with nested-dict accessors) get
-- the wide shape with those fields silently dropped from the pivot.
-- =====================================================================

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER FUNCTION dbo.pvt_build_cte_sql(
    @scheme_id          BIGINT,
    @fields             NVARCHAR(MAX),
    @source_mode        NVARCHAR(50)    = N'flat',
    @tree_ids           NVARCHAR(MAX)   = NULL,     -- JSON array of bigints (Stage 2)
    @max_depth          INT             = NULL,
    @force_outer        BIT             = 1,
    @extra_where        NVARCHAR(MAX)   = NULL,     -- Pro-style base-field pushdown
    @narrow             BIT             = 0,
    @include_seed       BIT             = 1,        -- tree mode (Stage 2)
    @polymorphic        BIT             = 1,        -- tree mode (Stage 2)
    @residual_where     NVARCHAR(MAX)   = NULL      -- post-pivot WHERE wrap (Pro parity)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @scheme_id IS NULL
        RETURN NULL;
    IF @source_mode IS NULL
        SET @source_mode = N'flat';
    IF @source_mode NOT IN (N'flat', N'tree', N'tree_descendants', N'tree_children',
                            N'tree_roots', N'tree_leaves', N'tree_ancestors')
        RETURN NULL;

    -- ------ Tree-mode object-set restriction --------------------------
    -- Builds @tree_filter as an `o.[_id] IN (SELECT _id FROM dbo.pvt_tree_<mode>(...))`
    -- predicate. Wide shape ANDs it into the WHERE; narrow shape folds
    -- it into the inner IN-subquery so the recursive walk runs BEFORE
    -- the _values scan. T-SQL forbids `WITH` inside a subquery, so the
    -- recursive CTEs live inside the multi-statement TVFs defined in
    -- 08_pvt_tree_functions.sql.
    DECLARE @tree_filter NVARCHAR(MAX) = N'';
    IF @source_mode <> N'flat'
    BEGIN
        DECLARE @seed_arg NVARCHAR(MAX) =
            CASE WHEN @tree_ids IS NULL THEN N'NULL'
                 ELSE N'N''' + REPLACE(@tree_ids, N'''', N'''''') + N'''' END;
        DECLARE @scheme_arg  NVARCHAR(40) = CAST(@scheme_id AS NVARCHAR(40));
        DECLARE @depth_arg   NVARCHAR(20) =
            CASE WHEN @max_depth IS NULL THEN N'NULL' ELSE CAST(@max_depth AS NVARCHAR(20)) END;
        DECLARE @poly_arg    NVARCHAR(1)  = CASE WHEN @polymorphic = 0 THEN N'0' ELSE N'1' END;
        DECLARE @seed_inc    NVARCHAR(1)  = CASE WHEN @include_seed = 0 THEN N'0' ELSE N'1' END;

        IF @source_mode IN (N'tree', N'tree_descendants')
        BEGIN
            IF @tree_ids IS NULL RETURN NULL;   -- seed required
            SET @tree_filter =
                N'o.[_id] IN (SELECT _id FROM dbo.pvt_tree_descendants('
                + @scheme_arg + N', ' + @seed_arg + N', ' + @depth_arg
                + N', ' + @poly_arg + N', ' + @seed_inc + N'))';
        END
        ELSE IF @source_mode = N'tree_children'
        BEGIN
            IF @tree_ids IS NULL RETURN NULL;
            SET @tree_filter =
                N'o.[_id] IN (SELECT _id FROM dbo.pvt_tree_children('
                + @scheme_arg + N', ' + @seed_arg + N', ' + @poly_arg + N'))';
        END
        ELSE IF @source_mode = N'tree_roots'
        BEGIN
            SET @tree_filter =
                N'o.[_id] IN (SELECT _id FROM dbo.pvt_tree_roots('
                + @scheme_arg + N', ' + @seed_arg + N'))';
        END
        ELSE IF @source_mode = N'tree_leaves'
        BEGIN
            SET @tree_filter =
                N'o.[_id] IN (SELECT _id FROM dbo.pvt_tree_leaves('
                + @scheme_arg + N', ' + @seed_arg + N'))';
        END
        ELSE IF @source_mode = N'tree_ancestors'
        BEGIN
            IF @tree_ids IS NULL RETURN NULL;
            SET @tree_filter =
                N'o.[_id] IN (SELECT _id FROM dbo.pvt_tree_ancestors('
                + @scheme_arg + N', ' + @seed_arg + N', ' + @depth_arg
                + N', ' + @poly_arg + N'))';
        END;
    END;

    DECLARE @base_cols NVARCHAR(MAX) =
        N'o.[_id], o.[_id_parent], o.[_id_scheme], o.[_id_owner], o.[_id_who_change], '
      + N'o.[_name], o.[_date_create], o.[_date_modify], o.[_date_begin], o.[_date_complete], '
      + N'o.[_key], o.[_note], o.[_hash], '
      + N'o.[_value_long], o.[_value_string], o.[_value_guid], o.[_value_bool], '
      + N'o.[_value_double], o.[_value_numeric], o.[_value_datetime], o.[_value_bytes]';

    -- Iterate field metadata: collect pivot sids, exprs, aliases.
    DECLARE @pivot_cols    NVARCHAR(MAX) = N'';
    DECLARE @pivot_aliases NVARCHAR(MAX) = N'';       -- comma-sep QUOTENAME'd
    DECLARE @sids          NVARCHAR(MAX) = N'';       -- comma-sep bigints
    DECLARE @has_listitem  BIT = 0;
    DECLARE @has_nested    BIT = 0;

    -- Per-(parent_sid, dict_key) nested-dict field bucket. Mirrors
    -- PG v_nested_groups: each row is one nested-dict child field to
    -- be projected from a per-group derived table (Pro parity, see
    -- redb.Postgres/sql/v2-pvt/12_pvt_cte_builder.sql nested_dict_N).
    DECLARE @nested_tbl TABLE(
        parent_sid  NVARCHAR(40),
        dict_key    NVARCHAR(400),
        field_sid   NVARCHAR(40),
        db_column   NVARCHAR(50),
        is_array    BIT,
        field_name  NVARCHAR(400)
    );

    -- First pass: detect nested-dict presence so the second pass below
    -- can pick the correct row-reference for pvt_build_column_expr
    -- (`v.[_id_object]` for pure-narrow vs `o.[_id]` for wide /
    -- narrow-with-nested). Without this, the order of fields in
    -- OPENJSON could leave a stale row-ref baked into @pivot_cols.
    IF @fields IS NOT NULL AND ISJSON(@fields) = 1 AND @fields <> N'{}'
    BEGIN
        IF EXISTS (
            SELECT 1 FROM OPENJSON(@fields)
             WHERE JSON_VALUE([value], N'$.parent_sid') IS NOT NULL
               AND JSON_VALUE([value], N'$.dict_key')   IS NOT NULL
        )
            SET @has_nested = 1;
    END;

    -- narrow_nest = the slim shape that keeps FROM _objects o (so nested
    -- LEFT JOINs on o.[_id] still resolve) but collapses GROUP BY to a
    -- single column. Outer SELECT JOINs _objects via _id_object alias.
    DECLARE @narrow_nest BIT =
        CASE WHEN @narrow = 1 AND @has_nested = 1 THEN 1 ELSE 0 END;
    DECLARE @row_ref NVARCHAR(20) =
        CASE WHEN @narrow = 1 AND @has_nested = 0 THEN N'v.[_id_object]'
             ELSE N'o.[_id]' END;

    IF @fields IS NOT NULL AND ISJSON(@fields) = 1 AND @fields <> N'{}'
    BEGIN
        DECLARE @fname NVARCHAR(400), @fmeta NVARCHAR(MAX);
        DECLARE c CURSOR LOCAL FAST_FORWARD FOR
            SELECT [key], [value] FROM OPENJSON(@fields);
        OPEN c;
        FETCH NEXT FROM c INTO @fname, @fmeta;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            DECLARE @kind        NVARCHAR(32)  = JSON_VALUE(@fmeta, N'$.kind');
            DECLARE @lenmod      NVARCHAR(10)  = JSON_VALUE(@fmeta, N'$.length_modifier');
            DECLARE @fsid        NVARCHAR(32)  = JSON_VALUE(@fmeta, N'$.sid');
            DECLARE @fparent_sid NVARCHAR(32)  = JSON_VALUE(@fmeta, N'$.parent_sid');
            DECLARE @fdict_key   NVARCHAR(400) = JSON_VALUE(@fmeta, N'$.dict_key');
            DECLARE @fli_prop    NVARCHAR(32)  = JSON_VALUE(@fmeta, N'$.list_item_prop');

            -- Base cols are already in @base_cols; modifier-only entries
            -- (.$length / .$count) project no col of their own.
            IF @kind = N'base' OR @lenmod = N'true'
            BEGIN
                FETCH NEXT FROM c INTO @fname, @fmeta;
                CONTINUE;
            END;

            -- Nested-dict (parent_sid + dict_key): collected per group
            -- and projected from a derived table joined on _id_object.
            -- Pro parity: see PG nested_dict_N CTE in 12_pvt_cte_builder.sql.
            IF @fparent_sid IS NOT NULL AND @fdict_key IS NOT NULL
            BEGIN
                SET @has_nested = 1;
                INSERT INTO @nested_tbl(parent_sid, dict_key, field_sid, db_column, is_array, field_name)
                VALUES(
                    @fparent_sid,
                    @fdict_key,
                    @fsid,
                    JSON_VALUE(@fmeta, N'$.db_column'),
                    CASE WHEN JSON_VALUE(@fmeta, N'$.is_array') = N'true' THEN 1 ELSE 0 END,
                    @fname);
                FETCH NEXT FROM c INTO @fname, @fmeta;
                CONTINUE;
            END;

            -- Collect sid for narrow-shape IN (...) filter. Skip nested
            -- children (parent_sid set: they project from a side CTE
            -- scoped to the parent dict row, not the main _values scan).
            -- Simple-dict (dict_key set, parent_sid NULL) keep their sid
            -- because pvt_build_column_expr's CASE reads straight from
            -- the main scan with _array_index = '<key>' filter.
            IF @fsid IS NOT NULL AND @fparent_sid IS NULL
            BEGIN
                IF @sids = N'' SET @sids = @fsid;
                ELSE SET @sids = @sids + N', ' + @fsid;
            END;

            DECLARE @expr NVARCHAR(MAX) =
                dbo.pvt_build_column_expr(@fname, @fmeta, 0, @row_ref);
            IF @expr IS NOT NULL
            BEGIN
                SET @pivot_cols = @pivot_cols
                                + N',' + CHAR(13) + CHAR(10)
                                + N'            ' + @expr;
                DECLARE @qalias NVARCHAR(420) = QUOTENAME(@fname);
                IF @pivot_aliases = N'' SET @pivot_aliases = @qalias;
                ELSE SET @pivot_aliases = @pivot_aliases + N', ' + @qalias;
            END;

            IF @fli_prop IN (N'Value', N'Alias')
                SET @has_listitem = 1;

            FETCH NEXT FROM c INTO @fname, @fmeta;
        END;
        CLOSE c; DEALLOCATE c;
    END;

    DECLARE @join_kind NVARCHAR(20) =
        CASE WHEN @force_outer = 1 THEN N'LEFT JOIN' ELSE N'INNER JOIN' END;
    DECLARE @li_join   NVARCHAR(MAX) =
        CASE WHEN @has_listitem = 1
             THEN CHAR(13) + CHAR(10) + N'        LEFT JOIN dbo._list_items li ON li.[_id] = v.[_ListItem]'
             ELSE N'' END;

    -- Pushdown predicates assembled early so nested derived tables can
    -- fold them into their own IN-subquery on _objects (Pro parity).
    DECLARE @extra_pred NVARCHAR(MAX) =
        CASE WHEN @extra_where IS NOT NULL AND @extra_where <> N''
             THEN N' AND ' + @extra_where ELSE N'' END;
    DECLARE @tree_pred  NVARCHAR(MAX) =
        CASE WHEN @tree_filter <> N'' THEN N' AND ' + @tree_filter ELSE N'' END;

    -- ------ Nested-dict derived tables (Stage 2c.E, PG parity) -------
    -- For each (parent_sid, dict_key) group, emit a derived table that
    -- joins the parent dict row (dp) to its child rows (nv via
    -- _array_parent_id) and projects one MAX-CASE per requested child
    -- field. The outer pivot LEFT JOINs the derived table on _id_object
    -- and projects each child column via MAX(ndN.col) so the existing
    -- GROUP BY <base cols> stays valid. Mirrors PG nested_dict_N CTE
    -- in redb.Postgres/sql/v2-pvt/12_pvt_cte_builder.sql; scalar takes
    -- the single non-array row, array fields are aggregated via a
    -- correlated FOR JSON PATH subselect (no native array_agg).
    DECLARE @nested_joins NVARCHAR(MAX) = N'';
    IF @has_nested = 1
    BEGIN
        DECLARE @group_idx INT = 0;
        DECLARE @g_parent NVARCHAR(40), @g_key NVARCHAR(400);
        DECLARE gc CURSOR LOCAL FAST_FORWARD FOR
            SELECT DISTINCT parent_sid, dict_key FROM @nested_tbl;
        OPEN gc;
        FETCH NEXT FROM gc INTO @g_parent, @g_key;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @group_idx = @group_idx + 1;
            DECLARE @alias NVARCHAR(40) = N'nd' + CAST(@group_idx AS NVARCHAR(10));
            DECLARE @inner_cols NVARCHAR(MAX) = N'';
            DECLARE @g_sid NVARCHAR(40), @g_col NVARCHAR(50), @g_arr BIT, @g_name NVARCHAR(400);
            DECLARE fc CURSOR LOCAL FAST_FORWARD FOR
                SELECT field_sid, db_column, is_array, field_name
                  FROM @nested_tbl
                 WHERE parent_sid = @g_parent AND dict_key = @g_key;
            OPEN fc;
            FETCH NEXT FROM fc INTO @g_sid, @g_col, @g_arr, @g_name;
            WHILE @@FETCH_STATUS = 0
            BEGIN
                IF @g_arr = 1
                BEGIN
                    SET @inner_cols = @inner_cols
                        + CASE WHEN @inner_cols = N'' THEN N'' ELSE N', ' END
                        + N'(SELECT nv2.[' + @g_col + N']'
                        + N' FROM dbo._values nv2'
                        + N' WHERE nv2.[_array_parent_id] = MAX(dp.[_id])'
                        + N'   AND nv2.[_id_structure] = ' + @g_sid
                        + N'   AND nv2.[_array_index] IS NOT NULL'
                        + N' ORDER BY nv2.[_array_index]'
                        + N' FOR JSON PATH, WITHOUT_ARRAY_WRAPPER) AS '
                        + QUOTENAME(@g_name);
                END
                ELSE
                BEGIN
                    SET @inner_cols = @inner_cols
                        + CASE WHEN @inner_cols = N'' THEN N'' ELSE N', ' END
                        + N'MAX(CASE WHEN nv.[_id_structure] = ' + @g_sid
                        + N' AND nv.[_array_index] IS NULL THEN nv.[' + @g_col + N'] END) AS '
                        + QUOTENAME(@g_name);
                END;
                SET @pivot_cols = @pivot_cols
                    + N',' + CHAR(13) + CHAR(10)
                    + N'            MAX(' + @alias + N'.' + QUOTENAME(@g_name)
                    + N') AS ' + QUOTENAME(@g_name);
                IF @pivot_aliases = N'' SET @pivot_aliases = QUOTENAME(@g_name);
                ELSE SET @pivot_aliases = @pivot_aliases + N', ' + QUOTENAME(@g_name);
                FETCH NEXT FROM fc INTO @g_sid, @g_col, @g_arr, @g_name;
            END;
            CLOSE fc; DEALLOCATE fc;

            SET @nested_joins = @nested_joins
                + CHAR(13) + CHAR(10)
                + N'        LEFT JOIN ('
                + N' SELECT dp.[_id_object], ' + @inner_cols
                + N' FROM dbo._values dp'
                + N' LEFT JOIN dbo._values nv ON nv.[_array_parent_id] = dp.[_id]'
                + N' WHERE dp.[_id_structure] = ' + @g_parent
                + N' AND dp.[_array_index] = N''' + REPLACE(@g_key, N'''', N'''''') + N''''
                -- Pro-parity: schema (and any pushdown predicates) folds
                -- into the nested CTE itself so the planner can prune dp
                -- rows by _id_scheme BEFORE the LEFT JOIN nv expands.
                -- Mirrors PG PRO 12_pvt_cte_builder.sql nested_dict_N.
                + N' AND dp.[_id_object] IN (SELECT o.[_id] FROM dbo._objects o'
                + N' WHERE o.[_id_scheme] = ' + CAST(@scheme_id AS NVARCHAR(40))
                + @extra_pred + @tree_pred + N')'
                + N' GROUP BY dp.[_id_object]'
                + N' ) ' + @alias + N' ON ' + @alias + N'.[_id_object] = o.[_id]';

            FETCH NEXT FROM gc INTO @g_parent, @g_key;
        END;
        CLOSE gc; DEALLOCATE gc;
    END;

    DECLARE @body NVARCHAR(MAX);

    -- ------ Narrow Pro-shape -----------------------------------------
    -- Eligible when: narrow flag set, no forced outer (LEFT JOIN would
    -- drop objects without any _values rows), no nested-dict groups,
    -- and at least one pivot sid was collected. Base pushdown folds
    -- into the IN-subquery so the planner can prune via system-column
    -- indexes BEFORE the _values scan.
    IF @narrow = 1 AND @force_outer = 0 AND @has_nested = 0 AND @sids <> N''
    BEGIN
        DECLARE @obj_subq NVARCHAR(MAX) =
            N'(SELECT o.[_id] FROM dbo._objects o WHERE o.[_id_scheme] = '
            + CAST(@scheme_id AS NVARCHAR(40)) + @extra_pred + @tree_pred + N')';

        SET @body =
            N'SELECT' + CHAR(13) + CHAR(10)
          + N'            v.[_id_object]' + @pivot_cols
          + CHAR(13) + CHAR(10) + N'        FROM dbo._values v' + @li_join
          + CHAR(13) + CHAR(10) + N'        WHERE v.[_id_structure] IN (' + @sids + N')'
          + CHAR(13) + CHAR(10) + N'          AND v.[_id_object] IN ' + @obj_subq
          + CHAR(13) + CHAR(10) + N'        GROUP BY v.[_id_object]';
    END
    ELSE IF @narrow_nest = 1 AND @force_outer = 0
    BEGIN
        -- ------ Narrow-with-nested Pro-shape ---------------------------
        -- Slim pivot for schemes that touch nested-dict accessors: keep
        -- FROM _objects o (nested LEFT JOIN binds to o.[_id]) but drop
        -- the 21-col base GROUP BY in favour of GROUP BY o.[_id]. The
        -- outer SELECT JOINs _objects via _pvt_cte.[_id_object] just
        -- like the regular narrow shape. Pivot CASEs already use
        -- @row_ref = 'o.[_id]' (set in the first pass above), so array
        -- sub-queries resolve correctly.
        DECLARE @where_nn NVARCHAR(MAX) =
            N'o.[_id_scheme] = ' + CAST(@scheme_id AS NVARCHAR(40)) + @extra_pred + @tree_pred;

        -- Skip the main _values join entirely when nothing scalar-pivot
        -- needs it (@sids = ''). Without this, INNER JOIN _values v
        -- multiplies o rows by their _values cardinality only to be
        -- collapsed by the GROUP BY — pure overhead. Pro-parity:
        -- mirrors the standalone-nested branch in PG PRO 12.
        DECLARE @main_join_nn NVARCHAR(MAX) = CASE
            WHEN @sids = N'' THEN N''
            ELSE CHAR(13) + CHAR(10) + N'        ' + @join_kind
                 + N' dbo._values v ON v.[_id_object] = o.[_id]' + @li_join
        END;

        SET @body =
            N'SELECT' + CHAR(13) + CHAR(10)
          + N'            o.[_id] AS [_id_object]' + @pivot_cols
          + CHAR(13) + CHAR(10) + N'        FROM dbo._objects o'
                                              + @main_join_nn
                                              + @nested_joins
          + CHAR(13) + CHAR(10) + N'        WHERE ' + @where_nn
          + CHAR(13) + CHAR(10) + N'        GROUP BY o.[_id]';
    END
    ELSE
    BEGIN
        -- ------ Wide legacy --------------------------------------------
        DECLARE @where NVARCHAR(MAX) =
            N'o.[_id_scheme] = ' + CAST(@scheme_id AS NVARCHAR(40)) + @extra_pred + @tree_pred;

        SET @body =
            N'SELECT' + CHAR(13) + CHAR(10)
          + N'            ' + @base_cols + @pivot_cols
          + CHAR(13) + CHAR(10) + N'        FROM dbo._objects o'
          + CHAR(13) + CHAR(10) + N'        ' + @join_kind
                                              + N' dbo._values v ON v.[_id_object] = o.[_id]'
                                              + @li_join
                                              + @nested_joins
          + CHAR(13) + CHAR(10) + N'        WHERE ' + @where
          + CHAR(13) + CHAR(10) + N'        GROUP BY ' + @base_cols;
    END;

    -- ------ Residual WHERE pushdown (Pro parity) ---------------------
    -- When pvt_build_query_sql determines the outer filter touches only
    -- pivoted columns, it passes the pre-rendered SQL here. Wrapping it
    -- inside the inner pivot body lets the planner prune rows BEFORE
    -- the outer JOIN _objects (when narrow shape is in use).
    IF @residual_where IS NOT NULL
       AND @residual_where <> N''
       AND @residual_where <> N'TRUE'
    BEGIN
        SET @body =
            N'SELECT pvt.* FROM (' + CHAR(13) + CHAR(10)
          + N'        ' + @body + CHAR(13) + CHAR(10)
          + N'        ) pvt WHERE ' + @residual_where;
    END;

    RETURN @body;
END;
GO


-- ===== 13_pvt_condition.sql =====
-- =====================================================================
-- 13_pvt_condition.sql  (MSSql v2-pvt) — single-leaf predicate builder
-- ---------------------------------------------------------------------
-- Functions:
--   dbo.pvt_sql_string_literal(@val)            -> NVARCHAR escape ' -> ''
--   dbo.pvt_jsonb_to_sql_literal(@val NVARCHAR(MAX), @type INT)
--                                               -> raw token (strings escaped + quoted, numbers bare, booleans 1/0)
--   dbo.pvt_build_field_condition(@field_name, @field_meta, @op_json, @base_prefix)
--                                               -> NVARCHAR(MAX) SQL fragment
--
-- Slice operators (v0.1):
--   $eq $ne $gt $gte $lt $lte
--   $in $nin
--   $like
--   $startsWith $endsWith $contains
--   $startsWithIgnoreCase $endsWithIgnoreCase $containsIgnoreCase
--   $null $isNull $notNull $exists
-- Scalar-literal shorthand: { Field: 5 } == { Field: { $eq: 5 } }
--
-- Skipped: $regex/$iregex/$fts/$array*/length modifier.
-- =====================================================================

SET QUOTED_IDENTIFIER ON;
GO

-- ---------- pvt_sql_string_literal: escape and quote for T-SQL --------
CREATE OR ALTER FUNCTION dbo.pvt_sql_string_literal(@v NVARCHAR(MAX))
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @v IS NULL
        RETURN N'NULL';
    RETURN N'N''' + REPLACE(@v, N'''', N'''''') + N'''';
END;
GO

-- ---------- pvt_jsonb_to_sql_literal ----------------------------------
-- @type follows OPENJSON convention: 0=null, 1=string, 2=number, 3=true/false,
-- 4=array, 5=object. For arrays/objects, callers must use OPENJSON themselves.
CREATE OR ALTER FUNCTION dbo.pvt_jsonb_to_sql_literal(@val NVARCHAR(MAX), @type INT)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @val IS NULL OR @type = 0
        RETURN N'NULL';
    IF @type = 3  -- boolean: T-SQL has no boolean literal in expressions, use 1/0
        RETURN CASE WHEN LOWER(@val) = N'true' THEN N'1' ELSE N'0' END;
    IF @type = 2  -- number
        RETURN @val;
    -- strings (1) or unspecified: quote
    RETURN dbo.pvt_sql_string_literal(@val);
END;
GO

-- ---------- pvt_build_field_condition ---------------------------------
CREATE OR ALTER FUNCTION dbo.pvt_build_field_condition(
    @field_name   NVARCHAR(400),
    @field_meta   NVARCHAR(MAX),
    @op_json      NVARCHAR(MAX),
    @op_type      INT,                 -- OPENJSON type code of @op_json
    @base_prefix  NVARCHAR(10)         -- '' or 'o.' or '_pvt_cte.'
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @field_meta IS NULL
        RETURN NULL;

    DECLARE @kind NVARCHAR(50)  = JSON_VALUE(@field_meta, '$.kind');
    DECLARE @col_name NVARCHAR(100);
    IF @kind = N'base'
        SET @col_name = ISNULL(@base_prefix, N'') + QUOTENAME(JSON_VALUE(@field_meta, '$.column'));
    ELSE
        SET @col_name = QUOTENAME(@field_name);

    -- Array-operator support: resolve field metadata for _values lookups.
    DECLARE @sid       NVARCHAR(32) = JSON_VALUE(@field_meta, N'$.sid');
    DECLARE @db_type_f NVARCHAR(64) = JSON_VALUE(@field_meta, N'$.db_type');
    -- Derive the outer table alias used in EXISTS subqueries against dbo._values.
    -- Shape A: base_prefix = 'o.'  -> alias 'o'
    -- Shape C: base_prefix = '_pvt_cte.' -> alias '_pvt_cte'
    DECLARE @obj_alias NVARCHAR(50) = CASE WHEN @base_prefix = N'o.' THEN N'o' ELSE N'_pvt_cte' END;

    -- ---- Dictionary indexer: Field[key] or Field[key].NestedProp ----
    -- When dict_key is set in field metadata, emit an EXISTS correlated
    -- subquery against dbo._values filtering both structure and _array_index.
    DECLARE @dict_key_val NVARCHAR(200) = JSON_VALUE(@field_meta, N'$.dict_key');
    IF @dict_key_val IS NOT NULL
    AND (@base_prefix = N'_pvt_cte.' OR @base_prefix = N'o.')
    BEGIN
        -- Post-pivot context: dict-key field is materialized as a pivot column
        -- on _pvt_cte by the nested-dict CTE builder (12_pvt_cte_builder.sql).
        -- Reference that column directly instead of re-running an EXISTS over
        -- dbo._values, which would duplicate the lookup work the CTE already did.
        SET @col_name = N'_pvt_cte.' + QUOTENAME(@field_name);
        SET @dict_key_val = NULL;  -- fall through to normal column-comparison flow
    END;
    IF @dict_key_val IS NOT NULL
    BEGIN
        DECLARE @dict_db_col    NVARCHAR(200) = N'av.' + QUOTENAME(ISNULL(JSON_VALUE(@field_meta, N'$.db_column'), N'_String'));
        DECLARE @dict_key_esc   NVARCHAR(400) = REPLACE(@dict_key_val, N'''', N'''''');
        DECLARE @dict_parent_sid NVARCHAR(32) = JSON_VALUE(@field_meta, N'$.parent_sid');
        DECLARE @dict_exist_pfx NVARCHAR(MAX);
        IF @dict_parent_sid IS NOT NULL
            -- Nested dict: Foo[key].Child -> dp holds the dict key on parent_sid,
            -- av is the child row joined through _array_parent_id.
            SET @dict_exist_pfx =
                N'EXISTS (SELECT 1 FROM dbo._values dp'
                + N' INNER JOIN dbo._values av ON av._array_parent_id = dp._id'
                + N' WHERE dp._id_object = ' + @obj_alias + N'.[_id]'
                + N' AND dp._id_structure = ' + @dict_parent_sid
                + N' AND dp._array_index = N''' + @dict_key_esc + N''''
                + N' AND av._id_structure = ' + ISNULL(@sid, N'0')
                + N' AND av._array_index IS NULL';
        ELSE
            SET @dict_exist_pfx =
                N'EXISTS (SELECT 1 FROM dbo._values av'
                + N' WHERE av._id_object = ' + @obj_alias + N'.[_id]'
                + N' AND av._id_structure = ' + ISNULL(@sid, N'0')
                + N' AND av._array_index = N''' + @dict_key_esc + N'''';

        -- Scalar-literal shorthand (non-object operand)
        IF @op_type <> 5
        BEGIN
            IF @op_type = 0
                RETURN @dict_exist_pfx + N' AND ' + @dict_db_col + N' IS NULL)';
            RETURN @dict_exist_pfx + N' AND ' + @dict_db_col + N' = '
                + dbo.pvt_jsonb_to_sql_literal(@op_json, @op_type) + N')';
        END;

        -- Operator object: walk and AND-join
        DECLARE @dparts NVARCHAR(MAX) = N'';
        DECLARE @dcnt   INT = 0;
        DECLARE c_dict CURSOR LOCAL FAST_FORWARD FOR
            SELECT [key], [value], [type] FROM OPENJSON(@op_json);
        DECLARE @dok NVARCHAR(50), @dov NVARCHAR(MAX), @dot INT;
        OPEN c_dict;
        FETCH NEXT FROM c_dict INTO @dok, @dov, @dot;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            DECLARE @don NVARCHAR(50) = LOWER(@dok);
            DECLARE @dpiece NVARCHAR(MAX) = NULL;

            IF @don IN (N'$null', N'$isnull')
                SET @dpiece = CASE WHEN LOWER(@dov) = N'true'
                    THEN N'NOT ' + @dict_exist_pfx + N')'
                    ELSE @dict_exist_pfx + N')' END;
            ELSE IF @don IN (N'$notnull', N'$exists')
                SET @dpiece = CASE WHEN LOWER(@dov) = N'true'
                    THEN @dict_exist_pfx + N')'
                    ELSE N'NOT ' + @dict_exist_pfx + N')' END;
            ELSE IF @don IN (N'$eq', N'$ne', N'$gt', N'$gte', N'$lt', N'$lte')
            BEGIN
                DECLARE @dcmp NVARCHAR(4) = CASE @don
                    WHEN N'$eq'  THEN N'='   WHEN N'$ne'  THEN N'<>'
                    WHEN N'$gt'  THEN N'>'   WHEN N'$gte' THEN N'>='
                    WHEN N'$lt'  THEN N'<'   WHEN N'$lte' THEN N'<=' END;
                IF @dot = 0
                    SET @dpiece = CASE @don
                        WHEN N'$eq' THEN @dict_exist_pfx + N' AND ' + @dict_db_col + N' IS NULL)'
                        WHEN N'$ne' THEN @dict_exist_pfx + N' AND ' + @dict_db_col + N' IS NOT NULL)'
                        ELSE @dict_exist_pfx + N' AND ' + @dict_db_col + N' ' + @dcmp + N' NULL)' END;
                ELSE
                    SET @dpiece = @dict_exist_pfx + N' AND ' + @dict_db_col + N' ' + @dcmp + N' '
                        + dbo.pvt_jsonb_to_sql_literal(@dov, @dot) + N')';
            END
            ELSE IF @don = N'$like'
                SET @dpiece = @dict_exist_pfx + N' AND ' + @dict_db_col + N' LIKE ' + dbo.pvt_sql_string_literal(@dov) + N')';
            ELSE IF @don = N'$contains'
                SET @dpiece = @dict_exist_pfx + N' AND ' + @dict_db_col + N' LIKE ' + dbo.pvt_sql_string_literal(N'%' + @dov + N'%') + N')';
            ELSE IF @don = N'$startswith'
                SET @dpiece = @dict_exist_pfx + N' AND ' + @dict_db_col + N' LIKE ' + dbo.pvt_sql_string_literal(@dov + N'%') + N')';
            ELSE IF @don = N'$endswith'
                SET @dpiece = @dict_exist_pfx + N' AND ' + @dict_db_col + N' LIKE ' + dbo.pvt_sql_string_literal(N'%' + @dov) + N')';
            ELSE IF @don = N'$containsignorecase'
                SET @dpiece = @dict_exist_pfx + N' AND LOWER(' + @dict_db_col + N') LIKE ' + dbo.pvt_sql_string_literal(N'%' + LOWER(@dov) + N'%') + N')';
            ELSE IF @don = N'$startswithignorecase'
                SET @dpiece = @dict_exist_pfx + N' AND LOWER(' + @dict_db_col + N') LIKE ' + dbo.pvt_sql_string_literal(LOWER(@dov) + N'%') + N')';
            ELSE IF @don = N'$endswithignorecase'
                SET @dpiece = @dict_exist_pfx + N' AND LOWER(' + @dict_db_col + N') LIKE ' + dbo.pvt_sql_string_literal(N'%' + LOWER(@dov)) + N')';
            ELSE IF @don IN (N'$in', N'$nin')
            BEGIN
                IF @dot <> 4
                    SET @dpiece = N'/*invalid-dict-in*/1=0';
                ELSE
                BEGIN
                    DECLARE @dlst NVARCHAR(MAX) = N'';
                    DECLARE c_di CURSOR LOCAL FAST_FORWARD FOR SELECT [value], [type] FROM OPENJSON(@dov);
                    DECLARE @div NVARCHAR(MAX), @dit INT;
                    OPEN c_di;
                    FETCH NEXT FROM c_di INTO @div, @dit;
                    WHILE @@FETCH_STATUS = 0
                    BEGIN
                        IF @dlst <> N'' SET @dlst += N', ';
                        SET @dlst += dbo.pvt_jsonb_to_sql_literal(@div, @dit);
                        FETCH NEXT FROM c_di INTO @div, @dit;
                    END;
                    CLOSE c_di; DEALLOCATE c_di;
                    IF @dlst = N'' SET @dlst = N'NULL';
                    SET @dpiece = @dict_exist_pfx + N' AND ' + @dict_db_col
                        + CASE WHEN @don = N'$in' THEN N' IN (' ELSE N' NOT IN (' END
                        + @dlst + N'))';
                END;
            END
            ELSE
                SET @dpiece = N'/*unsupported-dict-op:' + @dok + N'*/1=0';

            IF @dpiece IS NOT NULL
            BEGIN
                IF @dcnt > 0 SET @dparts += N' AND ';
                SET @dparts += @dpiece;
                SET @dcnt += 1;
            END;
            FETCH NEXT FROM c_dict INTO @dok, @dov, @dot;
        END;
        CLOSE c_dict; DEALLOCATE c_dict;

        IF @dcnt = 0 RETURN N'1=1';
        IF @dcnt = 1 RETURN @dparts;
        RETURN N'(' + @dparts + N')';
    END;

    -- ---- ListItem accessor (list_item_prop set): Status.Value / .Alias / .Id
    --      Emits EXISTS subquery against _values [JOIN _list_items] instead of
    --      a plain column comparison so that it works in both Shape A and C.
    DECLARE @li_prop_val  NVARCHAR(50) = JSON_VALUE(@field_meta, N'$.list_item_prop');
    DECLARE @li_is_array  BIT = CASE WHEN JSON_VALUE(@field_meta, N'$.is_array') = N'true' THEN 1 ELSE 0 END;

    IF @li_prop_val IS NOT NULL AND @sid IS NOT NULL
    BEGIN
        DECLARE @li_join     NVARCHAR(MAX) = N'';
        DECLARE @li_cmp_col  NVARCHAR(100);
        DECLARE @li_idx_cond NVARCHAR(60) = CASE WHEN @li_is_array = 1
            THEN N' AND av._array_index IS NOT NULL'
            ELSE N' AND av._array_index IS NULL' END;

        IF @li_prop_val = N'Id'
        BEGIN
            -- No join; compare the raw list-item foreign key (BIGINT)
            SET @li_join    = N'';
            SET @li_cmp_col = N'av.[_ListItem]';
        END
        ELSE IF @li_prop_val = N'Value'
        BEGIN
            SET @li_join    = N' JOIN dbo._list_items li ON li._id = av.[_ListItem]';
            SET @li_cmp_col = N'li.[_value]';
        END
        ELSE  -- Alias
        BEGIN
            SET @li_join    = N' JOIN dbo._list_items li ON li._id = av.[_ListItem]';
            SET @li_cmp_col = N'li.[_alias]';
        END;

        DECLARE @li_pfx NVARCHAR(MAX) =
            N'EXISTS (SELECT 1 FROM dbo._values av' + @li_join
            + N' WHERE av._id_object = ' + @obj_alias + N'.[_id]'
            + N' AND av._id_structure = ' + @sid
            + @li_idx_cond;

        -- Scalar shorthand: equality
        IF @op_type <> 5
        BEGIN
            IF @op_type = 0
                RETURN @li_pfx + N' AND ' + @li_cmp_col + N' IS NULL)';
            RETURN @li_pfx + N' AND ' + @li_cmp_col + N' = '
                + dbo.pvt_jsonb_to_sql_literal(@op_json, @op_type) + N')';
        END;

        -- Operator object
        DECLARE @li_parts NVARCHAR(MAX) = N'';
        DECLARE @li_cnt   INT = 0;
        DECLARE c_li CURSOR LOCAL FAST_FORWARD FOR
            SELECT [key], [value], [type] FROM OPENJSON(@op_json);
        DECLARE @li_ok NVARCHAR(50), @li_ov NVARCHAR(MAX), @li_ot INT;
        OPEN c_li;
        FETCH NEXT FROM c_li INTO @li_ok, @li_ov, @li_ot;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            DECLARE @li_on    NVARCHAR(50) = LOWER(@li_ok);
            DECLARE @li_piece NVARCHAR(MAX) = NULL;

            IF @li_on IN (N'$eq', N'$ne', N'$gt', N'$gte', N'$lt', N'$lte')
            BEGIN
                DECLARE @li_cmp NVARCHAR(4) = CASE @li_on
                    WHEN N'$eq'  THEN N'='   WHEN N'$ne'  THEN N'<>'
                    WHEN N'$gt'  THEN N'>'   WHEN N'$gte' THEN N'>='
                    WHEN N'$lt'  THEN N'<'   WHEN N'$lte' THEN N'<=' END;
                IF @li_ot = 0
                    SET @li_piece = CASE @li_on
                        WHEN N'$eq' THEN @li_pfx + N' AND ' + @li_cmp_col + N' IS NULL)'
                        WHEN N'$ne' THEN @li_pfx + N' AND ' + @li_cmp_col + N' IS NOT NULL)'
                        ELSE @li_pfx + N' AND ' + @li_cmp_col + N' ' + @li_cmp + N' NULL)' END;
                ELSE
                    SET @li_piece = @li_pfx + N' AND ' + @li_cmp_col + N' ' + @li_cmp + N' '
                        + dbo.pvt_jsonb_to_sql_literal(@li_ov, @li_ot) + N')';
            END
            ELSE IF @li_on IN (N'$in', N'$nin')
            BEGIN
                IF @li_ot <> 4
                    SET @li_piece = N'/*invalid-li-in*/1=0';
                ELSE
                BEGIN
                    DECLARE @li_lst NVARCHAR(MAX) = N'';
                    DECLARE c_li_in CURSOR LOCAL FAST_FORWARD FOR
                        SELECT [value], [type] FROM OPENJSON(@li_ov);
                    DECLARE @li_iv NVARCHAR(MAX), @li_it INT;
                    OPEN c_li_in;
                    FETCH NEXT FROM c_li_in INTO @li_iv, @li_it;
                    WHILE @@FETCH_STATUS = 0
                    BEGIN
                        IF @li_lst <> N'' SET @li_lst += N', ';
                        SET @li_lst += dbo.pvt_jsonb_to_sql_literal(@li_iv, @li_it);
                        FETCH NEXT FROM c_li_in INTO @li_iv, @li_it;
                    END;
                    CLOSE c_li_in; DEALLOCATE c_li_in;
                    IF @li_lst = N'' SET @li_lst = N'NULL';
                    -- $nin: NOT EXISTS any matching element
                    SET @li_piece = CASE WHEN @li_on = N'$nin' THEN N'NOT ' ELSE N'' END
                        + @li_pfx + N' AND ' + @li_cmp_col + N' IN (' + @li_lst + N'))';
                END;
            END
            ELSE IF @li_on IN (N'$null', N'$isnull')
                SET @li_piece = CASE WHEN LOWER(@li_ov) = N'true'
                    THEN N'NOT ' + @li_pfx + N')'
                    ELSE @li_pfx + N')' END;
            ELSE IF @li_on IN (N'$notnull', N'$exists')
                SET @li_piece = CASE WHEN LOWER(@li_ov) = N'true'
                    THEN @li_pfx + N')'
                    ELSE N'NOT ' + @li_pfx + N')' END;
            ELSE IF @li_on = N'$like'
                SET @li_piece = @li_pfx + N' AND ' + @li_cmp_col + N' LIKE '
                    + dbo.pvt_sql_string_literal(@li_ov) + N')';
            ELSE IF @li_on = N'$contains'
                SET @li_piece = @li_pfx + N' AND ' + @li_cmp_col + N' LIKE '
                    + dbo.pvt_sql_string_literal(N'%' + @li_ov + N'%') + N')';
            ELSE IF @li_on = N'$startswith'
                SET @li_piece = @li_pfx + N' AND ' + @li_cmp_col + N' LIKE '
                    + dbo.pvt_sql_string_literal(@li_ov + N'%') + N')';
            -- Array ops on ListItem-array accessor (parity with PG `= ANY(arr)`):
            -- the @li_pfx already restricts to array elements when @li_is_array=1,
            -- so $arrayContains is just an equality on the per-element column.
            ELSE IF @li_on = N'$arraycontains' AND @li_is_array = 1
                SET @li_piece = @li_pfx + N' AND ' + @li_cmp_col + N' = '
                    + dbo.pvt_jsonb_to_sql_literal(@li_ov, @li_ot) + N')';
            ELSE IF @li_on IN (N'$arrayany', N'$arrayempty') AND @li_is_array = 1
            BEGIN
                DECLARE @li_want BIT = CASE
                    WHEN @li_on = N'$arrayany'   AND LOWER(@li_ov) = N'true'  THEN 1
                    WHEN @li_on = N'$arrayany'   AND LOWER(@li_ov) = N'false' THEN 0
                    WHEN @li_on = N'$arrayempty' AND LOWER(@li_ov) = N'true'  THEN 0
                    WHEN @li_on = N'$arrayempty' AND LOWER(@li_ov) = N'false' THEN 1
                    ELSE 1 END;
                SET @li_piece = CASE WHEN @li_want = 1 THEN @li_pfx + N')' ELSE N'NOT ' + @li_pfx + N')' END;
            END
            ELSE
                SET @li_piece = N'/*unsupported-li-op:' + @li_ok + N'*/1=0';

            IF @li_piece IS NOT NULL
            BEGIN
                IF @li_cnt > 0 SET @li_parts += N' AND ';
                SET @li_parts += @li_piece;
                SET @li_cnt += 1;
            END;
            FETCH NEXT FROM c_li INTO @li_ok, @li_ov, @li_ot;
        END;
        CLOSE c_li; DEALLOCATE c_li;

        IF @li_cnt = 0 RETURN N'1=1';
        IF @li_cnt = 1 RETURN @li_parts;
        RETURN N'(' + @li_parts + N')';
    END;

    -- Scalar-literal shorthand
    IF @op_type <> 5
    BEGIN
        IF @op_type = 0
            RETURN @col_name + N' IS NULL';
        RETURN @col_name + N' = ' + dbo.pvt_jsonb_to_sql_literal(@op_json, @op_type);
    END;

    -- Walk operator object: collect AND-joined parts
    DECLARE @parts NVARCHAR(MAX) = N'';
    DECLARE @cnt INT = 0;

    DECLARE c CURSOR LOCAL FAST_FORWARD FOR
        SELECT [key], [value], [type] FROM OPENJSON(@op_json);
    DECLARE @opk NVARCHAR(50), @opv NVARCHAR(MAX), @opt INT;
    OPEN c;
    FETCH NEXT FROM c INTO @opk, @opv, @opt;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        DECLARE @on NVARCHAR(50) = LOWER(@opk);
        DECLARE @piece NVARCHAR(MAX) = NULL;

        IF @on IN (N'$eq', N'$ne', N'$gt', N'$gte', N'$lt', N'$lte')
        BEGIN
            DECLARE @cmp NVARCHAR(4) =
                CASE @on WHEN N'$eq' THEN N'=' WHEN N'$ne' THEN N'<>'
                         WHEN N'$gt' THEN N'>' WHEN N'$gte' THEN N'>='
                         WHEN N'$lt' THEN N'<' WHEN N'$lte' THEN N'<=' END;
            IF @opt = 0  -- null
                SET @piece = CASE @on WHEN N'$eq' THEN @col_name + N' IS NULL'
                                      WHEN N'$ne' THEN @col_name + N' IS NOT NULL'
                                      ELSE @col_name + N' ' + @cmp + N' NULL' END;
            ELSE
                SET @piece = @col_name + N' ' + @cmp + N' ' + dbo.pvt_jsonb_to_sql_literal(@opv, @opt);
        END
        ELSE IF @on IN (N'$in', N'$nin')
        BEGIN
            IF @opt <> 4 SET @piece = N'/*invalid-in*/1=0';
            ELSE
            BEGIN
                DECLARE @lst NVARCHAR(MAX) = N'';
                DECLARE c_in CURSOR LOCAL FAST_FORWARD FOR
                    SELECT [value], [type] FROM OPENJSON(@opv);
                DECLARE @iv NVARCHAR(MAX), @it INT;
                OPEN c_in;
                FETCH NEXT FROM c_in INTO @iv, @it;
                WHILE @@FETCH_STATUS = 0
                BEGIN
                    IF @lst <> N'' SET @lst = @lst + N', ';
                    SET @lst = @lst + dbo.pvt_jsonb_to_sql_literal(@iv, @it);
                    FETCH NEXT FROM c_in INTO @iv, @it;
                END;
                CLOSE c_in; DEALLOCATE c_in;
                IF @lst = N'' SET @lst = N'NULL';
                SET @piece = @col_name + CASE WHEN @on = N'$in' THEN N' IN (' ELSE N' NOT IN (' END + @lst + N')';
            END;
        END
        ELSE IF @on = N'$like'
            SET @piece = @col_name + N' LIKE ' + dbo.pvt_sql_string_literal(@opv);
        ELSE IF @on = N'$startswith'
            SET @piece = @col_name + N' LIKE ' + dbo.pvt_sql_string_literal(@opv + N'%');
        ELSE IF @on = N'$endswith'
            SET @piece = @col_name + N' LIKE ' + dbo.pvt_sql_string_literal(N'%' + @opv);
        ELSE IF @on = N'$contains'
            SET @piece = @col_name + N' LIKE ' + dbo.pvt_sql_string_literal(N'%' + @opv + N'%');
        -- T-SQL has no ILIKE; use COLLATE for case-insensitive matching.
        ELSE IF @on = N'$startswithignorecase'
            SET @piece = N'LOWER(' + @col_name + N') LIKE ' + dbo.pvt_sql_string_literal(LOWER(@opv) + N'%');
        ELSE IF @on = N'$endswithignorecase'
            SET @piece = N'LOWER(' + @col_name + N') LIKE ' + dbo.pvt_sql_string_literal(N'%' + LOWER(@opv));
        ELSE IF @on = N'$containsignorecase'
            SET @piece = N'LOWER(' + @col_name + N') LIKE ' + dbo.pvt_sql_string_literal(N'%' + LOWER(@opv) + N'%');
        ELSE IF @on IN (N'$null', N'$isnull')
            SET @piece = CASE WHEN @opv = N'true' THEN @col_name + N' IS NULL' ELSE @col_name + N' IS NOT NULL' END;
        ELSE IF @on IN (N'$notnull', N'$exists')
            SET @piece = CASE WHEN @opv = N'true' THEN @col_name + N' IS NOT NULL' ELSE @col_name + N' IS NULL' END;

        -- ---- Array operators ($arrayContains / $arrayAny / etc.) -------
        -- These emit EXISTS / COUNT subqueries against dbo._values using the
        -- structure id (sid) from the field meta.  They require kind='field'
        -- and a valid sid; for base fields they fall through to unsupported.
        ELSE IF @on = N'$arraycontains'
        BEGIN
            IF @sid IS NULL
                SET @piece = N'/*no-sid-$arrayContains*/1=0';
            ELSE
            BEGIN
                DECLARE @ac_cmp NVARCHAR(MAX) = CASE @db_type_f
                    WHEN N'Long'           THEN N'av.[_Long] = CAST(' + dbo.pvt_jsonb_to_sql_literal(@opv, @opt) + N' AS BIGINT)'
                    WHEN N'Double'         THEN N'av.[_Double] = CAST(' + dbo.pvt_jsonb_to_sql_literal(@opv, @opt) + N' AS FLOAT)'
                    WHEN N'Numeric'        THEN N'av.[_Numeric] = CAST(' + dbo.pvt_jsonb_to_sql_literal(@opv, @opt) + N' AS DECIMAL(28,10))'
                    WHEN N'Boolean'        THEN N'av.[_Boolean] = ' + dbo.pvt_jsonb_to_sql_literal(@opv, @opt)
                    WHEN N'DateTimeOffset' THEN N'av.[_DateTimeOffset] = CAST(' + dbo.pvt_jsonb_to_sql_literal(@opv, @opt) + N' AS DATETIMEOFFSET)'
                    ELSE                        N'av.[_String] = ' + dbo.pvt_jsonb_to_sql_literal(@opv, @opt)
                END;
                SET @piece = N'EXISTS (SELECT 1 FROM dbo._values av'
                    + N' WHERE av._id_object = ' + @obj_alias + N'.[_id]'
                    + N' AND av._id_structure = ' + @sid
                    + N' AND av._array_index IS NOT NULL'
                    + N' AND ' + @ac_cmp + N')';
            END;
        END
        ELSE IF @on IN (N'$arrayany', N'$arrayempty')
        BEGIN
            IF @sid IS NULL
                SET @piece = N'/*no-sid-' + @opk + N'*/1=0';
            ELSE
            BEGIN
                DECLARE @aae_base NVARCHAR(MAX) =
                    N'EXISTS (SELECT 1 FROM dbo._values av'
                    + N' WHERE av._id_object = ' + @obj_alias + N'.[_id]'
                    + N' AND av._id_structure = ' + @sid
                    + N' AND av._array_index IS NOT NULL)';
                -- $arrayAny:   true  -> EXISTS,     false -> NOT EXISTS
                -- $arrayEmpty: true  -> NOT EXISTS,  false -> EXISTS
                DECLARE @aae_want BIT = CASE
                    WHEN @on = N'$arrayany'   AND LOWER(@opv) = N'true'  THEN 1
                    WHEN @on = N'$arrayany'   AND LOWER(@opv) = N'false' THEN 0
                    WHEN @on = N'$arrayempty' AND LOWER(@opv) = N'true'  THEN 0
                    WHEN @on = N'$arrayempty' AND LOWER(@opv) = N'false' THEN 1
                    ELSE 1
                END;
                SET @piece = CASE WHEN @aae_want = 1 THEN @aae_base ELSE N'NOT ' + @aae_base END;
            END;
        END
        ELSE IF @on IN (N'$arraycount', N'$arraycountgt', N'$arraycountgte', N'$arraycountlt', N'$arraycountle')
        BEGIN
            IF @sid IS NULL
                SET @piece = N'/*no-sid-' + @opk + N'*/1=0';
            ELSE
            BEGIN
                DECLARE @acc_cnt NVARCHAR(MAX) =
                    N'(SELECT COUNT(*) FROM dbo._values av'
                    + N' WHERE av._id_object = ' + @obj_alias + N'.[_id]'
                    + N' AND av._id_structure = ' + @sid
                    + N' AND av._array_index IS NOT NULL)';
                DECLARE @acc_val NVARCHAR(20) = ISNULL(CAST(TRY_CAST(@opv AS BIGINT) AS NVARCHAR(20)), N'0');
                DECLARE @acc_op  NVARCHAR(3)  = CASE @on
                    WHEN N'$arraycount'    THEN N'='
                    WHEN N'$arraycountgt'  THEN N'>'
                    WHEN N'$arraycountgte' THEN N'>='
                    WHEN N'$arraycountlt'  THEN N'<'
                    WHEN N'$arraycountle'  THEN N'<='
                    ELSE N'='
                END;
                SET @piece = @acc_cnt + N' ' + @acc_op + N' ' + @acc_val;
            END;
        END
        ELSE IF @on = N'$arrayat'
        BEGIN
            IF @sid IS NULL
                SET @piece = N'/*no-sid-$arrayAt*/1=0';
            ELSE
                -- _array_index is NVARCHAR; the given index value is cast to string
                SET @piece = N'EXISTS (SELECT 1 FROM dbo._values av'
                    + N' WHERE av._id_object = ' + @obj_alias + N'.[_id]'
                    + N' AND av._id_structure = ' + @sid
                    + N' AND av._array_index = N''' + REPLACE(ISNULL(CAST(TRY_CAST(@opv AS BIGINT) AS NVARCHAR(20)), N'0'), N'''', N'''''') + N''')';
        END
        ELSE IF @on IN (N'$arrayfirst', N'$arraylast')
        BEGIN
            IF @sid IS NULL
                SET @piece = N'/*no-sid-' + @opk + N'*/1=0';
            ELSE
            BEGIN
                DECLARE @afl_cmp NVARCHAR(MAX) = CASE @db_type_f
                    WHEN N'Long'           THEN N'av.[_Long] = CAST(' + dbo.pvt_jsonb_to_sql_literal(@opv, @opt) + N' AS BIGINT)'
                    WHEN N'Double'         THEN N'av.[_Double] = CAST(' + dbo.pvt_jsonb_to_sql_literal(@opv, @opt) + N' AS FLOAT)'
                    WHEN N'Numeric'        THEN N'av.[_Numeric] = CAST(' + dbo.pvt_jsonb_to_sql_literal(@opv, @opt) + N' AS DECIMAL(28,10))'
                    WHEN N'Boolean'        THEN N'av.[_Boolean] = ' + dbo.pvt_jsonb_to_sql_literal(@opv, @opt)
                    WHEN N'DateTimeOffset' THEN N'av.[_DateTimeOffset] = CAST(' + dbo.pvt_jsonb_to_sql_literal(@opv, @opt) + N' AS DATETIMEOFFSET)'
                    ELSE                        N'av.[_String] = ' + dbo.pvt_jsonb_to_sql_literal(@opv, @opt)
                END;
                DECLARE @afl_idx NVARCHAR(MAX) = CASE @on
                    WHEN N'$arrayfirst' THEN N'N''0'''
                    ELSE N'(SELECT TOP 1 CAST(av2._array_index AS NVARCHAR(20))'
                        + N' FROM dbo._values av2'
                        + N' WHERE av2._id_object = ' + @obj_alias + N'.[_id]'
                        + N' AND av2._id_structure = ' + @sid
                        + N' AND av2._array_index IS NOT NULL'
                        + N' ORDER BY TRY_CAST(av2._array_index AS INT) DESC)'
                END;
                SET @piece = N'EXISTS (SELECT 1 FROM dbo._values av'
                    + N' WHERE av._id_object = ' + @obj_alias + N'.[_id]'
                    + N' AND av._id_structure = ' + @sid
                    + N' AND av._array_index = ' + @afl_idx
                    + N' AND ' + @afl_cmp + N')';
            END;
        END
        ELSE IF @on = N'$arraystartswith'
        BEGIN
            IF @sid IS NULL
                SET @piece = N'/*no-sid-$arrayStartsWith*/1=0';
            ELSE
                SET @piece = N'EXISTS (SELECT 1 FROM dbo._values av'
                    + N' WHERE av._id_object = ' + @obj_alias + N'.[_id]'
                    + N' AND av._id_structure = ' + @sid
                    + N' AND av._array_index IS NOT NULL'
                    + N' AND av.[_String] LIKE ' + dbo.pvt_sql_string_literal(@opv + N'%') + N')';
        END
        ELSE IF @on = N'$arrayendswith'
        BEGIN
            IF @sid IS NULL
                SET @piece = N'/*no-sid-$arrayEndsWith*/1=0';
            ELSE
                SET @piece = N'EXISTS (SELECT 1 FROM dbo._values av'
                    + N' WHERE av._id_object = ' + @obj_alias + N'.[_id]'
                    + N' AND av._id_structure = ' + @sid
                    + N' AND av._array_index IS NOT NULL'
                    + N' AND av.[_String] LIKE ' + dbo.pvt_sql_string_literal(N'%' + @opv) + N')';
        END
        -- ---- $arrayMatches: LIKE pattern against array element strings ----
        ELSE IF @on = N'$arraymatches'
        BEGIN
            IF @sid IS NULL
                SET @piece = N'/*no-sid-$arrayMatches*/1=0';
            ELSE
                SET @piece = N'EXISTS (SELECT 1 FROM dbo._values av'
                    + N' WHERE av._id_object = ' + @obj_alias + N'.[_id]'
                    + N' AND av._id_structure = ' + @sid
                    + N' AND av._array_index IS NOT NULL'
                    + N' AND av.[_String] LIKE ' + dbo.pvt_sql_string_literal(@opv) + N')';
        END
        -- ---- $arraySum / $arrayMin / $arrayMax / $arrayAvg ---------------
        -- Scalar operand: shorthand for = @value.
        -- Object operand: comparison operators ($eq/$ne/$gt/$gte/$lt/$lte).
        ELSE IF @on IN (N'$arraysum', N'$arraymin', N'$arraymax', N'$arrayavg')
        BEGIN
            IF @sid IS NULL
                SET @piece = N'/*no-sid-' + @opk + N'*/1=0';
            ELSE
            BEGIN
                DECLARE @agg_fn   NVARCHAR(10)  = CASE @on
                    WHEN N'$arraysum' THEN N'SUM'
                    WHEN N'$arraymin' THEN N'MIN'
                    WHEN N'$arraymax' THEN N'MAX'
                    WHEN N'$arrayavg' THEN N'AVG'
                    ELSE N'SUM' END;
                DECLARE @agg_col  NVARCHAR(100) = CASE @db_type_f
                    WHEN N'Long'     THEN N'av.[_Long]'
                    WHEN N'Double'   THEN N'av.[_Double]'
                    WHEN N'Numeric'  THEN N'av.[_Numeric]'
                    ELSE N'CAST(av.[_String] AS DECIMAL(28,10))' END;
                DECLARE @agg_sub  NVARCHAR(MAX) =
                    N'(SELECT ' + @agg_fn + N'(' + @agg_col + N') FROM dbo._values av'
                    + N' WHERE av._id_object = ' + @obj_alias + N'.[_id]'
                    + N' AND av._id_structure = ' + @sid
                    + N' AND av._array_index IS NOT NULL)';
                -- Scalar shorthand: equality check
                IF @opt <> 5
                    SET @piece = @agg_sub + N' = ' + dbo.pvt_jsonb_to_sql_literal(@opv, @opt);
                ELSE
                BEGIN
                    -- Operator object: walk and AND-join comparison ops
                    DECLARE @ag_parts NVARCHAR(MAX) = N'';
                    DECLARE @ag_cnt   INT = 0;
                    DECLARE c_ag CURSOR LOCAL FAST_FORWARD FOR
                        SELECT [key], [value], [type] FROM OPENJSON(@opv);
                    DECLARE @ag_ok NVARCHAR(50), @ag_ov NVARCHAR(MAX), @ag_ot INT;
                    OPEN c_ag;
                    FETCH NEXT FROM c_ag INTO @ag_ok, @ag_ov, @ag_ot;
                    WHILE @@FETCH_STATUS = 0
                    BEGIN
                        DECLARE @ag_cmp NVARCHAR(4) = CASE LOWER(@ag_ok)
                            WHEN N'$eq'  THEN N'='   WHEN N'$ne'  THEN N'<>'
                            WHEN N'$gt'  THEN N'>'   WHEN N'$gte' THEN N'>='
                            WHEN N'$lt'  THEN N'<'   WHEN N'$lte' THEN N'<='
                            ELSE NULL END;
                        IF @ag_cmp IS NOT NULL
                        BEGIN
                            IF @ag_cnt > 0 SET @ag_parts += N' AND ';
                            SET @ag_parts += @agg_sub + N' ' + @ag_cmp + N' '
                                + dbo.pvt_jsonb_to_sql_literal(@ag_ov, @ag_ot);
                            SET @ag_cnt += 1;
                        END;
                        FETCH NEXT FROM c_ag INTO @ag_ok, @ag_ov, @ag_ot;
                    END;
                    CLOSE c_ag; DEALLOCATE c_ag;
                    SET @piece = CASE
                        WHEN @ag_cnt = 0 THEN N'1=1'
                        WHEN @ag_cnt = 1 THEN @ag_parts
                        ELSE N'(' + @ag_parts + N')' END;
                END;
            END;
        END
        ELSE
            SET @piece = N'/*unsupported-op:' + @opk + N'*/1=0';

        IF @piece IS NOT NULL
        BEGIN
            IF @cnt > 0 SET @parts = @parts + N' AND ';
            SET @parts = @parts + @piece;
            SET @cnt = @cnt + 1;
        END;
        FETCH NEXT FROM c INTO @opk, @opv, @opt;
    END;
    CLOSE c; DEALLOCATE c;

    IF @cnt = 0 RETURN N'1=1';
    IF @cnt = 1 RETURN @parts;
    RETURN N'(' + @parts + N')';
END;
GO


-- ===== 14_pvt_where.sql =====
-- =====================================================================
-- 14_pvt_where.sql  (MSSql v2-pvt) — recursive filter walker
-- ---------------------------------------------------------------------
-- Build a single WHERE expression from filter JSON.
--
-- Grammar (slice):
--   { "$and": [ ... ] }              -> ( ... AND ... )
--   { "$or":  [ ... ] }              -> ( ... OR  ... )
--   { "$not": { ... } }              -> NOT ( ... )
--   { "$isRoot": true }              -> alias._id_parent IS NULL
--   { "$isLeaf": true }              -> NOT EXISTS (children)
--   { "$childrenOf": N }             -> alias._id_parent = N
--   { "$level": N }                  -> dbo.pvt_object_depth(alias._id) = N
--   { "$level": {"$gt":N} }          -> dbo.pvt_object_depth(alias._id) > N
--   { "$hasAncestor": N }            -> dbo.pvt_is_descendant_of(alias._id, N) = 1
--   { "$hasDescendant": N }          -> dbo.pvt_is_descendant_of(N, alias._id) = 1
--   { "Field.$length": {op} }        -> EXISTS LENGTH(fv._String) op N
--   { "Field[].$count": {op} }       -> (SELECT COUNT(*) ...) op N
--   { "<field>": <op_json> }         -> AND of per-field predicates
--   { "$gt": [expr, expr] }          -> B2-expr comparison (see pvt_b2_expr_sql)
--   { "$between": [expr, lo, hi] }   -> B2-expr range check
--   { "$in": [expr, [v1,...]] }      -> B2-expr IN list
--   { "$null": expr }                -> expr IS NULL
--   { "$contains": [expr, str] }     -> expr LIKE '%str%'
-- =====================================================================

SET QUOTED_IDENTIFIER ON;
GO

-- =====================================================================
-- pvt_b2_expr_sql — translate one B2 expression node to a T-SQL fragment
-- =====================================================================
-- expr:      JSON node e.g. {"$field":"Age"} or {"$add":[...]}
-- fields:    field metadata from pvt_collect_fields
-- obj_alias: 'o' (Shape A) or '_pvt_cte' (Shape C)
-- Handles: $field, $const, $gt/$gte/$lt/$lte/$eq/$ne,
--          $add/$sub/$mul, $abs, $power, $upper, $concat,
--          $coalesce, $length, $year, $trimStart,
--          $substring, $replace, $dateAdd, $if
CREATE OR ALTER FUNCTION dbo.pvt_b2_expr_sql(
    @expr      NVARCHAR(MAX),
    @fields    NVARCHAR(MAX),
    @obj_alias NVARCHAR(50)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @expr IS NULL OR ISJSON(@expr) = 0
        RETURN N'NULL';

    DECLARE @nk NVARCHAR(100), @nv NVARCHAR(MAX), @nt INT;
    SELECT TOP 1 @nk = LOWER([key]), @nv = [value], @nt = [type]
      FROM OPENJSON(@expr);
    IF @nk IS NULL RETURN N'NULL';

    -- ---- Leaf nodes --------------------------------------------------
    IF @nk = N'$field'
    BEGIN
        DECLARE @b2_fm NVARCHAR(MAX) = JSON_QUERY(@fields, N'$."' + STRING_ESCAPE(@nv, 'json') + N'"');
        IF @b2_fm IS NULL RETURN N'/*unknown-b2-field:' + @nv + N'*/NULL';
        IF JSON_VALUE(@b2_fm, N'$.kind') = N'base'
            RETURN @obj_alias + N'.' + QUOTENAME(JSON_VALUE(@b2_fm, N'$.column'));
        RETURN QUOTENAME(@nv);  -- props field resolved as CTE column in Shape C
    END;

    IF @nk = N'$const'
        RETURN dbo.pvt_jsonb_to_sql_literal(@nv, @nt);

    -- ---- Comparison operators (used in $if conditions) ---------------
    IF @nk IN (N'$gt', N'$gte', N'$lt', N'$lte', N'$eq', N'$ne')
    BEGIN
        DECLARE @cmp_1 NVARCHAR(MAX), @cmp_2 NVARCHAR(MAX);
        SELECT @cmp_1 = MIN(CASE WHEN [key] = '0' THEN [value] END),
               @cmp_2 = MIN(CASE WHEN [key] = '1' THEN [value] END)
          FROM OPENJSON(@nv);
        DECLARE @cmp_op NVARCHAR(4) = CASE @nk
            WHEN N'$gt'  THEN N'>'   WHEN N'$gte' THEN N'>='
            WHEN N'$lt'  THEN N'<'   WHEN N'$lte' THEN N'<='
            WHEN N'$eq'  THEN N'='   WHEN N'$ne'  THEN N'<>' END;
        DECLARE @cmp_l NVARCHAR(MAX) = dbo.pvt_b2_expr_sql(@cmp_1, @fields, @obj_alias);
        DECLARE @cmp_r NVARCHAR(MAX) = dbo.pvt_b2_expr_sql(@cmp_2, @fields, @obj_alias);
        -- Null-aware rewrite for $eq / $ne (parity with PG IS [NOT] DISTINCT FROM)
        -- For ordering ops vs NULL the result is UNKNOWN in T-SQL anyway, but at
        -- least don't emit `expr = NULL` / `expr <> NULL` which silently never match.
        IF @nk IN (N'$eq', N'$ne')
        BEGIN
            IF @cmp_r = N'NULL' AND @cmp_l <> N'NULL'
                RETURN @cmp_l + CASE WHEN @nk = N'$eq' THEN N' IS NULL' ELSE N' IS NOT NULL' END;
            IF @cmp_l = N'NULL' AND @cmp_r <> N'NULL'
                RETURN @cmp_r + CASE WHEN @nk = N'$eq' THEN N' IS NULL' ELSE N' IS NOT NULL' END;
            IF @cmp_l = N'NULL' AND @cmp_r = N'NULL'
                RETURN CASE WHEN @nk = N'$eq' THEN N'1=1' ELSE N'1=0' END;
        END;
        RETURN @cmp_l + N' ' + @cmp_op + N' ' + @cmp_r;
    END;

    -- ---- Arithmetic --------------------------------------------------
    IF @nk IN (N'$add', N'$sub', N'$mul', N'$div', N'$mod')
    BEGIN
        DECLARE @bx_1 NVARCHAR(MAX), @bx_2 NVARCHAR(MAX);
        SELECT @bx_1 = MIN(CASE WHEN [key] = '0' THEN [value] END),
               @bx_2 = MIN(CASE WHEN [key] = '1' THEN [value] END)
          FROM OPENJSON(@nv);
        DECLARE @bx_op NVARCHAR(3) = CASE @nk
            WHEN N'$add' THEN N'+' WHEN N'$sub' THEN N'-'
            WHEN N'$mul' THEN N'*' WHEN N'$div' THEN N'/'
            ELSE N'%' END;
        RETURN N'(' + dbo.pvt_b2_expr_sql(@bx_1, @fields, @obj_alias)
             + N' ' + @bx_op + N' '
             + dbo.pvt_b2_expr_sql(@bx_2, @fields, @obj_alias) + N')';
    END;

    -- ---- Unary functions (single-arg, array or object form) ----------
    -- Accept { $op: expr } or { $op: [expr] } like PG.
    IF @nk IN (N'$abs', N'$neg', N'$floor', N'$ceil', N'$lower',
               N'$trim', N'$trimend',
               N'$sqrt', N'$sign', N'$exp', N'$ln', N'$log10')
    BEGIN
        DECLARE @un_a NVARCHAR(MAX);
        IF @nt = 4 SELECT TOP 1 @un_a = [value] FROM OPENJSON(@nv);
        ELSE SET @un_a = @nv;
        DECLARE @un_x NVARCHAR(MAX) = dbo.pvt_b2_expr_sql(@un_a, @fields, @obj_alias);
        IF @nk = N'$abs'    RETURN N'ABS('             + @un_x + N')';
        IF @nk = N'$neg'    RETURN N'(-'              + @un_x + N')';
        IF @nk = N'$floor'  RETURN N'FLOOR('           + @un_x + N')';
        IF @nk = N'$ceil'   RETURN N'CEILING('         + @un_x + N')';
        IF @nk = N'$lower'  RETURN N'LOWER('           + @un_x + N')';
        IF @nk = N'$trim'   RETURN N'LTRIM(RTRIM('     + @un_x + N'))';
        IF @nk = N'$trimend' RETURN N'RTRIM('          + @un_x + N')';
        IF @nk = N'$sqrt'   RETURN N'SQRT(CAST('       + @un_x + N' AS FLOAT))';
        IF @nk = N'$sign'   RETURN N'SIGN('            + @un_x + N')';
        IF @nk = N'$exp'    RETURN N'EXP(CAST('        + @un_x + N' AS FLOAT))';
        IF @nk = N'$ln'     RETURN N'LOG(CAST('        + @un_x + N' AS FLOAT))';
        IF @nk = N'$log10'  RETURN N'LOG10(CAST('      + @un_x + N' AS FLOAT))';
    END;

    -- ---- $min / $max: n-ary LEAST / GREATEST (SQL Server 2022+) -----
    IF @nk IN (N'$min', N'$max')
    BEGIN
        DECLARE @mm_parts NVARCHAR(MAX) = N'';
        DECLARE c_mm CURSOR LOCAL FAST_FORWARD FOR
            SELECT [value] FROM OPENJSON(@nv) ORDER BY CAST([key] AS INT);
        DECLARE @mm_v NVARCHAR(MAX);
        OPEN c_mm;
        FETCH NEXT FROM c_mm INTO @mm_v;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            IF @mm_parts <> N'' SET @mm_parts += N', ';
            SET @mm_parts += dbo.pvt_b2_expr_sql(@mm_v, @fields, @obj_alias);
            FETCH NEXT FROM c_mm INTO @mm_v;
        END;
        CLOSE c_mm; DEALLOCATE c_mm;
        RETURN CASE WHEN @nk = N'$min'
            THEN N'LEAST('    + @mm_parts + N')'
            ELSE N'GREATEST(' + @mm_parts + N')' END;
    END;

    IF @nk = N'$power'
    BEGIN
        DECLARE @pw_1 NVARCHAR(MAX), @pw_2 NVARCHAR(MAX);
        SELECT @pw_1 = MIN(CASE WHEN [key] = '0' THEN [value] END),
               @pw_2 = MIN(CASE WHEN [key] = '1' THEN [value] END)
          FROM OPENJSON(@nv);
        RETURN N'POWER(CAST(' + dbo.pvt_b2_expr_sql(@pw_1, @fields, @obj_alias) + N' AS FLOAT),'
             + dbo.pvt_b2_expr_sql(@pw_2, @fields, @obj_alias) + N')';
    END;

    -- ---- String / general functions ----------------------------------
    IF @nk = N'$upper'
    BEGIN
        DECLARE @up_a NVARCHAR(MAX);
        IF @nt = 4 SELECT TOP 1 @up_a = [value] FROM OPENJSON(@nv);
        ELSE SET @up_a = @nv;
        RETURN N'UPPER(' + dbo.pvt_b2_expr_sql(@up_a, @fields, @obj_alias) + N')';
    END;

    IF @nk = N'$concat'
    BEGIN
        DECLARE @cc_parts NVARCHAR(MAX) = N'';
        DECLARE c_cc CURSOR LOCAL FAST_FORWARD FOR
            SELECT [value] FROM OPENJSON(@nv) ORDER BY CAST([key] AS INT);
        DECLARE @cc_v NVARCHAR(MAX);
        OPEN c_cc;
        FETCH NEXT FROM c_cc INTO @cc_v;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            IF @cc_parts <> N'' SET @cc_parts += N' + ';
            SET @cc_parts += N'CAST(' + dbo.pvt_b2_expr_sql(@cc_v, @fields, @obj_alias) + N' AS NVARCHAR(MAX))';
            FETCH NEXT FROM c_cc INTO @cc_v;
        END;
        CLOSE c_cc; DEALLOCATE c_cc;
        RETURN CASE WHEN @cc_parts = N'' THEN N'N'''' ' ELSE @cc_parts END;
    END;

    IF @nk = N'$coalesce'
    BEGIN
        DECLARE @co_parts NVARCHAR(MAX) = N'';
        DECLARE c_co CURSOR LOCAL FAST_FORWARD FOR
            SELECT [value] FROM OPENJSON(@nv) ORDER BY CAST([key] AS INT);
        DECLARE @co_v NVARCHAR(MAX);
        OPEN c_co;
        FETCH NEXT FROM c_co INTO @co_v;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            IF @co_parts <> N'' SET @co_parts += N', ';
            SET @co_parts += dbo.pvt_b2_expr_sql(@co_v, @fields, @obj_alias);
            FETCH NEXT FROM c_co INTO @co_v;
        END;
        CLOSE c_co; DEALLOCATE c_co;
        RETURN CASE WHEN @co_parts = N'' THEN N'NULL' ELSE N'COALESCE(' + @co_parts + N')' END;
    END;

    IF @nk = N'$length'
    BEGIN
        DECLARE @len_a NVARCHAR(MAX);
        IF @nt = 4 SELECT TOP 1 @len_a = [value] FROM OPENJSON(@nv);
        ELSE SET @len_a = @nv;
        RETURN N'LEN(' + dbo.pvt_b2_expr_sql(@len_a, @fields, @obj_alias) + N')';
    END;

    -- ---- Date / advanced functions -----------------------------------
    -- $year/$month/$day: T-SQL date part functions
    IF @nk = N'$year'   RETURN N'YEAR('  + dbo.pvt_b2_expr_sql(@nv, @fields, @obj_alias) + N')';
    IF @nk = N'$month'  RETURN N'MONTH(' + dbo.pvt_b2_expr_sql(@nv, @fields, @obj_alias) + N')';
    IF @nk = N'$day'    RETURN N'DAY('   + dbo.pvt_b2_expr_sql(@nv, @fields, @obj_alias) + N')';
    IF @nk = N'$hour'   RETURN N'DATEPART(HOUR, '   + dbo.pvt_b2_expr_sql(@nv, @fields, @obj_alias) + N')';
    IF @nk = N'$minute' RETURN N'DATEPART(MINUTE, ' + dbo.pvt_b2_expr_sql(@nv, @fields, @obj_alias) + N')';
    IF @nk = N'$second' RETURN N'DATEPART(SECOND, ' + dbo.pvt_b2_expr_sql(@nv, @fields, @obj_alias) + N')';
    -- $trimStart/$trimEnd: LTRIM/RTRIM (unary object or array form)
    IF @nk = N'$trimstart' RETURN N'LTRIM(' + dbo.pvt_b2_expr_sql(@nv, @fields, @obj_alias) + N')';

    IF @nk = N'$substring'
    BEGIN
        DECLARE @ss0 NVARCHAR(MAX), @ss1 NVARCHAR(MAX), @ss2 NVARCHAR(MAX);
        SELECT @ss0 = MIN(CASE WHEN [key] = '0' THEN [value] END),
               @ss1 = MIN(CASE WHEN [key] = '1' THEN [value] END),
               @ss2 = MIN(CASE WHEN [key] = '2' THEN [value] END)
          FROM OPENJSON(@nv);
        RETURN N'SUBSTRING(' + dbo.pvt_b2_expr_sql(@ss0, @fields, @obj_alias) + N', '
             + dbo.pvt_b2_expr_sql(@ss1, @fields, @obj_alias) + N', '
             + dbo.pvt_b2_expr_sql(@ss2, @fields, @obj_alias) + N')';
    END;

    IF @nk = N'$replace'
    BEGIN
        DECLARE @re0 NVARCHAR(MAX), @re1 NVARCHAR(MAX), @re2 NVARCHAR(MAX);
        SELECT @re0 = MIN(CASE WHEN [key] = '0' THEN [value] END),
               @re1 = MIN(CASE WHEN [key] = '1' THEN [value] END),
               @re2 = MIN(CASE WHEN [key] = '2' THEN [value] END)
          FROM OPENJSON(@nv);
        RETURN N'REPLACE(' + dbo.pvt_b2_expr_sql(@re0, @fields, @obj_alias) + N', '
             + dbo.pvt_b2_expr_sql(@re1, @fields, @obj_alias) + N', '
             + dbo.pvt_b2_expr_sql(@re2, @fields, @obj_alias) + N')';
    END;

    -- $dateAdd / $dateSub: JSON args = ["unit", date_expr, amount]
    -- T-SQL: DATEADD(unit, [+/-]amount, CAST(date_expr AS DATETIMEOFFSET))
    IF @nk IN (N'$dateadd', N'$datesub')
    BEGIN
        DECLARE @da0 NVARCHAR(MAX), @da1 NVARCHAR(MAX), @da2 NVARCHAR(MAX);
        SELECT @da0 = MIN(CASE WHEN [key] = '0' THEN [value] END),
               @da1 = MIN(CASE WHEN [key] = '1' THEN [value] END),
               @da2 = MIN(CASE WHEN [key] = '2' THEN [value] END)
          FROM OPENJSON(@nv);
        DECLARE @da_n NVARCHAR(MAX) = dbo.pvt_b2_expr_sql(@da2, @fields, @obj_alias);
        IF @nk = N'$datesub' SET @da_n = N'-(' + @da_n + N')';
        RETURN N'DATEADD(' + @da0 + N', ' + @da_n
             + N', CAST(' + dbo.pvt_b2_expr_sql(@da1, @fields, @obj_alias) + N' AS DATETIMEOFFSET))';
    END;

    -- $round: [value] or [value, digits]
    IF @nk = N'$round'
    BEGIN
        DECLARE @ro0 NVARCHAR(MAX), @ro1 NVARCHAR(MAX);
        SELECT @ro0 = MIN(CASE WHEN [key] = '0' THEN [value] END),
               @ro1 = MIN(CASE WHEN [key] = '1' THEN [value] END)
          FROM OPENJSON(@nv);
        IF @ro1 IS NULL
            RETURN N'ROUND(' + dbo.pvt_b2_expr_sql(@ro0, @fields, @obj_alias) + N', 0)';
        RETURN N'ROUND(' + dbo.pvt_b2_expr_sql(@ro0, @fields, @obj_alias) + N', '
             + dbo.pvt_b2_expr_sql(@ro1, @fields, @obj_alias) + N')';
    END;

    -- $log: [base, value] — T-SQL LOG(number, base) has reversed args vs PG
    IF @nk = N'$log'
    BEGIN
        DECLARE @lg0 NVARCHAR(MAX), @lg1 NVARCHAR(MAX);
        SELECT @lg0 = MIN(CASE WHEN [key] = '0' THEN [value] END),
               @lg1 = MIN(CASE WHEN [key] = '1' THEN [value] END)
          FROM OPENJSON(@nv);
        RETURN N'LOG(CAST(' + dbo.pvt_b2_expr_sql(@lg1, @fields, @obj_alias)
             + N' AS FLOAT), CAST(' + dbo.pvt_b2_expr_sql(@lg0, @fields, @obj_alias) + N' AS FLOAT))';
    END;

    -- $indexOf: [str, needle] — T-SQL CHARINDEX(needle, str)
    IF @nk = N'$indexof'
    BEGIN
        DECLARE @io0 NVARCHAR(MAX), @io1 NVARCHAR(MAX);
        SELECT @io0 = MIN(CASE WHEN [key] = '0' THEN [value] END),
               @io1 = MIN(CASE WHEN [key] = '1' THEN [value] END)
          FROM OPENJSON(@nv);
        RETURN N'CHARINDEX(' + dbo.pvt_b2_expr_sql(@io1, @fields, @obj_alias)
             + N', ' + dbo.pvt_b2_expr_sql(@io0, @fields, @obj_alias) + N')';
    END;

    -- $if: CASE WHEN cond THEN then_expr ELSE else_expr END
    -- Condition is itself a B2 expression (comparison returns boolean T-SQL text).
    IF @nk = N'$if'
    BEGIN
        DECLARE @if0 NVARCHAR(MAX), @if1 NVARCHAR(MAX), @if2 NVARCHAR(MAX);
        SELECT @if0 = MIN(CASE WHEN [key] = '0' THEN [value] END),
               @if1 = MIN(CASE WHEN [key] = '1' THEN [value] END),
               @if2 = MIN(CASE WHEN [key] = '2' THEN [value] END)
          FROM OPENJSON(@nv);
        RETURN N'CASE WHEN ' + dbo.pvt_b2_expr_sql(@if0, @fields, @obj_alias)
             + N' THEN ' + dbo.pvt_b2_expr_sql(@if1, @fields, @obj_alias)
             + N' ELSE '  + dbo.pvt_b2_expr_sql(@if2, @fields, @obj_alias) + N' END';
    END;

    -- $case: [{"when":<bool>,"then":<expr>},...,{"else":<expr>}?]
    IF @nk = N'$case'
    BEGIN
        DECLARE @ca_sb   NVARCHAR(MAX) = N'(CASE';
        DECLARE @ca_else NVARCHAR(MAX) = N'NULL';
        DECLARE c_ca CURSOR LOCAL FAST_FORWARD FOR
            SELECT [value] FROM OPENJSON(@nv) ORDER BY CAST([key] AS INT);
        DECLARE @ca_entry NVARCHAR(MAX);
        OPEN c_ca;
        FETCH NEXT FROM c_ca INTO @ca_entry;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            DECLARE @ca_when NVARCHAR(MAX) = JSON_QUERY(@ca_entry, N'$.when');
            DECLARE @ca_then NVARCHAR(MAX) = JSON_QUERY(@ca_entry, N'$.then');
            DECLARE @ca_el   NVARCHAR(MAX) = JSON_QUERY(@ca_entry, N'$.else');
            IF @ca_el IS NOT NULL
                SET @ca_else = dbo.pvt_b2_expr_sql(@ca_el, @fields, @obj_alias);
            ELSE IF @ca_when IS NOT NULL AND @ca_then IS NOT NULL
                SET @ca_sb += N' WHEN ' + dbo.pvt_b2_expr_sql(@ca_when, @fields, @obj_alias)
                           + N' THEN '  + dbo.pvt_b2_expr_sql(@ca_then, @fields, @obj_alias);
            FETCH NEXT FROM c_ca INTO @ca_entry;
        END;
        CLOSE c_ca; DEALLOCATE c_ca;
        RETURN @ca_sb + N' ELSE ' + @ca_else + N' END)';
    END;

    RETURN N'/*unknown-b2-expr:' + @nk + N'*/NULL';
END;
GO

CREATE OR ALTER FUNCTION dbo.pvt_build_where_from_json(
    @filter       NVARCHAR(MAX),
    @fields       NVARCHAR(MAX),
    @base_prefix  NVARCHAR(10)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @filter IS NULL OR @filter = N'{}' OR ISJSON(@filter) = 0
        RETURN N'1=1';

    -- Derive outer table alias for EXISTS subqueries and tree predicates.
    -- Shape A: base_prefix = 'o.'        -> alias 'o'
    -- Shape C: base_prefix = '_pvt_cte.' -> alias '_pvt_cte'
    DECLARE @obj_alias NVARCHAR(50) = CASE WHEN @base_prefix = N'o.' THEN N'o' ELSE N'_pvt_cte' END;

    DECLARE @parts NVARCHAR(MAX) = N'';
    DECLARE @cnt INT = 0;

    DECLARE c CURSOR LOCAL FAST_FORWARD FOR
        SELECT [key], [value], [type] FROM OPENJSON(@filter);
    DECLARE @k NVARCHAR(400), @v NVARCHAR(MAX), @t INT;
    OPEN c;
    FETCH NEXT FROM c INTO @k, @v, @t;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        DECLARE @lk NVARCHAR(50) = LOWER(@k);
        DECLARE @piece NVARCHAR(MAX) = NULL;

        IF @lk = N'$and' AND @t = 4
        BEGIN
            DECLARE @children NVARCHAR(MAX) = N'';
            DECLARE @ccnt INT = 0;
            DECLARE c_a CURSOR LOCAL FAST_FORWARD FOR SELECT [value] FROM OPENJSON(@v);
            DECLARE @e NVARCHAR(MAX);
            OPEN c_a;
            FETCH NEXT FROM c_a INTO @e;
            WHILE @@FETCH_STATUS = 0
            BEGIN
                IF @ccnt > 0 SET @children = @children + N' AND ';
                SET @children = @children + dbo.pvt_build_where_from_json(@e, @fields, @base_prefix);
                SET @ccnt = @ccnt + 1;
                FETCH NEXT FROM c_a INTO @e;
            END;
            CLOSE c_a; DEALLOCATE c_a;
            SET @piece = CASE WHEN @ccnt = 0 THEN N'1=1' ELSE N'(' + @children + N')' END;
        END
        ELSE IF @lk = N'$or' AND @t = 4
        BEGIN
            DECLARE @ochildren NVARCHAR(MAX) = N'';
            DECLARE @occnt INT = 0;
            DECLARE c_o CURSOR LOCAL FAST_FORWARD FOR SELECT [value] FROM OPENJSON(@v);
            DECLARE @oe NVARCHAR(MAX);
            OPEN c_o;
            FETCH NEXT FROM c_o INTO @oe;
            WHILE @@FETCH_STATUS = 0
            BEGIN
                IF @occnt > 0 SET @ochildren = @ochildren + N' OR ';
                SET @ochildren = @ochildren + dbo.pvt_build_where_from_json(@oe, @fields, @base_prefix);
                SET @occnt = @occnt + 1;
                FETCH NEXT FROM c_o INTO @oe;
            END;
            CLOSE c_o; DEALLOCATE c_o;
            SET @piece = CASE WHEN @occnt = 0 THEN N'1=0' ELSE N'(' + @ochildren + N')' END;
        END
        ELSE IF @lk = N'$not'
            SET @piece = N'NOT (' + dbo.pvt_build_where_from_json(@v, @fields, @base_prefix) + N')';

        -- $expr: arbitrary boolean expression subtree. Delegates back to
        -- the same walker so $and/$or/$not + leaf predicates are handled
        -- uniformly. Pro/PG parity (see 17_pvt_expr.sql commentary).
        ELSE IF @lk = N'$expr'
            SET @piece = N'(' + dbo.pvt_build_where_from_json(@v, @fields, @base_prefix) + N')';

        -- ---- Hierarchical operators ------------------------------------
        ELSE IF @lk = N'$isroot'
            SET @piece = CASE WHEN LOWER(@v) = N'false'
                THEN @obj_alias + N'.[_id_parent] IS NOT NULL'
                ELSE @obj_alias + N'.[_id_parent] IS NULL' END;

        ELSE IF @lk = N'$isleaf'
            SET @piece = CASE WHEN LOWER(@v) = N'false'
                THEN N'EXISTS (SELECT 1 FROM dbo._objects _leaf WHERE _leaf.[_id_parent] = ' + @obj_alias + N'.[_id])'
                ELSE N'NOT EXISTS (SELECT 1 FROM dbo._objects _leaf WHERE _leaf.[_id_parent] = ' + @obj_alias + N'.[_id])' END;

        ELSE IF @lk = N'$childrenof'
        BEGIN
            DECLARE @co_id BIGINT = TRY_CAST(@v AS BIGINT);
            SET @piece = CASE WHEN @co_id IS NOT NULL
                THEN @obj_alias + N'.[_id_parent] = ' + CAST(@co_id AS NVARCHAR(20))
                ELSE N'/*$childrenOf: invalid id*/1=0' END;
        END
        ELSE IF @lk = N'$level'
        BEGIN
            DECLARE @lvl_expr NVARCHAR(100) = N'dbo.pvt_object_depth(' + @obj_alias + N'.[_id])';
            IF @t = 2 OR @t = 3  -- direct number: exact equality
                SET @piece = @lvl_expr + N' = ' + ISNULL(CAST(TRY_CAST(@v AS BIGINT) AS NVARCHAR(20)), N'0');
            ELSE IF @t = 5       -- operator object: {"$gt":2} etc.
            BEGIN
                DECLARE @lvl_parts NVARCHAR(MAX) = N'';
                DECLARE @lvl_cnt INT = 0;
                DECLARE c_lvl CURSOR LOCAL FAST_FORWARD FOR SELECT [key], [value] FROM OPENJSON(@v);
                DECLARE @lvlk NVARCHAR(50), @lvlv NVARCHAR(MAX);
                OPEN c_lvl;
                FETCH NEXT FROM c_lvl INTO @lvlk, @lvlv;
                WHILE @@FETCH_STATUS = 0
                BEGIN
                    DECLARE @lvl_cmp NVARCHAR(3) = CASE LOWER(@lvlk)
                        WHEN N'$eq'  THEN N'='
                        WHEN N'$ne'  THEN N'<>'
                        WHEN N'$gt'  THEN N'>'
                        WHEN N'$gte' THEN N'>='
                        WHEN N'$lt'  THEN N'<'
                        WHEN N'$lte' THEN N'<='
                        ELSE N'='
                    END;
                    IF @lvl_cnt > 0 SET @lvl_parts = @lvl_parts + N' AND ';
                    SET @lvl_parts += @lvl_expr + N' ' + @lvl_cmp + N' '
                        + ISNULL(CAST(TRY_CAST(@lvlv AS BIGINT) AS NVARCHAR(20)), N'0');
                    SET @lvl_cnt += 1;
                    FETCH NEXT FROM c_lvl INTO @lvlk, @lvlv;
                END;
                CLOSE c_lvl; DEALLOCATE c_lvl;
                SET @piece = CASE WHEN @lvl_cnt > 0 THEN @lvl_parts ELSE N'1=1' END;
            END;
            ELSE
                SET @piece = N'/*$level: unsupported type*/1=1';
        END
        ELSE IF @lk = N'$hasancestor'
        BEGIN
            -- Value can be a bare bigint or {"id": N}
            DECLARE @ha_id BIGINT = TRY_CAST(@v AS BIGINT);
            IF @ha_id IS NULL AND @t = 5
                SET @ha_id = TRY_CAST(JSON_VALUE(@v, N'$.id') AS BIGINT);
            SET @piece = CASE WHEN @ha_id IS NOT NULL
                THEN N'dbo.pvt_is_descendant_of(' + @obj_alias + N'.[_id], ' + CAST(@ha_id AS NVARCHAR(20)) + N') = 1'
                ELSE N'/*$hasAncestor: no id*/1=0' END;
        END
        ELSE IF @lk = N'$hasdescendant'
        BEGIN
            -- Object has descendant X == X is descendant of this object
            DECLARE @hd_id BIGINT = TRY_CAST(@v AS BIGINT);
            IF @hd_id IS NULL AND @t = 5
                SET @hd_id = TRY_CAST(JSON_VALUE(@v, N'$.id') AS BIGINT);
            SET @piece = CASE WHEN @hd_id IS NOT NULL
                THEN N'dbo.pvt_is_descendant_of(' + CAST(@hd_id AS NVARCHAR(20)) + N', ' + @obj_alias + N'.[_id]) = 1'
                ELSE N'/*$hasDescendant: no id*/1=0' END;
        END

        -- ---- B2-expr: comparison operators with expression operands ----
        ELSE IF @lk IN (N'$gt', N'$gte', N'$lt', N'$lte', N'$eq', N'$ne', N'$ilike') AND @t = 4
        BEGIN
            DECLARE @b2_op1 NVARCHAR(MAX), @b2_op2 NVARCHAR(MAX);
            SELECT @b2_op1 = MIN(CASE WHEN [key] = '0' THEN [value] END),
                   @b2_op2 = MIN(CASE WHEN [key] = '1' THEN [value] END)
              FROM OPENJSON(@v);
            DECLARE @b2_cmp NVARCHAR(5) = CASE @lk
                WHEN N'$gt'    THEN N'>'    WHEN N'$gte'   THEN N'>='
                WHEN N'$lt'    THEN N'<'    WHEN N'$lte'   THEN N'<='
                WHEN N'$eq'    THEN N'='    WHEN N'$ne'    THEN N'<>'
                WHEN N'$ilike' THEN N'LIKE' END;
            DECLARE @b2_l NVARCHAR(MAX) = dbo.pvt_b2_expr_sql(@b2_op1, @fields, @obj_alias);
            DECLARE @b2_r NVARCHAR(MAX) = dbo.pvt_b2_expr_sql(@b2_op2, @fields, @obj_alias);
            -- Null-aware rewrite for $eq / $ne (parity with PG IS [NOT] DISTINCT FROM)
            IF @lk = N'$eq' AND @b2_r = N'NULL' AND @b2_l <> N'NULL'
                SET @piece = @b2_l + N' IS NULL';
            ELSE IF @lk = N'$ne' AND @b2_r = N'NULL' AND @b2_l <> N'NULL'
                SET @piece = @b2_l + N' IS NOT NULL';
            ELSE IF @lk = N'$eq' AND @b2_l = N'NULL' AND @b2_r <> N'NULL'
                SET @piece = @b2_r + N' IS NULL';
            ELSE IF @lk = N'$ne' AND @b2_l = N'NULL' AND @b2_r <> N'NULL'
                SET @piece = @b2_r + N' IS NOT NULL';
            ELSE IF @lk = N'$eq' AND @b2_l = N'NULL' AND @b2_r = N'NULL'
                SET @piece = N'1=1';
            ELSE IF @lk = N'$ne' AND @b2_l = N'NULL' AND @b2_r = N'NULL'
                SET @piece = N'1=0';
            ELSE
                SET @piece = @b2_l + N' ' + @b2_cmp + N' ' + @b2_r;
        END

        -- ---- B2-expr: $between [expr, lo, hi] -----------------------
        ELSE IF @lk = N'$between' AND @t = 4
        BEGIN
            DECLARE @bw_0 NVARCHAR(MAX), @bw_1 NVARCHAR(MAX), @bw_2 NVARCHAR(MAX);
            SELECT @bw_0 = MIN(CASE WHEN [key] = '0' THEN [value] END),
                   @bw_1 = MIN(CASE WHEN [key] = '1' THEN [value] END),
                   @bw_2 = MIN(CASE WHEN [key] = '2' THEN [value] END)
              FROM OPENJSON(@v);
            SET @piece = dbo.pvt_b2_expr_sql(@bw_0, @fields, @obj_alias)
                       + N' BETWEEN ' + dbo.pvt_b2_expr_sql(@bw_1, @fields, @obj_alias)
                       + N' AND '     + dbo.pvt_b2_expr_sql(@bw_2, @fields, @obj_alias);
        END

        -- ---- B2-expr: $in / $nin [expr, [v1, v2, ...]] --------------
        ELSE IF @lk IN (N'$in', N'$nin') AND @t = 4
        BEGIN
            DECLARE @iq_e NVARCHAR(MAX), @iq_vs NVARCHAR(MAX);
            SELECT @iq_e  = MIN(CASE WHEN [key] = '0' THEN [value] END),
                   @iq_vs = MIN(CASE WHEN [key] = '1' THEN [value] END)
              FROM OPENJSON(@v);
            DECLARE @iq_list NVARCHAR(MAX) = N'';
            DECLARE c_iq CURSOR LOCAL FAST_FORWARD FOR
                SELECT [value], [type] FROM OPENJSON(@iq_vs);
            DECLARE @iq_iv NVARCHAR(MAX), @iq_it INT;
            OPEN c_iq;
            FETCH NEXT FROM c_iq INTO @iq_iv, @iq_it;
            WHILE @@FETCH_STATUS = 0
            BEGIN
                IF @iq_list <> N'' SET @iq_list += N', ';
                SET @iq_list += dbo.pvt_jsonb_to_sql_literal(@iq_iv, @iq_it);
                FETCH NEXT FROM c_iq INTO @iq_iv, @iq_it;
            END;
            CLOSE c_iq; DEALLOCATE c_iq;
            IF @iq_list = N'' SET @iq_list = N'NULL';
            DECLARE @iq_not NVARCHAR(5) = CASE WHEN @lk = N'$nin' THEN N'NOT ' ELSE N'' END;
            SET @piece = dbo.pvt_b2_expr_sql(@iq_e, @fields, @obj_alias)
                       + N' ' + @iq_not + N'IN (' + @iq_list + N')';
        END

        -- ---- B2-expr: $null / $notNull {expr} -----------------------
        ELSE IF @lk = N'$null' AND @t = 5
            SET @piece = dbo.pvt_b2_expr_sql(@v, @fields, @obj_alias) + N' IS NULL';
        ELSE IF @lk = N'$notnull' AND @t = 5
            SET @piece = dbo.pvt_b2_expr_sql(@v, @fields, @obj_alias) + N' IS NOT NULL';

        -- ---- B2-expr: $contains / $startsWith [expr, literal_str] --
        -- Second arg may be a bare JSON string OR a {"$const":"..."} wrapper
        -- (the .NET expression builder always emits constants as $const).
        ELSE IF @lk = N'$contains' AND @t = 4
        BEGIN
            DECLARE @ct_e NVARCHAR(MAX), @ct_s NVARCHAR(MAX);
            SELECT @ct_e = MIN(CASE WHEN [key] = '0' THEN [value] END),
                   @ct_s = MIN(CASE WHEN [key] = '1' THEN [value] END)
              FROM OPENJSON(@v);
            IF @ct_s IS NOT NULL AND ISJSON(@ct_s) = 1
                SET @ct_s = COALESCE(JSON_VALUE(@ct_s, N'$."$const"'), @ct_s);
            SET @piece = dbo.pvt_b2_expr_sql(@ct_e, @fields, @obj_alias)
                       + N' LIKE ' + dbo.pvt_sql_string_literal(N'%' + @ct_s + N'%');
        END

        ELSE IF @lk = N'$startswith' AND @t = 4
        BEGIN
            DECLARE @sw_e NVARCHAR(MAX), @sw_s NVARCHAR(MAX);
            SELECT @sw_e = MIN(CASE WHEN [key] = '0' THEN [value] END),
                   @sw_s = MIN(CASE WHEN [key] = '1' THEN [value] END)
              FROM OPENJSON(@v);
            IF @sw_s IS NOT NULL AND ISJSON(@sw_s) = 1
                SET @sw_s = COALESCE(JSON_VALUE(@sw_s, N'$."$const"'), @sw_s);
            SET @piece = dbo.pvt_b2_expr_sql(@sw_e, @fields, @obj_alias)
                       + N' LIKE ' + dbo.pvt_sql_string_literal(@sw_s + N'%');
        END

        ELSE IF @lk = N'$endswith' AND @t = 4
        BEGIN
            DECLARE @es_e NVARCHAR(MAX), @es_s NVARCHAR(MAX);
            SELECT @es_e = MIN(CASE WHEN [key] = '0' THEN [value] END),
                   @es_s = MIN(CASE WHEN [key] = '1' THEN [value] END)
              FROM OPENJSON(@v);
            IF @es_s IS NOT NULL AND ISJSON(@es_s) = 1
                SET @es_s = COALESCE(JSON_VALUE(@es_s, N'$."$const"'), @es_s);
            SET @piece = dbo.pvt_b2_expr_sql(@es_e, @fields, @obj_alias)
                       + N' LIKE ' + dbo.pvt_sql_string_literal(N'%' + @es_s);
        END

        -- $like: raw LIKE with full pattern from second arg expression
        ELSE IF @lk = N'$like' AND @t = 4
        BEGIN
            DECLARE @lk_e NVARCHAR(MAX), @lk_p NVARCHAR(MAX);
            SELECT @lk_e = MIN(CASE WHEN [key] = '0' THEN [value] END),
                   @lk_p = MIN(CASE WHEN [key] = '1' THEN [value] END)
              FROM OPENJSON(@v);
            SET @piece = dbo.pvt_b2_expr_sql(@lk_e, @fields, @obj_alias)
                       + N' LIKE ' + dbo.pvt_b2_expr_sql(@lk_p, @fields, @obj_alias);
        END

        -- ---- Other unsupported top-level $* keys ----------------------
        ELSE IF LEFT(@k, 1) = N'$'
            SET @piece = N'/*unsupported-top:' + @k + N'*/1=1';

        ELSE
        BEGIN
            -- ---- Property function keys: Field.$length / Field[].$count ----
            IF RIGHT(@k, 8) = N'.$length' OR RIGHT(@k, 7) = N'.$count' OR RIGHT(@k, 9) = N'[].$count'
            BEGIN
                DECLARE @pf_is_len BIT = CASE WHEN RIGHT(@k, 8) = N'.$length' THEN 1 ELSE 0 END;
                DECLARE @pf_base   NVARCHAR(400);
                IF @pf_is_len = 1
                    SET @pf_base = LEFT(@k, LEN(@k) - 8);        -- strip '.$length'
                ELSE IF RIGHT(@k, 9) = N'[].$count'
                    SET @pf_base = LEFT(@k, LEN(@k) - 9);        -- strip '[].$count'
                ELSE
                    SET @pf_base = LEFT(@k, LEN(@k) - 7);        -- strip '.$count'

                DECLARE @pf_meta NVARCHAR(MAX) = JSON_QUERY(@fields, N'$."' + STRING_ESCAPE(@pf_base, 'json') + N'"');
                IF @pf_meta IS NOT NULL
                BEGIN
                    DECLARE @pf_sid NVARCHAR(32) = JSON_VALUE(@pf_meta, N'$.sid');
                    IF @pf_sid IS NOT NULL AND @t = 5
                    BEGIN
                        DECLARE @pf_parts NVARCHAR(MAX) = N'';
                        DECLARE @pf_cnt   INT = 0;
                        DECLARE c_pf CURSOR LOCAL FAST_FORWARD FOR SELECT [key], [value] FROM OPENJSON(@v);
                        DECLARE @pfk NVARCHAR(50), @pfv NVARCHAR(MAX);
                        OPEN c_pf;
                        FETCH NEXT FROM c_pf INTO @pfk, @pfv;
                        WHILE @@FETCH_STATUS = 0
                        BEGIN
                            DECLARE @pf_cmp NVARCHAR(3) = CASE LOWER(@pfk)
                                WHEN N'$eq'  THEN N'='
                                WHEN N'$ne'  THEN N'<>'
                                WHEN N'$gt'  THEN N'>'
                                WHEN N'$gte' THEN N'>='
                                WHEN N'$lt'  THEN N'<'
                                WHEN N'$lte' THEN N'<='
                                ELSE N'='
                            END;
                            DECLARE @pf_num  NVARCHAR(20) = ISNULL(CAST(TRY_CAST(@pfv AS INT) AS NVARCHAR(20)), N'0');
                            DECLARE @pf_frag NVARCHAR(MAX);
                            IF @pf_is_len = 1
                                SET @pf_frag = N'EXISTS (SELECT 1 FROM dbo._values fv'
                                    + N' WHERE fv._id_object = ' + @obj_alias + N'.[_id]'
                                    + N' AND fv._id_structure = ' + @pf_sid
                                    + N' AND fv._array_index IS NULL'
                                    + N' AND LEN(fv.[_String]) ' + @pf_cmp + N' ' + @pf_num + N')';
                            ELSE
                                SET @pf_frag = N'(SELECT COUNT(*) FROM dbo._values fv'
                                    + N' WHERE fv._id_object = ' + @obj_alias + N'.[_id]'
                                    + N' AND fv._id_structure = ' + @pf_sid
                                    + N' AND fv._array_index IS NOT NULL)'
                                    + N' ' + @pf_cmp + N' ' + @pf_num;

                            IF @pf_cnt > 0 SET @pf_parts = @pf_parts + N' AND ';
                            SET @pf_parts += @pf_frag;
                            SET @pf_cnt   += 1;
                            FETCH NEXT FROM c_pf INTO @pfk, @pfv;
                        END;
                        CLOSE c_pf; DEALLOCATE c_pf;
                        SET @piece = CASE WHEN @pf_cnt > 0 THEN @pf_parts ELSE N'1=1' END;
                    END;
                END;
                IF @piece IS NULL
                    SET @piece = N'/*pf-not-found:' + @pf_base + N'*/1=1';
            END
            ELSE
            BEGIN
                -- ---- Regular field leaf: ContainsKey rewrite + lookup -------
                DECLARE @peek NVARCHAR(MAX) = dbo.pvt_peek_contains_key_value(@v);
                DECLARE @norm NVARCHAR(400) = dbo.pvt_normalize_field_name(@k, @peek);
                DECLARE @meta NVARCHAR(MAX) = JSON_QUERY(@fields, N'$.' + N'"' + STRING_ESCAPE(@norm, 'json') + N'"');
                IF @meta IS NULL
                    SET @piece = N'/*missing-meta:' + @norm + N'*/1=0';
                ELSE IF @norm <> @k AND JSON_VALUE(@meta, '$.was_contains_key') = N'true'
                BEGIN
                    -- ContainsKey: emit EXISTS checking _array_index = key.
                    -- Covers both Dictionary<K,primitive> (rows in struct itself)
                    -- and Dictionary<K,Class> (rows in child structs).
                    DECLARE @ck_sid      NVARCHAR(32)  = JSON_VALUE(@meta, N'$.sid');
                    DECLARE @ck_dict_key NVARCHAR(200) = JSON_VALUE(@meta, N'$.dict_key');
                    SET @piece = N'EXISTS (SELECT 1 FROM dbo._values av'
                        + N' JOIN dbo._structures ds ON av._id_structure = ds._id'
                        + N' WHERE av._id_object = ' + @obj_alias + N'.[_id]'
                        + N' AND (ds._id = ' + ISNULL(@ck_sid, N'0') + N' OR ds._id_parent = ' + ISNULL(@ck_sid, N'0') + N')'
                        + N' AND av._array_index = N''' + REPLACE(ISNULL(@ck_dict_key, N''), N'''', N'''''') + N''')';
                END
                ELSE
                    SET @piece = dbo.pvt_build_field_condition(@norm, @meta, @v, @t, @base_prefix);
            END;
        END;

        IF @piece IS NOT NULL
        BEGIN
            IF @cnt > 0 SET @parts = @parts + N' AND ';
            SET @parts = @parts + @piece;
            SET @cnt = @cnt + 1;
        END;
        FETCH NEXT FROM c INTO @k, @v, @t;
    END;
    CLOSE c; DEALLOCATE c;

    IF @cnt = 0 RETURN N'1=1';
    IF @cnt = 1 RETURN @parts;
    RETURN N'(' + @parts + N')';
END;
GO


-- ===== 15_pvt_order.sql =====
-- =====================================================================
-- 15_pvt_order.sql  (MSSql v2-pvt) — ORDER BY builder
-- ---------------------------------------------------------------------
-- Supports {field|field_path,dir,nulls} and {"$expr":{...},"dir",...}
-- entries. $expr delegates to pvt_b2_expr_sql. No DISTINCT ON.
-- Returns either '' or '\nORDER BY <cols>'. T-SQL has no `NULLS FIRST/LAST`
-- syntax; emulate via CASE WHEN ... IS NULL THEN 0 ELSE 1 END prefix.
-- =====================================================================

SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER FUNCTION dbo.pvt_build_order_conditions(
    @order        NVARCHAR(MAX),
    @fields       NVARCHAR(MAX),
    @base_prefix  NVARCHAR(10)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @order IS NULL OR ISJSON(@order) = 0
        RETURN N'';

    DECLARE @parts NVARCHAR(MAX) = N'';
    DECLARE @cnt INT = 0;

    DECLARE c CURSOR LOCAL FAST_FORWARD FOR SELECT [value] FROM OPENJSON(@order);
    DECLARE @e NVARCHAR(MAX);
    OPEN c;
    FETCH NEXT FROM c INTO @e;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        DECLARE @fld   NVARCHAR(400) = COALESCE(JSON_VALUE(@e, '$.field'), JSON_VALUE(@e, '$.field_path'));
        DECLARE @dir   NVARCHAR(10)  = LOWER(COALESCE(JSON_VALUE(@e, '$.dir'), JSON_VALUE(@e, '$.direction'), N'asc'));
        DECLARE @nulls NVARCHAR(10)  = LOWER(COALESCE(JSON_VALUE(@e, '$.nulls'), N''));
        IF @dir NOT IN (N'asc', N'desc') SET @dir = N'asc';

        DECLARE @expr_node NVARCHAR(MAX) = JSON_QUERY(@e, N'$."$expr"');
        IF @expr_node IS NOT NULL
        BEGIN
            -- $expr ORDER BY: delegate to pvt_b2_expr_sql.
            -- pvt_b2_expr_sql expects @obj_alias WITHOUT trailing dot
            -- (matches pvt_build_where_from_json convention).
            DECLARE @b2_alias NVARCHAR(50) = CASE WHEN @base_prefix = N'o.' THEN N'o' ELSE N'_pvt_cte' END;
            DECLARE @eprt NVARCHAR(MAX) = dbo.pvt_b2_expr_sql(@expr_node, @fields, @b2_alias);
            IF @eprt IS NOT NULL AND @eprt <> N''
            BEGIN
                IF @cnt > 0 SET @parts = @parts + N', ';
                DECLARE @eterm NVARCHAR(MAX) = N'(' + @eprt + N') ' + UPPER(@dir);
                IF @nulls = N'first'
                    SET @parts = @parts + N'CASE WHEN (' + @eprt + N') IS NULL THEN 0 ELSE 1 END, ' + @eterm;
                ELSE IF @nulls = N'last'
                    SET @parts = @parts + N'CASE WHEN (' + @eprt + N') IS NULL THEN 1 ELSE 0 END, ' + @eterm;
                ELSE
                    SET @parts = @parts + @eterm;
                SET @cnt = @cnt + 1;
            END;
        END
        ELSE IF @fld IS NOT NULL AND @fld <> N''
        BEGIN
            DECLARE @meta NVARCHAR(MAX) = JSON_QUERY(@fields, N'$.' + N'"' + STRING_ESCAPE(@fld, 'json') + N'"');
            DECLARE @col NVARCHAR(200);
            IF @meta IS NOT NULL AND JSON_VALUE(@meta, '$.kind') = N'base'
                SET @col = ISNULL(@base_prefix, N'') + QUOTENAME(JSON_VALUE(@meta, '$.column'));
            ELSE
                SET @col = QUOTENAME(@fld);

            IF @cnt > 0 SET @parts = @parts + N', ';
            IF @nulls = N'first'
                SET @parts = @parts + N'CASE WHEN ' + @col + N' IS NULL THEN 0 ELSE 1 END, ';
            ELSE IF @nulls = N'last'
                SET @parts = @parts + N'CASE WHEN ' + @col + N' IS NULL THEN 1 ELSE 0 END, ';
            SET @parts = @parts + @col + N' ' + UPPER(@dir);
            SET @cnt = @cnt + 1;
        END;
        FETCH NEXT FROM c INTO @e;
    END;
    CLOSE c; DEALLOCATE c;

    IF @cnt = 0 RETURN N'';
    RETURN CHAR(10) + N'ORDER BY ' + @parts;
END;
GO


-- ===== 16_pvt_split.sql =====
-- =====================================================================
-- 16_pvt_split.sql  (MSSql v2-pvt) — Pro-parity base pushdown optimizer
-- ---------------------------------------------------------------------
-- Walks the filter JSON and splits it into two parts:
--
--   * push      — SQL predicate over `_objects o.*` safe to AND with
--                 `o._id_scheme = X` inside the pivot CTE; lets the
--                 optimizer narrow _objects BEFORE the expensive pivot
--                 JOIN with _values + GROUP BY.
--   * residual  — JSON subtree that still needs to be evaluated against
--                 pivot/props columns AFTER the CTE materializes; fed
--                 to the regular pvt_build_where_from_json walker.
--
-- Function:
--   dbo.pvt_split_filter(@filter NVARCHAR(MAX), @fields NVARCHAR(MAX))
--   RETURNS NVARCHAR(MAX) — JSON: {"push": "<sql or null>",
--                                  "residual": <node or null>}
--
-- T-SQL scalar UDFs cannot have OUT parameters, hence the JSON-encoded
-- composite return. Callers parse via JSON_VALUE / JSON_QUERY.
--
-- Splitting rules (conservative — never changes semantics):
--   leaf {f: ops}              base/hierarchical -> push,
--                              else                -> residual.
--   {$and: [c1, c2, ...]}      split each child; AND the push parts;
--                              residual = $and of non-null residuals
--                              (degraded to single child or NULL).
--   {$or:  [c1, c2, ...]}      pushable ONLY when every child is fully
--                              base (residual_i is NULL for all i).
--                              Mixed-leaf $or stays entirely in residual.
--   {$not: c}                  pushable ONLY when c is fully base.
--                              Otherwise stays entirely in residual.
--   top-level multi-key object treated as implicit $and of singletons.
--
-- Phase 1 + 2 (parity with PG / Pro):
--   * Hierarchical + field-leaf pushdown (Phase 1).
--   * Expression-form predicates at top level ($eq/$ne/.../$between/$in
--     and $expr) — pushable iff every $field reference inside resolves
--     to kind='base' (see dbo.pvt_expr_is_base_only in 17_pvt_expr.sql).
--   * Common form {Field: {$op: value}} is covered via the field-leaf
--     branch and benefits from the same kind='base' gating.
--
-- Recursion: T-SQL scalar UDFs support nesting up to @@NESTLEVEL = 32,
-- which is sufficient for any realistic filter tree.
-- =====================================================================

SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER FUNCTION dbo.pvt_split_filter(
    @filter  NVARCHAR(MAX),
    @fields  NVARCHAR(MAX)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    -- ----- Empty / null filter -----
    IF @filter IS NULL OR @filter = N'' OR @filter = N'{}'
        RETURN N'{"push":null,"residual":null}';

    IF ISJSON(@filter) = 0
        RETURN N'{"push":null,"residual":null}';

    -- ----- Scan top-level keys -----
    DECLARE @key_count INT = (SELECT COUNT(*) FROM OPENJSON(@filter));
    IF @key_count = 0
        RETURN N'{"push":null,"residual":null}';

    -- Detect logical singleton: $and / $or / $not (case-insensitive).
    DECLARE @op_key NVARCHAR(50) = (
        SELECT TOP 1 [key]
          FROM OPENJSON(@filter)
         WHERE LOWER([key]) IN (N'$and', N'$or', N'$not')
    );
    DECLARE @is_logical BIT = CASE
        WHEN @op_key IS NOT NULL AND @key_count = 1 THEN 1 ELSE 0
    END;

    -- Helpers used in many branches.
    DECLARE @push_sql  NVARCHAR(MAX) = NULL;
    DECLARE @residual  NVARCHAR(MAX) = NULL;
    DECLARE @push_buf  NVARCHAR(MAX) = N'';
    DECLARE @push_cnt  INT = 0;
    DECLARE @res_buf   NVARCHAR(MAX) = N'';
    DECLARE @res_cnt   INT = 0;

    -- Recursion scratch.
    DECLARE @elem_val   NVARCHAR(MAX);
    DECLARE @child      NVARCHAR(MAX);
    DECLARE @cpush      NVARCHAR(MAX);
    DECLARE @cres       NVARCHAR(MAX);

    -- ============================================================
    -- 1) Logical singleton: $and / $or / $not
    -- ============================================================
    IF @is_logical = 1
    BEGIN
        DECLARE @op_lower NVARCHAR(50) = LOWER(@op_key);
        DECLARE @op_val   NVARCHAR(MAX) = JSON_QUERY(@filter, N'$."' + STRING_ESCAPE(@op_key, 'json') + N'"');

        -- ---- $and ----
        IF @op_lower = N'$and'
        BEGIN
            IF @op_val IS NULL OR ISJSON(@op_val) = 0
                RETURN N'{"push":null,"residual":null}';

            DECLARE cAnd CURSOR LOCAL FAST_FORWARD FOR
                SELECT [value] FROM OPENJSON(@op_val);
            OPEN cAnd;
            FETCH NEXT FROM cAnd INTO @elem_val;
            WHILE @@FETCH_STATUS = 0
            BEGIN
                SET @child = dbo.pvt_split_filter(@elem_val, @fields);
                SET @cpush = JSON_VALUE(@child, N'$.push');
                SET @cres  = JSON_QUERY(@child, N'$.residual');

                IF @cpush IS NOT NULL
                BEGIN
                    IF @push_cnt > 0 SET @push_buf = @push_buf + N' AND ';
                    SET @push_buf = @push_buf + @cpush;
                    SET @push_cnt = @push_cnt + 1;
                END;
                IF @cres IS NOT NULL
                BEGIN
                    IF @res_cnt > 0 SET @res_buf = @res_buf + N',';
                    SET @res_buf = @res_buf + @cres;
                    SET @res_cnt = @res_cnt + 1;
                END;

                FETCH NEXT FROM cAnd INTO @elem_val;
            END;
            CLOSE cAnd; DEALLOCATE cAnd;

            IF @push_cnt = 1
                SET @push_sql = @push_buf;
            ELSE IF @push_cnt > 1
                SET @push_sql = N'(' + @push_buf + N')';

            IF @res_cnt = 1
                SET @residual = @res_buf;
            ELSE IF @res_cnt > 1
                SET @residual = N'{"$and":[' + @res_buf + N']}';

            GOTO ret;
        END;

        -- ---- $or ----
        IF @op_lower = N'$or'
        BEGIN
            IF @op_val IS NULL OR ISJSON(@op_val) = 0
                RETURN N'{"push":null,"residual":null}';

            DECLARE @all_pushable BIT = 1;
            DECLARE @or_buf NVARCHAR(MAX) = N'';
            DECLARE @or_cnt INT = 0;

            DECLARE cOr CURSOR LOCAL FAST_FORWARD FOR
                SELECT [value] FROM OPENJSON(@op_val);
            OPEN cOr;
            FETCH NEXT FROM cOr INTO @elem_val;
            WHILE @@FETCH_STATUS = 0 AND @all_pushable = 1
            BEGIN
                SET @child = dbo.pvt_split_filter(@elem_val, @fields);
                SET @cpush = JSON_VALUE(@child, N'$.push');
                SET @cres  = JSON_QUERY(@child, N'$.residual');

                IF @cres IS NOT NULL OR @cpush IS NULL
                    SET @all_pushable = 0;
                ELSE
                BEGIN
                    IF @or_cnt > 0 SET @or_buf = @or_buf + N' OR ';
                    SET @or_buf = @or_buf + @cpush;
                    SET @or_cnt = @or_cnt + 1;
                END;

                FETCH NEXT FROM cOr INTO @elem_val;
            END;
            CLOSE cOr; DEALLOCATE cOr;

            IF @all_pushable = 1 AND @or_cnt > 0
            BEGIN
                SET @push_sql = N'(' + @or_buf + N')';
                SET @residual = NULL;
            END
            ELSE
            BEGIN
                SET @push_sql = NULL;
                SET @residual = @filter;
            END;
            GOTO ret;
        END;

        -- ---- $not ----
        IF @op_lower = N'$not'
        BEGIN
            IF @op_val IS NULL
            BEGIN
                SET @residual = @filter;
                GOTO ret;
            END;
            SET @child = dbo.pvt_split_filter(@op_val, @fields);
            SET @cpush = JSON_VALUE(@child, N'$.push');
            SET @cres  = JSON_QUERY(@child, N'$.residual');
            IF @cres IS NULL AND @cpush IS NOT NULL
            BEGIN
                SET @push_sql = N'NOT (' + @cpush + N')';
                SET @residual = NULL;
            END
            ELSE
            BEGIN
                SET @push_sql = NULL;
                SET @residual = @filter;
            END;
            GOTO ret;
        END;
    END;

    -- ============================================================
    -- 2) Multi-key object: implicit $and of singletons.
    -- ============================================================
    IF @key_count > 1
    BEGIN
        DECLARE @ek NVARCHAR(400);
        DECLARE @ev NVARCHAR(MAX);
        DECLARE @et INT;
        DECLARE @singleton NVARCHAR(MAX);
        DECLARE @ev_json NVARCHAR(MAX);

        DECLARE cM CURSOR LOCAL FAST_FORWARD FOR
            SELECT [key], [value], [type] FROM OPENJSON(@filter);
        OPEN cM;
        FETCH NEXT FROM cM INTO @ek, @ev, @et;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            -- Re-encode value as JSON literal for singleton object.
            -- type codes: 1=string, 2=number, 3=bool, 4=array, 5=object, 0=null.
            SET @ev_json = CASE @et
                WHEN 0 THEN N'null'
                WHEN 1 THEN N'"' + STRING_ESCAPE(@ev, 'json') + N'"'
                WHEN 2 THEN @ev
                WHEN 3 THEN @ev
                WHEN 4 THEN @ev
                WHEN 5 THEN @ev
                ELSE @ev
            END;
            SET @singleton = N'{"' + STRING_ESCAPE(@ek, 'json') + N'":' + @ev_json + N'}';

            SET @child = dbo.pvt_split_filter(@singleton, @fields);
            SET @cpush = JSON_VALUE(@child, N'$.push');
            SET @cres  = JSON_QUERY(@child, N'$.residual');

            IF @cpush IS NOT NULL
            BEGIN
                IF @push_cnt > 0 SET @push_buf = @push_buf + N' AND ';
                SET @push_buf = @push_buf + @cpush;
                SET @push_cnt = @push_cnt + 1;
            END;
            IF @cres IS NOT NULL
            BEGIN
                IF @res_cnt > 0 SET @res_buf = @res_buf + N',';
                SET @res_buf = @res_buf + @cres;
                SET @res_cnt = @res_cnt + 1;
            END;

            FETCH NEXT FROM cM INTO @ek, @ev, @et;
        END;
        CLOSE cM; DEALLOCATE cM;

        IF @push_cnt = 1
            SET @push_sql = @push_buf;
        ELSE IF @push_cnt > 1
            SET @push_sql = N'(' + @push_buf + N')';

        IF @res_cnt = 1
            SET @residual = @res_buf;
        ELSE IF @res_cnt > 1
            SET @residual = N'{"$and":[' + @res_buf + N']}';

        GOTO ret;
    END;

    -- ============================================================
    -- 3) Single-key leaf: hierarchical / expression / field.
    -- ============================================================
    DECLARE @k NVARCHAR(400);
    DECLARE @v NVARCHAR(MAX);
    DECLARE @t INT;
    SELECT TOP 1 @k = [key], @v = [value], @t = [type] FROM OPENJSON(@filter);

    DECLARE @k_lower NVARCHAR(400) = LOWER(@k);

    -- ---- Hierarchical operators: always pushable ----
    IF @k_lower IN (N'$hasancestor', N'$hasdescendant', N'$level',
                    N'$isroot', N'$isleaf', N'$childrenof')
    BEGIN
        DECLARE @v_json_h NVARCHAR(MAX) = CASE @t
            WHEN 0 THEN N'null'
            WHEN 1 THEN N'"' + STRING_ESCAPE(@v, 'json') + N'"'
            ELSE @v
        END;
        DECLARE @h_in NVARCHAR(MAX) = N'{"' + STRING_ESCAPE(@k, 'json') + N'":' + @v_json_h + N'}';
        DECLARE @h_out NVARCHAR(MAX) = dbo.pvt_build_hierarchical_conditions(@h_in, N'o');

        IF @h_out IS NOT NULL AND @h_out <> N''
        BEGIN
            SET @h_out = LTRIM(@h_out);
            IF LEN(@h_out) >= 4 AND UPPER(LEFT(@h_out, 4)) = N'AND '
                SET @h_out = LTRIM(SUBSTRING(@h_out, 5, LEN(@h_out)));
            IF @h_out <> N''
                SET @push_sql = @h_out;
        END;
        SET @residual = NULL;
        GOTO ret;
    END;

    -- ---- Expression-form predicate / $expr at root (Phase 2 parity) ----
    -- Pushable iff every $field reference inside resolves to kind='base'.
    -- When pushable we reuse the regular WHERE walker against the inner
    -- `_objects o.*` alias (base_prefix='o.') so emitted SQL is exactly
    -- what runs in the outer Shape A path -- no duplicate compiler.
    IF @k_lower IN (
        N'$eq', N'$ne', N'$lt', N'$lte', N'$gt', N'$gte',
        N'$like', N'$ilike',
        N'$in', N'$nin', N'$between',
        N'$null', N'$notnull',
        N'$contains', N'$startswith', N'$endswith'
    )
    BEGIN
        IF dbo.pvt_expr_is_base_only(@v, @fields) = 1
        BEGIN
            DECLARE @push_pred NVARCHAR(MAX) = dbo.pvt_build_where_from_json(@filter, @fields, N'o.');
            IF @push_pred IS NOT NULL AND @push_pred <> N'' AND @push_pred <> N'1=1'
                SET @push_sql = @push_pred;
            SET @residual = NULL;
        END
        ELSE
        BEGIN
            SET @push_sql = NULL;
            SET @residual = @filter;
        END;
        GOTO ret;
    END;

    IF @k_lower = N'$expr'
    BEGIN
        IF dbo.pvt_expr_is_base_only(@v, @fields) = 1
        BEGIN
            DECLARE @push_expr NVARCHAR(MAX) = dbo.pvt_build_where_from_json(@v, @fields, N'o.');
            IF @push_expr IS NOT NULL AND @push_expr <> N'' AND @push_expr <> N'1=1'
                SET @push_sql = @push_expr;
            SET @residual = NULL;
        END
        ELSE
        BEGIN
            SET @push_sql = NULL;
            SET @residual = @filter;
        END;
        GOTO ret;
    END;

    -- ---- Field leaf: look up metadata; push only if kind='base' ----
    DECLARE @peek NVARCHAR(MAX) = dbo.pvt_peek_contains_key_value(@v);
    DECLARE @norm NVARCHAR(400) = dbo.pvt_normalize_field_name(@k, @peek);
    DECLARE @meta NVARCHAR(MAX) = JSON_QUERY(@fields, N'$."' + STRING_ESCAPE(@norm, 'json') + N'"');

    IF @meta IS NULL
    BEGIN
        -- Unknown field: leave to the outer walker so its error message fires.
        SET @push_sql = NULL;
        SET @residual = @filter;
        GOTO ret;
    END;

    DECLARE @kind NVARCHAR(50) = JSON_VALUE(@meta, N'$.kind');
    IF @kind = N'base'
    BEGIN
        -- ContainsKey rewrites are dict-pivot only, never base; safe here.
        -- pvt_build_field_condition needs op-type code; @t already captured.
        DECLARE @cond NVARCHAR(MAX) = dbo.pvt_build_field_condition(
            @norm, @meta, @v, @t, N'o.');
        IF @cond IS NOT NULL AND @cond <> N'' AND @cond <> N'1=1'
            SET @push_sql = @cond;
        ELSE
            SET @push_sql = NULL;
        SET @residual = NULL;
    END
    ELSE
    BEGIN
        SET @push_sql = NULL;
        SET @residual = @filter;
    END;

ret:
    DECLARE @out_push NVARCHAR(MAX) = CASE
        WHEN @push_sql IS NULL THEN N'null'
        ELSE N'"' + STRING_ESCAPE(@push_sql, 'json') + N'"'
    END;
    DECLARE @out_res NVARCHAR(MAX) = ISNULL(@residual, N'null');
    RETURN N'{"push":' + @out_push + N',"residual":' + @out_res + N'}';
END;
GO


-- ===== 17_pvt_expr.sql =====
-- =====================================================================
-- 17_pvt_expr.sql  (MSSql v2-pvt) — expression-form pushdown classifier
-- ---------------------------------------------------------------------
-- Port of PG's `pvt_expr_field_names` / `pvt_expr_is_base_only`
-- (see redb.Postgres/sql/v2-pvt/17_pvt_expr.sql), tailored for T-SQL
-- scalar UDF constraints.
--
-- Unlike PG we DO NOT reintroduce a separate scalar-expression compiler
-- here. T-SQL already has the proven `pvt_b2_expr_sql` (file 14) and
-- `pvt_build_where_from_json` (file 14) which together cover the same
-- scalar + predicate surface used by Pro/PG. The split optimizer
-- (file 16) reuses them to emit the pushed-down SQL once
-- pvt_expr_is_base_only confirms every `$field` reference resolves to
-- a base column.
--
-- Function:
--   dbo.pvt_expr_is_base_only(@node, @fields) RETURNS BIT
--     1 iff every "$field" reference inside @node has metadata with
--     kind='base' in @fields. NULL / non-JSON nodes are vacuously base.
--     Returns 0 if a referenced field has no metadata at all (so a
--     mistyped $field name does not silently get pushed down).
--
-- Walker rules:
--   * Recurses into every object / array value via OPENJSON.
--   * Treats a `$const` key as opaque (its value is a literal and may
--     contain strings shaped like field names — must not be walked).
--   * Treats a `$field` STRING value as a field reference.
--
-- Limitations vs the PG walker:
--   * Does not special-case unit-string literals of $cast / $dateAdd /
--     $dateDiff / $dateTrunc / $fts.language. In practice those are
--     plain JSON strings (type=1 in OPENJSON), never objects with a
--     `$field` key, so the naive walk does not generate false positives.
-- =====================================================================

SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER FUNCTION dbo.pvt_expr_is_base_only(
    @node    NVARCHAR(MAX),
    @fields  NVARCHAR(MAX)
)
RETURNS BIT
AS
BEGIN
    -- Vacuously base: NULL / non-JSON / bare scalar literal.
    IF @node IS NULL RETURN 1;
    IF ISJSON(@node) = 0 RETURN 1;

    DECLARE @k NVARCHAR(400), @v NVARCHAR(MAX), @t INT;
    DECLARE @lk NVARCHAR(50);
    DECLARE @meta NVARCHAR(MAX);
    DECLARE @kind NVARCHAR(50);

    DECLARE c CURSOR LOCAL FAST_FORWARD FOR
        SELECT [key], [value], [type] FROM OPENJSON(@node);
    OPEN c;
    FETCH NEXT FROM c INTO @k, @v, @t;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @lk = LOWER(@k);

        -- $const subtree: opaque literal, never walked.
        IF @lk = N'$const'
        BEGIN
            FETCH NEXT FROM c INTO @k, @v, @t;
            CONTINUE;
        END;

        -- $field leaf: must resolve to kind='base'.
        IF @lk = N'$field' AND @t = 1
        BEGIN
            SET @meta = JSON_QUERY(@fields, N'$."' + STRING_ESCAPE(@v, 'json') + N'"');
            IF @meta IS NULL
            BEGIN
                CLOSE c; DEALLOCATE c;
                RETURN 0;
            END;
            SET @kind = JSON_VALUE(@meta, N'$.kind');
            IF @kind IS NULL OR @kind <> N'base'
            BEGIN
                CLOSE c; DEALLOCATE c;
                RETURN 0;
            END;
        END
        -- Object / array value: recurse.
        ELSE IF @t IN (4, 5)
        BEGIN
            IF dbo.pvt_expr_is_base_only(@v, @fields) = 0
            BEGIN
                CLOSE c; DEALLOCATE c;
                RETURN 0;
            END;
        END;
        -- String / number / bool / null leaves not under $field: ignore.

        FETCH NEXT FROM c INTO @k, @v, @t;
    END;
    CLOSE c; DEALLOCATE c;
    RETURN 1;
END;
GO


-- ===== 20_pvt_build_query_sql.sql =====
-- =====================================================================
-- 20_pvt_build_query_sql.sql  (MSSql v2-pvt) — orchestrator
-- ---------------------------------------------------------------------
-- Pure SQL string generation. Output yields a single `_id` column.
--
-- Shapes:
--   A) pure-base (no PROPS field referenced):
--      SELECT [DISTINCT] _id FROM dbo._objects o
--       WHERE o.[_id_scheme] = X AND <where>
--       [ORDER BY ...] [OFFSET ... ROWS FETCH NEXT ... ROWS ONLY]
--
--   C) wide CTE (any PROPS field referenced):
--      WITH _pvt_cte AS (...)
--      SELECT [DISTINCT] [_id] FROM _pvt_cte
--       WHERE <where>
--       [ORDER BY ...] [OFFSET ... ROWS FETCH NEXT ... ROWS ONLY]
--
-- Slice limits: no narrow, no tree, no pushdown.
-- DistinctBy: emulated via ROW_NUMBER() OVER (PARTITION BY key) = 1.
-- MSSql paging: OFFSET/FETCH requires ORDER BY -> inject 'ORDER BY (SELECT 1)'
-- when none specified and paging requested.
-- =====================================================================

SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER FUNCTION dbo.pvt_build_query_sql(
    @scheme_id    BIGINT,
    @filter       NVARCHAR(MAX),
    @limit        INT,
    @offset       INT,
    @order        NVARCHAR(MAX),
    @max_depth    INT,
    @distinct     BIT,
    @source_mode  NVARCHAR(50),
    @tree_ids     NVARCHAR(MAX),
    @include_seed BIT,
    @polymorphic  BIT,
    @distinct_on  NVARCHAR(MAX)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @scheme_id IS NULL
        RETURN NULL;
    IF @source_mode IS NULL SET @source_mode = N'flat';

    -- Guard: @distinct=1 and @distinct_on not null are mutually exclusive.
    -- Scalar UDFs cannot THROW; force a runtime cast error whose message
    -- contains 'mutually exclusive' so callers can detect the guard.
    IF @distinct = 1 AND @distinct_on IS NOT NULL AND ISJSON(@distinct_on) = 1
    BEGIN
        DECLARE @_guard INT = CAST(N'p_distinct and p_distinct_on are mutually exclusive' AS INT);
    END;

    -- Merge @distinct_on entries into @order for field collection so that
    -- any PROPS field referenced in @distinct_on triggers Shape C correctly.
    DECLARE @collect_order NVARCHAR(MAX) =
        CASE
            WHEN @distinct_on IS NULL OR ISJSON(@distinct_on) = 0 THEN @order
            WHEN @order IS NULL OR ISJSON(@order) = 0             THEN @distinct_on
            ELSE LEFT(@order, LEN(@order) - 1) + N',' + SUBSTRING(@distinct_on, 2, LEN(@distinct_on) - 1)
        END;

    DECLARE @fields NVARCHAR(MAX) = dbo.pvt_collect_fields(@scheme_id, @filter, @collect_order);

    -- Pure-base shortcut: every collected field has kind='base'
    DECLARE @has_props BIT = 0;
    DECLARE @has_nested BIT = 0;     -- any nested-dict field (parent_sid + dict_key)
    DECLARE @has_scalar BIT = 0;     -- any PROPS field that is NOT nested-dict
    IF @fields IS NOT NULL AND ISJSON(@fields) = 1 AND @fields <> N'{}'
    BEGIN
        IF EXISTS (
            SELECT 1
              FROM OPENJSON(@fields)
             WHERE JSON_VALUE([value], '$.kind') <> N'base'
        )
            SET @has_props = 1;
        IF EXISTS (
            SELECT 1
              FROM OPENJSON(@fields)
             WHERE JSON_VALUE([value], '$.parent_sid') IS NOT NULL
               AND JSON_VALUE([value], '$.dict_key')   IS NOT NULL
        )
            SET @has_nested = 1;
        IF EXISTS (
            SELECT 1
              FROM OPENJSON(@fields)
             WHERE JSON_VALUE([value], '$.kind') <> N'base'
               AND NOT (
                       JSON_VALUE([value], '$.parent_sid') IS NOT NULL
                   AND JSON_VALUE([value], '$.dict_key')   IS NOT NULL
                   )
        )
            SET @has_scalar = 1;
    END;

    -- Narrow vs wide decision (PG parity, see redb.Postgres/sql/v2-pvt/20_pvt_build_query_sql.sql).
    -- @force_outer = 1 means the wide LEFT JOIN shape is mandatory:
    --   - any absence check ($null / $isNull) needs LEFT JOIN to surface NULL pivot rows,
    --   - pure-base / empty-field queries do not need a pivot at all.
    -- @narrow = 1 means we can use the Pro-shape narrow pivot
    -- (FROM _values v / GROUP BY 1 / IN (SELECT FROM _objects)) and JOIN _objects in the outer SELECT.
    -- Mixed (scalar + nested-dict in the same query) forces wide because the nested side CTE expects pi.* projection.
    DECLARE @has_null   BIT = dbo.pvt_has_absence_check(@filter);
    DECLARE @force_outer BIT = CASE
        WHEN @has_null = 1 OR @has_props = 0 THEN 1
        ELSE 0
    END;
    -- Narrow with nested: 12 emits a slim pivot (GROUP BY o.[_id]) and
    -- the orchestrator JOINs _objects in the outer SELECT, so the heavy
    -- 21-col GROUP BY is gone even when nested-dict fields are present.
    DECLARE @narrow BIT = CASE
        WHEN @force_outer = 0 THEN 1
        ELSE 0
    END;

    DECLARE @paging NVARCHAR(MAX) = N'';
    IF @limit IS NOT NULL AND @limit >= 0
    BEGIN
        IF COALESCE(@offset, 0) = 0
            SET @paging = CHAR(10) + N'OFFSET 0 ROWS FETCH NEXT ' + CAST(@limit AS NVARCHAR(20)) + N' ROWS ONLY';
        ELSE
            SET @paging = CHAR(10) + N'OFFSET ' + CAST(@offset AS NVARCHAR(20))
                        + N' ROWS FETCH NEXT ' + CAST(@limit AS NVARCHAR(20)) + N' ROWS ONLY';
    END
    ELSE IF COALESCE(@offset, 0) > 0
        SET @paging = CHAR(10) + N'OFFSET ' + CAST(@offset AS NVARCHAR(20)) + N' ROWS';

    -- Outer alias prefix for base columns: Shape A and narrow Shape B both expose
    -- base columns on alias `o` (via JOIN _objects); legacy wide Shape C exposes
    -- them on `_pvt_cte` (projected by the wide GROUP BY). Pivot/scalar columns
    -- always render unprefixed (pvt_build_field_condition/_order skip the prefix
    -- for kind <> 'base'), so unique pivot names resolve to _pvt_cte automatically.
    DECLARE @base_prefix NVARCHAR(10) = CASE
        WHEN @has_props = 0 OR @narrow = 1 THEN N'o.'
        ELSE N'_pvt_cte.'
    END;
    DECLARE @order_sql NVARCHAR(MAX) = dbo.pvt_build_order_conditions(@order, @fields, @base_prefix);
    -- T-SQL forbids ORDER BY inside subqueries unless paired with TOP/OFFSET-FETCH.
    -- The orchestrator output is always wrapped as a derived table by the C# layer,
    -- so drop ORDER BY when paging is absent and supply a stable default when
    -- present. Using @base_prefix + [_id] gives deterministic paging (vs the old
    -- (SELECT 1) placeholder which left row order to the planner).
    IF @paging <> N'' AND @order_sql = N''
        SET @order_sql = CHAR(10) + N'ORDER BY ' + @base_prefix + N'[_id]';
    ELSE IF @paging = N''
        SET @order_sql = N'';

    -- DistinctBy emulation: T-SQL has no SELECT DISTINCT ON; use
    -- ROW_NUMBER() OVER (PARTITION BY key ORDER BY (SELECT 1)) and
    -- filter to rn=1 in an outer wrapper. Resolve the partition column.
    -- Stored already alias-qualified per current shape:
    --   base col  -> @base_prefix + [DbCol]   (e.g. o.[_name] / _pvt_cte.[_name])
    --   props col -> _pvt_cte.[FieldName]     (pivot cols always live in _pvt_cte)
    DECLARE @rn_col NVARCHAR(MAX) = NULL;
    IF @distinct_on IS NOT NULL AND ISJSON(@distinct_on) = 1
    BEGIN
        DECLARE @don_field NVARCHAR(400) = JSON_VALUE(@distinct_on, '$[0].field');
        IF @don_field IS NOT NULL AND @don_field <> N''
        BEGIN
            DECLARE @don_meta NVARCHAR(MAX) = JSON_QUERY(@fields, N'$."' + STRING_ESCAPE(@don_field, 'json') + N'"');
            IF @don_meta IS NOT NULL AND JSON_VALUE(@don_meta, '$.kind') = N'base'
                SET @rn_col = @base_prefix + QUOTENAME(JSON_VALUE(@don_meta, '$.column'));
            ELSE
                SET @rn_col = N'_pvt_cte.' + QUOTENAME(@don_field);
        END;
    END;

    -- ---------- Tree mode branch ----------
    -- Handles tree_roots, tree_leaves, tree_children, tree_descendants, tree_ancestors.
    -- When @has_props = 0: emit a flat subquery-safe SELECT against _objects using
    -- the corresponding tree predicate; no WITH-CTE prefix so the result can be
    -- wrapped in a derived table by the C# layer.
    -- When @has_props = 1: delegate to pvt_build_cte_sql which natively supports
    -- every tree mode (it injects o.[_id] IN (SELECT _id FROM dbo.pvt_tree_*(...)))
    -- and then wrap with the Shape C outer SELECT / residual WHERE.
    IF @source_mode IN (N'tree_roots', N'tree_leaves', N'tree_children',
                        N'tree_descendants', N'tree_ancestors')
    BEGIN
        IF @has_props = 1
        BEGIN
            DECLARE @splitT NVARCHAR(MAX) = dbo.pvt_split_filter(@filter, @fields);
            DECLARE @pushT  NVARCHAR(MAX) = JSON_VALUE(@splitT, N'$.push');
            DECLARE @resT   NVARCHAR(MAX) = JSON_QUERY(@splitT, N'$.residual');
            DECLARE @innerT NVARCHAR(MAX) = dbo.pvt_build_cte_sql(
                @scheme_id, @fields, @source_mode, @tree_ids, @max_depth,
                @force_outer, @pushT, @narrow, COALESCE(@include_seed, 1), COALESCE(@polymorphic, 1), DEFAULT);
            IF @innerT IS NULL RETURN NULL;
            DECLARE @whereTp NVARCHAR(MAX) = dbo.pvt_build_where_from_json(@resT, @fields, @base_prefix);
            DECLARE @joinT   NVARCHAR(MAX) = CASE
                WHEN @narrow = 1 THEN CHAR(10) + N'JOIN dbo._objects o ON o.[_id] = _pvt_cte.[_id_object]'
                ELSE N''
            END;
            DECLARE @rowSelT NVARCHAR(MAX) = CASE WHEN @narrow = 1 THEN N'o.[_id]' ELSE N'_pvt_cte.[_id]' END;
            IF @rn_col IS NOT NULL
            BEGIN
                -- Project the full row-source so the outer derived table can be
                -- aliased with the same prefix `@order_sql` uses (`o.` or
                -- `_pvt_cte.`). Otherwise base-field ORDER BY references can't
                -- resolve in the outer scope (the ROW_NUMBER wrapper hides `o`).
                DECLARE @rn_aliasT NVARCHAR(40) = CASE WHEN @narrow = 1 THEN N'o' ELSE N'_pvt_cte' END;
                RETURN N'SELECT [_id] FROM (' + CHAR(10)
                     + N'SELECT ' + @rn_aliasT + N'.*, ROW_NUMBER() OVER (PARTITION BY ' + @rn_col + N' ORDER BY (SELECT 1)) AS [_rn]' + CHAR(10)
                     + N'FROM (' + CHAR(10) + @innerT + CHAR(10) + N') _pvt_cte' + @joinT + CHAR(10)
                     + N'WHERE ' + @whereTp + CHAR(10)
                     + N') ' + @rn_aliasT + CHAR(10) + N'WHERE ' + @rn_aliasT + N'.[_rn] = 1'
                     + @order_sql + @paging;
            END;
            -- T-SQL forbids ORDER BY items not in SELECT when DISTINCT.
            -- Wrap so paging-ORDER applies outside the DISTINCT.
            IF @distinct = 1
                RETURN N'SELECT [_id] FROM (' + CHAR(10)
                     + N'SELECT DISTINCT ' + @rowSelT + N' AS [_id] FROM ('
                     + CHAR(10) + @innerT + CHAR(10) + N') _pvt_cte' + @joinT
                     + CHAR(10) + N'WHERE ' + @whereTp + CHAR(10)
                     + N') _dist'
                     + @order_sql + @paging;
            RETURN N'SELECT '
                 + @rowSelT + N' FROM (' + CHAR(10) + @innerT + CHAR(10) + N') _pvt_cte' + @joinT
                 + CHAR(10) + N'WHERE ' + @whereTp
                 + @order_sql + @paging;
        END;

        DECLARE @whereT  NVARCHAR(MAX) = dbo.pvt_build_where_from_json(@filter, @fields, N'o.');
        DECLARE @filterT NVARCHAR(MAX) = CASE
            WHEN @whereT IS NULL OR @whereT = N'' OR @whereT = N'1=1' THEN N''
            ELSE N' AND ' + @whereT
        END;

        DECLARE @escapedIds NVARCHAR(MAX) = NULL;
        IF @tree_ids IS NOT NULL AND ISJSON(@tree_ids) = 1
            SET @escapedIds = REPLACE(@tree_ids, N'''', N'''''');

        DECLARE @treeWhere NVARCHAR(MAX);

        IF @source_mode = N'tree_roots'
            SET @treeWhere = N'o.[_id_parent] IS NULL';
        ELSE IF @source_mode = N'tree_leaves'
            SET @treeWhere = N'NOT EXISTS (SELECT 1 FROM dbo._objects _lc WHERE _lc.[_id_parent] = o.[_id])';
        ELSE IF @source_mode = N'tree_children'
        BEGIN
            IF @escapedIds IS NULL RETURN NULL;
            SET @treeWhere = N'o.[_id_parent] IN (SELECT CAST([value] AS BIGINT) FROM OPENJSON(N''' + @escapedIds + N'''))';
        END;
        ELSE IF @source_mode = N'tree_ancestors'
        BEGIN
            IF @escapedIds IS NULL RETURN NULL;
            SET @treeWhere = N'o.[_id] IN (SELECT _id FROM dbo.pvt_tree_ancestors('
                + CAST(@scheme_id AS NVARCHAR(40)) + N', N''' + @escapedIds + N''', '
                + CASE WHEN @max_depth IS NULL THEN N'NULL' ELSE CAST(@max_depth AS NVARCHAR(20)) END + N', '
                + CAST(COALESCE(@polymorphic, 1) AS NVARCHAR(1)) + N'))';
        END;
        ELSE  -- tree_descendants: correlated pvt_is_descendant_of, no CTE
        BEGIN
            IF @escapedIds IS NULL RETURN NULL;
            SET @treeWhere = N'EXISTS (SELECT 1 FROM OPENJSON(N''' + @escapedIds
                + N''') _s WHERE dbo.pvt_is_descendant_of(o.[_id], CAST(_s.[value] AS BIGINT)) = 1)';
            IF COALESCE(@include_seed, 1) = 0
                SET @treeWhere += N' AND o.[_id] NOT IN (SELECT CAST([value] AS BIGINT) FROM OPENJSON(N''' + @escapedIds + N'''))';
        END;

        RETURN N'SELECT o.[_id] FROM dbo._objects o'
             + CHAR(10) + N'WHERE o.[_id_scheme] = ' + CAST(@scheme_id AS NVARCHAR(40))
             + N' AND ' + @treeWhere
             + @filterT
             + @order_sql + @paging;
    END;

    -- Unrecognized non-flat source mode: not yet implemented
    IF @source_mode <> N'flat'
        RETURN NULL;

    -- ---------- Shape A (pure-base) ----------
    IF @has_props = 0
    BEGIN
        DECLARE @whereA  NVARCHAR(MAX) = dbo.pvt_build_where_from_json(@filter, @fields, N'o.');
        DECLARE @baseFrom NVARCHAR(MAX) = N'FROM dbo._objects o'
            + CHAR(10) + N'WHERE o.[_id_scheme] = ' + CAST(@scheme_id AS NVARCHAR(40))
            + CASE WHEN @whereA <> N'1=1' THEN N' AND ' + @whereA ELSE N'' END;
        IF @rn_col IS NOT NULL
            -- Project o.* so the outer derived table aliased `o` exposes every
            -- base column ORDER BY may reference (paging-only ORDER BY uses
            -- `(SELECT 1)`, but a user ORDER BY can hit any base field).
            RETURN N'SELECT [_id] FROM (' + CHAR(10)
                 + N'SELECT o.*, ROW_NUMBER() OVER (PARTITION BY ' + @rn_col + N' ORDER BY (SELECT 1)) AS [_rn]' + CHAR(10)
                 + @baseFrom + CHAR(10)
                 + N') o' + CHAR(10) + N'WHERE o.[_rn] = 1'
                 + @order_sql + @paging;
        IF @distinct = 1
            RETURN N'SELECT [_id] FROM (' + CHAR(10)
                 + N'SELECT DISTINCT o.[_id] AS [_id] ' + @baseFrom + CHAR(10)
                 + N') _dist'
                 + @order_sql + @paging;
        RETURN N'SELECT o.[_id] ' + @baseFrom
             + @order_sql + @paging;
    END;

    -- ---------- Shape B (narrow Pro-parity) / Shape C (wide pivot) ----------
    -- Pro-parity base pushdown: peel off base/hierarchical predicates so
    -- they narrow _objects BEFORE the correlated pivot subqueries execute
    -- per row. Residual stays on the outer WHERE.
    DECLARE @splitC NVARCHAR(MAX) = dbo.pvt_split_filter(@filter, @fields);
    DECLARE @pushC  NVARCHAR(MAX) = JSON_VALUE(@splitC, N'$.push');
    DECLARE @resC   NVARCHAR(MAX) = JSON_QUERY(@splitC, N'$.residual');
    DECLARE @inner  NVARCHAR(MAX) = dbo.pvt_build_cte_sql(
        @scheme_id, @fields, N'flat', NULL, NULL, @force_outer, @pushC, @narrow, DEFAULT, DEFAULT, DEFAULT);
    DECLARE @whereC NVARCHAR(MAX) = dbo.pvt_build_where_from_json(@resC, @fields, @base_prefix);
    DECLARE @joinC  NVARCHAR(MAX) = CASE
        WHEN @narrow = 1 THEN CHAR(10) + N'JOIN dbo._objects o ON o.[_id] = _pvt_cte.[_id_object]'
        ELSE N''
    END;
    DECLARE @rowSelC NVARCHAR(MAX) = CASE WHEN @narrow = 1 THEN N'o.[_id]' ELSE N'_pvt_cte.[_id]' END;
    IF @rn_col IS NOT NULL
    BEGIN
        -- Alias outer derived table with the same prefix `@order_sql` uses
        -- (`o.` or `_pvt_cte.`) so base-field references in ORDER BY resolve.
        DECLARE @rn_aliasC NVARCHAR(40) = CASE WHEN @narrow = 1 THEN N'o' ELSE N'_pvt_cte' END;
        RETURN N'SELECT [_id] FROM (' + CHAR(10)
             + N'SELECT ' + @rn_aliasC + N'.*, ROW_NUMBER() OVER (PARTITION BY ' + @rn_col + N' ORDER BY (SELECT 1)) AS [_rn]' + CHAR(10)
             + N'FROM (' + CHAR(10) + @inner + CHAR(10) + N') _pvt_cte' + @joinC + CHAR(10)
             + N'WHERE ' + @whereC + CHAR(10)
             + N') ' + @rn_aliasC + CHAR(10) + N'WHERE ' + @rn_aliasC + N'.[_rn] = 1'
             + @order_sql + @paging;
    END;
    IF @distinct = 1
        RETURN N'SELECT [_id] FROM (' + CHAR(10)
             + N'SELECT DISTINCT ' + @rowSelC + N' AS [_id] FROM ('
             + CHAR(10) + @inner + CHAR(10) + N') _pvt_cte' + @joinC
             + CHAR(10) + N'WHERE ' + @whereC + CHAR(10)
             + N') _dist'
             + @order_sql + @paging;
    RETURN N'SELECT '
         + @rowSelC + N' FROM (' + CHAR(10) + @inner + CHAR(10) + N') _pvt_cte' + @joinC
         + CHAR(10) + N'WHERE ' + @whereC
         + @order_sql + @paging;
END;
GO


-- ===== 21_pvt_aggregate.sql =====
-- =====================================================================
-- 21_pvt_aggregate.sql  (MSSql v2-pvt) — terminal aggregation orchestrator
-- ---------------------------------------------------------------------
-- Mirrors PG redb.Postgres/sql/v2-pvt/21_pvt_aggregate.sql, slimmed to
-- the operand grammar consumed by RedbQueryable.AggregateAsync and the
-- legacy ExecuteAggregateAsync / ExecuteAggregateBatchAsync paths.
--
-- Aggregation entry shape (per element of @aggs JSON array):
--
--   { "alias": "<name>", "$<func>": "*" | {"$field": "<path>"} }
--
-- Where <func> is one of: count / sum / avg / min / max.
--
-- NOT in this slice (matches MSSql free deliberate cuts):
--   * per-aggregate `distinct` / `filter`
--   * $string_agg / $bool_and / $bool_or
--   * arithmetic / function operands ($add/$mul/$upper/...)
--   * narrow shape (always Shape A or Shape C wide)
--   * pushdown / tree / polymorphic flags
--
-- Output shapes:
--   Shape A (every collected field is kind='base', flat mode):
--      SELECT <agg1> AS [<a1>], ... FROM dbo._objects o
--       WHERE o.[_id_scheme] = X [AND <where>]
--
--   Shape C (any PROPS field referenced anywhere):
--      SELECT <agg1> AS [<a1>], ... FROM (<pvt_build_cte_sql>) _pvt_cte
--       WHERE <where>
-- =====================================================================

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

-- ---------- pvt_extend_fields_with_aggs ------------------------------
-- Start from @existing fields-map (output of pvt_collect_fields on the
-- filter) and APPEND metadata for every $field path referenced inside
-- @aggs that is not already present. Returns a new fields-map JSON.
-- Paths absent from scheme metadata are silently skipped.
CREATE OR ALTER FUNCTION dbo.pvt_extend_fields_with_aggs(
    @scheme_id BIGINT,
    @existing  NVARCHAR(MAX),
    @aggs      NVARCHAR(MAX)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    DECLARE @out NVARCHAR(MAX) =
        CASE WHEN @existing IS NULL OR ISJSON(@existing) = 0 THEN N'{}' ELSE @existing END;
    IF @scheme_id IS NULL OR @aggs IS NULL OR ISJSON(@aggs) = 0
        RETURN @out;

    DECLARE entries CURSOR LOCAL FAST_FORWARD FOR
        SELECT [value] FROM OPENJSON(@aggs);
    DECLARE @entry NVARCHAR(MAX);
    OPEN entries;
    FETCH NEXT FROM entries INTO @entry;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @entry IS NOT NULL AND ISJSON(@entry) = 1
        BEGIN
            DECLARE props CURSOR LOCAL FAST_FORWARD FOR
                SELECT [key], [value], [type] FROM OPENJSON(@entry);
            DECLARE @k NVARCHAR(100), @v NVARCHAR(MAX), @t INT;
            OPEN props;
            FETCH NEXT FROM props INTO @k, @v, @t;
            WHILE @@FETCH_STATUS = 0
            BEGIN
                IF LEFT(@k, 1) = N'$' AND @t = 5
                BEGIN
                    DECLARE @path NVARCHAR(400) = JSON_VALUE(@v, N'$."$field"');
                    IF @path IS NOT NULL
                       AND JSON_QUERY(@out, N'$."' + STRING_ESCAPE(@path, 'json') + N'"') IS NULL
                    BEGIN
                        DECLARE @meta NVARCHAR(MAX) = dbo.pvt_resolve_field_path(@scheme_id, @path);
                        IF @meta IS NOT NULL
                        BEGIN
                            IF @out = N'{}'
                                SET @out = N'{"' + STRING_ESCAPE(@path, 'json') + N'":' + @meta + N'}';
                            ELSE
                                SET @out = LEFT(@out, LEN(@out) - 1)
                                         + N',"' + STRING_ESCAPE(@path, 'json') + N'":' + @meta + N'}';
                        END;
                    END;
                END;
                FETCH NEXT FROM props INTO @k, @v, @t;
            END;
            CLOSE props; DEALLOCATE props;
        END;
        FETCH NEXT FROM entries INTO @entry;
    END;
    CLOSE entries; DEALLOCATE entries;
    RETURN @out;
END;
GO


-- ---------- pvt_agg_field_ref ----------------------------------------
-- Resolve a single $field operand to its column expression. Returns
-- NULL when meta is missing (caller emits a comment marker).
CREATE OR ALTER FUNCTION dbo.pvt_agg_field_ref(
    @path        NVARCHAR(400),
    @fields      NVARCHAR(MAX),
    @base_prefix NVARCHAR(20)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @path IS NULL OR @fields IS NULL OR ISJSON(@fields) = 0
        RETURN NULL;
    DECLARE @meta NVARCHAR(MAX) =
        JSON_QUERY(@fields, N'$."' + STRING_ESCAPE(@path, 'json') + N'"');
    IF @meta IS NULL
        RETURN NULL;
    DECLARE @kind NVARCHAR(32) = JSON_VALUE(@meta, N'$.kind');
    IF @kind = N'base'
    BEGIN
        DECLARE @col NVARCHAR(64) = JSON_VALUE(@meta, N'$.column');
        IF @col IS NULL
            RETURN NULL;
        RETURN COALESCE(@base_prefix, N'') + QUOTENAME(@col);
    END;
    -- Prop / list_item / dict — exposed as a pivot-CTE column under its field-name alias.
    -- Narrow shape (base_prefix='o.') JOINs _objects so the outer SELECT has both
    -- `o.*` (base) and `_pvt_cte.<field>` (pivot). Pivot cols must NOT carry `o.`
    -- — they are unprefixed (resolved to _pvt_cte by scope). In wide shape
    -- (base_prefix='_pvt_cte.') both base and pivot live on _pvt_cte, so the
    -- same prefix applies.
    RETURN CASE WHEN @base_prefix = N'o.' THEN N''
                ELSE COALESCE(@base_prefix, N'') END
         + QUOTENAME(@path);
END;
GO


-- ---------- pvt_build_agg_expr ---------------------------------------
-- Compile a single aggregate entry into a SQL fragment WITHOUT alias.
-- Supports $count / $sum / $avg / $min / $max with operand:
--   "*"                          (count only)
--   { "$field": "<path>" }
--
-- Array-field operands (is_array=true, kind <> 'base') route through a
-- correlated subquery against dbo._values so SUM/AVG/MIN/MAX/COUNT
-- aggregate element values instead of failing on the JSON-array string
-- emitted by the wide pivot. Mirrors PG pvt_build_agg_expr unnest path
-- (redb.Postgres/sql/v2-pvt/19_pvt_agg_expr.sql), using @base_prefix to
-- resolve the per-row object id (`<prefix>[_id]`) for the correlation.
CREATE OR ALTER FUNCTION dbo.pvt_build_agg_expr(
    @entry       NVARCHAR(MAX),
    @fields      NVARCHAR(MAX),
    @base_prefix NVARCHAR(20)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @entry IS NULL OR ISJSON(@entry) = 0
        RETURN N'/* pvt_build_agg_expr: entry not JSON */ NULL';

    DECLARE @op NVARCHAR(50) = NULL, @arg NVARCHAR(MAX) = NULL, @arg_t INT = NULL;
    DECLARE c CURSOR LOCAL FAST_FORWARD FOR
        SELECT [key], [value], [type] FROM OPENJSON(@entry) WHERE LEFT([key], 1) = N'$';
    OPEN c;
    FETCH NEXT FROM c INTO @op, @arg, @arg_t;
    CLOSE c; DEALLOCATE c;
    IF @op IS NULL
        RETURN N'/* pvt_build_agg_expr: no $<func> key */ NULL';

    DECLARE @lo NVARCHAR(50) = LOWER(@op);
    DECLARE @func NVARCHAR(20) = CASE @lo
        WHEN N'$count' THEN N'COUNT'
        WHEN N'$sum'   THEN N'SUM'
        WHEN N'$avg'   THEN N'AVG'
        WHEN N'$min'   THEN N'MIN'
        WHEN N'$max'   THEN N'MAX'
        ELSE NULL
    END;
    IF @func IS NULL
        RETURN N'/* pvt_build_agg_expr: unsupported operator ' + @op + N' */ NULL';

    -- $count "*" → COUNT(*)
    IF @lo = N'$count' AND @arg_t = 1 AND @arg = N'*'
        RETURN N'COUNT(*)';

    -- Operand must be {"$field": "<path>"}.
    DECLARE @path NVARCHAR(400) = NULL;
    IF @arg_t = 5 AND ISJSON(@arg) = 1
        SET @path = JSON_VALUE(@arg, N'$."$field"');
    IF @path IS NULL
        RETURN N'/* pvt_build_agg_expr: operand of ' + @op + N' must be "*" or {"$field":"..."} */ NULL';

    -- Array-field branch: detect is_array=true & non-base. The pivot
    -- column would project the array as JSON, so the orchestrator
    -- emits an OUTER APPLY against _values that pre-computes the
    -- per-row aggregates (sum/cnt/min/max) under the alias
    -- `_aa_<sid>`; each aggregate then references the precomputed
    -- column rather than nesting an aggregate subquery (which T-SQL
    -- forbids). Mirrors PG's unnest path semantically.
    DECLARE @meta NVARCHAR(MAX) =
        JSON_QUERY(@fields, N'$."' + STRING_ESCAPE(@path, 'json') + N'"');
    IF @meta IS NOT NULL
       AND JSON_VALUE(@meta, N'$.is_array') = N'true'
       AND COALESCE(JSON_VALUE(@meta, N'$.kind'), N'') <> N'base'
    BEGIN
        DECLARE @sid NVARCHAR(40) = JSON_VALUE(@meta, N'$.sid');
        IF @sid IS NULL
            RETURN N'/* pvt_build_agg_expr: array field "' + @path + N'" missing sid */ NULL';
        DECLARE @aa NVARCHAR(80) = N'_aa_' + @sid;
        IF @lo = N'$sum'   RETURN N'SUM(' + @aa + N'.[sum])';
        IF @lo = N'$count' RETURN N'SUM(' + @aa + N'.[cnt])';
        IF @lo = N'$min'   RETURN N'MIN(' + @aa + N'.[min])';
        IF @lo = N'$max'   RETURN N'MAX(' + @aa + N'.[max])';
        IF @lo = N'$avg'
            RETURN N'(SUM(' + @aa + N'.[sum]) / NULLIF(SUM(' + @aa + N'.[cnt]), 0))';
        RETURN N'/* pvt_build_agg_expr: array operator ' + @op + N' unsupported */ NULL';
    END;

    DECLARE @col NVARCHAR(MAX) = dbo.pvt_agg_field_ref(@path, @fields, @base_prefix);
    IF @col IS NULL
        RETURN N'/* pvt_build_agg_expr: unknown field "' + @path + N'" */ NULL';

    -- Promote SUM / AVG operands to NUMERIC(38,10) for overflow safety and
    -- to match Pro's decimal-typed public surface.
    IF @lo = N'$sum'
        RETURN N'SUM(CAST(' + @col + N' AS NUMERIC(38, 10)))';
    IF @lo = N'$avg'
        RETURN N'AVG(CAST(' + @col + N' AS NUMERIC(38, 10)))';

    RETURN @func + N'(' + @col + N')';
END;
GO


-- ---------- pvt_build_agg_projection ---------------------------------
-- Compile an array of aggregate entries into a `<sql> AS [<alias>], ...`
-- projection fragment. Defaults aliases to positional `_agg_<i>`.
CREATE OR ALTER FUNCTION dbo.pvt_build_agg_projection(
    @aggs        NVARCHAR(MAX),
    @fields      NVARCHAR(MAX),
    @base_prefix NVARCHAR(20)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @aggs IS NULL OR ISJSON(@aggs) = 0
        RETURN NULL;

    DECLARE @out NVARCHAR(MAX) = N'';
    DECLARE @idx INT = 0;

    DECLARE c CURSOR LOCAL FAST_FORWARD FOR
        SELECT [value] FROM OPENJSON(@aggs);
    DECLARE @entry NVARCHAR(MAX);
    OPEN c;
    FETCH NEXT FROM c INTO @entry;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @idx = @idx + 1;
        DECLARE @alias NVARCHAR(200) = NULL;
        IF @entry IS NOT NULL AND ISJSON(@entry) = 1
            SET @alias = JSON_VALUE(@entry, N'$.alias');
        IF @alias IS NULL OR @alias = N''
            SET @alias = N'_agg_' + CAST(@idx AS NVARCHAR(20));

        DECLARE @sql NVARCHAR(MAX) = dbo.pvt_build_agg_expr(@entry, @fields, @base_prefix);
        IF @idx > 1 SET @out = @out + N', ';
        SET @out = @out + @sql + N' AS ' + QUOTENAME(@alias);
        FETCH NEXT FROM c INTO @entry;
    END;
    CLOSE c; DEALLOCATE c;

    IF @idx = 0
        RETURN NULL;
    RETURN @out;
END;
GO


-- ---------- pvt_build_aggregate_sql ----------------------------------
-- Orchestrator. Returns a single-row terminal SELECT projecting N
-- aggregate columns over the (filtered) PVT source for @scheme_id.
CREATE OR ALTER FUNCTION dbo.pvt_build_aggregate_sql(
    @scheme_id   BIGINT,
    @filter      NVARCHAR(MAX),
    @aggs        NVARCHAR(MAX),
    @source_mode NVARCHAR(50)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @scheme_id IS NULL
        RETURN NULL;
    IF @aggs IS NULL OR ISJSON(@aggs) = 0
        RETURN NULL;
    IF @source_mode IS NULL SET @source_mode = N'flat';
    IF @source_mode <> N'flat'
        RETURN NULL;

    -- 1. Collect fields from filter; extend with fields referenced inside
    --    aggregate operands so kind/column metadata is available downstream.
    DECLARE @filter_fields NVARCHAR(MAX) = dbo.pvt_collect_fields(@scheme_id, @filter, NULL);
    DECLARE @fields        NVARCHAR(MAX) =
        dbo.pvt_extend_fields_with_aggs(@scheme_id, @filter_fields, @aggs);

    -- 2. Decide shape — pure-base flat shortcut (Shape A) vs Shape B/C.
    --    Mirrors PG redb.Postgres/sql/v2-pvt/21_pvt_aggregate.sql narrow/wide selection.
    DECLARE @has_props  BIT = 0;
    DECLARE @has_nested BIT = 0;
    DECLARE @has_scalar BIT = 0;
    IF @fields IS NOT NULL AND ISJSON(@fields) = 1 AND @fields <> N'{}'
    BEGIN
        IF EXISTS (
            SELECT 1 FROM OPENJSON(@fields)
             WHERE JSON_VALUE([value], N'$.kind') <> N'base'
        ) SET @has_props = 1;
        IF EXISTS (
            SELECT 1 FROM OPENJSON(@fields)
             WHERE JSON_VALUE([value], N'$.parent_sid') IS NOT NULL
               AND JSON_VALUE([value], N'$.dict_key')   IS NOT NULL
        ) SET @has_nested = 1;
        IF EXISTS (
            SELECT 1 FROM OPENJSON(@fields)
             WHERE JSON_VALUE([value], N'$.kind') <> N'base'
               AND NOT (
                       JSON_VALUE([value], N'$.parent_sid') IS NOT NULL
                   AND JSON_VALUE([value], N'$.dict_key')   IS NOT NULL
                   )
        ) SET @has_scalar = 1;
    END;

    -- ---------- Shape A: pure-base flat aggregation ----------
    IF @has_props = 0
    BEGIN
        DECLARE @whereA NVARCHAR(MAX) = dbo.pvt_build_where_from_json(@filter, @fields, N'o.');
        DECLARE @projA  NVARCHAR(MAX) = dbo.pvt_build_agg_projection(@aggs, @fields, N'o.');
        RETURN N'SELECT ' + @projA
             + N' FROM dbo._objects o'
             + CHAR(10) + N'WHERE o.[_id_scheme] = ' + CAST(@scheme_id AS NVARCHAR(40))
             + CASE WHEN @whereA <> N'1=1' THEN N' AND ' + @whereA ELSE N'' END;
    END;

    -- ---------- Shape B (narrow) or Shape C (wide) ----------
    -- PG-parity narrow/wide decision (mirrors file 20).
    --   @force_outer = 1 -> wide is mandatory (absence checks, no props).
    --   @narrow      = 1 -> Pro-shape: narrow GROUP BY pivot + JOIN _objects.
    -- Pro-parity base pushdown: base/hierarchical predicates narrow
    -- _objects BEFORE correlated pivot subqueries; residual stays outer.
    -- Aggregations over array fields are routed via OUTER APPLY blocks
    -- that pre-compute per-row sum/cnt/min/max of element values
    -- (T-SQL forbids aggregate-of-aggregate-subquery, unlike PG which
    -- can SUM((SELECT SUM(_x) FROM unnest(arr))) directly).
    DECLARE @splitC NVARCHAR(MAX) = dbo.pvt_split_filter(@filter, @fields);
    DECLARE @pushC  NVARCHAR(MAX) = JSON_VALUE(@splitC, N'$.push');
    DECLARE @resC   NVARCHAR(MAX) = JSON_QUERY(@splitC, N'$.residual');

    DECLARE @has_null    BIT = dbo.pvt_has_absence_check(@resC);
    DECLARE @force_outer BIT = CASE WHEN @has_null = 1 OR @has_props = 0 THEN 1 ELSE 0 END;
    -- PG parity: narrow body in 12 currently does NOT support nested-dict.
    DECLARE @narrow      BIT = CASE
        WHEN @force_outer = 0 AND @has_nested = 0 THEN 1
        ELSE 0
    END;

    -- Base alias: narrow JOINs _objects, wide projects base cols from _pvt_cte.
    DECLARE @base_prefix NVARCHAR(20) = CASE WHEN @narrow = 1 THEN N'o.' ELSE N'_pvt_cte.' END;
    -- Per-row correlation for OUTER APPLY: narrow uses o.[_id] (joined),
    -- wide uses _pvt_cte.[_id] (projected by the wide GROUP BY).
    DECLARE @aa_row_ref  NVARCHAR(40) = CASE WHEN @narrow = 1 THEN N'o.[_id]' ELSE N'_pvt_cte.[_id]' END;

    DECLARE @inner  NVARCHAR(MAX) = dbo.pvt_build_cte_sql(
        @scheme_id, @fields, N'flat', NULL, NULL, @force_outer, @pushC, @narrow,
        DEFAULT, DEFAULT, DEFAULT);
    DECLARE @whereC NVARCHAR(MAX) = dbo.pvt_build_where_from_json(@resC, @fields, @base_prefix);
    DECLARE @projC  NVARCHAR(MAX) = dbo.pvt_build_agg_projection(@aggs, @fields, @base_prefix);

    -- Collect distinct array fields referenced by aggregations and
    -- emit one OUTER APPLY per (sid, db_column). Each APPLY exposes
    -- columns [sum]/[cnt]/[min]/[max] keyed on the row's [_id].
    DECLARE @apply_sql NVARCHAR(MAX) = N'';
    DECLARE @seen_sids TABLE(sid NVARCHAR(40) PRIMARY KEY);
    DECLARE ac CURSOR LOCAL FAST_FORWARD FOR
        SELECT [value] FROM OPENJSON(@aggs);
    DECLARE @aentry NVARCHAR(MAX);
    OPEN ac;
    FETCH NEXT FROM ac INTO @aentry;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @aentry IS NOT NULL AND ISJSON(@aentry) = 1
        BEGIN
            DECLARE pc CURSOR LOCAL FAST_FORWARD FOR
                SELECT [value], [type] FROM OPENJSON(@aentry) WHERE LEFT([key], 1) = N'$';
            DECLARE @opv NVARCHAR(MAX), @opt INT;
            OPEN pc;
            FETCH NEXT FROM pc INTO @opv, @opt;
            CLOSE pc; DEALLOCATE pc;
            IF @opt = 5 AND ISJSON(@opv) = 1
            BEGIN
                DECLARE @apath NVARCHAR(400) = JSON_VALUE(@opv, N'$."$field"');
                IF @apath IS NOT NULL
                BEGIN
                    DECLARE @ameta NVARCHAR(MAX) =
                        JSON_QUERY(@fields, N'$."' + STRING_ESCAPE(@apath, 'json') + N'"');
                    IF @ameta IS NOT NULL
                       AND JSON_VALUE(@ameta, N'$.is_array') = N'true'
                       AND COALESCE(JSON_VALUE(@ameta, N'$.kind'), N'') <> N'base'
                    BEGIN
                        DECLARE @asid NVARCHAR(40) = JSON_VALUE(@ameta, N'$.sid');
                        DECLARE @adbcol NVARCHAR(64) = JSON_VALUE(@ameta, N'$.db_column');
                        IF @asid IS NOT NULL AND @adbcol IS NOT NULL
                           AND NOT EXISTS (SELECT 1 FROM @seen_sids WHERE sid = @asid)
                        BEGIN
                            INSERT INTO @seen_sids(sid) VALUES(@asid);
                            SET @apply_sql = @apply_sql + CHAR(10)
                                + N'OUTER APPLY (SELECT '
                                + N'SUM(CAST(va.' + QUOTENAME(@adbcol) + N' AS NUMERIC(38, 10))) AS [sum], '
                                + N'COUNT(va.' + QUOTENAME(@adbcol) + N') AS [cnt], '
                                + N'MIN(va.' + QUOTENAME(@adbcol) + N') AS [min], '
                                + N'MAX(va.' + QUOTENAME(@adbcol) + N') AS [max] '
                                + N'FROM dbo._values va '
                                + N'WHERE va.[_id_object] = ' + @aa_row_ref + N' '
                                + N'AND va.[_id_structure] = ' + @asid + N' '
                                + N'AND va.[_array_index] IS NOT NULL) _aa_' + @asid;
                        END;
                    END;
                END;
            END;
        END;
        FETCH NEXT FROM ac INTO @aentry;
    END;
    CLOSE ac; DEALLOCATE ac;

    RETURN N'SELECT ' + @projC
         + N' FROM (' + CHAR(10) + @inner + CHAR(10) + N') _pvt_cte'
         + CASE WHEN @narrow = 1
                THEN CHAR(10) + N'JOIN dbo._objects o ON o.[_id] = _pvt_cte.[_id_object]'
                ELSE N'' END
         + @apply_sql
         + CHAR(10) + N'WHERE ' + @whereC;
END;
GO


-- ===== 22_pvt_groupby.sql =====
-- =====================================================================
-- 22_pvt_groupby.sql  (MSSql v2-pvt) — GROUP BY orchestrator
-- ---------------------------------------------------------------------
-- Mirrors redb.Postgres/sql/v2-pvt/22_pvt_groupby.sql, slimmed to
-- the flat-shape grammar consumed by RedbQueryable.GroupBy().SelectAsync().
--
-- Group-by entry shape (per element of @group_by JSON array):
--   { "field": "<path>", "alias": "<name>" }
-- Base fields carry the "0$:" prefix on the field path; alias is always set.
--
-- Aggregation entries: same grammar as file 21 (pvt_build_aggregate_sql).
-- @having:  reserved; not emitted in this slice.
-- @order:   optional ORDER BY entries (same {field,dir,nulls} grammar as file 15).
--
-- NOT in this slice:
--   * HAVING
--   * narrow shape (always Shape A or Shape C wide)
--   * pushdown / tree / polymorphic
--   * $expr group keys
--
-- Output shapes:
--   Shape A (every field is kind='base'):
--     SELECT <key_col> AS [alias], ..., <agg_expr> AS [alias]
--       FROM dbo._objects o
--      WHERE o.[_id_scheme] = X [AND <where>]
--      GROUP BY <key_col>, ...
--     [ORDER BY ...] [OFFSET ... FETCH ...]
--
--   Shape C (any PROPS field referenced):
--     SELECT <key_col> AS [alias], ..., <agg_expr> AS [alias]
--       FROM (<pvt_build_cte_sql>) _pvt_cte
--      WHERE <where>
--      GROUP BY <key_col>, ...
--     [ORDER BY ...] [OFFSET ... FETCH ...]
-- =====================================================================

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER FUNCTION dbo.pvt_build_groupby_sql(
    @scheme_id    BIGINT,
    @filter       NVARCHAR(MAX),
    @group_by     NVARCHAR(MAX),
    @aggregations NVARCHAR(MAX),
    @having       NVARCHAR(MAX),   -- reserved; HAVING not emitted in this slice
    @order        NVARCHAR(MAX),
    @limit        INT,
    @offset       INT,
    @source_mode  NVARCHAR(50)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @scheme_id IS NULL RETURN NULL;
    IF @group_by IS NULL OR ISJSON(@group_by) = 0 RETURN NULL;
    IF @source_mode IS NULL SET @source_mode = N'flat';
    IF @source_mode <> N'flat' RETURN NULL;

    -- 1. Collect fields: merge group_by + order so that every referenced
    --    field is resolved to kind/column metadata before shape decision.
    DECLARE @collect_order NVARCHAR(MAX) =
        CASE
            WHEN @order IS NULL OR ISJSON(@order) = 0 THEN @group_by
            ELSE LEFT(@group_by, LEN(@group_by) - 1)
                 + N',' + SUBSTRING(@order, 2, LEN(@order) - 1)
        END;

    DECLARE @fields NVARCHAR(MAX) = dbo.pvt_collect_fields(@scheme_id, @filter, @collect_order);

    -- Extend fields map with aggregation operand fields (from file 21).
    IF @aggregations IS NOT NULL AND ISJSON(@aggregations) = 1
        SET @fields = dbo.pvt_extend_fields_with_aggs(@scheme_id, @fields, @aggregations);

    -- 2. Decide shape: Shape A (pure base) vs Shape B narrow vs Shape C wide.
    --    Mirrors PG/file-20 narrow/wide selection.
    DECLARE @has_props  BIT = 0;
    DECLARE @has_nested BIT = 0;
    DECLARE @has_scalar BIT = 0;
    IF @fields IS NOT NULL AND ISJSON(@fields) = 1 AND @fields <> N'{}'
    BEGIN
        IF EXISTS (
            SELECT 1 FROM OPENJSON(@fields)
             WHERE JSON_VALUE([value], N'$.kind') <> N'base'
        ) SET @has_props = 1;
        IF EXISTS (
            SELECT 1 FROM OPENJSON(@fields)
             WHERE JSON_VALUE([value], N'$.parent_sid') IS NOT NULL
               AND JSON_VALUE([value], N'$.dict_key')   IS NOT NULL
        ) SET @has_nested = 1;
        IF EXISTS (
            SELECT 1 FROM OPENJSON(@fields)
             WHERE JSON_VALUE([value], N'$.kind') <> N'base'
               AND NOT (
                       JSON_VALUE([value], N'$.parent_sid') IS NOT NULL
                   AND JSON_VALUE([value], N'$.dict_key')   IS NOT NULL
                   )
        ) SET @has_scalar = 1;
    END;

    -- Split filter / narrow vs wide decision (only relevant when has_props=1;
    -- when has_props=0 we go to Shape A path below).
    DECLARE @splitC      NVARCHAR(MAX) = dbo.pvt_split_filter(@filter, @fields);
    DECLARE @pushC       NVARCHAR(MAX) = JSON_VALUE(@splitC, N'$.push');
    DECLARE @resC        NVARCHAR(MAX) = JSON_QUERY(@splitC, N'$.residual');
    DECLARE @has_null    BIT = dbo.pvt_has_absence_check(@resC);
    DECLARE @force_outer BIT = CASE WHEN @has_null = 1 OR @has_props = 0 THEN 1 ELSE 0 END;
    -- PG parity: narrow body in 12 currently does NOT support nested-dict.
    DECLARE @narrow      BIT = CASE
        WHEN @force_outer = 0 AND @has_nested = 0 THEN 1
        ELSE 0
    END;

    -- 3. ORDER BY (uses the same per-shape prefix convention as file 20).
    DECLARE @order_sql NVARCHAR(MAX) =
        dbo.pvt_build_order_conditions(
            @order, @fields,
            CASE WHEN @has_props = 0 OR @narrow = 1 THEN N'o.' ELSE N'_pvt_cte.' END);

    -- 4. Paging.
    DECLARE @paging NVARCHAR(MAX) = N'';
    IF @limit IS NOT NULL AND @limit >= 0
    BEGIN
        IF COALESCE(@offset, 0) = 0
            SET @paging = CHAR(10) + N'OFFSET 0 ROWS FETCH NEXT '
                        + CAST(@limit AS NVARCHAR(20)) + N' ROWS ONLY';
        ELSE
            SET @paging = CHAR(10) + N'OFFSET ' + CAST(@offset AS NVARCHAR(20))
                        + N' ROWS FETCH NEXT ' + CAST(@limit AS NVARCHAR(20)) + N' ROWS ONLY';
    END
    ELSE IF COALESCE(@offset, 0) > 0
        SET @paging = CHAR(10) + N'OFFSET ' + CAST(@offset AS NVARCHAR(20)) + N' ROWS';

    -- Paging requires ORDER BY in T-SQL.
    IF @paging <> N'' AND @order_sql = N''
        SET @order_sql = CHAR(10) + N'ORDER BY (SELECT 1)';

    -- 5. Iterate group_by entries: build SELECT projection and GROUP BY list.
    --    Base-field prefix:  N'o.' when going against _objects directly
    --                        (Shape A, or narrow Shape B via JOIN);
    --                        N''   in legacy wide Shape C (_pvt_cte projects base cols).
    --    Props-field: always QUOTENAME(@grp_fld) — _pvt_cte hoists props columns.
    DECLARE @grp_base_prefix NVARCHAR(20) =
        CASE WHEN @has_props = 0 OR @narrow = 1 THEN N'o.' ELSE N'' END;
    DECLARE @select_parts    NVARCHAR(MAX) = N'';
    DECLARE @groupby_parts   NVARCHAR(MAX) = N'';
    DECLARE @grp_cnt         INT           = 0;

    DECLARE gc CURSOR LOCAL FAST_FORWARD FOR SELECT [value] FROM OPENJSON(@group_by);
    DECLARE @grp_entry NVARCHAR(MAX);
    OPEN gc;
    FETCH NEXT FROM gc INTO @grp_entry;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @grp_cnt = @grp_cnt + 1;

        DECLARE @grp_fld   NVARCHAR(400) = COALESCE(
            JSON_VALUE(@grp_entry, N'$.field'),
            JSON_VALUE(@grp_entry, N'$.field_path'));
        DECLARE @grp_alias NVARCHAR(200) = COALESCE(
            JSON_VALUE(@grp_entry, N'$.alias'),
            @grp_fld,
            N'_grp_' + CAST(@grp_cnt AS NVARCHAR(10)));

        IF @grp_fld IS NOT NULL AND @grp_fld <> N''
        BEGIN
            DECLARE @grp_meta NVARCHAR(MAX) = JSON_QUERY(
                @fields, N'$."' + STRING_ESCAPE(@grp_fld, N'json') + N'"');
            DECLARE @grp_col  NVARCHAR(MAX);

            IF @grp_meta IS NOT NULL AND JSON_VALUE(@grp_meta, N'$.kind') = N'base'
                SET @grp_col = @grp_base_prefix
                             + QUOTENAME(JSON_VALUE(@grp_meta, N'$.column'));
            ELSE
                SET @grp_col = QUOTENAME(@grp_fld);

            IF @select_parts  <> N'' SET @select_parts  = @select_parts  + N', ';
            IF @groupby_parts <> N'' SET @groupby_parts = @groupby_parts + N', ';

            SET @select_parts  = @select_parts  + @grp_col + N' AS ' + QUOTENAME(@grp_alias);
            SET @groupby_parts = @groupby_parts + @grp_col;
        END;

        FETCH NEXT FROM gc INTO @grp_entry;
    END;
    CLOSE gc; DEALLOCATE gc;

    IF @grp_cnt = 0 RETURN NULL;

    -- 6. Append aggregate SELECT projection (reuses pvt_build_agg_projection from file 21).
    IF @aggregations IS NOT NULL AND ISJSON(@aggregations) = 1
    BEGIN
        DECLARE @agg_proj NVARCHAR(MAX) =
            dbo.pvt_build_agg_projection(@aggregations, @fields, @grp_base_prefix);
        IF @agg_proj IS NOT NULL AND LEN(@agg_proj) > 0
        BEGIN
            IF @select_parts <> N'' SET @select_parts = @select_parts + N', ';
            SET @select_parts = @select_parts + @agg_proj;
        END;
    END;

    -- 7. WHERE clause.
    DECLARE @where_sql NVARCHAR(MAX) = dbo.pvt_build_where_from_json(
        @filter, @fields,
        CASE WHEN @has_props = 0 OR @narrow = 1 THEN N'o.' ELSE N'_pvt_cte.' END);

    -- 8. Assemble.
    --    Slice: HAVING not supported (comment preserved for future slice expansion).

    -- Shape A: pure-base, query directly against dbo._objects.
    IF @has_props = 0
        RETURN N'SELECT ' + @select_parts
             + CHAR(10) + N'FROM dbo._objects o'
             + CHAR(10) + N'WHERE o.[_id_scheme] = ' + CAST(@scheme_id AS NVARCHAR(40))
             + CASE WHEN @where_sql <> N'1=1' THEN N' AND ' + @where_sql ELSE N'' END
             + CHAR(10) + N'GROUP BY ' + @groupby_parts
             + @order_sql + @paging;

    -- Shape B (narrow) / Shape C (wide): props present — wrap pvt_build_cte_sql as derived table.
    -- Pro-parity base pushdown: base/hierarchical predicates narrow
    -- _objects BEFORE correlated pivot subqueries; residual stays outer.
    DECLARE @inner NVARCHAR(MAX) = dbo.pvt_build_cte_sql(
        @scheme_id, @fields, N'flat', NULL, NULL, @force_outer, @pushC, @narrow,
        DEFAULT, DEFAULT, DEFAULT);
    DECLARE @where_sql_c NVARCHAR(MAX) = dbo.pvt_build_where_from_json(
        @resC, @fields,
        CASE WHEN @narrow = 1 THEN N'o.' ELSE N'_pvt_cte.' END);

    RETURN N'SELECT ' + @select_parts
         + CHAR(10) + N'FROM (' + CHAR(10) + @inner + CHAR(10) + N') _pvt_cte'
         + CASE WHEN @narrow = 1
                THEN CHAR(10) + N'JOIN dbo._objects o ON o.[_id] = _pvt_cte.[_id_object]'
                ELSE N'' END
         + CHAR(10) + N'WHERE ' + @where_sql_c
         + CHAR(10) + N'GROUP BY ' + @groupby_parts
         + @order_sql + @paging;
END;
GO


-- ===== 23_pvt_window.sql =====
-- =====================================================================
-- 23_pvt_window.sql  (MSSql v2-pvt) — Window functions orchestrator
-- ---------------------------------------------------------------------
-- Mirrors redb.Postgres/sql/v2-pvt/23_pvt_window.sql, rewritten for
-- T-SQL. The $over node shape is identical to PG so C# callers can
-- share BuildPvtWindowSelectJson between dialects.
--
-- Window node shape (identical to PG):
--   {
--     "func":         "row_number"|"rank"|"dense_rank"|"percent_rank"
--                   | "cume_dist"|"ntile"|"lag"|"lead"
--                   | "first_value"|"last_value"
--                   | "sum"|"avg"|"min"|"max"|"count",
--     "args":         [<arg>, ...]?,   -- {"$field":"..."} | {"$const":N} | "*"
--     "partition_by": [{"field":"..."}, ...]?,
--     "order_by":     [{"field":"...","dir":"asc"|"desc"}, ...]?,
--     "frame": {
--         "type":  "rows"|"range",     -- "groups" not in T-SQL, degrades to ROWS
--         "start": <bound>,
--         "end":   <bound>?
--         -- "exclude" not in T-SQL, silently ignored
--     }?
--   }
--
-- Frame bound forms (same as PG):
--   "unbounded_preceding" | "current_row" | "unbounded_following"
--   {"preceding": N} | {"following": N}
--
-- T-SQL omissions vs PG:
--   * NTH_VALUE          — not in SQL Server; raises error comment in output
--   * GROUPS frame type  — silently demoted to ROWS
--   * EXCLUDE clause     — not in SQL Server; silently dropped
--   * FILTER (WHERE ...) — not in SQL Server; no-op (not in C# API anyway)
--
-- Functions:
--   dbo.pvt_compile_frame_bound(@str NVARCHAR(200), @obj NVARCHAR(MAX))
--       -> NVARCHAR(100)
--   dbo.pvt_build_window_expr(@node NVARCHAR(MAX),
--                             @fields NVARCHAR(MAX),
--                             @base_prefix NVARCHAR(20))
--       -> NVARCHAR(MAX)
--   dbo.pvt_build_window_sql(@scheme_id BIGINT,
--                            @filter NVARCHAR(MAX),
--                            @select NVARCHAR(MAX),
--                            @order  NVARCHAR(MAX),
--                            @limit  INT,
--                            @offset INT,
--                            @source_mode NVARCHAR(50))
--       -> NVARCHAR(MAX)
--
-- Select entries shape (same as order entries + optional $expr.$over):
--   {"field":"<path>", "alias":"<opt>"}        -- plain projection
--   {"alias":"<name>", "$expr":{"$over":{...}}} -- window expression
-- =====================================================================

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

-- ---------- pvt_compile_frame_bound ----------------------------------
-- Converts a JSON frame-bound value to T-SQL OVER-clause syntax.
-- The caller extracts both the scalar form (@str via JSON_VALUE) and the
-- object form (@obj via JSON_QUERY) for the same JSON property. Exactly
-- one of them will be non-NULL for any valid bound.
--   @str  – result of JSON_VALUE(frame, '$.start') (non-NULL when bound is a string)
--   @obj  – result of JSON_QUERY(frame, '$.start') (non-NULL when bound is an object)
CREATE OR ALTER FUNCTION dbo.pvt_compile_frame_bound(
    @str NVARCHAR(200),
    @obj NVARCHAR(MAX)
)
RETURNS NVARCHAR(100)
AS
BEGIN
    IF @str IS NOT NULL
    BEGIN
        DECLARE @s NVARCHAR(200) = LOWER(@str);
        IF @s = N'unbounded_preceding' RETURN N'UNBOUNDED PRECEDING';
        IF @s = N'current_row'         RETURN N'CURRENT ROW';
        IF @s = N'unbounded_following' RETURN N'UNBOUNDED FOLLOWING';
        RETURN N'CURRENT ROW';
    END;
    IF @obj IS NOT NULL
    BEGIN
        DECLARE @prec NVARCHAR(20) = JSON_VALUE(@obj, N'$.preceding');
        IF @prec IS NOT NULL RETURN @prec + N' PRECEDING';
        DECLARE @foll NVARCHAR(20) = JSON_VALUE(@obj, N'$.following');
        IF @foll IS NOT NULL RETURN @foll + N' FOLLOWING';
    END;
    RETURN N'CURRENT ROW';
END;
GO


-- ---------- pvt_build_window_expr ------------------------------------
-- Compiles one window-function $over node into:
--   FUNC(args) OVER (PARTITION BY ... ORDER BY ... ROWS/RANGE ...)
--
-- @node        – the $over JSON object
-- @fields      – resolved fields map (from pvt_collect_fields)
-- @base_prefix – 'o.' (Shape A) or '' (Shape C)
CREATE OR ALTER FUNCTION dbo.pvt_build_window_expr(
    @node        NVARCHAR(MAX),
    @fields      NVARCHAR(MAX),
    @base_prefix NVARCHAR(20)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @node IS NULL OR ISJSON(@node) = 0
        RETURN N'NULL /* pvt_build_window_expr: invalid node */';

    DECLARE @func NVARCHAR(50) = LOWER(JSON_VALUE(@node, N'$.func'));
    IF @func IS NULL
        RETURN N'NULL /* pvt_build_window_expr: missing func */';

    IF @func NOT IN (
        N'row_number', N'rank', N'dense_rank', N'percent_rank', N'cume_dist',
        N'ntile', N'lag', N'lead', N'first_value', N'last_value',
        N'sum', N'avg', N'min', N'max', N'count'
    )
        RETURN N'NULL /* pvt_build_window_expr: unsupported func "' + @func + N'" */';

    -- nth_value is not available in SQL Server
    IF @func = N'nth_value'
        RETURN N'NULL /* pvt_build_window_expr: nth_value not supported in T-SQL */';

    -- ----------------------------------------------------------------
    -- Build the function call: FUNC(args)
    -- ----------------------------------------------------------------
    DECLARE @call NVARCHAR(MAX);

    -- Ranking / navigation functions with no user-supplied args
    IF @func IN (N'row_number', N'rank', N'dense_rank', N'percent_rank', N'cume_dist')
    BEGIN
        -- T-SQL uses ROW_NUMBER, DENSE_RANK etc. (underscores)
        DECLARE @ufunc NVARCHAR(50) =
            CASE @func
                WHEN N'row_number'   THEN N'ROW_NUMBER'
                WHEN N'rank'         THEN N'RANK'
                WHEN N'dense_rank'   THEN N'DENSE_RANK'
                WHEN N'percent_rank' THEN N'PERCENT_RANK'
                WHEN N'cume_dist'    THEN N'CUME_DIST'
                ELSE UPPER(@func)
            END;
        SET @call = @ufunc + N'()';
    END
    ELSE
    BEGIN
        -- Build arg list from args JSON array
        DECLARE @args_json NVARCHAR(MAX) = JSON_QUERY(@node, N'$.args');
        DECLARE @args_sql  NVARCHAR(MAX) = N'';

        IF @args_json IS NOT NULL AND ISJSON(@args_json) = 1
        BEGIN
            -- COUNT(*) shorthand: args = ["*"]
            IF @func = N'count'
               AND JSON_VALUE(@args_json, N'$[0]') = N'*'
            BEGIN
                SET @args_sql = N'*';
            END
            ELSE
            BEGIN
                DECLARE args_c CURSOR LOCAL FAST_FORWARD FOR
                    SELECT [value], [type] FROM OPENJSON(@args_json);
                DECLARE @av NVARCHAR(MAX), @at INT;
                OPEN args_c;
                FETCH NEXT FROM args_c INTO @av, @at;
                WHILE @@FETCH_STATUS = 0
                BEGIN
                    DECLARE @ae NVARCHAR(MAX);

                    IF @at = 1  -- JSON string literal
                    BEGIN
                        IF @av = N'*'
                            SET @ae = N'*';
                        ELSE
                            SET @ae = N'N''' + REPLACE(@av, N'''', N'''''') + N'''';
                    END
                    ELSE IF @at IN (2, 3)  -- number
                        SET @ae = @av;
                    ELSE IF @at = 5  -- object: {"$field":"..."} or {"$const":...}
                    BEGIN
                        DECLARE @arg_fp NVARCHAR(400) = JSON_VALUE(@av, N'$."$field"');
                        IF @arg_fp IS NOT NULL
                        BEGIN
                            DECLARE @am NVARCHAR(MAX) = JSON_QUERY(@fields,
                                N'$."' + STRING_ESCAPE(@arg_fp, N'json') + N'"');
                            IF @am IS NOT NULL AND JSON_VALUE(@am, N'$.kind') = N'base'
                                SET @ae = ISNULL(@base_prefix, N'')
                                        + QUOTENAME(JSON_VALUE(@am, N'$.column'));
                            ELSE
                                SET @ae = QUOTENAME(@arg_fp);
                        END
                        ELSE
                        BEGIN
                            DECLARE @cv NVARCHAR(MAX) = JSON_VALUE(@av, N'$."$const"');
                            SET @ae = ISNULL(@cv, N'NULL');
                        END;
                    END
                    ELSE
                        SET @ae = N'NULL';

                    IF @args_sql <> N'' SET @args_sql = @args_sql + N', ';
                    SET @args_sql = @args_sql + @ae;
                    FETCH NEXT FROM args_c INTO @av, @at;
                END;
                CLOSE args_c; DEALLOCATE args_c;
            END;
        END;

        -- NTILE requires exactly one integer arg
        IF @func = N'ntile'
            SET @call = N'NTILE(' + ISNULL(@args_sql, N'4') + N')';
        ELSE IF @func = N'count'
            SET @call = N'COUNT(' + CASE WHEN @args_sql = N'' THEN N'*' ELSE @args_sql END + N')';
        ELSE
            SET @call = UPPER(@func) + N'(' + @args_sql + N')';
    END;

    -- ----------------------------------------------------------------
    -- Build OVER clause parts
    -- ----------------------------------------------------------------
    DECLARE @over_parts NVARCHAR(MAX) = N'';

    -- PARTITION BY
    DECLARE @pb_json NVARCHAR(MAX) = JSON_QUERY(@node, N'$.partition_by');
    IF @pb_json IS NOT NULL AND ISJSON(@pb_json) = 1
    BEGIN
        DECLARE @pb_parts NVARCHAR(MAX) = N'';

        DECLARE pb_c CURSOR LOCAL FAST_FORWARD FOR
            SELECT [value] FROM OPENJSON(@pb_json);
        DECLARE @pbe NVARCHAR(MAX);
        OPEN pb_c;
        FETCH NEXT FROM pb_c INTO @pbe;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            DECLARE @pb_fld NVARCHAR(400) = COALESCE(
                JSON_VALUE(@pbe, N'$.field'), JSON_VALUE(@pbe, N'$.field_path'));
            IF @pb_fld IS NOT NULL
            BEGIN
                DECLARE @pb_meta NVARCHAR(MAX) = JSON_QUERY(@fields,
                    N'$."' + STRING_ESCAPE(@pb_fld, N'json') + N'"');
                DECLARE @pb_col NVARCHAR(MAX);
                IF @pb_meta IS NOT NULL AND JSON_VALUE(@pb_meta, N'$.kind') = N'base'
                    SET @pb_col = ISNULL(@base_prefix, N'')
                                + QUOTENAME(JSON_VALUE(@pb_meta, N'$.column'));
                ELSE
                    SET @pb_col = QUOTENAME(@pb_fld);

                IF @pb_parts <> N'' SET @pb_parts = @pb_parts + N', ';
                SET @pb_parts = @pb_parts + @pb_col;
            END;
            FETCH NEXT FROM pb_c INTO @pbe;
        END;
        CLOSE pb_c; DEALLOCATE pb_c;

        IF @pb_parts <> N''
        BEGIN
            IF @over_parts <> N'' SET @over_parts = @over_parts + N' ';
            SET @over_parts = @over_parts + N'PARTITION BY ' + @pb_parts;
        END;
    END;

    -- ORDER BY
    DECLARE @ob_json NVARCHAR(MAX) = JSON_QUERY(@node, N'$.order_by');
    IF @ob_json IS NOT NULL AND ISJSON(@ob_json) = 1
    BEGIN
        DECLARE @ob_parts NVARCHAR(MAX) = N'';

        DECLARE ob_c CURSOR LOCAL FAST_FORWARD FOR
            SELECT [value] FROM OPENJSON(@ob_json);
        DECLARE @obe NVARCHAR(MAX);
        OPEN ob_c;
        FETCH NEXT FROM ob_c INTO @obe;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            DECLARE @ob_fld NVARCHAR(400) = COALESCE(
                JSON_VALUE(@obe, N'$.field'), JSON_VALUE(@obe, N'$.field_path'));
            DECLARE @ob_dir NVARCHAR(10) = LOWER(COALESCE(
                JSON_VALUE(@obe, N'$.dir'), N'asc'));
            IF @ob_dir NOT IN (N'asc', N'desc') SET @ob_dir = N'asc';

            IF @ob_fld IS NOT NULL
            BEGIN
                DECLARE @ob_meta NVARCHAR(MAX) = JSON_QUERY(@fields,
                    N'$."' + STRING_ESCAPE(@ob_fld, N'json') + N'"');
                DECLARE @ob_col NVARCHAR(MAX);
                IF @ob_meta IS NOT NULL AND JSON_VALUE(@ob_meta, N'$.kind') = N'base'
                    SET @ob_col = ISNULL(@base_prefix, N'')
                                + QUOTENAME(JSON_VALUE(@ob_meta, N'$.column'));
                ELSE
                    SET @ob_col = QUOTENAME(@ob_fld);

                IF @ob_parts <> N'' SET @ob_parts = @ob_parts + N', ';
                SET @ob_parts = @ob_parts + @ob_col + N' ' + UPPER(@ob_dir);
            END;
            FETCH NEXT FROM ob_c INTO @obe;
        END;
        CLOSE ob_c; DEALLOCATE ob_c;

        IF @ob_parts <> N''
        BEGIN
            IF @over_parts <> N'' SET @over_parts = @over_parts + N' ';
            SET @over_parts = @over_parts + N'ORDER BY ' + @ob_parts;
        END;
    END;

    -- FRAME (T-SQL: ROWS|RANGE BETWEEN ... AND ...)
    DECLARE @fr_json NVARCHAR(MAX) = JSON_QUERY(@node, N'$.frame');
    IF @fr_json IS NOT NULL AND ISJSON(@fr_json) = 1
    BEGIN
        DECLARE @fr_type NVARCHAR(20) = UPPER(COALESCE(JSON_VALUE(@fr_json, N'$.type'), N'rows'));
        -- GROUPS is not supported in T-SQL; demote to ROWS
        IF @fr_type NOT IN (N'ROWS', N'RANGE') SET @fr_type = N'ROWS';

        DECLARE @fs_str NVARCHAR(200) = JSON_VALUE(@fr_json, N'$.start');
        DECLARE @fs_obj NVARCHAR(MAX) = JSON_QUERY(@fr_json, N'$.start');
        DECLARE @fs_sql NVARCHAR(100) = dbo.pvt_compile_frame_bound(@fs_str, @fs_obj);

        DECLARE @fe_str NVARCHAR(200) = JSON_VALUE(@fr_json, N'$.end');
        DECLARE @fe_obj NVARCHAR(MAX) = JSON_QUERY(@fr_json, N'$.end');
        DECLARE @frame_sql NVARCHAR(200);

        IF @fe_str IS NOT NULL OR @fe_obj IS NOT NULL
        BEGIN
            DECLARE @fe_sql NVARCHAR(100) = dbo.pvt_compile_frame_bound(@fe_str, @fe_obj);
            SET @frame_sql = @fr_type + N' BETWEEN ' + @fs_sql + N' AND ' + @fe_sql;
        END
        ELSE
            SET @frame_sql = @fr_type + N' ' + @fs_sql;

        -- EXCLUDE clause: not supported in T-SQL, silently dropped.
        IF @over_parts <> N'' SET @over_parts = @over_parts + N' ';
        SET @over_parts = @over_parts + @frame_sql;
    END;

    RETURN @call + N' OVER (' + @over_parts + N')';
END;
GO


-- ---------- pvt_build_window_sql ------------------------------------
-- Window-function orchestrator. Mirrors pvt_build_window_sql in PG.
--
-- @scheme_id    – required
-- @filter       – facet JSON or NULL
-- @select       – non-empty JSON array of select entries:
--                   {"field":"<path>","alias":"<opt>"}  OR
--                   {"alias":"<name>","$expr":{"$over":{...}}}
-- @order        – optional ORDER BY array (outer, for paging)
-- @limit / @offset – optional paging
-- @source_mode  – 'flat' (only supported value)
--
-- Output shapes:
--   Shape A (every referenced field is kind='base'):
--     SELECT <projection>
--       FROM dbo._objects o
--      WHERE o.[_id_scheme] = X [AND <where>]
--     [ORDER BY ...] [OFFSET ... FETCH ...]
--
--   Shape C (any props field referenced):
--     SELECT <projection>
--       FROM (<pvt_build_cte_sql>) _pvt_cte
--      WHERE <where>
--     [ORDER BY ...] [OFFSET ... FETCH ...]
CREATE OR ALTER FUNCTION dbo.pvt_build_window_sql(
    @scheme_id   BIGINT,
    @filter      NVARCHAR(MAX),
    @select      NVARCHAR(MAX),
    @order       NVARCHAR(MAX),
    @limit       INT,
    @offset      INT,
    @source_mode NVARCHAR(50)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @scheme_id IS NULL RETURN NULL;
    IF @select IS NULL OR ISJSON(@select) = 0 OR @select = N'[]' RETURN NULL;
    IF @source_mode IS NULL SET @source_mode = N'flat';
    IF @source_mode <> N'flat' RETURN NULL;

    -- ----------------------------------------------------------------
    -- 1. Collect ALL field paths needed for the fields map:
    --    plain select "field" entries + window node args/$field +
    --    partition_by + order_by + outer @order
    -- ----------------------------------------------------------------
    DECLARE @all_flds NVARCHAR(MAX) = N'[';
    DECLARE @af_first BIT = 1;

    DECLARE s1 CURSOR LOCAL FAST_FORWARD FOR
        SELECT [value] FROM OPENJSON(@select);
    DECLARE @s1e NVARCHAR(MAX);
    OPEN s1;
    FETCH NEXT FROM s1 INTO @s1e;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Plain field entry
        DECLARE @s1_plain NVARCHAR(400) = COALESCE(
            JSON_VALUE(@s1e, N'$.field'), JSON_VALUE(@s1e, N'$.field_path'));
        IF @s1_plain IS NOT NULL
        BEGIN
            IF @af_first = 0 SET @all_flds = @all_flds + N',';
            SET @all_flds = @all_flds
                + N'{"field":"' + STRING_ESCAPE(@s1_plain, N'json') + N'"}';
            SET @af_first = 0;
        END;

        -- $over node: harvest partition_by / order_by / args fields
        DECLARE @s1_over NVARCHAR(MAX) = JSON_QUERY(@s1e, N'$."$expr"."$over"');
        IF @s1_over IS NOT NULL
        BEGIN
            -- partition_by
            DECLARE pb1 CURSOR LOCAL FAST_FORWARD FOR
                SELECT JSON_VALUE([value], N'$.field')
                  FROM OPENJSON(JSON_QUERY(@s1_over, N'$.partition_by'))
                 WHERE JSON_VALUE([value], N'$.field') IS NOT NULL;
            DECLARE @pb1f NVARCHAR(400);
            OPEN pb1;
            FETCH NEXT FROM pb1 INTO @pb1f;
            WHILE @@FETCH_STATUS = 0
            BEGIN
                IF @af_first = 0 SET @all_flds = @all_flds + N',';
                SET @all_flds = @all_flds
                    + N'{"field":"' + STRING_ESCAPE(@pb1f, N'json') + N'"}';
                SET @af_first = 0;
                FETCH NEXT FROM pb1 INTO @pb1f;
            END;
            CLOSE pb1; DEALLOCATE pb1;

            -- order_by
            DECLARE ob1 CURSOR LOCAL FAST_FORWARD FOR
                SELECT JSON_VALUE([value], N'$.field')
                  FROM OPENJSON(JSON_QUERY(@s1_over, N'$.order_by'))
                 WHERE JSON_VALUE([value], N'$.field') IS NOT NULL;
            DECLARE @ob1f NVARCHAR(400);
            OPEN ob1;
            FETCH NEXT FROM ob1 INTO @ob1f;
            WHILE @@FETCH_STATUS = 0
            BEGIN
                IF @af_first = 0 SET @all_flds = @all_flds + N',';
                SET @all_flds = @all_flds
                    + N'{"field":"' + STRING_ESCAPE(@ob1f, N'json') + N'"}';
                SET @af_first = 0;
                FETCH NEXT FROM ob1 INTO @ob1f;
            END;
            CLOSE ob1; DEALLOCATE ob1;

            -- args: {"$field":"..."} references.
            -- Use CASE in projection to gate JSON_VALUE: a raw scalar
            -- like `*` (COUNT(*) shorthand, type=1) makes JSON_VALUE
            -- error out, and SQL Server's WHERE-clause evaluation
            -- order is unspecified — even a derived-table type filter
            -- can be folded back into the same scope by the optimizer.
            -- CASE in SELECT is the only reliable per-row guard.
            DECLARE ar1 CURSOR LOCAL FAST_FORWARD FOR
                SELECT v FROM (
                    SELECT CASE WHEN [type] = 5
                                THEN JSON_VALUE([value], N'$."$field"')
                           END AS v
                      FROM OPENJSON(JSON_QUERY(@s1_over, N'$.args'))
                ) t
                 WHERE v IS NOT NULL;
            DECLARE @ar1f NVARCHAR(400);
            OPEN ar1;
            FETCH NEXT FROM ar1 INTO @ar1f;
            WHILE @@FETCH_STATUS = 0
            BEGIN
                IF @af_first = 0 SET @all_flds = @all_flds + N',';
                SET @all_flds = @all_flds
                    + N'{"field":"' + STRING_ESCAPE(@ar1f, N'json') + N'"}';
                SET @af_first = 0;
                FETCH NEXT FROM ar1 INTO @ar1f;
            END;
            CLOSE ar1; DEALLOCATE ar1;
        END;

        FETCH NEXT FROM s1 INTO @s1e;
    END;
    CLOSE s1; DEALLOCATE s1;

    -- Append outer @order field paths
    IF @order IS NOT NULL AND ISJSON(@order) = 1
    BEGIN
        DECLARE o1 CURSOR LOCAL FAST_FORWARD FOR
            SELECT COALESCE(JSON_VALUE([value], N'$.field'),
                            JSON_VALUE([value], N'$.field_path'))
              FROM OPENJSON(@order)
             WHERE COALESCE(JSON_VALUE([value], N'$.field'),
                            JSON_VALUE([value], N'$.field_path')) IS NOT NULL;
        DECLARE @o1f NVARCHAR(400);
        OPEN o1;
        FETCH NEXT FROM o1 INTO @o1f;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            IF @af_first = 0 SET @all_flds = @all_flds + N',';
            SET @all_flds = @all_flds
                + N'{"field":"' + STRING_ESCAPE(@o1f, N'json') + N'"}';
            SET @af_first = 0;
            FETCH NEXT FROM o1 INTO @o1f;
        END;
        CLOSE o1; DEALLOCATE o1;
    END;

    SET @all_flds = @all_flds + N']';

    -- ----------------------------------------------------------------
    -- 2. Resolve field metadata
    -- ----------------------------------------------------------------
    DECLARE @fields NVARCHAR(MAX) = dbo.pvt_collect_fields(
        @scheme_id, @filter,
        CASE WHEN @all_flds = N'[]' THEN NULL ELSE @all_flds END);

    -- ----------------------------------------------------------------
    -- 3. Shape decision: Shape A (pure base) vs Shape B narrow vs Shape C wide.
    --    Mirrors PG/file-20 narrow/wide selection.
    -- ----------------------------------------------------------------
    DECLARE @has_props  BIT = 0;
    DECLARE @has_nested BIT = 0;
    DECLARE @has_scalar BIT = 0;
    IF @fields IS NOT NULL AND ISJSON(@fields) = 1 AND @fields <> N'{}'
    BEGIN
        IF EXISTS (
            SELECT 1 FROM OPENJSON(@fields)
             WHERE JSON_VALUE([value], N'$.kind') <> N'base'
        ) SET @has_props = 1;
        IF EXISTS (
            SELECT 1 FROM OPENJSON(@fields)
             WHERE JSON_VALUE([value], N'$.parent_sid') IS NOT NULL
               AND JSON_VALUE([value], N'$.dict_key')   IS NOT NULL
        ) SET @has_nested = 1;
        IF EXISTS (
            SELECT 1 FROM OPENJSON(@fields)
             WHERE JSON_VALUE([value], N'$.kind') <> N'base'
               AND NOT (
                       JSON_VALUE([value], N'$.parent_sid') IS NOT NULL
                   AND JSON_VALUE([value], N'$.dict_key')   IS NOT NULL
                   )
        ) SET @has_scalar = 1;
    END;

    -- Split filter / narrow vs wide decision.
    DECLARE @splitC      NVARCHAR(MAX) = dbo.pvt_split_filter(@filter, @fields);
    DECLARE @pushC       NVARCHAR(MAX) = JSON_VALUE(@splitC, N'$.push');
    DECLARE @resC        NVARCHAR(MAX) = JSON_QUERY(@splitC, N'$.residual');
    DECLARE @has_null    BIT = dbo.pvt_has_absence_check(@resC);
    DECLARE @force_outer BIT = CASE WHEN @has_null = 1 OR @has_props = 0 THEN 1 ELSE 0 END;
    -- PG parity: narrow body in 12 currently does NOT support nested-dict.
    DECLARE @narrow      BIT = CASE
        WHEN @force_outer = 0 AND @has_nested = 0 THEN 1
        ELSE 0
    END;

    DECLARE @base_prefix NVARCHAR(20) =
        CASE WHEN @has_props = 0 OR @narrow = 1 THEN N'o.' ELSE N'' END;

    -- ----------------------------------------------------------------
    -- 4. Outer ORDER BY / paging
    -- ----------------------------------------------------------------
    DECLARE @order_sql NVARCHAR(MAX) = dbo.pvt_build_order_conditions(
        @order, @fields,
        CASE WHEN @has_props = 0 OR @narrow = 1 THEN N'o.' ELSE N'_pvt_cte.' END);

    DECLARE @paging NVARCHAR(MAX) = N'';
    IF @limit IS NOT NULL AND @limit >= 0
    BEGIN
        IF COALESCE(@offset, 0) = 0
            SET @paging = CHAR(10) + N'OFFSET 0 ROWS FETCH NEXT '
                        + CAST(@limit AS NVARCHAR(20)) + N' ROWS ONLY';
        ELSE
            SET @paging = CHAR(10) + N'OFFSET ' + CAST(@offset AS NVARCHAR(20))
                        + N' ROWS FETCH NEXT ' + CAST(@limit AS NVARCHAR(20))
                        + N' ROWS ONLY';
    END
    ELSE IF COALESCE(@offset, 0) > 0
        SET @paging = CHAR(10) + N'OFFSET ' + CAST(@offset AS NVARCHAR(20)) + N' ROWS';

    IF @paging <> N'' AND @order_sql = N''
        SET @order_sql = CHAR(10) + N'ORDER BY (SELECT 1)';

    -- ----------------------------------------------------------------
    -- 5. Build SELECT projection (second pass over @select)
    -- ----------------------------------------------------------------
    DECLARE @select_parts NVARCHAR(MAX) = N'';
    DECLARE @sel_cnt INT = 0;

    DECLARE s2 CURSOR LOCAL FAST_FORWARD FOR
        SELECT [value] FROM OPENJSON(@select);
    DECLARE @s2e NVARCHAR(MAX);
    OPEN s2;
    FETCH NEXT FROM s2 INTO @s2e;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @sel_cnt = @sel_cnt + 1;

        DECLARE @s2_alias NVARCHAR(200) = JSON_VALUE(@s2e, N'$.alias');
        DECLARE @s2_col   NVARCHAR(MAX);

        -- Window $over expression?
        DECLARE @s2_over NVARCHAR(MAX) = JSON_QUERY(@s2e, N'$."$expr"."$over"');
        IF @s2_over IS NOT NULL
        BEGIN
            SET @s2_col = dbo.pvt_build_window_expr(@s2_over, @fields, @base_prefix);
            IF @s2_alias IS NULL
                SET @s2_alias = LOWER(ISNULL(JSON_VALUE(@s2_over, N'$.func'), N'_win'))
                              + N'_' + CAST(@sel_cnt AS NVARCHAR(10));
        END
        ELSE
        BEGIN
            -- Plain field projection
            DECLARE @s2_fld NVARCHAR(400) = COALESCE(
                JSON_VALUE(@s2e, N'$.field'), JSON_VALUE(@s2e, N'$.field_path'));

            IF @s2_fld IS NOT NULL
            BEGIN
                DECLARE @s2_meta NVARCHAR(MAX) = JSON_QUERY(@fields,
                    N'$."' + STRING_ESCAPE(@s2_fld, N'json') + N'"');
                IF @s2_meta IS NOT NULL AND JSON_VALUE(@s2_meta, N'$.kind') = N'base'
                    SET @s2_col = ISNULL(@base_prefix, N'')
                                + QUOTENAME(JSON_VALUE(@s2_meta, N'$.column'));
                ELSE
                    SET @s2_col = QUOTENAME(@s2_fld);

                IF @s2_alias IS NULL SET @s2_alias = @s2_fld;
            END
            ELSE
            BEGIN
                SET @s2_col = N'NULL';
                IF @s2_alias IS NULL SET @s2_alias = N'_sel_' + CAST(@sel_cnt AS NVARCHAR(10));
            END;
        END;

        IF @s2_alias IS NULL SET @s2_alias = N'_sel_' + CAST(@sel_cnt AS NVARCHAR(10));

        IF @select_parts <> N'' SET @select_parts = @select_parts + N', ';
        SET @select_parts = @select_parts + @s2_col + N' AS ' + QUOTENAME(@s2_alias);

        FETCH NEXT FROM s2 INTO @s2e;
    END;
    CLOSE s2; DEALLOCATE s2;

    IF @sel_cnt = 0 RETURN NULL;

    -- ----------------------------------------------------------------
    -- 6. WHERE clause
    -- ----------------------------------------------------------------
    DECLARE @where_pfx NVARCHAR(20) =
        CASE WHEN @has_props = 0 OR @narrow = 1 THEN N'o.' ELSE N'_pvt_cte.' END;
    DECLARE @where_sql NVARCHAR(MAX) = dbo.pvt_build_where_from_json(
        @filter, @fields, @where_pfx);

    -- ----------------------------------------------------------------
    -- 7. Assemble final SQL
    -- ----------------------------------------------------------------
    IF @has_props = 0
        RETURN N'SELECT ' + @select_parts
             + CHAR(10) + N'FROM dbo._objects o'
             + CHAR(10) + N'WHERE o.[_id_scheme] = '
             + CAST(@scheme_id AS NVARCHAR(40))
             + CASE WHEN @where_sql <> N'1=1'
                    THEN N' AND ' + @where_sql ELSE N'' END
             + @order_sql + @paging;

    -- Shape B (narrow) / Shape C (wide).
    -- Pro-parity base pushdown: base/hierarchical predicates narrow
    -- _objects BEFORE correlated pivot subqueries; residual stays outer.
    DECLARE @inner NVARCHAR(MAX) = dbo.pvt_build_cte_sql(
        @scheme_id, @fields, N'flat', NULL, NULL, @force_outer, @pushC, @narrow,
        DEFAULT, DEFAULT, DEFAULT);
    DECLARE @where_sql_c NVARCHAR(MAX) = dbo.pvt_build_where_from_json(
        @resC, @fields, @where_pfx);

    RETURN N'SELECT ' + @select_parts
         + CHAR(10) + N'FROM (' + CHAR(10) + @inner + CHAR(10) + N') _pvt_cte'
         + CASE WHEN @narrow = 1
                THEN CHAR(10) + N'JOIN dbo._objects o ON o.[_id] = _pvt_cte.[_id_object]'
                ELSE N'' END
         + CHAR(10) + N'WHERE ' + @where_sql_c
         + @order_sql + @paging;
END;
GO


-- ===== 24_pvt_projection.sql =====
-- =====================================================================
-- 24_pvt_projection.sql  (MSSql v2-pvt)
-- ---------------------------------------------------------------------
-- Projection orchestrator. Returns a complete T-SQL SELECT statement
-- for a given projection spec (arbitrary columns from redb scheme).
--
--   pvt_build_projection_sql(
--       @scheme_id   BIGINT,
--       @filter      NVARCHAR(MAX),    -- optional PVT filter JSON
--       @limit       INT,              -- optional paging
--       @offset      INT,
--       @projection  NVARCHAR(MAX),    -- JSON array of string | {field,alias?}
--       @source_mode NVARCHAR(50)      -- 'flat' (others return NULL)
--   ) RETURNS NVARCHAR(MAX)
--
-- Projection entry shapes:
--   "FieldName"                      -- simple string
--   {"field": "FieldName"}           -- object with 'field' key
--   {"field": "FieldName", "alias": "MyCol"} -- aliased
--   {"field_path": "FieldName"}      -- alternative key
--
-- Output shapes:
--   Shape A (all projected fields are base _objects columns):
--       SELECT <proj_cols> FROM dbo._objects o
--        WHERE o.[_id_scheme] = X [AND <where>] [OFFSET/FETCH]
--
--   Shape C (any projected field is a props field):
--       SELECT <proj_cols>
--         FROM (<pvt_build_cte_sql>) _pvt_cte
--        WHERE <where> [ORDER BY (SELECT 1) OFFSET/FETCH]
--
-- Depends on:
--   dbo.pvt_collect_fields      (10_pvt_field_collection.sql)
--   dbo.pvt_resolve_field_path  (10_pvt_field_collection.sql)
--   dbo.pvt_build_where_from_json (14_pvt_where.sql)
--   dbo.pvt_build_cte_sql       (12_pvt_cte_builder.sql)
-- =====================================================================

SET QUOTED_IDENTIFIER ON;
GO

-- ---------- pvt_collect_extra_fields ----------------------------------
-- Resolve metadata for a flat JSON array of field paths and merge the
-- results into an existing fields-map JSON. Paths that are absent from
-- the scheme metadata are silently skipped.
CREATE OR ALTER FUNCTION dbo.pvt_collect_extra_fields(
    @scheme_id   BIGINT,
    @paths_json  NVARCHAR(MAX)     -- JSON array of strings
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @scheme_id IS NULL RETURN N'{}';
    IF @paths_json IS NULL OR ISJSON(@paths_json) = 0 RETURN N'{}';

    DECLARE @out NVARCHAR(MAX) = N'{}';

    DECLARE c CURSOR LOCAL FAST_FORWARD FOR
        SELECT [value] FROM OPENJSON(@paths_json) WHERE [type] = 1;  -- strings only
    DECLARE @path NVARCHAR(400);
    OPEN c;
    FETCH NEXT FROM c INTO @path;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @path IS NOT NULL AND @path <> N''
        BEGIN
            DECLARE @meta NVARCHAR(MAX) = dbo.pvt_resolve_field_path(@scheme_id, @path);
            IF @meta IS NOT NULL
            BEGIN
                IF @out = N'{}'
                    SET @out = N'{"' + STRING_ESCAPE(@path, 'json') + N'":' + @meta + N'}';
                ELSE
                    SET @out = LEFT(@out, LEN(@out) - 1)
                             + N',"' + STRING_ESCAPE(@path, 'json') + N'":' + @meta + N'}';
            END;
        END;
        FETCH NEXT FROM c INTO @path;
    END;
    CLOSE c; DEALLOCATE c;
    RETURN @out;
END;
GO

-- ---------- pvt_build_projection_sql ----------------------------------
CREATE OR ALTER FUNCTION dbo.pvt_build_projection_sql(
    @scheme_id   BIGINT,
    @filter      NVARCHAR(MAX),
    @limit       INT,
    @offset      INT,
    @projection  NVARCHAR(MAX),
    @source_mode NVARCHAR(50)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @scheme_id IS NULL
        RETURN NULL;
    IF @projection IS NULL OR ISJSON(@projection) = 0
        RETURN NULL;
    IF @source_mode IS NULL SET @source_mode = N'flat';
    IF @source_mode <> N'flat'
        RETURN NULL;

    -- Collect fields from @filter (may return '{}' when filter is NULL)
    DECLARE @fields NVARCHAR(MAX) = dbo.pvt_collect_fields(@scheme_id, @filter, NULL);
    IF @fields IS NULL OR ISJSON(@fields) = 0 SET @fields = N'{}';

    -- ---------- Parse projection entries -----------------------------------------
    -- Build two SELECT lists in parallel:
    --   @proj_c  — for Shape C (_pvt_cte.col references)
    --   @proj_a  — for Shape A (o.[col] references, valid only when kind='base')
    -- Also track @has_props to choose the final shape.
    DECLARE @proj_c    NVARCHAR(MAX) = N'';
    DECLARE @proj_a    NVARCHAR(MAX) = N'';
    DECLARE @has_props BIT = 0;

    DECLARE c_p CURSOR LOCAL FAST_FORWARD FOR
        SELECT [value], [type] FROM OPENJSON(@projection);
    DECLARE @pentry NVARCHAR(MAX), @ptype INT;
    OPEN c_p;
    FETCH NEXT FROM c_p INTO @pentry, @ptype;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        DECLARE @fpath  NVARCHAR(400) = NULL;
        DECLARE @falias NVARCHAR(200) = NULL;

        IF @ptype = 1       -- plain string entry: "FieldName"
        BEGIN
            SET @fpath  = @pentry;
            SET @falias = @pentry;
        END
        ELSE IF @ptype = 5  -- object entry: {"field":...,"alias":...}
        BEGIN
            SET @fpath = ISNULL(
                JSON_VALUE(@pentry, N'$.field'),
                JSON_VALUE(@pentry, N'$.field_path'));
            SET @falias = ISNULL(JSON_VALUE(@pentry, N'$.alias'), @fpath);
        END;

        IF @fpath IS NOT NULL AND @fpath <> N''
        BEGIN
            -- Resolve metadata for this field
            DECLARE @fm NVARCHAR(MAX) = dbo.pvt_resolve_field_path(@scheme_id, @fpath);
            IF @fm IS NOT NULL
            BEGIN
                -- Merge into @fields when not already present
                IF JSON_QUERY(@fields, N'$."' + STRING_ESCAPE(@fpath, 'json') + N'"') IS NULL
                BEGIN
                    IF @fields = N'{}'
                        SET @fields = N'{"' + STRING_ESCAPE(@fpath, 'json') + N'":' + @fm + N'}';
                    ELSE
                        SET @fields = LEFT(@fields, LEN(@fields) - 1)
                                    + N',"' + STRING_ESCAPE(@fpath, 'json') + N'":' + @fm + N'}';
                END;

                DECLARE @fkind NVARCHAR(50) = JSON_VALUE(@fm, N'$.kind');
                IF @fkind <> N'base'
                    SET @has_props = 1;

                -- Build SELECT expressions
                DECLARE @alias_q  NVARCHAR(200) = QUOTENAME(@falias);
                DECLARE @expr_c   NVARCHAR(MAX);
                DECLARE @expr_a   NVARCHAR(MAX) = NULL;

                IF @fkind = N'base'
                BEGIN
                    DECLARE @bcol NVARCHAR(100) = JSON_VALUE(@fm, N'$.column');
                    SET @expr_c = N'_pvt_cte.[' + @bcol + N'] AS ' + @alias_q;
                    SET @expr_a = N'o.[' + @bcol + N'] AS ' + @alias_q;
                END
                ELSE
                BEGIN
                    -- Props field: the CTE names the column after @fpath
                    SET @expr_c = N'_pvt_cte.' + QUOTENAME(@fpath) + N' AS ' + @alias_q;
                    -- @expr_a stays NULL; Shape A is invalid for props fields
                END;

                IF @proj_c <> N'' SET @proj_c += N', ';
                SET @proj_c += @expr_c;

                IF @expr_a IS NOT NULL
                BEGIN
                    IF @proj_a <> N'' SET @proj_a += N', ';
                    SET @proj_a += @expr_a;
                END;
            END;
        END;

        FETCH NEXT FROM c_p INTO @pentry, @ptype;
    END;
    CLOSE c_p; DEALLOCATE c_p;

    IF @proj_c = N''
        RETURN NULL;

    -- Paging clause
    DECLARE @paging NVARCHAR(MAX) = N'';
    IF @limit IS NOT NULL AND @limit >= 0
    BEGIN
        DECLARE @off INT = ISNULL(@offset, 0);
        SET @paging = CHAR(10)
                    + N'ORDER BY (SELECT 1)'
                    + N' OFFSET ' + CAST(@off AS NVARCHAR(10)) + N' ROWS'
                    + N' FETCH NEXT ' + CAST(@limit AS NVARCHAR(10)) + N' ROWS ONLY';
    END;

    -- ---- Shape A: all projected fields are base _objects columns ----
    IF @has_props = 0 AND @proj_a <> N''
    BEGIN
        DECLARE @wa NVARCHAR(MAX) = dbo.pvt_build_where_from_json(@filter, @fields, N'o.');
        RETURN N'SELECT ' + @proj_a + CHAR(10)
             + N'FROM dbo._objects o' + CHAR(10)
             + N'WHERE o.[_id_scheme] = ' + CAST(@scheme_id AS NVARCHAR(20))
             + CASE WHEN @wa <> N'1=1' THEN N' AND ' + @wa ELSE N'' END
             + @paging;
    END;

    -- ---- Shape C: at least one props field (wide pivot CTE) ----------
    DECLARE @inner  NVARCHAR(MAX) = dbo.pvt_build_cte_sql(
        @scheme_id, @fields, N'flat', NULL, NULL, 1, NULL, 0, DEFAULT, DEFAULT, DEFAULT);
    IF @inner IS NULL
        RETURN NULL;
    DECLARE @wc NVARCHAR(MAX) = dbo.pvt_build_where_from_json(@filter, @fields, N'_pvt_cte.');
    RETURN N'SELECT ' + @proj_c + CHAR(10)
         + N'FROM (' + CHAR(10) + @inner + CHAR(10) + N') _pvt_cte' + CHAR(10)
         + N'WHERE ' + @wc
         + @paging;
END;
GO


-- ===== 25_object_props_v2.sql =====
-- =====================================================================
-- 25_object_props_v2.sql  (MSSql v2-pvt)
-- ---------------------------------------------------------------------
-- PG-specific functions build_hierarchical_properties_sql and
-- get_object_json_v2 rely on PostgreSQL composite-type arrays (_values[])
-- which have no direct T-SQL equivalent. The MSSql architecture uses
-- separate correlated subqueries and the wide-pivot CTE machinery
-- (pvt_build_cte_sql, pvt_build_query_sql) to surface all props.
--
-- These stubs allow the file to be deployed without error. Actual
-- structured JSON for a single object is composed from the _objects row
-- plus _values lookups at the application layer (RedbServiceBase).
--
-- Functions:
--   dbo.build_hierarchical_properties_sql(@object_id BIGINT)
--       -> NVARCHAR(MAX)  Always NULL (not implemented in MSSql slice)
--   dbo.get_object_json_v2(@object_id BIGINT)
--       -> NVARCHAR(MAX)  Always NULL (not implemented in MSSql slice)
-- =====================================================================

SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER FUNCTION dbo.build_hierarchical_properties_sql(@object_id BIGINT)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    -- Not implemented: PG-specific composite-type array approach.
    -- MSSql returns props via the wide-pivot CTE (pvt_build_cte_sql).
    RETURN NULL;
END;
GO

CREATE OR ALTER FUNCTION dbo.get_object_json_v2(@object_id BIGINT)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    -- Not implemented: PG-specific function.
    -- MSSql surfaces single-object props via correlated _values subqueries.
    RETURN NULL;
END;
GO


-- ===== 26_pvt_array_groupby.sql =====
-- =====================================================================
-- 26_pvt_array_groupby.sql  (MSSql v2-pvt)
-- ---------------------------------------------------------------------
-- Array-element GROUP BY orchestrator. Returns a complete T-SQL SELECT
-- statement as a string ready to be used in a derived-table context.
--
-- Parity with PG `pvt_build_array_groupby_sql`:
--   pvt_build_array_groupby_sql(
--       @scheme_id     BIGINT,
--       @array_path    NVARCHAR(400),   -- e.g. 'Skills'
--       @filter        NVARCHAR(MAX),   -- optional object-level filter (PVT JSON)
--       @group_by      NVARCHAR(MAX),   -- optional: JSON array of {field[, alias]}
--       @aggregations  NVARCHAR(MAX),   -- optional: JSON array of {field, func, alias}
--                                       -- func: COUNT/SUM/AVG/MIN/MAX
--                                       -- field=NULL or "*" with COUNT -> COUNT(*)
--       @having        NVARCHAR(MAX),   -- optional: PVT bool expression over outer aliases
--                                       -- supports $and/$or/$not + $eq/$ne/$gt/$gte/$lt/$lte
--                                       -- with $count/$sum/$avg/$min/$max/$field/$const
--       @source_mode   NVARCHAR(50)     -- 'flat' (others: return NULL)
--   ) RETURNS NVARCHAR(MAX)
--
-- When @group_by IS NULL: returns the flat-list element subquery.
-- Otherwise: groups by nested fields; @aggregations append outer SELECT cols;
-- @having (when supplied) emits HAVING <translated predicate>.
--
-- Depends on:
--   dbo.pvt_resolve_field_path     (10_pvt_field_collection.sql)
--   dbo.pvt_db_type_to_value_column (11_pvt_column_expr.sql)
--   dbo.pvt_build_query_sql        (20_pvt_build_query_sql.sql)
--   dbo.pvt_build_array_having_expr (this file, below)
-- =====================================================================

SET QUOTED_IDENTIFIER ON;
GO

-- ---------------------------------------------------------------------
-- pvt_build_array_having_expr — minimal HAVING translator for array-GROUP-BY.
--
-- Translates a PVT bool-expression JSON node into a T-SQL predicate over
-- the outer-query column aliases produced by pvt_build_array_groupby_sql.
-- Used only by pvt_build_array_groupby_sql; not a full pvt_build_bool_expr
-- port (which would require base-fields / FTS / array operators).
--
-- Supported shapes:
--   { "$and": [ ... ] }                        -> (a AND b AND ...)
--   { "$or":  [ ... ] }                        -> (a OR  b OR ...)
--   { "$not": { ... } }                        -> NOT (...)
--   { "$gt": [ <expr>, <expr> ] }              -> (<l> > <r>)
--     and $gte / $lt / $lte / $eq / $ne (=, <>) likewise
--   <expr> ::=
--     { "$count": "*" }                        -> COUNT(*)
--     { "$count": { "$field": "X" } }          -> COUNT([X])
--     { "$sum"|"$avg"|"$min"|"$max": { "$field": "X" } } -> FUNC([X])
--     { "$field": "X" }                        -> [X]   (any outer alias)
--     { "$const": <scalar> }                   -> quoted literal
--     <bare JSON scalar>                       -> quoted literal
-- ---------------------------------------------------------------------
CREATE OR ALTER FUNCTION dbo.pvt_build_array_having_expr(
    @node    NVARCHAR(MAX)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @node IS NULL RETURN N'NULL';

    -- Bare JSON scalar (not an object/array): emit as literal.
    IF ISJSON(@node) = 0
    BEGIN
        DECLARE @lit NVARCHAR(MAX) = @node;
        IF LEN(@lit) >= 2 AND LEFT(@lit, 1) = N'"' AND RIGHT(@lit, 1) = N'"'
            RETURN N'N''' + REPLACE(SUBSTRING(@lit, 2, LEN(@lit) - 2), N'''', N'''''') + N'''';
        RETURN @lit;
    END;

    -- Detect first key in the object.
    DECLARE @k NVARCHAR(200), @v NVARCHAR(MAX), @t INT;
    SELECT TOP 1 @k = [key], @v = [value], @t = [type] FROM OPENJSON(@node);
    IF @k IS NULL RETURN N'1=1';

    -- ---- Logical connectives -----------------------------------------
    IF @k = N'$and' OR @k = N'$or'
    BEGIN
        DECLARE @op NVARCHAR(5) = CASE @k WHEN N'$and' THEN N' AND ' ELSE N' OR ' END;
        DECLARE @acc NVARCHAR(MAX) = N'';
        DECLARE @child NVARCHAR(MAX);
        DECLARE c_l CURSOR LOCAL FAST_FORWARD FOR
            SELECT [value] FROM OPENJSON(@v);
        OPEN c_l;
        FETCH NEXT FROM c_l INTO @child;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            IF @acc <> N'' SET @acc += @op;
            SET @acc += dbo.pvt_build_array_having_expr(@child);
            FETCH NEXT FROM c_l INTO @child;
        END;
        CLOSE c_l; DEALLOCATE c_l;
        IF @acc = N'' RETURN N'1=1';
        RETURN N'(' + @acc + N')';
    END;

    IF @k = N'$not'
        RETURN N'NOT (' + dbo.pvt_build_array_having_expr(@v) + N')';

    -- ---- Aggregate / field expressions (leaf operand) ----------------
    IF @k = N'$count'
    BEGIN
        IF @t = 1
        BEGIN
            IF @v IS NULL OR @v = N'*' OR @v = N''
                RETURN N'COUNT(*)';
            RETURN N'COUNT(' + QUOTENAME(@v) + N')';
        END;
        DECLARE @cf NVARCHAR(400) = JSON_VALUE(@v, N'$."$field"');
        IF @cf IS NOT NULL
            RETURN N'COUNT(' + QUOTENAME(@cf) + N')';
        RETURN N'COUNT(*)';
    END;

    IF @k IN (N'$sum', N'$avg', N'$min', N'$max')
    BEGIN
        DECLARE @fn NVARCHAR(10) = CASE @k
            WHEN N'$sum' THEN N'SUM'
            WHEN N'$avg' THEN N'AVG'
            WHEN N'$min' THEN N'MIN'
            ELSE N'MAX'
        END;
        DECLARE @af NVARCHAR(400) = JSON_VALUE(@v, N'$."$field"');
        IF @af IS NULL AND @t = 1 SET @af = @v;
        IF @af IS NULL RETURN N'NULL';
        RETURN @fn + N'(' + QUOTENAME(@af) + N')';
    END;

    IF @k = N'$field'
    BEGIN
        IF @v IS NULL RETURN N'NULL';
        RETURN QUOTENAME(@v);
    END;

    IF @k = N'$const'
    BEGIN
        IF @t IN (2, 3) RETURN @v;            -- number, true/false
        IF @t = 0 RETURN N'NULL';
        IF @v IS NULL RETURN N'NULL';
        RETURN N'N''' + REPLACE(@v, N'''', N'''''') + N'''';
    END;

    -- ---- Comparison operators ----------------------------------------
    DECLARE @symbol NVARCHAR(5);
    IF @k = N'$eq'  SET @symbol = N' = ';
    ELSE IF @k = N'$ne'  SET @symbol = N' <> ';
    ELSE IF @k = N'$gt'  SET @symbol = N' > ';
    ELSE IF @k = N'$gte' SET @symbol = N' >= ';
    ELSE IF @k = N'$lt'  SET @symbol = N' < ';
    ELSE IF @k = N'$lte' SET @symbol = N' <= ';

    IF @symbol IS NOT NULL
    BEGIN
        DECLARE @ops TABLE (idx INT IDENTITY, val NVARCHAR(MAX), tt INT);
        INSERT INTO @ops(val, tt)
            SELECT [value], [type] FROM OPENJSON(@v);
        DECLARE @lhs NVARCHAR(MAX), @rhs NVARCHAR(MAX);
        SELECT @lhs = val FROM @ops WHERE idx = 1;
        SELECT @rhs = val FROM @ops WHERE idx = 2;
        RETURN N'(' + dbo.pvt_build_array_having_expr(@lhs)
             + @symbol
             + dbo.pvt_build_array_having_expr(@rhs) + N')';
    END;

    -- Unknown operator: conservative pass-through so we never break
    -- the whole query. Caller sees no filtering rather than a syntax error.
    RETURN N'1=1';
END;
GO


-- ---------------------------------------------------------------------
-- pvt_build_array_groupby_sql — main orchestrator (7 positional params).
-- ---------------------------------------------------------------------
CREATE OR ALTER FUNCTION dbo.pvt_build_array_groupby_sql(
    @scheme_id     BIGINT,
    @array_path    NVARCHAR(400),
    @filter        NVARCHAR(MAX),
    @group_by      NVARCHAR(MAX),
    @aggregations  NVARCHAR(MAX),
    @having        NVARCHAR(MAX),
    @source_mode   NVARCHAR(50)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @scheme_id IS NULL OR @array_path IS NULL OR @array_path = N''
        RETURN NULL;
    IF @source_mode IS NULL SET @source_mode = N'flat';
    IF @source_mode <> N'flat'
        RETURN NULL;

    DECLARE @arr_meta NVARCHAR(MAX) = dbo.pvt_resolve_field_path(@scheme_id, @array_path);
    IF @arr_meta IS NULL
        RETURN NULL;

    DECLARE @arr_sid BIGINT = TRY_CAST(JSON_VALUE(@arr_meta, N'$.sid') AS BIGINT);
    DECLARE @db_type NVARCHAR(50) = JSON_VALUE(@arr_meta, N'$.db_type');
    DECLARE @db_col  NVARCHAR(64) = dbo.pvt_db_type_to_value_column(@db_type);
    IF @arr_sid IS NULL
        RETURN NULL;

    -- Optional outer object filter: compile via pvt_build_query_sql and apply
    -- as v._id_object IN (<filtered ids>) before the array-element WHERE.
    DECLARE @filter_clause NVARCHAR(MAX) = N'';
    IF @filter IS NOT NULL AND ISJSON(@filter) = 1 AND @filter <> N'{}'
    BEGIN
        DECLARE @filter_sql NVARCHAR(MAX) = dbo.pvt_build_query_sql(
            @scheme_id, @filter, NULL, NULL, NULL, NULL, 0,
            N'flat', NULL, NULL, 0, NULL);
        IF @filter_sql IS NOT NULL
            SET @filter_clause =
                N' AND v.[_id_object] IN (SELECT [_id] FROM (' + @filter_sql + N') _filt)';
    END;

    DECLARE @val_col_expr NVARCHAR(200) =
        CASE WHEN @db_col IS NOT NULL
             THEN N'v.[' + @db_col + N'] AS ' + QUOTENAME(@array_path)
             ELSE N'NULL AS ' + QUOTENAME(@array_path)
        END;

    -- ---- Flat-list mode (no GROUP BY) --------------------------------
    IF @group_by IS NULL OR @group_by = N'' OR ISJSON(@group_by) = 0
    BEGIN
        DECLARE @flat_sql NVARCHAR(MAX) =
              N'SELECT o.[_id] AS [_id_object], v.[_array_index] AS [_idx], '
            + @val_col_expr + CHAR(10)
            + N'FROM dbo._values v' + CHAR(10)
            + N'INNER JOIN dbo._objects o ON o.[_id] = v.[_id_object]'
            + N' AND o.[_id_scheme] = ' + CAST(@scheme_id AS NVARCHAR(20)) + CHAR(10)
            + N'WHERE v.[_id_structure] = ' + CAST(@arr_sid AS NVARCHAR(20))
            + N' AND v.[_array_index] IS NOT NULL'
            + @filter_clause;
        RETURN @flat_sql;
    END;

    -- ---- Group-by mode -----------------------------------------------
    DECLARE @joins       NVARCHAR(MAX) = N'';
    DECLARE @sel_grp     NVARCHAR(MAX) = N'';
    DECLARE @group_cols  NVARCHAR(MAX) = N'';
    DECLARE @join_idx    INT           = 0;
    DECLARE @joined_fields TABLE(field_path NVARCHAR(400) PRIMARY KEY);

    DECLARE c_grp CURSOR LOCAL FAST_FORWARD FOR
        SELECT [value] FROM OPENJSON(@group_by);
    DECLARE @grp_entry NVARCHAR(MAX);
    OPEN c_grp;
    FETCH NEXT FROM c_grp INTO @grp_entry;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        DECLARE @gf_path    NVARCHAR(400) = JSON_VALUE(@grp_entry, N'$.field');
        DECLARE @gf_alias   NVARCHAR(200) = ISNULL(JSON_VALUE(@grp_entry, N'$.alias'), @gf_path);
        IF @gf_path IS NOT NULL AND @gf_path <> N''
        BEGIN
            DECLARE @nested_path NVARCHAR(500) = @array_path + N'[].' + @gf_path;
            DECLARE @gf_meta NVARCHAR(MAX)     = dbo.pvt_resolve_field_path(@scheme_id, @nested_path);
            IF @gf_meta IS NOT NULL
            BEGIN
                DECLARE @gf_sid  BIGINT = TRY_CAST(JSON_VALUE(@gf_meta, N'$.sid') AS BIGINT);
                DECLARE @gf_dt   NVARCHAR(50) = JSON_VALUE(@gf_meta, N'$.db_type');
                DECLARE @gf_col  NVARCHAR(64) = dbo.pvt_db_type_to_value_column(@gf_dt);
                IF @gf_sid IS NOT NULL AND @gf_col IS NOT NULL
                BEGIN
                    SET @join_idx += 1;
                    DECLARE @ja NVARCHAR(10) = N'g' + CAST(@join_idx AS NVARCHAR(5));

                    SET @joins +=
                          CHAR(10) + N'LEFT JOIN dbo._values ' + @ja
                        + N' ON ' + @ja + N'.[_id_object] = v.[_id_object]'
                        + N'  AND ' + @ja + N'.[_id_structure] = ' + CAST(@gf_sid AS NVARCHAR(20))
                        + N'  AND ' + @ja + N'.[_array_parent_id] = v.[_id]';

                    IF @sel_grp <> N'' SET @sel_grp += N', ';
                    SET @sel_grp += @ja + N'.[' + @gf_col + N'] AS ' + QUOTENAME(@gf_alias);

                    IF @group_cols <> N'' SET @group_cols += N', ';
                    SET @group_cols += @ja + N'.[' + @gf_col + N']';

                    INSERT @joined_fields(field_path) VALUES (@gf_path);
                END;
            END;
        END;
        FETCH NEXT FROM c_grp INTO @grp_entry;
    END;
    CLOSE c_grp; DEALLOCATE c_grp;

    IF @sel_grp = N''
    BEGIN
        RETURN N'SELECT o.[_id] AS [_id_object], v.[_array_index] AS [_idx], '
             + @val_col_expr + CHAR(10)
             + N'FROM dbo._values v' + CHAR(10)
             + N'INNER JOIN dbo._objects o ON o.[_id] = v.[_id_object]'
             + N' AND o.[_id_scheme] = ' + CAST(@scheme_id AS NVARCHAR(20)) + CHAR(10)
             + N'WHERE v.[_id_structure] = ' + CAST(@arr_sid AS NVARCHAR(20))
             + N' AND v.[_array_index] IS NOT NULL'
             + @filter_clause;
    END;

    -- ---- Aggregations ------------------------------------------------
    -- Each entry: { field, func: COUNT|SUM|AVG|MIN|MAX, alias }.
    -- COUNT(*) when field IS NULL or "*". Other funcs reuse the group-by
    -- join if the field is already present, otherwise add a dedicated
    -- 'aN' LEFT JOIN and reference the typed column directly.
    IF @aggregations IS NOT NULL AND ISJSON(@aggregations) = 1
    BEGIN
        DECLARE c_agg CURSOR LOCAL FAST_FORWARD FOR
            SELECT [value] FROM OPENJSON(@aggregations);
        DECLARE @agg_entry NVARCHAR(MAX);
        OPEN c_agg;
        FETCH NEXT FROM c_agg INTO @agg_entry;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            DECLARE @af_path  NVARCHAR(400) = JSON_VALUE(@agg_entry, N'$.field');
            DECLARE @af_func  NVARCHAR(20)  = UPPER(ISNULL(JSON_VALUE(@agg_entry, N'$.func'), N''));
            DECLARE @af_alias NVARCHAR(200) = JSON_VALUE(@agg_entry, N'$.alias');
            IF @af_alias IS NULL OR @af_alias = N'' SET @af_alias = @af_func;
            IF @af_func = N'AVERAGE' SET @af_func = N'AVG';

            IF @af_func = N'COUNT' AND (@af_path IS NULL OR @af_path = N'*')
            BEGIN
                SET @sel_grp += N', COUNT(*) AS ' + QUOTENAME(@af_alias);
            END
            ELSE IF @af_func IN (N'COUNT', N'SUM', N'AVG', N'MIN', N'MAX')
                  AND @af_path IS NOT NULL AND @af_path <> N''
            BEGIN
                IF EXISTS(SELECT 1 FROM @joined_fields WHERE field_path = @af_path)
                BEGIN
                    -- Already projected by group_by under alias = field name;
                    -- reference the outer alias via QUOTENAME(field).
                    SET @sel_grp += N', ' + @af_func + N'(' + QUOTENAME(@af_path) + N') AS ' + QUOTENAME(@af_alias);
                END
                ELSE
                BEGIN
                    DECLARE @af_meta NVARCHAR(MAX) =
                        dbo.pvt_resolve_field_path(@scheme_id, @array_path + N'[].' + @af_path);
                    IF @af_meta IS NOT NULL
                    BEGIN
                        DECLARE @af_sid BIGINT = TRY_CAST(JSON_VALUE(@af_meta, N'$.sid') AS BIGINT);
                        DECLARE @af_col NVARCHAR(64) =
                            dbo.pvt_db_type_to_value_column(JSON_VALUE(@af_meta, N'$.db_type'));
                        IF @af_sid IS NOT NULL AND @af_col IS NOT NULL
                        BEGIN
                            SET @join_idx += 1;
                            DECLARE @af_join_alias NVARCHAR(10) = N'a' + CAST(@join_idx AS NVARCHAR(5));
                            SET @joins +=
                                  CHAR(10) + N'LEFT JOIN dbo._values ' + @af_join_alias
                                + N' ON ' + @af_join_alias + N'.[_id_object] = v.[_id_object]'
                                + N'  AND ' + @af_join_alias + N'.[_id_structure] = ' + CAST(@af_sid AS NVARCHAR(20))
                                + N'  AND ' + @af_join_alias + N'.[_array_parent_id] = v.[_id]';
                            SET @sel_grp += N', ' + @af_func + N'('
                                + @af_join_alias + N'.[' + @af_col + N']) AS '
                                + QUOTENAME(@af_alias);
                            INSERT @joined_fields(field_path) VALUES (@af_path);
                        END;
                    END;
                END;
            END;
            FETCH NEXT FROM c_agg INTO @agg_entry;
        END;
        CLOSE c_agg; DEALLOCATE c_agg;
    END;

    -- ---- HAVING ------------------------------------------------------
    DECLARE @having_clause NVARCHAR(MAX) = N'';
    IF @having IS NOT NULL AND ISJSON(@having) = 1 AND @having <> N'{}'
    BEGIN
        DECLARE @having_sql NVARCHAR(MAX) = dbo.pvt_build_array_having_expr(@having);
        IF @having_sql IS NOT NULL AND @having_sql <> N'' AND @having_sql <> N'1=1'
            SET @having_clause = CHAR(10) + N'HAVING ' + @having_sql;
    END;

    DECLARE @grp_sql NVARCHAR(MAX) =
          N'SELECT ' + @sel_grp + CHAR(10)
        + N'FROM dbo._values v' + CHAR(10)
        + N'INNER JOIN dbo._objects o ON o.[_id] = v.[_id_object]'
        + N' AND o.[_id_scheme] = ' + CAST(@scheme_id AS NVARCHAR(20))
        + @joins + CHAR(10)
        + N'WHERE v.[_id_structure] = ' + CAST(@arr_sid AS NVARCHAR(20))
        + N' AND v.[_array_index] IS NOT NULL'
        + @filter_clause + CHAR(10)
        + N'GROUP BY ' + @group_cols
        + @having_clause;

    RETURN @grp_sql;
END;
GO


