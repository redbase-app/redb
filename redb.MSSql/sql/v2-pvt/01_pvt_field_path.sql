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
