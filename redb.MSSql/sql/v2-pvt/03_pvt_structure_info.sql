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
