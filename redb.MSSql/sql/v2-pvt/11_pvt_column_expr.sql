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
