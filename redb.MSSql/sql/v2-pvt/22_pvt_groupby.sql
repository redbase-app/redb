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
