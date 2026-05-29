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
