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
