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
--       @having        NVARCHAR(MAX),   -- optional: HAVING node (HavingPredicateParser grammar)
--                                       -- supports $and/$or/$not + $eq/$ne/$gt/$gte/$lt/$lte
--                                       -- with $count/$sum/$avg/$min/$max over item fields, $const;
--                                       -- an unknown shape makes the whole result NULL
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
-- pvt_build_array_having_expr — HAVING translator for array-GROUP-BY.
--
-- Translates the HAVING node HavingPredicateParser writes into a T-SQL
-- predicate. Item fields are read through @cols, a JSON map
-- {"<item field>": "<joined typed column>"} built by the orchestrator
-- (g1.[_String], a2.[_Long]) - the same columns the SELECT list reads.
--
-- Grammar:
--   { "$and": [ ... ] } / { "$or": [ ... ] } / { "$not": { ... } }
--   { "$gt"|"$gte"|"$lt"|"$lte"|"$eq"|"$ne": [ <operand>, <operand> ] }
--   <operand> ::=
--     { "$count": "*" }                                  -> COUNT(*)
--     { "$count"|"$sum"|"$avg"|"$min"|"$max": { "$field": "X" } } -> FUNC(<column of X>)
--     { "$const": <scalar> }                             -> literal
--
-- Anything else - an unknown node, a field with no joined column - returns
-- NULL, and NULL reaches the whole query: the orchestrator returns NULL and
-- the caller refuses. It used to return 1=1, dropping the condition, and to
-- write an item field as [X], a column the query does not have.
-- ---------------------------------------------------------------------
CREATE OR ALTER FUNCTION dbo.pvt_build_array_having_expr(
    @node    NVARCHAR(MAX),
    @cols    NVARCHAR(MAX)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @node IS NULL OR ISJSON(@node) = 0 RETURN NULL;

    DECLARE @k NVARCHAR(200), @v NVARCHAR(MAX), @t INT;
    SELECT TOP 1 @k = [key], @v = [value], @t = [type] FROM OPENJSON(@node);
    IF @k IS NULL RETURN NULL;

    -- ---- Logical connectives -----------------------------------------
    IF @k = N'$and' OR @k = N'$or'
    BEGIN
        IF @t <> 4 RETURN NULL;
        DECLARE @op NVARCHAR(5) = CASE @k WHEN N'$and' THEN N' AND ' ELSE N' OR ' END;
        DECLARE @acc NVARCHAR(MAX) = N'', @child NVARCHAR(MAX), @child_t INT, @part NVARCHAR(MAX);
        DECLARE c_l CURSOR LOCAL FAST_FORWARD FOR
            SELECT [value], [type] FROM OPENJSON(@v);
        OPEN c_l;
        FETCH NEXT FROM c_l INTO @child, @child_t;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @part = CASE WHEN @child_t = 5
                             THEN dbo.pvt_build_array_having_expr(@child, @cols)
                             ELSE NULL END;
            -- NULL + anything is NULL: one unknown child makes the whole node unknown.
            SET @acc = CASE WHEN @acc = N'' THEN @part ELSE @acc + @op + @part END;
            FETCH NEXT FROM c_l INTO @child, @child_t;
        END;
        CLOSE c_l; DEALLOCATE c_l;
        IF @acc = N'' RETURN NULL;
        RETURN N'(' + @acc + N')';
    END;

    IF @k = N'$not'
        RETURN CASE WHEN @t = 5
                    THEN N'NOT (' + dbo.pvt_build_array_having_expr(@v, @cols) + N')'
                    ELSE NULL END;

    -- ---- Aggregates --------------------------------------------------
    IF @k IN (N'$count', N'$sum', N'$avg', N'$min', N'$max')
    BEGIN
        DECLARE @fn NVARCHAR(10) = CASE @k
            WHEN N'$count' THEN N'COUNT'
            WHEN N'$sum'   THEN N'SUM'
            WHEN N'$avg'   THEN N'AVG'
            WHEN N'$min'   THEN N'MIN'
            ELSE N'MAX'
        END;
        IF @k = N'$count' AND @t = 1 AND @v = N'*'
            RETURN N'COUNT(*)';
        IF @t <> 5 RETURN NULL;
        DECLARE @af NVARCHAR(400) = JSON_VALUE(@v, N'$."$field"');
        IF @af IS NULL OR @cols IS NULL RETURN NULL;
        DECLARE @col NVARCHAR(200) = JSON_VALUE(@cols, N'$."' + STRING_ESCAPE(@af, 'json') + N'"');
        IF @col IS NULL RETURN NULL;
        RETURN @fn + N'(' + @col + N')';
    END;

    IF @k = N'$const'
        RETURN CASE WHEN @t IN (4, 5) THEN NULL ELSE dbo.pvt_jsonb_to_sql_literal(@v, @t) END;

    -- ---- Comparison operators ----------------------------------------
    DECLARE @symbol NVARCHAR(5) = CASE @k
        WHEN N'$eq'  THEN N' = '
        WHEN N'$ne'  THEN N' <> '
        WHEN N'$gt'  THEN N' > '
        WHEN N'$gte' THEN N' >= '
        WHEN N'$lt'  THEN N' < '
        WHEN N'$lte' THEN N' <= '
        ELSE NULL END;
    IF @symbol IS NULL OR @t <> 4 RETURN NULL;
    IF (SELECT COUNT(*) FROM OPENJSON(@v)) <> 2 RETURN NULL;

    DECLARE @lhs NVARCHAR(MAX), @lhs_t INT, @rhs NVARCHAR(MAX), @rhs_t INT;
    SELECT @lhs = [value], @lhs_t = [type] FROM OPENJSON(@v) WHERE [key] = N'0';
    SELECT @rhs = [value], @rhs_t = [type] FROM OPENJSON(@v) WHERE [key] = N'1';
    IF @lhs_t <> 5 OR @rhs_t <> 5 RETURN NULL;

    RETURN N'(' + dbo.pvt_build_array_having_expr(@lhs, @cols)
         + @symbol
         + dbo.pvt_build_array_having_expr(@rhs, @cols) + N')';
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
    -- Every item field already joined, with the typed column it reads (g1.[_String], a2.[_Long]).
    DECLARE @joined_fields TABLE(field_path NVARCHAR(400) PRIMARY KEY, col_expr NVARCHAR(200) NOT NULL);

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

                    INSERT @joined_fields(field_path, col_expr) VALUES (@gf_path, @ja + N'.[' + @gf_col + N']');
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
                    -- Already joined (by a group key or an earlier aggregate): read the same typed
                    -- column. It used to name the field as if it were an output alias, which only a
                    -- group key whose alias equals its field has - Min(Value) + Max(Value) failed
                    -- with "Invalid column name 'Value'".
                    DECLARE @af_joined NVARCHAR(200) =
                        (SELECT col_expr FROM @joined_fields WHERE field_path = @af_path);
                    SET @sel_grp += N', ' + @af_func + N'(' + @af_joined + N') AS ' + QUOTENAME(@af_alias);
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
                            INSERT @joined_fields(field_path, col_expr)
                                VALUES (@af_path, @af_join_alias + N'.[' + @af_col + N']');
                        END;
                    END;
                END;
            END;
            FETCH NEXT FROM c_agg INTO @agg_entry;
        END;
        CLOSE c_agg; DEALLOCATE c_agg;
    END;

    -- ---- HAVING ------------------------------------------------------
    -- An item field only the HAVING aggregates gets its own join, as an aggregate
    -- of the SELECT list would; then every aggregate reads the joined column
    -- through @cols. A HAVING the translator cannot read makes the query NULL.
    DECLARE @having_clause NVARCHAR(MAX) = N'';
    IF @having IS NOT NULL
    BEGIN
        IF ISJSON(@having) = 0 OR @having = N'{}' RETURN NULL;

        DECLARE @hv_aggs NVARCHAR(MAX) = dbo.pvt_having_agg_entries(@having);
        IF @hv_aggs <> N''
        BEGIN
            DECLARE c_hv CURSOR LOCAL FAST_FORWARD FOR
                SELECT DISTINCT JSON_VALUE(a.[value], N'$."$field"')
                  FROM OPENJSON(N'[' + @hv_aggs + N']') e
                 CROSS APPLY OPENJSON(e.[value]) a
                 WHERE LEFT(a.[key], 1) = N'$' AND a.[type] = 5;
            DECLARE @hv_path NVARCHAR(400);
            OPEN c_hv;
            FETCH NEXT FROM c_hv INTO @hv_path;
            WHILE @@FETCH_STATUS = 0
            BEGIN
                IF @hv_path IS NOT NULL
                   AND NOT EXISTS(SELECT 1 FROM @joined_fields WHERE field_path = @hv_path)
                BEGIN
                    DECLARE @hv_meta NVARCHAR(MAX) =
                        dbo.pvt_resolve_field_path(@scheme_id, @array_path + N'[].' + @hv_path);
                    DECLARE @hv_sid BIGINT = TRY_CAST(JSON_VALUE(@hv_meta, N'$.sid') AS BIGINT);
                    DECLARE @hv_col NVARCHAR(64) =
                        dbo.pvt_db_type_to_value_column(JSON_VALUE(@hv_meta, N'$.db_type'));
                    -- Not an item field: the translator finds no column and refuses.
                    IF @hv_sid IS NOT NULL AND @hv_col IS NOT NULL
                    BEGIN
                        SET @join_idx += 1;
                        DECLARE @hv_join_alias NVARCHAR(10) = N'h' + CAST(@join_idx AS NVARCHAR(5));
                        SET @joins +=
                              CHAR(10) + N'LEFT JOIN dbo._values ' + @hv_join_alias
                            + N' ON ' + @hv_join_alias + N'.[_id_object] = v.[_id_object]'
                            + N'  AND ' + @hv_join_alias + N'.[_id_structure] = ' + CAST(@hv_sid AS NVARCHAR(20))
                            + N'  AND ' + @hv_join_alias + N'.[_array_parent_id] = v.[_id]';
                        INSERT @joined_fields(field_path, col_expr)
                            VALUES (@hv_path, @hv_join_alias + N'.[' + @hv_col + N']');
                    END;
                END;
                FETCH NEXT FROM c_hv INTO @hv_path;
            END;
            CLOSE c_hv; DEALLOCATE c_hv;
        END;

        DECLARE @hv_cols NVARCHAR(MAX) = N'{' + COALESCE(
            (SELECT STRING_AGG(N'"' + STRING_ESCAPE(field_path, 'json') + N'":"'
                               + STRING_ESCAPE(col_expr, 'json') + N'"', N',')
               FROM @joined_fields), N'') + N'}';
        DECLARE @having_sql NVARCHAR(MAX) = dbo.pvt_build_array_having_expr(@having, @hv_cols);
        IF @having_sql IS NULL RETURN NULL;
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
