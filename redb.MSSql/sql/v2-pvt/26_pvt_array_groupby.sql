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
