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
