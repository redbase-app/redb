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
