-- =====================================================================
-- 20_pvt_build_query_sql.sql  (MSSql v2-pvt) — orchestrator
-- ---------------------------------------------------------------------
-- Pure SQL string generation. Output yields a single `_id` column.
--
-- Shapes:
--   A) pure-base (no PROPS field referenced):
--      SELECT [DISTINCT] _id FROM dbo._objects o
--       WHERE o.[_id_scheme] = X AND <where>
--       [ORDER BY ...] [OFFSET ... ROWS FETCH NEXT ... ROWS ONLY]
--
--   C) wide CTE (any PROPS field referenced):
--      WITH _pvt_cte AS (...)
--      SELECT [DISTINCT] [_id] FROM _pvt_cte
--       WHERE <where>
--       [ORDER BY ...] [OFFSET ... ROWS FETCH NEXT ... ROWS ONLY]
--
-- Slice limits: no narrow, no tree, no pushdown.
-- DistinctBy: emulated via ROW_NUMBER() OVER (PARTITION BY key) = 1.
-- MSSql paging: OFFSET/FETCH requires ORDER BY -> inject 'ORDER BY (SELECT 1)'
-- when none specified and paging requested.
-- =====================================================================

SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER FUNCTION dbo.pvt_build_query_sql(
    @scheme_id    BIGINT,
    @filter       NVARCHAR(MAX),
    @limit        INT,
    @offset       INT,
    @order        NVARCHAR(MAX),
    @max_depth    INT,
    @distinct     BIT,
    @source_mode  NVARCHAR(50),
    @tree_ids     NVARCHAR(MAX),
    @include_seed BIT,
    @polymorphic  BIT,
    @distinct_on  NVARCHAR(MAX)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @scheme_id IS NULL
        RETURN NULL;
    IF @source_mode IS NULL SET @source_mode = N'flat';

    -- Guard: @distinct=1 and @distinct_on not null are mutually exclusive.
    -- Scalar UDFs cannot THROW; force a runtime cast error whose message
    -- contains 'mutually exclusive' so callers can detect the guard.
    IF @distinct = 1 AND @distinct_on IS NOT NULL AND ISJSON(@distinct_on) = 1
    BEGIN
        DECLARE @_guard INT = CAST(N'p_distinct and p_distinct_on are mutually exclusive' AS INT);
    END;

    -- Merge @distinct_on entries into @order for field collection so that
    -- any PROPS field referenced in @distinct_on triggers Shape C correctly.
    DECLARE @collect_order NVARCHAR(MAX) =
        CASE
            WHEN @distinct_on IS NULL OR ISJSON(@distinct_on) = 0 THEN @order
            WHEN @order IS NULL OR ISJSON(@order) = 0             THEN @distinct_on
            ELSE LEFT(@order, LEN(@order) - 1) + N',' + SUBSTRING(@distinct_on, 2, LEN(@distinct_on) - 1)
        END;

    DECLARE @fields NVARCHAR(MAX) = dbo.pvt_collect_fields(@scheme_id, @filter, @collect_order);

    -- Pure-base shortcut: every collected field has kind='base'
    DECLARE @has_props BIT = 0;
    DECLARE @has_nested BIT = 0;     -- any nested-dict field (parent_sid + dict_key)
    DECLARE @has_scalar BIT = 0;     -- any PROPS field that is NOT nested-dict
    IF @fields IS NOT NULL AND ISJSON(@fields) = 1 AND @fields <> N'{}'
    BEGIN
        IF EXISTS (
            SELECT 1
              FROM OPENJSON(@fields)
             WHERE JSON_VALUE([value], '$.kind') <> N'base'
        )
            SET @has_props = 1;
        IF EXISTS (
            SELECT 1
              FROM OPENJSON(@fields)
             WHERE JSON_VALUE([value], '$.parent_sid') IS NOT NULL
               AND JSON_VALUE([value], '$.dict_key')   IS NOT NULL
        )
            SET @has_nested = 1;
        IF EXISTS (
            SELECT 1
              FROM OPENJSON(@fields)
             WHERE JSON_VALUE([value], '$.kind') <> N'base'
               AND NOT (
                       JSON_VALUE([value], '$.parent_sid') IS NOT NULL
                   AND JSON_VALUE([value], '$.dict_key')   IS NOT NULL
                   )
        )
            SET @has_scalar = 1;
    END;

    -- Narrow vs wide decision (PG parity, see redb.Postgres/sql/v2-pvt/20_pvt_build_query_sql.sql).
    -- @force_outer = 1 means the wide LEFT JOIN shape is mandatory:
    --   - any absence check ($null / $isNull) needs LEFT JOIN to surface NULL pivot rows,
    --   - pure-base / empty-field queries do not need a pivot at all.
    -- @narrow = 1 means we can use the Pro-shape narrow pivot
    -- (FROM _values v / GROUP BY 1 / IN (SELECT FROM _objects)) and JOIN _objects in the outer SELECT.
    -- Mixed (scalar + nested-dict in the same query) forces wide because the nested side CTE expects pi.* projection.
    DECLARE @has_null   BIT = dbo.pvt_has_absence_check(@filter);
    DECLARE @force_outer BIT = CASE
        WHEN @has_null = 1 OR @has_props = 0 THEN 1
        ELSE 0
    END;
    -- Narrow with nested: 12 emits a slim pivot (GROUP BY o.[_id]) and
    -- the orchestrator JOINs _objects in the outer SELECT, so the heavy
    -- 21-col GROUP BY is gone even when nested-dict fields are present.
    DECLARE @narrow BIT = CASE
        WHEN @force_outer = 0 THEN 1
        ELSE 0
    END;

    DECLARE @paging NVARCHAR(MAX) = N'';
    IF @limit IS NOT NULL AND @limit >= 0
    BEGIN
        IF COALESCE(@offset, 0) = 0
            SET @paging = CHAR(10) + N'OFFSET 0 ROWS FETCH NEXT ' + CAST(@limit AS NVARCHAR(20)) + N' ROWS ONLY';
        ELSE
            SET @paging = CHAR(10) + N'OFFSET ' + CAST(@offset AS NVARCHAR(20))
                        + N' ROWS FETCH NEXT ' + CAST(@limit AS NVARCHAR(20)) + N' ROWS ONLY';
    END
    ELSE IF COALESCE(@offset, 0) > 0
        SET @paging = CHAR(10) + N'OFFSET ' + CAST(@offset AS NVARCHAR(20)) + N' ROWS';

    -- Outer alias prefix for base columns: Shape A and narrow Shape B both expose
    -- base columns on alias `o` (via JOIN _objects); legacy wide Shape C exposes
    -- them on `_pvt_cte` (projected by the wide GROUP BY). Pivot/scalar columns
    -- always render unprefixed (pvt_build_field_condition/_order skip the prefix
    -- for kind <> 'base'), so unique pivot names resolve to _pvt_cte automatically.
    DECLARE @base_prefix NVARCHAR(10) = CASE
        WHEN @has_props = 0 OR @narrow = 1 THEN N'o.'
        ELSE N'_pvt_cte.'
    END;
    DECLARE @order_sql NVARCHAR(MAX) = dbo.pvt_build_order_conditions(@order, @fields, @base_prefix);
    -- T-SQL forbids ORDER BY inside subqueries unless paired with TOP/OFFSET-FETCH.
    -- The orchestrator output is always wrapped as a derived table by the C# layer,
    -- so drop ORDER BY when paging is absent and supply a stable default when
    -- present. Using @base_prefix + [_id] gives deterministic paging (vs the old
    -- (SELECT 1) placeholder which left row order to the planner).
    IF @paging <> N'' AND @order_sql = N''
        SET @order_sql = CHAR(10) + N'ORDER BY ' + @base_prefix + N'[_id]';
    ELSE IF @paging = N''
        SET @order_sql = N'';

    -- DistinctBy emulation: T-SQL has no SELECT DISTINCT ON; use
    -- ROW_NUMBER() OVER (PARTITION BY key ORDER BY (SELECT 1)) and
    -- filter to rn=1 in an outer wrapper. Resolve the partition column.
    -- Stored already alias-qualified per current shape:
    --   base col  -> @base_prefix + [DbCol]   (e.g. o.[_name] / _pvt_cte.[_name])
    --   props col -> _pvt_cte.[FieldName]     (pivot cols always live in _pvt_cte)
    DECLARE @rn_col NVARCHAR(MAX) = NULL;
    IF @distinct_on IS NOT NULL AND ISJSON(@distinct_on) = 1
    BEGIN
        DECLARE @don_field NVARCHAR(400) = JSON_VALUE(@distinct_on, '$[0].field');
        IF @don_field IS NOT NULL AND @don_field <> N''
        BEGIN
            DECLARE @don_meta NVARCHAR(MAX) = JSON_QUERY(@fields, N'$."' + STRING_ESCAPE(@don_field, 'json') + N'"');
            IF @don_meta IS NOT NULL AND JSON_VALUE(@don_meta, '$.kind') = N'base'
                SET @rn_col = @base_prefix + QUOTENAME(JSON_VALUE(@don_meta, '$.column'));
            ELSE
                SET @rn_col = N'_pvt_cte.' + QUOTENAME(@don_field);
        END;
    END;

    -- ---------- Tree mode branch ----------
    -- Handles tree_roots, tree_leaves, tree_children, tree_descendants, tree_ancestors.
    -- When @has_props = 0: emit a flat subquery-safe SELECT against _objects using
    -- the corresponding tree predicate; no WITH-CTE prefix so the result can be
    -- wrapped in a derived table by the C# layer.
    -- When @has_props = 1: delegate to pvt_build_cte_sql which natively supports
    -- every tree mode (it injects o.[_id] IN (SELECT _id FROM dbo.pvt_tree_*(...)))
    -- and then wrap with the Shape C outer SELECT / residual WHERE.
    IF @source_mode IN (N'tree_roots', N'tree_leaves', N'tree_children',
                        N'tree_descendants', N'tree_ancestors')
    BEGIN
        IF @has_props = 1
        BEGIN
            DECLARE @splitT NVARCHAR(MAX) = dbo.pvt_split_filter(@filter, @fields);
            DECLARE @pushT  NVARCHAR(MAX) = JSON_VALUE(@splitT, N'$.push');
            DECLARE @resT   NVARCHAR(MAX) = JSON_QUERY(@splitT, N'$.residual');
            DECLARE @innerT NVARCHAR(MAX) = dbo.pvt_build_cte_sql(
                @scheme_id, @fields, @source_mode, @tree_ids, @max_depth,
                @force_outer, @pushT, @narrow, COALESCE(@include_seed, 1), COALESCE(@polymorphic, 1), DEFAULT);
            IF @innerT IS NULL RETURN NULL;
            DECLARE @whereTp NVARCHAR(MAX) = dbo.pvt_build_where_from_json(@resT, @fields, @base_prefix);
            DECLARE @joinT   NVARCHAR(MAX) = CASE
                WHEN @narrow = 1 THEN CHAR(10) + N'JOIN dbo._objects o ON o.[_id] = _pvt_cte.[_id_object]'
                ELSE N''
            END;
            DECLARE @rowSelT NVARCHAR(MAX) = CASE WHEN @narrow = 1 THEN N'o.[_id]' ELSE N'_pvt_cte.[_id]' END;
            IF @rn_col IS NOT NULL
            BEGIN
                -- Project the full row-source so the outer derived table can be
                -- aliased with the same prefix `@order_sql` uses (`o.` or
                -- `_pvt_cte.`). Otherwise base-field ORDER BY references can't
                -- resolve in the outer scope (the ROW_NUMBER wrapper hides `o`).
                DECLARE @rn_aliasT NVARCHAR(40) = CASE WHEN @narrow = 1 THEN N'o' ELSE N'_pvt_cte' END;
                RETURN N'SELECT [_id] FROM (' + CHAR(10)
                     + N'SELECT ' + @rn_aliasT + N'.*, ROW_NUMBER() OVER (PARTITION BY ' + @rn_col + N' ORDER BY (SELECT 1)) AS [_rn]' + CHAR(10)
                     + N'FROM (' + CHAR(10) + @innerT + CHAR(10) + N') _pvt_cte' + @joinT + CHAR(10)
                     + N'WHERE ' + @whereTp + CHAR(10)
                     + N') ' + @rn_aliasT + CHAR(10) + N'WHERE ' + @rn_aliasT + N'.[_rn] = 1'
                     + @order_sql + @paging;
            END;
            -- T-SQL forbids ORDER BY items not in SELECT when DISTINCT.
            -- Wrap so paging-ORDER applies outside the DISTINCT.
            IF @distinct = 1
                RETURN N'SELECT [_id] FROM (' + CHAR(10)
                     + N'SELECT DISTINCT ' + @rowSelT + N' AS [_id] FROM ('
                     + CHAR(10) + @innerT + CHAR(10) + N') _pvt_cte' + @joinT
                     + CHAR(10) + N'WHERE ' + @whereTp + CHAR(10)
                     + N') _dist'
                     + @order_sql + @paging;
            RETURN N'SELECT '
                 + @rowSelT + N' FROM (' + CHAR(10) + @innerT + CHAR(10) + N') _pvt_cte' + @joinT
                 + CHAR(10) + N'WHERE ' + @whereTp
                 + @order_sql + @paging;
        END;

        DECLARE @whereT  NVARCHAR(MAX) = dbo.pvt_build_where_from_json(@filter, @fields, N'o.');
        DECLARE @filterT NVARCHAR(MAX) = CASE
            WHEN @whereT IS NULL OR @whereT = N'' OR @whereT = N'1=1' THEN N''
            ELSE N' AND ' + @whereT
        END;

        DECLARE @escapedIds NVARCHAR(MAX) = NULL;
        IF @tree_ids IS NOT NULL AND ISJSON(@tree_ids) = 1
            SET @escapedIds = REPLACE(@tree_ids, N'''', N'''''');

        DECLARE @treeWhere NVARCHAR(MAX);

        IF @source_mode = N'tree_roots'
            SET @treeWhere = N'o.[_id_parent] IS NULL';
        ELSE IF @source_mode = N'tree_leaves'
            SET @treeWhere = N'NOT EXISTS (SELECT 1 FROM dbo._objects _lc WHERE _lc.[_id_parent] = o.[_id])';
        ELSE IF @source_mode = N'tree_children'
        BEGIN
            IF @escapedIds IS NULL RETURN NULL;
            SET @treeWhere = N'o.[_id_parent] IN (SELECT CAST([value] AS BIGINT) FROM OPENJSON(N''' + @escapedIds + N'''))';
        END;
        ELSE IF @source_mode = N'tree_ancestors'
        BEGIN
            IF @escapedIds IS NULL RETURN NULL;
            SET @treeWhere = N'o.[_id] IN (SELECT _id FROM dbo.pvt_tree_ancestors('
                + CAST(@scheme_id AS NVARCHAR(40)) + N', N''' + @escapedIds + N''', '
                + CASE WHEN @max_depth IS NULL THEN N'NULL' ELSE CAST(@max_depth AS NVARCHAR(20)) END + N', '
                + CAST(COALESCE(@polymorphic, 1) AS NVARCHAR(1)) + N'))';
        END;
        ELSE  -- tree_descendants: correlated pvt_is_descendant_of, no CTE
        BEGIN
            IF @escapedIds IS NULL RETURN NULL;
            SET @treeWhere = N'EXISTS (SELECT 1 FROM OPENJSON(N''' + @escapedIds
                + N''') _s WHERE dbo.pvt_is_descendant_of(o.[_id], CAST(_s.[value] AS BIGINT)) = 1)';
            IF COALESCE(@include_seed, 1) = 0
                SET @treeWhere += N' AND o.[_id] NOT IN (SELECT CAST([value] AS BIGINT) FROM OPENJSON(N''' + @escapedIds + N'''))';
        END;

        RETURN N'SELECT o.[_id] FROM dbo._objects o'
             + CHAR(10) + N'WHERE o.[_id_scheme] = ' + CAST(@scheme_id AS NVARCHAR(40))
             + N' AND ' + @treeWhere
             + @filterT
             + @order_sql + @paging;
    END;

    -- Unrecognized non-flat source mode: not yet implemented
    IF @source_mode <> N'flat'
        RETURN NULL;

    -- ---------- Shape A (pure-base) ----------
    IF @has_props = 0
    BEGIN
        DECLARE @whereA  NVARCHAR(MAX) = dbo.pvt_build_where_from_json(@filter, @fields, N'o.');
        DECLARE @baseFrom NVARCHAR(MAX) = N'FROM dbo._objects o'
            + CHAR(10) + N'WHERE o.[_id_scheme] = ' + CAST(@scheme_id AS NVARCHAR(40))
            + CASE WHEN @whereA <> N'1=1' THEN N' AND ' + @whereA ELSE N'' END;
        IF @rn_col IS NOT NULL
            -- Project o.* so the outer derived table aliased `o` exposes every
            -- base column ORDER BY may reference (paging-only ORDER BY uses
            -- `(SELECT 1)`, but a user ORDER BY can hit any base field).
            RETURN N'SELECT [_id] FROM (' + CHAR(10)
                 + N'SELECT o.*, ROW_NUMBER() OVER (PARTITION BY ' + @rn_col + N' ORDER BY (SELECT 1)) AS [_rn]' + CHAR(10)
                 + @baseFrom + CHAR(10)
                 + N') o' + CHAR(10) + N'WHERE o.[_rn] = 1'
                 + @order_sql + @paging;
        IF @distinct = 1
            RETURN N'SELECT [_id] FROM (' + CHAR(10)
                 + N'SELECT DISTINCT o.[_id] AS [_id] ' + @baseFrom + CHAR(10)
                 + N') _dist'
                 + @order_sql + @paging;
        RETURN N'SELECT o.[_id] ' + @baseFrom
             + @order_sql + @paging;
    END;

    -- ---------- Shape B (narrow Pro-parity) / Shape C (wide pivot) ----------
    -- Pro-parity base pushdown: peel off base/hierarchical predicates so
    -- they narrow _objects BEFORE the correlated pivot subqueries execute
    -- per row. Residual stays on the outer WHERE.
    DECLARE @splitC NVARCHAR(MAX) = dbo.pvt_split_filter(@filter, @fields);
    DECLARE @pushC  NVARCHAR(MAX) = JSON_VALUE(@splitC, N'$.push');
    DECLARE @resC   NVARCHAR(MAX) = JSON_QUERY(@splitC, N'$.residual');
    DECLARE @inner  NVARCHAR(MAX) = dbo.pvt_build_cte_sql(
        @scheme_id, @fields, N'flat', NULL, NULL, @force_outer, @pushC, @narrow, DEFAULT, DEFAULT, DEFAULT);
    DECLARE @whereC NVARCHAR(MAX) = dbo.pvt_build_where_from_json(@resC, @fields, @base_prefix);
    DECLARE @joinC  NVARCHAR(MAX) = CASE
        WHEN @narrow = 1 THEN CHAR(10) + N'JOIN dbo._objects o ON o.[_id] = _pvt_cte.[_id_object]'
        ELSE N''
    END;
    DECLARE @rowSelC NVARCHAR(MAX) = CASE WHEN @narrow = 1 THEN N'o.[_id]' ELSE N'_pvt_cte.[_id]' END;
    IF @rn_col IS NOT NULL
    BEGIN
        -- Alias outer derived table with the same prefix `@order_sql` uses
        -- (`o.` or `_pvt_cte.`) so base-field references in ORDER BY resolve.
        DECLARE @rn_aliasC NVARCHAR(40) = CASE WHEN @narrow = 1 THEN N'o' ELSE N'_pvt_cte' END;
        RETURN N'SELECT [_id] FROM (' + CHAR(10)
             + N'SELECT ' + @rn_aliasC + N'.*, ROW_NUMBER() OVER (PARTITION BY ' + @rn_col + N' ORDER BY (SELECT 1)) AS [_rn]' + CHAR(10)
             + N'FROM (' + CHAR(10) + @inner + CHAR(10) + N') _pvt_cte' + @joinC + CHAR(10)
             + N'WHERE ' + @whereC + CHAR(10)
             + N') ' + @rn_aliasC + CHAR(10) + N'WHERE ' + @rn_aliasC + N'.[_rn] = 1'
             + @order_sql + @paging;
    END;
    IF @distinct = 1
        RETURN N'SELECT [_id] FROM (' + CHAR(10)
             + N'SELECT DISTINCT ' + @rowSelC + N' AS [_id] FROM ('
             + CHAR(10) + @inner + CHAR(10) + N') _pvt_cte' + @joinC
             + CHAR(10) + N'WHERE ' + @whereC + CHAR(10)
             + N') _dist'
             + @order_sql + @paging;
    RETURN N'SELECT '
         + @rowSelC + N' FROM (' + CHAR(10) + @inner + CHAR(10) + N') _pvt_cte' + @joinC
         + CHAR(10) + N'WHERE ' + @whereC
         + @order_sql + @paging;
END;
GO
