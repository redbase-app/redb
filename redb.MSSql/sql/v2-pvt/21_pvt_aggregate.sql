-- =====================================================================
-- 21_pvt_aggregate.sql  (MSSql v2-pvt) — terminal aggregation orchestrator
-- ---------------------------------------------------------------------
-- Mirrors PG redb.Postgres/sql/v2-pvt/21_pvt_aggregate.sql, slimmed to
-- the operand grammar consumed by RedbQueryable.AggregateAsync and the
-- legacy ExecuteAggregateAsync / ExecuteAggregateBatchAsync paths.
--
-- Aggregation entry shape (per element of @aggs JSON array):
--
--   { "alias": "<name>", "$<func>": "*" | {"$field": "<path>"} }
--
-- Where <func> is one of: count / sum / avg / min / max.
--
-- NOT in this slice (matches MSSql free deliberate cuts):
--   * per-aggregate `distinct` / `filter`
--   * $string_agg / $bool_and / $bool_or
--   * arithmetic / function operands ($add/$mul/$upper/...)
--   * narrow shape (always Shape A or Shape C wide)
--   * pushdown / tree / polymorphic flags
--
-- Output shapes:
--   Shape A (every collected field is kind='base', flat mode):
--      SELECT <agg1> AS [<a1>], ... FROM dbo._objects o
--       WHERE o.[_id_scheme] = X [AND <where>]
--
--   Shape C (any PROPS field referenced anywhere):
--      SELECT <agg1> AS [<a1>], ... FROM (<pvt_build_cte_sql>) _pvt_cte
--       WHERE <where>
-- =====================================================================

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

-- ---------- pvt_extend_fields_with_aggs ------------------------------
-- Start from @existing fields-map (output of pvt_collect_fields on the
-- filter) and APPEND metadata for every $field path referenced inside
-- @aggs that is not already present. Returns a new fields-map JSON.
-- Paths absent from scheme metadata are silently skipped.
CREATE OR ALTER FUNCTION dbo.pvt_extend_fields_with_aggs(
    @scheme_id BIGINT,
    @existing  NVARCHAR(MAX),
    @aggs      NVARCHAR(MAX)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    DECLARE @out NVARCHAR(MAX) =
        CASE WHEN @existing IS NULL OR ISJSON(@existing) = 0 THEN N'{}' ELSE @existing END;
    IF @scheme_id IS NULL OR @aggs IS NULL OR ISJSON(@aggs) = 0
        RETURN @out;

    DECLARE entries CURSOR LOCAL FAST_FORWARD FOR
        SELECT [value] FROM OPENJSON(@aggs);
    DECLARE @entry NVARCHAR(MAX);
    OPEN entries;
    FETCH NEXT FROM entries INTO @entry;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @entry IS NOT NULL AND ISJSON(@entry) = 1
        BEGIN
            DECLARE props CURSOR LOCAL FAST_FORWARD FOR
                SELECT [key], [value], [type] FROM OPENJSON(@entry);
            DECLARE @k NVARCHAR(100), @v NVARCHAR(MAX), @t INT;
            OPEN props;
            FETCH NEXT FROM props INTO @k, @v, @t;
            WHILE @@FETCH_STATUS = 0
            BEGIN
                IF LEFT(@k, 1) = N'$' AND @t = 5
                BEGIN
                    DECLARE @path NVARCHAR(400) = JSON_VALUE(@v, N'$."$field"');
                    IF @path IS NOT NULL
                       AND JSON_QUERY(@out, N'$."' + STRING_ESCAPE(@path, 'json') + N'"') IS NULL
                    BEGIN
                        DECLARE @meta NVARCHAR(MAX) = dbo.pvt_resolve_field_path(@scheme_id, @path);
                        IF @meta IS NOT NULL
                        BEGIN
                            IF @out = N'{}'
                                SET @out = N'{"' + STRING_ESCAPE(@path, 'json') + N'":' + @meta + N'}';
                            ELSE
                                SET @out = LEFT(@out, LEN(@out) - 1)
                                         + N',"' + STRING_ESCAPE(@path, 'json') + N'":' + @meta + N'}';
                        END;
                    END;
                END;
                FETCH NEXT FROM props INTO @k, @v, @t;
            END;
            CLOSE props; DEALLOCATE props;
        END;
        FETCH NEXT FROM entries INTO @entry;
    END;
    CLOSE entries; DEALLOCATE entries;
    RETURN @out;
END;
GO


-- ---------- pvt_agg_field_ref ----------------------------------------
-- Resolve a single $field operand to its column expression. Returns
-- NULL when meta is missing (caller emits a comment marker).
CREATE OR ALTER FUNCTION dbo.pvt_agg_field_ref(
    @path        NVARCHAR(400),
    @fields      NVARCHAR(MAX),
    @base_prefix NVARCHAR(20)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @path IS NULL OR @fields IS NULL OR ISJSON(@fields) = 0
        RETURN NULL;
    DECLARE @meta NVARCHAR(MAX) =
        JSON_QUERY(@fields, N'$."' + STRING_ESCAPE(@path, 'json') + N'"');
    IF @meta IS NULL
        RETURN NULL;
    DECLARE @kind NVARCHAR(32) = JSON_VALUE(@meta, N'$.kind');
    IF @kind = N'base'
    BEGIN
        DECLARE @col NVARCHAR(64) = JSON_VALUE(@meta, N'$.column');
        IF @col IS NULL
            RETURN NULL;
        RETURN COALESCE(@base_prefix, N'') + QUOTENAME(@col);
    END;
    -- Prop / list_item / dict — exposed as a pivot-CTE column under its field-name alias.
    -- Narrow shape (base_prefix='o.') JOINs _objects so the outer SELECT has both
    -- `o.*` (base) and `_pvt_cte.<field>` (pivot). Pivot cols must NOT carry `o.`
    -- — they are unprefixed (resolved to _pvt_cte by scope). In wide shape
    -- (base_prefix='_pvt_cte.') both base and pivot live on _pvt_cte, so the
    -- same prefix applies.
    RETURN CASE WHEN @base_prefix = N'o.' THEN N''
                ELSE COALESCE(@base_prefix, N'') END
         + QUOTENAME(@path);
END;
GO


-- ---------- pvt_build_agg_expr ---------------------------------------
-- Compile a single aggregate entry into a SQL fragment WITHOUT alias.
-- Supports $count / $sum / $avg / $min / $max with operand:
--   "*"                          (count only)
--   { "$field": "<path>" }
--
-- Array-field operands (is_array=true, kind <> 'base') route through a
-- correlated subquery against dbo._values so SUM/AVG/MIN/MAX/COUNT
-- aggregate element values instead of failing on the JSON-array string
-- emitted by the wide pivot. Mirrors PG pvt_build_agg_expr unnest path
-- (redb.Postgres/sql/v2-pvt/19_pvt_agg_expr.sql), using @base_prefix to
-- resolve the per-row object id (`<prefix>[_id]`) for the correlation.
CREATE OR ALTER FUNCTION dbo.pvt_build_agg_expr(
    @entry       NVARCHAR(MAX),
    @fields      NVARCHAR(MAX),
    @base_prefix NVARCHAR(20)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @entry IS NULL OR ISJSON(@entry) = 0
        RETURN N'/* pvt_build_agg_expr: entry not JSON */ NULL';

    DECLARE @op NVARCHAR(50) = NULL, @arg NVARCHAR(MAX) = NULL, @arg_t INT = NULL;
    DECLARE c CURSOR LOCAL FAST_FORWARD FOR
        SELECT [key], [value], [type] FROM OPENJSON(@entry) WHERE LEFT([key], 1) = N'$';
    OPEN c;
    FETCH NEXT FROM c INTO @op, @arg, @arg_t;
    CLOSE c; DEALLOCATE c;
    IF @op IS NULL
        RETURN N'/* pvt_build_agg_expr: no $<func> key */ NULL';

    DECLARE @lo NVARCHAR(50) = LOWER(@op);
    DECLARE @func NVARCHAR(20) = CASE @lo
        WHEN N'$count' THEN N'COUNT'
        WHEN N'$sum'   THEN N'SUM'
        WHEN N'$avg'   THEN N'AVG'
        WHEN N'$min'   THEN N'MIN'
        WHEN N'$max'   THEN N'MAX'
        ELSE NULL
    END;
    IF @func IS NULL
        RETURN N'/* pvt_build_agg_expr: unsupported operator ' + @op + N' */ NULL';

    -- $count "*" → COUNT(*)
    IF @lo = N'$count' AND @arg_t = 1 AND @arg = N'*'
        RETURN N'COUNT(*)';

    -- Operand must be {"$field": "<path>"}.
    DECLARE @path NVARCHAR(400) = NULL;
    IF @arg_t = 5 AND ISJSON(@arg) = 1
        SET @path = JSON_VALUE(@arg, N'$."$field"');
    IF @path IS NULL
        RETURN N'/* pvt_build_agg_expr: operand of ' + @op + N' must be "*" or {"$field":"..."} */ NULL';

    -- Array-field branch: detect is_array=true & non-base. The pivot
    -- column would project the array as JSON, so the orchestrator
    -- emits an OUTER APPLY against _values that pre-computes the
    -- per-row aggregates (sum/cnt/min/max) under the alias
    -- `_aa_<sid>`; each aggregate then references the precomputed
    -- column rather than nesting an aggregate subquery (which T-SQL
    -- forbids). Mirrors PG's unnest path semantically.
    DECLARE @meta NVARCHAR(MAX) =
        JSON_QUERY(@fields, N'$."' + STRING_ESCAPE(@path, 'json') + N'"');
    IF @meta IS NOT NULL
       AND JSON_VALUE(@meta, N'$.is_array') = N'true'
       AND COALESCE(JSON_VALUE(@meta, N'$.kind'), N'') <> N'base'
    BEGIN
        DECLARE @sid NVARCHAR(40) = JSON_VALUE(@meta, N'$.sid');
        IF @sid IS NULL
            RETURN N'/* pvt_build_agg_expr: array field "' + @path + N'" missing sid */ NULL';
        DECLARE @aa NVARCHAR(80) = N'_aa_' + @sid;
        IF @lo = N'$sum'   RETURN N'SUM(' + @aa + N'.[sum])';
        IF @lo = N'$count' RETURN N'SUM(' + @aa + N'.[cnt])';
        IF @lo = N'$min'   RETURN N'MIN(' + @aa + N'.[min])';
        IF @lo = N'$max'   RETURN N'MAX(' + @aa + N'.[max])';
        IF @lo = N'$avg'
            RETURN N'(SUM(' + @aa + N'.[sum]) / NULLIF(SUM(' + @aa + N'.[cnt]), 0))';
        RETURN N'/* pvt_build_agg_expr: array operator ' + @op + N' unsupported */ NULL';
    END;

    DECLARE @col NVARCHAR(MAX) = dbo.pvt_agg_field_ref(@path, @fields, @base_prefix);
    IF @col IS NULL
        RETURN N'/* pvt_build_agg_expr: unknown field "' + @path + N'" */ NULL';

    -- Promote SUM / AVG operands to NUMERIC(38,10) for overflow safety and
    -- to match Pro's decimal-typed public surface.
    IF @lo = N'$sum'
        RETURN N'SUM(CAST(' + @col + N' AS NUMERIC(38, 10)))';
    IF @lo = N'$avg'
        RETURN N'AVG(CAST(' + @col + N' AS NUMERIC(38, 10)))';

    RETURN @func + N'(' + @col + N')';
END;
GO


-- ---------- pvt_build_agg_projection ---------------------------------
-- Compile an array of aggregate entries into a `<sql> AS [<alias>], ...`
-- projection fragment. Defaults aliases to positional `_agg_<i>`.
CREATE OR ALTER FUNCTION dbo.pvt_build_agg_projection(
    @aggs        NVARCHAR(MAX),
    @fields      NVARCHAR(MAX),
    @base_prefix NVARCHAR(20)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @aggs IS NULL OR ISJSON(@aggs) = 0
        RETURN NULL;

    DECLARE @out NVARCHAR(MAX) = N'';
    DECLARE @idx INT = 0;

    DECLARE c CURSOR LOCAL FAST_FORWARD FOR
        SELECT [value] FROM OPENJSON(@aggs);
    DECLARE @entry NVARCHAR(MAX);
    OPEN c;
    FETCH NEXT FROM c INTO @entry;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @idx = @idx + 1;
        DECLARE @alias NVARCHAR(200) = NULL;
        IF @entry IS NOT NULL AND ISJSON(@entry) = 1
            SET @alias = JSON_VALUE(@entry, N'$.alias');
        IF @alias IS NULL OR @alias = N''
            SET @alias = N'_agg_' + CAST(@idx AS NVARCHAR(20));

        DECLARE @sql NVARCHAR(MAX) = dbo.pvt_build_agg_expr(@entry, @fields, @base_prefix);
        IF @idx > 1 SET @out = @out + N', ';
        SET @out = @out + @sql + N' AS ' + QUOTENAME(@alias);
        FETCH NEXT FROM c INTO @entry;
    END;
    CLOSE c; DEALLOCATE c;

    IF @idx = 0
        RETURN NULL;
    RETURN @out;
END;
GO


-- ---------- pvt_build_aggregate_sql ----------------------------------
-- Orchestrator. Returns a single-row terminal SELECT projecting N
-- aggregate columns over the (filtered) PVT source for @scheme_id.
CREATE OR ALTER FUNCTION dbo.pvt_build_aggregate_sql(
    @scheme_id   BIGINT,
    @filter      NVARCHAR(MAX),
    @aggs        NVARCHAR(MAX),
    @source_mode NVARCHAR(50)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @scheme_id IS NULL
        RETURN NULL;
    IF @aggs IS NULL OR ISJSON(@aggs) = 0
        RETURN NULL;
    IF @source_mode IS NULL SET @source_mode = N'flat';
    IF @source_mode <> N'flat'
        RETURN NULL;

    -- 1. Collect fields from filter; extend with fields referenced inside
    --    aggregate operands so kind/column metadata is available downstream.
    DECLARE @filter_fields NVARCHAR(MAX) = dbo.pvt_collect_fields(@scheme_id, @filter, NULL);
    DECLARE @fields        NVARCHAR(MAX) =
        dbo.pvt_extend_fields_with_aggs(@scheme_id, @filter_fields, @aggs);

    -- 2. Decide shape — pure-base flat shortcut (Shape A) vs Shape B/C.
    --    Mirrors PG redb.Postgres/sql/v2-pvt/21_pvt_aggregate.sql narrow/wide selection.
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

    -- ---------- Shape A: pure-base flat aggregation ----------
    IF @has_props = 0
    BEGIN
        DECLARE @whereA NVARCHAR(MAX) = dbo.pvt_build_where_from_json(@filter, @fields, N'o.');
        DECLARE @projA  NVARCHAR(MAX) = dbo.pvt_build_agg_projection(@aggs, @fields, N'o.');
        RETURN N'SELECT ' + @projA
             + N' FROM dbo._objects o'
             + CHAR(10) + N'WHERE o.[_id_scheme] = ' + CAST(@scheme_id AS NVARCHAR(40))
             + CASE WHEN @whereA <> N'1=1' THEN N' AND ' + @whereA ELSE N'' END;
    END;

    -- ---------- Shape B (narrow) or Shape C (wide) ----------
    -- PG-parity narrow/wide decision (mirrors file 20).
    --   @force_outer = 1 -> wide is mandatory (absence checks, no props).
    --   @narrow      = 1 -> Pro-shape: narrow GROUP BY pivot + JOIN _objects.
    -- Pro-parity base pushdown: base/hierarchical predicates narrow
    -- _objects BEFORE correlated pivot subqueries; residual stays outer.
    -- Aggregations over array fields are routed via OUTER APPLY blocks
    -- that pre-compute per-row sum/cnt/min/max of element values
    -- (T-SQL forbids aggregate-of-aggregate-subquery, unlike PG which
    -- can SUM((SELECT SUM(_x) FROM unnest(arr))) directly).
    DECLARE @splitC NVARCHAR(MAX) = dbo.pvt_split_filter(@filter, @fields);
    DECLARE @pushC  NVARCHAR(MAX) = JSON_VALUE(@splitC, N'$.push');
    DECLARE @resC   NVARCHAR(MAX) = JSON_QUERY(@splitC, N'$.residual');

    DECLARE @has_null    BIT = dbo.pvt_has_absence_check(@resC);
    DECLARE @force_outer BIT = CASE WHEN @has_null = 1 OR @has_props = 0 THEN 1 ELSE 0 END;
    -- PG parity: narrow body in 12 currently does NOT support nested-dict.
    DECLARE @narrow      BIT = CASE
        WHEN @force_outer = 0 AND @has_nested = 0 THEN 1
        ELSE 0
    END;

    -- Base alias: narrow JOINs _objects, wide projects base cols from _pvt_cte.
    DECLARE @base_prefix NVARCHAR(20) = CASE WHEN @narrow = 1 THEN N'o.' ELSE N'_pvt_cte.' END;
    -- Per-row correlation for OUTER APPLY: narrow uses o.[_id] (joined),
    -- wide uses _pvt_cte.[_id] (projected by the wide GROUP BY).
    DECLARE @aa_row_ref  NVARCHAR(40) = CASE WHEN @narrow = 1 THEN N'o.[_id]' ELSE N'_pvt_cte.[_id]' END;

    DECLARE @inner  NVARCHAR(MAX) = dbo.pvt_build_cte_sql(
        @scheme_id, @fields, N'flat', NULL, NULL, @force_outer, @pushC, @narrow,
        DEFAULT, DEFAULT, DEFAULT);
    DECLARE @whereC NVARCHAR(MAX) = dbo.pvt_build_where_from_json(@resC, @fields, @base_prefix);
    DECLARE @projC  NVARCHAR(MAX) = dbo.pvt_build_agg_projection(@aggs, @fields, @base_prefix);

    -- Collect distinct array fields referenced by aggregations and
    -- emit one OUTER APPLY per (sid, db_column). Each APPLY exposes
    -- columns [sum]/[cnt]/[min]/[max] keyed on the row's [_id].
    DECLARE @apply_sql NVARCHAR(MAX) = N'';
    DECLARE @seen_sids TABLE(sid NVARCHAR(40) PRIMARY KEY);
    DECLARE ac CURSOR LOCAL FAST_FORWARD FOR
        SELECT [value] FROM OPENJSON(@aggs);
    DECLARE @aentry NVARCHAR(MAX);
    OPEN ac;
    FETCH NEXT FROM ac INTO @aentry;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @aentry IS NOT NULL AND ISJSON(@aentry) = 1
        BEGIN
            DECLARE pc CURSOR LOCAL FAST_FORWARD FOR
                SELECT [value], [type] FROM OPENJSON(@aentry) WHERE LEFT([key], 1) = N'$';
            DECLARE @opv NVARCHAR(MAX), @opt INT;
            OPEN pc;
            FETCH NEXT FROM pc INTO @opv, @opt;
            CLOSE pc; DEALLOCATE pc;
            IF @opt = 5 AND ISJSON(@opv) = 1
            BEGIN
                DECLARE @apath NVARCHAR(400) = JSON_VALUE(@opv, N'$."$field"');
                IF @apath IS NOT NULL
                BEGIN
                    DECLARE @ameta NVARCHAR(MAX) =
                        JSON_QUERY(@fields, N'$."' + STRING_ESCAPE(@apath, 'json') + N'"');
                    IF @ameta IS NOT NULL
                       AND JSON_VALUE(@ameta, N'$.is_array') = N'true'
                       AND COALESCE(JSON_VALUE(@ameta, N'$.kind'), N'') <> N'base'
                    BEGIN
                        DECLARE @asid NVARCHAR(40) = JSON_VALUE(@ameta, N'$.sid');
                        DECLARE @adbcol NVARCHAR(64) = JSON_VALUE(@ameta, N'$.db_column');
                        IF @asid IS NOT NULL AND @adbcol IS NOT NULL
                           AND NOT EXISTS (SELECT 1 FROM @seen_sids WHERE sid = @asid)
                        BEGIN
                            INSERT INTO @seen_sids(sid) VALUES(@asid);
                            SET @apply_sql = @apply_sql + CHAR(10)
                                + N'OUTER APPLY (SELECT '
                                + N'SUM(CAST(va.' + QUOTENAME(@adbcol) + N' AS NUMERIC(38, 10))) AS [sum], '
                                + N'COUNT(va.' + QUOTENAME(@adbcol) + N') AS [cnt], '
                                + N'MIN(va.' + QUOTENAME(@adbcol) + N') AS [min], '
                                + N'MAX(va.' + QUOTENAME(@adbcol) + N') AS [max] '
                                + N'FROM dbo._values va '
                                + N'WHERE va.[_id_object] = ' + @aa_row_ref + N' '
                                + N'AND va.[_id_structure] = ' + @asid + N' '
                                + N'AND va.[_array_index] IS NOT NULL) _aa_' + @asid;
                        END;
                    END;
                END;
            END;
        END;
        FETCH NEXT FROM ac INTO @aentry;
    END;
    CLOSE ac; DEALLOCATE ac;

    RETURN N'SELECT ' + @projC
         + N' FROM (' + CHAR(10) + @inner + CHAR(10) + N') _pvt_cte'
         + CASE WHEN @narrow = 1
                THEN CHAR(10) + N'JOIN dbo._objects o ON o.[_id] = _pvt_cte.[_id_object]'
                ELSE N'' END
         + @apply_sql
         + CHAR(10) + N'WHERE ' + @whereC;
END;
GO
