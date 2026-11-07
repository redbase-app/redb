-- =====================================================================
-- 26_pvt_array_groupby.sql  (MSSql v2-pvt)
-- ---------------------------------------------------------------------
-- GROUP BY over the elements of an array (GroupByArray of the free provider). Returns a complete
-- T-SQL SELECT ready to be used as a derived table.
--
--   dbo.pvt_build_array_groupby_sql(
--       @scheme_id     BIGINT,
--       @array_path    NVARCHAR(400),   -- path to the array through classes and arrays:
--                                       -- 'Contacts', 'Contacts.Metadata', 'HomeAddress.Building.Amenities'
--       @filter        NVARCHAR(MAX),   -- optional condition on the owning objects (PVT JSON)
--       @group_by      NVARCHAR(MAX),   -- non-empty JSON array of {field, alias}
--       @aggregations  NVARCHAR(MAX),   -- optional JSON array of {field, func, alias, filter?}
--       @having        NVARCHAR(MAX),   -- optional boolean expression (see pvt_build_array_having_expr)
--       @source_mode   NVARCHAR(50)     -- 'flat'
--   ) RETURNS NVARCHAR(MAX)
--
-- A field is a path inside the element ('Type', 'Address.City') or '$element', the element itself
-- of an array of simple values. A func is COUNT, SUM, AVG, MIN or MAX; COUNT of '*' with a filter is
-- the count of the elements that satisfy it.
--
-- Shape (the same in the PostgreSQL module, the SQLite extension and the Pro builder;
-- docs/array-groupby/PLAN.md): the elements of the last array of the path are found by its
-- structure; with a condition they are reached from the ids of the objects that satisfy it; every
-- field is joined to its element (or to its class inside the element) by _array_parent_id with
-- "_array_parent_id IS NOT NULL", which lets SQL Server read the filtered covering index
-- IX__values__structure_parent_batch instead of scanning IX__values__array_parent_id.
--
-- A scalar function cannot raise. Everything it cannot answer comes back as
-- N'--error: <text>' and the provider throws with that text: a path that is not an array, a
-- dictionary in the path, a field the elements do not have, '$element' on an array of classes, a
-- field of an array of simple values, a collection or a class named as a value, an unknown '$'
-- token, an unknown func, a condition that does not translate, an outer filter that does not compile.
-- =====================================================================

SET QUOTED_IDENTIFIER ON;
GO

-- ---------------------------------------------------------------------
-- pvt_build_array_having_expr - boolean expressions of the array GROUP BY (HAVING and the filter
-- of a conditional count).
--
-- Element paths are read through @cols, a JSON map {"<path>": "<joined typed column>"} built by the
-- orchestrator - the same columns the SELECT list reads.
--
-- Grammar:
--   { "$and": [ ... ] } / { "$or": [ ... ] } / { "$not": { ... } }
--   { "$isnull": <operand> } / { "$notnull": <operand> }
--   { "$gt"|"$gte"|"$lt"|"$lte"|"$eq"|"$ne": [ <operand>, <operand> ] }
--   <operand> ::=
--     { "$field": "X" }                                  -> <column of X>
--     { "$count": "*" }                                  -> COUNT(*)
--     { "$count": "*", "filter": <expression> }          -> COUNT(CASE WHEN <expression> THEN 1 END)
--     { "$count"|"$sum"|"$avg"|"$min"|"$max": { "$field": "X" } } -> FUNC(<column of X>)
--     { "$const": <non-null scalar> }                    -> literal
--
-- Anything else - an unknown node, a path with no joined column, a null constant - returns NULL,
-- and the orchestrator refuses the whole query.
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
    SELECT TOP 1 @k = [key], @v = [value], @t = [type] FROM OPENJSON(@node) WHERE [key] <> N'filter';
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

    IF @k IN (N'$isnull', N'$notnull')
        RETURN CASE WHEN @t = 5
                    THEN N'(' + dbo.pvt_build_array_having_expr(@v, @cols)
                         + CASE @k WHEN N'$isnull' THEN N' IS NULL)' ELSE N' IS NOT NULL)' END
                    ELSE NULL END;

    IF @k = N'$field'
    BEGIN
        IF @t <> 1 OR @cols IS NULL RETURN NULL;
        RETURN JSON_VALUE(@cols, N'$."' + STRING_ESCAPE(@v, 'json') + N'"');
    END;

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
        DECLARE @cond NVARCHAR(MAX) = JSON_QUERY(@node, N'$.filter');
        IF @cond IS NOT NULL
        BEGIN
            IF @k <> N'$count' OR @t <> 1 OR @v <> N'*' RETURN NULL;
            RETURN N'COUNT(CASE WHEN ' + dbo.pvt_build_array_having_expr(@cond, @cols) + N' THEN 1 END)';
        END;
        IF @k = N'$count' AND @t = 1 AND @v = N'*'
            RETURN N'COUNT(*)';
        IF @t <> 5 RETURN NULL;
        DECLARE @col NVARCHAR(400) = dbo.pvt_build_array_having_expr(@v, @cols);
        IF @col IS NULL RETURN NULL;
        RETURN @fn + N'(' + @col + N')';
    END;

    IF @k = N'$const'
        RETURN CASE WHEN @t IN (0, 4, 5) THEN NULL ELSE dbo.pvt_jsonb_to_sql_literal(@v, @t) END;

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
-- pvt_build_array_groupby_sql - main orchestrator (7 positional params).
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
    IF @scheme_id IS NULL RETURN N'--error: the scheme id is required';
    IF @array_path IS NULL OR @array_path = N'' RETURN N'--error: the array path is required';
    IF ISNULL(@source_mode, N'flat') <> N'flat' RETURN N'--error: only the flat source is supported';
    IF @group_by IS NULL OR ISJSON(@group_by) = 0 OR (SELECT COUNT(*) FROM OPENJSON(@group_by)) = 0
        RETURN N'--error: the grouping needs at least one key';

    -- ---- the array: every segment exists, none is a dictionary, the last one is an array
    DECLARE @parent BIGINT = NULL, @arr_sid BIGINT = NULL, @arr_db_type NVARCHAR(50), @arr_type NVARCHAR(100),
            @arr_coll NVARCHAR(100), @seg NVARCHAR(400);
    DECLARE c_seg CURSOR LOCAL FAST_FORWARD FOR
        SELECT [value] FROM OPENJSON(N'["' + REPLACE(STRING_ESCAPE(@array_path, 'json'), N'.', N'","') + N'"]') ORDER BY CAST([key] AS INT);
    OPEN c_seg;
    FETCH NEXT FROM c_seg INTO @seg;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @arr_sid = NULL;
        SELECT TOP 1 @arr_sid = c._structure_id, @arr_db_type = c.db_type, @arr_type = c.type_name, @arr_coll = c.collection_type_name
        FROM dbo._scheme_metadata_cache c
        WHERE c._scheme_id = @scheme_id AND c._name = @seg
          AND ((@parent IS NULL AND c._parent_structure_id IS NULL) OR c._parent_structure_id = @parent);
        IF @arr_sid IS NULL
        BEGIN
            CLOSE c_seg; DEALLOCATE c_seg;
            RETURN N'--error: "' + @seg + N'" of the array path "' + @array_path + N'" is not a field of scheme ' + CAST(@scheme_id AS NVARCHAR(20));
        END;
        IF @arr_coll = N'Dictionary'
        BEGIN
            CLOSE c_seg; DEALLOCATE c_seg;
            RETURN N'--error: "' + @seg + N'" in the array path "' + @array_path + N'" is a dictionary; dictionaries are not supported in the array path';
        END;
        SET @parent = @arr_sid;
        FETCH NEXT FROM c_seg INTO @seg;
    END;
    CLOSE c_seg; DEALLOCATE c_seg;
    IF ISNULL(@arr_coll, N'') <> N'Array' RETURN N'--error: "' + @array_path + N'" is not an array';
    DECLARE @is_class BIT = CASE WHEN @arr_type = N'Class' THEN 1 ELSE 0 END;

    -- ---- every element path the statement reads
    DECLARE @paths TABLE(path NVARCHAR(400) PRIMARY KEY);
    INSERT @paths(path)
        SELECT DISTINCT JSON_VALUE([value], N'$.field') FROM OPENJSON(@group_by)
        WHERE JSON_VALUE([value], N'$.field') IS NOT NULL;
    IF EXISTS (SELECT 1 FROM OPENJSON(@group_by) WHERE ISNULL(JSON_VALUE([value], N'$.field'), N'') = N'')
        RETURN N'--error: a key has no field';
    IF @aggregations IS NOT NULL
    BEGIN
        IF ISJSON(@aggregations) = 0 RETURN N'--error: the aggregations are not JSON';
        INSERT @paths(path)
            SELECT DISTINCT p FROM (
                SELECT JSON_VALUE([value], N'$.field') AS p FROM OPENJSON(@aggregations)
                UNION
                SELECT r.name FROM OPENJSON(@aggregations) a CROSS APPLY dbo.pvt_json_field_refs(JSON_QUERY(a.[value], N'$.filter')) r
            ) x
            WHERE p IS NOT NULL AND p <> N'*' AND p NOT IN (SELECT path FROM @paths);
    END;
    IF @having IS NOT NULL
    BEGIN
        IF ISJSON(@having) = 0 OR @having = N'{}' RETURN N'--error: the HAVING is not a JSON expression';
        INSERT @paths(path)
            SELECT name FROM dbo.pvt_json_field_refs(@having) WHERE name NOT IN (SELECT path FROM @paths);
    END;

    -- ---- the column of every path, with its joins
    DECLARE @joins TABLE(ord INT PRIMARY KEY, alias NVARCHAR(10), parent NVARCHAR(10), sid BIGINT);
    DECLARE @cols TABLE(path NVARCHAR(400) PRIMARY KEY, col NVARCHAR(400));
    DECLARE @path NVARCHAR(400), @col NVARCHAR(64), @alias NVARCHAR(10), @fparent BIGINT, @fsid BIGINT,
            @fdb NVARCHAR(50), @ftype NVARCHAR(100), @fcoll NVARCHAR(100), @i INT, @n INT, @prefix NVARCHAR(400);
    DECLARE c_path CURSOR LOCAL FAST_FORWARD FOR SELECT path FROM @paths;
    OPEN c_path;
    FETCH NEXT FROM c_path INTO @path;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @path = N'$element'
        BEGIN
            IF @is_class = 1
            BEGIN
                CLOSE c_path; DEALLOCATE c_path;
                RETURN N'--error: "$element" names the element itself, but the elements of "' + @array_path + N'" are classes; name a field of the element';
            END;
            SET @col = dbo.pvt_db_type_to_value_column(@arr_db_type);
            IF @col IS NULL
            BEGIN
                CLOSE c_path; DEALLOCATE c_path;
                RETURN N'--error: the elements of "' + @array_path + N'" are stored as ' + ISNULL(@arr_db_type, N'?') + N', which cannot be grouped or aggregated';
            END;
            INSERT @cols(path, col) VALUES (@path, N'v.[' + @col + N']');
        END
        ELSE IF LEFT(@path, 1) = N'$'
        BEGIN
            CLOSE c_path; DEALLOCATE c_path;
            RETURN N'--error: "' + @path + N'" is not a field of the elements of "' + @array_path + N'"';
        END
        ELSE IF @is_class = 0
        BEGIN
            CLOSE c_path; DEALLOCATE c_path;
            RETURN N'--error: the elements of "' + @array_path + N'" are simple values and have no field "' + @path + N'"; use the element itself';
        END
        ELSE
        BEGIN
            SET @fparent = @arr_sid; SET @alias = N'v'; SET @prefix = N'';
            SELECT @n = COUNT(*) FROM STRING_SPLIT(@path, N'.');
            SET @i = 0;
            DECLARE c_fs CURSOR LOCAL FAST_FORWARD FOR
                SELECT [value] FROM OPENJSON(N'["' + REPLACE(STRING_ESCAPE(@path, 'json'), N'.', N'","') + N'"]') ORDER BY CAST([key] AS INT);
            OPEN c_fs;
            FETCH NEXT FROM c_fs INTO @seg;
            WHILE @@FETCH_STATUS = 0
            BEGIN
                SET @i += 1;
                SET @prefix = CASE WHEN @prefix = N'' THEN @seg ELSE @prefix + N'.' + @seg END;
                SET @fsid = NULL;
                SELECT TOP 1 @fsid = c._structure_id, @fdb = c.db_type, @ftype = c.type_name, @fcoll = c.collection_type_name
                FROM dbo._scheme_metadata_cache c
                WHERE c._scheme_id = @scheme_id AND c._name = @seg AND c._parent_structure_id = @fparent;
                IF @fsid IS NULL
                BEGIN
                    CLOSE c_fs; DEALLOCATE c_fs; CLOSE c_path; DEALLOCATE c_path;
                    RETURN N'--error: "' + @prefix + N'" is not a field of the elements of "' + @array_path + N'"';
                END;
                IF @fcoll IS NOT NULL
                BEGIN
                    CLOSE c_fs; DEALLOCATE c_fs; CLOSE c_path; DEALLOCATE c_path;
                    RETURN N'--error: "' + @prefix + N'" is a collection inside the element of "' + @array_path + N'"; group the inner collection instead';
                END;
                IF @i < @n AND ISNULL(@ftype, N'') <> N'Class'
                BEGIN
                    CLOSE c_fs; DEALLOCATE c_fs; CLOSE c_path; DEALLOCATE c_path;
                    RETURN N'--error: "' + @prefix + N'" is a value, not a class with fields';
                END;
                IF @i = @n AND @ftype = N'Class'
                BEGIN
                    CLOSE c_fs; DEALLOCATE c_fs; CLOSE c_path; DEALLOCATE c_path;
                    RETURN N'--error: "' + @path + N'" is a class; a key or aggregate reads a stored value';
                END;
                DECLARE @existing NVARCHAR(10) = (SELECT alias FROM @joins WHERE parent = @alias AND sid = @fsid);
                IF @existing IS NULL
                BEGIN
                    DECLARE @ord INT = (SELECT COUNT(*) FROM @joins) + 1;
                    SET @existing = N'e' + CAST(@ord AS NVARCHAR(5));
                    INSERT @joins(ord, alias, parent, sid) VALUES (@ord, @existing, @alias, @fsid);
                END;
                SET @alias = @existing;
                SET @fparent = @fsid;
                FETCH NEXT FROM c_fs INTO @seg;
            END;
            CLOSE c_fs; DEALLOCATE c_fs;
            SET @col = dbo.pvt_db_type_to_value_column(@fdb);
            IF @col IS NULL
            BEGIN
                CLOSE c_path; DEALLOCATE c_path;
                RETURN N'--error: "' + @path + N'" is stored as ' + ISNULL(@fdb, N'?') + N', which cannot be grouped or aggregated';
            END;
            INSERT @cols(path, col) VALUES (@path, @alias + N'.[' + @col + N']');
        END;
        FETCH NEXT FROM c_path INTO @path;
    END;
    CLOSE c_path; DEALLOCATE c_path;

    DECLARE @cols_json NVARCHAR(MAX) = N'{' + COALESCE(
        (SELECT STRING_AGG(N'"' + STRING_ESCAPE(path, 'json') + N'":"' + STRING_ESCAPE(col, 'json') + N'"', N',') FROM @cols), N'') + N'}';
    DECLARE @join_sql NVARCHAR(MAX) = COALESCE((
        SELECT STRING_AGG(CAST(CHAR(10) + N'LEFT JOIN dbo._values ' + alias + N' ON ' + alias + N'.[_array_parent_id] = ' + parent + N'.[_id]'
               + N' AND ' + alias + N'.[_id_structure] = ' + CAST(sid AS NVARCHAR(20))
               + N' AND ' + alias + N'.[_array_parent_id] IS NOT NULL' AS NVARCHAR(MAX)), N'') WITHIN GROUP (ORDER BY ord)
        FROM @joins), N'');

    -- ---- keys
    DECLARE @select NVARCHAR(MAX) = N'', @group_cols NVARCHAR(MAX) = N'';
    SELECT @select = STRING_AGG(CAST(c.col + N' AS ' + QUOTENAME(ISNULL(JSON_VALUE(g.[value], N'$.alias'), JSON_VALUE(g.[value], N'$.field'))) AS NVARCHAR(MAX)), N', ')
                     WITHIN GROUP (ORDER BY CAST(g.[key] AS INT))
    FROM OPENJSON(@group_by) g
    JOIN @cols c ON c.path = JSON_VALUE(g.[value], N'$.field');
    SELECT @group_cols = STRING_AGG(CAST(c.col AS NVARCHAR(MAX)), N', ') WITHIN GROUP (ORDER BY CAST(g.[key] AS INT))
    FROM OPENJSON(@group_by) g
    JOIN @cols c ON c.path = JSON_VALUE(g.[value], N'$.field');

    -- ---- aggregates
    IF @aggregations IS NOT NULL
    BEGIN
        DECLARE @a NVARCHAR(MAX), @af NVARCHAR(400), @afn NVARCHAR(20), @aal NVARCHAR(200), @afilter NVARCHAR(MAX), @expr NVARCHAR(MAX);
        DECLARE c_agg CURSOR LOCAL FAST_FORWARD FOR SELECT [value] FROM OPENJSON(@aggregations) ORDER BY CAST([key] AS INT);
        OPEN c_agg;
        FETCH NEXT FROM c_agg INTO @a;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @af = ISNULL(JSON_VALUE(@a, N'$.field'), N'*');
            SET @afn = UPPER(ISNULL(JSON_VALUE(@a, N'$.func'), N''));
            SET @aal = ISNULL(JSON_VALUE(@a, N'$.alias'), @afn);
            SET @afilter = JSON_QUERY(@a, N'$.filter');
            IF @afn NOT IN (N'COUNT', N'SUM', N'AVG', N'MIN', N'MAX')
            BEGIN
                CLOSE c_agg; DEALLOCATE c_agg;
                RETURN N'--error: unknown aggregate function "' + ISNULL(JSON_VALUE(@a, N'$.func'), N'') + N'"';
            END;
            IF @afilter IS NOT NULL
            BEGIN
                IF @afn <> N'COUNT' OR @af <> N'*'
                BEGIN
                    CLOSE c_agg; DEALLOCATE c_agg;
                    RETURN N'--error: only COUNT(*) takes a filter ("' + @aal + N'")';
                END;
                SET @expr = dbo.pvt_build_array_having_expr(@afilter, @cols_json);
                IF @expr IS NULL
                BEGIN
                    CLOSE c_agg; DEALLOCATE c_agg;
                    RETURN N'--error: the condition of "' + @aal + N'" does not translate';
                END;
                SET @expr = N'COUNT(CASE WHEN ' + @expr + N' THEN 1 END)';
            END
            ELSE IF @af = N'*'
            BEGIN
                IF @afn <> N'COUNT'
                BEGIN
                    CLOSE c_agg; DEALLOCATE c_agg;
                    RETURN N'--error: ' + @afn + N' needs a field ("' + @aal + N'")';
                END;
                SET @expr = N'COUNT(*)';
            END
            ELSE
                SET @expr = @afn + N'(' + (SELECT col FROM @cols WHERE path = @af) + N')';
            SET @select += N', ' + @expr + N' AS ' + QUOTENAME(@aal);
            FETCH NEXT FROM c_agg INTO @a;
        END;
        CLOSE c_agg; DEALLOCATE c_agg;
    END;

    -- ---- HAVING
    DECLARE @having_clause NVARCHAR(MAX) = N'';
    IF @having IS NOT NULL
    BEGIN
        DECLARE @having_sql NVARCHAR(MAX) = dbo.pvt_build_array_having_expr(@having, @cols_json);
        IF @having_sql IS NULL RETURN N'--error: the HAVING does not translate';
        SET @having_clause = CHAR(10) + N'HAVING ' + @having_sql;
    END;

    -- ---- source of the elements
    DECLARE @from NVARCHAR(MAX);
    IF @filter IS NOT NULL AND ISJSON(@filter) = 1 AND @filter <> N'{}'
    BEGIN
        DECLARE @filter_sql NVARCHAR(MAX) = dbo.pvt_build_query_sql(
            @scheme_id, @filter, NULL, NULL, NULL, NULL, 0,
            N'flat', NULL, NULL, 0, NULL);
        IF @filter_sql IS NULL RETURN N'--error: the condition on the objects did not compile';
        SET @from = N'FROM (SELECT DISTINCT [_id] FROM (' + @filter_sql + N') _filt) f' + CHAR(10)
            + N'INNER JOIN dbo._objects o ON o.[_id] = f.[_id] AND o.[_id_scheme] = ' + CAST(@scheme_id AS NVARCHAR(20)) + CHAR(10)
            + N'INNER JOIN dbo._values v ON v.[_id_object] = f.[_id] AND v.[_id_structure] = ' + CAST(@arr_sid AS NVARCHAR(20))
            + N' AND v.[_array_index] IS NOT NULL'
            + @join_sql;
    END
    ELSE
        SET @from = N'FROM dbo._values v' + CHAR(10)
            + N'INNER JOIN dbo._objects o ON o.[_id] = v.[_id_object] AND o.[_id_scheme] = ' + CAST(@scheme_id AS NVARCHAR(20))
            + @join_sql + CHAR(10)
            + N'WHERE v.[_id_structure] = ' + CAST(@arr_sid AS NVARCHAR(20)) + N' AND v.[_array_index] IS NOT NULL';

    RETURN N'SELECT ' + @select + CHAR(10) + @from + CHAR(10) + N'GROUP BY ' + @group_cols + @having_clause;
END;
GO
