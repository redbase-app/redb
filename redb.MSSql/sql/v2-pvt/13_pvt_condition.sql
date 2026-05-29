-- =====================================================================
-- 13_pvt_condition.sql  (MSSql v2-pvt) — single-leaf predicate builder
-- ---------------------------------------------------------------------
-- Functions:
--   dbo.pvt_sql_string_literal(@val)            -> NVARCHAR escape ' -> ''
--   dbo.pvt_jsonb_to_sql_literal(@val NVARCHAR(MAX), @type INT)
--                                               -> raw token (strings escaped + quoted, numbers bare, booleans 1/0)
--   dbo.pvt_build_field_condition(@field_name, @field_meta, @op_json, @base_prefix)
--                                               -> NVARCHAR(MAX) SQL fragment
--
-- Slice operators (v0.1):
--   $eq $ne $gt $gte $lt $lte
--   $in $nin
--   $like
--   $startsWith $endsWith $contains
--   $startsWithIgnoreCase $endsWithIgnoreCase $containsIgnoreCase
--   $null $isNull $notNull $exists
-- Scalar-literal shorthand: { Field: 5 } == { Field: { $eq: 5 } }
--
-- Skipped: $regex/$iregex/$fts/$array*/length modifier.
-- =====================================================================

SET QUOTED_IDENTIFIER ON;
GO

-- ---------- pvt_sql_string_literal: escape and quote for T-SQL --------
CREATE OR ALTER FUNCTION dbo.pvt_sql_string_literal(@v NVARCHAR(MAX))
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @v IS NULL
        RETURN N'NULL';
    RETURN N'N''' + REPLACE(@v, N'''', N'''''') + N'''';
END;
GO

-- ---------- pvt_jsonb_to_sql_literal ----------------------------------
-- @type follows OPENJSON convention: 0=null, 1=string, 2=number, 3=true/false,
-- 4=array, 5=object. For arrays/objects, callers must use OPENJSON themselves.
CREATE OR ALTER FUNCTION dbo.pvt_jsonb_to_sql_literal(@val NVARCHAR(MAX), @type INT)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @val IS NULL OR @type = 0
        RETURN N'NULL';
    IF @type = 3  -- boolean: T-SQL has no boolean literal in expressions, use 1/0
        RETURN CASE WHEN LOWER(@val) = N'true' THEN N'1' ELSE N'0' END;
    IF @type = 2  -- number
        RETURN @val;
    -- strings (1) or unspecified: quote
    RETURN dbo.pvt_sql_string_literal(@val);
END;
GO

-- ---------- pvt_build_field_condition ---------------------------------
CREATE OR ALTER FUNCTION dbo.pvt_build_field_condition(
    @field_name   NVARCHAR(400),
    @field_meta   NVARCHAR(MAX),
    @op_json      NVARCHAR(MAX),
    @op_type      INT,                 -- OPENJSON type code of @op_json
    @base_prefix  NVARCHAR(10)         -- '' or 'o.' or '_pvt_cte.'
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @field_meta IS NULL
        RETURN NULL;

    DECLARE @kind NVARCHAR(50)  = JSON_VALUE(@field_meta, '$.kind');
    DECLARE @col_name NVARCHAR(100);
    IF @kind = N'base'
        SET @col_name = ISNULL(@base_prefix, N'') + QUOTENAME(JSON_VALUE(@field_meta, '$.column'));
    ELSE
        SET @col_name = QUOTENAME(@field_name);

    -- Array-operator support: resolve field metadata for _values lookups.
    DECLARE @sid       NVARCHAR(32) = JSON_VALUE(@field_meta, N'$.sid');
    DECLARE @db_type_f NVARCHAR(64) = JSON_VALUE(@field_meta, N'$.db_type');
    -- Derive the outer table alias used in EXISTS subqueries against dbo._values.
    -- Shape A: base_prefix = 'o.'  -> alias 'o'
    -- Shape C: base_prefix = '_pvt_cte.' -> alias '_pvt_cte'
    DECLARE @obj_alias NVARCHAR(50) = CASE WHEN @base_prefix = N'o.' THEN N'o' ELSE N'_pvt_cte' END;

    -- ---- Dictionary indexer: Field[key] or Field[key].NestedProp ----
    -- When dict_key is set in field metadata, emit an EXISTS correlated
    -- subquery against dbo._values filtering both structure and _array_index.
    DECLARE @dict_key_val NVARCHAR(200) = JSON_VALUE(@field_meta, N'$.dict_key');
    IF @dict_key_val IS NOT NULL
    AND (@base_prefix = N'_pvt_cte.' OR @base_prefix = N'o.')
    BEGIN
        -- Post-pivot context: dict-key field is materialized as a pivot column
        -- on _pvt_cte by the nested-dict CTE builder (12_pvt_cte_builder.sql).
        -- Reference that column directly instead of re-running an EXISTS over
        -- dbo._values, which would duplicate the lookup work the CTE already did.
        SET @col_name = N'_pvt_cte.' + QUOTENAME(@field_name);
        SET @dict_key_val = NULL;  -- fall through to normal column-comparison flow
    END;
    IF @dict_key_val IS NOT NULL
    BEGIN
        DECLARE @dict_db_col    NVARCHAR(200) = N'av.' + QUOTENAME(ISNULL(JSON_VALUE(@field_meta, N'$.db_column'), N'_String'));
        DECLARE @dict_key_esc   NVARCHAR(400) = REPLACE(@dict_key_val, N'''', N'''''');
        DECLARE @dict_parent_sid NVARCHAR(32) = JSON_VALUE(@field_meta, N'$.parent_sid');
        DECLARE @dict_exist_pfx NVARCHAR(MAX);
        IF @dict_parent_sid IS NOT NULL
            -- Nested dict: Foo[key].Child -> dp holds the dict key on parent_sid,
            -- av is the child row joined through _array_parent_id.
            SET @dict_exist_pfx =
                N'EXISTS (SELECT 1 FROM dbo._values dp'
                + N' INNER JOIN dbo._values av ON av._array_parent_id = dp._id'
                + N' WHERE dp._id_object = ' + @obj_alias + N'.[_id]'
                + N' AND dp._id_structure = ' + @dict_parent_sid
                + N' AND dp._array_index = N''' + @dict_key_esc + N''''
                + N' AND av._id_structure = ' + ISNULL(@sid, N'0')
                + N' AND av._array_index IS NULL';
        ELSE
            SET @dict_exist_pfx =
                N'EXISTS (SELECT 1 FROM dbo._values av'
                + N' WHERE av._id_object = ' + @obj_alias + N'.[_id]'
                + N' AND av._id_structure = ' + ISNULL(@sid, N'0')
                + N' AND av._array_index = N''' + @dict_key_esc + N'''';

        -- Scalar-literal shorthand (non-object operand)
        IF @op_type <> 5
        BEGIN
            IF @op_type = 0
                RETURN @dict_exist_pfx + N' AND ' + @dict_db_col + N' IS NULL)';
            RETURN @dict_exist_pfx + N' AND ' + @dict_db_col + N' = '
                + dbo.pvt_jsonb_to_sql_literal(@op_json, @op_type) + N')';
        END;

        -- Operator object: walk and AND-join
        DECLARE @dparts NVARCHAR(MAX) = N'';
        DECLARE @dcnt   INT = 0;
        DECLARE c_dict CURSOR LOCAL FAST_FORWARD FOR
            SELECT [key], [value], [type] FROM OPENJSON(@op_json);
        DECLARE @dok NVARCHAR(50), @dov NVARCHAR(MAX), @dot INT;
        OPEN c_dict;
        FETCH NEXT FROM c_dict INTO @dok, @dov, @dot;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            DECLARE @don NVARCHAR(50) = LOWER(@dok);
            DECLARE @dpiece NVARCHAR(MAX) = NULL;

            IF @don IN (N'$null', N'$isnull')
                SET @dpiece = CASE WHEN LOWER(@dov) = N'true'
                    THEN N'NOT ' + @dict_exist_pfx + N')'
                    ELSE @dict_exist_pfx + N')' END;
            ELSE IF @don IN (N'$notnull', N'$exists')
                SET @dpiece = CASE WHEN LOWER(@dov) = N'true'
                    THEN @dict_exist_pfx + N')'
                    ELSE N'NOT ' + @dict_exist_pfx + N')' END;
            ELSE IF @don IN (N'$eq', N'$ne', N'$gt', N'$gte', N'$lt', N'$lte')
            BEGIN
                DECLARE @dcmp NVARCHAR(4) = CASE @don
                    WHEN N'$eq'  THEN N'='   WHEN N'$ne'  THEN N'<>'
                    WHEN N'$gt'  THEN N'>'   WHEN N'$gte' THEN N'>='
                    WHEN N'$lt'  THEN N'<'   WHEN N'$lte' THEN N'<=' END;
                IF @dot = 0
                    SET @dpiece = CASE @don
                        WHEN N'$eq' THEN @dict_exist_pfx + N' AND ' + @dict_db_col + N' IS NULL)'
                        WHEN N'$ne' THEN @dict_exist_pfx + N' AND ' + @dict_db_col + N' IS NOT NULL)'
                        ELSE @dict_exist_pfx + N' AND ' + @dict_db_col + N' ' + @dcmp + N' NULL)' END;
                ELSE
                    SET @dpiece = @dict_exist_pfx + N' AND ' + @dict_db_col + N' ' + @dcmp + N' '
                        + dbo.pvt_jsonb_to_sql_literal(@dov, @dot) + N')';
            END
            ELSE IF @don = N'$like'
                SET @dpiece = @dict_exist_pfx + N' AND ' + @dict_db_col + N' LIKE ' + dbo.pvt_sql_string_literal(@dov) + N')';
            ELSE IF @don = N'$contains'
                SET @dpiece = @dict_exist_pfx + N' AND ' + @dict_db_col + N' LIKE ' + dbo.pvt_sql_string_literal(N'%' + @dov + N'%') + N')';
            ELSE IF @don = N'$startswith'
                SET @dpiece = @dict_exist_pfx + N' AND ' + @dict_db_col + N' LIKE ' + dbo.pvt_sql_string_literal(@dov + N'%') + N')';
            ELSE IF @don = N'$endswith'
                SET @dpiece = @dict_exist_pfx + N' AND ' + @dict_db_col + N' LIKE ' + dbo.pvt_sql_string_literal(N'%' + @dov) + N')';
            ELSE IF @don = N'$containsignorecase'
                SET @dpiece = @dict_exist_pfx + N' AND LOWER(' + @dict_db_col + N') LIKE ' + dbo.pvt_sql_string_literal(N'%' + LOWER(@dov) + N'%') + N')';
            ELSE IF @don = N'$startswithignorecase'
                SET @dpiece = @dict_exist_pfx + N' AND LOWER(' + @dict_db_col + N') LIKE ' + dbo.pvt_sql_string_literal(LOWER(@dov) + N'%') + N')';
            ELSE IF @don = N'$endswithignorecase'
                SET @dpiece = @dict_exist_pfx + N' AND LOWER(' + @dict_db_col + N') LIKE ' + dbo.pvt_sql_string_literal(N'%' + LOWER(@dov)) + N')';
            ELSE IF @don IN (N'$in', N'$nin')
            BEGIN
                IF @dot <> 4
                    SET @dpiece = N'/*invalid-dict-in*/1=0';
                ELSE
                BEGIN
                    DECLARE @dlst NVARCHAR(MAX) = N'';
                    DECLARE c_di CURSOR LOCAL FAST_FORWARD FOR SELECT [value], [type] FROM OPENJSON(@dov);
                    DECLARE @div NVARCHAR(MAX), @dit INT;
                    OPEN c_di;
                    FETCH NEXT FROM c_di INTO @div, @dit;
                    WHILE @@FETCH_STATUS = 0
                    BEGIN
                        IF @dlst <> N'' SET @dlst += N', ';
                        SET @dlst += dbo.pvt_jsonb_to_sql_literal(@div, @dit);
                        FETCH NEXT FROM c_di INTO @div, @dit;
                    END;
                    CLOSE c_di; DEALLOCATE c_di;
                    IF @dlst = N'' SET @dlst = N'NULL';
                    SET @dpiece = @dict_exist_pfx + N' AND ' + @dict_db_col
                        + CASE WHEN @don = N'$in' THEN N' IN (' ELSE N' NOT IN (' END
                        + @dlst + N'))';
                END;
            END
            ELSE
                SET @dpiece = N'/*unsupported-dict-op:' + @dok + N'*/1=0';

            IF @dpiece IS NOT NULL
            BEGIN
                IF @dcnt > 0 SET @dparts += N' AND ';
                SET @dparts += @dpiece;
                SET @dcnt += 1;
            END;
            FETCH NEXT FROM c_dict INTO @dok, @dov, @dot;
        END;
        CLOSE c_dict; DEALLOCATE c_dict;

        IF @dcnt = 0 RETURN N'1=1';
        IF @dcnt = 1 RETURN @dparts;
        RETURN N'(' + @dparts + N')';
    END;

    -- ---- ListItem accessor (list_item_prop set): Status.Value / .Alias / .Id
    --      Emits EXISTS subquery against _values [JOIN _list_items] instead of
    --      a plain column comparison so that it works in both Shape A and C.
    DECLARE @li_prop_val  NVARCHAR(50) = JSON_VALUE(@field_meta, N'$.list_item_prop');
    DECLARE @li_is_array  BIT = CASE WHEN JSON_VALUE(@field_meta, N'$.is_array') = N'true' THEN 1 ELSE 0 END;

    IF @li_prop_val IS NOT NULL AND @sid IS NOT NULL
    BEGIN
        DECLARE @li_join     NVARCHAR(MAX) = N'';
        DECLARE @li_cmp_col  NVARCHAR(100);
        DECLARE @li_idx_cond NVARCHAR(60) = CASE WHEN @li_is_array = 1
            THEN N' AND av._array_index IS NOT NULL'
            ELSE N' AND av._array_index IS NULL' END;

        IF @li_prop_val = N'Id'
        BEGIN
            -- No join; compare the raw list-item foreign key (BIGINT)
            SET @li_join    = N'';
            SET @li_cmp_col = N'av.[_ListItem]';
        END
        ELSE IF @li_prop_val = N'Value'
        BEGIN
            SET @li_join    = N' JOIN dbo._list_items li ON li._id = av.[_ListItem]';
            SET @li_cmp_col = N'li.[_value]';
        END
        ELSE  -- Alias
        BEGIN
            SET @li_join    = N' JOIN dbo._list_items li ON li._id = av.[_ListItem]';
            SET @li_cmp_col = N'li.[_alias]';
        END;

        DECLARE @li_pfx NVARCHAR(MAX) =
            N'EXISTS (SELECT 1 FROM dbo._values av' + @li_join
            + N' WHERE av._id_object = ' + @obj_alias + N'.[_id]'
            + N' AND av._id_structure = ' + @sid
            + @li_idx_cond;

        -- Scalar shorthand: equality
        IF @op_type <> 5
        BEGIN
            IF @op_type = 0
                RETURN @li_pfx + N' AND ' + @li_cmp_col + N' IS NULL)';
            RETURN @li_pfx + N' AND ' + @li_cmp_col + N' = '
                + dbo.pvt_jsonb_to_sql_literal(@op_json, @op_type) + N')';
        END;

        -- Operator object
        DECLARE @li_parts NVARCHAR(MAX) = N'';
        DECLARE @li_cnt   INT = 0;
        DECLARE c_li CURSOR LOCAL FAST_FORWARD FOR
            SELECT [key], [value], [type] FROM OPENJSON(@op_json);
        DECLARE @li_ok NVARCHAR(50), @li_ov NVARCHAR(MAX), @li_ot INT;
        OPEN c_li;
        FETCH NEXT FROM c_li INTO @li_ok, @li_ov, @li_ot;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            DECLARE @li_on    NVARCHAR(50) = LOWER(@li_ok);
            DECLARE @li_piece NVARCHAR(MAX) = NULL;

            IF @li_on IN (N'$eq', N'$ne', N'$gt', N'$gte', N'$lt', N'$lte')
            BEGIN
                DECLARE @li_cmp NVARCHAR(4) = CASE @li_on
                    WHEN N'$eq'  THEN N'='   WHEN N'$ne'  THEN N'<>'
                    WHEN N'$gt'  THEN N'>'   WHEN N'$gte' THEN N'>='
                    WHEN N'$lt'  THEN N'<'   WHEN N'$lte' THEN N'<=' END;
                IF @li_ot = 0
                    SET @li_piece = CASE @li_on
                        WHEN N'$eq' THEN @li_pfx + N' AND ' + @li_cmp_col + N' IS NULL)'
                        WHEN N'$ne' THEN @li_pfx + N' AND ' + @li_cmp_col + N' IS NOT NULL)'
                        ELSE @li_pfx + N' AND ' + @li_cmp_col + N' ' + @li_cmp + N' NULL)' END;
                ELSE
                    SET @li_piece = @li_pfx + N' AND ' + @li_cmp_col + N' ' + @li_cmp + N' '
                        + dbo.pvt_jsonb_to_sql_literal(@li_ov, @li_ot) + N')';
            END
            ELSE IF @li_on IN (N'$in', N'$nin')
            BEGIN
                IF @li_ot <> 4
                    SET @li_piece = N'/*invalid-li-in*/1=0';
                ELSE
                BEGIN
                    DECLARE @li_lst NVARCHAR(MAX) = N'';
                    DECLARE c_li_in CURSOR LOCAL FAST_FORWARD FOR
                        SELECT [value], [type] FROM OPENJSON(@li_ov);
                    DECLARE @li_iv NVARCHAR(MAX), @li_it INT;
                    OPEN c_li_in;
                    FETCH NEXT FROM c_li_in INTO @li_iv, @li_it;
                    WHILE @@FETCH_STATUS = 0
                    BEGIN
                        IF @li_lst <> N'' SET @li_lst += N', ';
                        SET @li_lst += dbo.pvt_jsonb_to_sql_literal(@li_iv, @li_it);
                        FETCH NEXT FROM c_li_in INTO @li_iv, @li_it;
                    END;
                    CLOSE c_li_in; DEALLOCATE c_li_in;
                    IF @li_lst = N'' SET @li_lst = N'NULL';
                    -- $nin: NOT EXISTS any matching element
                    SET @li_piece = CASE WHEN @li_on = N'$nin' THEN N'NOT ' ELSE N'' END
                        + @li_pfx + N' AND ' + @li_cmp_col + N' IN (' + @li_lst + N'))';
                END;
            END
            ELSE IF @li_on IN (N'$null', N'$isnull')
                SET @li_piece = CASE WHEN LOWER(@li_ov) = N'true'
                    THEN N'NOT ' + @li_pfx + N')'
                    ELSE @li_pfx + N')' END;
            ELSE IF @li_on IN (N'$notnull', N'$exists')
                SET @li_piece = CASE WHEN LOWER(@li_ov) = N'true'
                    THEN @li_pfx + N')'
                    ELSE N'NOT ' + @li_pfx + N')' END;
            ELSE IF @li_on = N'$like'
                SET @li_piece = @li_pfx + N' AND ' + @li_cmp_col + N' LIKE '
                    + dbo.pvt_sql_string_literal(@li_ov) + N')';
            ELSE IF @li_on = N'$contains'
                SET @li_piece = @li_pfx + N' AND ' + @li_cmp_col + N' LIKE '
                    + dbo.pvt_sql_string_literal(N'%' + @li_ov + N'%') + N')';
            ELSE IF @li_on = N'$startswith'
                SET @li_piece = @li_pfx + N' AND ' + @li_cmp_col + N' LIKE '
                    + dbo.pvt_sql_string_literal(@li_ov + N'%') + N')';
            -- Array ops on ListItem-array accessor (parity with PG `= ANY(arr)`):
            -- the @li_pfx already restricts to array elements when @li_is_array=1,
            -- so $arrayContains is just an equality on the per-element column.
            ELSE IF @li_on = N'$arraycontains' AND @li_is_array = 1
                SET @li_piece = @li_pfx + N' AND ' + @li_cmp_col + N' = '
                    + dbo.pvt_jsonb_to_sql_literal(@li_ov, @li_ot) + N')';
            ELSE IF @li_on IN (N'$arrayany', N'$arrayempty') AND @li_is_array = 1
            BEGIN
                DECLARE @li_want BIT = CASE
                    WHEN @li_on = N'$arrayany'   AND LOWER(@li_ov) = N'true'  THEN 1
                    WHEN @li_on = N'$arrayany'   AND LOWER(@li_ov) = N'false' THEN 0
                    WHEN @li_on = N'$arrayempty' AND LOWER(@li_ov) = N'true'  THEN 0
                    WHEN @li_on = N'$arrayempty' AND LOWER(@li_ov) = N'false' THEN 1
                    ELSE 1 END;
                SET @li_piece = CASE WHEN @li_want = 1 THEN @li_pfx + N')' ELSE N'NOT ' + @li_pfx + N')' END;
            END
            ELSE
                SET @li_piece = N'/*unsupported-li-op:' + @li_ok + N'*/1=0';

            IF @li_piece IS NOT NULL
            BEGIN
                IF @li_cnt > 0 SET @li_parts += N' AND ';
                SET @li_parts += @li_piece;
                SET @li_cnt += 1;
            END;
            FETCH NEXT FROM c_li INTO @li_ok, @li_ov, @li_ot;
        END;
        CLOSE c_li; DEALLOCATE c_li;

        IF @li_cnt = 0 RETURN N'1=1';
        IF @li_cnt = 1 RETURN @li_parts;
        RETURN N'(' + @li_parts + N')';
    END;

    -- Scalar-literal shorthand
    IF @op_type <> 5
    BEGIN
        IF @op_type = 0
            RETURN @col_name + N' IS NULL';
        RETURN @col_name + N' = ' + dbo.pvt_jsonb_to_sql_literal(@op_json, @op_type);
    END;

    -- Walk operator object: collect AND-joined parts
    DECLARE @parts NVARCHAR(MAX) = N'';
    DECLARE @cnt INT = 0;

    DECLARE c CURSOR LOCAL FAST_FORWARD FOR
        SELECT [key], [value], [type] FROM OPENJSON(@op_json);
    DECLARE @opk NVARCHAR(50), @opv NVARCHAR(MAX), @opt INT;
    OPEN c;
    FETCH NEXT FROM c INTO @opk, @opv, @opt;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        DECLARE @on NVARCHAR(50) = LOWER(@opk);
        DECLARE @piece NVARCHAR(MAX) = NULL;

        IF @on IN (N'$eq', N'$ne', N'$gt', N'$gte', N'$lt', N'$lte')
        BEGIN
            DECLARE @cmp NVARCHAR(4) =
                CASE @on WHEN N'$eq' THEN N'=' WHEN N'$ne' THEN N'<>'
                         WHEN N'$gt' THEN N'>' WHEN N'$gte' THEN N'>='
                         WHEN N'$lt' THEN N'<' WHEN N'$lte' THEN N'<=' END;
            IF @opt = 0  -- null
                SET @piece = CASE @on WHEN N'$eq' THEN @col_name + N' IS NULL'
                                      WHEN N'$ne' THEN @col_name + N' IS NOT NULL'
                                      ELSE @col_name + N' ' + @cmp + N' NULL' END;
            ELSE
                SET @piece = @col_name + N' ' + @cmp + N' ' + dbo.pvt_jsonb_to_sql_literal(@opv, @opt);
        END
        ELSE IF @on IN (N'$in', N'$nin')
        BEGIN
            IF @opt <> 4 SET @piece = N'/*invalid-in*/1=0';
            ELSE
            BEGIN
                DECLARE @lst NVARCHAR(MAX) = N'';
                DECLARE c_in CURSOR LOCAL FAST_FORWARD FOR
                    SELECT [value], [type] FROM OPENJSON(@opv);
                DECLARE @iv NVARCHAR(MAX), @it INT;
                OPEN c_in;
                FETCH NEXT FROM c_in INTO @iv, @it;
                WHILE @@FETCH_STATUS = 0
                BEGIN
                    IF @lst <> N'' SET @lst = @lst + N', ';
                    SET @lst = @lst + dbo.pvt_jsonb_to_sql_literal(@iv, @it);
                    FETCH NEXT FROM c_in INTO @iv, @it;
                END;
                CLOSE c_in; DEALLOCATE c_in;
                IF @lst = N'' SET @lst = N'NULL';
                SET @piece = @col_name + CASE WHEN @on = N'$in' THEN N' IN (' ELSE N' NOT IN (' END + @lst + N')';
            END;
        END
        ELSE IF @on = N'$like'
            SET @piece = @col_name + N' LIKE ' + dbo.pvt_sql_string_literal(@opv);
        ELSE IF @on = N'$startswith'
            SET @piece = @col_name + N' LIKE ' + dbo.pvt_sql_string_literal(@opv + N'%');
        ELSE IF @on = N'$endswith'
            SET @piece = @col_name + N' LIKE ' + dbo.pvt_sql_string_literal(N'%' + @opv);
        ELSE IF @on = N'$contains'
            SET @piece = @col_name + N' LIKE ' + dbo.pvt_sql_string_literal(N'%' + @opv + N'%');
        -- T-SQL has no ILIKE; use COLLATE for case-insensitive matching.
        ELSE IF @on = N'$startswithignorecase'
            SET @piece = N'LOWER(' + @col_name + N') LIKE ' + dbo.pvt_sql_string_literal(LOWER(@opv) + N'%');
        ELSE IF @on = N'$endswithignorecase'
            SET @piece = N'LOWER(' + @col_name + N') LIKE ' + dbo.pvt_sql_string_literal(N'%' + LOWER(@opv));
        ELSE IF @on = N'$containsignorecase'
            SET @piece = N'LOWER(' + @col_name + N') LIKE ' + dbo.pvt_sql_string_literal(N'%' + LOWER(@opv) + N'%');
        ELSE IF @on IN (N'$null', N'$isnull')
            SET @piece = CASE WHEN @opv = N'true' THEN @col_name + N' IS NULL' ELSE @col_name + N' IS NOT NULL' END;
        ELSE IF @on IN (N'$notnull', N'$exists')
            SET @piece = CASE WHEN @opv = N'true' THEN @col_name + N' IS NOT NULL' ELSE @col_name + N' IS NULL' END;

        -- ---- Array operators ($arrayContains / $arrayAny / etc.) -------
        -- These emit EXISTS / COUNT subqueries against dbo._values using the
        -- structure id (sid) from the field meta.  They require kind='field'
        -- and a valid sid; for base fields they fall through to unsupported.
        ELSE IF @on = N'$arraycontains'
        BEGIN
            IF @sid IS NULL
                SET @piece = N'/*no-sid-$arrayContains*/1=0';
            ELSE
            BEGIN
                DECLARE @ac_cmp NVARCHAR(MAX) = CASE @db_type_f
                    WHEN N'Long'           THEN N'av.[_Long] = CAST(' + dbo.pvt_jsonb_to_sql_literal(@opv, @opt) + N' AS BIGINT)'
                    WHEN N'Double'         THEN N'av.[_Double] = CAST(' + dbo.pvt_jsonb_to_sql_literal(@opv, @opt) + N' AS FLOAT)'
                    WHEN N'Numeric'        THEN N'av.[_Numeric] = CAST(' + dbo.pvt_jsonb_to_sql_literal(@opv, @opt) + N' AS DECIMAL(28,10))'
                    WHEN N'Boolean'        THEN N'av.[_Boolean] = ' + dbo.pvt_jsonb_to_sql_literal(@opv, @opt)
                    WHEN N'DateTimeOffset' THEN N'av.[_DateTimeOffset] = CAST(' + dbo.pvt_jsonb_to_sql_literal(@opv, @opt) + N' AS DATETIMEOFFSET)'
                    ELSE                        N'av.[_String] = ' + dbo.pvt_jsonb_to_sql_literal(@opv, @opt)
                END;
                SET @piece = N'EXISTS (SELECT 1 FROM dbo._values av'
                    + N' WHERE av._id_object = ' + @obj_alias + N'.[_id]'
                    + N' AND av._id_structure = ' + @sid
                    + N' AND av._array_index IS NOT NULL'
                    + N' AND ' + @ac_cmp + N')';
            END;
        END
        ELSE IF @on IN (N'$arrayany', N'$arrayempty')
        BEGIN
            IF @sid IS NULL
                SET @piece = N'/*no-sid-' + @opk + N'*/1=0';
            ELSE
            BEGIN
                DECLARE @aae_base NVARCHAR(MAX) =
                    N'EXISTS (SELECT 1 FROM dbo._values av'
                    + N' WHERE av._id_object = ' + @obj_alias + N'.[_id]'
                    + N' AND av._id_structure = ' + @sid
                    + N' AND av._array_index IS NOT NULL)';
                -- $arrayAny:   true  -> EXISTS,     false -> NOT EXISTS
                -- $arrayEmpty: true  -> NOT EXISTS,  false -> EXISTS
                DECLARE @aae_want BIT = CASE
                    WHEN @on = N'$arrayany'   AND LOWER(@opv) = N'true'  THEN 1
                    WHEN @on = N'$arrayany'   AND LOWER(@opv) = N'false' THEN 0
                    WHEN @on = N'$arrayempty' AND LOWER(@opv) = N'true'  THEN 0
                    WHEN @on = N'$arrayempty' AND LOWER(@opv) = N'false' THEN 1
                    ELSE 1
                END;
                SET @piece = CASE WHEN @aae_want = 1 THEN @aae_base ELSE N'NOT ' + @aae_base END;
            END;
        END
        ELSE IF @on IN (N'$arraycount', N'$arraycountgt', N'$arraycountgte', N'$arraycountlt', N'$arraycountle')
        BEGIN
            IF @sid IS NULL
                SET @piece = N'/*no-sid-' + @opk + N'*/1=0';
            ELSE
            BEGIN
                DECLARE @acc_cnt NVARCHAR(MAX) =
                    N'(SELECT COUNT(*) FROM dbo._values av'
                    + N' WHERE av._id_object = ' + @obj_alias + N'.[_id]'
                    + N' AND av._id_structure = ' + @sid
                    + N' AND av._array_index IS NOT NULL)';
                DECLARE @acc_val NVARCHAR(20) = ISNULL(CAST(TRY_CAST(@opv AS BIGINT) AS NVARCHAR(20)), N'0');
                DECLARE @acc_op  NVARCHAR(3)  = CASE @on
                    WHEN N'$arraycount'    THEN N'='
                    WHEN N'$arraycountgt'  THEN N'>'
                    WHEN N'$arraycountgte' THEN N'>='
                    WHEN N'$arraycountlt'  THEN N'<'
                    WHEN N'$arraycountle'  THEN N'<='
                    ELSE N'='
                END;
                SET @piece = @acc_cnt + N' ' + @acc_op + N' ' + @acc_val;
            END;
        END
        ELSE IF @on = N'$arrayat'
        BEGIN
            IF @sid IS NULL
                SET @piece = N'/*no-sid-$arrayAt*/1=0';
            ELSE
                -- _array_index is NVARCHAR; the given index value is cast to string
                SET @piece = N'EXISTS (SELECT 1 FROM dbo._values av'
                    + N' WHERE av._id_object = ' + @obj_alias + N'.[_id]'
                    + N' AND av._id_structure = ' + @sid
                    + N' AND av._array_index = N''' + REPLACE(ISNULL(CAST(TRY_CAST(@opv AS BIGINT) AS NVARCHAR(20)), N'0'), N'''', N'''''') + N''')';
        END
        ELSE IF @on IN (N'$arrayfirst', N'$arraylast')
        BEGIN
            IF @sid IS NULL
                SET @piece = N'/*no-sid-' + @opk + N'*/1=0';
            ELSE
            BEGIN
                DECLARE @afl_cmp NVARCHAR(MAX) = CASE @db_type_f
                    WHEN N'Long'           THEN N'av.[_Long] = CAST(' + dbo.pvt_jsonb_to_sql_literal(@opv, @opt) + N' AS BIGINT)'
                    WHEN N'Double'         THEN N'av.[_Double] = CAST(' + dbo.pvt_jsonb_to_sql_literal(@opv, @opt) + N' AS FLOAT)'
                    WHEN N'Numeric'        THEN N'av.[_Numeric] = CAST(' + dbo.pvt_jsonb_to_sql_literal(@opv, @opt) + N' AS DECIMAL(28,10))'
                    WHEN N'Boolean'        THEN N'av.[_Boolean] = ' + dbo.pvt_jsonb_to_sql_literal(@opv, @opt)
                    WHEN N'DateTimeOffset' THEN N'av.[_DateTimeOffset] = CAST(' + dbo.pvt_jsonb_to_sql_literal(@opv, @opt) + N' AS DATETIMEOFFSET)'
                    ELSE                        N'av.[_String] = ' + dbo.pvt_jsonb_to_sql_literal(@opv, @opt)
                END;
                DECLARE @afl_idx NVARCHAR(MAX) = CASE @on
                    WHEN N'$arrayfirst' THEN N'N''0'''
                    ELSE N'(SELECT TOP 1 CAST(av2._array_index AS NVARCHAR(20))'
                        + N' FROM dbo._values av2'
                        + N' WHERE av2._id_object = ' + @obj_alias + N'.[_id]'
                        + N' AND av2._id_structure = ' + @sid
                        + N' AND av2._array_index IS NOT NULL'
                        + N' ORDER BY TRY_CAST(av2._array_index AS INT) DESC)'
                END;
                SET @piece = N'EXISTS (SELECT 1 FROM dbo._values av'
                    + N' WHERE av._id_object = ' + @obj_alias + N'.[_id]'
                    + N' AND av._id_structure = ' + @sid
                    + N' AND av._array_index = ' + @afl_idx
                    + N' AND ' + @afl_cmp + N')';
            END;
        END
        ELSE IF @on = N'$arraystartswith'
        BEGIN
            IF @sid IS NULL
                SET @piece = N'/*no-sid-$arrayStartsWith*/1=0';
            ELSE
                SET @piece = N'EXISTS (SELECT 1 FROM dbo._values av'
                    + N' WHERE av._id_object = ' + @obj_alias + N'.[_id]'
                    + N' AND av._id_structure = ' + @sid
                    + N' AND av._array_index IS NOT NULL'
                    + N' AND av.[_String] LIKE ' + dbo.pvt_sql_string_literal(@opv + N'%') + N')';
        END
        ELSE IF @on = N'$arrayendswith'
        BEGIN
            IF @sid IS NULL
                SET @piece = N'/*no-sid-$arrayEndsWith*/1=0';
            ELSE
                SET @piece = N'EXISTS (SELECT 1 FROM dbo._values av'
                    + N' WHERE av._id_object = ' + @obj_alias + N'.[_id]'
                    + N' AND av._id_structure = ' + @sid
                    + N' AND av._array_index IS NOT NULL'
                    + N' AND av.[_String] LIKE ' + dbo.pvt_sql_string_literal(N'%' + @opv) + N')';
        END
        -- ---- $arrayMatches: LIKE pattern against array element strings ----
        ELSE IF @on = N'$arraymatches'
        BEGIN
            IF @sid IS NULL
                SET @piece = N'/*no-sid-$arrayMatches*/1=0';
            ELSE
                SET @piece = N'EXISTS (SELECT 1 FROM dbo._values av'
                    + N' WHERE av._id_object = ' + @obj_alias + N'.[_id]'
                    + N' AND av._id_structure = ' + @sid
                    + N' AND av._array_index IS NOT NULL'
                    + N' AND av.[_String] LIKE ' + dbo.pvt_sql_string_literal(@opv) + N')';
        END
        -- ---- $arraySum / $arrayMin / $arrayMax / $arrayAvg ---------------
        -- Scalar operand: shorthand for = @value.
        -- Object operand: comparison operators ($eq/$ne/$gt/$gte/$lt/$lte).
        ELSE IF @on IN (N'$arraysum', N'$arraymin', N'$arraymax', N'$arrayavg')
        BEGIN
            IF @sid IS NULL
                SET @piece = N'/*no-sid-' + @opk + N'*/1=0';
            ELSE
            BEGIN
                DECLARE @agg_fn   NVARCHAR(10)  = CASE @on
                    WHEN N'$arraysum' THEN N'SUM'
                    WHEN N'$arraymin' THEN N'MIN'
                    WHEN N'$arraymax' THEN N'MAX'
                    WHEN N'$arrayavg' THEN N'AVG'
                    ELSE N'SUM' END;
                DECLARE @agg_col  NVARCHAR(100) = CASE @db_type_f
                    WHEN N'Long'     THEN N'av.[_Long]'
                    WHEN N'Double'   THEN N'av.[_Double]'
                    WHEN N'Numeric'  THEN N'av.[_Numeric]'
                    ELSE N'CAST(av.[_String] AS DECIMAL(28,10))' END;
                DECLARE @agg_sub  NVARCHAR(MAX) =
                    N'(SELECT ' + @agg_fn + N'(' + @agg_col + N') FROM dbo._values av'
                    + N' WHERE av._id_object = ' + @obj_alias + N'.[_id]'
                    + N' AND av._id_structure = ' + @sid
                    + N' AND av._array_index IS NOT NULL)';
                -- Scalar shorthand: equality check
                IF @opt <> 5
                    SET @piece = @agg_sub + N' = ' + dbo.pvt_jsonb_to_sql_literal(@opv, @opt);
                ELSE
                BEGIN
                    -- Operator object: walk and AND-join comparison ops
                    DECLARE @ag_parts NVARCHAR(MAX) = N'';
                    DECLARE @ag_cnt   INT = 0;
                    DECLARE c_ag CURSOR LOCAL FAST_FORWARD FOR
                        SELECT [key], [value], [type] FROM OPENJSON(@opv);
                    DECLARE @ag_ok NVARCHAR(50), @ag_ov NVARCHAR(MAX), @ag_ot INT;
                    OPEN c_ag;
                    FETCH NEXT FROM c_ag INTO @ag_ok, @ag_ov, @ag_ot;
                    WHILE @@FETCH_STATUS = 0
                    BEGIN
                        DECLARE @ag_cmp NVARCHAR(4) = CASE LOWER(@ag_ok)
                            WHEN N'$eq'  THEN N'='   WHEN N'$ne'  THEN N'<>'
                            WHEN N'$gt'  THEN N'>'   WHEN N'$gte' THEN N'>='
                            WHEN N'$lt'  THEN N'<'   WHEN N'$lte' THEN N'<='
                            ELSE NULL END;
                        IF @ag_cmp IS NOT NULL
                        BEGIN
                            IF @ag_cnt > 0 SET @ag_parts += N' AND ';
                            SET @ag_parts += @agg_sub + N' ' + @ag_cmp + N' '
                                + dbo.pvt_jsonb_to_sql_literal(@ag_ov, @ag_ot);
                            SET @ag_cnt += 1;
                        END;
                        FETCH NEXT FROM c_ag INTO @ag_ok, @ag_ov, @ag_ot;
                    END;
                    CLOSE c_ag; DEALLOCATE c_ag;
                    SET @piece = CASE
                        WHEN @ag_cnt = 0 THEN N'1=1'
                        WHEN @ag_cnt = 1 THEN @ag_parts
                        ELSE N'(' + @ag_parts + N')' END;
                END;
            END;
        END
        ELSE
            SET @piece = N'/*unsupported-op:' + @opk + N'*/1=0';

        IF @piece IS NOT NULL
        BEGIN
            IF @cnt > 0 SET @parts = @parts + N' AND ';
            SET @parts = @parts + @piece;
            SET @cnt = @cnt + 1;
        END;
        FETCH NEXT FROM c INTO @opk, @opv, @opt;
    END;
    CLOSE c; DEALLOCATE c;

    IF @cnt = 0 RETURN N'1=1';
    IF @cnt = 1 RETURN @parts;
    RETURN N'(' + @parts + N')';
END;
GO
