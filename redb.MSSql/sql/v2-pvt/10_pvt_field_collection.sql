-- =====================================================================
-- 10_pvt_field_collection.sql  (MSSql v2-pvt)
-- ---------------------------------------------------------------------
-- Field-name collection from filter/order JSON and metadata resolution
-- via _scheme_metadata_cache. Produces the `fields` JSON consumed by
-- pvt_build_column_expr / pvt_build_cte_sql / pvt_build_*_condition.
--
-- Minimal-port functions:
--   dbo.pvt_resolve_field_path(@scheme_id, @path)            -> NVARCHAR(MAX) JSON FieldInfo
--   dbo.pvt_extract_field_pairs(@filter)                     -> NVARCHAR(MAX) JSON array of {path,op_value}
--   dbo.pvt_collect_fields(@scheme_id, @filter, @order)      -> NVARCHAR(MAX) JSON object keyed by field name
--   dbo.pvt_has_absence_check(@filter)                       -> BIT
--
-- Skipped vs PG (v0.1 slice): $expr field walk, $case/$fts/$cast/$dateAdd
-- branches, length/count modifier registration.
-- =====================================================================

SET QUOTED_IDENTIFIER ON;
GO

-- ---------- pvt_resolve_field_path ------------------------------------
CREATE OR ALTER FUNCTION dbo.pvt_resolve_field_path(
    @scheme_id BIGINT,
    @path      NVARCHAR(400)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @scheme_id IS NULL OR @path IS NULL OR @path = N''
        RETURN NULL;

    -- 0. Base?
    DECLARE @base_col NVARCHAR(100) = dbo.pvt_normalize_base_field_name(@path);
    IF @base_col IS NOT NULL
        RETURN N'{"kind":"base","column":"' + @base_col
             + N'","name":' + dbo.pvt_json_string_or_null(@path)
             + N',"is_array":false,"list_item_prop":null,"dict_key":null,"parent_sid":null,"sid":null,"db_type":null,"db_column":null}';

    DECLARE @sid          BIGINT,
            @db_type      NVARCHAR(50),
            @db_col       NVARCHAR(50),
            @is_array     BIT = 0,
            @list_prop    NVARCHAR(20) = NULL,
            @dict_key     NVARCHAR(200) = NULL,
            @parent_sid   BIGINT = NULL,
            @root_name    NVARCHAR(200),
            @child_name   NVARCHAR(200),
            @rest         NVARCHAR(400);

    -- 1. Dictionary path: Name[key] or Name[key].Child
    DECLARE @lb INT = CHARINDEX(N'[', @path);
    DECLARE @rb INT = CHARINDEX(N']', @path);
    IF @lb > 0 AND @rb > @lb + 1
    BEGIN
        SET @root_name = SUBSTRING(@path, 1, @lb - 1);
        SET @dict_key  = SUBSTRING(@path, @lb + 1, @rb - @lb - 1);
        SET @rest      = SUBSTRING(@path, @rb + 1, 400);

        SELECT TOP 1 @sid = c._structure_id, @db_type = c.db_type
          FROM dbo._scheme_metadata_cache c
         WHERE c._scheme_id = @scheme_id AND c._name = @root_name AND c._parent_structure_id IS NULL;
        IF @sid IS NULL
            RETURN NULL;

        IF @rest LIKE N'.%'
        BEGIN
            SET @parent_sid = @sid;
            SET @child_name = SUBSTRING(@rest, 2, 400);
            SELECT TOP 1 @sid = c._structure_id, @db_type = c.db_type,
                         @is_array = CASE WHEN c._collection_type IS NOT NULL THEN 1 ELSE 0 END
              FROM dbo._scheme_metadata_cache c
             WHERE c._scheme_id = @scheme_id AND c._name = @child_name AND c._parent_structure_id = @parent_sid;
            IF @sid IS NULL
                RETURN NULL;
        END;

        SET @db_col = dbo.pvt_db_type_to_value_column(@db_type);
        RETURN N'{"kind":"field","sid":' + CAST(@sid AS NVARCHAR(40))
             + N',"db_type":' + dbo.pvt_json_string_or_null(@db_type)
             + N',"db_column":' + dbo.pvt_json_string_or_null(@db_col)
             + N',"name":' + dbo.pvt_json_string_or_null(@path)
             + N',"is_array":' + CASE WHEN @is_array = 1 THEN N'true' ELSE N'false' END
             + N',"list_item_prop":null'
             + N',"dict_key":' + dbo.pvt_json_string_or_null(@dict_key)
             + N',"parent_sid":' + CASE WHEN @parent_sid IS NULL THEN N'null' ELSE CAST(@parent_sid AS NVARCHAR(40)) END
             + N'}';
    END;

    -- 2. Dotted nested
    IF CHARINDEX(N'.', @path) > 0
    BEGIN
        DECLARE @last NVARCHAR(200), @first NVARCHAR(200);
        SET @last  = REVERSE(LEFT(REVERSE(@path), CHARINDEX(N'.', REVERSE(@path)) - 1));
        SET @first = LEFT(@path, CHARINDEX(N'.', @path) - 1);

        -- 2a. Foo[].Value|Alias|Id  (ListItem array accessors)
        IF @last IN (N'Id', N'Value', N'Alias') AND RIGHT(@first, 2) = N'[]'
           AND CHARINDEX(N'.', @path) = LEN(@first) + 1
        BEGIN
            DECLARE @root_li NVARCHAR(200) = LEFT(@first, LEN(@first) - 2);
            SELECT TOP 1 @sid = c._structure_id, @db_type = c.db_type,
                         @is_array = CASE WHEN c._collection_type IS NOT NULL THEN 1 ELSE 0 END
              FROM dbo._scheme_metadata_cache c
             WHERE c._scheme_id = @scheme_id AND c._name = @root_li AND c._parent_structure_id IS NULL;
            IF @sid IS NOT NULL AND @db_type = N'ListItem' AND @is_array = 1
            BEGIN
                SET @list_prop = @last;
                RETURN N'{"kind":"field","sid":' + CAST(@sid AS NVARCHAR(40))
                     + N',"db_type":"ListItem","db_column":"_ListItem"'
                     + N',"name":' + dbo.pvt_json_string_or_null(@path)
                     + N',"is_array":true'
                     + N',"list_item_prop":' + dbo.pvt_json_string_or_null(@list_prop)
                     + N',"dict_key":null,"parent_sid":null}';
            END;
        END;

        -- 2b. Status.Value|Alias|Id  (scalar ListItem accessors)
        IF @last IN (N'Id', N'Value', N'Alias') AND CHARINDEX(N'.', @path) = LEN(@first) + 1
        BEGIN
            SELECT TOP 1 @sid = c._structure_id, @db_type = c.db_type,
                         @is_array = CASE WHEN c._collection_type IS NOT NULL THEN 1 ELSE 0 END
              FROM dbo._scheme_metadata_cache c
             WHERE c._scheme_id = @scheme_id AND c._name = @first AND c._parent_structure_id IS NULL;
            IF @sid IS NOT NULL AND @db_type = N'ListItem'
            BEGIN
                SET @list_prop = @last;
                RETURN N'{"kind":"field","sid":' + CAST(@sid AS NVARCHAR(40))
                     + N',"db_type":"ListItem","db_column":"_ListItem"'
                     + N',"name":' + dbo.pvt_json_string_or_null(@path)
                     + N',"is_array":' + CASE WHEN @is_array = 1 THEN N'true' ELSE N'false' END
                     + N',"list_item_prop":' + dbo.pvt_json_string_or_null(@list_prop)
                     + N',"dict_key":null,"parent_sid":null}';
            END;
        END;

        -- 2c. Generic nested walk
        DECLARE @cur_sid BIGINT, @segment NVARCHAR(200), @rem NVARCHAR(400), @cut INT;
        SET @rem = @path;
        SET @cut = CHARINDEX(N'.', @rem);
        SET @segment = LEFT(@rem, @cut - 1);
        -- Strip trailing "[]" so element-of-struct-array paths like
        -- "Contacts[].Type" walk into the nested "Type" field of the
        -- Contacts struct-array element.
        IF RIGHT(@segment, 2) = N'[]' SET @segment = LEFT(@segment, LEN(@segment) - 2);
        SELECT TOP 1 @cur_sid = c._structure_id
          FROM dbo._scheme_metadata_cache c
         WHERE c._scheme_id = @scheme_id AND c._name = @segment AND c._parent_structure_id IS NULL;
        IF @cur_sid IS NULL
            RETURN NULL;
        SET @rem = SUBSTRING(@rem, @cut + 1, 400);
        WHILE @rem IS NOT NULL AND @rem <> N''
        BEGIN
            SET @cut = CHARINDEX(N'.', @rem);
            IF @cut = 0
            BEGIN
                SET @segment = @rem;
                SET @rem = N'';
            END
            ELSE
            BEGIN
                SET @segment = LEFT(@rem, @cut - 1);
                SET @rem = SUBSTRING(@rem, @cut + 1, 400);
            END;
            IF RIGHT(@segment, 2) = N'[]' SET @segment = LEFT(@segment, LEN(@segment) - 2);
            SELECT TOP 1 @cur_sid = c._structure_id
              FROM dbo._scheme_metadata_cache c
             WHERE c._scheme_id = @scheme_id AND c._name = @segment AND c._parent_structure_id = @cur_sid;
            IF @cur_sid IS NULL
                RETURN NULL;
        END;

        SELECT TOP 1 @db_type = c.db_type,
                     @is_array = CASE WHEN c._collection_type IS NOT NULL THEN 1 ELSE 0 END
          FROM dbo._scheme_metadata_cache c
         WHERE c._structure_id = @cur_sid;
        SET @db_col = dbo.pvt_db_type_to_value_column(@db_type);
        RETURN N'{"kind":"field","sid":' + CAST(@cur_sid AS NVARCHAR(40))
             + N',"db_type":' + dbo.pvt_json_string_or_null(@db_type)
             + N',"db_column":' + dbo.pvt_json_string_or_null(@db_col)
             + N',"name":' + dbo.pvt_json_string_or_null(@path)
             + N',"is_array":' + CASE WHEN @is_array = 1 THEN N'true' ELSE N'false' END
             + N',"list_item_prop":null,"dict_key":null,"parent_sid":null}';
    END;

    -- 3. Bare root field (possibly Foo[] for arrays)
    DECLARE @base_name NVARCHAR(200) = @path;
    DECLARE @force_array BIT = 0;
    IF RIGHT(@path, 2) = N'[]'
    BEGIN
        SET @base_name = LEFT(@path, LEN(@path) - 2);
        SET @force_array = 1;
    END;

    SELECT TOP 1 @sid = c._structure_id, @db_type = c.db_type,
                 @is_array = CASE WHEN c._collection_type IS NOT NULL THEN 1 ELSE 0 END
      FROM dbo._scheme_metadata_cache c
     WHERE c._scheme_id = @scheme_id AND c._name = @base_name AND c._parent_structure_id IS NULL;
    IF @sid IS NULL
        RETURN NULL;
    IF @force_array = 1
        SET @is_array = 1;

    IF @db_type = N'ListItem'
    BEGIN
        SET @db_col = N'_ListItem';
        SET @list_prop = N'Id';
    END
    ELSE
        SET @db_col = dbo.pvt_db_type_to_value_column(@db_type);

    RETURN N'{"kind":"field","sid":' + CAST(@sid AS NVARCHAR(40))
         + N',"db_type":' + dbo.pvt_json_string_or_null(@db_type)
         + N',"db_column":' + dbo.pvt_json_string_or_null(@db_col)
         + N',"name":' + dbo.pvt_json_string_or_null(@path)
         + N',"is_array":' + CASE WHEN @is_array = 1 THEN N'true' ELSE N'false' END
         + N',"list_item_prop":' + dbo.pvt_json_string_or_null(@list_prop)
         + N',"dict_key":null,"parent_sid":null}';
END;
GO

-- ---------- pvt_extract_field_pairs -----------------------------------
-- Walks a filter JSON recursively and returns JSON array of {path, op_value}.
-- Logical ops ($and/$or/$not) are descended into; their child arrays/objects
-- contribute pairs but the operator keys themselves never appear as paths.
CREATE OR ALTER FUNCTION dbo.pvt_extract_field_pairs(@filter NVARCHAR(MAX))
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @filter IS NULL OR ISJSON(@filter) = 0
        RETURN N'[]';

    DECLARE @out NVARCHAR(MAX) = N'[';
    DECLARE @first BIT = 1;

    DECLARE c_keys CURSOR LOCAL FAST_FORWARD FOR
        SELECT [key], [value], [type] FROM OPENJSON(@filter);
    DECLARE @k NVARCHAR(400), @v NVARCHAR(MAX), @t INT;
    OPEN c_keys;
    FETCH NEXT FROM c_keys INTO @k, @v, @t;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF LEFT(@k, 1) = N'$'
        BEGIN
            -- Logical operator: descend
            IF @t = 4  -- array
            BEGIN
                DECLARE @elem NVARCHAR(MAX);
                DECLARE c_arr CURSOR LOCAL FAST_FORWARD FOR
                    SELECT [value] FROM OPENJSON(@v);
                OPEN c_arr;
                FETCH NEXT FROM c_arr INTO @elem;
                WHILE @@FETCH_STATUS = 0
                BEGIN
                    DECLARE @sub NVARCHAR(MAX) = dbo.pvt_extract_field_pairs(@elem);
                    IF @sub <> N'[]' AND LEN(@sub) > 2
                    BEGIN
                        IF @first = 0 SET @out = @out + N',';
                        SET @out = @out + SUBSTRING(@sub, 2, LEN(@sub) - 2);
                        SET @first = 0;
                    END;
                    FETCH NEXT FROM c_arr INTO @elem;
                END;
                CLOSE c_arr; DEALLOCATE c_arr;
            END
            ELSE IF @t = 5  -- object: descend into it
            BEGIN
                DECLARE @sub2 NVARCHAR(MAX) = dbo.pvt_extract_field_pairs(@v);
                IF @sub2 <> N'[]' AND LEN(@sub2) > 2
                BEGIN
                    IF @first = 0 SET @out = @out + N',';
                    SET @out = @out + SUBSTRING(@sub2, 2, LEN(@sub2) - 2);
                    SET @first = 0;
                END;
            END
            ELSE IF LOWER(@k) = N'$field' AND @t = 1
            BEGIN
                -- B2-expr $field: the string value is the field path to collect
                IF @first = 0 SET @out = @out + N',';
                SET @out = @out
                         + N'{"path":' + dbo.pvt_json_string_or_null(@v)
                         + N',"op_value":null}';
                SET @first = 0;
            END
            ELSE IF LOWER(@k) = N'$case' AND @t = 4
            BEGIN
                -- $case arms: descend into each {when, then, else} object
                DECLARE @ca_arm NVARCHAR(MAX);
                DECLARE c_ca2 CURSOR LOCAL FAST_FORWARD FOR
                    SELECT [value] FROM OPENJSON(@v);
                OPEN c_ca2;
                FETCH NEXT FROM c_ca2 INTO @ca_arm;
                WHILE @@FETCH_STATUS = 0
                BEGIN
                    DECLARE @ca_sub NVARCHAR(MAX);
                    -- descend into 'when', 'then', 'else' branches
                    DECLARE @ca_w NVARCHAR(MAX) = JSON_QUERY(@ca_arm, N'$.when');
                    DECLARE @ca_t NVARCHAR(MAX) = JSON_QUERY(@ca_arm, N'$.then');
                    DECLARE @ca_e NVARCHAR(MAX) = JSON_QUERY(@ca_arm, N'$.else');
                    IF @ca_w IS NOT NULL
                    BEGIN
                        SET @ca_sub = dbo.pvt_extract_field_pairs(@ca_w);
                        IF @ca_sub <> N'[]' AND LEN(@ca_sub) > 2
                        BEGIN
                            IF @first = 0 SET @out = @out + N',';
                            SET @out = @out + SUBSTRING(@ca_sub, 2, LEN(@ca_sub) - 2);
                            SET @first = 0;
                        END;
                    END;
                    IF @ca_t IS NOT NULL
                    BEGIN
                        SET @ca_sub = dbo.pvt_extract_field_pairs(@ca_t);
                        IF @ca_sub <> N'[]' AND LEN(@ca_sub) > 2
                        BEGIN
                            IF @first = 0 SET @out = @out + N',';
                            SET @out = @out + SUBSTRING(@ca_sub, 2, LEN(@ca_sub) - 2);
                            SET @first = 0;
                        END;
                    END;
                    IF @ca_e IS NOT NULL
                    BEGIN
                        SET @ca_sub = dbo.pvt_extract_field_pairs(@ca_e);
                        IF @ca_sub <> N'[]' AND LEN(@ca_sub) > 2
                        BEGIN
                            IF @first = 0 SET @out = @out + N',';
                            SET @out = @out + SUBSTRING(@ca_sub, 2, LEN(@ca_sub) - 2);
                            SET @first = 0;
                        END;
                    END;
                    FETCH NEXT FROM c_ca2 INTO @ca_arm;
                END;
                CLOSE c_ca2; DEALLOCATE c_ca2;
            END;
        END
        ELSE
        BEGIN
            -- Field reference; strip property-function suffixes so the base name
            -- is used for metadata resolution (.$length, .$count, [].$count).
            DECLARE @raw_path NVARCHAR(400) = @k;
            DECLARE @field_path NVARCHAR(400) = @k;
            IF RIGHT(@field_path, 9) = N'[].$count'
                SET @field_path = LEFT(@field_path, LEN(@field_path) - 9);
            ELSE IF RIGHT(@field_path, 8) = N'.$length'
                SET @field_path = LEFT(@field_path, LEN(@field_path) - 8);
            ELSE IF RIGHT(@field_path, 7) = N'.$count'
                SET @field_path = LEFT(@field_path, LEN(@field_path) - 7);
            DECLARE @opv NVARCHAR(MAX) = NULL;
            IF RIGHT(@raw_path, 12) = N'.ContainsKey'
                SET @opv = dbo.pvt_peek_contains_key_value(@v);
            IF @first = 0 SET @out = @out + N',';
            SET @out = @out
                     + N'{"path":' + dbo.pvt_json_string_or_null(@field_path)
                     + N',"op_value":' + dbo.pvt_json_string_or_null(@opv) + N'}';
            SET @first = 0;
        END;
        FETCH NEXT FROM c_keys INTO @k, @v, @t;
    END;
    CLOSE c_keys; DEALLOCATE c_keys;

    SET @out = @out + N']';
    RETURN @out;
END;
GO

-- ---------- pvt_collect_fields ----------------------------------------
-- Resolves metadata for every field referenced in filter/order JSON.
-- Returns JSON object {field_name: <FieldInfo>}.
-- ContainsKey-rewritten entries carry "was_contains_key": true.
CREATE OR ALTER FUNCTION dbo.pvt_collect_fields(
    @scheme_id BIGINT,
    @filter    NVARCHAR(MAX),
    @order     NVARCHAR(MAX)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @scheme_id IS NULL
        RETURN N'{}';

    DECLARE @seen TABLE (name NVARCHAR(400) PRIMARY KEY, meta NVARCHAR(MAX) NOT NULL);

    -- ---- From filter (via extract_field_pairs) ----
    DECLARE @pairs NVARCHAR(MAX) = dbo.pvt_extract_field_pairs(@filter);
    IF @pairs IS NOT NULL AND @pairs <> N'[]'
    BEGIN
        DECLARE c_p CURSOR LOCAL FAST_FORWARD FOR
            SELECT JSON_VALUE([value], '$.path'), JSON_VALUE([value], '$.op_value')
              FROM OPENJSON(@pairs);
        DECLARE @path NVARCHAR(400), @opv NVARCHAR(MAX);
        OPEN c_p;
        FETCH NEXT FROM c_p INTO @path, @opv;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            DECLARE @norm NVARCHAR(400) = dbo.pvt_normalize_field_name(@path, @opv);
            IF NOT EXISTS (SELECT 1 FROM @seen WHERE name = @norm)
            BEGIN
                DECLARE @meta NVARCHAR(MAX) = dbo.pvt_resolve_field_path(@scheme_id, @norm);
                IF @meta IS NOT NULL
                BEGIN
                    IF @norm <> @path
                        SET @meta = LEFT(@meta, LEN(@meta) - 1) + N',"was_contains_key":true}';
                    INSERT INTO @seen(name, meta) VALUES (@norm, @meta);
                END;
            END;
            FETCH NEXT FROM c_p INTO @path, @opv;
        END;
        CLOSE c_p; DEALLOCATE c_p;
    END;

    -- ---- From order (plain {field|field_path}; also walks $expr nodes) ----
    IF @order IS NOT NULL AND ISJSON(@order) = 1
    BEGIN
        DECLARE c_o CURSOR LOCAL FAST_FORWARD FOR
            SELECT COALESCE(JSON_VALUE([value], '$.field'), JSON_VALUE([value], '$.field_path')),
                   JSON_QUERY([value], '$."$expr"')
              FROM OPENJSON(@order);
        DECLARE @ofp NVARCHAR(400), @oexpr NVARCHAR(MAX);
        OPEN c_o;
        FETCH NEXT FROM c_o INTO @ofp, @oexpr;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            IF @ofp IS NOT NULL AND NOT EXISTS (SELECT 1 FROM @seen WHERE name = @ofp)
            BEGIN
                DECLARE @ometa NVARCHAR(MAX) = dbo.pvt_resolve_field_path(@scheme_id, @ofp);
                IF @ometa IS NOT NULL
                    INSERT INTO @seen(name, meta) VALUES (@ofp, @ometa);
            END;
            IF @oexpr IS NOT NULL
            BEGIN
                DECLARE @oepairs NVARCHAR(MAX) = dbo.pvt_extract_field_pairs(@oexpr);
                IF @oepairs IS NOT NULL AND @oepairs <> N'[]'
                BEGIN
                    DECLARE c_oe CURSOR LOCAL FAST_FORWARD FOR
                        SELECT JSON_VALUE([value], '$.path'), JSON_VALUE([value], '$.op_value')
                          FROM OPENJSON(@oepairs);
                    DECLARE @oepath NVARCHAR(400), @oeopv NVARCHAR(MAX);
                    OPEN c_oe;
                    FETCH NEXT FROM c_oe INTO @oepath, @oeopv;
                    WHILE @@FETCH_STATUS = 0
                    BEGIN
                        DECLARE @oenorm NVARCHAR(400) = dbo.pvt_normalize_field_name(@oepath, @oeopv);
                        IF NOT EXISTS (SELECT 1 FROM @seen WHERE name = @oenorm)
                        BEGIN
                            DECLARE @oemeta NVARCHAR(MAX) = dbo.pvt_resolve_field_path(@scheme_id, @oenorm);
                            IF @oemeta IS NOT NULL
                            BEGIN
                                IF @oenorm <> @oepath
                                    SET @oemeta = LEFT(@oemeta, LEN(@oemeta) - 1) + N',"was_contains_key":true}';
                                INSERT INTO @seen(name, meta) VALUES (@oenorm, @oemeta);
                            END;
                        END;
                        FETCH NEXT FROM c_oe INTO @oepath, @oeopv;
                    END;
                    CLOSE c_oe; DEALLOCATE c_oe;
                END;
            END;
            FETCH NEXT FROM c_o INTO @ofp, @oexpr;
        END;
        CLOSE c_o; DEALLOCATE c_o;
    END;

    DECLARE @out NVARCHAR(MAX) = N'{';
    DECLARE @first BIT = 1;
    DECLARE c_s CURSOR LOCAL FAST_FORWARD FOR SELECT name, meta FROM @seen ORDER BY name;
    DECLARE @n NVARCHAR(400), @m NVARCHAR(MAX);
    OPEN c_s;
    FETCH NEXT FROM c_s INTO @n, @m;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @first = 0 SET @out = @out + N',';
        SET @out = @out + dbo.pvt_json_string_or_null(@n) + N':' + @m;
        SET @first = 0;
        FETCH NEXT FROM c_s INTO @n, @m;
    END;
    CLOSE c_s; DEALLOCATE c_s;
    SET @out = @out + N'}';
    RETURN @out;
END;
GO

-- ---------- pvt_has_absence_check -------------------------------------
-- Strict subset of "has null check": returns 1 only for predicates that
-- require detecting absent _values rows ($null/$isNull/$exists/{$eq:null}).
-- $notNull / {$ne:null} are NOT flagged (INNER JOIN already drops absent).
CREATE OR ALTER FUNCTION dbo.pvt_has_absence_check(@filter NVARCHAR(MAX))
RETURNS BIT
AS
BEGIN
    IF @filter IS NULL OR ISJSON(@filter) = 0
        RETURN 0;

    DECLARE c CURSOR LOCAL FAST_FORWARD FOR
        SELECT [key], [value], [type] FROM OPENJSON(@filter);
    DECLARE @k NVARCHAR(400), @v NVARCHAR(MAX), @t INT;
    DECLARE @hit BIT = 0;
    OPEN c;
    FETCH NEXT FROM c INTO @k, @v, @t;
    WHILE @@FETCH_STATUS = 0 AND @hit = 0
    BEGIN
        IF LEFT(@k, 1) = N'$'
        BEGIN
            IF LOWER(@k) IN (N'$null', N'$isnull', N'$exists')
                SET @hit = 1;
            ELSE IF @t = 4  -- array
            BEGIN
                DECLARE c_a CURSOR LOCAL FAST_FORWARD FOR SELECT [value] FROM OPENJSON(@v);
                DECLARE @e NVARCHAR(MAX);
                OPEN c_a;
                FETCH NEXT FROM c_a INTO @e;
                WHILE @@FETCH_STATUS = 0 AND @hit = 0
                BEGIN
                    IF dbo.pvt_has_absence_check(@e) = 1 SET @hit = 1;
                    FETCH NEXT FROM c_a INTO @e;
                END;
                CLOSE c_a; DEALLOCATE c_a;
            END
            ELSE IF @t = 5  -- object
                IF dbo.pvt_has_absence_check(@v) = 1 SET @hit = 1;
        END
        ELSE
        BEGIN
            IF @t = 5  -- object operand
            BEGIN
                DECLARE c_op CURSOR LOCAL FAST_FORWARD FOR
                    SELECT [key], [value], [type] FROM OPENJSON(@v);
                DECLARE @sk NVARCHAR(400), @sv NVARCHAR(MAX), @st INT;
                OPEN c_op;
                FETCH NEXT FROM c_op INTO @sk, @sv, @st;
                WHILE @@FETCH_STATUS = 0 AND @hit = 0
                BEGIN
                    IF LOWER(@sk) IN (N'$null', N'$isnull', N'$exists')
                        SET @hit = 1;
                    ELSE IF LOWER(@sk) = N'$eq' AND @st = 0  -- null literal
                        SET @hit = 1;
                    FETCH NEXT FROM c_op INTO @sk, @sv, @st;
                END;
                CLOSE c_op; DEALLOCATE c_op;
            END
            ELSE IF @t = 0  -- null literal directly
                SET @hit = 1;
        END;
        FETCH NEXT FROM c INTO @k, @v, @t;
    END;
    CLOSE c; DEALLOCATE c;
    RETURN @hit;
END;
GO
