-- =====================================================================
-- 24_pvt_projection.sql  (MSSql v2-pvt)
-- ---------------------------------------------------------------------
-- Projection orchestrator. Returns a complete T-SQL SELECT statement
-- for a given projection spec (arbitrary columns from redb scheme).
--
--   pvt_build_projection_sql(
--       @scheme_id   BIGINT,
--       @filter      NVARCHAR(MAX),    -- optional PVT filter JSON
--       @limit       INT,              -- optional paging
--       @offset      INT,
--       @projection  NVARCHAR(MAX),    -- JSON array of string | {field,alias?}
--       @source_mode NVARCHAR(50)      -- 'flat' (others return NULL)
--   ) RETURNS NVARCHAR(MAX)
--
-- Projection entry shapes:
--   "FieldName"                      -- simple string
--   {"field": "FieldName"}           -- object with 'field' key
--   {"field": "FieldName", "alias": "MyCol"} -- aliased
--   {"field_path": "FieldName"}      -- alternative key
--
-- Output shapes:
--   Shape A (all projected fields are base _objects columns):
--       SELECT <proj_cols> FROM dbo._objects o
--        WHERE o.[_id_scheme] = X [AND <where>] [OFFSET/FETCH]
--
--   Shape C (any projected field is a props field):
--       SELECT <proj_cols>
--         FROM (<pvt_build_cte_sql>) _pvt_cte
--        WHERE <where> [ORDER BY (SELECT 1) OFFSET/FETCH]
--
-- Depends on:
--   dbo.pvt_collect_fields      (10_pvt_field_collection.sql)
--   dbo.pvt_resolve_field_path  (10_pvt_field_collection.sql)
--   dbo.pvt_build_where_from_json (14_pvt_where.sql)
--   dbo.pvt_build_cte_sql       (12_pvt_cte_builder.sql)
-- =====================================================================

SET QUOTED_IDENTIFIER ON;
GO

-- ---------- pvt_collect_extra_fields ----------------------------------
-- Resolve metadata for a flat JSON array of field paths and merge the
-- results into an existing fields-map JSON. Paths that are absent from
-- the scheme metadata are silently skipped.
CREATE OR ALTER FUNCTION dbo.pvt_collect_extra_fields(
    @scheme_id   BIGINT,
    @paths_json  NVARCHAR(MAX)     -- JSON array of strings
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @scheme_id IS NULL RETURN N'{}';
    IF @paths_json IS NULL OR ISJSON(@paths_json) = 0 RETURN N'{}';

    DECLARE @out NVARCHAR(MAX) = N'{}';

    DECLARE c CURSOR LOCAL FAST_FORWARD FOR
        SELECT [value] FROM OPENJSON(@paths_json) WHERE [type] = 1;  -- strings only
    DECLARE @path NVARCHAR(400);
    OPEN c;
    FETCH NEXT FROM c INTO @path;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @path IS NOT NULL AND @path <> N''
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
        FETCH NEXT FROM c INTO @path;
    END;
    CLOSE c; DEALLOCATE c;
    RETURN @out;
END;
GO

-- ---------- pvt_build_projection_sql ----------------------------------
CREATE OR ALTER FUNCTION dbo.pvt_build_projection_sql(
    @scheme_id   BIGINT,
    @filter      NVARCHAR(MAX),
    @limit       INT,
    @offset      INT,
    @projection  NVARCHAR(MAX),
    @source_mode NVARCHAR(50)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @scheme_id IS NULL
        RETURN NULL;
    IF @projection IS NULL OR ISJSON(@projection) = 0
        RETURN NULL;
    IF @source_mode IS NULL SET @source_mode = N'flat';
    IF @source_mode <> N'flat'
        RETURN NULL;

    -- Collect fields from @filter (may return '{}' when filter is NULL)
    DECLARE @fields NVARCHAR(MAX) = dbo.pvt_collect_fields(@scheme_id, @filter, NULL);
    IF @fields IS NULL OR ISJSON(@fields) = 0 SET @fields = N'{}';

    -- ---------- Parse projection entries -----------------------------------------
    -- Build two SELECT lists in parallel:
    --   @proj_c  — for Shape C (_pvt_cte.col references)
    --   @proj_a  — for Shape A (o.[col] references, valid only when kind='base')
    -- Also track @has_props to choose the final shape.
    DECLARE @proj_c    NVARCHAR(MAX) = N'';
    DECLARE @proj_a    NVARCHAR(MAX) = N'';
    DECLARE @has_props BIT = 0;

    DECLARE c_p CURSOR LOCAL FAST_FORWARD FOR
        SELECT [value], [type] FROM OPENJSON(@projection);
    DECLARE @pentry NVARCHAR(MAX), @ptype INT;
    OPEN c_p;
    FETCH NEXT FROM c_p INTO @pentry, @ptype;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        DECLARE @fpath  NVARCHAR(400) = NULL;
        DECLARE @falias NVARCHAR(200) = NULL;

        IF @ptype = 1       -- plain string entry: "FieldName"
        BEGIN
            SET @fpath  = @pentry;
            SET @falias = @pentry;
        END
        ELSE IF @ptype = 5  -- object entry: {"field":...,"alias":...}
        BEGIN
            SET @fpath = ISNULL(
                JSON_VALUE(@pentry, N'$.field'),
                JSON_VALUE(@pentry, N'$.field_path'));
            SET @falias = ISNULL(JSON_VALUE(@pentry, N'$.alias'), @fpath);
        END;

        IF @fpath IS NOT NULL AND @fpath <> N''
        BEGIN
            -- Resolve metadata for this field
            DECLARE @fm NVARCHAR(MAX) = dbo.pvt_resolve_field_path(@scheme_id, @fpath);
            IF @fm IS NOT NULL
            BEGIN
                -- Merge into @fields when not already present
                IF JSON_QUERY(@fields, N'$."' + STRING_ESCAPE(@fpath, 'json') + N'"') IS NULL
                BEGIN
                    IF @fields = N'{}'
                        SET @fields = N'{"' + STRING_ESCAPE(@fpath, 'json') + N'":' + @fm + N'}';
                    ELSE
                        SET @fields = LEFT(@fields, LEN(@fields) - 1)
                                    + N',"' + STRING_ESCAPE(@fpath, 'json') + N'":' + @fm + N'}';
                END;

                DECLARE @fkind NVARCHAR(50) = JSON_VALUE(@fm, N'$.kind');
                IF @fkind <> N'base'
                    SET @has_props = 1;

                -- Build SELECT expressions
                DECLARE @alias_q  NVARCHAR(200) = QUOTENAME(@falias);
                DECLARE @expr_c   NVARCHAR(MAX);
                DECLARE @expr_a   NVARCHAR(MAX) = NULL;

                IF @fkind = N'base'
                BEGIN
                    DECLARE @bcol NVARCHAR(100) = JSON_VALUE(@fm, N'$.column');
                    SET @expr_c = N'_pvt_cte.[' + @bcol + N'] AS ' + @alias_q;
                    SET @expr_a = N'o.[' + @bcol + N'] AS ' + @alias_q;
                END
                ELSE
                BEGIN
                    -- Props field: the CTE names the column after @fpath
                    SET @expr_c = N'_pvt_cte.' + QUOTENAME(@fpath) + N' AS ' + @alias_q;
                    -- @expr_a stays NULL; Shape A is invalid for props fields
                END;

                IF @proj_c <> N'' SET @proj_c += N', ';
                SET @proj_c += @expr_c;

                IF @expr_a IS NOT NULL
                BEGIN
                    IF @proj_a <> N'' SET @proj_a += N', ';
                    SET @proj_a += @expr_a;
                END;
            END;
        END;

        FETCH NEXT FROM c_p INTO @pentry, @ptype;
    END;
    CLOSE c_p; DEALLOCATE c_p;

    IF @proj_c = N''
        RETURN NULL;

    -- Paging clause
    DECLARE @paging NVARCHAR(MAX) = N'';
    IF @limit IS NOT NULL AND @limit >= 0
    BEGIN
        DECLARE @off INT = ISNULL(@offset, 0);
        SET @paging = CHAR(10)
                    + N'ORDER BY (SELECT 1)'
                    + N' OFFSET ' + CAST(@off AS NVARCHAR(10)) + N' ROWS'
                    + N' FETCH NEXT ' + CAST(@limit AS NVARCHAR(10)) + N' ROWS ONLY';
    END;

    -- ---- Shape A: all projected fields are base _objects columns ----
    IF @has_props = 0 AND @proj_a <> N''
    BEGIN
        DECLARE @wa NVARCHAR(MAX) = dbo.pvt_build_where_from_json(@filter, @fields, N'o.');
        RETURN N'SELECT ' + @proj_a + CHAR(10)
             + N'FROM dbo._objects o' + CHAR(10)
             + N'WHERE o.[_id_scheme] = ' + CAST(@scheme_id AS NVARCHAR(20))
             + CASE WHEN @wa <> N'1=1' THEN N' AND ' + @wa ELSE N'' END
             + @paging;
    END;

    -- ---- Shape C: at least one props field (wide pivot CTE) ----------
    DECLARE @inner  NVARCHAR(MAX) = dbo.pvt_build_cte_sql(
        @scheme_id, @fields, N'flat', NULL, NULL, 1, NULL, 0, DEFAULT, DEFAULT, DEFAULT);
    IF @inner IS NULL
        RETURN NULL;
    DECLARE @wc NVARCHAR(MAX) = dbo.pvt_build_where_from_json(@filter, @fields, N'_pvt_cte.');
    RETURN N'SELECT ' + @proj_c + CHAR(10)
         + N'FROM (' + CHAR(10) + @inner + CHAR(10) + N') _pvt_cte' + CHAR(10)
         + N'WHERE ' + @wc
         + @paging;
END;
GO
