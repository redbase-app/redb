-- =====================================================================
-- 15_pvt_order.sql  (MSSql v2-pvt) — ORDER BY builder
-- ---------------------------------------------------------------------
-- Supports {field|field_path,dir,nulls} and {"$expr":{...},"dir",...}
-- entries. $expr delegates to pvt_b2_expr_sql. No DISTINCT ON.
-- Returns either '' or '\nORDER BY <cols>'. T-SQL has no `NULLS FIRST/LAST`
-- syntax; emulate via CASE WHEN ... IS NULL THEN 0 ELSE 1 END prefix.
-- =====================================================================

SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER FUNCTION dbo.pvt_build_order_conditions(
    @order        NVARCHAR(MAX),
    @fields       NVARCHAR(MAX),
    @base_prefix  NVARCHAR(10)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @order IS NULL OR ISJSON(@order) = 0
        RETURN N'';

    DECLARE @parts NVARCHAR(MAX) = N'';
    DECLARE @cnt INT = 0;

    DECLARE c CURSOR LOCAL FAST_FORWARD FOR SELECT [value] FROM OPENJSON(@order);
    DECLARE @e NVARCHAR(MAX);
    OPEN c;
    FETCH NEXT FROM c INTO @e;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        DECLARE @fld   NVARCHAR(400) = COALESCE(JSON_VALUE(@e, '$.field'), JSON_VALUE(@e, '$.field_path'));
        DECLARE @dir   NVARCHAR(10)  = LOWER(COALESCE(JSON_VALUE(@e, '$.dir'), JSON_VALUE(@e, '$.direction'), N'asc'));
        DECLARE @nulls NVARCHAR(10)  = LOWER(COALESCE(JSON_VALUE(@e, '$.nulls'), N''));
        IF @dir NOT IN (N'asc', N'desc') SET @dir = N'asc';

        DECLARE @expr_node NVARCHAR(MAX) = JSON_QUERY(@e, N'$."$expr"');
        IF @expr_node IS NOT NULL
        BEGIN
            -- $expr ORDER BY: delegate to pvt_b2_expr_sql.
            -- pvt_b2_expr_sql expects @obj_alias WITHOUT trailing dot
            -- (matches pvt_build_where_from_json convention).
            DECLARE @b2_alias NVARCHAR(50) = CASE WHEN @base_prefix = N'o.' THEN N'o' ELSE N'_pvt_cte' END;
            DECLARE @eprt NVARCHAR(MAX) = dbo.pvt_b2_expr_sql(@expr_node, @fields, @b2_alias);
            IF @eprt IS NOT NULL AND @eprt <> N''
            BEGIN
                IF @cnt > 0 SET @parts = @parts + N', ';
                DECLARE @eterm NVARCHAR(MAX) = N'(' + @eprt + N') ' + UPPER(@dir);
                IF @nulls = N'first'
                    SET @parts = @parts + N'CASE WHEN (' + @eprt + N') IS NULL THEN 0 ELSE 1 END, ' + @eterm;
                ELSE IF @nulls = N'last'
                    SET @parts = @parts + N'CASE WHEN (' + @eprt + N') IS NULL THEN 1 ELSE 0 END, ' + @eterm;
                ELSE
                    SET @parts = @parts + @eterm;
                SET @cnt = @cnt + 1;
            END;
        END
        ELSE IF @fld IS NOT NULL AND @fld <> N''
        BEGIN
            DECLARE @meta NVARCHAR(MAX) = JSON_QUERY(@fields, N'$.' + N'"' + STRING_ESCAPE(@fld, 'json') + N'"');
            DECLARE @col NVARCHAR(200);
            IF @meta IS NOT NULL AND JSON_VALUE(@meta, '$.kind') = N'base'
                SET @col = ISNULL(@base_prefix, N'') + QUOTENAME(JSON_VALUE(@meta, '$.column'));
            ELSE
                SET @col = QUOTENAME(@fld);

            IF @cnt > 0 SET @parts = @parts + N', ';
            IF @nulls = N'first'
                SET @parts = @parts + N'CASE WHEN ' + @col + N' IS NULL THEN 0 ELSE 1 END, ';
            ELSE IF @nulls = N'last'
                SET @parts = @parts + N'CASE WHEN ' + @col + N' IS NULL THEN 1 ELSE 0 END, ';
            SET @parts = @parts + @col + N' ' + UPPER(@dir);
            SET @cnt = @cnt + 1;
        END;
        FETCH NEXT FROM c INTO @e;
    END;
    CLOSE c; DEALLOCATE c;

    IF @cnt = 0 RETURN N'';
    RETURN CHAR(10) + N'ORDER BY ' + @parts;
END;
GO
