-- =====================================================================
-- 06a_pvt_legacy_helpers.sql  (MSSql v2-pvt)
-- ---------------------------------------------------------------------
-- Level-condition helpers required by pvt_build_hierarchical_conditions.
-- Mirrors pvt_build_level_condition / pvt_build_level_condition_with_operators
-- from PG v2-pvt/06a_pvt_legacy_helpers.sql.
--
-- Functions:
--   dbo.pvt_build_level_condition(@target_level INT, @table_alias NVARCHAR(50))
--       -> NVARCHAR(MAX)  SQL fragment: ' AND dbo.pvt_object_depth(alias.[_id]) = N'
--   dbo.pvt_build_level_condition_with_operators(@level_operators NVARCHAR(MAX),
--                                                @table_alias     NVARCHAR(50))
--       -> NVARCHAR(MAX)  SQL fragment: ' AND (depth op N [AND ...])'
--
-- Depends on: dbo.pvt_object_depth (04_pvt_tree_helpers.sql)
-- =====================================================================

SET QUOTED_IDENTIFIER ON;
GO

-- ---------- pvt_build_level_condition ---------------------------------
-- Returns a T-SQL predicate fragment for exact tree-depth equality.
-- Level 0 = root (no parent). Level 1 = direct child of root, etc.
CREATE OR ALTER FUNCTION dbo.pvt_build_level_condition(
    @target_level INT,
    @table_alias  NVARCHAR(50)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    DECLARE @alias NVARCHAR(60) = ISNULL(@table_alias, N'o');
    RETURN N' AND dbo.pvt_object_depth(' + @alias + N'.[_id]) = '
         + CAST(@target_level AS NVARCHAR(10));
END;
GO

-- ---------- pvt_build_level_condition_with_operators ------------------
-- Parses a JSON object of comparison operators ({"$gt":2,"$lt":5})
-- and builds an AND-joined depth predicate fragment.
-- Returns empty string when no recognized operators are found.
CREATE OR ALTER FUNCTION dbo.pvt_build_level_condition_with_operators(
    @level_operators NVARCHAR(MAX),
    @table_alias     NVARCHAR(50)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @level_operators IS NULL OR ISJSON(@level_operators) = 0
        RETURN N'';

    DECLARE @alias NVARCHAR(60) = ISNULL(@table_alias, N'o');
    DECLARE @depth_expr NVARCHAR(200) = N'dbo.pvt_object_depth(' + @alias + N'.[_id])';
    DECLARE @parts NVARCHAR(MAX) = N'';

    DECLARE c CURSOR LOCAL FAST_FORWARD FOR
        SELECT [key], [value] FROM OPENJSON(@level_operators);
    DECLARE @opk NVARCHAR(20), @opv NVARCHAR(50);
    OPEN c;
    FETCH NEXT FROM c INTO @opk, @opv;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        DECLARE @sym NVARCHAR(4) = CASE @opk
            WHEN N'$gt'  THEN N'>'
            WHEN N'$gte' THEN N'>='
            WHEN N'$lt'  THEN N'<'
            WHEN N'$lte' THEN N'<='
            WHEN N'$eq'  THEN N'='
            WHEN N'$ne'  THEN N'<>'
            ELSE NULL
        END;
        IF @sym IS NOT NULL AND TRY_CAST(@opv AS INT) IS NOT NULL
        BEGIN
            IF @parts <> N'' SET @parts += N' AND ';
            SET @parts += @depth_expr + N' ' + @sym + N' ' + CAST(CAST(@opv AS INT) AS NVARCHAR(10));
        END;
        FETCH NEXT FROM c INTO @opk, @opv;
    END;
    CLOSE c; DEALLOCATE c;

    IF @parts = N'' RETURN N'';
    RETURN N' AND (' + @parts + N')';
END;
GO
