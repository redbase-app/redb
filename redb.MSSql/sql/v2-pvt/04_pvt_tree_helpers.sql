-- =====================================================================
-- 04_pvt_tree_helpers.sql  (MSSql v2-pvt) — iterative tree traversal
-- ---------------------------------------------------------------------
-- Functions:
--   dbo.pvt_object_depth(@id)
--       Returns the depth of an object in the tree (root = 0).
--       Uses an iterative WHILE loop (max 200 levels) because scalar
--       UDFs in SQL Server cannot contain recursive CTEs.
--
--   dbo.pvt_is_descendant_of(@id, @ancestor_id)
--       Returns 1 if @id is a direct or indirect descendant of
--       @ancestor_id, 0 otherwise. Also iterative.
--
-- Used by pvt_build_where_from_json to emit $level, $hasAncestor, and
-- $hasDescendant predicates as correlated scalar UDF calls.
-- =====================================================================

SET QUOTED_IDENTIFIER ON;
GO

-- ---------- pvt_object_depth -----------------------------------------
CREATE OR ALTER FUNCTION dbo.pvt_object_depth(@id BIGINT)
RETURNS INT
AS
BEGIN
    DECLARE @depth  INT    = 0;
    DECLARE @cur_id BIGINT = @id;
    DECLARE @par_id BIGINT;
    DECLARE @iter   INT    = 200;

    WHILE @iter > 0
    BEGIN
        SELECT @par_id = [_id_parent]
        FROM   dbo._objects
        WHERE  [_id] = @cur_id;

        IF @par_id IS NULL
            RETURN @depth;

        SET @depth  += 1;
        SET @cur_id  = @par_id;
        SET @iter   -= 1;
    END;

    RETURN @depth;
END;
GO

-- ---------- pvt_is_descendant_of -------------------------------------
-- Returns 1 when @id is a (direct or indirect) descendant of @ancestor_id.
CREATE OR ALTER FUNCTION dbo.pvt_is_descendant_of(
    @id          BIGINT,
    @ancestor_id BIGINT
)
RETURNS BIT
AS
BEGIN
    IF @id IS NULL OR @ancestor_id IS NULL
        RETURN 0;

    DECLARE @cur_id BIGINT = @id;
    DECLARE @par_id BIGINT;
    DECLARE @iter   INT    = 200;

    WHILE @iter > 0
    BEGIN
        SELECT @par_id = [_id_parent]
        FROM   dbo._objects
        WHERE  [_id] = @cur_id;

        IF @par_id IS NULL
            RETURN 0;
        IF @par_id = @ancestor_id
            RETURN 1;

        SET @cur_id = @par_id;
        SET @iter  -= 1;
    END;

    RETURN 0;
END;
GO
