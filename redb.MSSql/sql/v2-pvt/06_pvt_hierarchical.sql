-- =====================================================================
-- 06_pvt_hierarchical.sql  (MSSql v2-pvt)
-- ---------------------------------------------------------------------
-- Tree predicate builder for $hasAncestor, $hasDescendant, $level,
-- $isRoot, $isLeaf, $childrenOf. Mirrors PG pvt_build_hierarchical_conditions.
--
-- Note: The core WHERE builder (14_pvt_where.sql) handles all these
-- operators inline. This function is a focused variant that extracts
-- only the hierarchical keys from a filter JSON and builds a WHERE
-- fragment — useful when callers want to separate tree conditions from
-- field conditions.
--
-- Functions:
--   dbo.pvt_build_hierarchical_conditions(@facet_filters NVARCHAR(MAX),
--                                          @table_alias  NVARCHAR(50))
--       -> NVARCHAR(MAX)  SQL fragment, leading ' AND ...' or ''
--
-- Depends on:
--   dbo.pvt_object_depth          (04_pvt_tree_helpers.sql)
--   dbo.pvt_is_descendant_of      (04_pvt_tree_helpers.sql)
--   dbo.pvt_build_level_condition (06a_pvt_legacy_helpers.sql)
--   dbo.pvt_build_level_condition_with_operators (06a)
-- =====================================================================

SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER FUNCTION dbo.pvt_build_hierarchical_conditions(
    @facet_filters NVARCHAR(MAX),
    @table_alias   NVARCHAR(50)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @facet_filters IS NULL OR ISJSON(@facet_filters) = 0
        RETURN N'';

    DECLARE @alias     NVARCHAR(60) = ISNULL(@table_alias, N'o');
    DECLARE @result    NVARCHAR(MAX) = N'';

    -- $isRoot
    DECLARE @is_root_raw NVARCHAR(10) = JSON_VALUE(@facet_filters, N'$."$isRoot"');
    IF @is_root_raw IS NOT NULL
    BEGIN
        IF LOWER(@is_root_raw) = N'true'
            SET @result += N' AND ' + @alias + N'.[_id_parent] IS NULL';
        ELSE
            SET @result += N' AND ' + @alias + N'.[_id_parent] IS NOT NULL';
    END;

    -- $isLeaf
    DECLARE @is_leaf_raw NVARCHAR(10) = JSON_VALUE(@facet_filters, N'$."$isLeaf"');
    IF @is_leaf_raw IS NOT NULL
    BEGIN
        IF LOWER(@is_leaf_raw) = N'true'
            SET @result += N' AND NOT EXISTS (SELECT 1 FROM dbo._objects _ch WHERE _ch.[_id_parent] = ' + @alias + N'.[_id])';
        ELSE
            SET @result += N' AND EXISTS (SELECT 1 FROM dbo._objects _ch WHERE _ch.[_id_parent] = ' + @alias + N'.[_id])';
    END;

    -- $level: integer (exact) or object (operators)
    DECLARE @level_raw NVARCHAR(MAX) = JSON_QUERY(@facet_filters, N'$."$level"');
    IF @level_raw IS NOT NULL
    BEGIN
        IF ISJSON(@level_raw) = 1
            -- Operator object: {"$gt":2,"$lt":5}
            SET @result += dbo.pvt_build_level_condition_with_operators(@level_raw, @alias);
        ELSE
        BEGIN
            DECLARE @lvl_val NVARCHAR(20) = JSON_VALUE(@facet_filters, N'$."$level"');
            DECLARE @lvl_int INT = TRY_CAST(@lvl_val AS INT);
            IF @lvl_int IS NOT NULL
                SET @result += dbo.pvt_build_level_condition(@lvl_int, @alias);
        END;
    END;

    -- $hasAncestor: object is a descendant of the given ancestor id
    DECLARE @ha_raw NVARCHAR(40) = JSON_VALUE(@facet_filters, N'$."$hasAncestor"');
    IF @ha_raw IS NOT NULL
    BEGIN
        DECLARE @ha_id BIGINT = TRY_CAST(@ha_raw AS BIGINT);
        IF @ha_id IS NOT NULL
            SET @result += N' AND dbo.pvt_is_descendant_of(' + @alias + N'.[_id], '
                         + CAST(@ha_id AS NVARCHAR(20)) + N') = 1';
    END;

    -- $hasDescendant: given id is a descendant of this object
    DECLARE @hd_raw NVARCHAR(40) = JSON_VALUE(@facet_filters, N'$."$hasDescendant"');
    IF @hd_raw IS NOT NULL
    BEGIN
        DECLARE @hd_id BIGINT = TRY_CAST(@hd_raw AS BIGINT);
        IF @hd_id IS NOT NULL
            SET @result += N' AND dbo.pvt_is_descendant_of('
                         + CAST(@hd_id AS NVARCHAR(20)) + N', ' + @alias + N'.[_id]) = 1';
    END;

    -- $childrenOf: direct children of the given parent id
    DECLARE @co_raw NVARCHAR(40) = JSON_VALUE(@facet_filters, N'$."$childrenOf"');
    IF @co_raw IS NOT NULL
    BEGIN
        DECLARE @co_id BIGINT = TRY_CAST(@co_raw AS BIGINT);
        IF @co_id IS NOT NULL
            SET @result += N' AND ' + @alias + N'.[_id_parent] = ' + CAST(@co_id AS NVARCHAR(20));
    END;

    RETURN @result;
END;
GO
