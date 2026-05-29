-- ======================================================================
-- 08_pvt_tree_functions.sql  (MSSql v2-pvt — Stage 2a)
-- ----------------------------------------------------------------------
-- Multi-statement table-valued functions for the 5 tree-walk modes
-- consumed by pvt_build_cte_sql. PG uses inline recursive CTEs in the
-- generated SQL; T-SQL forbids `WITH` inside a subquery, so we wrap
-- the recursive walk in TVFs and let 12_pvt_cte_builder emit
--
--     o.[_id] IN (SELECT _id FROM dbo.pvt_tree_<mode>(...))
--
-- as an ordinary predicate. This keeps the contract of file 12 (a bare
-- SELECT body) intact and avoids touching the 5 callers.
--
-- Modes:
--   pvt_tree_descendants : recursive walk DOWN from seed ids.
--                          depth=0 at seed; descendants depth>0.
--                          @include_seed=0 strips depth=0 rows.
--                          @max_depth NULL = unbounded.
--                          @polymorphic=0 restricts the recursive
--                          step to o._id_scheme = @scheme_id.
--   pvt_tree_children    : non-recursive — direct children of seed ids.
--   pvt_tree_roots       : non-recursive — scheme objects with
--                          _id_parent IS NULL. Optional seed_ids
--                          restricts to that root subset.
--   pvt_tree_leaves      : non-recursive — scheme objects with no
--                          children (NOT EXISTS child._id_parent = o._id).
--                          Optional seed_ids restricts the leaf subset.
--   pvt_tree_ancestors   : recursive walk UP from seed ids via _id_parent.
--                          Seed depth=1 = direct parent of seed objects.
--                          The seeds themselves are NEVER in the output.
--
-- Seed input: NVARCHAR(MAX) JSON array of bigints, e.g. N'[1,42,77]'.
-- NULL or N'[]' = no seeds (only valid for roots/leaves).
--
-- All functions return TABLE(_id BIGINT PRIMARY KEY, depth INT NULL).
-- ======================================================================
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

-- ---------------------------------------------------------------- DROPS
IF OBJECT_ID('dbo.pvt_tree_descendants', 'TF') IS NOT NULL DROP FUNCTION dbo.pvt_tree_descendants;
GO
IF OBJECT_ID('dbo.pvt_tree_children', 'TF') IS NOT NULL DROP FUNCTION dbo.pvt_tree_children;
GO
IF OBJECT_ID('dbo.pvt_tree_roots', 'TF') IS NOT NULL DROP FUNCTION dbo.pvt_tree_roots;
GO
IF OBJECT_ID('dbo.pvt_tree_leaves', 'TF') IS NOT NULL DROP FUNCTION dbo.pvt_tree_leaves;
GO
IF OBJECT_ID('dbo.pvt_tree_ancestors', 'TF') IS NOT NULL DROP FUNCTION dbo.pvt_tree_ancestors;
GO

-- ====================================================================
-- pvt_tree_descendants
-- ====================================================================
CREATE FUNCTION dbo.pvt_tree_descendants(
    @scheme_id    BIGINT,
    @seed_ids     NVARCHAR(MAX),     -- JSON array of bigints
    @max_depth    INT          = NULL,
    @polymorphic  BIT          = 1,
    @include_seed BIT          = 1
)
RETURNS @T TABLE (_id BIGINT PRIMARY KEY, depth INT NULL)
AS
BEGIN
    IF @seed_ids IS NULL RETURN;

    ;WITH seeds(_id) AS (
        SELECT CAST([value] AS BIGINT) FROM OPENJSON(@seed_ids)
    ),
    walk(_id, depth) AS (
        SELECT s._id, 0 FROM seeds s
        UNION ALL
        SELECT o.[_id], w.depth + 1
        FROM dbo._objects o
        JOIN walk w ON o.[_id_parent] = w._id
        WHERE (@max_depth IS NULL OR w.depth < @max_depth)
          AND (@polymorphic = 1 OR o.[_id_scheme] = @scheme_id)
    )
    INSERT INTO @T(_id, depth)
    SELECT _id, MIN(depth)
    FROM walk
    WHERE @include_seed = 1 OR depth > 0
    GROUP BY _id;

    RETURN;
END;
GO

-- ====================================================================
-- pvt_tree_children
-- ====================================================================
CREATE FUNCTION dbo.pvt_tree_children(
    @scheme_id   BIGINT,
    @seed_ids    NVARCHAR(MAX),
    @polymorphic BIT = 1
)
RETURNS @T TABLE (_id BIGINT PRIMARY KEY, depth INT NULL)
AS
BEGIN
    IF @seed_ids IS NULL RETURN;

    ;WITH seeds(_id) AS (
        SELECT CAST([value] AS BIGINT) FROM OPENJSON(@seed_ids)
    )
    INSERT INTO @T(_id, depth)
    SELECT o.[_id], 1
    FROM dbo._objects o
    JOIN seeds s ON o.[_id_parent] = s._id
    WHERE (@polymorphic = 1 OR o.[_id_scheme] = @scheme_id);

    RETURN;
END;
GO

-- ====================================================================
-- pvt_tree_roots
-- ====================================================================
CREATE FUNCTION dbo.pvt_tree_roots(
    @scheme_id BIGINT,
    @seed_ids  NVARCHAR(MAX)         -- optional; NULL = all roots in scheme
)
RETURNS @T TABLE (_id BIGINT PRIMARY KEY, depth INT NULL)
AS
BEGIN
    IF @seed_ids IS NULL
    BEGIN
        INSERT INTO @T(_id, depth)
        SELECT o.[_id], 0
        FROM dbo._objects o
        WHERE o.[_id_parent] IS NULL
          AND o.[_id_scheme] = @scheme_id;
    END
    ELSE
    BEGIN
        ;WITH seeds(_id) AS (
            SELECT CAST([value] AS BIGINT) FROM OPENJSON(@seed_ids)
        )
        INSERT INTO @T(_id, depth)
        SELECT o.[_id], 0
        FROM dbo._objects o
        JOIN seeds s ON s._id = o.[_id]
        WHERE o.[_id_parent] IS NULL
          AND o.[_id_scheme] = @scheme_id;
    END;

    RETURN;
END;
GO

-- ====================================================================
-- pvt_tree_leaves
-- ====================================================================
CREATE FUNCTION dbo.pvt_tree_leaves(
    @scheme_id BIGINT,
    @seed_ids  NVARCHAR(MAX)         -- optional; NULL = all leaves in scheme
)
RETURNS @T TABLE (_id BIGINT PRIMARY KEY, depth INT NULL)
AS
BEGIN
    IF @seed_ids IS NULL
    BEGIN
        INSERT INTO @T(_id, depth)
        SELECT o.[_id], 0
        FROM dbo._objects o
        WHERE o.[_id_scheme] = @scheme_id
          AND NOT EXISTS (
              SELECT 1 FROM dbo._objects c
              WHERE c.[_id_parent] = o.[_id]
          );
    END
    ELSE
    BEGIN
        ;WITH seeds(_id) AS (
            SELECT CAST([value] AS BIGINT) FROM OPENJSON(@seed_ids)
        )
        INSERT INTO @T(_id, depth)
        SELECT o.[_id], 0
        FROM dbo._objects o
        JOIN seeds s ON s._id = o.[_id]
        WHERE o.[_id_scheme] = @scheme_id
          AND NOT EXISTS (
              SELECT 1 FROM dbo._objects c
              WHERE c.[_id_parent] = o.[_id]
          );
    END;

    RETURN;
END;
GO

-- ====================================================================
-- pvt_tree_ancestors
-- ====================================================================
CREATE FUNCTION dbo.pvt_tree_ancestors(
    @scheme_id   BIGINT,
    @seed_ids    NVARCHAR(MAX),
    @max_depth   INT          = NULL,
    @polymorphic BIT          = 1
)
RETURNS @T TABLE (_id BIGINT PRIMARY KEY, depth INT NULL)
AS
BEGIN
    IF @seed_ids IS NULL RETURN;

    ;WITH seeds(_id) AS (
        SELECT CAST([value] AS BIGINT) FROM OPENJSON(@seed_ids)
    ),
    walk(_id, depth) AS (
        -- Seed: direct parents of the input ids (depth=1).
        SELECT seed.[_id_parent], 1
        FROM dbo._objects seed
        JOIN seeds s ON s._id = seed.[_id]
        WHERE seed.[_id_parent] IS NOT NULL

        UNION ALL

        -- Recursive step: grandparents and so on.
        SELECT o.[_id_parent], w.depth + 1
        FROM dbo._objects o
        JOIN walk w ON o.[_id] = w._id
        WHERE o.[_id_parent] IS NOT NULL
          AND (@max_depth IS NULL OR w.depth < @max_depth)
          AND (@polymorphic = 1 OR o.[_id_scheme] = @scheme_id)
    )
    INSERT INTO @T(_id, depth)
    SELECT _id, MIN(depth) FROM walk GROUP BY _id;

    RETURN;
END;
GO
