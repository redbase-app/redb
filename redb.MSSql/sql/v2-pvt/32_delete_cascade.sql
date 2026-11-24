-- =====================================================
-- delete_object_cascade / delete_objects_cascade: delete objects with their descendants
-- =====================================================
-- The procedure's row count, which ExecuteAsync returns, is the number of REQUESTED objects this call deleted, as
-- PostgreSQL and SQLite return it: descendants are not counted, and an object another session deletes concurrently is
-- not counted either.
--
-- In one transaction (its own when the caller has none): the requested rows are locked first (UPDLOCK, a seek on the
-- primary key for each), then their descendants are collected by a plain read and everything is deleted bottom-up
-- (_id_parent has no cascade on SQL Server; the INSTEAD OF trigger TR__objects__cascade_values removes the values of
-- each object). A requested object that is locked here is deleted by this call, so the locked rows are the count. A
-- concurrent delete of the same object waits at its own lock and, once this one commits, no longer finds it.
-- Only the requested rows are locked, by key: a lock hint on the walk would hold every row it scans.
--
-- The count used to be the number of ids collected by a plain read, descendants included: two concurrent deletes both
-- collected the object, one deleted it, and both returned 1, so DeleteAsync answered true for an object another session
-- had deleted. Counting the rows of the DELETE itself would not help: the INSTEAD OF trigger receives the rows the
-- statement qualified, also when it then finds them gone.
--
-- Part of the versioned bundle (from 0.2.43) so existing databases get it on the next start; the installation script
-- no longer defines them. CREATE OR ALTER keeps one definition of each.
-- =====================================================

CREATE OR ALTER PROCEDURE [dbo].[delete_object_cascade]
    @object_id BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @own_tran BIT = CASE WHEN @@TRANCOUNT = 0 THEN 1 ELSE 0 END;
    IF @own_tran = 1 BEGIN TRANSACTION;

    CREATE TABLE #locked (_id BIGINT PRIMARY KEY);
    CREATE TABLE #to_delete (_id BIGINT, depth INT);

    INSERT INTO #locked (_id)
    SELECT _id FROM _objects WITH (UPDLOCK, ROWLOCK) WHERE _id = @object_id;

    ;WITH Descendants AS (
        SELECT _id, 0 AS depth FROM #locked
        UNION ALL
        SELECT o._id, d.depth + 1
        FROM _objects o
        INNER JOIN Descendants d ON o._id_parent = d._id
    )
    INSERT INTO #to_delete SELECT _id, depth FROM Descendants;

    -- Bottom-up: deepest children first.
    DECLARE @max_depth INT = (SELECT MAX(depth) FROM #to_delete);
    WHILE @max_depth >= 0
    BEGIN
        DELETE FROM _objects WHERE _id IN (SELECT _id FROM #to_delete WHERE depth = @max_depth);
        SET @max_depth = @max_depth - 1;
    END

    IF @own_tran = 1 COMMIT TRANSACTION;

    -- The row count of this last statement is the result.
    SET NOCOUNT OFF;
    DELETE FROM #locked;
END
GO

CREATE OR ALTER PROCEDURE [dbo].[delete_objects_cascade]
    @object_ids NVARCHAR(MAX) -- Comma-separated list of IDs
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @own_tran BIT = CASE WHEN @@TRANCOUNT = 0 THEN 1 ELSE 0 END;
    IF @own_tran = 1 BEGIN TRANSACTION;

    CREATE TABLE #requested (_id BIGINT PRIMARY KEY);
    CREATE TABLE #locked (_id BIGINT PRIMARY KEY);
    CREATE TABLE #to_delete (_id BIGINT, depth INT);

    INSERT INTO #requested (_id)
    SELECT DISTINCT CAST(value AS BIGINT) FROM STRING_SPLIT(@object_ids, ',') WHERE LTRIM(value) <> N'';

    -- One key seek per requested id, in id order.
    INSERT INTO #locked (_id)
    SELECT o._id
    FROM #requested r
    INNER JOIN _objects o WITH (UPDLOCK, ROWLOCK, FORCESEEK) ON o._id = r._id;

    ;WITH Descendants AS (
        SELECT _id, 0 AS depth FROM #locked
        UNION ALL
        SELECT o._id, d.depth + 1
        FROM _objects o
        INNER JOIN Descendants d ON o._id_parent = d._id
    )
    INSERT INTO #to_delete SELECT _id, depth FROM Descendants;

    DECLARE @max_depth INT = (SELECT MAX(depth) FROM #to_delete);
    WHILE @max_depth >= 0
    BEGIN
        DELETE FROM _objects WHERE _id IN (SELECT _id FROM #to_delete WHERE depth = @max_depth);
        SET @max_depth = @max_depth - 1;
    END

    IF @own_tran = 1 COMMIT TRANSACTION;

    -- The requested objects this call locked and so deleted, each once (a requested object reached also as a
    -- descendant of another requested one is still one row here).
    SET NOCOUNT OFF;
    DELETE FROM #locked;
END
GO
