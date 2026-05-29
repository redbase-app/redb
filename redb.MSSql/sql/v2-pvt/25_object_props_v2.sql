-- =====================================================================
-- 25_object_props_v2.sql  (MSSql v2-pvt)
-- ---------------------------------------------------------------------
-- PG-specific functions build_hierarchical_properties_sql and
-- get_object_json_v2 rely on PostgreSQL composite-type arrays (_values[])
-- which have no direct T-SQL equivalent. The MSSql architecture uses
-- separate correlated subqueries and the wide-pivot CTE machinery
-- (pvt_build_cte_sql, pvt_build_query_sql) to surface all props.
--
-- These stubs allow the file to be deployed without error. Actual
-- structured JSON for a single object is composed from the _objects row
-- plus _values lookups at the application layer (RedbServiceBase).
--
-- Functions:
--   dbo.build_hierarchical_properties_sql(@object_id BIGINT)
--       -> NVARCHAR(MAX)  Always NULL (not implemented in MSSql slice)
--   dbo.get_object_json_v2(@object_id BIGINT)
--       -> NVARCHAR(MAX)  Always NULL (not implemented in MSSql slice)
-- =====================================================================

SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER FUNCTION dbo.build_hierarchical_properties_sql(@object_id BIGINT)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    -- Not implemented: PG-specific composite-type array approach.
    -- MSSql returns props via the wide-pivot CTE (pvt_build_cte_sql).
    RETURN NULL;
END;
GO

CREATE OR ALTER FUNCTION dbo.get_object_json_v2(@object_id BIGINT)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    -- Not implemented: PG-specific function.
    -- MSSql surfaces single-object props via correlated _values subqueries.
    RETURN NULL;
END;
GO
