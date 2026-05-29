-- =====================================================================
-- 05_pvt_single_facet.sql  (MSSql v2-pvt)
-- ---------------------------------------------------------------------
-- Legacy facet condition builder — MSSql thin wrapper.
-- In MSSql v2-pvt, pvt_build_where_from_json (14_pvt_where.sql) handles
-- all filter cases inline, including property functions, hierarchical
-- operators, and 0$: base-field prefixes. This function delegates to
-- that engine via pvt_collect_fields + pvt_build_where_from_json.
--
-- Functions:
--   dbo.pvt_build_single_facet_condition(
--       @facet_condition NVARCHAR(MAX),
--       @scheme_id       BIGINT,
--       @table_alias     NVARCHAR(50),   -- default 'o'
--       @max_depth       INT             -- ignored (not used in T-SQL engine)
--   ) -> NVARCHAR(MAX)
--
-- Returns the AND-joined SQL predicate string (no leading ' AND ').
-- Returns '1=1' for empty / null input.
--
-- Depends on:
--   dbo.pvt_collect_fields     (10_pvt_field_collection.sql)
--   dbo.pvt_build_where_from_json (14_pvt_where.sql)
-- =====================================================================

SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER FUNCTION dbo.pvt_build_single_facet_condition(
    @facet_condition NVARCHAR(MAX),
    @scheme_id       BIGINT,
    @table_alias     NVARCHAR(50),
    @max_depth       INT
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    IF @facet_condition IS NULL OR ISJSON(@facet_condition) = 0
        RETURN N'1=1';

    DECLARE @prefix NVARCHAR(60) = ISNULL(@table_alias, N'o') + N'.';
    DECLARE @fields NVARCHAR(MAX) = dbo.pvt_collect_fields(@scheme_id, @facet_condition, NULL);
    RETURN dbo.pvt_build_where_from_json(@facet_condition, @fields, @prefix);
END;
GO
