-- =====================================================================
-- 02_pvt_type_info.sql (MSSql)
-- ---------------------------------------------------------------------
-- ListItem type-info resolver. Mirrors PG v2-pvt/02_pvt_type_info.sql.
-- Returns NVARCHAR(MAX) JSON with { db_type, type_semantic, is_array }
-- or NULL if the ListItem accessor is not recognized.
-- =====================================================================
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

CREATE FUNCTION dbo.pvt_get_listitem_field_type_info(@field_name NVARCHAR(100))
RETURNS NVARCHAR(MAX)
AS
BEGIN
    RETURN CASE @field_name
        WHEN N'Value'    THEN N'{"db_type":"String","type_semantic":"String","is_array":false}'
        WHEN N'Alias'    THEN N'{"db_type":"String","type_semantic":"String","is_array":false}'
        WHEN N'IdObject' THEN N'{"db_type":"Long","type_semantic":"Long","is_array":false}'
        WHEN N'IdList'   THEN N'{"db_type":"Long","type_semantic":"Long","is_array":false}'
        WHEN N'Id'       THEN N'{"db_type":"Long","type_semantic":"Long","is_array":false}'
        ELSE NULL
    END;
END;
GO
