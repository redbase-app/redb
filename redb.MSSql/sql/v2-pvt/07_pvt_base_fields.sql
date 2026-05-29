-- =====================================================================
-- 07_pvt_base_fields.sql  (MSSql v2-pvt)
-- ---------------------------------------------------------------------
-- Return a JSON object containing all base (_objects) fields of a
-- single redb object (no Props). Mirrors PG pvt_get_object_base_fields.
--
-- Functions:
--   dbo.pvt_get_object_base_fields(@object_id BIGINT)
--       -> NVARCHAR(MAX)  JSON object with all base-table columns
--
-- Notes:
--   - Includes [_hash] which is critical for cache validation.
--   - FOR JSON PATH, WITHOUT_ARRAY_WRAPPER collapses the single row.
--   - Returns NULL when no matching object is found.
-- =====================================================================

SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER FUNCTION dbo.pvt_get_object_base_fields(@object_id BIGINT)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    DECLARE @json NVARCHAR(MAX);
    SELECT @json = (
        SELECT
            o.[_id]               AS [id],
            o.[_name]             AS [name],
            o.[_id_scheme]        AS [scheme_id],
            o.[_id_parent]        AS [parent_id],
            o.[_id_owner]         AS [owner_id],
            o.[_id_who_change]    AS [who_change_id],
            o.[_date_create]      AS [date_create],
            o.[_date_modify]      AS [date_modify],
            o.[_date_begin]       AS [date_begin],
            o.[_date_complete]    AS [date_complete],
            o.[_key]              AS [key],
            o.[_value_long]       AS [value_long],
            o.[_value_string]     AS [value_string],
            o.[_value_guid]       AS [value_guid],
            o.[_note]             AS [note],
            o.[_value_bool]       AS [value_bool],
            o.[_value_double]     AS [value_double],
            o.[_value_numeric]    AS [value_numeric],
            o.[_value_datetime]   AS [value_datetime],
            o.[_value_bytes]      AS [value_bytes],
            o.[_hash]             AS [hash]
        FROM dbo._objects o
        WHERE o.[_id] = @object_id
        FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    );
    RETURN @json;
END;
GO
