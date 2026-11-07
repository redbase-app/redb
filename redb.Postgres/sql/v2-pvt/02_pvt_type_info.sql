-- =====================================================================
-- pvt_get_listitem_field_type_info: resolve type info for ListItem-typed fields
-- ---------------------------------------------------------------------
-- Forked from redb_facets_search.sql L268 on 2026-05-18.
-- Legacy version stays untouched. Mirror legacy bug fixes here.
-- =====================================================================

CREATE OR REPLACE FUNCTION pvt_get_listitem_field_type_info(field_name text)
RETURNS jsonb
LANGUAGE 'plpgsql'
IMMUTABLE
AS $BODY$
BEGIN
    CASE field_name
        WHEN 'Value' THEN 
            RETURN jsonb_build_object('db_type', 'String', 'type_semantic', 'String', 'is_array', false);
        WHEN 'Alias' THEN 
            RETURN jsonb_build_object('db_type', 'String', 'type_semantic', 'String', 'is_array', false);
        WHEN 'IdObject' THEN 
            RETURN jsonb_build_object('db_type', 'Long', 'type_semantic', 'Long', 'is_array', false);
        WHEN 'IdList' THEN 
            RETURN jsonb_build_object('db_type', 'Long', 'type_semantic', 'Long', 'is_array', false);
        WHEN 'Id' THEN 
            RETURN jsonb_build_object('db_type', 'Long', 'type_semantic', 'Long', 'is_array', false);
        ELSE
            RETURN NULL;
    END CASE;
END;
$BODY$;

