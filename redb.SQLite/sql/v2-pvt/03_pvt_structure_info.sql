-- =====================================================================
-- pvt_find_structure_info: look up structure metadata by field path
-- ---------------------------------------------------------------------
-- Forked from redb_facets_search.sql L291 on 2026-05-18.
-- Legacy version stays untouched. Mirror legacy bug fixes here.
-- =====================================================================

CREATE OR REPLACE FUNCTION pvt_find_structure_info(
    scheme_id bigint,
    root_field text,
    nested_field text DEFAULT NULL
) RETURNS TABLE (
    root_structure_id bigint,
    nested_structure_id bigint,
    root_type_info jsonb,
    nested_type_info jsonb
)
LANGUAGE 'plpgsql'
COST 50
VOLATILE NOT LEAKPROOF  
AS $BODY$
DECLARE
    scheme_def jsonb;
BEGIN
    -- Get scheme definition using existing function
    SELECT get_scheme_definition(scheme_id) INTO scheme_def;
    
    -- Find root structure
    SELECT 
        (struct->>'_id')::bigint,
        jsonb_build_object(
            'type_name', struct->>'_type_name',
            'db_type', struct->>'_type_db_type', 
            'type_semantic', struct->>'_type_dotnet_type',
            'is_array', (struct->>'_collection_type')::bigint IS NOT NULL
        )
    INTO root_structure_id, root_type_info
    FROM jsonb_array_elements(scheme_def->'structures') AS struct
    WHERE struct->>'_name' = root_field
      AND struct->>'_id_parent' IS NULL;
    
    -- If there is a nested field, find its structure
    IF nested_field IS NOT NULL AND root_structure_id IS NOT NULL THEN
        -- 🎯 NEW: Special handling for ListItem fields
        IF root_type_info->>'type_semantic' = '_RListItem' THEN
            -- For ListItem nested fields (Value, Alias, etc.) are not structures
            -- They are stored as columns in _list_items
            nested_structure_id := NULL;
            nested_type_info := pvt_get_listitem_field_type_info(nested_field);
        ELSE
            -- Normal logic for other types
            SELECT 
                (struct->>'_id')::bigint,
                jsonb_build_object(
                    'type_name', struct->>'_type_name',
                    'db_type', struct->>'_type_db_type',
                    'type_semantic', struct->>'_type_dotnet_type', 
                    'is_array', (struct->>'_collection_type')::bigint IS NOT NULL
                )
            INTO nested_structure_id, nested_type_info
            FROM jsonb_array_elements(scheme_def->'structures') AS struct
            WHERE struct->>'_name' = nested_field
              AND (struct->>'_id_parent')::bigint = root_structure_id;
        END IF;
    ELSE
        nested_structure_id := NULL;
        nested_type_info := NULL;
    END IF;
    
    RETURN QUERY SELECT root_structure_id, nested_structure_id, root_type_info, nested_type_info;
END;
$BODY$;

COMMENT ON FUNCTION pvt_find_structure_info(bigint, text, text) IS 'Finds structure information for Class fields using get_scheme_definition. Returns structure IDs and type metadata for root and nested fields.';

