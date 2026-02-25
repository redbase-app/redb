-- ===== REDB FACETS & SEARCH MODULE =====
-- Module for faceted search and filtering of objects
-- Architecture: Modular system by Ruslan + our relational arrays + Class fields
-- Includes: LINQ operators, logical operators, Class fields, hierarchical search

-- ===== DATA TYPES =====

-- 🎯 Composite type for structural information (used in condition building functions)
DROP TYPE IF EXISTS structure_info_type CASCADE;
CREATE TYPE structure_info_type AS (
    root_structure_id bigint,
    nested_structure_id bigint,
    root_type_info jsonb,
    nested_type_info jsonb
);

-- ===== CLEANUP OF EXISTING FUNCTIONS =====
DROP FUNCTION IF EXISTS _format_json_array_for_in CASCADE;
DROP FUNCTION IF EXISTS _parse_field_path CASCADE;
DROP FUNCTION IF EXISTS _find_structure_info CASCADE;
DROP FUNCTION IF EXISTS _build_inner_condition CASCADE;
DROP FUNCTION IF EXISTS _build_exists_condition CASCADE;
DROP FUNCTION IF EXISTS _build_and_condition CASCADE;
DROP FUNCTION IF EXISTS _build_or_condition CASCADE;
DROP FUNCTION IF EXISTS _build_not_condition CASCADE;
DROP FUNCTION IF EXISTS _build_single_facet_condition CASCADE;
DROP FUNCTION IF EXISTS _build_facet_field_path CASCADE;
DROP FUNCTION IF EXISTS get_facets CASCADE;
-- DROP FUNCTION IF EXISTS build_advanced_facet_conditions CASCADE; -- ✅ REMOVED IN VARIANT C
-- DROP FUNCTION IF EXISTS build_base_facet_conditions CASCADE; -- ✅ REMOVED! DEAD CODE!
DROP FUNCTION IF EXISTS build_order_conditions CASCADE;
DROP FUNCTION IF EXISTS build_has_ancestor_condition CASCADE;
DROP FUNCTION IF EXISTS build_has_descendant_condition CASCADE;
DROP FUNCTION IF EXISTS build_level_condition CASCADE;
DROP FUNCTION IF EXISTS build_hierarchical_conditions CASCADE;
DROP FUNCTION IF EXISTS execute_objects_query CASCADE;
DROP FUNCTION IF EXISTS search_objects_with_facets CASCADE;
-- Remove ALL versions of search_tree_objects_with_facets (old and new)
DROP FUNCTION IF EXISTS search_tree_objects_with_facets(bigint, bigint, jsonb, integer, integer, jsonb, integer, integer) CASCADE;
DROP FUNCTION IF EXISTS search_tree_objects_with_facets(bigint, bigint, jsonb, integer, integer, jsonb, integer, integer, boolean) CASCADE;
DROP FUNCTION IF EXISTS search_tree_objects_with_facets(bigint, bigint[], jsonb, integer, integer, jsonb, integer, integer, boolean) CASCADE;
DROP FUNCTION IF EXISTS get_search_sql_preview CASCADE;
DROP FUNCTION IF EXISTS get_search_tree_sql_preview(bigint, bigint, jsonb, integer, integer, jsonb, integer, integer) CASCADE;
DROP FUNCTION IF EXISTS get_search_tree_sql_preview(bigint, bigint[], jsonb, integer, integer, jsonb, integer, integer) CASCADE;

-- ===== HELPER FUNCTIONS =====

-- 🚀 Function to normalize base field names C# → SQL
-- Maps C# names (snake_case and PascalCase) to _objects column names with _ prefix
DROP FUNCTION IF EXISTS _normalize_base_field_name CASCADE;
CREATE OR REPLACE FUNCTION _normalize_base_field_name(field_name text)
RETURNS text
LANGUAGE 'plpgsql'
IMMUTABLE
AS $BODY$
BEGIN
    RETURN CASE field_name
        -- ID fields
        WHEN 'id' THEN '_id'
        WHEN 'Id' THEN '_id'
        WHEN '_id' THEN '_id'
        WHEN 'parent_id' THEN '_id_parent'
        WHEN 'ParentId' THEN '_id_parent'
        WHEN 'id_parent' THEN '_id_parent'
        WHEN '_id_parent' THEN '_id_parent'
        WHEN 'scheme_id' THEN '_id_scheme'
        WHEN 'SchemeId' THEN '_id_scheme'
        WHEN 'id_scheme' THEN '_id_scheme'
        WHEN '_id_scheme' THEN '_id_scheme'
        WHEN 'owner_id' THEN '_id_owner'
        WHEN 'OwnerId' THEN '_id_owner'
        WHEN '_id_owner' THEN '_id_owner'
        WHEN 'who_change_id' THEN '_id_who_change'
        WHEN 'WhoChangeId' THEN '_id_who_change'
        WHEN '_id_who_change' THEN '_id_who_change'
        -- Value fields (RedbPrimitive<T> support)
        WHEN 'value_long' THEN '_value_long'
        WHEN 'ValueLong' THEN '_value_long'
        WHEN '_value_long' THEN '_value_long'
        WHEN 'value_string' THEN '_value_string'
        WHEN 'ValueString' THEN '_value_string'
        WHEN '_value_string' THEN '_value_string'
        WHEN 'value_guid' THEN '_value_guid'
        WHEN 'ValueGuid' THEN '_value_guid'
        WHEN '_value_guid' THEN '_value_guid'
        -- Other base fields
        WHEN 'key' THEN '_key'
        WHEN 'Key' THEN '_key'
        WHEN '_key' THEN '_key'
        WHEN 'name' THEN '_name'
        WHEN 'Name' THEN '_name'
        WHEN '_name' THEN '_name'
        WHEN 'note' THEN '_note'
        WHEN 'Note' THEN '_note'
        WHEN '_note' THEN '_note'
        WHEN 'value_bool' THEN '_value_bool'
        WHEN 'ValueBool' THEN '_value_bool'
        WHEN '_value_bool' THEN '_value_bool'
        -- New RedbPrimitive<T> value fields
        WHEN 'value_double' THEN '_value_double'
        WHEN 'ValueDouble' THEN '_value_double'
        WHEN '_value_double' THEN '_value_double'
        WHEN 'value_numeric' THEN '_value_numeric'
        WHEN 'ValueNumeric' THEN '_value_numeric'
        WHEN '_value_numeric' THEN '_value_numeric'
        WHEN 'value_datetime' THEN '_value_datetime'
        WHEN 'ValueDatetime' THEN '_value_datetime'
        WHEN '_value_datetime' THEN '_value_datetime'
        WHEN 'value_bytes' THEN '_value_bytes'
        WHEN 'ValueBytes' THEN '_value_bytes'
        WHEN '_value_bytes' THEN '_value_bytes'
        WHEN 'hash' THEN '_hash'
        WHEN 'Hash' THEN '_hash'
        WHEN '_hash' THEN '_hash'
        -- DateTime fields
        WHEN 'date_create' THEN '_date_create'
        WHEN 'DateCreate' THEN '_date_create'
        WHEN '_date_create' THEN '_date_create'
        WHEN 'date_modify' THEN '_date_modify'
        WHEN 'DateModify' THEN '_date_modify'
        WHEN '_date_modify' THEN '_date_modify'
        WHEN 'date_begin' THEN '_date_begin'
        WHEN 'DateBegin' THEN '_date_begin'
        WHEN '_date_begin' THEN '_date_begin'
        WHEN 'date_complete' THEN '_date_complete'
        WHEN 'DateComplete' THEN '_date_complete'
        WHEN '_date_complete' THEN '_date_complete'
        -- Not a base field - return NULL
        ELSE NULL
    END;
END;
$BODY$;

COMMENT ON FUNCTION _normalize_base_field_name(text) IS 'Normalizes C# base field names to SQL column names in _objects. Returns NULL if field is not a base field.';

-- Function to format JSON array for IN operator
CREATE OR REPLACE FUNCTION _format_json_array_for_in(
    array_data jsonb
) RETURNS text
LANGUAGE 'plpgsql'
IMMUTABLE
AS $BODY$
DECLARE
    in_values text := '';
    json_element jsonb;
    first_item boolean := true;
    element_text text;
BEGIN
    -- Check that this is an array
    IF jsonb_typeof(array_data) != 'array' THEN
        RAISE EXCEPTION 'JSON array expected, got: %', jsonb_typeof(array_data);
    END IF;
    
    -- Process each array element
    FOR json_element IN SELECT value FROM jsonb_array_elements(array_data) LOOP
        IF NOT first_item THEN
            in_values := in_values || ', ';
        END IF;
        first_item := false;
        
        -- Format element based on type
        CASE jsonb_typeof(json_element)
            WHEN 'string' THEN
                -- ✅ FIX: Extract clean string WITHOUT JSON quotes, then quote
                element_text := quote_literal(json_element #>> '{}');
            WHEN 'number' THEN
                element_text := json_element::text;
            WHEN 'boolean' THEN
                element_text := CASE WHEN (json_element)::boolean THEN 'true' ELSE 'false' END;
            ELSE
                -- ✅ FIX: Here too for other types
                element_text := quote_literal(json_element #>> '{}');
        END CASE;
        
        in_values := in_values || element_text;
    END LOOP;
    
    RETURN in_values;
END;
$BODY$;

COMMENT ON FUNCTION _format_json_array_for_in(jsonb) IS 'Converts JSONB array to string of values for SQL IN clause. Supports string, number, boolean types. Used in $in operators.';

-- Function for parsing field path for Class fields, arrays and Dictionary
CREATE OR REPLACE FUNCTION _parse_field_path(
    field_path text
) RETURNS TABLE (
    root_field text,
    nested_field text, 
    is_array boolean,
    is_nested boolean,
    dict_key text  -- NEW: Dictionary key for AddressBook[home] -> 'home'
)
LANGUAGE 'plpgsql'
IMMUTABLE
AS $BODY$
DECLARE
    bracket_pos int;
    dot_after_bracket int;
    key_end int;
BEGIN
    -- Initialize dict_key as NULL
    dict_key := NULL;
    
    -- Determine if field is array (contains [] WITHOUT key inside)
    is_array := field_path LIKE '%[]%';
    
    -- Check for Dictionary path: AddressBook[home] or AddressBook[home].City
    -- Dictionary differs from array by having non-empty key in brackets
    IF field_path ~ '\[[^\]]+\]' AND NOT is_array THEN
        -- This is a Dictionary path: AddressBook[home] or AddressBook[home].City
        bracket_pos := position('[' in field_path);
        key_end := position(']' in field_path);
        
        -- Extract root field (before bracket)
        root_field := substring(field_path from 1 for bracket_pos - 1);
        
        -- Extract dict key (inside brackets)
        dict_key := substring(field_path from bracket_pos + 1 for key_end - bracket_pos - 1);
        
        -- Check for nested field after bracket: AddressBook[home].City
        dot_after_bracket := position('.' in substring(field_path from key_end + 1));
        IF dot_after_bracket > 0 THEN
            nested_field := substring(field_path from key_end + 2); -- +2 to skip '].'
            is_nested := true;
        ELSE
            nested_field := NULL;
            is_nested := false;
        END IF;
        
        RETURN QUERY SELECT root_field, nested_field, false, is_nested, dict_key;
        RETURN;
    END IF;
    
    -- Determine if field is nested (contains dot)
    is_nested := field_path LIKE '%.%';
    
    IF is_nested THEN
        IF is_array THEN
            -- Case: "Contacts[].Email" -> root="Contacts", nested="Email", is_array=true
            root_field := split_part(replace(field_path, '[]', ''), '.', 1);
            nested_field := split_part(replace(field_path, '[]', ''), '.', 2);
        ELSE
            -- Case: "Contact.Name" -> root="Contact", nested="Name", is_array=false  
            root_field := split_part(field_path, '.', 1);
            nested_field := split_part(field_path, '.', 2);
        END IF;
    ELSE
        IF is_array THEN
            -- Case: "Tags[]" -> root="Tags", nested=NULL, is_array=true
            root_field := replace(field_path, '[]', '');
            nested_field := NULL;
        ELSE
            -- Case: "Name" -> root="Name", nested=NULL, is_array=false
            root_field := field_path;
            nested_field := NULL;
        END IF;
    END IF;
    
    RETURN QUERY SELECT root_field, nested_field, is_array, is_nested, dict_key;
END;
$BODY$;

COMMENT ON FUNCTION _parse_field_path(text) IS 'Parses field path to support Class fields, arrays and Dictionary. Supports: "Name", "Contact.Name", "Tags[]", "Contacts[].Email", "PhoneBook[home]", "AddressBook[home].City". Returns path components for further processing.';

-- Function to search for structure information for Class fields
-- 🎯 NEW: Helper function to determine ListItem field type
CREATE OR REPLACE FUNCTION _get_listitem_field_type_info(field_name text)
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

CREATE OR REPLACE FUNCTION _find_structure_info(
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
            nested_type_info := _get_listitem_field_type_info(nested_field);
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

COMMENT ON FUNCTION _find_structure_info(bigint, text, text) IS 'Finds structure information for Class fields using get_scheme_definition. Returns structure IDs and type metadata for root and nested fields.';

-- ===== SYSTEM CORE: LINQ OPERATORS =====

-- Function to build inner conditions with support for all LINQ operators
CREATE OR REPLACE FUNCTION _build_inner_condition(
    operator_name text,
    operator_value text,
    type_info jsonb  -- Type information from _find_structure_info
) RETURNS text
LANGUAGE 'plpgsql'
IMMUTABLE
AS $BODY$
DECLARE
    op_symbol text;
    pattern text;
    in_values_list text;
    db_type text := type_info->>'db_type';
    is_array boolean := (type_info->>'is_array')::boolean;
BEGIN
    -- Numeric and DateTime operators
    IF operator_name IN ('$gt', '$lt', '$gte', '$lte') THEN
        CASE operator_name
            WHEN '$gt' THEN op_symbol := '>';
            WHEN '$lt' THEN op_symbol := '<';
            WHEN '$gte' THEN op_symbol := '>=';
            WHEN '$lte' THEN op_symbol := '<=';
        END CASE;
        
        -- 🚀 OPTIMIZATION: removed fs.db_type checks - use db_type from type_info directly
        IF type_info->>'type_semantic' = 'TimeSpan' THEN
            -- TimeSpan: convert to INTERVAL for correct comparison
            RETURN format('fv._String::interval %s %L::interval', op_symbol, operator_value);
        
        ELSIF operator_value ~ '^\d{4}-\d{2}-\d{2}' OR db_type = 'DateTimeOffset' THEN
            RETURN format('fv._DateTimeOffset %s %L::timestamptz', op_symbol, operator_value);
        ELSIF db_type = 'Long' THEN
            -- Known Long type (int, long)
            RETURN format('fv._Long %s %L::bigint', op_symbol, operator_value);
        ELSIF db_type = 'Double' THEN
            -- Known Double type (float, double)
            RETURN format('fv._Double %s %L::double precision', op_symbol, operator_value);
        ELSIF db_type = 'Numeric' THEN
            -- Known Numeric type (decimal)
            RETURN format('fv._Numeric %s %L::numeric', op_symbol, operator_value);
        ELSE
            -- Type unknown - check all numeric types (fallback)
            RETURN format('((fv._Long %s %L::bigint) OR (fv._Double %s %L::double precision) OR (fv._Numeric %s %L::numeric))',
                op_symbol, operator_value, op_symbol, operator_value, op_symbol, operator_value);
        END IF;
    
    -- String operators (case-sensitive)
    -- 🚀 OPTIMIZATION: removed fs.db_type check - type already known for string operators
    ELSIF operator_name IN ('$startsWith', '$endsWith', '$contains') THEN
        CASE operator_name
            WHEN '$startsWith' THEN pattern := operator_value || '%';
            WHEN '$endsWith' THEN pattern := '%' || operator_value;
            WHEN '$contains' THEN pattern := '%' || operator_value || '%';
        END CASE;
        
        RETURN format('fv._String LIKE %L', pattern);
    
    -- String operators (case-insensitive)
    -- 🚀 OPTIMIZATION: removed fs.db_type check
    ELSIF operator_name IN ('$startsWithIgnoreCase', '$endsWithIgnoreCase', '$containsIgnoreCase') THEN
        CASE operator_name
            WHEN '$startsWithIgnoreCase' THEN pattern := operator_value || '%';
            WHEN '$endsWithIgnoreCase' THEN pattern := '%' || operator_value;
            WHEN '$containsIgnoreCase' THEN pattern := '%' || operator_value || '%';
        END CASE;
        
        RETURN format('fv._String ILIKE %L', pattern);
    
    -- IN operator
    -- 🚀 OPTIMIZATION: removed fs.db_type checks - use db_type from type_info directly
    ELSIF operator_name = '$in' THEN
        in_values_list := _format_json_array_for_in(operator_value::jsonb);
        IF type_info->>'type_semantic' = 'TimeSpan' THEN
            -- TimeSpan: convert to INTERVAL for correct comparison
            RETURN format('fv._String::interval IN (%s)', 
                regexp_replace(in_values_list, '([^,]+)', '\1::interval', 'g'));
        ELSIF type_info->>'type_semantic' = '_RListItem' THEN
            -- ListItem: values stored in _listitem column as list element IDs
            RETURN format('fv._listitem IN (%s)', in_values_list);
        ELSIF db_type = 'String' THEN
            RETURN format('fv._String IN (%s)', in_values_list);
        ELSIF db_type = 'Long' THEN
            RETURN format('fv._Long IN (%s)', in_values_list);
        ELSIF db_type = 'Double' THEN
            RETURN format('fv._Double IN (%s)', in_values_list);
        ELSIF db_type = 'Numeric' THEN
            RETURN format('fv._Numeric IN (%s)', in_values_list);
        ELSIF db_type = 'Boolean' THEN
            RETURN format('fv._Boolean IN (%s)', in_values_list);
        ELSIF db_type = 'DateTimeOffset' THEN
            RETURN format('fv._DateTimeOffset IN (%s)', in_values_list);
        ELSE
            -- Fallback: try all types (only if type unknown)
            RETURN format('(fv._String IN (%s) OR fv._Long IN (%s) OR fv._Double IN (%s) OR fv._Numeric IN (%s) OR fv._Boolean IN (%s) OR fv._DateTimeOffset IN (%s))',
                in_values_list, in_values_list, in_values_list, in_values_list, in_values_list, in_values_list);
        END IF;
    
    -- NOT EQUAL operator - requires special handling
    -- 🚀 OPTIMIZATION: removed fs.db_type checks - use db_type from type_info directly
    ELSIF operator_name = '$ne' THEN
        -- For $ne null this is a special case - look for existing records (in EAV null = no record)
        IF operator_value IS NULL OR operator_value = 'null' OR operator_value = '' THEN
            RETURN 'TRUE';  -- Any existing record means "not null"
        ELSE
            -- $ne specific value - build positive condition for negation via NOT EXISTS
            IF type_info->>'type_semantic' = '_RListItem' THEN
                RETURN format('fv._listitem = %L::bigint', operator_value);
            ELSIF db_type = 'DateTimeOffset' OR operator_value ~ '^\d{4}-\d{2}-\d{2}' THEN
                RETURN format('fv._DateTimeOffset = %L::timestamptz', operator_value);
            ELSIF db_type = 'Guid' OR operator_value ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
                RETURN format('fv._Guid = %L::uuid', operator_value);
            ELSIF db_type = 'Long' THEN
                RETURN format('fv._Long = %L::bigint', operator_value);
            ELSIF db_type = 'Double' THEN
                RETURN format('fv._Double = %L::double precision', operator_value);
            ELSIF db_type = 'Numeric' THEN
                RETURN format('fv._Numeric = %L::numeric', operator_value);
            ELSIF db_type = 'Boolean' OR operator_value IN ('true', 'false') THEN
                RETURN format('fv._Boolean = %L::boolean', operator_value);
            ELSIF db_type = 'String' THEN
                RETURN format('fv._String = %L', operator_value);
            ELSIF operator_value ~ '^-?\d+(\.\d+)?$' THEN
                -- Fallback: numeric value, type unknown
                IF operator_value ~ '^-?\d+$' THEN
                    RETURN format('(fv._Long = %L::bigint OR fv._Double = %L::double precision OR fv._Numeric = %L::numeric)', 
                        operator_value, operator_value, operator_value);
                ELSE
                    RETURN format('(fv._Double = %L::double precision OR fv._Numeric = %L::numeric)', 
                        operator_value, operator_value);
                END IF;
            ELSE
                RETURN format('fv._String = %L', operator_value);
            END IF;
        END IF;
    
    -- Explicit equality operator
    -- 🚀 OPTIMIZATION: removed fs.db_type checks - use db_type from type_info directly
    ELSIF operator_name = '$eq' THEN
        IF type_info->>'type_semantic' = 'TimeSpan' THEN
            RETURN format('fv._String::interval = %L::interval', operator_value);
        ELSIF type_info->>'type_semantic' = '_RListItem' THEN
            RETURN format('fv._listitem = %L::bigint', operator_value);
        ELSIF db_type = 'DateTimeOffset' OR operator_value ~ '^\d{4}-\d{2}-\d{2}' THEN
            RETURN format('fv._DateTimeOffset = %L::timestamptz', operator_value);
        ELSIF db_type = 'Guid' OR operator_value ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
            RETURN format('fv._Guid = %L::uuid', operator_value);
        ELSIF db_type = 'Long' THEN
            RETURN format('fv._Long = %L::bigint', operator_value);
        ELSIF db_type = 'Double' THEN
            RETURN format('fv._Double = %L::double precision', operator_value);
        ELSIF db_type = 'Numeric' THEN
            RETURN format('fv._Numeric = %L::numeric', operator_value);
        ELSIF db_type = 'Boolean' OR operator_value IN ('true', 'false') THEN
            RETURN format('fv._Boolean = %L::boolean', operator_value);
        ELSIF db_type = 'String' THEN
            RETURN format('fv._String = %L', operator_value);
        ELSIF operator_value ~ '^-?\d+(\.\d+)?$' THEN
            -- Fallback: numeric value, type unknown
            IF operator_value ~ '^-?\d+$' THEN
                RETURN format('(fv._Long = %L::bigint OR fv._Double = %L::double precision OR fv._Numeric = %L::numeric)', 
                    operator_value, operator_value, operator_value);
            ELSE
                RETURN format('(fv._Double = %L::double precision OR fv._Numeric = %L::numeric)', 
                    operator_value, operator_value);
            END IF;
        ELSE
            RETURN format('fv._String = %L', operator_value);
        END IF;
    
    -- 🚀 EXTENDED RELATIONAL ARRAY OPERATORS
    -- ✅ OPTIMIZATION: Simple condition instead of nested EXISTS
    -- _build_exists_condition already creates fv context with fv._array_index IS NOT NULL
    ELSIF operator_name = '$arrayContains' THEN
        -- 🎯 Search value in relational array with SMART typing
        IF db_type = 'Long' OR (operator_value ~ '^-?\d+$' AND type_info->>'type_semantic' != '_RListItem') THEN
            -- Numeric value (Long)
            RETURN format('fv._Long = %L::bigint', operator_value);
        ELSIF db_type = 'Boolean' OR operator_value IN ('true', 'false') THEN
            -- Boolean value
            RETURN format('fv._Boolean = %L::boolean', operator_value);
        ELSIF db_type = 'DateTimeOffset' OR operator_value ~ '^\d{4}-\d{2}-\d{2}' THEN
            -- DateTime value
            RETURN format('fv._DateTimeOffset = %L::timestamp', operator_value);
        ELSIF type_info->>'type_semantic' = '_RListItem' THEN
            -- ListItem array: fv._String will be replaced with li._value in _build_exists_condition
            RETURN format('fv._String = %L', operator_value);
        ELSE
            -- String array (default)
            RETURN format('fv._String = %L', operator_value);
        END IF;
    
    -- Non-empty array check operator  
    ELSIF operator_name = '$arrayAny' THEN
        RETURN 'EXISTS(
            SELECT 1 FROM _values av
            WHERE av._id_object = fv._id_object
              AND av._id_structure = fv._id_structure
              AND av._array_index IS NOT NULL
        )';
    
    -- Empty array check operator
    ELSIF operator_name = '$arrayEmpty' THEN
        RETURN 'NOT EXISTS(
            SELECT 1 FROM _values av
            WHERE av._id_object = fv._id_object
              AND av._id_structure = fv._id_structure  
              AND av._array_index IS NOT NULL
        )';
    
    -- 📊 ARRAY ELEMENT COUNT OPERATORS
    ELSIF operator_name = '$arrayCount' THEN
        RETURN format('(
            SELECT COUNT(*) FROM _values av
            WHERE av._id_object = fv._id_object
              AND av._id_structure = fv._id_structure
              AND av._array_index IS NOT NULL
        ) = %L::int', operator_value::int);
    
    ELSIF operator_name = '$arrayCountGt' THEN
        RETURN format('(
            SELECT COUNT(*) FROM _values av
            WHERE av._id_object = fv._id_object
              AND av._id_structure = fv._id_structure
              AND av._array_index IS NOT NULL
        ) > %L::int', operator_value::int);
    
    ELSIF operator_name = '$arrayCountGte' THEN
        RETURN format('(
            SELECT COUNT(*) FROM _values av
            WHERE av._id_object = fv._id_object
              AND av._id_structure = fv._id_structure
              AND av._array_index IS NOT NULL
        ) >= %L::int', operator_value::int);
    
    ELSIF operator_name = '$arrayCountLt' THEN
        RETURN format('(
            SELECT COUNT(*) FROM _values av
            WHERE av._id_object = fv._id_object
              AND av._id_structure = fv._id_structure
              AND av._array_index IS NOT NULL
        ) < %L::int', operator_value::int);
    
    ELSIF operator_name = '$arrayCountLte' THEN
        RETURN format('(
            SELECT COUNT(*) FROM _values av
            WHERE av._id_object = fv._id_object
              AND av._id_structure = fv._id_structure
              AND av._array_index IS NOT NULL
        ) <= %L::int', operator_value::int);
    
    -- 🎯 OPERATORS FOR RELATIONAL ARRAYS
    ELSIF operator_name = '$arrayAt' THEN
        -- Get array element by index
        RETURN format('EXISTS(
            SELECT 1 FROM _values av
            WHERE av._id_object = fv._id_object
              AND av._id_structure = fv._id_structure
              AND av._array_index = %L
        )', operator_value::text);
    
    ELSIF operator_name = '$arrayFirst' THEN
        -- Check first array element
        -- 🚀 OPTIMIZATION: use db_type from type_info
        IF db_type = 'String' THEN
            RETURN format('EXISTS(SELECT 1 FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index = ''0'' AND av._String = %L)', operator_value);
        ELSIF db_type = 'Long' THEN
            RETURN format('EXISTS(SELECT 1 FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index = ''0'' AND av._Long = %L::bigint)', operator_value);
        ELSIF db_type = 'Double' THEN
            RETURN format('EXISTS(SELECT 1 FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index = ''0'' AND av._Double = %L::double precision)', operator_value);
        ELSIF db_type = 'Boolean' THEN
            RETURN format('EXISTS(SELECT 1 FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index = ''0'' AND av._Boolean = %L::boolean)', operator_value);
        ELSIF db_type = 'DateTimeOffset' THEN
            RETURN format('EXISTS(SELECT 1 FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index = ''0'' AND av._DateTimeOffset = %L::timestamp)', operator_value);
        ELSE
            -- Fallback: check all types
            RETURN format('EXISTS(SELECT 1 FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index = ''0'' AND (av._String = %L OR av._Long = %L::bigint OR av._Double = %L::double precision))', operator_value, operator_value, operator_value);
        END IF;
    
    ELSIF operator_name = '$arrayLast' THEN
        -- Check last array element
        -- 🚀 OPTIMIZATION: use db_type from type_info
        IF db_type = 'String' THEN
            RETURN format('EXISTS(SELECT 1 FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index = (SELECT MAX(av2._array_index::int)::text FROM _values av2 WHERE av2._id_object = fv._id_object AND av2._id_structure = fv._id_structure AND av2._array_index IS NOT NULL) AND av._String = %L)', operator_value);
        ELSIF db_type = 'Long' THEN
            RETURN format('EXISTS(SELECT 1 FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index = (SELECT MAX(av2._array_index::int)::text FROM _values av2 WHERE av2._id_object = fv._id_object AND av2._id_structure = fv._id_structure AND av2._array_index IS NOT NULL) AND av._Long = %L::bigint)', operator_value);
        ELSIF db_type = 'Double' THEN
            RETURN format('EXISTS(SELECT 1 FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index = (SELECT MAX(av2._array_index::int)::text FROM _values av2 WHERE av2._id_object = fv._id_object AND av2._id_structure = fv._id_structure AND av2._array_index IS NOT NULL) AND av._Double = %L::double precision)', operator_value);
        ELSE
            -- Fallback
            RETURN format('EXISTS(SELECT 1 FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index = (SELECT MAX(av2._array_index::int)::text FROM _values av2 WHERE av2._id_object = fv._id_object AND av2._id_structure = fv._id_structure AND av2._array_index IS NOT NULL) AND (av._String = %L OR av._Long = %L::bigint))', operator_value, operator_value);
        END IF;
    
    -- 🔍 ARRAY SEARCH OPERATORS
    -- 🚀 OPTIMIZATION: removed JOIN and fs._collection_type
    ELSIF operator_name = '$arrayStartsWith' THEN
        RETURN format('EXISTS(
            SELECT 1 FROM _values av
            WHERE av._id_object = fv._id_object
              AND av._id_structure = fv._id_structure
              AND av._array_index IS NOT NULL
              AND av._String LIKE %L
        )', operator_value || '%');
    
    ELSIF operator_name = '$arrayEndsWith' THEN
        RETURN format('EXISTS(
            SELECT 1 FROM _values av
            WHERE av._id_object = fv._id_object
              AND av._id_structure = fv._id_structure
              AND av._array_index IS NOT NULL
              AND av._String LIKE %L
        )', '%' || operator_value);
    
    ELSIF operator_name = '$arrayMatches' THEN
        RETURN format('EXISTS(
            SELECT 1 FROM _values av
            WHERE av._id_object = fv._id_object
              AND av._id_structure = fv._id_structure
              AND av._array_index IS NOT NULL
              AND av._String ~ %L
        )', operator_value);
    
    -- 📈 ARRAY AGGREGATION OPERATORS
    -- 🚀 OPTIMIZATION: use db_type from type_info
    ELSIF operator_name = '$arraySum' THEN
        IF db_type = 'Long' THEN
            RETURN format('(SELECT COALESCE(SUM(av._Long), 0) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::bigint', operator_value);
        ELSIF db_type = 'Double' THEN
            RETURN format('(SELECT COALESCE(SUM(av._Double), 0) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::double precision', operator_value);
        ELSIF db_type = 'Numeric' THEN
            RETURN format('(SELECT COALESCE(SUM(av._Numeric), 0) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::numeric', operator_value);
        ELSE
            -- Fallback: Long + Double
            RETURN format('(SELECT COALESCE(SUM(COALESCE(av._Long, 0) + COALESCE(av._Double, 0)), 0) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::numeric', operator_value);
        END IF;
    
    ELSIF operator_name = '$arrayAvg' THEN
        IF db_type = 'Long' THEN
            RETURN format('(SELECT AVG(av._Long) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::numeric', operator_value);
        ELSIF db_type = 'Double' THEN
            RETURN format('(SELECT AVG(av._Double) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::double precision', operator_value);
        ELSIF db_type = 'Numeric' THEN
            RETURN format('(SELECT AVG(av._Numeric) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::numeric', operator_value);
        ELSE
            RETURN format('(SELECT AVG(COALESCE(av._Long, av._Double)) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::numeric', operator_value);
        END IF;
    
    ELSIF operator_name = '$arrayMin' THEN
        -- 🚀 OPTIMIZATION: use db_type from type_info
        IF db_type = 'Long' THEN
            RETURN format('(SELECT MIN(av._Long) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::bigint', operator_value);
        ELSIF db_type = 'Double' THEN
            RETURN format('(SELECT MIN(av._Double) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::double precision', operator_value);
        ELSIF db_type = 'Numeric' THEN
            RETURN format('(SELECT MIN(av._Numeric) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::numeric', operator_value);
        ELSIF db_type = 'DateTimeOffset' THEN
            RETURN format('(SELECT MIN(av._DateTimeOffset) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::timestamp', operator_value);
        ELSE
            RETURN format('(SELECT MIN(COALESCE(av._Long, av._Double)) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::numeric', operator_value);
        END IF;
    
    ELSIF operator_name = '$arrayMax' THEN
        IF db_type = 'Long' THEN
            RETURN format('(SELECT MAX(av._Long) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::bigint', operator_value);
        ELSIF db_type = 'Double' THEN
            RETURN format('(SELECT MAX(av._Double) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::double precision', operator_value);
        ELSIF db_type = 'Numeric' THEN
            RETURN format('(SELECT MAX(av._Numeric) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::numeric', operator_value);
        ELSIF db_type = 'DateTimeOffset' THEN
            RETURN format('(SELECT MAX(av._DateTimeOffset) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::timestamp', operator_value);
        ELSE
            RETURN format('(SELECT MAX(COALESCE(av._Long, av._Double)) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::numeric', operator_value);
        END IF;
    
    ELSE
        -- 🚀 OPTIMIZATION: Simple equality - use db_type from type_info if known
        IF type_info->>'type_semantic' = 'TimeSpan' THEN
            RETURN format('fv._String::interval = %L::interval', operator_value);
        ELSIF db_type = 'DateTimeOffset' OR operator_value ~ '^\d{4}-\d{2}-\d{2}' THEN
            RETURN format('fv._DateTimeOffset = %L::timestamptz', operator_value);
        ELSIF db_type = 'Guid' OR operator_value ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
            RETURN format('fv._Guid = %L::uuid', operator_value);
        ELSIF operator_value ~ '^\d+(\.\d+)?:\d{2}:\d{2}' THEN
            -- TimeSpan format (HH:MM:SS)
            RETURN format('fv._String::interval = %L::interval', operator_value);
        ELSIF db_type = 'Long' THEN
            RETURN format('fv._Long = %L::bigint', operator_value);
        ELSIF db_type = 'Double' THEN
            RETURN format('fv._Double = %L::double precision', operator_value);
        ELSIF db_type = 'Numeric' THEN
            RETURN format('fv._Numeric = %L::numeric', operator_value);
        ELSIF db_type = 'Boolean' OR operator_value IN ('true', 'false') THEN
            RETURN format('fv._Boolean = %L::boolean', operator_value);
        ELSIF db_type = 'String' THEN
            RETURN format('fv._String = %L', operator_value);
        ELSIF operator_value ~ '^-?\d+(\.\d+)?$' THEN
            -- Fallback: numeric value, type unknown
            IF operator_value ~ '^-?\d+$' THEN
                RETURN format('(fv._Long = %L::bigint OR fv._Double = %L::double precision OR fv._Numeric = %L::numeric)', 
                    operator_value, operator_value, operator_value);
            ELSE
                RETURN format('(fv._Double = %L::double precision OR fv._Numeric = %L::numeric)', 
                    operator_value, operator_value);
            END IF;
        ELSE
            -- String value by default
            RETURN format('fv._String = %L', operator_value);
        END IF;
    END IF;
END;
$BODY$;

COMMENT ON FUNCTION _build_inner_condition(text, text, jsonb) IS '🚀 EXTENDED core system of LINQ operators. Supports 25+ operators: 
📊 Numeric: $gt, $gte, $lt, $lte, $ne, $in (Long, Double, Numeric)
📝 String: $contains, $startsWith, $endsWith  
⏱️ TimeSpan: $gt, $lt, $eq (conversion to INTERVAL for correct comparison)
📅 DateTimeOffset: $gt, $lt, $eq (timestamptz with timezone)
🔢 Arrays (basic): $arrayContains, $arrayAny, $arrayEmpty, $arrayCount*
🎯 Arrays (position): $arrayAt, $arrayFirst, $arrayLast
🔍 Arrays (search): $arrayStartsWith, $arrayEndsWith, $arrayMatches
📈 Arrays (aggregation): $arraySum, $arrayAvg, $arrayMin, $arrayMax
All operators adapted for relational arrays via _array_index. Supports distinguishing _RObject vs Object types. Auto type detection by value format. Special handling for TimeSpan via INTERVAL.';

-- ===== UNIVERSAL WRAPPERS =====

-- Universal function to build EXISTS/NOT EXISTS conditions with full Class field support
CREATE OR REPLACE FUNCTION _build_exists_condition(
    field_path text,
    condition_sql text,
    use_not_exists boolean DEFAULT false,
    scheme_id bigint DEFAULT NULL,
    table_alias text DEFAULT 'o'
) RETURNS text
LANGUAGE 'plpgsql'
IMMUTABLE
AS $BODY$
DECLARE
    parsed_path RECORD;
    structure_info structure_info_type;
    exists_query text;
    field_condition text;
    nested_join text := '';
    nested_condition text := '';
BEGIN
    -- Parse field path
    SELECT * INTO parsed_path FROM _parse_field_path(field_path);
    
    -- 🆕 DICTIONARY FIELDS (AddressBook[home].City or PhoneBook[home])
    IF parsed_path.dict_key IS NOT NULL AND scheme_id IS NOT NULL THEN
        -- Dictionary path: AddressBook[home].City
        SELECT 
            fi.root_structure_id,
            fi.nested_structure_id,
            fi.root_type_info,
            fi.nested_type_info
        INTO 
            structure_info
        FROM _find_structure_info(scheme_id, parsed_path.root_field, parsed_path.nested_field) AS fi 
        LIMIT 1;
        
        -- Check that root structure (Dictionary) is found
        IF structure_info.root_structure_id IS NULL THEN
            RAISE EXCEPTION 'Root structure not found for Dictionary field: %', parsed_path.root_field;
        END IF;
        
        IF parsed_path.nested_field IS NOT NULL THEN
            -- Dictionary with nested field: AddressBook[home].City
            IF structure_info.nested_structure_id IS NULL THEN
                RAISE EXCEPTION 'Nested structure % not found in Dictionary field %', parsed_path.nested_field, parsed_path.root_field;
            END IF;
            
            -- 🚀 OPTIMIZATION: JOIN only to _values for nested field, without _scheme_metadata_cache
            nested_join := format('
                JOIN _values nv ON nv._array_parent_id = fv._id
                  AND nv._id_structure = %s',
                structure_info.nested_structure_id);
            
            -- Replace fv.->nv. for nested field
            nested_condition := replace(condition_sql, 'fv.', 'nv.');
            -- 🚀 OPTIMIZATION: use fv._id_structure instead of fs._structure_id
            field_condition := format(
                'fv._id_structure = %s AND fv._array_index = %L AND %s', 
                structure_info.root_structure_id, 
                parsed_path.dict_key,
                nested_condition
            );
        ELSE
            -- 🚀 OPTIMIZATION: Simple Dictionary without JOIN
            field_condition := format(
                'fv._id_structure = %s AND fv._array_index = %L AND %s', 
                structure_info.root_structure_id, 
                parsed_path.dict_key,
                condition_sql
            );
        END IF;
        
        -- 🚀 OPTIMIZATION: EXISTS WITHOUT JOIN for Dictionary!
        exists_query := format('
            %s EXISTS (
                SELECT 1 FROM _values fv 
                %s
                WHERE fv._id_object = %s._id 
                  AND %s
            )',
            CASE WHEN use_not_exists THEN 'NOT' ELSE '' END,
            nested_join,
            table_alias,
            field_condition
        );
        
        RETURN ' AND ' || exists_query;
    
    -- 📦 CLASS FIELDS (Contact.Name syntax)
    ELSIF parsed_path.is_nested AND scheme_id IS NOT NULL THEN
        SELECT 
            fi.root_structure_id,
            fi.nested_structure_id,
            fi.root_type_info,
            fi.nested_type_info
        INTO 
            structure_info
        FROM _find_structure_info(scheme_id, parsed_path.root_field, parsed_path.nested_field) AS fi 
        LIMIT 1;
        
        -- Check that both structures are found
        IF structure_info.root_structure_id IS NULL THEN
            RAISE EXCEPTION 'Root structure not found for field: %', parsed_path.root_field;
        END IF;
        
        -- 🎯 NEW: SPECIAL HANDLING OF LISTITEM ARRAYS (Roles[].Value)
        -- For ListItem arrays nested_structure_id will be NULL, because Value/Alias are not structures but _list_items columns
        IF parsed_path.is_array 
           AND structure_info.root_type_info->>'type_semantic' = '_RListItem' 
           AND structure_info.nested_structure_id IS NULL THEN
            
            DECLARE
                listitem_column text;
                listitem_condition text;
            BEGIN
                -- Mapping IRedbListItem properties to _list_items columns
                listitem_column := CASE parsed_path.nested_field
                    WHEN 'Value' THEN '_value'
                    WHEN 'Alias' THEN '_alias'
                    WHEN 'IdList' THEN '_id_list'
                    WHEN 'Id' THEN '_id'
                    ELSE NULL
                END;
                
                IF listitem_column IS NULL THEN
                    RAISE EXCEPTION 'Unsupported ListItem property: %. Available: Value, Alias, IdList, Id', parsed_path.nested_field;
                END IF;
                
                -- 🎯 Build JOIN to ListItem array and _list_items table (use _listitem column!)
                nested_join := format('
                    JOIN _list_items li ON li._id = fv._listitem');
                
                -- Replace fv./av. with li. and adjust condition for _list_items
                -- condition_sql may contain: fv._String = 'value' or av._String = 'value' (from $arrayContains)
                -- For ListItem.Value we need: li._value = 'value'
                listitem_condition := replace(condition_sql, 'fv._String', 'li.' || listitem_column);
                listitem_condition := replace(listitem_condition, 'av._String', 'li.' || listitem_column);
                listitem_condition := replace(listitem_condition, 'fv._Long', 'li.' || listitem_column);
                listitem_condition := replace(listitem_condition, 'av._Long', 'li.' || listitem_column);
                listitem_condition := replace(listitem_condition, 'fv._DateTimeOffset', 'li.' || listitem_column);
                listitem_condition := replace(listitem_condition, 'av._DateTimeOffset', 'li.' || listitem_column);
                
                -- Remove type checks from condition_sql, since we work directly with _list_items
                listitem_condition := regexp_replace(listitem_condition, 'fs\.db_type = ''[^'']+'' AND ', '');
                listitem_condition := regexp_replace(listitem_condition, 'fs\.type_semantic = ''[^'']+'' AND ', '');
                
                -- 🚀 OPTIMIZATION: fv._id_structure instead of fs._structure_id
                field_condition := format(
                    'fv._id_structure = %s AND fv._array_index IS NOT NULL AND %s', 
                    structure_info.root_structure_id, 
                    listitem_condition
                );
            END;
            
            -- 🚀 OPTIMIZATION: EXISTS WITHOUT JOIN for ListItem array!
            exists_query := format('
                %s EXISTS (
                    SELECT 1 FROM _values fv 
                    %s
                    WHERE fv._id_object = %s._id 
                      AND %s
                )',
                CASE WHEN use_not_exists THEN 'NOT' ELSE '' END,
                nested_join,
                table_alias,
                field_condition
            );
            RETURN ' AND ' || exists_query;
        
        -- 🔍 HANDLING CLASS ARRAYS (Contact[].Name)
        ELSIF parsed_path.is_array THEN
            IF structure_info.nested_structure_id IS NULL THEN
                RAISE EXCEPTION 'Nested structure % not found in field %', parsed_path.nested_field, parsed_path.root_field;
            END IF;
            -- 🚀 OPTIMIZATION: JOIN only to _values, without _scheme_metadata_cache
            nested_join := format('
                JOIN _values nv ON nv._id_object = fv._id_object
                  AND nv._id_structure = %s
                  AND nv._array_parent_id = fv._id
                  AND nv._array_index IS NOT NULL',
                structure_info.nested_structure_id);
            
            nested_condition := replace(condition_sql, 'fv.', 'nv.');
            field_condition := format(
                'fv._id_structure = %s AND fv._array_index IS NOT NULL AND %s', 
                structure_info.root_structure_id, 
                nested_condition
            );
            
            -- 🚀 OPTIMIZATION: EXISTS WITHOUT JOIN for Class array!
            exists_query := format('
                %s EXISTS (
                    SELECT 1 FROM _values fv 
                    %s
                    WHERE fv._id_object = %s._id 
                      AND %s
                )',
                CASE WHEN use_not_exists THEN 'NOT' ELSE '' END,
                nested_join,
                table_alias,
                field_condition
            );
            RETURN ' AND ' || exists_query;
        
        -- 🔍 REGULAR CLASS FIELDS (Contact.Name) AND LISTITEM FIELDS (Status.Value)
        ELSE
            -- SPECIAL HANDLING OF REGULAR LISTITEM FIELDS (Status.Value)
            IF structure_info.root_type_info->>'type_semantic' = '_RListItem' 
               AND structure_info.nested_structure_id IS NULL THEN
                
                DECLARE
                    listitem_column text;
                    listitem_condition text;
                BEGIN
                    listitem_column := CASE parsed_path.nested_field
                        WHEN 'Value' THEN '_value'
                        WHEN 'Alias' THEN '_alias'
                        WHEN 'IdList' THEN '_id_list'
                        WHEN 'Id' THEN '_id'
                        ELSE NULL
                    END;
                    
                    IF listitem_column IS NULL THEN
                        RAISE EXCEPTION 'Unsupported ListItem property: %. Available: Value, Alias, IdList, Id', parsed_path.nested_field;
                    END IF;
                    
                    nested_join := 'JOIN _list_items li ON li._id = fv._listitem';
                    
                    -- Replace fv./av. with li. (av.* from $arrayContains)
                    listitem_condition := replace(condition_sql, 'fv._String', 'li.' || listitem_column);
                    listitem_condition := replace(listitem_condition, 'av._String', 'li.' || listitem_column);
                    listitem_condition := replace(listitem_condition, 'fv._Long', 'li.' || listitem_column);
                    listitem_condition := replace(listitem_condition, 'av._Long', 'li.' || listitem_column);
                    listitem_condition := replace(listitem_condition, 'fv._DateTimeOffset', 'li.' || listitem_column);
                    listitem_condition := replace(listitem_condition, 'av._DateTimeOffset', 'li.' || listitem_column);
                    
                    -- 🚀 OPTIMIZATION: fv._id_structure instead of fs._structure_id
                    field_condition := format(
                        'fv._id_structure = %s AND fv._array_index IS NULL AND %s', 
                        structure_info.root_structure_id, 
                        listitem_condition
                    );
                    
                    -- 🚀 OPTIMIZATION: EXISTS WITHOUT JOIN for ListItem!
                    exists_query := format('
                        %s EXISTS (
                            SELECT 1 FROM _values fv 
                            %s
                            WHERE fv._id_object = %s._id 
                              AND %s
                        )',
                        CASE WHEN use_not_exists THEN 'NOT' ELSE '' END,
                        nested_join,
                        table_alias,
                        field_condition
                    );
                    RETURN ' AND ' || exists_query;
                END;
            
            -- REGULAR CLASS FIELDS (Contact.Name)
            ELSE
                IF structure_info.nested_structure_id IS NULL THEN
                    RAISE EXCEPTION 'Nested structure % not found in field %', parsed_path.nested_field, parsed_path.root_field;
                END IF;
                
                -- 🚀 OPTIMIZATION: JOIN only to _values, without _scheme_metadata_cache
                nested_join := format('
                    JOIN _values nv ON nv._id_object = fv._id_object
                      AND nv._id_structure = %s
                      AND nv._array_index IS NULL',
                    structure_info.nested_structure_id);
                
                nested_condition := replace(condition_sql, 'fv.', 'nv.');
                field_condition := format(
                    'fv._id_structure = %s AND fv._array_index IS NULL AND %s', 
                    structure_info.root_structure_id, 
                    nested_condition
                );
                
                -- 🚀 OPTIMIZATION: EXISTS WITHOUT JOIN for Class!
                exists_query := format('
                    %s EXISTS (
                        SELECT 1 FROM _values fv 
                        %s
                        WHERE fv._id_object = %s._id 
                          AND %s
                    )',
                    CASE WHEN use_not_exists THEN 'NOT' ELSE '' END,
                    nested_join,
                    table_alias,
                    field_condition
                );
                RETURN ' AND ' || exists_query;
            END IF;
        END IF;
    
    -- 📋 REGULAR FIELDS AND ARRAYS (Name, Tags[])
    -- 🚀 OPTIMIZATION: Get structure_id and use it directly without JOIN!
    ELSE
        -- Get structure information (if scheme_id provided)
        IF scheme_id IS NOT NULL THEN
            SELECT 
                fi.root_structure_id,
                fi.nested_structure_id,
                fi.root_type_info,
                fi.nested_type_info
            INTO structure_info
            FROM _find_structure_info(scheme_id, parsed_path.root_field, NULL) AS fi 
            LIMIT 1;
        END IF;
        
        IF structure_info.root_structure_id IS NOT NULL THEN
            -- ✅ OPTIMAL PATH: Use structure_id directly without JOIN
            IF parsed_path.is_array THEN
                field_condition := format('fv._id_structure = %s AND fv._array_index IS NOT NULL AND %s', 
                                        structure_info.root_structure_id, 
                                        condition_sql);
            ELSE
                field_condition := format('fv._id_structure = %s AND fv._array_index IS NULL AND %s', 
                                        structure_info.root_structure_id, 
                                        condition_sql);
            END IF;
            nested_join := '';
            
            -- 🚀 SUPER-OPTIMIZATION: EXISTS WITHOUT JOIN!
            exists_query := format('
                %s EXISTS (
                    SELECT 1 FROM _values fv 
                    %s
                    WHERE fv._id_object = %s._id 
                      AND %s
                )',
                CASE WHEN use_not_exists THEN 'NOT' ELSE '' END,
                nested_join,
                table_alias,
                field_condition
            );
            
            RETURN ' AND ' || exists_query;
        ELSE
            -- Fallback: scheme_id not provided or structure not found - use old path with JOIN
            IF parsed_path.is_array THEN
                field_condition := format('fs._name = %L AND fs._collection_type IS NOT NULL AND %s', 
                                        parsed_path.root_field, 
                                        condition_sql);
            ELSE
                field_condition := format('fs._name = %L AND fs._collection_type IS NULL AND fv._array_index IS NULL AND %s', 
                                        parsed_path.root_field, 
                                        condition_sql);
            END IF;
            nested_join := '';
        END IF;
    END IF;
    
    -- Fallback path with JOIN (for complex cases: Dictionary, Class, or when structure_id not found)
    exists_query := format('
        %s EXISTS (
            SELECT 1 FROM _values fv 
            JOIN _scheme_metadata_cache fs ON fs._structure_id = fv._id_structure
            %s
            WHERE fv._id_object = %s._id 
              AND %s
        )',
        CASE WHEN use_not_exists THEN 'NOT' ELSE '' END,
        nested_join,
        table_alias,
        field_condition
    );
    
    RETURN ' AND ' || exists_query;
END;
$BODY$;

COMMENT ON FUNCTION _build_exists_condition(text, text, boolean, bigint, text) IS '🚀 EXTENDED universal wrapper for building EXISTS/NOT EXISTS conditions with full Class architecture support:
📝 Regular fields: Name, Title  
📋 Regular arrays: Tags[], Categories[]
📦 Class fields: Contact.Name, Address.City (via _structures._id_parent)
🔗 Class arrays: Contacts[].Email, Addresses[].Street (combination of _array_index + _id_parent)
Automatically determines field type, builds correct JOINs for nested structures, checks for structure existence in scheme.';

-- ===== LOGICAL OPERATORS =====

-- Function to build AND conditions (recursive)
CREATE OR REPLACE FUNCTION _build_and_condition(
    and_array jsonb,
    scheme_id bigint,
    table_alias text DEFAULT 'o',
    max_depth integer DEFAULT 50
) RETURNS text
LANGUAGE 'plpgsql'
IMMUTABLE
AS $BODY$
DECLARE
    conditions text := '';
    condition_item jsonb;
    single_condition text;
    i integer;
BEGIN
    -- Recursion depth check
    IF max_depth <= 0 THEN
        RAISE EXCEPTION 'Maximum recursion depth reached for $and operator';
    END IF;
    
    -- Check that this is an array
    IF jsonb_typeof(and_array) != 'array' OR jsonb_array_length(and_array) = 0 THEN
        RETURN '';
    END IF;
    
    -- Process each array element
    FOR i IN 0..jsonb_array_length(and_array) - 1 LOOP
        condition_item := and_array->i;
        
        -- Recursively process each element
        single_condition := _build_single_facet_condition(condition_item, scheme_id, table_alias, max_depth - 1);
        
        IF single_condition != '' AND single_condition != ' AND TRUE' THEN
            -- Remove extra ' AND ' from the beginning of each condition
            single_condition := ltrim(single_condition, ' AND ');
            
            IF conditions != '' THEN
                conditions := conditions || ' AND ';
            END IF;
            conditions := conditions || single_condition;
        END IF;
    END LOOP;
    
    IF conditions != '' THEN
        RETURN ' AND (' || conditions || ')';
    ELSE
        RETURN '';
    END IF;
END;
$BODY$;

-- Function to build OR conditions (recursive)  
CREATE OR REPLACE FUNCTION _build_or_condition(
    or_array jsonb,
    scheme_id bigint,
    table_alias text DEFAULT 'o',
    max_depth integer DEFAULT 50
) RETURNS text
LANGUAGE 'plpgsql'
IMMUTABLE
AS $BODY$
DECLARE
    conditions text := '';
    condition_item jsonb;
    single_condition text;
    or_parts text[] := '{}';
    i integer;
    final_condition text;
BEGIN
    -- Recursion depth check
    IF max_depth <= 0 THEN
        RAISE EXCEPTION 'Maximum recursion depth reached for $or operator';
    END IF;
    
    -- Check that this is an array
    IF jsonb_typeof(or_array) != 'array' OR jsonb_array_length(or_array) = 0 THEN
        RETURN '';
    END IF;
    
    -- Process each array element
    FOR i IN 0..jsonb_array_length(or_array) - 1 LOOP
        condition_item := or_array->i;
        
        -- Recursively process each element (remove prefix ' AND ')
        single_condition := _build_single_facet_condition(condition_item, scheme_id, table_alias, max_depth - 1);
        
        IF single_condition != '' AND single_condition != ' AND TRUE' THEN
            -- Remove ' AND ' from the beginning of each condition for OR
            single_condition := ltrim(single_condition, ' AND ');
            or_parts := array_append(or_parts, single_condition);
        END IF;
    END LOOP;
    
    -- Combine via OR
    IF array_length(or_parts, 1) > 0 THEN
        final_condition := array_to_string(or_parts, ' OR ');
        RETURN ' AND (' || final_condition || ')';
    END IF;
    
    RETURN '';
END;
$BODY$;

-- Function to build NOT conditions (recursive)
CREATE OR REPLACE FUNCTION _build_not_condition(
    not_object jsonb,
    scheme_id bigint,
    table_alias text DEFAULT 'o',
    max_depth integer DEFAULT 50
) RETURNS text
LANGUAGE 'plpgsql'
IMMUTABLE
AS $BODY$
DECLARE
    inner_condition text;
BEGIN
    -- Recursion depth check
    IF max_depth <= 0 THEN
        RAISE EXCEPTION 'Maximum recursion depth reached for $not operator';
    END IF;
    
    -- Recursively process inner condition
    inner_condition := _build_single_facet_condition(not_object, scheme_id, table_alias, max_depth - 1);
    
    IF inner_condition != '' AND inner_condition != 'TRUE' THEN
        -- Convert EXISTS to NOT EXISTS and vice versa
        IF inner_condition LIKE '%EXISTS (%' THEN
            inner_condition := replace(inner_condition, 'EXISTS (', 'NOT EXISTS (');
            RETURN ' AND ' || inner_condition;
        ELSIF inner_condition LIKE '%NOT EXISTS (%' THEN  
            inner_condition := replace(inner_condition, 'NOT EXISTS (', 'EXISTS (');
            RETURN ' AND ' || inner_condition;
        ELSE
            -- For complex conditions wrap in NOT
            RETURN ' AND NOT (' || inner_condition || ')';
        END IF;
    END IF;
    
    RETURN '';
END;
$BODY$;

-- Universal function for processing single facet condition (recursive)
CREATE OR REPLACE FUNCTION _build_single_facet_condition(
    facet_condition jsonb,
    scheme_id bigint,
    table_alias text DEFAULT 'o',
    max_depth integer DEFAULT 50
) RETURNS text
LANGUAGE 'plpgsql'
IMMUTABLE  
AS $BODY$
DECLARE
    condition_key text;
    condition_value jsonb;
    field_path text;
    parsed_path RECORD;
    structure_info structure_info_type;
    operator_name text;
    operator_value text;
    inner_condition_sql text;
    all_conditions text := '';
    single_condition text;
BEGIN
    -- Recursion depth check
    IF max_depth <= 0 THEN
        RAISE EXCEPTION 'Maximum recursion depth (50) reached for filter. Simplify JSON filter.';
    END IF;
    
    -- Check input data type
    IF jsonb_typeof(facet_condition) != 'object' THEN
        RETURN '';
    END IF;
    
    -- Process each key-value pair
    FOR condition_key, condition_value IN SELECT * FROM jsonb_each(facet_condition) LOOP
        -- Logical operators - ACCUMULATE instead of RETURN to process ALL keys at same level
        -- NOTE: Pass max_depth WITHOUT decrement, because _build_and/or/not decrement themselves when calling _build_single
        IF condition_key = '$and' THEN
            single_condition := _build_and_condition(condition_value, scheme_id, table_alias, max_depth);
            IF single_condition != '' THEN
                single_condition := ltrim(single_condition, ' AND ');
                IF all_conditions != '' THEN
                    all_conditions := all_conditions || ' AND ' || single_condition;
                ELSE
                    all_conditions := single_condition;
                END IF;
            END IF;
            CONTINUE;
        ELSIF condition_key = '$or' THEN
            single_condition := _build_or_condition(condition_value, scheme_id, table_alias, max_depth);
            IF single_condition != '' THEN
                single_condition := ltrim(single_condition, ' AND ');
                IF all_conditions != '' THEN
                    all_conditions := all_conditions || ' AND ' || single_condition;
                ELSE
                    all_conditions := single_condition;
                END IF;
            END IF;
            CONTINUE;
        ELSIF condition_key = '$not' THEN
            single_condition := _build_not_condition(condition_value, scheme_id, table_alias, max_depth);
            IF single_condition != '' THEN
                single_condition := ltrim(single_condition, ' AND ');
                IF all_conditions != '' THEN
                    all_conditions := all_conditions || ' AND ' || single_condition;
                ELSE
                    all_conditions := single_condition;
                END IF;
            END IF;
            CONTINUE;
        
        -- Hierarchical operators (processed separately)
        ELSIF condition_key IN ('$hasAncestor', '$hasDescendant', '$level', '$isRoot', '$isLeaf', '$childrenOf') THEN
            CONTINUE; -- Skip, they are processed in build_hierarchical_conditions
        
        -- 🆕 Property functions: Field.$length, Field[].$count
        -- p.Name.Length > 3  → {"Name.$length": {"$gt": 3}}
        -- p.Tags.Count >= 5  → {"Tags[].$count": {"$gte": 5}}
        ELSIF condition_key ~ '\.\$length$' OR condition_key ~ '\.\$count$' THEN
            DECLARE
                func_is_length boolean := condition_key ~ '\.\$length$';
                func_field_name text;
                func_structure_id bigint;
                func_condition text;
                func_op_name text;
                func_op_value text;
                func_compare_op text;
            BEGIN
                -- Extract field name: "Name.$length" -> "Name", "Tags[].$count" -> "Tags"
                IF func_is_length THEN
                    func_field_name := regexp_replace(condition_key, '\.\$length$', '');
                ELSE
                    func_field_name := regexp_replace(condition_key, '\[\]\.\$count$', '');
                    func_field_name := regexp_replace(func_field_name, '\.\$count$', '');
                END IF;
                
                -- Find structure
                SELECT _id INTO func_structure_id
                FROM _structures
                WHERE _id_scheme = scheme_id 
                  AND _name = func_field_name
                  AND _id_parent IS NULL;
                
                IF func_structure_id IS NULL THEN
                    CONTINUE; -- Field not found, skip
                END IF;
                
                -- Process operators
                IF jsonb_typeof(condition_value) = 'object' THEN
                    FOR func_op_name, func_op_value IN SELECT key, value #>> '{}' FROM jsonb_each(condition_value) LOOP
                        func_compare_op := CASE func_op_name
                            WHEN '$eq' THEN '='
                            WHEN '$ne' THEN '<>'
                            WHEN '$gt' THEN '>'
                            WHEN '$gte' THEN '>='
                            WHEN '$lt' THEN '<'
                            WHEN '$lte' THEN '<='
                            ELSE '='
                        END;
                        
                        IF func_is_length THEN
                            -- String length: LENGTH(v._String)
                            func_condition := format(
                                'EXISTS (SELECT 1 FROM _values fv WHERE fv._id_object = %I._id AND fv._id_structure = %L AND fv._array_index IS NULL AND LENGTH(fv._String) %s %L::integer)',
                                table_alias, func_structure_id, func_compare_op, func_op_value
                            );
                        ELSE
                            -- Array count
                            func_condition := format(
                                '(SELECT COUNT(*) FROM _values fv WHERE fv._id_object = %I._id AND fv._id_structure = %L AND fv._array_index IS NOT NULL) %s %L::integer',
                                table_alias, func_structure_id, func_compare_op, func_op_value
                            );
                        END IF;
                        
                        IF all_conditions != '' THEN
                            all_conditions := all_conditions || ' AND ' || func_condition;
                        ELSE
                            all_conditions := func_condition;
                        END IF;
                    END LOOP;
                END IF;
            END;
        
        -- 🚀 _objects TABLE BASE FIELDS (with "0$:" prefix)
        -- 🆕 CRITICAL BUG FIX: Now base fields are EXPLICITLY marked with "0$:" prefix
        -- This resolves name conflicts: Props.Name vs RedbObject.name
        -- Prefix "0$:" is impossible as identifier in any programming language
        -- C# passes: "0$:name", "0$:parent_id", "0$:Id" etc.
        ELSIF condition_key LIKE '0$:%' THEN
            DECLARE
                raw_field_name text := substring(condition_key from 4);  -- remove '0$:'
                sql_field_name text := _normalize_base_field_name(raw_field_name);
                base_condition text := '';
            BEGIN
                -- 🛡️ PROTECTION: Check that field is recognized as base
                IF sql_field_name IS NULL THEN
                    RAISE EXCEPTION 'Unknown RedbObject base field: "%" (passed as "0$:%"). Valid fields: id, parent_id, scheme_id, owner_id, who_change_id, date_create, date_modify, date_begin, date_complete, key, value_long, value_string, value_guid, value_bool, value_double, value_numeric, value_datetime, value_bytes, name, note, hash', 
                        raw_field_name, raw_field_name;
                END IF;
                
                -- Determine field type and build condition
                -- Numeric fields (bigint): _id, _id_parent, _id_scheme, _id_owner, _id_who_change, _value_long, _key
                -- 🛡️ SECURITY: Use %L::bigint instead of %s to protect against SQL injection
                IF sql_field_name IN ('_id', '_id_parent', '_id_scheme', '_id_owner', '_id_who_change', '_value_long', '_key') THEN
                    IF jsonb_typeof(condition_value) = 'object' THEN
                        FOR operator_name, operator_value IN SELECT key, COALESCE(value #>> '{}', 'null') FROM jsonb_each(condition_value) LOOP
                            single_condition := CASE operator_name
                                WHEN '$in' THEN format('%I.%I IN (%s)', table_alias, sql_field_name, _format_json_array_for_in(condition_value->'$in'))
                                WHEN '$eq' THEN 
                                    CASE WHEN operator_value = 'null' THEN format('%I.%I IS NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I = %L::bigint', table_alias, sql_field_name, operator_value)
                                    END
                                WHEN '$ne' THEN 
                                    CASE WHEN operator_value = 'null' THEN format('%I.%I IS NOT NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I != %L::bigint', table_alias, sql_field_name, operator_value)
                                    END
                                WHEN '$gt' THEN format('%I.%I > %L::bigint', table_alias, sql_field_name, operator_value)
                                WHEN '$gte' THEN format('%I.%I >= %L::bigint', table_alias, sql_field_name, operator_value)
                                WHEN '$lt' THEN format('%I.%I < %L::bigint', table_alias, sql_field_name, operator_value)
                                WHEN '$lte' THEN format('%I.%I <= %L::bigint', table_alias, sql_field_name, operator_value)
                                -- 🎯 $exists for base fields: IS NULL / IS NOT NULL
                                WHEN '$exists' THEN 
                                    CASE WHEN operator_value = 'true' THEN format('%I.%I IS NOT NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I IS NULL', table_alias, sql_field_name)
                                    END
                                ELSE NULL
                            END;
                            IF single_condition IS NOT NULL THEN
                                IF base_condition != '' THEN base_condition := base_condition || ' AND ' || single_condition;
                                ELSE base_condition := single_condition; END IF;
                            END IF;
                        END LOOP;
                    ELSIF jsonb_typeof(condition_value) = 'array' THEN
                        base_condition := format('%I.%I IN (%s)', table_alias, sql_field_name, _format_json_array_for_in(condition_value));
                    ELSIF jsonb_typeof(condition_value) = 'number' THEN
                        base_condition := format('%I.%I = %L::bigint', table_alias, sql_field_name, condition_value::text);
                    ELSIF condition_value = 'null'::jsonb THEN
                        base_condition := format('%I.%I IS NULL', table_alias, sql_field_name);
                    END IF;
                
                -- String fields (text): _value_string, _name, _note
                ELSIF sql_field_name IN ('_value_string', '_name', '_note') THEN
                    IF jsonb_typeof(condition_value) = 'object' THEN
                        FOR operator_name, operator_value IN SELECT key, COALESCE(value #>> '{}', 'null') FROM jsonb_each(condition_value) LOOP
                            single_condition := CASE operator_name
                                WHEN '$eq' THEN 
                                    CASE WHEN operator_value = 'null' THEN format('%I.%I IS NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I = %L', table_alias, sql_field_name, operator_value)
                                    END
                                WHEN '$ne' THEN 
                                    CASE WHEN operator_value = 'null' THEN format('%I.%I IS NOT NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I != %L', table_alias, sql_field_name, operator_value)
                                    END
                                WHEN '$in' THEN format('%I.%I IN (%s)', table_alias, sql_field_name, _format_json_array_for_in(condition_value->'$in'))
                                WHEN '$contains' THEN format('%I.%I LIKE %L', table_alias, sql_field_name, '%' || operator_value || '%')
                                WHEN '$containsIgnoreCase' THEN format('%I.%I ILIKE %L', table_alias, sql_field_name, '%' || operator_value || '%')
                                WHEN '$startsWith' THEN format('%I.%I LIKE %L', table_alias, sql_field_name, operator_value || '%')
                                WHEN '$startsWithIgnoreCase' THEN format('%I.%I ILIKE %L', table_alias, sql_field_name, operator_value || '%')
                                WHEN '$endsWith' THEN format('%I.%I LIKE %L', table_alias, sql_field_name, '%' || operator_value)
                                WHEN '$endsWithIgnoreCase' THEN format('%I.%I ILIKE %L', table_alias, sql_field_name, '%' || operator_value)
                                WHEN '$exists' THEN 
                                    CASE WHEN operator_value = 'true' THEN format('%I.%I IS NOT NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I IS NULL', table_alias, sql_field_name)
                                    END
                                ELSE NULL
                            END;
                            IF single_condition IS NOT NULL THEN
                                IF base_condition != '' THEN base_condition := base_condition || ' AND ' || single_condition;
                                ELSE base_condition := single_condition; END IF;
                            END IF;
                        END LOOP;
                    ELSIF jsonb_typeof(condition_value) = 'array' THEN
                        base_condition := format('%I.%I IN (%s)', table_alias, sql_field_name, _format_json_array_for_in(condition_value));
                    ELSIF jsonb_typeof(condition_value) = 'string' THEN
                        base_condition := format('%I.%I = %L', table_alias, sql_field_name, condition_value #>> '{}');
                    ELSIF condition_value = 'null'::jsonb THEN
                        base_condition := format('%I.%I IS NULL', table_alias, sql_field_name);
                    END IF;
                
                -- UUID fields: _value_guid, _hash
                ELSIF sql_field_name IN ('_value_guid', '_hash') THEN
                    IF jsonb_typeof(condition_value) = 'object' THEN
                        FOR operator_name, operator_value IN SELECT key, COALESCE(value #>> '{}', 'null') FROM jsonb_each(condition_value) LOOP
                            single_condition := CASE operator_name
                                WHEN '$eq' THEN 
                                    CASE WHEN operator_value = 'null' THEN format('%I.%I IS NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I = %L::uuid', table_alias, sql_field_name, operator_value)
                                    END
                                WHEN '$ne' THEN 
                                    CASE WHEN operator_value = 'null' THEN format('%I.%I IS NOT NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I != %L::uuid', table_alias, sql_field_name, operator_value)
                                    END
                                WHEN '$in' THEN format('%I.%I IN (%s)', table_alias, sql_field_name, _format_json_array_for_in(condition_value->'$in'))
                                WHEN '$exists' THEN 
                                    CASE WHEN operator_value = 'true' THEN format('%I.%I IS NOT NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I IS NULL', table_alias, sql_field_name)
                                    END
                                ELSE NULL
                            END;
                            IF single_condition IS NOT NULL THEN
                                IF base_condition != '' THEN base_condition := base_condition || ' AND ' || single_condition;
                                ELSE base_condition := single_condition; END IF;
                            END IF;
                        END LOOP;
                    ELSIF jsonb_typeof(condition_value) = 'string' THEN
                        base_condition := format('%I.%I = %L::uuid', table_alias, sql_field_name, condition_value #>> '{}');
                    ELSIF condition_value = 'null'::jsonb THEN
                        base_condition := format('%I.%I IS NULL', table_alias, sql_field_name);
                    END IF;
                
                -- DateTime fields: _date_create, _date_modify, _date_begin, _date_complete
                ELSIF sql_field_name IN ('_date_create', '_date_modify', '_date_begin', '_date_complete', '_value_datetime') THEN
                    IF jsonb_typeof(condition_value) = 'object' THEN
                        FOR operator_name, operator_value IN SELECT key, COALESCE(value #>> '{}', 'null') FROM jsonb_each(condition_value) LOOP
                            single_condition := CASE operator_name
                                WHEN '$eq' THEN 
                                    CASE WHEN operator_value = 'null' THEN format('%I.%I IS NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I = %L::timestamptz', table_alias, sql_field_name, operator_value)
                                    END
                                WHEN '$ne' THEN 
                                    CASE WHEN operator_value = 'null' THEN format('%I.%I IS NOT NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I != %L::timestamptz', table_alias, sql_field_name, operator_value)
                                    END
                                WHEN '$gt' THEN format('%I.%I > %L::timestamptz', table_alias, sql_field_name, operator_value)
                                WHEN '$gte' THEN format('%I.%I >= %L::timestamptz', table_alias, sql_field_name, operator_value)
                                WHEN '$lt' THEN format('%I.%I < %L::timestamptz', table_alias, sql_field_name, operator_value)
                                WHEN '$lte' THEN format('%I.%I <= %L::timestamptz', table_alias, sql_field_name, operator_value)
                                WHEN '$exists' THEN 
                                    CASE WHEN operator_value = 'true' THEN format('%I.%I IS NOT NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I IS NULL', table_alias, sql_field_name)
                                    END
                                ELSE NULL
                            END;
                            IF single_condition IS NOT NULL THEN
                                IF base_condition != '' THEN base_condition := base_condition || ' AND ' || single_condition;
                                ELSE base_condition := single_condition; END IF;
                            END IF;
                        END LOOP;
                    ELSIF jsonb_typeof(condition_value) = 'string' THEN
                        base_condition := format('%I.%I = %L::timestamptz', table_alias, sql_field_name, condition_value #>> '{}');
                    ELSIF condition_value = 'null'::jsonb THEN
                        base_condition := format('%I.%I IS NULL', table_alias, sql_field_name);
                    END IF;
                
                -- 🛡️ SECURITY: Boolean field with %L::boolean
                -- Boolean field: _value_bool
                ELSIF sql_field_name = '_value_bool' THEN
                    IF jsonb_typeof(condition_value) = 'object' THEN
                        FOR operator_name, operator_value IN SELECT key, COALESCE(value #>> '{}', 'null') FROM jsonb_each(condition_value) LOOP
                            single_condition := CASE operator_name
                                WHEN '$eq' THEN 
                                    CASE WHEN operator_value = 'null' THEN format('%I.%I IS NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I = %L::boolean', table_alias, sql_field_name, operator_value)
                                    END
                                WHEN '$ne' THEN 
                                    CASE WHEN operator_value = 'null' THEN format('%I.%I IS NOT NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I != %L::boolean', table_alias, sql_field_name, operator_value)
                                    END
                                WHEN '$exists' THEN 
                                    CASE WHEN operator_value = 'true' THEN format('%I.%I IS NOT NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I IS NULL', table_alias, sql_field_name)
                                    END
                                ELSE NULL
                            END;
                            IF single_condition IS NOT NULL THEN
                                IF base_condition != '' THEN base_condition := base_condition || ' AND ' || single_condition;
                                ELSE base_condition := single_condition; END IF;
                            END IF;
                        END LOOP;
                    ELSIF jsonb_typeof(condition_value) = 'boolean' THEN
                        base_condition := format('%I.%I = %L::boolean', table_alias, sql_field_name, condition_value::text);
                    ELSIF condition_value = 'null'::jsonb THEN
                        base_condition := format('%I.%I IS NULL', table_alias, sql_field_name);
                    END IF;
                
                -- Double/Numeric fields: _value_double, _value_numeric
                ELSIF sql_field_name IN ('_value_double', '_value_numeric') THEN
                    IF jsonb_typeof(condition_value) = 'object' THEN
                        FOR operator_name, operator_value IN SELECT key, COALESCE(value #>> '{}', 'null') FROM jsonb_each(condition_value) LOOP
                            single_condition := CASE operator_name
                                WHEN '$eq' THEN 
                                    CASE WHEN operator_value = 'null' THEN format('%I.%I IS NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I = %L::numeric', table_alias, sql_field_name, operator_value)
                                    END
                                WHEN '$ne' THEN 
                                    CASE WHEN operator_value = 'null' THEN format('%I.%I IS NOT NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I != %L::numeric', table_alias, sql_field_name, operator_value)
                                    END
                                WHEN '$gt' THEN format('%I.%I > %L::numeric', table_alias, sql_field_name, operator_value)
                                WHEN '$gte' THEN format('%I.%I >= %L::numeric', table_alias, sql_field_name, operator_value)
                                WHEN '$lt' THEN format('%I.%I < %L::numeric', table_alias, sql_field_name, operator_value)
                                WHEN '$lte' THEN format('%I.%I <= %L::numeric', table_alias, sql_field_name, operator_value)
                                WHEN '$exists' THEN 
                                    CASE WHEN operator_value = 'true' THEN format('%I.%I IS NOT NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I IS NULL', table_alias, sql_field_name)
                                    END
                                ELSE NULL
                            END;
                            IF single_condition IS NOT NULL THEN
                                IF base_condition != '' THEN base_condition := base_condition || ' AND ' || single_condition;
                                ELSE base_condition := single_condition; END IF;
                            END IF;
                        END LOOP;
                    ELSIF jsonb_typeof(condition_value) = 'number' THEN
                        base_condition := format('%I.%I = %L::numeric', table_alias, sql_field_name, condition_value::text);
                    ELSIF condition_value = 'null'::jsonb THEN
                        base_condition := format('%I.%I IS NULL', table_alias, sql_field_name);
                    END IF;
                
                -- Bytes field: _value_bytes (only $exists supported)
                ELSIF sql_field_name = '_value_bytes' THEN
                    IF jsonb_typeof(condition_value) = 'object' THEN
                        FOR operator_name, operator_value IN SELECT key, COALESCE(value #>> '{}', 'null') FROM jsonb_each(condition_value) LOOP
                            single_condition := CASE operator_name
                                WHEN '$exists' THEN 
                                    CASE WHEN operator_value = 'true' THEN format('%I.%I IS NOT NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I IS NULL', table_alias, sql_field_name)
                                    END
                                ELSE NULL
                            END;
                            IF single_condition IS NOT NULL THEN
                                IF base_condition != '' THEN base_condition := base_condition || ' AND ' || single_condition;
                                ELSE base_condition := single_condition; END IF;
                            END IF;
                        END LOOP;
                    ELSIF condition_value = 'null'::jsonb THEN
                        base_condition := format('%I.%I IS NULL', table_alias, sql_field_name);
                    END IF;
                END IF;
                
                -- ✅ ACCUMULATE base condition in all_conditions (instead of RETURN)
                IF base_condition != '' THEN
                    IF all_conditions != '' THEN
                        all_conditions := all_conditions || ' AND ' || base_condition;
                    ELSE
                        all_conditions := base_condition;
                    END IF;
                END IF;
            END;
            -- Do NOT RETURN - continue processing other fields!
        
        -- 🆕 Collection != null / == null: Check that Array/Dictionary has/does not have elements
        -- Triggers when condition_value = {"$ne": null} or {"$exists": true/false} for collection field
        ELSIF jsonb_typeof(condition_value) = 'object' 
              AND (condition_value ? '$ne' OR condition_value ? '$exists')
              AND condition_key ~ '^[A-Za-z_][A-Za-z0-9_]*$' THEN
            DECLARE
                coll_field_name text := condition_key;
                coll_structure_id bigint;
                coll_collection_type text;
                coll_condition text;
                coll_is_ne_null boolean;
                eav_ne_null_value text;
                eav_exists_value text;
            BEGIN
                -- Check if this is a Collection field (Array or Dictionary)
                SELECT s._id, s._collection_type INTO coll_structure_id, coll_collection_type
                FROM _structures s
                WHERE s._id_scheme = scheme_id 
                  AND s._name = coll_field_name
                  AND s._id_parent IS NULL;
                
                -- Only handle if it's a Collection (Array or Dictionary)
                IF coll_structure_id IS NOT NULL AND coll_collection_type IS NOT NULL THEN
                    -- Determine if checking for "not null" or "null"
                    coll_is_ne_null := (condition_value->>'$ne' IS NULL AND condition_value ? '$ne')
                                    OR (condition_value->>'$exists' = 'true');
                    
                    IF coll_is_ne_null OR condition_value ? '$ne' THEN
                        -- Collection != null: at least one element exists
                        coll_condition := format(
                            'EXISTS (SELECT 1 FROM _values cv WHERE cv._id_object = %I._id AND cv._id_structure = %L AND cv._array_index IS NOT NULL)',
                            table_alias, coll_structure_id
                        );
                    ELSE
                        -- Collection == null: no elements
                        coll_condition := format(
                            'NOT EXISTS (SELECT 1 FROM _values cv WHERE cv._id_object = %I._id AND cv._id_structure = %L AND cv._array_index IS NOT NULL)',
                            table_alias, coll_structure_id
                        );
                    END IF;
                    
                    IF all_conditions != '' THEN
                        all_conditions := all_conditions || ' AND ' || coll_condition;
                    ELSE
                        all_conditions := coll_condition;
                    END IF;
                    
                    CONTINUE;  -- Skip to next field, don't fall through to standard handling
                
                -- 🆕 FIX: Regular EAV fields with $ne null or $exists (NOT collections!)
                ELSIF coll_structure_id IS NOT NULL THEN
                    -- This is a regular EAV field (not a collection)
                    eav_ne_null_value := condition_value->>'$ne';
                    eav_exists_value := condition_value->>'$exists';
                    
                    -- $ne null OR $exists true = "field exists" (record exists in _values)
                    IF (condition_value ? '$ne' AND (eav_ne_null_value IS NULL OR eav_ne_null_value = 'null'))
                       OR (eav_exists_value = 'true') THEN
                        -- In EAV model: record existence = field exists and not null
                        coll_condition := format(
                            'EXISTS (SELECT 1 FROM _values ev WHERE ev._id_object = %I._id AND ev._id_structure = %L AND ev._array_index IS NULL)',
                            table_alias, coll_structure_id
                        );
                        
                        IF all_conditions != '' THEN
                            all_conditions := all_conditions || ' AND ' || coll_condition;
                        ELSE
                            all_conditions := coll_condition;
                        END IF;
                        CONTINUE;
                    
                    -- $exists false = "field does NOT exist" (no record in _values)
                    ELSIF eav_exists_value = 'false' THEN
                        coll_condition := format(
                            'NOT EXISTS (SELECT 1 FROM _values ev WHERE ev._id_object = %I._id AND ev._id_structure = %L AND ev._array_index IS NULL)',
                            table_alias, coll_structure_id
                        );
                        
                        IF all_conditions != '' THEN
                            all_conditions := all_conditions || ' AND ' || coll_condition;
                        ELSE
                            all_conditions := coll_condition;
                        END IF;
                        CONTINUE;
                    END IF;
                    -- If $ne with specific value — fall through to standard handling
                END IF;
                -- If structure not found, fall through to standard handling
            END;
        
        -- 🆕 Dictionary indexer: FieldName[key] (example: "PhoneBook[home]": {"$eq": "+7-999..."})
        -- Direct implementation without _build_inner_condition (it uses incompatible aliases fs/fv)
        ELSIF condition_key ~ '^[A-Za-z_][A-Za-z0-9_]*\[.+\]$' THEN
            DECLARE
                dict_field_name text;
                dict_key text;
                dict_structure_id bigint;
                dict_type_info jsonb;
                dict_db_type text;
                dict_condition text;
                dict_op_name text;
                dict_op_value text;
                dict_value text;
                dict_value_condition text;
            BEGIN
                -- Parse: "PhoneBook[home]" -> field="PhoneBook", key="home"
                dict_field_name := substring(condition_key from '^([A-Za-z_][A-Za-z0-9_]*)\[');
                dict_key := substring(condition_key from '\[(.+)\]$');
                
                -- Find structure and type_info for dictionary field
                SELECT fi.root_structure_id, fi.root_type_info
                INTO dict_structure_id, dict_type_info
                FROM _find_structure_info(scheme_id, dict_field_name, NULL) AS fi
                LIMIT 1;
                
                IF dict_structure_id IS NULL THEN
                    RAISE EXCEPTION 'Dictionary field "%" not found in scheme %', dict_field_name, scheme_id;
                END IF;
                
                -- Get db_type from type_info for correct column selection
                dict_db_type := dict_type_info->>'db_type';
                
                -- Process condition_value - build value comparison
                IF jsonb_typeof(condition_value) = 'object' THEN
                    -- Complex condition: {"$eq": "value"}, {"$ne": "x"}, {"$contains": "substr"}, {"$in": [...]}, etc.
                    FOR dict_op_name, dict_op_value IN SELECT key, value FROM jsonb_each_text(condition_value) LOOP
                        -- Build value condition based on operator
                        IF dict_op_name = '$in' THEN
                            -- $in operator: value in list (use db_type for correct column)
                            dict_value_condition := CASE dict_db_type
                                WHEN 'Long' THEN format('dv._Long IN (SELECT (jsonb_array_elements_text(%L::jsonb))::bigint)', dict_op_value)
                                WHEN 'Numeric' THEN format('dv._Numeric IN (SELECT (jsonb_array_elements_text(%L::jsonb))::numeric)', dict_op_value)
                                WHEN 'Double' THEN format('dv._Double IN (SELECT (jsonb_array_elements_text(%L::jsonb))::double precision)', dict_op_value)
                                ELSE format('dv._String IN (SELECT jsonb_array_elements_text(%L::jsonb))', dict_op_value)
                            END;
                        ELSIF dict_op_name = '$nin' THEN
                            -- $nin operator: value not in list (use db_type for correct column)
                            dict_value_condition := CASE dict_db_type
                                WHEN 'Long' THEN format('dv._Long NOT IN (SELECT (jsonb_array_elements_text(%L::jsonb))::bigint)', dict_op_value)
                                WHEN 'Numeric' THEN format('dv._Numeric NOT IN (SELECT (jsonb_array_elements_text(%L::jsonb))::numeric)', dict_op_value)
                                WHEN 'Double' THEN format('dv._Double NOT IN (SELECT (jsonb_array_elements_text(%L::jsonb))::double precision)', dict_op_value)
                                ELSE format('dv._String NOT IN (SELECT jsonb_array_elements_text(%L::jsonb))', dict_op_value)
                            END;
                        ELSE
                            -- Numeric comparisons - use db_type to select correct column
                            IF dict_op_name IN ('$gt', '$gte', '$lt', '$lte') THEN
                                dict_value_condition := CASE dict_db_type
                                    WHEN 'Numeric' THEN format('dv._Numeric %s %L::numeric', 
                                        CASE dict_op_name WHEN '$gt' THEN '>' WHEN '$gte' THEN '>=' WHEN '$lt' THEN '<' WHEN '$lte' THEN '<=' END, 
                                        dict_op_value)
                                    WHEN 'Double' THEN format('dv._Double %s %L::double precision',
                                        CASE dict_op_name WHEN '$gt' THEN '>' WHEN '$gte' THEN '>=' WHEN '$lt' THEN '<' WHEN '$lte' THEN '<=' END,
                                        dict_op_value)
                                    ELSE format('dv._Long %s %L::bigint',
                                        CASE dict_op_name WHEN '$gt' THEN '>' WHEN '$gte' THEN '>=' WHEN '$lt' THEN '<' WHEN '$lte' THEN '<=' END,
                                        dict_op_value)
                                END;
                            ELSE
                                dict_value_condition := CASE dict_op_name
                                    -- Equality/Inequality - use db_type for correct column
                                    WHEN '$eq' THEN CASE dict_db_type
                                        WHEN 'Long' THEN format('dv._Long = %L::bigint', dict_op_value)
                                        WHEN 'Numeric' THEN format('dv._Numeric = %L::numeric', dict_op_value)
                                        WHEN 'Double' THEN format('dv._Double = %L::double precision', dict_op_value)
                                        ELSE format('dv._String = %L', dict_op_value)
                                    END
                                    WHEN '$ne' THEN CASE dict_db_type
                                        WHEN 'Long' THEN format('dv._Long <> %L::bigint', dict_op_value)
                                        WHEN 'Numeric' THEN format('dv._Numeric <> %L::numeric', dict_op_value)
                                        WHEN 'Double' THEN format('dv._Double <> %L::double precision', dict_op_value)
                                        ELSE format('dv._String <> %L', dict_op_value)
                                    END
                                    -- String operations
                                    WHEN '$contains' THEN format('dv._String LIKE %L', '%' || dict_op_value || '%')
                                    WHEN '$startsWith' THEN format('dv._String LIKE %L', dict_op_value || '%')
                                    WHEN '$endsWith' THEN format('dv._String LIKE %L', '%' || dict_op_value)
                                    WHEN '$containsIgnoreCase' THEN format('dv._String ILIKE %L', '%' || dict_op_value || '%')
                                    WHEN '$startsWithIgnoreCase' THEN format('dv._String ILIKE %L', dict_op_value || '%')
                                    WHEN '$endsWithIgnoreCase' THEN format('dv._String ILIKE %L', '%' || dict_op_value)
                                    -- Regex
                                    WHEN '$regex' THEN format('dv._String ~ %L', dict_op_value)
                                    WHEN '$iregex' THEN format('dv._String ~* %L', dict_op_value)
                                    -- Explicit Double comparisons (legacy support)
                                    WHEN '$gtDouble' THEN format('dv._Double > %L', dict_op_value::double precision)
                                    WHEN '$gteDouble' THEN format('dv._Double >= %L', dict_op_value::double precision)
                                    WHEN '$ltDouble' THEN format('dv._Double < %L', dict_op_value::double precision)
                                    WHEN '$lteDouble' THEN format('dv._Double <= %L', dict_op_value::double precision)
                                    ELSE format('dv._String = %L', dict_op_value)  -- fallback to equality
                                END;
                            END IF;
                        END IF;
                        
                        dict_condition := format(
                            'EXISTS (SELECT 1 FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._array_index = %L AND %s)',
                            table_alias, dict_structure_id, dict_key, dict_value_condition
                        );
                        
                        IF all_conditions != '' THEN
                            all_conditions := all_conditions || ' AND ' || dict_condition;
                        ELSE
                            all_conditions := dict_condition;
                        END IF;
                    END LOOP;
                ELSE
                    -- Simple value: direct equality (use db_type for correct column)
                    dict_value := condition_value #>> '{}';
                    dict_condition := format(
                        'EXISTS (SELECT 1 FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._array_index = %L AND %s)',
                        table_alias, dict_structure_id, dict_key,
                        CASE dict_db_type
                            WHEN 'Long' THEN format('dv._Long = %L::bigint', dict_value)
                            WHEN 'Numeric' THEN format('dv._Numeric = %L::numeric', dict_value)
                            WHEN 'Double' THEN format('dv._Double = %L::double precision', dict_value)
                            ELSE format('dv._String = %L', dict_value)
                        END
                    );
                    
                    IF all_conditions != '' THEN
                        all_conditions := all_conditions || ' AND ' || dict_condition;
                    ELSE
                        all_conditions := dict_condition;
                    END IF;
                END IF;
            END;
        
        -- 🆕 Dictionary ContainsKey: FieldName.ContainsKey (example: "PhoneBook.ContainsKey": "home")
        -- ⚠️ For Dictionary<K, Class> records are stored in child structures, not in the structure itself!
        ELSIF condition_key ~ '^[A-Za-z_][A-Za-z0-9_]*\.ContainsKey$' THEN
            DECLARE
                dict_field_name text;
                dict_key text;
                dict_structure_id bigint;
                dict_condition text;
            BEGIN
                -- Parse: "PhoneBook.ContainsKey" -> field="PhoneBook"
                dict_field_name := substring(condition_key from '^([A-Za-z_][A-Za-z0-9_]*)\.ContainsKey$');
                -- Get the key (handle both {"$eq": "key"} and "key")
                IF condition_value ? '$eq' THEN
                    dict_key := condition_value->>'$eq';
                ELSE
                    dict_key := condition_value #>> '{}';
                END IF;
                
                -- Find structure for dictionary field
                SELECT _id INTO dict_structure_id
                FROM _structures 
                WHERE _id_scheme = scheme_id 
                  AND _name = dict_field_name
                  AND _id_parent IS NULL;
                
                IF dict_structure_id IS NULL THEN
                    RAISE EXCEPTION 'Dictionary field "%" not found in scheme %', dict_field_name, scheme_id;
                END IF;
                
                -- Build EXISTS condition
                -- 🆕 FIX: Check BOTH cases:
                --   1) Dictionary<K, primitive>: records in the structure itself (_id_structure = dict_id)
                --   2) Dictionary<K, Class>: records in child structures (_id_parent = dict_id)
                dict_condition := format(
                    'EXISTS (SELECT 1 FROM _values dv JOIN _structures s ON dv._id_structure = s._id WHERE dv._id_object = %I._id AND (s._id = %L OR s._id_parent = %L) AND dv._array_index = %L)',
                    table_alias, dict_structure_id, dict_structure_id, dict_key
                );
                
                IF all_conditions != '' THEN
                    all_conditions := all_conditions || ' AND ' || dict_condition;
                ELSE
                    all_conditions := dict_condition;
                END IF;
            END;
        
        -- 🆕 Dictionary aggregation operators: FieldName.$dictCount, FieldName.$dictSum, etc.
        ELSIF condition_key ~ '^[A-Za-z_][A-Za-z0-9_]*\.\$dict(Count|Sum|Avg|Min|Max|Keys|Values|HasValue)$' THEN
            DECLARE
                dict_field_name text;
                dict_op text;
                dict_structure_id bigint;
                dict_condition text;
                dict_value text;
            BEGIN
                -- Parse: "PhoneBook.$dictCount" -> field="PhoneBook", op="Count"
                dict_field_name := substring(condition_key from '^([A-Za-z_][A-Za-z0-9_]*)\.\$dict');
                dict_op := substring(condition_key from '\$dict([A-Za-z]+)$');
                dict_value := condition_value #>> '{}';
                
                -- Find structure for dictionary field
                SELECT _id INTO dict_structure_id
                FROM _structures 
                WHERE _id_scheme = scheme_id 
                  AND _name = dict_field_name
                  AND _id_parent IS NULL;
                
                IF dict_structure_id IS NULL THEN
                    RAISE EXCEPTION 'Dictionary field "%" not found in scheme %', dict_field_name, scheme_id;
                END IF;
                
                -- Build aggregation condition
                dict_condition := CASE dict_op
                    WHEN 'Count' THEN format(
                        '(SELECT COUNT(*) FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._array_index IS NOT NULL) = %L::int',
                        table_alias, dict_structure_id, dict_value::int)
                    WHEN 'Sum' THEN format(
                        '(SELECT COALESCE(SUM(dv._Long), 0) FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._array_index IS NOT NULL) = %L::bigint',
                        table_alias, dict_structure_id, dict_value::bigint)
                    WHEN 'Avg' THEN format(
                        '(SELECT AVG(dv._Long) FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._array_index IS NOT NULL) = %L::numeric',
                        table_alias, dict_structure_id, dict_value::numeric)
                    WHEN 'Min' THEN format(
                        '(SELECT MIN(dv._Long) FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._array_index IS NOT NULL) = %L::bigint',
                        table_alias, dict_structure_id, dict_value::bigint)
                    WHEN 'Max' THEN format(
                        '(SELECT MAX(dv._Long) FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._array_index IS NOT NULL) = %L::bigint',
                        table_alias, dict_structure_id, dict_value::bigint)
                    WHEN 'Keys' THEN format(
                        'EXISTS (SELECT 1 FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._array_index = %L)',
                        table_alias, dict_structure_id, dict_value)
                    WHEN 'Values' THEN format(
                        'EXISTS (SELECT 1 FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._String = %L)',
                        table_alias, dict_structure_id, dict_value)
                    WHEN 'HasValue' THEN format(
                        'EXISTS (SELECT 1 FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._String = %L)',
                        table_alias, dict_structure_id, dict_value)
                    ELSE 'TRUE'
                END;
                
                IF all_conditions != '' THEN
                    all_conditions := all_conditions || ' AND ' || dict_condition;
                ELSE
                    all_conditions := dict_condition;
                END IF;
            END;
        
        -- 🆕 Dictionary comparison operators: FieldName.$dictCountGt, FieldName.$dictCountLt, etc.
        ELSIF condition_key ~ '^[A-Za-z_][A-Za-z0-9_]*\.\$dict(Count|Sum|Avg|Min|Max)(Gt|Gte|Lt|Lte)$' THEN
            DECLARE
                dict_field_name text;
                dict_agg text;
                dict_cmp text;
                dict_structure_id bigint;
                dict_condition text;
                dict_value text;
                dict_cmp_op text;
            BEGIN
                -- Parse: "PhoneBook.$dictCountGt" -> field="PhoneBook", agg="Count", cmp="Gt"
                dict_field_name := substring(condition_key from '^([A-Za-z_][A-Za-z0-9_]*)\.\$dict');
                dict_agg := substring(condition_key from '\$dict(Count|Sum|Avg|Min|Max)');
                dict_cmp := substring(condition_key from '(Gt|Gte|Lt|Lte)$');
                dict_value := condition_value #>> '{}';
                
                dict_cmp_op := CASE dict_cmp
                    WHEN 'Gt' THEN '>'
                    WHEN 'Gte' THEN '>='
                    WHEN 'Lt' THEN '<'
                    WHEN 'Lte' THEN '<='
                END;
                
                -- Find structure for dictionary field
                SELECT _id INTO dict_structure_id
                FROM _structures 
                WHERE _id_scheme = scheme_id 
                  AND _name = dict_field_name
                  AND _id_parent IS NULL;
                
                IF dict_structure_id IS NULL THEN
                    RAISE EXCEPTION 'Dictionary field "%" not found in scheme %', dict_field_name, scheme_id;
                END IF;
                
                -- Build aggregation condition with comparison
                dict_condition := CASE dict_agg
                    WHEN 'Count' THEN format(
                        '(SELECT COUNT(*) FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._array_index IS NOT NULL) %s %L::int',
                        table_alias, dict_structure_id, dict_cmp_op, dict_value::int)
                    WHEN 'Sum' THEN format(
                        '(SELECT COALESCE(SUM(dv._Long), 0) FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._array_index IS NOT NULL) %s %L::bigint',
                        table_alias, dict_structure_id, dict_cmp_op, dict_value::bigint)
                    WHEN 'Avg' THEN format(
                        '(SELECT AVG(dv._Long) FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._array_index IS NOT NULL) %s %L::numeric',
                        table_alias, dict_structure_id, dict_cmp_op, dict_value::numeric)
                    WHEN 'Min' THEN format(
                        '(SELECT MIN(dv._Long) FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._array_index IS NOT NULL) %s %L::bigint',
                        table_alias, dict_structure_id, dict_cmp_op, dict_value::bigint)
                    WHEN 'Max' THEN format(
                        '(SELECT MAX(dv._Long) FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._array_index IS NOT NULL) %s %L::bigint',
                        table_alias, dict_structure_id, dict_cmp_op, dict_value::bigint)
                    ELSE 'TRUE'
                END;
                
                IF all_conditions != '' THEN
                    all_conditions := all_conditions || ' AND ' || dict_condition;
                ELSE
                    all_conditions := dict_condition;
                END IF;
            END;
        
        -- Field operators
        ELSE
            -- Parse field path
            field_path := condition_key;
            SELECT * INTO parsed_path FROM _parse_field_path(field_path);
            
            -- Get structure information for all fields
            -- 🎯 FIX: Explicit assignment of fields from TABLE-returning function to RECORD
            SELECT 
                fi.root_structure_id,
                fi.nested_structure_id,
                fi.root_type_info,
                fi.nested_type_info
            INTO 
                structure_info
            FROM _find_structure_info(scheme_id, parsed_path.root_field, parsed_path.nested_field) AS fi
            LIMIT 1;
            
            -- Process field value
            IF jsonb_typeof(condition_value) = 'object' THEN
                -- Complex condition with operators like {"$gt": 100, "$lt": 200}
                FOR operator_name, operator_value IN SELECT key, value FROM jsonb_each_text(condition_value) LOOP
                    inner_condition_sql := _build_inner_condition(
                        operator_name, 
                        operator_value, 
                        CASE 
                            WHEN parsed_path.is_nested THEN structure_info.nested_type_info
                            ELSE structure_info.root_type_info
                        END
                    );
                    
                    -- $ne for specific value (not null) requires NOT EXISTS
                    single_condition := _build_exists_condition(
                        field_path, 
                        inner_condition_sql, 
                        operator_name = '$ne' AND operator_value IS NOT NULL AND operator_value != 'null' AND operator_value != '',
                        scheme_id, 
                        table_alias
                    );
                    
                    -- Accumulate conditions via AND
                    IF all_conditions != '' THEN
                        all_conditions := all_conditions || ' AND ';
                    END IF;
                    all_conditions := all_conditions || ltrim(single_condition, ' AND ');
                END LOOP;
            
            ELSIF jsonb_typeof(condition_value) = 'array' THEN
                -- Array of values - process as $in
                inner_condition_sql := _build_inner_condition(
                    '$in', 
                    condition_value::text,
                    CASE 
                        WHEN parsed_path.is_nested THEN structure_info.nested_type_info
                        ELSE structure_info.root_type_info
                    END
                );
                
                single_condition := _build_exists_condition(field_path, inner_condition_sql, false, scheme_id, table_alias);
                
                -- Accumulate conditions via AND
                IF all_conditions != '' THEN
                    all_conditions := all_conditions || ' AND ';
                END IF;
                all_conditions := all_conditions || ltrim(single_condition, ' AND ');
            
            ELSE
                -- Simple value - equality
                inner_condition_sql := _build_inner_condition(
                    '=', 
                    -- Remove extra quotes from string values
                    CASE 
                        WHEN jsonb_typeof(condition_value) = 'string' THEN condition_value #>> '{}'
                        ELSE condition_value::text 
                    END,
                    CASE 
                        WHEN parsed_path.is_nested THEN structure_info.nested_type_info
                        ELSE structure_info.root_type_info
                    END
                );
                
                single_condition := _build_exists_condition(field_path, inner_condition_sql, false, scheme_id, table_alias);
                
                -- Accumulate conditions via AND
                IF all_conditions != '' THEN
                    all_conditions := all_conditions || ' AND ';
                END IF;
                all_conditions := all_conditions || ltrim(single_condition, ' AND ');
            END IF;
        END IF;
    END LOOP;
    
    -- Return all accumulated conditions
    IF all_conditions != '' THEN
        RETURN ' AND (' || all_conditions || ')';
    END IF;
    RETURN '';
END;
$BODY$;

-- Comments for logical operators
COMMENT ON FUNCTION _build_and_condition(jsonb, bigint, text, integer) IS 'Recursive AND condition builder. Supports nested logical operators and Class fields. Recursion limit: 10 levels.';
COMMENT ON FUNCTION _build_or_condition(jsonb, bigint, text, integer) IS 'Recursive OR condition builder. Combines conditions via OR with proper parenthesis handling. Recursion limit: 10 levels.';
COMMENT ON FUNCTION _build_not_condition(jsonb, bigint, text, integer) IS 'Recursive NOT condition builder. Inverts EXISTS to NOT EXISTS and handles complex conditions. Recursion limit: 10 levels.';
COMMENT ON FUNCTION _build_single_facet_condition(jsonb, bigint, text, integer) IS 'Universal recursive function for processing facet conditions. Supports logical operators ($and, $or, $not), LINQ operators, Class fields and arrays. FIXED: Now correctly processes multiple fields in JSON via condition accumulation, not premature RETURN.';

-- ===== EXTENDED FACETS FUNCTION WITH CLASS FIELDS =====

-- Recursive function to build facet field path (example: "Contact.Name", "Contacts[].Email")  
CREATE OR REPLACE FUNCTION _build_facet_field_path(
    structure_id bigint,
    scheme_id bigint,
    current_path text DEFAULT '',
    max_depth integer DEFAULT 10
) RETURNS text
LANGUAGE 'plpgsql'
COST 50
VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    structure_record RECORD;
    parent_path text;
BEGIN
    -- Recursion depth check
    IF max_depth <= 0 THEN
        RETURN current_path;
    END IF;
    
    -- Get information about current structure (use cache)
    SELECT c._name, c._parent_structure_id, c._collection_type IS NOT NULL as _is_array
    INTO structure_record
    FROM _scheme_metadata_cache c 
    WHERE c._structure_id = structure_id AND c._scheme_id = scheme_id;
    
    -- If structure not found, return current path
    IF NOT FOUND THEN
        RETURN current_path;
    END IF;
    
    -- Form field name considering arrays
    current_path := structure_record._name || 
                   CASE WHEN structure_record._is_array THEN '[]' ELSE '' END ||
                   CASE WHEN current_path != '' THEN '.' || current_path ELSE '' END;
    
    -- If there is a parent, recursively build path
    IF structure_record._parent_structure_id IS NOT NULL THEN
        RETURN _build_facet_field_path(structure_record._parent_structure_id, scheme_id, current_path, max_depth - 1);
    END IF;
    
    -- Return built path
    RETURN current_path;
END;
$BODY$;

-- Function to build extended facets with Class fields
CREATE OR REPLACE FUNCTION get_facets(scheme_id bigint)
RETURNS jsonb 
LANGUAGE 'plpgsql'
COST 150
VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    result_facets jsonb := '{}'::jsonb;
    all_facets jsonb;
    class_facets jsonb;
BEGIN
    -- 🔥 AUTOMATIC CHECK AND CACHE POPULATION
    IF NOT EXISTS(SELECT 1 FROM _scheme_metadata_cache WHERE _scheme_id = scheme_id LIMIT 1) THEN
        PERFORM sync_metadata_cache_for_scheme(scheme_id);
        -- Auto-population without NOTICE (use warmup_all_metadata_caches() for explicit warming)
    END IF;
    
    -- 🚀 STEP 1: Get all basic facets (root fields and simple arrays)
    SELECT jsonb_object_agg(s._name, COALESCE(f.facet_values, '[]'::jsonb))
    INTO all_facets
    FROM _structures s
    LEFT JOIN (
        SELECT 
            v._id_structure, 
            jsonb_agg(DISTINCT 
                CASE 
                    -- Arrays (collections)
                    WHEN st._collection_type IS NOT NULL THEN
                        (
                            SELECT COALESCE(jsonb_agg(
                                CASE 
                                    -- Simple array types
                                    WHEN av_cache.db_type = 'String' THEN to_jsonb(av._String)
                                    WHEN av_cache.db_type = 'Long' AND av_cache.type_semantic != '_RObject' THEN to_jsonb(av._Long)
                                    WHEN av_cache.db_type = 'Guid' AND av_cache.type_semantic != 'Object' THEN to_jsonb(av._Guid)
                                    WHEN av_cache.db_type = 'Double' THEN to_jsonb(av._Double)
                                    WHEN av_cache.db_type = 'Numeric' THEN to_jsonb(av._Numeric)
                                    WHEN av_cache.db_type = 'DateTimeOffset' THEN to_jsonb(av._DateTimeOffset)
                                    WHEN av_cache.db_type = 'Boolean' THEN to_jsonb(av._Boolean)
                                    
                                    -- _RObject arrays - COMMENTED OUT (not needed in facets)
                                    -- WHEN av_cache.db_type = 'Long' AND av_cache.type_semantic = '_RObject' THEN 
                                    --     get_object_json(av._Object, 0)
                                    
                                    WHEN av_cache.db_type = 'ListItem' THEN
                                        (SELECT jsonb_build_object(
                                            'id', li._id,
                                            'value', li._value,
                                            'object', CASE 
                                                WHEN li._id_object IS NOT NULL THEN
                                                    get_object_json(li._id_object, 0)  -- Always base fields
                                                ELSE NULL 
                                            END
                                        )
                                        FROM _list_items li
                                        WHERE li._id = av._listitem)
                                    WHEN av_cache.db_type = 'ByteArray' THEN 
                                        to_jsonb(encode(av._ByteArray, 'base64'))
                                    ELSE to_jsonb(av._String)
                                END ORDER BY av._array_index::int
                            ), '[]'::jsonb)
                            FROM _values av 
                            JOIN _scheme_metadata_cache av_cache ON av_cache._structure_id = av._id_structure
                            WHERE av._id_object = v._id_object 
                              AND av._id_structure = v._id_structure 
                              AND av._array_index IS NOT NULL
                        )
                    
                    -- Regular fields
                    WHEN st.db_type = 'String' THEN to_jsonb(v._String)
                    WHEN st.db_type = 'Long' AND st.type_semantic != '_RObject' THEN to_jsonb(v._Long)
                    WHEN st.db_type = 'Guid' AND st.type_semantic != 'Object' THEN to_jsonb(v._Guid)
                    WHEN st.db_type = 'Double' THEN to_jsonb(v._Double)
                    WHEN st.db_type = 'Numeric' THEN to_jsonb(v._Numeric)
                    WHEN st.db_type = 'DateTimeOffset' THEN to_jsonb(v._DateTimeOffset)
                    WHEN st.db_type = 'Boolean' THEN to_jsonb(v._Boolean)
                    
                    -- _RObject fields - COMMENTED OUT (not needed in facets)
                    -- WHEN st.db_type = 'Long' AND st.type_semantic = '_RObject' THEN 
                    --     CASE 
                    --         WHEN v._Object IS NOT NULL THEN 
                    --             get_object_json(v._Object, 0)
                    --         ELSE NULL
                    --     END
                        
                    WHEN st.db_type = 'ListItem' THEN
                        CASE 
                            WHEN v._listitem IS NOT NULL THEN 
                                (SELECT jsonb_build_object(
                                    'id', li._id,
                                    'value', li._value,
                                    'object', CASE 
                                        WHEN li._id_object IS NOT NULL THEN
                                            get_object_json(li._id_object, 0)  -- Always base fields
                                        ELSE NULL 
                                    END
                                )
                                FROM _list_items li
                                WHERE li._id = v._listitem)
                            ELSE NULL
                        END
                    WHEN st.db_type = 'ByteArray' THEN 
                        CASE 
                            WHEN v._ByteArray IS NOT NULL THEN 
                                to_jsonb(encode(v._ByteArray, 'base64'))
                            ELSE NULL
                        END
                    ELSE to_jsonb(v._String)
                END
            ) FILTER (WHERE 
                CASE 
                    -- Array filtering (collections)
                    WHEN st._collection_type IS NOT NULL THEN 
                        EXISTS(SELECT 1 FROM _values av2 WHERE av2._id_object = v._id_object AND av2._id_structure = v._id_structure AND av2._array_index IS NOT NULL)
                    -- Regular field filtering
                    WHEN st.db_type = 'String' THEN v._String IS NOT NULL
                    WHEN st.db_type = 'Long' AND st.type_semantic != '_RObject' THEN v._Long IS NOT NULL
                    WHEN st.db_type = 'Guid' AND st.type_semantic != 'Object' THEN v._Guid IS NOT NULL
                    WHEN st.db_type = 'Double' THEN v._Double IS NOT NULL
                    WHEN st.db_type = 'DateTimeOffset' THEN v._DateTimeOffset IS NOT NULL
                    WHEN st.db_type = 'Boolean' THEN v._Boolean IS NOT NULL
                    -- _RObject fields excluded from facets (not needed for UI filters)
                    -- WHEN st.db_type = 'Long' AND st.type_semantic = '_RObject' THEN v._Object IS NOT NULL
                    WHEN st.db_type = 'ListItem' THEN v._listitem IS NOT NULL
                    WHEN st.db_type = 'ByteArray' THEN v._ByteArray IS NOT NULL
                    WHEN st.db_type = 'Numeric' THEN v._Numeric IS NOT NULL
                    ELSE FALSE
                END
            ) as facet_values
        FROM _values v
        JOIN _objects o ON o._id = v._id_object
        JOIN _scheme_metadata_cache st ON st._structure_id = v._id_structure
        WHERE o._id_scheme = scheme_id
          AND st._parent_structure_id IS NULL  -- 🔑 Only root fields at this stage
          AND NOT (st.db_type = 'Guid' AND st.type_semantic = 'Object') -- 🔑 Exclude Class fields, they will be processed separately
        GROUP BY v._id_structure
        HAVING COUNT(DISTINCT COALESCE(v._String, v._Long::text, v._Double::text, v._Guid::text)) <= 100  -- 🔑 Limit: max 100 unique values
    ) f ON f._id_structure = s._id
    WHERE s._id_scheme = scheme_id 
      AND s._id_parent IS NULL;  -- 🔑 Only root structures
    
    -- 🚀 STEP 2: Add expanded Class fields (Contact.Name, Contact[].Email)
    SELECT jsonb_object_agg(
        field_path,
        COALESCE(field_values, '[]'::jsonb)
    ) INTO class_facets
    FROM (
        SELECT 
            _build_facet_field_path(nested_s._structure_id, scheme_id) as field_path,
            jsonb_agg(DISTINCT
                CASE 
                    WHEN nested_s._collection_type IS NOT NULL THEN
                        (
                            SELECT COALESCE(jsonb_agg(
                                CASE 
                                    WHEN nested_s.db_type = 'String' THEN to_jsonb(nested_v._String)
                                    WHEN nested_s.db_type = 'Long' AND nested_s.type_semantic != '_RObject' THEN to_jsonb(nested_v._Long)
                                    WHEN nested_s.db_type = 'Double' THEN to_jsonb(nested_v._Double)
                                    WHEN nested_s.db_type = 'Numeric' THEN to_jsonb(nested_v._Numeric)
                                    WHEN nested_s.db_type = 'Boolean' THEN to_jsonb(nested_v._Boolean)
                                    WHEN nested_s.db_type = 'DateTimeOffset' THEN to_jsonb(nested_v._DateTimeOffset)
                                    WHEN nested_s.db_type = 'Guid' AND nested_s.type_semantic != 'Object' THEN to_jsonb(nested_v._Guid)
                                    ELSE to_jsonb(nested_v._String)
                                END ORDER BY nested_v._array_index::int
                            ), '[]'::jsonb)
                            FROM _values nested_v
                            WHERE nested_v._id_object = o._id 
                              AND nested_v._id_structure = nested_s._structure_id
                              AND nested_v._array_index IS NOT NULL
                        )
                    ELSE
                        CASE 
                            WHEN nested_s.db_type = 'String' THEN to_jsonb(nested_v._String)
                            WHEN nested_s.db_type = 'Long' AND nested_s.type_semantic != '_RObject' THEN to_jsonb(nested_v._Long)
                            WHEN nested_s.db_type = 'Double' THEN to_jsonb(nested_v._Double)
                            WHEN nested_s.db_type = 'Numeric' THEN to_jsonb(nested_v._Numeric)
                            WHEN nested_s.db_type = 'Boolean' THEN to_jsonb(nested_v._Boolean)
                            WHEN nested_s.db_type = 'DateTimeOffset' THEN to_jsonb(nested_v._DateTimeOffset)
                            WHEN nested_s.db_type = 'Guid' AND nested_s.type_semantic != 'Object' THEN to_jsonb(nested_v._Guid)
                            ELSE to_jsonb(nested_v._String)
                        END
                END
            ) FILTER (WHERE nested_v._id IS NOT NULL) as field_values
        FROM _objects o
        JOIN _values root_v ON root_v._id_object = o._id AND root_v._array_index IS NULL
        JOIN _scheme_metadata_cache root_s ON root_s._structure_id = root_v._id_structure AND root_s._parent_structure_id IS NULL AND root_s.db_type = 'Guid' AND root_s.type_semantic = 'Object'  -- 🔑 Only Class fields
        JOIN _scheme_metadata_cache nested_s ON nested_s._parent_structure_id = root_s._structure_id  -- 🔑 Nested structures
        LEFT JOIN _values nested_v ON nested_v._id_object = o._id AND nested_v._id_structure = nested_s._structure_id
        WHERE o._id_scheme = scheme_id
        GROUP BY nested_s._structure_id
        HAVING COUNT(nested_v._id) > 0  -- 🔑 Only fields with real values
           AND COUNT(DISTINCT COALESCE(nested_v._String, nested_v._Long::text, nested_v._Double::text)) <= 100  -- 🔑 Limit: max 100 unique values
    ) class_fields
    WHERE field_path IS NOT NULL AND field_path != '';
    
    -- 🚀 STEP 3: Combine basic and Class facets
    result_facets := COALESCE(all_facets, '{}'::jsonb) || COALESCE(class_facets, '{}'::jsonb);
    
    RETURN result_facets;
END;
$BODY$;

-- Comments for extended facets function
COMMENT ON FUNCTION _build_facet_field_path(bigint, bigint, text, integer) IS 'Recursive function for building paths for Class fields in facets. Creates paths like "Contact.Name", "Contacts[].Email", "Address.City" from _structures._id_parent hierarchy. Supports arrays and multi-level nesting.';

COMMENT ON FUNCTION get_facets(bigint) IS '🚀 EXTENDED function for building facets with full Class architecture support:
📋 Basic facets: Name, Status, Tags[] (root fields and simple arrays)
📦 Class facets: Address.City, Address.Street, Contacts[].Type (expanded from _structures._id_parent)  
🔗 Class arrays: Contacts[].Email, Products[].Price (combination of arrays + nesting)
📋 ListItem facets: {id, value, object} where object - base fields of related object
⚡ OPTIMIZATION: 
  - Returns only fields with <= 100 unique values (avoids Article, Description)
  - Object references (_RObject) EXCLUDED from facets (not needed for UI filters)
Two-stage processing: first basic facets, then Class field expansion. Excludes deleted objects.';

-- ===== NEW MODULAR ARCHITECTURE =====

-- ===== FINAL ARCHITECTURE: ABSOLUTE PURITY =====
-- ✅ build_advanced_facet_conditions() - REMOVED
-- ✅ build_base_facet_conditions() - REMOVED 
-- ✅ use_advanced_facets - REMOVED
-- 🚀 REMAINS: ONLY _build_single_facet_condition() as SINGLE ENTRY POINT
-- 💎 PERFECT PURITY WITHOUT A SINGLE EXTRA LINE!

-- Function 1: Building sort conditions
-- 🆕 UPDATED: Support for compact format {"fieldName": "asc"} and base fields with "0$:" prefix
CREATE OR REPLACE FUNCTION build_order_conditions(
    order_by jsonb,
    table_alias text DEFAULT 'o'
) RETURNS text
LANGUAGE 'plpgsql'
COST 50
IMMUTABLE
AS $BODY$
DECLARE
    -- ✅ FIXED: Empty string by default (default ORDER BY _id killed performance!)
    order_conditions text := '';
    order_item jsonb;
    field_name text;
    direction text;
    order_clause text;
    i integer;
    item_key text;
    item_value text;
BEGIN
    -- Process sort parameters
    IF order_by IS NOT NULL AND jsonb_typeof(order_by) = 'array' AND jsonb_array_length(order_by) > 0 THEN
        order_conditions := '';
        
        -- Process each sort element
        FOR i IN 0..jsonb_array_length(order_by) - 1 LOOP
            order_item := order_by->i;
            
            -- 🆕 Support for two formats:
            -- 1. Compact: {"Name": "asc"} or {"0$:name": "desc"}
            -- 2. Expanded: {"field": "Name", "direction": "ASC"}
            IF order_item ? 'field' THEN
                -- Expanded format
                field_name := order_item->>'field';
                direction := UPPER(COALESCE(order_item->>'direction', 'ASC'));
            ELSE
                -- Compact format: first key = field name, value = direction
                SELECT key, value INTO item_key, item_value 
                FROM jsonb_each_text(order_item) LIMIT 1;
                field_name := item_key;
                direction := UPPER(COALESCE(item_value, 'ASC'));
            END IF;
            
            -- Skip incorrect sort elements
            IF field_name IS NOT NULL AND field_name != '' THEN
                
                -- 🆕 RedbObject BASE FIELDS: check for "0$:" prefix
                IF field_name LIKE '0$:%' THEN
                    DECLARE
                        raw_field_name text := substring(field_name from 4);  -- remove '0$:'
                        sql_column text := _normalize_base_field_name(raw_field_name);
                    BEGIN
                        -- Check that field is recognized as base
                        IF sql_column IS NULL THEN
                            RAISE EXCEPTION 'Unknown RedbObject base field for sorting: "%" (passed as "0$:%")', 
                                raw_field_name, raw_field_name;
                        END IF;
                        
                        -- 🚀 DIRECT SORTING by _objects column (faster than subquery to _values!)
                        order_clause := format('%s.%s %s NULLS LAST', table_alias, sql_column, direction);
                    END;
                ELSE
                    -- Props fields: sorting via subquery to _values
                    order_clause := format('(
                        SELECT CASE 
                            WHEN s.type_semantic = ''TimeSpan'' THEN TO_CHAR(v._String::interval, ''HH24:MI:SS'')
                            WHEN v._String IS NOT NULL THEN v._String
                            WHEN v._Long IS NOT NULL THEN LPAD(v._Long::text, 20, ''0'')
                            WHEN v._Double IS NOT NULL THEN LPAD(REPLACE(v._Double::text, ''.'', ''~''), 25, ''0'')
                            WHEN v._Numeric IS NOT NULL THEN LPAD(REPLACE(v._Numeric::text, ''.'', ''~''), 30, ''0'')
                            WHEN v._DateTimeOffset IS NOT NULL THEN TO_CHAR(v._DateTimeOffset, ''YYYY-MM-DD HH24:MI:SS.US'')
                            WHEN v._Boolean IS NOT NULL THEN v._Boolean::text
                            ELSE NULL
                        END
                        FROM _values v 
                        JOIN _scheme_metadata_cache s ON v._id_structure = s._structure_id 
                        WHERE v._id_object = %s._id AND s._name = %L
                          AND v._array_index IS NULL  -- exclude array elements
                        LIMIT 1
                    ) %s NULLS LAST', table_alias, field_name, direction);
                END IF;
                
                -- Add comma if conditions already exist
                IF order_conditions != '' THEN
                    order_conditions := order_conditions || ', ';
                END IF;
                order_conditions := order_conditions || order_clause;
            END IF;
        END LOOP;
        
        -- Form final ORDER BY
        IF order_conditions != '' THEN
            -- ✅ Add _id only for pagination stability with explicit sorting
            order_conditions := 'ORDER BY ' || order_conditions || format(', %s._id', table_alias);
        ELSE
            -- ✅ FIXED: No default sorting - otherwise kills performance for large CTEs!
            order_conditions := '';
        END IF;
    END IF;
    
    RETURN order_conditions;
END;
$BODY$;

-- Comment for sort function
COMMENT ON FUNCTION build_order_conditions(jsonb, text) IS '🆕 Builds ORDER BY conditions with support for:
- Compact format: [{"Name": "asc"}, {"0$:date_create": "desc"}]
- Expanded format: [{"field": "Name", "direction": "ASC"}]
- 🚀 RedbObject base fields (0$: prefix): direct sorting by _objects (faster!)
- Props fields: sorting via _values with correct type handling
- Cascading sorts: OrderBy().ThenByRedb().ThenBy()';

-- Function 2: Building hierarchical conditions
CREATE OR REPLACE FUNCTION build_has_ancestor_condition(
    ancestor_filter jsonb,
    table_alias text DEFAULT 'o'
) RETURNS text
LANGUAGE 'plpgsql'
COST 50
IMMUTABLE
AS $BODY$
DECLARE
    condition_json jsonb;
    target_scheme_id bigint;
    max_depth_value integer;
    condition_sql text := '';
    depth_limit_sql text := '';
    scheme_filter_sql text := '';
BEGIN
    -- Extract parameters from JSON
    condition_json := ancestor_filter->'condition';
    target_scheme_id := (ancestor_filter->>'scheme_id')::bigint;
    max_depth_value := (ancestor_filter->>'max_depth')::integer;
    
    -- Form SQL for depth limitation
    IF max_depth_value IS NOT NULL THEN
        depth_limit_sql := format(' AND ancestors.level <= %s', max_depth_value);
    ELSE
        depth_limit_sql := ' AND ancestors.level < 50';
    END IF;
    
    -- Form SQL for scheme_id filtering
    IF target_scheme_id IS NOT NULL THEN
        scheme_filter_sql := format(' AND anc_obj._id_scheme = %s', target_scheme_id);
    END IF;
    
    -- Form SQL for Props condition
    IF condition_json IS NOT NULL AND jsonb_typeof(condition_json) = 'object' THEN
        -- Use _build_single_facet_condition to build WHERE conditions
        IF target_scheme_id IS NOT NULL THEN
            condition_sql := format(' AND %s', _build_single_facet_condition(condition_json, target_scheme_id, 'anc_obj'));
        END IF;
    END IF;
    
    RETURN format(
        ' AND EXISTS (
            WITH RECURSIVE ancestors AS (
                SELECT %s._id_parent as parent_id, 1 as level
                FROM _objects dummy WHERE dummy._id = %s._id
                UNION ALL
                SELECT o._id_parent, ancestors.level + 1
                FROM _objects o
                JOIN ancestors ON o._id = ancestors.parent_id
                WHERE true %s
            )
            SELECT 1 FROM ancestors
            JOIN _objects anc_obj ON anc_obj._id = ancestors.parent_id
            WHERE true %s %s
        )', 
        table_alias, table_alias, depth_limit_sql, scheme_filter_sql, condition_sql
    );
END;
$BODY$;

CREATE OR REPLACE FUNCTION build_has_descendant_condition(
    descendant_filter jsonb,
    table_alias text DEFAULT 'o'  
) RETURNS text
LANGUAGE 'plpgsql'
COST 50
IMMUTABLE
AS $BODY$
DECLARE
    condition_json jsonb;
    target_scheme_id bigint;
    max_depth_value integer;
    condition_sql text := '';
    depth_limit_sql text := '';
    scheme_filter_sql text := '';
BEGIN
    -- Extract parameters from JSON
    condition_json := descendant_filter->'condition';
    target_scheme_id := (descendant_filter->>'scheme_id')::bigint;
    max_depth_value := (descendant_filter->>'max_depth')::integer;
    
    -- Form SQL for depth limitation
    IF max_depth_value IS NOT NULL THEN
        depth_limit_sql := format(' AND descendants.level <= %s', max_depth_value);
    ELSE
        depth_limit_sql := ' AND descendants.level < 50';
    END IF;
    
    -- Form SQL for scheme_id filtering
    IF target_scheme_id IS NOT NULL THEN
        scheme_filter_sql := format(' AND desc_obj._id_scheme = %s', target_scheme_id);
    END IF;
    
    -- Form SQL for Props condition
    IF condition_json IS NOT NULL AND jsonb_typeof(condition_json) = 'object' THEN
        -- Use _build_single_facet_condition to build WHERE conditions
        IF target_scheme_id IS NOT NULL THEN
            condition_sql := format(' AND %s', _build_single_facet_condition(condition_json, target_scheme_id, 'desc_obj'));
        END IF;
    END IF;
    
    RETURN format(
        ' AND EXISTS (
            WITH RECURSIVE descendants AS (
                SELECT %s._id as parent_id, 1 as level
                UNION ALL
                SELECT o._id, descendants.level + 1
                FROM _objects o
                JOIN descendants ON o._id_parent = descendants.parent_id
                WHERE true %s
            )
            SELECT 1 FROM descendants
            JOIN _objects desc_obj ON desc_obj._id = descendants.parent_id
            WHERE desc_obj._id != %s._id %s %s
        )', 
        table_alias, depth_limit_sql, table_alias, scheme_filter_sql, condition_sql
    );
END;
$BODY$;

CREATE OR REPLACE FUNCTION build_level_condition(
    target_level integer,
    table_alias text DEFAULT 'o'
) RETURNS text
LANGUAGE 'plpgsql'
COST 50
IMMUTABLE
AS $BODY$
BEGIN
    -- ✅ FIX: Calculate level going UP from object to root
    -- Level 0 = root (where _id_parent IS NULL)
    -- Level 1 = direct child of root
    -- etc.
    RETURN format(
        ' AND (
            SELECT COUNT(*)::integer FROM (
                WITH RECURSIVE ancestors AS (
                    SELECT %s._id_parent as parent_id
                    UNION ALL
                    SELECT o._id_parent
                    FROM _objects o
                    JOIN ancestors ON o._id = ancestors.parent_id
                    WHERE o._id_parent IS NOT NULL
                )
                SELECT parent_id FROM ancestors WHERE parent_id IS NOT NULL
            ) AS a
        ) = %s', 
        table_alias, target_level
    );
END;
$BODY$;

-- ✅ NEW FUNCTION: Support for comparison operators for levels
CREATE OR REPLACE FUNCTION build_level_condition_with_operators(
    level_operators jsonb,
    table_alias text DEFAULT 'o'
) RETURNS text
LANGUAGE 'plpgsql'
COST 50
IMMUTABLE
AS $BODY$
DECLARE
    operator_name text;
    operator_value text;
    level_condition text := '';
    op_symbol text;
BEGIN
    -- Process each operator in JSON object
    FOR operator_name, operator_value IN SELECT key, value FROM jsonb_each_text(level_operators) LOOP
        
        -- Determine SQL operator
        CASE operator_name
            WHEN '$gt' THEN op_symbol := '>';
            WHEN '$gte' THEN op_symbol := '>=';
            WHEN '$lt' THEN op_symbol := '<';
            WHEN '$lte' THEN op_symbol := '<=';
            WHEN '$eq' THEN op_symbol := '=';
            WHEN '$ne' THEN op_symbol := '!=';
            ELSE 
                CONTINUE; -- Skip unknown operators
        END CASE;
        
        -- Form condition for current operator
        IF level_condition != '' THEN
            level_condition := level_condition || ' AND ';
        END IF;
        
        -- ✅ FIX: Calculate level going UP from object to root
        level_condition := level_condition || format(
            '(
                SELECT COUNT(*)::integer FROM (
                    WITH RECURSIVE ancestors AS (
                        SELECT %s._id_parent as parent_id
                        UNION ALL
                        SELECT o._id_parent
                        FROM _objects o
                        JOIN ancestors ON o._id = ancestors.parent_id
                        WHERE o._id_parent IS NOT NULL
                    )
                    SELECT parent_id FROM ancestors WHERE parent_id IS NOT NULL
                ) AS a
            ) %s %s',
            table_alias, op_symbol, operator_value
        );
    END LOOP;
    
    -- Return full condition with AND prefix
    IF level_condition != '' THEN
        RETURN ' AND (' || level_condition || ')';
    END IF;
    
    RETURN '';
END;
$BODY$;

-- Function to combine hierarchical conditions
CREATE OR REPLACE FUNCTION build_hierarchical_conditions(
    facet_filters jsonb,
    table_alias text DEFAULT 'o'
) RETURNS text
LANGUAGE 'plpgsql'
COST 50
VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    where_conditions text := '';
    ancestor_id bigint;
    descendant_id bigint;
    target_level integer;
BEGIN
    IF facet_filters IS NOT NULL AND jsonb_typeof(facet_filters) = 'object' THEN
        -- $hasAncestor: Polymorphic ancestor search with condition, scheme_id and max_depth
        IF facet_filters ? '$hasAncestor' THEN
            where_conditions := where_conditions || build_has_ancestor_condition(facet_filters->'$hasAncestor', table_alias);
        END IF;
        
        -- $hasDescendant: Polymorphic descendant search with condition, scheme_id and max_depth
        IF facet_filters ? '$hasDescendant' THEN
            where_conditions := where_conditions || build_has_descendant_condition(facet_filters->'$hasDescendant', table_alias);
        END IF;
        
        -- $level: Support for comparison operators {"$gt": 2}, {"$eq": 3} etc.
        IF facet_filters ? '$level' THEN
            -- ✅ FIX: Processing JSON operators for $level
            IF jsonb_typeof(facet_filters->'$level') = 'object' THEN
                -- Complex condition with operators like {"$gt": 2}, {"$lt": 5}
                where_conditions := where_conditions || build_level_condition_with_operators(facet_filters->'$level', table_alias);
            ELSE
                -- Simple value - exact equality
                target_level := (facet_filters->>'$level')::integer;
                where_conditions := where_conditions || build_level_condition(target_level, table_alias);
            END IF;
        END IF;
        
        -- $isRoot
        IF facet_filters ? '$isRoot' AND (facet_filters->>'$isRoot')::boolean THEN
            where_conditions := where_conditions || format(' AND %s._id_parent IS NULL', table_alias);
        END IF;
        
        -- $isLeaf  
        IF facet_filters ? '$isLeaf' AND (facet_filters->>'$isLeaf')::boolean THEN
            where_conditions := where_conditions || format(
                ' AND NOT EXISTS (SELECT 1 FROM _objects child WHERE child._id_parent = %s._id)', 
                table_alias
            );
        END IF;
        
        -- $childrenOf - direct children of specified parent
        IF facet_filters ? '$childrenOf' THEN
            where_conditions := where_conditions || format(
                ' AND %s._id_parent = %s', 
                table_alias,
                (facet_filters->>'$childrenOf')::bigint
            );
        END IF;
    END IF;
    
    RETURN where_conditions;
END;
$BODY$;

-- Comment for hierarchical conditions
COMMENT ON FUNCTION build_hierarchical_conditions(jsonb, text) IS 'Builds WHERE conditions for hierarchical filters: $hasAncestor, $hasDescendant, $level, $isRoot, $isLeaf, $childrenOf. Uses recursive CTEs for efficient search in object hierarchy. Recursion depth limit: 50 levels.';

-- Function 3: Execute query and return result
-- ✅ DISTINCT ON (_hash) for Open Source version Distinct()
DROP FUNCTION IF EXISTS execute_objects_query(bigint, text, text, text, integer, integer, boolean) CASCADE;
DROP FUNCTION IF EXISTS execute_objects_query(bigint, text, text, text, integer, integer, boolean, boolean) CASCADE;

CREATE OR REPLACE FUNCTION execute_objects_query(
    scheme_id bigint,
    base_conditions text,
    hierarchical_conditions text,
    order_conditions text,
    limit_count integer DEFAULT NULL,
    offset_count integer DEFAULT 0,
    include_facets boolean DEFAULT false,
    distinct_hash boolean DEFAULT false  -- ✅ NEW: DISTINCT ON (_hash)
) RETURNS jsonb
LANGUAGE 'plpgsql'
COST 200
VOLATILE NOT LEAKPROOF  
AS $BODY$
DECLARE
    query_text text;
    count_query_text text;
    objects_result jsonb;
    total_count integer;
    final_where text;
    distinct_clause text;
    order_for_distinct text;
BEGIN
    -- Combine all conditions
    final_where := format('WHERE o._id_scheme = %s%s%s', 
                         scheme_id, 
                         COALESCE(base_conditions, ''),
                         COALESCE(hierarchical_conditions, ''));
    
    -- ✅ DISTINCT ON for uniqueness by Hash (Props)
    IF distinct_hash THEN
        distinct_clause := 'DISTINCT ON (o._hash)';
        -- PostgreSQL requires: ORDER BY must start with DISTINCT ON expression
        order_for_distinct := 'ORDER BY o._hash' || 
            CASE WHEN order_conditions IS NOT NULL AND order_conditions != '' 
                 THEN ', ' || regexp_replace(order_conditions, '^ORDER BY\s*', '', 'i')
                 ELSE '' 
            END;
    ELSE
        distinct_clause := '';
        order_for_distinct := COALESCE(order_conditions, 'ORDER BY o._id');
    END IF;
    
    -- ✅ FIX: Build main query with NULL limit handling
    query_text := format('
        SELECT jsonb_agg(get_object_json(sub._id, 10))
        FROM (
            SELECT %s o._id
            FROM _objects o
            %s
            %s
            %s
        ) sub',
        distinct_clause,
        final_where,
        order_for_distinct,
        CASE 
            WHEN limit_count IS NULL OR limit_count >= 2000000000 THEN ''  -- ✅ NO LIMIT if not specified or very large
            ELSE format('LIMIT %s OFFSET %s', limit_count, offset_count)
        END
    );
    
    -- Build count query
    -- ✅ With DISTINCT count unique hash values
    IF distinct_hash THEN
        count_query_text := format('
            SELECT COUNT(DISTINCT o._hash)
            FROM _objects o  
            %s',
            final_where
        );
    ELSE
        count_query_text := format('
            SELECT COUNT(*)
            FROM _objects o  
            %s',
            final_where
        );
    END IF;
    
    -- Execute queries
    EXECUTE query_text INTO objects_result;
    EXECUTE count_query_text INTO total_count;
    
    -- Form result
    RETURN jsonb_build_object(
        'objects', COALESCE(objects_result, '[]'::jsonb),
        'total_count', total_count,
        'limit', limit_count,
        'offset', offset_count,
        'facets', CASE 
            WHEN include_facets THEN get_facets(scheme_id)
            ELSE '{}'::jsonb  -- empty object for speed
        END
    );
END;
$BODY$;

-- Comment for query execution function
COMMENT ON FUNCTION execute_objects_query(bigint, text, text, text, integer, integer, boolean, boolean) IS 'Executes object search with built conditions and returns standardized result with objects, metadata and optional facets. include_facets (DEFAULT false) - disabling heavy facets for speed on large schemes (10,000+ objects). ✅ distinct_hash=true adds DISTINCT ON (_hash) for uniqueness by Props.';

-- Main function for faceted object search with purest architecture
-- ✅ DISTINCT ON (_hash) for Open Source version Distinct()
DROP FUNCTION IF EXISTS search_objects_with_facets(bigint, jsonb, integer, integer, jsonb, integer, boolean) CASCADE;
DROP FUNCTION IF EXISTS search_objects_with_facets(bigint, jsonb, integer, integer, jsonb, integer, boolean, boolean) CASCADE;

CREATE OR REPLACE FUNCTION search_objects_with_facets(
    scheme_id bigint,
    facet_filters jsonb DEFAULT NULL,
    limit_count integer DEFAULT NULL,
    offset_count integer DEFAULT 0,
    order_by jsonb DEFAULT NULL,
    max_recursion_depth integer DEFAULT 10,
    include_facets boolean DEFAULT false,
    distinct_hash boolean DEFAULT false  -- ✅ NEW: DISTINCT ON (_hash)
) RETURNS jsonb
LANGUAGE 'plpgsql'
COST 200
VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    base_conditions text;
    hierarchical_conditions text;
    order_conditions text;
BEGIN
    -- 🚀 FINAL PURITY: ONLY _build_single_facet_condition() - NO DEAD CODE!
    base_conditions := _build_single_facet_condition(facet_filters, scheme_id, 'o', max_recursion_depth);
    
    -- Build hierarchical and sort conditions (unchanged)
    hierarchical_conditions := build_hierarchical_conditions(facet_filters, 'o');
    order_conditions := build_order_conditions(order_by, 'o');
    
    -- Execute search
    RETURN execute_objects_query(
        scheme_id,
        base_conditions,
        hierarchical_conditions,
        order_conditions,
        limit_count,
        offset_count,
        include_facets,
        distinct_hash  -- ✅ Pass parameter
    );
END;
$BODY$;

-- Comment for main search function with new capabilities
COMMENT ON FUNCTION search_objects_with_facets(bigint, jsonb, integer, integer, jsonb, integer, boolean, boolean) IS '🚀 FINAL PURITY: Absolutely clean architecture NO DEAD CODE! Direct call to _build_single_facet_condition() as SINGLE entry point. NO legacy functions, NO use_advanced_facets, NO dead branches! Supports logical operators ($and, $or, $not), 25+ LINQ operators ($gt, $contains, $arrayContains, etc.), Class fields (Contact.Name), Class arrays (Contacts[].Email). 🆕 max_recursion_depth for complex queries (DEFAULT 10). 🆕 include_facets (DEFAULT false) - disabling heavy facets for speed on large schemes. ✅ distinct_hash=true adds DISTINCT ON (_hash) for uniqueness by Props.';

-- Function for hierarchical search (object children) with SUPPORT FOR NEW LINQ PARADIGM
CREATE OR REPLACE FUNCTION search_tree_objects_with_facets(
    scheme_id bigint,
    parent_ids bigint[],  -- ✅ BATCH: Array of parents for optimization (was: parent_id bigint)
    facet_filters jsonb DEFAULT NULL,
    limit_count integer DEFAULT NULL,
    offset_count integer DEFAULT 0,
    order_by jsonb DEFAULT NULL,
    max_depth integer DEFAULT 10,
    max_recursion_depth integer DEFAULT 10,
    include_facets boolean DEFAULT false  -- ⭐ NEW: facets disabled by default
) RETURNS jsonb
LANGUAGE 'plpgsql'
COST 300
VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    query_text text;
    count_query_text text;
    objects_result jsonb;
    total_count integer;
    base_conditions text;
    hierarchical_conditions text;  -- ✅ FIX: added for $level, $isRoot, $isLeaf
    order_conditions text;
BEGIN
    -- 🔥 AUTOMATIC CHECK AND CACHE POPULATION
    -- Guarantees that scheme metadata cache is populated before building conditions
    IF NOT EXISTS(SELECT 1 FROM _scheme_metadata_cache WHERE _scheme_id = scheme_id LIMIT 1) THEN
        PERFORM sync_metadata_cache_for_scheme(scheme_id);
        -- Auto-population without NOTICE (use warmup_all_metadata_caches() for explicit warming)
    END IF;
    
    -- 🚀 VARIANT C: PUREST ARCHITECTURE - direct call to universal system
    base_conditions := _build_single_facet_condition(facet_filters, scheme_id, 'd', max_recursion_depth);
    hierarchical_conditions := build_hierarchical_conditions(facet_filters, 'd');  -- ✅ FIX: processing $level, $isRoot, $isLeaf
    order_conditions := build_order_conditions(order_by, 'd');
    
    -- ✅ FIX: When parent_ids empty - search ENTIRE _objects table (without CTE)
    -- Needed for TreeQuery<T>().WhereLeaves() and similar queries without rootId
    IF parent_ids IS NULL OR array_length(parent_ids, 1) IS NULL OR array_length(parent_ids, 1) = 0 THEN
        query_text := format('
            SELECT jsonb_agg(get_object_json(sub._id, 10))
            FROM (
                SELECT d._id
                FROM _objects d
                WHERE d._id_scheme = %s%s%s
                %s
                %s
            ) sub',
            scheme_id,
            COALESCE(base_conditions, ''),
            COALESCE(hierarchical_conditions, ''),
            order_conditions,
            CASE 
                WHEN limit_count IS NOT NULL THEN format('LIMIT %s OFFSET %s', limit_count, offset_count)
                ELSE ''
            END
        );
        
        count_query_text := format('
            SELECT COUNT(*)
            FROM _objects d
            WHERE d._id_scheme = %s%s%s',
            scheme_id,
            COALESCE(base_conditions, ''),
            COALESCE(hierarchical_conditions, '')
        );
        
        -- Execute queries WITHOUT USING (no $1 parameter)
        EXECUTE query_text INTO objects_result;
        EXECUTE count_query_text INTO total_count;
        
        RETURN jsonb_build_object(
            'objects', COALESCE(objects_result, '[]'::jsonb),
            'total_count', total_count,
            'limit', limit_count,
            'offset', offset_count,
            'parent_ids', parent_ids,
            'max_depth', max_depth,
            'facets', CASE 
                WHEN include_facets THEN get_facets(scheme_id)
                ELSE '{}'::jsonb
            END
        );
    END IF;
    
    -- If max_depth = 1, search only direct children
    IF max_depth = 1 THEN
        query_text := format('
            SELECT jsonb_agg(get_object_json(sub._id, 10))
            FROM (
                SELECT d._id
                FROM _objects d
                WHERE d._id_scheme = %s 
                  AND d._id_parent = ANY($1)%s%s
                %s
                %s
            ) sub',
            scheme_id,
            COALESCE(base_conditions, ''),
            COALESCE(hierarchical_conditions, ''),  -- ✅ FIX: added $level, $isRoot, $isLeaf
            order_conditions,
            CASE 
                WHEN limit_count IS NOT NULL THEN format('LIMIT %s OFFSET %s', limit_count, offset_count)
                ELSE ''  -- ✅ NO LIMIT if not explicitly specified
            END
        );
        
        count_query_text := format('
            SELECT COUNT(*)
            FROM _objects d
            WHERE d._id_scheme = %s 
              AND d._id_parent = ANY($1)%s%s',
            scheme_id,
            COALESCE(base_conditions, ''),
            COALESCE(hierarchical_conditions, '')  -- ✅ FIX: added $level, $isRoot, $isLeaf
        );
    ELSE
        -- Recursive descendant search
        -- 🔥 FIXED: Removed DISTINCT for compatibility with ORDER BY
        -- In tree duplicates impossible (each object has one parent)
        query_text := format('
            WITH RECURSIVE descendants AS (
                SELECT unnest($1) as _id, 0::bigint as depth
                UNION ALL
                SELECT o._id, d.depth + 1
                FROM _objects o
                JOIN descendants d ON o._id_parent = d._id
                WHERE d.depth < %s
            )
            SELECT jsonb_agg(get_object_json(sub._id, 10))
            FROM (
                SELECT d._id
                FROM descendants dt
                JOIN _objects d ON dt._id = d._id
                WHERE dt.depth > 0 
                  AND d._id_scheme = %s%s%s
                %s
                %s
            ) sub',
            max_depth,
            scheme_id,
            COALESCE(base_conditions, ''),
            COALESCE(hierarchical_conditions, ''),  -- ✅ FIX: added $level, $isRoot, $isLeaf
            order_conditions,
            CASE 
                WHEN limit_count IS NOT NULL THEN format('LIMIT %s OFFSET %s', limit_count, offset_count)
                ELSE ''  -- ✅ NO LIMIT if not explicitly specified
            END
        );
        
        count_query_text := format('
            WITH RECURSIVE descendants AS (
                SELECT unnest($1) as _id, 0::bigint as depth
                UNION ALL
                SELECT o._id, d.depth + 1
                FROM _objects o
                JOIN descendants d ON o._id_parent = d._id
                WHERE d.depth < %s
            )
            SELECT COUNT(DISTINCT d._id)
            FROM descendants dt
            JOIN _objects d ON dt._id = d._id
            WHERE dt.depth > 0 
              AND d._id_scheme = %s%s%s',
            max_depth,
            scheme_id,
            COALESCE(base_conditions, ''),
            COALESCE(hierarchical_conditions, '')  -- ✅ FIX: added $level, $isRoot, $isLeaf
        );
    END IF;
    
    -- Execute queries with USING for array passing!
    EXECUTE query_text INTO objects_result USING parent_ids;
    EXECUTE count_query_text INTO total_count USING parent_ids;
    
    -- Form result
    RETURN jsonb_build_object(
        'objects', COALESCE(objects_result, '[]'::jsonb),
        'total_count', total_count,
        'limit', limit_count,
        'offset', offset_count,
        'parent_ids', parent_ids,  -- ✅ BATCH: Array of parents
        'max_depth', max_depth,
        'facets', CASE 
            WHEN include_facets THEN get_facets(scheme_id)
            ELSE '{}'::jsonb  -- empty object for speed
        END
    );
END;
$BODY$;

-- Comment for tree search function
COMMENT ON FUNCTION search_tree_objects_with_facets(bigint, bigint[], jsonb, integer, integer, jsonb, integer, integer, boolean) IS '🚀 BATCH OPTIMIZATION! Accepts parent_ids[] array for 3-4x speedup. Direct call to _build_single_facet_condition() for tree queries. NO build_advanced_facet_conditions() - MAXIMUM PURITY! Supports:
📊 Logical operators: $and, $or, $not
🔍 LINQ operators: $gt, $contains, $arrayContains, $arrayAny, etc.
📦 Class fields: Contact.Name, Address.City
🔗 Class arrays: Contacts[].Email, Products[].Price  
🌳 Hierarchical conditions: direct children search (max_depth=1) and recursive descendant search
🆕 max_recursion_depth for complex queries (DEFAULT 10)
🆕 include_facets (DEFAULT false) - disabling heavy facets for speed on large schemes. SINGLE entry point!';

-- ===== SQL PREVIEW FUNCTIONS (for debugging) =====

-- Function 1: Preview for standard search
-- ✅ DISTINCT ON (_hash) for Open Source version Distinct()
DROP FUNCTION IF EXISTS get_search_sql_preview(bigint, jsonb, integer, integer, jsonb, integer) CASCADE;
DROP FUNCTION IF EXISTS get_search_sql_preview(bigint, jsonb, integer, integer, jsonb, integer, boolean) CASCADE;

CREATE OR REPLACE FUNCTION get_search_sql_preview(
    scheme_id bigint,
    facet_filters jsonb DEFAULT NULL,
    limit_count integer DEFAULT NULL,
    offset_count integer DEFAULT 0,
    order_by jsonb DEFAULT NULL,
    max_recursion_depth integer DEFAULT 10,
    distinct_hash boolean DEFAULT false  -- ✅ NEW: DISTINCT ON (_hash)
) RETURNS text
LANGUAGE 'plpgsql'
COST 50
IMMUTABLE
AS $BODY$
DECLARE
    base_conditions text;
    hierarchical_conditions text;
    order_conditions text;
    final_where text;
    query_text text;
    distinct_clause text;
    order_for_distinct text;
BEGIN
    -- Reuse condition building functions
    base_conditions := _build_single_facet_condition(facet_filters, scheme_id, 'o', max_recursion_depth);
    hierarchical_conditions := build_hierarchical_conditions(facet_filters, 'o');
    order_conditions := build_order_conditions(order_by, 'o');
    
    -- Combine WHERE
    final_where := format('WHERE o._id_scheme = %s%s%s', 
                         scheme_id, 
                         COALESCE(base_conditions, ''),
                         COALESCE(hierarchical_conditions, ''));
    
    -- ✅ DISTINCT ON for uniqueness by Hash (Props)
    IF distinct_hash THEN
        distinct_clause := 'DISTINCT ON (o._hash)';
        order_for_distinct := 'ORDER BY o._hash' || 
            CASE WHEN order_conditions IS NOT NULL AND order_conditions != '' 
                 THEN ', ' || regexp_replace(order_conditions, '^ORDER BY\s*', '', 'i')
                 ELSE '' 
            END;
    ELSE
        distinct_clause := '';
        order_for_distinct := COALESCE(order_conditions, 'ORDER BY o._id');
    END IF;
    
    -- Build SQL (DO NOT EXECUTE!)
    query_text := format('
SELECT jsonb_agg(get_object_json(sub._id, 10))
FROM (
    SELECT %s o._id
    FROM _objects o
    %s
    %s
    %s
) sub',
        distinct_clause,
        final_where,
        order_for_distinct,
        CASE 
            WHEN limit_count IS NULL OR limit_count >= 2000000000 THEN ''
            ELSE format('LIMIT %s OFFSET %s', limit_count, offset_count)
        END
    );
    
    RETURN query_text;
END;
$BODY$;

COMMENT ON FUNCTION get_search_sql_preview(bigint, jsonb, integer, integer, jsonb, integer, boolean) IS 
'Returns SQL query for debugging. Shows what will be executed in search_objects_with_facets(). ✅ distinct_hash=true adds DISTINCT ON (_hash). Used in ToSqlStringAsync() to view final SQL without execution.';

-- Function 2: Preview for tree search
CREATE OR REPLACE FUNCTION get_search_tree_sql_preview(
    scheme_id bigint,
    parent_ids bigint[],  -- ✅ BATCH: Array of parents (was: parent_id bigint)
    facet_filters jsonb DEFAULT NULL,
    limit_count integer DEFAULT NULL,
    offset_count integer DEFAULT 0,
    order_by jsonb DEFAULT NULL,
    max_depth integer DEFAULT 10,
    max_recursion_depth integer DEFAULT 10
) RETURNS text
LANGUAGE 'plpgsql'
COST 100
IMMUTABLE
AS $BODY$
DECLARE
    query_text text;
    base_conditions text;
    hierarchical_conditions text;  -- ✅ FIX: added for $level, $isRoot, $isLeaf
    order_conditions text;
BEGIN
    -- Reuse condition building functions
    base_conditions := _build_single_facet_condition(facet_filters, scheme_id, 'd', max_recursion_depth);
    hierarchical_conditions := build_hierarchical_conditions(facet_filters, 'd');  -- ✅ FIX: processing $level, $isRoot, $isLeaf
    order_conditions := build_order_conditions(order_by, 'd');
    
    -- ✅ FIX: When parent_ids empty - show query without CTE
    IF parent_ids IS NULL OR array_length(parent_ids, 1) IS NULL OR array_length(parent_ids, 1) = 0 THEN
        query_text := format('
SELECT jsonb_agg(get_object_json(sub._id, 10))
FROM (
    SELECT d._id
    FROM _objects d
    WHERE d._id_scheme = %s%s%s
    %s
    %s
) sub',
            scheme_id,
            COALESCE(base_conditions, ''),
            COALESCE(hierarchical_conditions, ''),
            order_conditions,
            CASE 
                WHEN limit_count IS NOT NULL THEN format('LIMIT %s OFFSET %s', limit_count, offset_count)
                ELSE ''
            END
        );
        RETURN query_text;
    END IF;
    
    -- If max_depth = 1, search only direct children
    IF max_depth = 1 THEN
        query_text := format('
SELECT jsonb_agg(get_object_json(sub._id, 10))
FROM (
    SELECT d._id
    FROM _objects d
    WHERE d._id_scheme = %s 
      AND d._id_parent = ANY(%L)%s%s
    %s
    %s
) sub',
            scheme_id,
            parent_ids,  -- ✅ BATCH: Array of parents
            COALESCE(base_conditions, ''),
            COALESCE(hierarchical_conditions, ''),  -- ✅ FIX: added $level, $isRoot, $isLeaf
            order_conditions,
            CASE 
                WHEN limit_count IS NOT NULL THEN format('LIMIT %s OFFSET %s', limit_count, offset_count)
                ELSE ''
            END
        );
    ELSE
        -- Recursive descendant search
        -- 🔥 FIXED: Removed DISTINCT for compatibility with ORDER BY
        query_text := format('
WITH RECURSIVE descendants AS (
    SELECT unnest(%L) as _id, 0::bigint as depth
    UNION ALL
    SELECT o._id, d.depth + 1
    FROM _objects o
    JOIN descendants d ON o._id_parent = d._id
    WHERE d.depth < %s
)
SELECT jsonb_agg(get_object_json(sub._id, 10))
FROM (
    SELECT d._id
    FROM descendants dt
    JOIN _objects d ON dt._id = d._id
    WHERE dt.depth > 0 
      AND d._id_scheme = %s%s%s
    %s
    %s
) sub',
            parent_ids,  -- ✅ BATCH: Array of parents
            max_depth,
            scheme_id,
            COALESCE(base_conditions, ''),
            COALESCE(hierarchical_conditions, ''),  -- ✅ FIX: added $level, $isRoot, $isLeaf
            order_conditions,
            CASE 
                WHEN limit_count IS NOT NULL THEN format('LIMIT %s OFFSET %s', limit_count, offset_count)
                ELSE ''
            END
        );
    END IF;
    
    RETURN query_text;
END;
$BODY$;

COMMENT ON FUNCTION get_search_tree_sql_preview(bigint, bigint[], jsonb, integer, integer, jsonb, integer, integer) IS 
'Returns SQL query for tree search (for debugging). Shows what will be executed in search_tree_objects_with_facets(). Supports direct children search (max_depth=1) and recursive descendant search. 🔥 No DISTINCT for compatibility with ORDER BY.';

-- ===== EXAMPLES OF NEW ARCHITECTURE USAGE =====
/*
-- 🚀 UPDATED CAPABILITIES WITH OPTIMIZED EAV SEMANTICS:

-- 🎯 NEW NULL SEMANTICS:
-- = null now searches for MISSING fields (NOT records with NULL values)
SELECT search_objects_with_facets(
    9001, 
    '{"OptionalField": null}'::jsonb  -- will find objects WITHOUT this field in _values
);

-- $ne null now searches for fields with REAL non-NULL values  
SELECT search_objects_with_facets(
    9001,
    '{"Name": {"$ne": null}}'::jsonb  -- will find objects where Name is actually filled
);

-- 🎯 NEW $exists OPERATOR:
-- Explicit field existence control
SELECT search_objects_with_facets(
    9001,
    '{
        "RequiredField": {"$exists": true},    -- field MUST exist
        "OptionalField": {"$exists": false}    -- field must NOT exist
    }'::jsonb
);

-- 🚀 NEW CAPABILITIES:

-- 1. Logical operators:
SELECT search_objects_with_facets(
    1002, 
    '{
        "$and": [
            {"Status": "Active"}, 
            {"$or": [{"Priority": "High"}, {"Urgent": true}]}
        ]
    }'::jsonb,
    10, 0, NULL
);

-- 2. LINQ operators:
SELECT search_objects_with_facets(
    1002,
    '{
        "Price": {"$gt": "100", "$lt": "500"},
        "Title": {"$contains": "analytics"},
        "CreatedDate": {"$gte": "2024-01-01"}
    }'::jsonb,
    10, 0, NULL
);

-- 3. Basic array operators:
SELECT search_objects_with_facets(
    1002,
    '{
        "Tags[]": {"$arrayContains": "important"},
        "Scores[]": {"$arrayCountGt": 3},
        "Categories[]": {"$arrayAny": true},
        "Items[]": {"$arrayEmpty": false}
    }'::jsonb,
    10, 0, NULL
);

-- 4. Positional array operators:
SELECT search_objects_with_facets(
    1002,
    '{
        "Tags[]": {"$arrayFirst": "urgent"},
        "Scores[]": {"$arrayLast": "100"},
        "Items[]": {"$arrayAt": "2"}
    }'::jsonb,
    10, 0, NULL
);

-- 5. Search array operators:  
SELECT search_objects_with_facets(
    1002,
    '{
        "Tags[]": {"$arrayStartsWith": "test_"},
        "Names[]": {"$arrayEndsWith": "_prod"},
        "Descriptions[]": {"$arrayMatches": ".*error.*"}
    }'::jsonb,
    10, 0, NULL
);

-- 6. Aggregation array operators:
SELECT search_objects_with_facets(
    1002,
    '{
        "Scores[]": {"$arraySum": "300"},
        "Ratings[]": {"$arrayAvg": "4.5"},
        "Prices[]": {"$arrayMin": "10.50"},
        "Quantities[]": {"$arrayMax": "1000"}
    }'::jsonb,
    10, 0, NULL
);

-- 7. NOT conditions:
SELECT search_objects_with_facets(
    1002,
    '{
        "$not": {"Status": "Deleted"},
        "Title": {"$ne": null}
    }'::jsonb,
    10, 0, NULL
);

-- 8. Class fields - full support:
SELECT search_objects_with_facets(
    1002,
    '{
        "Contact.Name": "John Doe",
        "Address.City": "Moscow",
        "Contact.Phone": {"$startsWith": "+7"},
        "Address.PostalCode": {"$in": ["101000", "102000"]},
        "$not": {"Contact.Email": {"$endsWith": "@test.com"}}
    }'::jsonb,
    10, 0, NULL
);

-- 9. Class arrays with nested fields:
SELECT search_objects_with_facets(
    1002,
    '{
        "Contacts[].Name": "Jane Smith",
        "Addresses[].Country": "Russia", 
        "Products[].Price": {"$gt": "100"},
        "Tags[].Category": {"$contains": "business"},
        "$or": [
            {"Contacts[].Email": {"$endsWith": "@company.com"}},
            {"Addresses[].City": {"$in": ["Moscow", "SPb"]}}
        ]
    }'::jsonb,
    10, 0, NULL
);

-- 10. 🎯 RECURSION CONFIGURATION - custom depth:
SELECT search_objects_with_facets(
    1002, 
    '{"$and": [{"Tags[]": {"$arrayContains": "complex"}}, {"$or": [{"Age": {"$gt": "25"}}, {"Stock": {"$gt": "100"}}]}]}'::jsonb,
    10, 0,
    '[{"field": "Date", "direction": "DESC"}]'::jsonb,
    20  -- max_recursion_depth = 20 for complex queries
);

-- 📊 HIERARCHICAL conditions:
SELECT search_objects_with_facets(
    1002,
    '{"$isRoot": true, "Status": ["Active"]}'::jsonb
);

-- 🌳 TREE SEARCH:
SELECT search_tree_objects_with_facets(
    1002, 1021,  -- scheme_id, parent_id
    '{"Status": ["Active"]}'::jsonb,
    10, 0, NULL, 1  -- direct children
);

-- Recursive descendant search:
SELECT search_tree_objects_with_facets(
    1002, 1021,  -- scheme_id, parent_id  
    NULL, 20, 0, NULL, 5  -- up to 10 levels deep
);

-- 📈 GETTING FACETS for UI:
SELECT get_facets(1002);

-- ⚡ COMPLEX EXAMPLE - combination of all capabilities:
SELECT search_objects_with_facets(
    1002,
    '{
        "$and": [
            {"Status": {"$ne": "Deleted"}},
            {"$or": [
                {"Priority": {"$in": ["High", "Critical"]}},
                {"Tags[]": {"$arrayContains": "urgent"}}
            ]},
            {"CreatedDate": {"$gte": "2024-01-01"}},
            {"Price": {"$gt": "0"}},
            {"$not": {"Archive": true}}
        ],
        "$isRoot": false
    }'::jsonb,
    20, 0,
    '[{"field": "CreatedDate", "direction": "DESC"}]'::jsonb,
    15  -- max_recursion_depth = 15 for extremely complex queries
);
*/