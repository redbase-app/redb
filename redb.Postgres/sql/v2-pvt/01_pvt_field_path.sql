-- =====================================================================
-- pvt_normalize_base_field_name: map C# base field names to _objects columns
-- ---------------------------------------------------------------------
-- Forked from redb_facets_search.sql L51 on 2026-05-18.
-- Legacy version stays untouched. Mirror legacy bug fixes here.
-- =====================================================================

CREATE OR REPLACE FUNCTION pvt_normalize_base_field_name(field_name text)
RETURNS text
LANGUAGE 'plpgsql'
IMMUTABLE
AS $BODY$
DECLARE
    v_had_prefix boolean := false;
    v_result     text;
BEGIN
    -- FacetFilterBuilder marks base fields by prepending '0$:' to the raw name.
    -- Strip it before the lookup so '0$:Id', '0$:Name' resolve to base columns.
    IF field_name LIKE '0$:%' THEN
        v_had_prefix := true;
        field_name := substring(field_name from 4);
    END IF;

    v_result := CASE field_name
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

    -- Disambiguation: bare names like `Name`, `Id`, `Key` collide with
    -- legitimate user-defined Props fields of the same name. Only honor
    -- the base-column mapping when the caller explicitly opted in via
    -- the `0$:` prefix, OR the input already starts with an underscore
    -- (system column naming convention).
    IF v_result IS NOT NULL
       AND NOT v_had_prefix
       AND substring(field_name from 1 for 1) <> '_' THEN
        RETURN NULL;
    END IF;

    RETURN v_result;
END;
$BODY$;

COMMENT ON FUNCTION pvt_normalize_base_field_name(text) IS 'Normalizes C# base field names to SQL column names in _objects. Returns NULL if field is not a base field.';

-- =====================================================================
-- pvt_parse_field_path: split dotted/bracketed field paths into components
-- ---------------------------------------------------------------------
-- Forked from redb_facets_search.sql L185 on 2026-05-18.
-- Legacy version stays untouched. Mirror legacy bug fixes here.
-- =====================================================================

CREATE OR REPLACE FUNCTION pvt_parse_field_path(
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

COMMENT ON FUNCTION pvt_parse_field_path(text) IS 'Parses field path to support Class fields, arrays and Dictionary. Supports: "Name", "Contact.Name", "Tags[]", "Contacts[].Email", "PhoneBook[home]", "AddressBook[home].City". Returns path components for further processing.';


-- =====================================================================
-- pvt_normalize_field_name: rewrite `<dict>.ContainsKey` predicate paths
-- ---------------------------------------------------------------------
-- Mirrors redb.Core.Pro.Query.ProSqlBuilderBase.NormalizeDictionaryFieldName:
-- when the C# predicate is `Dict.ContainsKey("home")`, the builder emits a
-- field name of "<Dict>.ContainsKey" with a string operand "home". The PVT
-- layer rewrites that to "<Dict>[home]" so the existing dictionary-pivot
-- machinery handles it and the operator collapses to IS NOT NULL.
--
-- Returns the rewritten path. When the conditions are not met, returns
-- the input path unchanged.
-- =====================================================================
CREATE OR REPLACE FUNCTION pvt_normalize_field_name(
    p_path text,
    p_op_value text
)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
AS $BODY$
DECLARE
    v_base text;
BEGIN
    IF p_path IS NULL THEN
        RETURN NULL;
    END IF;
    IF right(p_path, length('.ContainsKey')) <> '.ContainsKey' THEN
        RETURN p_path;
    END IF;
    IF p_op_value IS NULL OR p_op_value = '' THEN
        RETURN p_path;
    END IF;

    v_base := left(p_path, length(p_path) - length('.ContainsKey'));
    RETURN v_base || '[' || p_op_value || ']';
END;
$BODY$;

COMMENT ON FUNCTION pvt_normalize_field_name(text, text) IS
    'If path ends with ".ContainsKey" and a string operand is provided, rewrites to "<base>[<key>]" so dictionary-pivot path resolution kicks in. Mirrors Pro.ProSqlBuilderBase.NormalizeDictionaryFieldName.';


-- =====================================================================
-- pvt_peek_contains_key_value: extract a string operand for ContainsKey
-- ---------------------------------------------------------------------
-- The operand of `Dict.ContainsKey(...)` can arrive either as a bare
-- JSON string (shorthand $eq) or as an object `{ "$eq": "<key>" }`.
-- Returns the underlying text, or NULL if the operand is not a string.
-- =====================================================================
CREATE OR REPLACE FUNCTION pvt_peek_contains_key_value(p_op jsonb)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
AS $BODY$
DECLARE
    v jsonb;
BEGIN
    IF p_op IS NULL THEN
        RETURN NULL;
    END IF;
    IF jsonb_typeof(p_op) = 'string' THEN
        RETURN p_op #>> '{}';
    END IF;
    IF jsonb_typeof(p_op) = 'object' THEN
        v := p_op -> '$eq';
        IF v IS NOT NULL AND jsonb_typeof(v) = 'string' THEN
            RETURN v #>> '{}';
        END IF;
    END IF;
    RETURN NULL;
END;
$BODY$;

COMMENT ON FUNCTION pvt_peek_contains_key_value(jsonb) IS
    'Returns the string operand of a ContainsKey predicate (bare string or {"$eq": "..."}), else NULL.';

