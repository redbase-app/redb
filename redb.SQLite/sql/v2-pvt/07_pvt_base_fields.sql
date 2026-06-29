-- =====================================================================
-- pvt_get_object_base_fields: return JSONB with all base fields of a single object (no Props)
-- ---------------------------------------------------------------------
-- Forked from redb_lazy_loading_search.sql L17 on 2026-05-18.
-- Legacy version stays untouched. Mirror legacy bug fixes here.
-- =====================================================================

CREATE OR REPLACE FUNCTION pvt_get_object_base_fields(object_id bigint)
RETURNS jsonb
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT jsonb_build_object(
        'id', o._id,
        'name', o._name,
        'scheme_id', o._id_scheme,
        'parent_id', o._id_parent,
        'owner_id', o._id_owner,
        'who_change_id', o._id_who_change,
        'date_create', o._date_create,
        'date_modify', o._date_modify,
        'date_begin', o._date_begin,
        'date_complete', o._date_complete,
        'key', o._key,
        'value_long', o._value_long,
        'value_string', o._value_string,
        'value_guid', o._value_guid,
        'note', o._note,
        'value_bool', o._value_bool,
        'value_double', o._value_double,
        'value_numeric', o._value_numeric,
        'value_datetime', o._value_datetime,
        'value_bytes', o._value_bytes,
        'hash', o._hash  -- CRITICAL for cache!
    )
    FROM _objects o
    WHERE o._id = object_id;
$$;

COMMENT ON FUNCTION pvt_get_object_base_fields(bigint) IS 
'Returns base object fields WITHOUT Props for lazy loading.
Includes hash for cache validation. 10-50x faster than get_object_json().
ATTENTION: Function kept for compatibility and direct use.
In aggregate queries (search_*_base), direct JOIN is used instead of function call for optimization.';

