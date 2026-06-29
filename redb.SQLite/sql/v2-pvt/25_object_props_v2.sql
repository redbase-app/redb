-- =====================================================================
-- 25_object_props_v2.sql
-- ---------------------------------------------------------------------
-- Experimental SQL-driven re-implementation of the per-level reader
--   build_hierarchical_properties_optimized()  →  build_hierarchical_properties_sql()
-- and a wrapper
--   get_object_json()  →  get_object_json_v2()
-- for A/B benchmarking against the production reader.
--
-- Goal:
--   replace the plpgsql FOR-LOOP over _scheme_metadata_cache fields with
--   a single SQL statement that materializes all field values at the
--   current nesting level via jsonb_object_agg(field_name, field_value).
--
-- Semantics:
--   MUST be byte-for-byte identical to build_hierarchical_properties_optimized()
--   for a fixed depth. Verified by comparing md5(get_object_json(id)::text)
--   against md5(get_object_json_v2(id)::text) over all objects.
--
-- Notes:
--   - These functions live alongside the production ones; nothing existing
--     is overridden. To switch the reader globally, change get_object_json
--     in redb_json_objects.sql to call build_hierarchical_properties_sql.
--   - File is bundled into pvt_bundle.sql by _bundle.ps1. It only adds new
--     functions, so re-running the bundle on prod is safe.
-- =====================================================================

DROP FUNCTION IF EXISTS build_hierarchical_properties_sql(bigint, bigint, bigint, _values[], integer, text, bigint);
DROP FUNCTION IF EXISTS get_object_json_v2(bigint, integer);

-- ---------------------------------------------------------------------
-- build_hierarchical_properties_sql
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION build_hierarchical_properties_sql(
    object_id bigint,
    parent_structure_id bigint,
    object_scheme_id bigint,
    all_values _values[],
    max_depth integer DEFAULT 10,
    array_index text DEFAULT NULL,
    parent_value_id bigint DEFAULT NULL
) RETURNS jsonb
LANGUAGE plpgsql
COST 100
VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    result_json jsonb;
BEGIN
    -- Defensive recursion guard (matches original behavior)
    IF max_depth < -100 THEN
        RETURN jsonb_build_object('error', 'Max recursion depth reached for hierarchical fields');
    END IF;

    -- Auto-populate metadata cache (matches original behavior)
    IF NOT EXISTS(SELECT 1 FROM _scheme_metadata_cache WHERE _scheme_id = object_scheme_id LIMIT 1) THEN
        PERFORM sync_metadata_cache_for_scheme(object_scheme_id);
    END IF;

    -- ONE SQL statement: build all fields at this nesting level.
    -- jsonb_object_agg replaces the plpgsql FOR-LOOP + `||` concat.
    SELECT jsonb_object_agg(field_name, field_value ORDER BY field_order, field_struct_id)
    INTO result_json
    FROM (
        SELECT
            c._name AS field_name,
            c._order AS field_order,
            c._structure_id AS field_struct_id,
            CASE
                -- =========== ARRAY ===========
                WHEN c._collection_type = -9223372036854775668 THEN
                    CASE
                        -- No head record => array property is NULL
                        WHEN NOT EXISTS (
                            SELECT 1 FROM unnest(all_values) v
                            WHERE v._id_structure = c._structure_id
                              AND v._array_index IS NULL
                              AND ((parent_value_id IS NULL AND v._array_parent_id IS NULL)
                                OR (parent_value_id IS NOT NULL AND v._array_parent_id = parent_value_id))
                        ) THEN NULL
                        -- Array of Class
                        WHEN c.type_semantic = 'Object' THEN
                            COALESCE(
                                (SELECT jsonb_agg(
                                            build_hierarchical_properties_sql(
                                                object_id, c._structure_id, object_scheme_id,
                                                all_values, max_depth, v._array_index, v._id)
                                            ORDER BY CASE WHEN v._array_index ~ '^[0-9]+$' THEN v._array_index::int ELSE 0 END,
                                                     v._array_index)
                                 FROM unnest(all_values) v
                                 WHERE v._id_structure = c._structure_id
                                   AND v._array_index IS NOT NULL
                                   AND v._array_parent_id = (
                                       SELECT v2._id FROM unnest(all_values) v2
                                       WHERE v2._id_structure = c._structure_id
                                         AND v2._array_index IS NULL
                                         AND ((parent_value_id IS NULL AND v2._array_parent_id IS NULL)
                                           OR (parent_value_id IS NOT NULL AND v2._array_parent_id = parent_value_id))
                                       LIMIT 1)),
                                '[]'::jsonb)
                        -- Array of primitives / RObject
                        ELSE
                            COALESCE(
                                (SELECT jsonb_agg(
                                            CASE
                                                WHEN c.type_semantic = '_RObject' AND v._Object IS NOT NULL THEN
                                                    get_object_json(v._Object, max_depth - 1)
                                                WHEN c.db_type = 'String' THEN to_jsonb(v._String)
                                                WHEN c.db_type = 'Long' THEN
                                                    CASE WHEN v._ListItem IS NOT NULL
                                                         THEN build_listitem_jsonb(v._ListItem, max_depth)
                                                         ELSE to_jsonb(v._Long) END
                                                WHEN c.db_type = 'Guid' THEN to_jsonb(v._Guid)
                                                WHEN c.db_type = 'Double' THEN to_jsonb(v._Double)
                                                WHEN c.db_type = 'Numeric' THEN to_jsonb(v._Numeric)
                                                WHEN c.db_type = 'DateTimeOffset' THEN to_jsonb(v._DateTimeOffset)
                                                WHEN c.db_type = 'Boolean' THEN to_jsonb(v._Boolean)
                                                WHEN c.db_type = 'ListItem' THEN
                                                    build_listitem_jsonb(v._ListItem, max_depth)
                                                WHEN c.db_type = 'ByteArray' THEN
                                                    to_jsonb(encode(decode(v._ByteArray::text, 'base64'), 'base64'))
                                                ELSE NULL
                                            END
                                            ORDER BY CASE WHEN v._array_index ~ '^[0-9]+$' THEN v._array_index::int ELSE 0 END,
                                                     v._array_index)
                                 FROM unnest(all_values) v
                                 WHERE v._id_structure = c._structure_id
                                   AND v._array_index IS NOT NULL
                                   AND v._array_parent_id = (
                                       SELECT v2._id FROM unnest(all_values) v2
                                       WHERE v2._id_structure = c._structure_id
                                         AND v2._array_index IS NULL
                                         AND ((parent_value_id IS NULL AND v2._array_parent_id IS NULL)
                                           OR (parent_value_id IS NOT NULL AND v2._array_parent_id = parent_value_id))
                                       LIMIT 1)),
                                '[]'::jsonb)
                    END
                -- =========== DICTIONARY ===========
                WHEN c._collection_type = -9223372036854775667 THEN
                    CASE
                        WHEN NOT EXISTS (
                            SELECT 1 FROM unnest(all_values) v
                            WHERE v._id_structure = c._structure_id
                              AND v._array_index IS NULL
                              AND ((parent_value_id IS NULL AND v._array_parent_id IS NULL)
                                OR (parent_value_id IS NOT NULL AND v._array_parent_id = parent_value_id))
                        ) THEN NULL
                        -- Dict<K, RObject>
                        WHEN c.type_semantic = '_RObject' THEN
                            COALESCE(
                                (SELECT jsonb_object_agg(v._array_index,
                                            CASE WHEN v._Object IS NOT NULL THEN get_object_json(v._Object, max_depth - 1) ELSE NULL END)
                                 FROM unnest(all_values) v
                                 WHERE v._id_structure = c._structure_id
                                   AND v._array_index IS NOT NULL
                                   AND v._array_parent_id = (
                                       SELECT v2._id FROM unnest(all_values) v2
                                       WHERE v2._id_structure = c._structure_id
                                         AND v2._array_index IS NULL
                                         AND ((parent_value_id IS NULL AND v2._array_parent_id IS NULL)
                                           OR (parent_value_id IS NOT NULL AND v2._array_parent_id = parent_value_id))
                                       LIMIT 1)),
                                '{}'::jsonb)
                        -- Dict<K, Class>
                        WHEN c.type_semantic = 'Object' THEN
                            COALESCE(
                                (SELECT jsonb_object_agg(v._array_index,
                                            build_hierarchical_properties_sql(
                                                object_id, c._structure_id, object_scheme_id,
                                                all_values, max_depth, NULL, v._id))
                                 FROM unnest(all_values) v
                                 WHERE v._id_structure = c._structure_id
                                   AND v._array_index IS NOT NULL
                                   AND v._array_parent_id = (
                                       SELECT v2._id FROM unnest(all_values) v2
                                       WHERE v2._id_structure = c._structure_id
                                         AND v2._array_index IS NULL
                                         AND ((parent_value_id IS NULL AND v2._array_parent_id IS NULL)
                                           OR (parent_value_id IS NOT NULL AND v2._array_parent_id = parent_value_id))
                                       LIMIT 1)),
                                '{}'::jsonb)
                        -- Dict<K, primitive>
                        ELSE
                            COALESCE(
                                (SELECT jsonb_object_agg(v._array_index,
                                            CASE
                                                WHEN c.db_type = 'String' THEN to_jsonb(v._String)
                                                WHEN c.db_type = 'Long' THEN to_jsonb(v._Long)
                                                WHEN c.db_type = 'Guid' THEN to_jsonb(v._Guid)
                                                WHEN c.db_type = 'Double' THEN to_jsonb(v._Double)
                                                WHEN c.db_type = 'Numeric' THEN to_jsonb(v._Numeric)
                                                WHEN c.db_type = 'DateTimeOffset' THEN to_jsonb(v._DateTimeOffset)
                                                WHEN c.db_type = 'Boolean' THEN to_jsonb(v._Boolean)
                                                ELSE NULL
                                            END)
                                 FROM unnest(all_values) v
                                 WHERE v._id_structure = c._structure_id
                                   AND v._array_index IS NOT NULL
                                   AND v._array_parent_id = (
                                       SELECT v2._id FROM unnest(all_values) v2
                                       WHERE v2._id_structure = c._structure_id
                                         AND v2._array_index IS NULL
                                         AND ((parent_value_id IS NULL AND v2._array_parent_id IS NULL)
                                           OR (parent_value_id IS NOT NULL AND v2._array_parent_id = parent_value_id))
                                       LIMIT 1)),
                                '{}'::jsonb)
                    END
                -- =========== SCALAR RObject ===========
                WHEN c.type_name = 'Object' AND c.type_semantic = '_RObject' THEN
                    (SELECT CASE WHEN v._Object IS NOT NULL THEN get_object_json(v._Object, max_depth - 1) ELSE NULL END
                     FROM unnest(all_values) v
                     WHERE v._id_structure = c._structure_id
                       AND CASE
                           WHEN parent_value_id IS NOT NULL THEN v._array_parent_id = parent_value_id
                           WHEN array_index    IS NOT NULL THEN v._array_index = array_index
                           ELSE                                 v._array_index IS NULL
                           END
                     LIMIT 1)
                -- =========== SCALAR Class ===========
                WHEN c.type_semantic = 'Object' THEN
                    (SELECT CASE WHEN v._Guid IS NULL THEN NULL
                                 ELSE build_hierarchical_properties_sql(
                                          object_id, c._structure_id, object_scheme_id,
                                          all_values, max_depth, NULL, v._id) END
                     FROM unnest(all_values) v
                     WHERE v._id_structure = c._structure_id
                       AND CASE
                           WHEN parent_value_id IS NOT NULL THEN v._array_parent_id = parent_value_id
                           WHEN array_index    IS NOT NULL THEN v._array_index = array_index
                           ELSE                                 v._array_index IS NULL
                           END
                     LIMIT 1)
                -- =========== SCALAR primitives ===========
                ELSE
                    (SELECT
                         CASE
                             WHEN c.db_type = 'String' THEN to_jsonb(v._String)
                             WHEN c.db_type = 'Long' THEN
                                 CASE WHEN v._ListItem IS NOT NULL
                                      THEN build_listitem_jsonb(v._ListItem, max_depth)
                                      ELSE to_jsonb(v._Long) END
                             WHEN c.db_type = 'Guid' THEN to_jsonb(v._Guid)
                             WHEN c.db_type = 'Double' THEN to_jsonb(v._Double)
                             WHEN c.db_type = 'Numeric' THEN to_jsonb(v._Numeric)
                             WHEN c.db_type = 'DateTimeOffset' THEN to_jsonb(v._DateTimeOffset)
                             WHEN c.db_type = 'Boolean' THEN to_jsonb(v._Boolean)
                             WHEN c.db_type = 'ListItem' OR v._ListItem IS NOT NULL THEN
                                 CASE WHEN v._ListItem IS NOT NULL
                                      THEN build_listitem_jsonb(v._ListItem, max_depth)
                                      ELSE NULL END
                             WHEN c.db_type = 'ByteArray' THEN
                                 CASE WHEN v._ByteArray IS NOT NULL
                                      THEN to_jsonb(encode(decode(v._ByteArray::text, 'base64'), 'base64'))
                                      ELSE NULL END
                             ELSE NULL
                         END
                     FROM unnest(all_values) v
                     WHERE v._id_structure = c._structure_id
                       AND CASE
                           WHEN parent_value_id IS NOT NULL THEN v._array_parent_id = parent_value_id
                           WHEN array_index    IS NOT NULL THEN v._array_index = array_index
                           ELSE                                 v._array_index IS NULL
                           END
                     LIMIT 1)
            END AS field_value
        FROM _scheme_metadata_cache c
        WHERE c._scheme_id = object_scheme_id
          AND ((parent_structure_id IS NULL AND c._parent_structure_id IS NULL)
            OR (parent_structure_id IS NOT NULL AND c._parent_structure_id = parent_structure_id))
    ) f
    WHERE f.field_value IS NOT NULL;

    RETURN COALESCE(result_json, '{}'::jsonb);
END;
$BODY$;

-- ---------------------------------------------------------------------
-- get_object_json_v2 — wrapper that calls build_hierarchical_properties_sql.
-- Top-level body identical to get_object_json (only the inner call differs).
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_object_json_v2(
    object_id bigint,
    max_depth integer DEFAULT 10
) RETURNS jsonb
LANGUAGE plpgsql
COST 100
VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    result_json jsonb;
    object_exists boolean;
    base_info jsonb;
    properties_info jsonb;
    object_scheme_id bigint;
    all_values _values[];
BEGIN
    SELECT EXISTS(SELECT 1 FROM _objects WHERE _id = object_id) INTO object_exists;
    IF NOT object_exists THEN
        RETURN NULL;
    END IF;

    IF max_depth <= 0 THEN
        SELECT jsonb_build_object(
            'id', o._id, 'name', o._name, 'scheme_id', o._id_scheme,
            'scheme_name', sc._name, 'parent_id', o._id_parent, 'owner_id', o._id_owner,
            'who_change_id', o._id_who_change, 'date_create', o._date_create,
            'date_modify', o._date_modify, 'date_begin', o._date_begin,
            'date_complete', o._date_complete, 'key', o._key, 'value_long', o._value_long,
            'value_string', o._value_string, 'value_guid', o._value_guid, 'note', o._note,
            'value_bool', o._value_bool, 'value_double', o._value_double,
            'value_numeric', o._value_numeric, 'value_datetime', o._value_datetime,
            'value_bytes', o._value_bytes, 'hash', o._hash
        ) INTO result_json
        FROM _objects o JOIN _schemes sc ON sc._id = o._id_scheme
        WHERE o._id = object_id;
        RETURN result_json;
    END IF;

    SELECT jsonb_build_object(
        'id', o._id, 'name', o._name, 'scheme_id', o._id_scheme,
        'scheme_name', sc._name, 'parent_id', o._id_parent, 'owner_id', o._id_owner,
        'who_change_id', o._id_who_change, 'date_create', o._date_create,
        'date_modify', o._date_modify, 'date_begin', o._date_begin,
        'date_complete', o._date_complete, 'key', o._key, 'value_long', o._value_long,
        'value_string', o._value_string, 'value_guid', o._value_guid, 'note', o._note,
        'value_bool', o._value_bool, 'value_double', o._value_double,
        'value_numeric', o._value_numeric, 'value_datetime', o._value_datetime,
        'value_bytes', o._value_bytes, 'hash', o._hash
    ), o._id_scheme
    INTO base_info, object_scheme_id
    FROM _objects o JOIN _schemes sc ON sc._id = o._id_scheme
    WHERE o._id = object_id;

    SELECT array_agg(v) INTO all_values
    FROM _values v
    WHERE v._id_object = object_id;

    IF all_values IS NULL THEN
        RETURN base_info || jsonb_build_object('properties', NULL);
    END IF;

    SELECT build_hierarchical_properties_sql(
        object_id, NULL, object_scheme_id,
        COALESCE(all_values, ARRAY[]::_values[]),
        max_depth, NULL, NULL
    ) INTO properties_info;

    RETURN base_info || jsonb_build_object('properties', COALESCE(properties_info, '{}'::jsonb));
END;
$BODY$;
