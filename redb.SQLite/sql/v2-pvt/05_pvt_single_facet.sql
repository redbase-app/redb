-- =====================================================================
-- pvt_build_single_facet_condition: build a single field facet WHERE fragment (legacy EXISTS engine; used as fallback for complex ops in PVT)
-- ---------------------------------------------------------------------
-- Forked from redb_facets_search.sql L1311 on 2026-05-18.
-- Legacy version stays untouched. Mirror legacy bug fixes here.
-- =====================================================================

CREATE OR REPLACE FUNCTION pvt_build_single_facet_condition(
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
            CONTINUE; -- Skip, they are processed in pvt_build_hierarchical_conditions
        
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
                sql_field_name text := pvt_normalize_base_field_name(raw_field_name);
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
        -- ⚠️ Only match $ne when the value IS null (not a specific value like "TEST")
        --    Otherwise $ne with specific value must fall through to standard ELSE handler
        ELSIF jsonb_typeof(condition_value) = 'object' 
              AND (
                  (condition_value ? '$ne' AND (condition_value->>'$ne' IS NULL OR condition_value->>'$ne' = 'null'))
                  OR condition_value ? '$exists'
              )
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
                    -- Guard already filters: only $ne null or $exists reach here
                END IF;
                -- If structure not found, fall through to standard handling
            END;
        
        -- 🆕 Dictionary indexer: FieldName[key] (example: "PhoneBook[home]": {"$eq": "+7-999..."})
        -- Direct implementation without pvt_build_inner_condition (it uses incompatible aliases fs/fv)
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
                FROM pvt_find_structure_info(scheme_id, dict_field_name, NULL) AS fi
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
            SELECT * INTO parsed_path FROM pvt_parse_field_path(field_path);
            
            -- Get structure information for all fields
            -- 🎯 FIX: Explicit assignment of fields from TABLE-returning function to RECORD
            SELECT 
                fi.root_structure_id,
                fi.nested_structure_id,
                fi.root_type_info,
                fi.nested_type_info
            INTO 
                structure_info
            FROM pvt_find_structure_info(scheme_id, parsed_path.root_field, parsed_path.nested_field) AS fi
            LIMIT 1;
            
            -- Process field value
            IF jsonb_typeof(condition_value) = 'object' THEN
                -- Complex condition with operators like {"$gt": 100, "$lt": 200}
                FOR operator_name, operator_value IN SELECT key, value FROM jsonb_each_text(condition_value) LOOP
                    inner_condition_sql := pvt_build_inner_condition(
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
                inner_condition_sql := pvt_build_inner_condition(
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
                inner_condition_sql := pvt_build_inner_condition(
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

