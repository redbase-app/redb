-- =====================================================================
-- pvt_build_inner_condition: build SQL operator/value fragment for a typed value column
-- ---------------------------------------------------------------------
-- Forked from redb_facets_search.sql L362 on 2026-05-18.
-- Legacy version stays untouched. Mirror legacy bug fixes here.
-- =====================================================================

CREATE OR REPLACE FUNCTION pvt_build_inner_condition(
    operator_name text,
    operator_value text,
    type_info jsonb  -- Type information from pvt_find_structure_info
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
            -- ListItem array: search by _listitem column (stores ListItem ID)
            RETURN format('fv._listitem = %L::bigint', operator_value);
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

COMMENT ON FUNCTION pvt_build_inner_condition(text, text, jsonb) IS '🚀 EXTENDED core system of LINQ operators. Supports 25+ operators: 
📊 Numeric: $gt, $gte, $lt, $lte, $ne, $in (Long, Double, Numeric)
📝 String: $contains, $startsWith, $endsWith  
⏱️ TimeSpan: $gt, $lt, $eq (conversion to INTERVAL for correct comparison)
📅 DateTimeOffset: $gt, $lt, $eq (timestamptz with timezone)
🔢 Arrays (basic): $arrayContains, $arrayAny, $arrayEmpty, $arrayCount*
🎯 Arrays (position): $arrayAt, $arrayFirst, $arrayLast
🔍 Arrays (search): $arrayStartsWith, $arrayEndsWith, $arrayMatches
📈 Arrays (aggregation): $arraySum, $arrayAvg, $arrayMin, $arrayMax
All operators adapted for relational arrays via _array_index. Supports distinguishing _RObject vs Object types. Auto type detection by value format. Special handling for TimeSpan via INTERVAL.';

