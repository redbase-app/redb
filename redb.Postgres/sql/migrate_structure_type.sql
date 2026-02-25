-- ============================================================
-- HELPER FUNCTION: get_value_column
-- Returns the _values column name for a given type name
-- ============================================================

-- DROP FUNCTION IF EXISTS public.get_value_column(text);

CREATE OR REPLACE FUNCTION public.get_value_column(p_type_name text)
RETURNS text
LANGUAGE 'plpgsql'
IMMUTABLE
AS $BODY$
BEGIN
    RETURN CASE LOWER(p_type_name)
        WHEN 'string' THEN '_string'
        WHEN 'text' THEN '_string'
        WHEN 'mimetype' THEN '_string'
        WHEN 'filepath' THEN '_string'
        WHEN 'filename' THEN '_string'
        WHEN 'long' THEN '_long'
        WHEN 'int' THEN '_long'
        WHEN 'short' THEN '_long'
        WHEN 'byte' THEN '_long'
        WHEN 'object' THEN '_object'
        WHEN 'double' THEN '_double'
        WHEN 'float' THEN '_double'
        WHEN 'boolean' THEN '_boolean'
        WHEN 'datetime' THEN '_datetimeoffset'
        WHEN 'datetimeoffset' THEN '_datetimeoffset'
        WHEN 'dateonly' THEN '_datetimeoffset'
        WHEN 'timeonly' THEN '_datetimeoffset'
        WHEN 'timespan' THEN '_long'
        WHEN 'guid' THEN '_guid'
        WHEN 'bytearray' THEN '_bytearray'
        WHEN 'numeric' THEN '_numeric'
        WHEN 'listitem' THEN '_listitem'
        ELSE NULL
    END;
END;
$BODY$;

COMMENT ON FUNCTION public.get_value_column(text)
    IS 'Returns the _values column name for a given REDB type name.
Examples:
  SELECT get_value_column(''String'');  -- returns ''_string''
  SELECT get_value_column(''Long'');    -- returns ''_long''';

-- ============================================================
-- FUNCTION: public.migrate_structure_type(bigint, text, text, boolean)
-- ============================================================

-- DROP FUNCTION IF EXISTS public.migrate_structure_type(bigint, text, text, boolean);

CREATE OR REPLACE FUNCTION public.migrate_structure_type(
    p_structure_id bigint,
    p_old_type_name text,
    p_new_type_name text,
    p_dry_run boolean DEFAULT false)
    RETURNS TABLE(affected_rows integer, success_count integer, error_count integer, errors text) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE
    v_source_col TEXT;
    v_target_col TEXT;
    v_affected_rows INT := 0;
    v_success_count INT := 0;
    v_has_collision BOOLEAN;
    v_conversion_sql TEXT;
BEGIN
    -- Get column names
    v_source_col := get_value_column(p_old_type_name);
    v_target_col := get_value_column(p_new_type_name);
    
    -- Type validation
    IF v_source_col IS NULL THEN
        RETURN QUERY SELECT 0, 0, 0, format('Unknown source type: %s', p_old_type_name);
        RETURN;
    END IF;
    
    IF v_target_col IS NULL THEN
        RETURN QUERY SELECT 0, 0, 0, format('Unknown target type: %s', p_new_type_name);
        RETURN;
    END IF;
    
    -- Same columns - migration not needed (e.g., Int->Long both in _Long)
    IF v_source_col = v_target_col THEN
        RETURN QUERY SELECT 0, 0, 0, NULL::TEXT;
        RETURN;
    END IF;
    
    -- Check if structure exists
    IF NOT EXISTS (SELECT 1 FROM _structures WHERE _id = p_structure_id) THEN
        RETURN QUERY SELECT 0, 0, 0, format('Structure %s not found', p_structure_id);
        RETURN;
    END IF;
    
    -- Count affected rows
    EXECUTE format(
        'SELECT COUNT(*) FROM _values WHERE _id_structure = $1 AND %I IS NOT NULL',
        v_source_col
    ) INTO v_affected_rows USING p_structure_id;
    
    -- Dry run - only counting
    IF p_dry_run THEN
        RETURN QUERY SELECT v_affected_rows, 0, 0, NULL::TEXT;
        RETURN;
    END IF;
    
    -- ========================================
    -- COLLISION CHECK (key point!)
    -- If target is filled and source is empty - data was already migrated manually
    -- ========================================
    EXECUTE format(
        'SELECT EXISTS(
            SELECT 1 FROM _values 
            WHERE _id_structure = $1 
              AND %I IS NOT NULL
              AND %I IS NULL
            LIMIT 1
        )', v_target_col, v_source_col
    ) INTO v_has_collision USING p_structure_id;
    
    IF v_has_collision THEN
        RETURN QUERY SELECT v_affected_rows, 0, v_affected_rows, 
            format('TYPE_MIGRATION_COLLISION: Data already in %s but _id_type = %s. Fix manually: UPDATE _structures SET _id_type = (SELECT _id FROM _types WHERE _name = ''%s'') WHERE _id = %s',
                v_target_col, p_old_type_name, p_new_type_name, p_structure_id);
        RETURN;
    END IF;
    
    -- No data to migrate
    IF v_affected_rows = 0 THEN
        RETURN QUERY SELECT 0, 0, 0, NULL::TEXT;
        RETURN;
    END IF;
    
    -- ========================================
    -- CONVERSION MATRIX
    -- ========================================
    v_conversion_sql := NULL;
    
    -- STRING -> *
    IF v_source_col = '_string' THEN
        IF v_target_col = '_long' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = %I::bigint, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL AND %I ~ ''^-?[0-9]+$''',
                v_target_col, v_source_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_double' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = %I::double precision, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL AND %I ~ ''^-?[0-9]+\.?[0-9]*$''',
                v_target_col, v_source_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_numeric' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = %I::numeric, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL AND %I ~ ''^-?[0-9]+\.?[0-9]*$''',
                v_target_col, v_source_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_boolean' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = CASE WHEN LOWER(%I) IN (''true'', ''1'', ''yes'', ''t'', ''y'') THEN TRUE WHEN LOWER(%I) IN (''false'', ''0'', ''no'', ''f'', ''n'') THEN FALSE ELSE NULL END, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_datetimeoffset' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = %I::timestamptz, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_guid' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = %I::uuid, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        END IF;
    
    -- LONG -> *
    ELSIF v_source_col = '_long' THEN
        IF v_target_col = '_string' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = %I::text, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_double' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = %I::double precision, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_numeric' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = %I::numeric, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_boolean' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = (%I != 0), %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_datetimeoffset' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = to_timestamp(%I), %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        END IF;
    
    -- DOUBLE -> *
    ELSIF v_source_col = '_double' THEN
        IF v_target_col = '_string' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = %I::text, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_long' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = ROUND(%I)::bigint, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_numeric' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = %I::numeric, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        END IF;
    
    -- NUMERIC -> *
    ELSIF v_source_col = '_numeric' THEN
        IF v_target_col = '_string' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = %I::text, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_long' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = ROUND(%I)::bigint, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_double' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = %I::double precision, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        END IF;
    
    -- BOOLEAN -> *
    ELSIF v_source_col = '_boolean' THEN
        IF v_target_col = '_string' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = CASE WHEN %I THEN ''true'' ELSE ''false'' END, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_long' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = CASE WHEN %I THEN 1 ELSE 0 END, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        END IF;
    
    -- DATETIMEOFFSET -> *
    ELSIF v_source_col = '_datetimeoffset' THEN
        IF v_target_col = '_string' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = %I::text, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_long' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = EXTRACT(EPOCH FROM %I)::bigint, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        END IF;
    
    -- GUID -> *
    ELSIF v_source_col = '_guid' THEN
        IF v_target_col = '_string' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = %I::text, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        END IF;
    END IF;
    
    -- Conversion not supported
    IF v_conversion_sql IS NULL THEN
        RETURN QUERY SELECT v_affected_rows, 0, v_affected_rows, 
            format('Conversion %s -> %s not supported', p_old_type_name, p_new_type_name);
        RETURN;
    END IF;
    
    -- Execute migration
    EXECUTE v_conversion_sql USING p_structure_id;
    GET DIAGNOSTICS v_success_count = ROW_COUNT;
    
    RETURN QUERY SELECT v_affected_rows, v_success_count, v_affected_rows - v_success_count, NULL::TEXT;
END;
$BODY$;

ALTER FUNCTION public.migrate_structure_type(bigint, text, text, boolean)
    OWNER TO postgres;

COMMENT ON FUNCTION public.migrate_structure_type(bigint, text, text, boolean)
    IS 'Atomic data migration when changing structure type.
Parameters:
  p_structure_id - structure ID in _structures
  p_old_type_name - old type name (String, Long, Double, etc.)
  p_new_type_name - new type name
  p_dry_run - TRUE for test run without changes

Returns:
  affected_rows - total rows with data
  success_count - successfully migrated
  error_count - failed to migrate
  errors - error text (NULL if success)

Returns TYPE_MIGRATION_COLLISION error if data is already in target column.

Examples:
  SELECT * FROM migrate_structure_type(12345, ''String'', ''Long'', TRUE);  -- dry run
  SELECT * FROM migrate_structure_type(12345, ''String'', ''Long'', FALSE); -- execute';