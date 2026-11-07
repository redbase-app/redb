-- =====================================================================
-- pvt_build_level_condition / pvt_build_level_condition_with_operators
-- ---------------------------------------------------------------------
-- Forked from redb_init.sql (deprecated) on 2026-05-23.
-- Required by pvt_build_hierarchical_conditions (06_pvt_hierarchical.sql).
-- The legacy bundle that originally defined `build_level_condition`/
-- `build_level_condition_with_operators` was removed in PG free
-- (v2-pvt is now the only engine), so we re-host these helpers under
-- the pvt_* namespace.
-- =====================================================================

CREATE OR REPLACE FUNCTION pvt_build_level_condition(
    target_level integer,
    table_alias text DEFAULT 'o'
) RETURNS text
LANGUAGE 'plpgsql'
COST 50
IMMUTABLE
AS $BODY$
BEGIN
    -- Calculate level going UP from object to root.
    -- Level 0 = root (where _id_parent IS NULL).
    -- Level 1 = direct child of root, etc.
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

CREATE OR REPLACE FUNCTION pvt_build_level_condition_with_operators(
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
    FOR operator_name, operator_value IN
        SELECT key, value FROM jsonb_each_text(level_operators)
    LOOP
        CASE operator_name
            WHEN '$gt' THEN op_symbol := '>';
            WHEN '$gte' THEN op_symbol := '>=';
            WHEN '$lt' THEN op_symbol := '<';
            WHEN '$lte' THEN op_symbol := '<=';
            WHEN '$eq' THEN op_symbol := '=';
            WHEN '$ne' THEN op_symbol := '!=';
            ELSE
                CONTINUE;
        END CASE;

        IF level_condition != '' THEN
            level_condition := level_condition || ' AND ';
        END IF;

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

    IF level_condition != '' THEN
        RETURN ' AND (' || level_condition || ')';
    END IF;

    RETURN '';
END;
$BODY$;
