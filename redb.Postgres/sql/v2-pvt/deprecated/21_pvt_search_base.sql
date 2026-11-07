-- =====================================================================
-- 21_pvt_search_base.sql
-- ---------------------------------------------------------------------
-- Base entry points: return base fields only (no Props). Uses
-- pvt_build_query_sql to get a list of _id values, then materializes
-- each row via pvt_get_object_base_fields (forked legacy helper).
--
-- Functions:
--   pvt_search_objects_base(...)   RETURNS SETOF jsonb
--   pvt_get_sql_preview_base(...)  RETURNS text
-- =====================================================================

CREATE OR REPLACE FUNCTION pvt_search_objects_base(
    p_scheme_id   bigint,
    p_filter      jsonb    DEFAULT NULL,
    p_limit       integer  DEFAULT NULL,
    p_offset      integer  DEFAULT 0,
    p_order       jsonb    DEFAULT NULL,
    p_max_depth   integer  DEFAULT NULL,
    p_distinct    boolean  DEFAULT false,
    p_source_mode text     DEFAULT 'flat',
    p_tree_ids    bigint[] DEFAULT NULL
)
RETURNS SETOF jsonb
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_sql text;
    v_id  bigint;
BEGIN
    v_sql := pvt_build_query_sql(
        p_scheme_id, p_filter, p_limit, p_offset, p_order,
        p_max_depth, p_distinct, p_source_mode, p_tree_ids
    );

    FOR v_id IN EXECUTE v_sql LOOP
        RETURN NEXT pvt_get_object_base_fields(v_id);
    END LOOP;
END;
$BODY$;

COMMENT ON FUNCTION pvt_search_objects_base(bigint, jsonb, integer, integer, jsonb, integer, boolean, text, bigint[]) IS
    'PVT base-fields search. Executes the SQL produced by pvt_build_query_sql and projects each matched _id through pvt_get_object_base_fields (no Props).';


CREATE OR REPLACE FUNCTION pvt_get_sql_preview_base(
    p_scheme_id   bigint,
    p_filter      jsonb    DEFAULT NULL,
    p_limit       integer  DEFAULT NULL,
    p_offset      integer  DEFAULT 0,
    p_order       jsonb    DEFAULT NULL,
    p_max_depth   integer  DEFAULT NULL,
    p_distinct    boolean  DEFAULT false,
    p_source_mode text     DEFAULT 'flat',
    p_tree_ids    bigint[] DEFAULT NULL
)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $BODY$
BEGIN
    RETURN pvt_build_query_sql(
        p_scheme_id, p_filter, p_limit, p_offset, p_order,
        p_max_depth, p_distinct, p_source_mode, p_tree_ids
    );
END;
$BODY$;

COMMENT ON FUNCTION pvt_get_sql_preview_base(bigint, jsonb, integer, integer, jsonb, integer, boolean, text, bigint[]) IS
    'Returns the generated SQL string for a base-fields PVT search without executing it. Useful for diagnostics and tests.';
