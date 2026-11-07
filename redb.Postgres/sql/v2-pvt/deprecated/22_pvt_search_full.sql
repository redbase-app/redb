-- =====================================================================
-- 22_pvt_search_full.sql
-- ---------------------------------------------------------------------
-- Full entry points: return base fields + Props as JSONB via the
-- system get_object_json materializer.
--
-- Functions:
--   pvt_search_objects(...)   RETURNS SETOF jsonb
--   pvt_get_sql_preview(...)  RETURNS text
-- =====================================================================

CREATE OR REPLACE FUNCTION pvt_search_objects(
    p_scheme_id   bigint,
    p_filter      jsonb    DEFAULT NULL,
    p_limit       integer  DEFAULT NULL,
    p_offset      integer  DEFAULT 0,
    p_order       jsonb    DEFAULT NULL,
    p_max_depth   integer  DEFAULT 10,
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
        RETURN NEXT get_object_json(v_id, COALESCE(p_max_depth, 10));
    END LOOP;
END;
$BODY$;

COMMENT ON FUNCTION pvt_search_objects(bigint, jsonb, integer, integer, jsonb, integer, boolean, text, bigint[]) IS
    'PVT full-object search. Executes the SQL from pvt_build_query_sql and projects each matched _id through the system get_object_json materializer (base fields + Props).';


CREATE OR REPLACE FUNCTION pvt_get_sql_preview(
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

COMMENT ON FUNCTION pvt_get_sql_preview(bigint, jsonb, integer, integer, jsonb, integer, boolean, text, bigint[]) IS
    'Returns the generated SQL string for a full-object PVT search without executing it. Identical body to pvt_get_sql_preview_base — get_object_json wraps the same _id list.';
