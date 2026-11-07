-- =====================================================================
-- pvt_build_hierarchical_conditions: build tree predicates ($hasAncestor, $hasDescendant, $level, ...)
-- ---------------------------------------------------------------------
-- Forked from redb_facets_search.sql L2818 on 2026-05-18.
-- Legacy version stays untouched. Mirror legacy bug fixes here.
-- =====================================================================

CREATE OR REPLACE FUNCTION pvt_build_hierarchical_conditions(
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
                where_conditions := where_conditions || pvt_build_level_condition_with_operators(facet_filters->'$level', table_alias);
            ELSE
                -- Simple value - exact equality
                target_level := (facet_filters->>'$level')::integer;
                where_conditions := where_conditions || pvt_build_level_condition(target_level, table_alias);
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

