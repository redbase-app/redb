-- =====================================================================
-- v2-pvt module version (MSSql)
-- =====================================================================
-- Applied LAST on purpose, see the PostgreSQL twin. The bundle runs as GO
-- batches with autocommit, so a failure in the middle leaves the earlier
-- batches applied; with the version created first (as until 0.1.9) such a
-- failure left the NEW version next to the OLD functions and nothing ever
-- retried. Step 2 of 00_module_init.sql drops dbo.pvt_module_version with
-- the rest; it is recreated here only after everything else succeeded.
-- =====================================================================
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

-- ---------- 3. Module version function ---------------------------------
-- semver: bump MAJOR on breaking changes to entry-point signatures or
-- result shape; bump MINOR on additive features; bump PATCH on bug fixes.
CREATE FUNCTION dbo.pvt_module_version()
RETURNS nvarchar(50)
WITH SCHEMABINDING
AS
BEGIN
    -- 0.1.2 - 13_pvt_condition.sql: pvt_build_field_condition dict_key
    --         branch now short-circuits to `_pvt_cte.[FieldName] OP val`
    --         when called in a post-pivot context (@base_prefix = 'o.'
    --         or '_pvt_cte.'). The nested-dict CTE already materializes
    --         the pivot column; emitting an independent EXISTS over
    --         dbo._values for the outer WHERE duplicated the lookup
    --         work. Symmetric with PG's pvt_build_field_condition.
    -- 0.1.1 - narrow-with-nested CTE shape + stable ORDER BY +
    --         Pro-parity nested-dict pushdown:
    --         * 12_pvt_cte_builder.sql: each LEFT JOIN nested derived
    --           table now folds `_id_scheme = X` (+ extra_where +
    --           tree_filter) into a dp._id_object IN (SELECT _id FROM
    --           _objects ...) subquery — mirrors PG PRO.
    --         * narrow-with-nested body skips INNER JOIN _values v
    --           when @sids = N'' (nested-only): no scalar pivot
    --           sids, no point in expanding+collapsing _values.
    --         * 20_pvt_build_query_sql.sql: narrow eligibility now
    --           allows nested groups; default ORDER BY @base_prefix
    --           + [_id] when paging is present without ORDER BY.
    -- 0.1.3 - fix: DISTINCT (@distinct=1) outer ORDER BY referenced the inner
    --         alias prefix (o./_pvt_cte.) outside the `_dist` wrapper -> "multi-part
    --         identifier 'o._id' could not be bound" with .Distinct().Take(). Outer
    --         order now uses the projected [_id] (@order_sql_dist) in all 3 branches.
    -- 0.1.4 - Soft-delete read-path fix + object-json materializer ownership:
    --         * The whole object->JSON materializer (dbo.get_object_json plus
    --           helpers build_properties / build_field_json / build_listitem_json
    --           / escape_json_string) moved from core (redb_json_objects.sql,
    --           now deleted) into the module (09_core_object_json.sql) so its
    --           fixes auto-redeploy to existing databases via the version check
    --           (full redb_init.sql is not re-run once _schemes exists).
    --         * dbo.get_object_json now treats soft-deleted objects
    --           (_id_scheme = -10, @@__deleted) as non-existent: a nested
    --           _Object reference to a trashed object resolves to NULL instead
    --           of materializing the tombstone. The _values pointer stays
    --           intact, so soft-delete remains reversible.
    -- 0.2.10 - base64 через FOR XML PATH вместо XML-инстанса (.value): 111 -> 21 мс на мегабайте.
    --   В STRING_AGG подзапрос запрещён (Msg 130) - массив и словарь берут значение из OUTER APPLY.
    -- 0.2.9 - root value_bytes emitted (base64 via XML trick) - was absent entirely, read gave null
    --   (bug report BUG_BYTES �3.3 read side).
    -- 0.2.8 - техбамп-2: OUTER APPLY попал в соседние блоки (0.2.7 битый в тестовых базах).
    -- 0.2.7 - техбамп: T-SQL запрещает подзапрос в STRING_AGG, Р-1 переехал на OUTER APPLY
    --   (0.2.6 успел задеплоиться битым в тестовые базы).
    -- 0.2.6 - Р-1 (2026-09-03): legacy-толерантность «без _Guid И без детей» (оба места).
    -- 0.2.5 - В-1 «null должен быть null» (2026-09-03): null-элемент коллекции классов
    --   эмитится JSON-null (массив и словарь).
    -- 0.2.4 - BR-7 (2026-09-02): dbo.pvt_like_escape() - the operand of every LIKE sugar
    --   operator (field, dict, listitem, array, expression) is a LITERAL now, paired with
    --   ESCAPE; the raw $like/$arrayMatches operators keep the caller's pattern.
    -- 0.2.3 - the unread full-text index on _values._String is dropped (owner decision
    --   2026-09-02): LIKE never used it, CONTAINS was never generated.
    -- 0.2.2 - V4 LAZY L2 (lazy references):
    --   * block 0: _structures._lazy and _scheme_metadata_cache._lazy (the virtual marker).
    --   * 29: the metadata cache carries the marker.
    --   * 09 build_field_json: @lazy parameter from the cache cursor; a lazy reference
    --     emits the base-fields stub (depth 0) when SESSION_CONTEXT(N'redb.lazy_refs') = 1.
    -- 0.1.12 - V4 byte[] scalar storage (B1):
    --   * build_field_json fetched _ByteArray but had no emit branch at all, so a
    --     ByteArray property always loaded as null. Base64 emit added to the three
    --     CASE paths (scalar, array element, dictionary value) via xs:base64Binary.
    -- 0.1.11 - V4 unique keys, stage 1 (UNIQUE_STRING_KEY_PLAN, Э1):
    --   * block "0. Schema upgrades": _objects._value_unique NVARCHAR(440) and the filtered
    --     unique index UIX__objects__scheme_unique (INCLUDE _id; 8+880 = 888 < 900).
    --   * get_object_json and the PVT base-field surface emit and filter the column
    --     (07/09/12/01), so the key behaves exactly like _value_string.
    --   * sp_mark_for_deletion (30_soft_delete.sql) also releases the object key on the
    --     way into the trash (decision 9).

    -- 0.1.10 - V4 unique keys, stage 2 (UNIQUE_STRING_KEY_PLAN, Э2):
    --   * block "0. Schema upgrades" opens 00_module_init.sql: _structures._unique,
    --     _structures._unique_version, _values._unique UNIQUEIDENTIFIER, the filtered unique
    --     index UIX__values__structure_unique, and the same two columns on
    --     _scheme_metadata_cache (SCHEMA_DELIVERY plan, work S.1).
    --   * 29_metadata_cache_sync.sql: sync_metadata_cache_for_scheme and
    --     warmup_all_metadata_caches move in from sql/redb_metadata_cache.sql.
    --   * 30_soft_delete.sql: sp_mark_for_deletion and sp_purge_trash move in from
    --     sql/redb_soft_delete.sql; sp_mark_for_deletion now releases unique keys
    --     (_values._unique = NULL) in the same transaction (decision 9).

    -- 0.1.8 - the DateOnly seed correction joins the module:
    --         * 28_migrate_dateonly_db_type.sql moved in from sql/. DateOnly was
    --           seeded with _db_type = 'DateTime', which no JSON projection
    --           branches on, so every DateOnly property materialised as
    --           0001-01-01. The correction lived in sql/, which lands only in
    --           redb_init.sql and is applied when the tables are absent, so it
    --           reached new databases and no existing one.
    --         * Idempotent by construction, as it must be: the bundle is
    --           reapplied on every version change.
    -- 0.1.7 - migrate_structure_type joins the module, and its String -> Boolean
    --         conversion is guarded:
    --         * 27_migrate_structure_type.sql moved in from sql/. It used to ship
    --           only in redb_init.sql, applied to fresh databases only, so a fix
    --           to it never reached an existing one. In the module, the version
    --           check redeploys it like everything else here.
    --         * String -> Boolean destroyed unrecognised values: the CASE fell
    --           through to NULL while the same statement cleared _String, and the
    --           row counted as a success. It is now predicated on the accepted
    --           token list, like every other text conversion in that procedure,
    --           which already used TRY_CAST(...) IS NOT NULL.
    -- 0.1.6 - Scoped WhereLeaves()/WhereRoots() cross-tree leak fix:
    --         * 20_pvt_build_query_sql.sql tree_leaves/tree_roots fast-path now
    --           honours the seed: leaves = childless descendants of the seed root
    --           (via pvt_is_descendant_of), roots = the seed object itself —
    --           instead of a whole-scheme scan that ignored @tree_ids. So
    --           TreeQuery(rootObj).WhereLeaves() returns the leaves of that
    --           subtree, not every leaf in the scheme.
    --         * pvt_tree_leaves / pvt_tree_roots (08_pvt_tree_functions.sql) —
    --           the pvt_build_cte_sql (props-shape) path — seeded the same way.
    -- 0.2.14 - soft delete resets _objects._hash (full-object hash, 2026-09-11): the trash
    --          move changes parent and key outside the save path; a props-cache copy must not
    --          outlive it.
    -- 0.2.13 - block 0: S3 - _structures._unique_scope, _tags on schemes/structures/cache,
    --          IX__structures__tags; cache sync carries the two new columns.
    -- 0.2.12 - block 0: UIX__values__structure_unique recreated without the positional filter
    --          (S2, subtree-unique plan): nested scalar keys carry _array_parent_id, the old
    --          predicate kept them out of the index, so their uniqueness silently never fired.
    -- 0.2.11 - block 0: FK-column indexes IX__values__ListItem_not_null /
    --          IX__values__Object_not_null (filtered). Without them every DELETE of a
    --          referenced object or list item scanned the whole _values for the FK check
    --          (perf finding, 2026-09-10).
    -- 0.1.0 - skeleton: module bootstrap, drop-all, version function.
    --         Builder functions (pvt_build_query_sql etc.) not implemented yet.
    RETURN N'0.2.14';
END;
GO

-- ---------- 4. Smoke -----------------------------------------------------
DECLARE @v nvarchar(50) = dbo.pvt_module_version();
PRINT N'v2-pvt module init OK, version: ' + @v;
GO
