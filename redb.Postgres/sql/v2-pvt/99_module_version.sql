-- =====================================================================
-- v2-pvt module version (PostgreSQL)
-- =====================================================================
-- This file is applied LAST on purpose. The version function is what the
-- C# client compares on start-up to decide whether the module must be
-- (re)applied, so it must come into existence only after every other file
-- of the module has succeeded. Step 2 of 00_module_init.sql drops every
-- pvt_* function, this one included; a failure anywhere in the middle of
-- the bundle therefore leaves the database with NO version function, the
-- next start treats the module as missing and applies the bundle again.
-- With the function created first (as it was until 0.6.8) a mid-bundle
-- failure left the NEW version next to the OLD functions, and nothing ever
-- retried.
-- =====================================================================

-- ---------- 3. Module version function ---------------------------------
-- semver: bump MAJOR on breaking changes to entry-point signatures or
-- result shape; bump MINOR on additive features; bump PATCH on bug fixes.
CREATE OR REPLACE FUNCTION pvt_module_version()
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
AS $BODY$
BEGIN
    -- 0.7.9 - soft delete resets _objects._hash (full-object hash, 2026-09-11): the trash move
    --   changes parent and key outside the save path; a props-cache copy must not outlive it.
    -- 0.7.8 - block 0: S3 - _structures._unique_scope (element-key scope), _tags on schemes,
    --   structures and the metadata cache (free-form marker), IX__structures__tags;
    --   sync_metadata_cache_for_scheme carries the two new columns.
    -- 0.7.7 - block 0: UIX__values__structure_unique recreated without the positional filter
    --   (S2, subtree-unique plan): nested scalar keys carry _array_parent_id, the old predicate
    --   kept them out of the index, so their uniqueness silently never fired.
    -- 0.7.6 - block 0: FK-column indexes IX__values__ListItem_not_null / IX__values__Object_not_null
    --   (partial). Without them every DELETE of a referenced object or list item seq-scanned the
    --   whole _values for the FK check (perf finding, 2026-09-10).
    -- 0.7.5 - root value_bytes emitted as base64 (was bytea hex; bug report BUG_BYTES �3.2).
    -- 0.7.4 - Р-1 (2026-09-03): legacy-толерантность - null-элемент класса определяется
    --   как «без _Guid И без детей»; класс без исторического хеша с детьми читается.
    -- 0.7.3 - В-1 «null должен быть null» (2026-09-03): null-элемент коллекции классов
    --   (строка без _Guid) эмитится JSON-null, а не пустым объектом.
    -- 0.7.2 - BR-7 (2026-09-02): pvt_like_escape() - the operand of every LIKE/ILIKE sugar
    --   operator (field, class-field, dict, array, expression) is a LITERAL now; the raw
    --   $like/$ilike/$matches operators keep the caller's pattern.
    -- 0.7.1 - V4 LAZY L2 (lazy references):
    --   * block 0: _structures._lazy and _scheme_metadata_cache._lazy (the virtual marker).
    --   * 29: the metadata cache carries the marker.
    --   * 08 get_object_json: a reference whose structure is _lazy emits the base-fields
    --     stub (depth 0) when the session GUC redb.lazy_refs = '1' - regardless of depth.
    -- 0.6.11 - V4 byte[] scalar storage (B1):
    --   * get_object_json: the ByteArray emit decoded _ByteArray::text as base64,
    --     but bytea::text is hex ('\x...') - error 22023 on the first reachable
    --     value. Now encode(v._ByteArray, 'base64') directly (2 sites in 08).
    -- 0.6.10 - V4 unique keys, stage 1 (UNIQUE_STRING_KEY_PLAN, Э1):
    --   * block "0. Schema upgrades": _objects._value_unique varchar(440) and the partial
    --     unique index UIX__objects__scheme_unique (INCLUDE _id).
    --   * get_object_json and the PVT base-field surface emit and filter the column
    --     (07/08/12/13/01), so the key behaves exactly like _value_string.
    --   * mark_for_deletion (30_soft_delete.sql) also releases the object key on the way
    --     into the trash (decision 9).

    -- 0.6.9 - V4 unique keys, stage 2 (UNIQUE_STRING_KEY_PLAN, Э2):
    --   * block "0. Schema upgrades" opens 00_module_init.sql: _structures._unique,
    --     _structures._unique_version, _values._unique uuid, the partial unique index
    --     UIX__values__structure_unique, and the same two columns on _scheme_metadata_cache -
    --     the delivery of DDL to existing databases (SCHEMA_DELIVERY plan, work S.1).
    --   * 29_metadata_cache_sync.sql: sync_metadata_cache_for_scheme and
    --     warmup_all_metadata_caches move in from sql/redb_metadata_cache.sql - they carry
    --     the cache column list, which otherwise reaches fresh databases only.
    --   * 30_soft_delete.sql: mark_for_deletion and purge_trash move in from
    --     sql/redb_soft_delete.sql; mark_for_deletion now releases unique keys
    --     (_values._unique = NULL) in the same transaction (decision 9).

    -- 0.6.8 — version function moved to 99_module_version.sql, applied LAST:
    --   a failure in the middle of the bundle used to leave the NEW version next
    --   to the OLD functions, and the next start saw no mismatch. Now the
    --   function is dropped with the rest in step 2 and recreated only after
    --   every other file succeeded. No behaviour change on a healthy deploy.
    -- 0.6.7 — the DateOnly seed correction joins the module:
    --   * 28_migrate_dateonly_db_type.sql moved in from sql/. DateOnly was seeded
    --     with _db_type = 'DateTime', a value get_object_json has no branch for,
    --     so every DateOnly property materialised as 0001-01-01. The correction
    --     was written but lived in sql/, which lands only in redb_init.sql and is
    --     applied when the tables are absent — so it reached new databases and no
    --     existing one. Nothing else applied it: it was dead on arrival.
    --   * Idempotent by construction (AND _db_type <> 'DateTimeOffset'), which it
    --     has to be: the bundle is reapplied on every version change.
    -- 0.6.6 — migrate_structure_type joins the module, and its text conversions
    --   are guarded:
    --   * 27_migrate_structure_type.sql moved in from sql/. It used to ship only
    --     in redb_init.sql, which is applied to fresh databases only, so a fix to
    --     it never reached an existing one. Living in the module means the
    --     version check redeploys it like everything else here.
    --   * String -> Boolean destroyed unrecognised values: the CASE fell through
    --     to NULL while the same statement cleared _String, and the row counted
    --     as a success. Now predicated on the accepted token list, so anything
    --     else stays put and lands in error_count.
    --   * String -> DateTimeOffset and String -> Guid were bare casts that raised
    --     on the first bad row and aborted the whole migration. Now guarded, as
    --     the numeric branches always were and as MSSQL's TRY_CAST already did.
    -- 0.6.5 — Unicode-aware case folding for the Free query path:
    --   * new pvt_fold_case(text): wraps an expression in COLLATE when the
    --     redb.string_collation GUC is set, and returns it untouched otherwise.
    --     Case folding is driven by the database ctype, so on a database created
    --     with LC_CTYPE=C the ILIKE/LOWER/UPPER family folds ASCII only and
    --     'Привет' ILIKE '%привет%' is false. The GUC is set by the C# provider
    --     per connection, which keeps every existing function signature intact.
    --   * applied in 13_pvt_condition.sql and 17_pvt_expr.sql at every ILIKE,
    --     and at $lower/$upper, so the three operations cannot disagree.
    -- 0.6.4 — Scoped WhereLeaves()/WhereRoots() cross-tree leak fix:
    --   * 12_pvt_cte_builder.sql tree_leaves/tree_roots now honour the seed as a
    --     SUBTREE ROOT (descend, then apply the leaf/root predicate) instead of an
    --     exact-id membership, so TreeQuery(rootObj).WhereLeaves() returns the leaves
    --     of that subtree — not every leaf in the scheme.
    -- 0.6.3 — Soft-delete read-path fix + object-json materializer ownership:
    --   * The whole object->JSON materializer (get_object_json, get_objects_json,
    --     build_hierarchical_properties_optimized, build_listitem_jsonb) moved
    --     from core (redb_json_objects.sql, now deleted) into the module
    --     (08_core_object_json.sql) so its fixes auto-redeploy to existing
    --     databases via the version check (full redb_init.sql is not re-run
    --     once _schemes exists).
    --   * get_object_json() now treats soft-deleted objects
    --     (_id_scheme = -10, @@__deleted) as non-existent: a nested
    --     _Object reference to a trashed object resolves to NULL instead
    --     of materializing the tombstone. The _values pointer stays
    --     intact, so soft-delete remains reversible.
    -- 0.6.2 — Nested-dict object-set pushdown (mixed scalar+nested):
    --   * 12_pvt_cte_builder.sql now folds the object-set restriction
    --     (scheme + base pushdown + tree filter) into every
    --     nested_dict_N CTE's WHERE, not just the nested-only path.
    --     Mixed scalar+nested queries were previously scanning the
    --     full parent_sid partition of _values and gating via the
    --     outer JOIN; with this change PG prunes dp rows by
    --     _id_scheme BEFORE the LEFT JOIN nv expansion.
    -- 0.6.1 — ListItem.Value/.Alias Pro-parity perf:
    --   * pvt_build_cte_sql and the inline GROUP BY path in
    --     pvt_build_groupby_sql now emit a single
    --     `LEFT JOIN _list_items li ON li._id = v._ListItem` on the
    --     pivot source whenever any field projects `list_item_prop`
    --     in (Value, Alias). Per-column expressions reference bare
    --     `li._value` / `li._alias` and aggregate via
    --     `array_agg(li.<col>) FILTER (...)`.
    --   * Replaces N per-column correlated subselects
    --     `(SELECT li._value FROM _list_items li WHERE li._id = v._ListItem)`
    --     with one JOIN per pivot — matches Pro PivotSqlGenerator.
    --   * Scalar Value/Alias pivot column still holds resolved text
    --     (Free LINQ passes string literals; comparison is `= '...'::text`).
    -- 0.6.0 — Pro-parity perf rewrite (large-scale ops):
    --   * #1 Filter pushdown: pvt_split_filter detects narrow filter sets
    --     that contain no base refs and inlines the residual WHERE
    --     inside _pvt_cte with an explicit `SELECT pvt._id_object, pvt."col", ...`
    --     wrapper (pvt_filter_has_base_refs gate + explicit-cols
    --     projection in pvt_build_cte_sql). Outer WHERE collapses to TRUE.
    --   * #2 GROUP BY inline subquery: pvt_build_groupby_sql now skips
    --     the CTE for pure-scalar narrow shapes and emits
    --     `SELECT pvt.<grp>, agg(...) FROM (<inline pivot>) pvt`.
    --     `v._array_index IS NULL` is lifted from per-column FILTER into
    --     the inline subquery's outer WHERE — index-friendly at 100M+ rows.
    --     pvt_build_column_expr gained p_array_index_in_outer for this.
    --   * #3 Nested-dict side CTE: a single LEFT JOIN _values + per-field
    --     `array_agg(...) FILTER (...)` replaces N correlated subselects.
    --     SID list collapses to `IN (...)` (or `= sid`) with dedup.
    -- 0.5.0 — Expression engine (Pro parity, capability):
    --   * 17_pvt_expr.sql introduces pvt_build_scalar_expr (recursive
    --     compiler for $field/$const/arithmetic/Math/String/Concat/
    --     Coalesce/Cast) and pvt_build_expr_predicate (full predicate
    --     family $eq..$gte / $like / $ilike / $in / $nin / $between /
    --     $null / $notNull / $contains[IgnoreCase] / $startsWith / $endsWith).
    --   * pvt_build_where_from_json and pvt_split_filter route
    --     filter-level expression-form predicates through the new engine.
    --   * pvt_extract_field_pairs harvests $field references from
    --     expression subtrees so pvt_collect_fields resolves them.
    --   * Pushdown: expression predicates are pushed iff every $field
    --     reference inside resolves to kind=base (pvt_expr_is_base_only).
    -- 0.4.0 — Base-field pushdown (Pro parity, perf):
    --   * pvt_split_filter walks the filter and peels off base/hierarchical
    --     predicates into a SQL fragment over `_objects o.*`.
    --   * pvt_build_cte_sql accepts p_extra_where and ANDs it into the
    --     inner WHERE so PG can use system-column indexes BEFORE the
    --     JOIN with _values and the GROUP BY agg.
    --   * pvt_build_field_condition gained p_base_prefix; passed as 'o.'
    --     in pushdown context, '' (default) in the outer CTE WHERE.
    --   * $or/$not are pushed only when every leaf inside is base —
    --     mixed branches keep the original semantics.
    -- 0.3.0 — Pro parity rewrite:
    --   * `(array_agg(v.<col>) FILTER (...))[1]` idiom (works for bool/uuid/etc).
    --   * `_array_index IS NULL` filter for scalars (NOT `_array_parent_id IS NULL`).
    --   * `0$:` base-field prefix stripping in pvt_normalize_base_field_name.
    --   * full collection / nested / dictionary / ListItem.Value/Alias / array-op support.
    RETURN '0.7.9';
END;
$BODY$;

COMMENT ON FUNCTION pvt_module_version() IS
    'Returns the semver of the v2-pvt module. Used by the C# client on InitializeAsync to enforce compatibility (major must match, deployed minor >= required).';

DO $$
BEGIN
    RAISE NOTICE 'v2-pvt module init OK, version: %', pvt_module_version();
END $$;
