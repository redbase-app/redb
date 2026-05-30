# Changelog

All notable changes to RedBase will be documented in this file.
This changelog covers the **NuGet-published packages** only:

| Package | Edition |
|---------|---------|
| `RedBase.Core` | Free |
| `RedBase.Postgres` | Free |
| `RedBase.MSSql` | Free |
| `RedBase.Export` | Free |
| `RedBase.Core.Pro` | Pro |
| `RedBase.Postgres.Pro` | Pro |
| `RedBase.MSSql.Pro` | Pro |
| `RedBase.CLI` | Tool |

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [3.0.0] — 2026-05-28

### Added
- **PG Free: full v2-pvt query engine reaches Pro-parity (0.5.x → 0.6.1)**.
  The PostgreSQL Free path got the feature-complete v2-pvt module ahead of
  MSSql Free (commits 2026-05-21 … 2026-05-28). Before this series the Free
  path was emitting `-- not available in Open Source` stubs for several
  preview surfaces and was missing several Pro-only operators. Now in Free
  on PG:
  - **Universal "no black box" SQL preview** for `GroupBy` / `Window` /
    `GroupedWindow` / `Tree-*` via two-pass compile (`pvt_build_*_sql`);
    tree previews resolve the subtree and delegate to the matching non-tree
    preview with a `-- Tree …: subtree resolved to N object(s)` header.
  - **`Sql.Function<T>` whitelist** at the SQL boundary
    ([17_pvt_expr.sql](redb.Postgres/sql/v2-pvt/17_pvt_expr.sql)) with a
    hardcoded ELSIF chain and `RAISE EXCEPTION` for non-whitelisted names;
    parser routes `Sql.Function<T>(name, args)` to
    `CustomFunctionExpression` (FREE-OVER-PRO §2.4).
  - **`ValueTuple` composite dict keys** (`Dictionary<(int,int), V>`)
    consistently encoded as Base64-JSON on both write and read sides
    (FREE-OVER-PRO §2.2).
  - **`arr.Length` / `coll.Count`** in filters via the array-aware
    `FacetFilterBuilder` (`.$count` modifier in Free); `e.Tags.Any()`
    1-arg form mapped to `<field>.$length > 0`.
  - **`Take(0)` returns empty** instead of `ArgumentException`.
  - **`HAVING` parser + `ArrayGroupBy`** with PVT agg array `unnest`
    ([19_pvt_agg_expr.sql](redb.Postgres/sql/v2-pvt/19_pvt_agg_expr.sql)) —
    fixes `42883 function sum(bigint[]) does not exist`;
    [26_pvt_array_groupby.sql](redb.Postgres/sql/v2-pvt/26_pvt_array_groupby.sql)
    added.
  - **`ListItem.Value` / `.Alias` via a single `LEFT JOIN _list_items`**
    (v2-pvt 0.6.1) — plan-shape parity with Pro; replaces correlated
    subquery per field.
  - **Nested-dict CTE pushdown** for `Field[key].Child` (FREE-OVER-PRO §2.x):
    outer `WHERE` references the already-built pivot column instead of a
    redundant `EXISTS` over `_values`.
  - **Auto-deploy of the v2-pvt bundle on version mismatch** (see the
    matching item below — same infrastructure serves both PG and MSSql).

  The MSSql Free engine described next ports this PG Free baseline; the
  parity line in the next item ("145/145 parity with PG Free") refers to
  this newly-completed PG Free feature set, not a pre-existing one.

- **MSSql Free: full v2-pvt query engine (0.1.0 → 0.1.3) — 145/145 parity
  with PG Free**. The old MSSql Free path generated a wide inline CASE WHEN
  aggregate; it is now replaced with the Pro-shape CTE: a single pass over
  `_values` using `MAX(CASE WHEN _id_structure = X AND _array_index IS NULL
  THEN ...)` and a single `LEFT JOIN _list_items`. All modes present in PG
  Free are implemented: flat/tree, scalar/array/dict fields, ListItem
  (`.Id`/`.Value`/`.Alias`), same-scheme nested POCO (compound path),
  `OrderBy`/`DistinctBy`/`Take`/`Skip`, `GroupBy`/`HAVING`, `ArrayGroupBy`
  (via `OUTER APPLY`), array aggregates (`$count`, `$sum`/`$avg`/`$min`/`$max`
  over `_Long`/`_Double`/`_Numeric`/`_DateTimeOffset`), array operators
  (`$arrayContains`, `$arrayAny`, `$arrayCount*`, `$arrayAt`,
  `$arrayStartsWith`, etc.), `Sql.Function` (whitelist), `$expr`, null
  semantics (`$exists`/`$notNull`). The SQL module is split into 27 source
  files under [redb.MSSql/sql/v2-pvt/](redb.MSSql/sql/v2-pvt/) assembled
  into a single `pvt_bundle.sql` by MSBuild. Delivery stages: Stage 1 (pivot
  CTE) → 2a (tree TVFs) → 2b (tree provider) → 2c.E (nested-dict accessor
  `Field[key].Child`) → 0.1.1 LIKE-pattern fix → 0.1.2 string `$const`
  unwrap + ListItem `$arrayContains` → 0.1.3 nested-dict CTE pushdown +
  outer `WHERE` references pivot column instead of a redundant `EXISTS`.
  **Shape parity with Pro throughout**: `_id_scheme` + `extra_where` +
  tree-filter pushed into inner `_objects` subquery, narrow-with-nested CTE
  (skips `_values` JOIN when no scalar sids), stable default `ORDER BY` when
  paging without an explicit order.

- **Auto-deploy v2-pvt bundle on version mismatch (both databases)**.
  `ISqlDialect` gained `Query_PvtRequiredVersion()` — the semver the embedded
  bundle ships. `RedbServiceBase.EnsurePvtModuleDeployedAsync` reads
  `pvt_module_version()` on `InitializeAsync()`, compares with an exact-match,
  and automatically applies the embedded `pvt_bundle.sql` resource when the
  deployed version differs. No more manual `DROP FUNCTION … CREATE FUNCTION …`
  after a SQL change. The MSBuild target `ConcatenateSqlFiles` regenerates the
  bundle whenever any `.sql` source changes (hooked to `DispatchToInnerBuilds`
  for multi-TFM builds; `EmbeddedResource` uses an explicit `LogicalName` —
  without it MSBuild silently replaces `-` with `_` in resource paths, causing
  `GetManifestResourceStream` to return `null`).

- **Pro: `GroupBy` + `HAVING` via PVT pipeline on both providers
  (Postgres.Pro + MSSql.Pro)**. `HavingAsync` existed in Free but had no Pro
  counterpart. Added full HAVING parser in the shared facet layer
  (`FacetFilterBuilder`), SQL generation in both Pro providers, and a base
  test suite in
  [GroupByHavingTestsBase](redb.Tests.Integration/Tests/Base/GroupByHavingTestsBase.cs)
  with per-dialect wrappers (PG, PG.Pro, MSSql.Pro). 33/33 HAVING + 6/6
  no-HAVING — all green.

- **Pro: `GroupBy` over array fields (`ArrayGroupBy`) — unified implementation
  for Postgres.Pro + MSSql.Pro**. PG.Pro uses an inline `GroupByArray` override
  with PVT agg array `unnest`; MSSql.Pro has its own override.
  `GroupBy(items => items.SelectMany(o => o.Skills))` with aggregates works
  on all four tiers (PG Free, PG.Pro, MSSql Free, MSSql.Pro).

- **MSSql Pro: `AggregateBatch` parity with PG.Pro — non-numeric MIN/MAX and
  inline filter subquery**. `MinAsync`/`MaxAsync` over `string`/`DateTime`/`Guid`
  fields and a `Where` filter inside a batch aggregation now produce the same
  query shape as PG.Pro (PVT CTE + outer aggregate).

- **MSSql Free: pushdown parity with Pro/PG for expression-form predicates
  and `$expr`** — the filter-splitting optimizer
  [`pvt_split_filter`](redb.MSSql/sql/v2-pvt/16_pvt_split.sql) now pushes
  top-level `$eq/$ne/$lt/$lte/$gt/$gte/$like/$ilike/$in/$nin/$between/$null/
  $notnull/$contains/$startsWith/$endsWith` expressions and arbitrary boolean
  `$expr` trees into the inner `_objects o` subquery (Shape A) when all
  `$field` references resolve to `kind='base'`. If any props field is present
  the node stays in the residual (Shape C). The new classifier
  [`pvt_expr_is_base_only`](redb.MSSql/sql/v2-pvt/17_pvt_expr.sql) makes
  this decision; the pushdown SQL itself is generated by the existing
  [`pvt_build_where_from_json`](redb.MSSql/sql/v2-pvt/14_pvt_where.sql)
  walker (extended with a `$expr` branch). Covered by 4 functional and 3
  shape-inspect tests in
  [`99_smoke_auto.sql`](redb.MSSql/sql/v2-pvt/99_smoke_auto.sql)
  (195 PASS / 0 FAIL / 1 SKIP).

### Fixed
- **Schema sync now honors `Configuration.DefaultStrictDeleteExtra`**
  (FREE-OVER-PRO §4 #1). Prior to this fix `RedbServiceConfiguration.DefaultStrictDeleteExtra`
  was set by builders, copied across configuration clones and read from
  `appsettings`, but **no execution-path code consumed it** —
  [`SchemeSyncProviderBase.SyncSchemeAsync<T>`](redb.Core/Providers/Base/SchemeSyncProviderBase.cs)
  hardcoded `strictDeleteExtra: true`, so old binaries restarting in a
  multi-version rolling deploy would unconditionally remove `_structures`
  rows added by the new binary, and every `_values` row referencing those
  structures along with them. On PostgreSQL this is done via the FK
  `_values._id_structure -> _structures._id ON DELETE CASCADE`
  ([redbPostgre.sql:215](redb.Postgres/sql/redbPostgre.sql#L215)). On MSSQL
  the same effect is produced by the `INSTEAD OF DELETE` trigger
  `TR__structures__cascade_values`
  ([redbMSSQL.sql:717](redb.MSSql/sql/redbMSSQL.sql#L717)) — the FK
  `NO ACTION` at [redbMSSQL.sql:270](redb.MSSql/sql/redbMSSQL.sql#L270)
  is a workaround for the MSSQL multiple-cascade-paths restriction, not a
  behavioral difference. `SyncSchemeAsync<T>` now reads
  now reads `Configuration.DefaultStrictDeleteExtra` instead. The default
  value is preserved (`true`) so users on the default config see no
  behavioral change. **Behavioral change**: the built-in presets
  `Development`, `HighPerformance`, and `Migration` (in
  [`PredefinedConfigurations.cs`](redb.Core/Models/Configuration/PredefinedConfigurations.cs))
  already declared `DefaultStrictDeleteExtra = false`; that setting was
  silently ignored before and now actually takes effect — apps on those
  presets will no longer auto-delete `_structures` rows missing from the
  `Props` class on startup.

 Added
  a fallback to `ROW_NUMBER() OVER (PARTITION BY <key> ORDER BY (SELECT 1))`
  + `WHERE _rn = 1` (symmetric with the Free path), plus support for
  `CoalesceExpression` in the `DistinctBy` key.

- **PG v2-pvt 0.6.1: ListItem `.Value`/`.Alias` now uses a single
  `LEFT JOIN _list_items`** instead of a correlated subquery per field —
  plan-shape parity with Pro. Additionally: nested-dict predicates in the
  outer `WHERE` now reference `_pvt_cte.[<field>]` (the already-built pivot
  column) instead of re-running a separate `EXISTS` over `_values`.

- **MSSql Free: `ORDER BY $expr` on base fields no longer produces "constant
  in ORDER BY"** — two regressions fixed: (1)
  [`pvt_collect_fields`](redb.MSSql/sql/v2-pvt/10_pvt_field_collection.sql)
  did not walk `$expr` nodes in order entries, so a field like `Age` was not
  collected, the shape was classified as A, and `pvt_b2_expr_sql` emitted
  `/*unknown-b2-field:Age*/NULL` turning `Age*2` into a constant; (2)
  [`pvt_build_order_conditions`](redb.MSSql/sql/v2-pvt/15_pvt_order.sql)
  passed a trailing-dot alias (`_pvt_cte.`) into `pvt_b2_expr_sql`, producing
  the double-dot `_pvt_cte..[_name]` for base fields inside `$expr` ORDER.
  Both sites fixed.

- **`arr.Length` / `coll.Count` in `Where` filters no longer crash on array
  PVT columns** — `e.Skills!.Length >= 3` was translated to `LENGTH(text[])`
  and raised PostgreSQL error 42883.
  [BaseFilterExpressionParser](redb.Core/Query/Parsing/BaseFilterExpressionParser.cs)
  now emits `PropertyFunction.Count` (instead of `PropertyFunction.Length`)
  for CLR `UnaryExpression(ArrayLength)` nodes. In Pro this produces
  `COALESCE(array_length(col,1), 0)`; in Free,
  [FacetFilterBuilder.TryBuildArrayLengthCountFilter](redb.Core/Query/FacetFilterBuilder.cs)
  translates the filter to the PVT modifier `.$count`. `PropertyInfo` gained
  an optional `FunctionSourceType` field so the facet builder can distinguish
  arrays from strings when choosing the modifier. Covered by
  `PropertyFunction_ArrayCount_Filters` on both tiers.

- **`Take(0)` now returns an empty result instead of `ArgumentException`** —
  validation in `RedbQueryable.Take()` and `TreeQueryableBase.Take()` relaxed
  from `count <= 0` to `count < 0` to match standard LINQ semantics
  (`Enumerable.Take(0)` → empty). Affects both tiers (Free and Pro), flat and
  tree queries. Covered by `Take_Zero_ReturnsEmpty_WithoutThrowing` and
  `Take_Zero_ReturnsEmpty_OnTreeQuery` in `PvtAuditTestsBase`.

### Tests
- `PostgresFreePvtAuditTests` moved to the shared base
  [PvtAuditTestsBase](redb.Tests.Integration/Tests/Base/PvtAuditTestsBase.cs)
  and now runs against both `PostgresFixture` (Free) and `PostgresProFixture`
  (Pro) — a regression on either tier fails immediately. Added tests for
  `Take(0)` (flat + tree), `Take(-1)` (still throws), and `DistinctBy` on a
  tree query.
- **Three audit probes from FREE-OVER-PRO §2.x confirmed working** on both
  tiers without any SQL/parser changes — tests were the only missing piece:
  - `DictTupleKey_PerformanceReviews_FiltersByCompositeKey` (§2.2) —
    `ValueTuple` dict keys are encoded by `RedbKeySerializer` to Base64-JSON
    consistently on the write side and in
    [BaseFilterExpressionParser L602](redb.Core/Query/Parsing/BaseFilterExpressionParser.cs#L602).
  - `ObjectRef_CurrentProject_NotNull_Filters` /
    `ObjectRef_CurrentProject_IsNull_Filters` (§2.3, null-check path) —
    `e.CurrentProject != null` / `== null` on `RedbObject<T>?` fields works
    via `$exists` / `$ne null`.
  - `SqlFunction_Coalesce_Filters` + `SqlFunction_UnknownName_ThrowsWhitelistViolation`
    (§2.4) — `Sql.Function<T>(name, args)` is routed by the parser to
    `CustomFunctionExpression`, `FacetFilterBuilder` emits
    `{"$<funcname>": [...]}`, and `pvt_build_scalar_expr`
    ([17_pvt_expr.sql](redb.Postgres/sql/v2-pvt/17_pvt_expr.sql)) implements
    the whitelist with a hardcoded ELSIF chain and `RAISE EXCEPTION` for
    unknown names.
  - Full PG suite (Free + Pro): **328 passed / 0 failed / 2 skipped**. The
    two remaining skips are `ObjectRef_CurrentProject_NestedField_Filters`
    (cross-scheme JOIN path, confirmed broken in both tiers — requires new
    infrastructure in both PVT and `ProQueryProvider`).
- **ListItem `.Value`/`.Alias` `OrderBy` capability gate** — PG Free PVT
  sorts by `Status.Value`/`.Alias` correctly (on par with Pro); the
  `if (IsPro)` guard in `ListItem_OrderByValue_SortsAlphabetically` /
  `ListItem_OrderByAlias_SortsAlphabetically` was overly conservative.
  Added virtual `SupportsListItemValueAliasOrdering` (default = `IsPro`) in
  `ListTestsBase`; `PostgresListTests` overrides to `true`. Result: PG Free +
  PG Pro + MsSql Pro pass with strict ordering; MsSql Free remains gated
  (insertion-order only — `ORDER BY` on a JSON expression is ignored).

### Documentation
- **New section "Schema lifecycle and multi-version deployments"** in the
  root [README.md](README.md): documents read=graceful / write=destructive,
  the `services.AddRedb(... .Configure(c => c.DefaultStrictDeleteExtra = false))`
  opt-out, the new warning log, and the equivalent cascade semantics across
  backends \u2014 PostgreSQL uses FK `ON DELETE CASCADE` on
  `_values._id_structure`, while MSSQL achieves the same effect through the
  `TR__structures__cascade_values` `INSTEAD OF DELETE` trigger (MSSQL FK is
  `NO ACTION` only to work around the multiple-cascade-paths restriction).
- Rewrote [docs/FreePvtQuery/FREE-OVER-PRO.md](docs/FreePvtQuery/FREE-OVER-PRO.md)
  §4: marked F0+F1 (this release) as done, demoted F3 (default flip) to a
  major-version task, made the cache-state-dependent nature of the Pro
  ChangeTracking destructiveness explicit (per-instance cache refresh window),
  and corrected §4.1 — the previous "obligatory `DefaultStrictDeleteExtra = false`"
  guidance was non-functional before v2.0.3 and is now actually wired.
- Updated [docs/FreePvtQuery/FREE-OVER-PRO.md](docs/FreePvtQuery/FREE-OVER-PRO.md):
  H1 (`Take(0)`) marked fixed; H8 (tree `DistinctBy`) re-classified as
  already implemented in both tiers; §0 and §2.x updated for §2.2 /
  §2.3-null / §2.4 closures; §1 #5 (`Sql.Function`) no longer marked
  unimplemented; §2 #3, #5, #6 marked done; added §2 #6b (deferred
  nested-field cross-scheme JOIN — confirmed as a two-sided gap in Free PVT
  and Pro `SchemeFieldResolver`); added §3 #11 (MsSql Free ignores
  `OrderBy(Status.Value)`/`.Alias`).

## [2.0.2] — 2026-05-16

### Changed
- **`EavSaveStrategy` renamed to `PropsSaveStrategy`** — users frequently asked
  whether RedBase uses the EAV (Entity-Attribute-Value) pattern. RedBase's
  storage model resembles EAV in structure (`_objects` + `_values`), but
  differs in key ways: schemes are strictly typed, fields are schema-bound
  (not free-form key-value pairs), and the query layer compiles LINQ directly
  to typed SQL without generic key-value lookups. The `Eav` prefix was
  misleading. Renamed throughout:
  - `EavSaveStrategy` enum → `PropsSaveStrategy`
  - `RedbServiceConfiguration.EavSaveStrategy` property → `PropsSaveStrategy`
  - JSON/appsettings key `"EavSaveStrategy"` → `"PropsSaveStrategy"`
    (**breaking**: update `appsettings.json` / environment variables if set explicitly)
  - `Tsak:Redb:EavSaveStrategy` config key → `Tsak:Redb:PropsSaveStrategy`

## [2.0.1] — 2026-05-08

### Fixed
- **Pro Props LINQ→SQL: 80× perf regression on mixed base + props filters** —
  when a query combined a base-field predicate (`WhereRedb(o => o._id_parent == X)`,
  `parentIds.Contains(o.ParentId.Value)`, etc.) with a props predicate
  (`Where(props => ...)`), the base predicate was applied as an outer `WHERE`
  **after** the PVT CTE aggregation (`array_agg FILTER` on Postgres /
  `MAX(CASE WHEN ...)` on MSSql). PVT was built over the entire scheme
  (millions of rows) and only then filtered down — observed 894 ms vs 11 ms.
  - The base predicate is now compiled with an empty table alias and pushed
    **into** the inner `(SELECT _id FROM _objects WHERE _id_scheme = X AND <baseFilter>)`
    subquery of the PVT CTE. The outer base `WHERE` is removed when pushdown
    fires, so generated SQL contains no duplicated predicate.
  - Affects flat queries (`ToListAsync`, `CountAsync`, `ExecuteDeleteAsync`),
    aggregations (`SumAsync` etc., aggregate batch), and window functions.
  - Both providers fixed symmetrically (`redb.Postgres.Pro`, `redb.MSSql.Pro`).
  - Added new public helper `ProSqlBuilder.CompileBaseFieldsForObjectsSubquery`
    that emits base-field SQL without the `o.` alias prefix for use inside
    the `_objects` subquery.

- **Pro Props LINQ→SQL: same regression on tree queries** — the tree variant
  (`AsTreeQuery`, `AsTreeWindowQuery`) had the identical anti-pattern: base-field
  predicate was applied as outer `WHERE` after the tree pvt_cte aggregation
  (`_objects oo JOIN tree t JOIN _values v` with `array_agg FILTER` /
  `MAX(CASE WHEN)`). With large trees + selective base filter (e.g.
  `_id_parent = X`), PVT aggregated all tree members before the predicate
  narrowed the result.
  - Base predicate is now pushed into the tree pvt_cte's inner
    `WHERE oo._id_scheme = X AND <baseFilter>` (with `oo` alias to disambiguate
    `_id`/`_id_parent`/`_hash` from the joined `tree t`).
  - The Tree-Window path (`BuildTreeWindowSqlTypedAsync`) now also pushes the
    base filter into `BuildPvtSubquery`'s additional WHERE
    (`o._id = ANY(ARRAY(SELECT _id FROM tree)) AND <baseFilter>` on Postgres,
    `o._id IN (SELECT _id FROM tree) AND <baseFilter>` on MSSql).
  - Tree-Aggregation uses correlated per-row subqueries and is not affected;
    Tree-GroupBy and Tree-GroupedWindow already pushed correctly.
  - `CompileBaseFieldsForObjectsSubquery` extended with optional
    `baseTableAlias` parameter (default `""`) — backwards-compatible for the
    flat case; tree case passes `"oo"`.
  - All 134 existing tree + ParentId integration tests pass on both providers.

### Added
- **Integration tests for the pushdown contract** in
  [redb.Tests.Integration/Tests/Base/WhereTestsBase.cs](redb.Tests.Integration/Tests/Base/WhereTestsBase.cs)
  covering `WhereRedb(parentIds.Contains(...)) + Where(props)` for
  `ToListAsync`, `CountAsync`, and `OrderBy + Take`. Tests run for both
  Postgres Pro and MSSql Pro fixtures.

## [2.0.0] — 2026-05-07

### Changed
- **License changed from MIT to Apache-2.0** for all OSS packages
  (`redb.Core`, `redb.Postgres`, `redb.MSSql`, `redb.CLI`, `redb.Export`,
  `redb.Templates`, `redb.PropsEditor`).
  - Apache 2.0 adds an explicit patent grant (§ 3) and termination clause —
    stronger protection for users and contributors.
  - All previously published versions (≤ 1.3.0) on nuget.org remain under MIT.
  - Pro packages (`*.Pro`, `redb.Licensing`) are unaffected — still under the
    commercial license in `LICENSE-PRO.txt`.
  - Every nupkg now ships `LICENSE` + `NOTICE` files (Apache 2.0 § 4 attribution).
  - Contributions are now accepted under Apache-2.0; see `CONTRIBUTING.md`.
- **Strong-Name signing** is now active for all Pro assemblies
  (Public Key Token: `8e6fea371ffeb38e`). This is a binary-identity change
  for Pro consumers — assembly identity differs from previous unsigned releases.

### Why this is a major version bump
- License change is a downstream-compliance breaking change.
- Pro Strong-Name change is a binary-identity breaking change.
- No source-level API changes vs 1.3.0.

## [1.3.0] — 2026-04-18

### Fixed
- **Nullable `.Value` in `WhereRedb` resolved to wrong column** — `o.ParentId.Value == 42` generated SQL against `_id` instead of `_id_parent`. Fixed in all parsers and Pro SQL compilers.
- **`.HasValue` generated `field = true` instead of `IS NOT NULL`** — `o.ParentId.HasValue` produced type mismatch (`bigint = boolean`). Now emits `IS NOT NULL` / `IS NULL`.
- **Props cache skipped hashless objects** — `LoadPropsForManyAsync` never added objects without `_hash` to `needToLoad`, leaving their Props null.
- **Missing `_hash` in nested object SQL** — `Materialization_SelectObjectsByIds` lacked `_hash` column; nested saves corrupted existing hashes.
- **Lexicographic ArrayIndex sorting** — arrays with 10+ items sorted as `"10" < "2"`, causing false ChangeTracking updates. Now uses numeric sort.
- **Duplicate RedbObject in SaveAsync** — `CollectNestedRedbObjectsFromProperties` didn't deduplicate by ID, crashing ChangeTracking `ToDictionary`.
- **MERGE duplicate row in ChangeTracking** — `BulkUpdateValuesAsync` received duplicate `_id` values. Added `DeduplicateValueUpdates` guard.
- **Missing `ArrayParentId` for nested RedbObject refs** — `ProcessSingleIRedbObject` didn't set `_array_parent_id` inside business class arrays, breaking ChangeTracking diffs.
- **Guid field not persisted** — `SetSimpleValueByType` lacked `case "Guid"`, saving to `_String` instead of `_Guid` column.
- **`DeleteSubtreeAsync` threw "Scheme for type Object not found"** — replaced `GetDescendantsWithUserAsync<object>()` with polymorphic `CollectDescendantIds`.
- **`LoadTreeAsync(maxDepth: 1)` returned no children** — off-by-one: `maxDepth` was decremented before recursion instead of inside it.
- **MsSql Pro `Where(x => x.Field == null)`** — `CompileNullCheck` now correctly generates `IS NULL` / `IS NOT NULL` against PVT CTE columns.
- **MsSql Pro `DistinctBy` returned duplicates** — added `ROW_NUMBER() OVER (PARTITION BY ...)` CTE wrapper with `WHERE _rn = 1`.
- **MsSql DELETE stored procedures** — `SET NOCOUNT OFF` for correct affected-rows count from `ExecuteNonQueryAsync`.
- **MsSql Free WHERE null (`$exists false`)** — generates `NOT EXISTS(...)` instead of `1=0`.
- **MsSql Free OrderBy** — zero-padded numeric conversion; `ROW_NUMBER()` preserves sort through JOIN.
- **`get_object_json` / `build_field_json`** — returns `"properties": null` for objects without `_values`. Array/dict without head records return NULL instead of `[]`/`{}`.
- **KeyGenerator shared cache** — domain-isolated `KeyCacheDomain` prevents duplicate key violations across providers.
- **SaveAsync deadlocks** — `ORDER BY _id` + `ROWLOCK` (MsSql) in locking queries; consistent lock ordering.
- **MsSql reader-writer deadlocks** — init script enables `READ_COMMITTED_SNAPSHOT ON`.
- **`SchemeFieldResolver` not domain-isolated** — per-domain cache with 5-min TTL and self-heal on cache miss.
- **`SyncSchemeAsync` didn't cache scheme** — schemes cached immediately after sync, eliminating extra DB roundtrips.
- **PropsSaveStrategy ignored in Tsak Worker** — reads `Tsak:Redb:PropsSaveStrategy` from config.
- **`SimplePasswordHasher`** — replaced custom compare with `CryptographicOperations.FixedTimeEquals`.
- **GroupBy aliased keys returned null** — alias resolution for `g.Key`, `g.Key.X`, `g.Key.X.Id` patterns.
- **ListItem `Contains` passed raw objects** — `IRedbListItem` → `.Id` conversion in `VisitEnumerableContains` / `VisitCollectionContains`.
- **MsSql ListItem operators** — `$in`, `$notIn`, `$arrayContains` now use `_listitem` column.
- **Postgres `$arrayContains` for ListItem arrays** — `_listitem` column with `bigint` cast instead of `_String`.
- **Pro OrderBy `ListItem.Value` / `ListItem.Alias`** — subquery JOINs `_list_items` for text sorting; CTE bypassed for ListItem fields.
- **GroupBy/Window base field filter crash** — `WhereRedb` on base fields (`ValueString`, `Name`, etc.) combined with `GroupBy`/`Window`/`GroupedWindow` produced `pvt._value_string does not exist`. Root cause: naive `.Replace("o.", "pvt.")` put base field filter outside PVT subquery where `o.` doesn't exist. Fix: base filters now injected inside the inner subquery WHERE clause (before aggregation), props filters remain on outer `pvt`. Affected 9 sites across Postgres.Pro and MSSql.Pro (Grouping, TreeGrouping, TreeWindow, TreeGroupedWindow, GroupedWindow). Also fixes Postgres TreeGroupedWindow where base filter was silently ignored.

### Added
- **`save_object_json`** SQL functions (Postgres + MsSql) — inverse of `get_object_json`, writes JSON back via DeleteInsert.
- **`DeadlockRetryHelper`** — automatic retry with exponential backoff for deadlock exceptions.
- **`BcryptPasswordHasher`** — bcrypt (work factor 12) with backward-compatible SHA256 verification and lazy rehash.

## [1.2.14] — 2026-02-16

### Fixed
- **Free projection double-load bug** — `LazyPropsLoader.LoadPropsForManyAsync` ignored `projectedStructureIds` and reloaded full objects via `get_object_json`, overwriting partial Props returned by SQL projection. Fix: `SkipPropsLoading = true` + `UseLazyLoading = false` in `ToListWithProjectionAsync` (`RedbQueryable`, `TreeQueryableBase`). `RedbProjectedQueryable` now always sets `skipProps = true`.

### Changed
- `QueryContext.SkipPropsLoading` now also controls lazy loader assignment — prevents both eager and lazy Props post-processing during projections.
- Multi-target NuGet packages: `net8.0`, `net9.0`, `net10.0`.
- CLI: trial limit 1,024 requests per app launch (resets on restart).

## [1.2.13] — 2026-01-20

### Fixed
- Pro `DeleteAsync` uses PVT builders instead of facet functions.
- ChangeTracking: handle existing nested `RedbObject` references correctly.
- Multiple bug fixes for open-source (FREE) version.

### Added
- Tree API for hierarchical queries.
- Window functions support in LINQ queries.
- Domain-isolated caches: `GlobalMetadataCache`, `GlobalListCache`, `GlobalPropsCache`.

### Changed
- Unified Pro feature exceptions.
- Query pipeline improvements.
