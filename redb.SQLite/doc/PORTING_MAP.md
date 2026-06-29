# redb.SQLite — file-by-file porting map

Every file below was copied verbatim from the PG provider. Change types:

- **COPY** — DB-agnostic, keep as-is (only namespace rename).
- **RENAME** — thin shell / inherits Core base; rename class + types, body barely changes.
- **ADAPT** — real SQLite-specific edits (SQL strings, types, params).
- **REWRITE** — replaces a PG-only mechanism (sequences, COPY) wholesale.
- **REF** — `.sql` plpgsql kept as reference/spec; reimplemented in C (Free) or C# (Pro), not run as-is.

> Global to all `.cs`: `namespace redb.Postgres[.Pro]` → `redb.SQLite[.Pro]`; `Npgsql*`→`Sqlite*`;
> `Postgres`→`Sqlite` in type names.

## redb.SQLite (Free) — C#

| Current file | Target | Change | Notes |
|---|---|---|---|
| `Data/NpgsqlRedbConnection.cs` | `SqliteRedbConnection.cs` | ADAPT | Microsoft.Data.Sqlite; **`$1`→`@p1` param mapping** lives here. |
| `Data/NpgsqlRedbContext.cs` | `SqliteRedbContext.cs` | ADAPT | `PRAGMA foreign_keys=ON`, WAL on open. |
| `Data/NpgsqlRedbTransaction.cs` | `SqliteRedbTransaction.cs` | RENAME | SqliteTransaction; logic same. |
| `Data/NpgsqlKeyGenerator.cs` | `SqliteKeyGenerator.cs` | REWRITE | sequence → hi-lo counter table. |
| `Data/NpgsqlBulkOperations.cs` | `SqliteBulkOperations.cs` | REWRITE | COPY → batched INSERT + param chunking. |
| `sql/PostgreSqlDialect.cs` | `SqliteDialect.cs` | ADAPT | The big one. Return `null` from `Query_BuildPvt*`. |
| `RedbService.cs` | `RedbService.cs` | ADAPT | `DatabaseTypeName`, dialect ref, `GetObjectJsonSql`, embedded-resource names, `TableExistsAsync` SQL. |
| `Providers/PostgresObjectStorageProvider.cs` | `SqliteObjectStorageProvider.cs` | RENAME | thin shell over `ObjectStorageProviderBase`. |
| `Providers/LazyPropsLoader.cs` | `LazyPropsLoader.cs` | ADAPT | Free uses `get_object_json`/batch (ext fn); SQL strings via dialect. |
| `Providers/PostgresSchemeSyncProvider.cs` | `SqliteSchemeSyncProvider.cs` | RENAME/ADAPT | DDL for new structures may need SQLite type map. |
| `Providers/PostgresUserProvider.cs` | `SqliteUserProvider.cs` | RENAME | inherits Core base. |
| `Providers/PostgresRoleProvider.cs` | `SqliteRoleProvider.cs` | RENAME | |
| `Providers/PostgresPermissionProvider.cs` | `SqlitePermissionProvider.cs` | ADAPT | uses `get_user_permissions_for_object` (ext fn for Free). |
| `Providers/PostgresListProvider.cs` | `SqliteListProvider.cs` | RENAME | |
| `Providers/PostgresValidationProvider.cs` | `SqliteValidationProvider.cs` | RENAME | |
| `Providers/PostgresTreeProvider.cs` | `SqliteTreeProvider.cs` | RENAME/ADAPT | tree walks → `WITH RECURSIVE` (portable). |
| `Providers/PostgresQueryableProvider.cs` | `SqliteQueryableProvider.cs` | RENAME | factory shell. |
| `Query/PostgresQueryProvider*.cs` (5) | `SqliteQueryProvider*.cs` | ADAPT | Free query path; Pro-only partials throw; facet→`search_objects_with_facets` (ext fn). |
| `Query/PostgresTreeQueryProvider*.cs` (3) | `SqliteTreeQueryProvider*.cs` | ADAPT | `.Pvt`/`.PvtHasFilter` call server PVT — Free routes to ext fn. |
| `Query/PostgresTreeQueryable.cs` | `SqliteTreeQueryable.cs` | RENAME | |
| `Extensions/PostgresOptionsExtensions.cs` | `SqliteOptionsExtensions.cs` | ADAPT | connection-string options. |
| `Extensions/ServiceCollectionExtensions.cs` | same | ADAPT | `AddRedbSqlite`. |
| `Extensions/RedbServiceExtensions.cs` | same | ADAPT | |
| `Extensions/PropertyInfoExtensions.cs` | same | COPY | reflection helper. |
| `Configuration/UserConfigurationService.cs` | same | COPY | DB-agnostic. |
| `Utils/ValuesTopologicalSort.cs` | same | COPY | pure algorithm. |

## redb.SQLite (Free) — sql/ (all REF; Free → C extension, schema → SQLite DDL)

| File | Becomes | Notes |
|---|---|---|
| `sql/redbPostgre.sql` | SQLite DDL init | port tables/indexes/FK/types; drop sequences/views/functions. |
| `sql/redb_json_objects.sql` | C: `get_object_json` | data-driven materializer (not a generator). See C_EXTENSION. |
| `sql/v2-pvt/*` | C: `pvt_build_*` | SQL-string generators → C string building. |
| `sql/redb_metadata_cache.sql` | C: `sync_metadata_cache_for_scheme` + `_scheme_metadata_cache` table | shared dependency of both materializer & query gen. |
| `sql/redb_soft_delete.sql`, `redb_structure_tree.sql`, `redb_migrations.sql`, `migrate_structure_type.sql`, `migration_drop_deleted_objects.sql` | C / SQLite SQL | port per function. |
| `sql/redb_save_json_objects.sql` | (likely unused) | save path is already pure C# in Core; confirm no caller. |
| `sql/redb_init.sql`, `sql/v2-pvt/pvt_bundle.sql` | **generated** | build artifacts — never hand-edit (see project memory). |

## redb.SQLite.Pro — C# (Phase 2 priority; no native code)

| Current file | Target | Change | Notes |
|---|---|---|---|
| `Sql/ProPostgreSqlDialect.cs` | `ProSqliteDialect.cs` | ADAPT | `ISqlDialectPro` strings. |
| `Query/ProSqlBuilder.cs` | `ProSqlBuilder.cs` | ADAPT | **key file**: `ANY`→`IN`, `DISTINCT ON`→`GROUP BY`/window, drop casts, json1. |
| `Query/ExpressionToSqlCompiler.cs` | same | ADAPT | PG funcs → SQLite (LOWER/UPPER/TRIM/ABS/ROUND/strftime…). |
| `Query/PivotSqlGenerator.cs` | same | ADAPT | `FILTER (WHERE)` ok in 3.44+. |
| `Query/PostgresHavingResolver.cs` | `SqliteHavingResolver.cs` | ADAPT | |
| `Query/ProQueryProvider*.cs` (6) | `Sqlite…` | ADAPT | ensure pure inline SQL, no server PVT. |
| `Query/ProTreeQueryProvider*.cs` (7) | `Sqlite…` | ADAPT | recursive CTE; audit for server-PVT calls. |
| `Providers/ProPostgresObjectStorageProvider.cs` | `ProSqliteObjectStorageProvider.cs` | ADAPT | references `get_object_json`/save — Pro must use C# materializer. |
| `Providers/ProPostgresTreeProvider.cs` | `ProSqliteTreeProvider.cs` | RENAME/ADAPT | |
| `Providers/ProQueryableProvider.cs` | same | RENAME | factory. |
| `Services/ProRedbService.cs` | same | ADAPT | factory + dialect. |
| `Extensions/ProServiceCollectionExtensions.cs` | same | ADAPT | `AddRedbSqlitePro`. |
| `Extensions/PostgresProOptionsExtensions.cs` | `SqliteProOptionsExtensions.cs` | ADAPT | |

> `ProPropsMaterializer` is NOT here — it lives in `redb.Core.Pro` (shared, DB-agnostic). Pro reads rows
> and materializes in C#, so it should work for SQLite unchanged. Confirm in Phase 2.
