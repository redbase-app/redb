# redb.SQLite — roadmap

Order: **Pro-first** (pure C#, no native extension, WASM-ready), then Free (native C extension).
The skeleton for **both** projects is already copied from `redb.Postgres` / `redb.Postgres.Pro`
and added to `redb.sln`.

Legend: `[ ]` todo · `[~]` partial · `[x]` done.

---

## Phase 0 — Scaffolding (DONE)

- [x] Copy `redb.Postgres` → `redb.SQLite`, `redb.Postgres.Pro` → `redb.SQLite.Pro` (build artifacts stripped).
- [x] Rename `.csproj` files; add both to `redb.sln`.
- [x] Mass mechanical rename (identifiers → `Sqlite`, namespace → `redb.SQLite`). Ordered token replace
      over all `.cs`/`.csproj`/`.md` (excl. `doc/`): `using Npgsql;`→`using Microsoft.Data.Sqlite;`,
      `Include="Npgsql"`→`Include="Microsoft.Data.Sqlite"`, `redb.Postgres`→`redb.SQLite`,
      `PostgreSQL`→`SQLite`, `PostgreSql`→`Sqlite`, `Postgres`→`Sqlite`, `Npgsql`→`Sqlite` (+ lowercase).
      30 files renamed. 0 residual Postgres/Npgsql tokens in `.cs`/`.csproj`.
- [x] `redb.SQLite.Pro` ProjectReference auto-repointed to `redb.SQLite` (covered by `redb.Postgres`→`redb.SQLite`).

**Build state after rename (net8.0, Free):** 28 errors, ALL CS0246 (missing types), ALL confined to the
Data layer — exactly the REWRITE files. Providers/Query/Dialect compile clean (they go through
`ISqlDialect` strings, no Npgsql API). Error surface:
- `SqliteDataSource` ×12 (was `NpgsqlDataSource` — no SQLite equiv; use `SqliteConnection` directly)
- `SqliteBinaryImporter` ×8 (was `NpgsqlBinaryImporter`/COPY — → `SqliteBulkOperations` rewrite)
- `SqliteDbType` ×6 (was `NpgsqlDbType` — → `SqliteType`, different members)
- `SqliteTypes` ×2 (was `using NpgsqlTypes;` — remove)

> Open: keep the `ConcatenateSql` MSBuild target? (Free needs SQLite DDL init; Pro needs none.) The `.sql`
> sources are untouched PG plpgsql; the target still concatenates them into `redb_init.sql` (harmless for now).

## Phase 1 — Infrastructure (Data layer, shared by both tiers)

- [ ] `SqliteRedbConnection` — Microsoft.Data.Sqlite; **parameter mapping `$1,$2`→`@p1,@p2`** (central).
- [ ] `SqliteRedbContext` — facade; `PRAGMA foreign_keys=ON`, `journal_mode=WAL` on open.
- [ ] `SqliteRedbTransaction` — thin (SqliteTransaction).
- [ ] `SqliteKeyGenerator` — replace PG `nextval('global_identity')` with a **hi-lo allocator** over a
      counter table (no sequences in SQLite). See DIALECT_NOTES.
- [ ] `SqliteBulkOperations` — replace COPY/binary-import with **batched multi-row INSERT**, chunked to
      the SQLite parameter limit (≤999 or ≤32766). See DIALECT_NOTES.
- [ ] SQLite DDL init: port the schema (tables/indexes/FKs) part of `sql/redbPostgre.sql` to SQLite types.

## Phase 2 — Pro tier runtime (the MVP target)

- [ ] `SqliteDialect : ISqlDialect` — port `PostgreSqlDialect.cs` strings to SQLite; **return `null` from
      all `Query_BuildPvt*` methods** (no server-side PVT → forces the C# inline path, MSSql-phase-1 style).
- [ ] `ProSqliteDialect : ISqlDialectPro` — port `ProPostgreSqlDialect.cs`.
- [ ] `ProSqlBuilder` (Pro) — adapt SQL output: `= ANY(ARRAY[…])`→`IN(…)`, `DISTINCT ON`→`GROUP BY`/
      `ROW_NUMBER() OVER(…)=1`, drop `::casts`, json1 instead of jsonb. Core inline subquery pattern
      already portable.
- [ ] `ExpressionToSqlCompiler` (Pro) — map PG functions → SQLite (string/math/datetime).
- [ ] `PivotSqlGenerator` (Pro) — adapt to SQLite (`FILTER (WHERE)` available in 3.44+).
- [ ] Verify Pro materialization: `ProPropsMaterializer` lives in `redb.Core.Pro` (shared, DB-agnostic) →
      should work as-is; confirm it reads rows, not `get_object_json`.
- [ ] `ProRedbService` (Sqlite) factory + DI `AddRedbSqlitePro`.
- [ ] **Milestone: Pro provider runs everywhere incl. Blazor WASM, no native code.**

## Phase 3 — Free tier: native C extension

See [C_EXTENSION.md](C_EXTENSION.md). Heavy net-new C work.

- [ ] `get_object_json(object_id, max_depth)` in C — direct port of `redb_json_objects.sql` plpgsql:
      load `_values` for object once, recurse in C building JSON, `_RObject`→nested call (`max_depth-1`).
      Scheme-agnostic, reads `_scheme_metadata_cache` at runtime — **no SQL generation, no cache invalidation.**
- [ ] `search_objects_with_facets` + `pvt_build_*` in C — these ARE SQL-string generators; port the
      JSON-driven string building, emit SQLite-dialect SQL.
- [ ] Port supporting functions: `redb_metadata_cache` (`sync_metadata_cache_for_scheme`),
      `redb_soft_delete`, `redb_structure_tree`, `redb_migrations`, `migrate_structure_type`.
- [ ] Extension build/packaging per platform (`.so`/`.dll`/`.dylib`); load via Microsoft.Data.Sqlite
      `EnableExtensions`/`LoadExtension`.
- [ ] Free `SqliteDialect` methods point to the extension's functions (mirror PG Free).
- [ ] **Milestone: Free provider with in-DB functions on desktop/mobile/server.**

## Phase 4 — Validation & polish

- [ ] Run the existing `redb.Examples` / `redb.ConsoleTest` suites against SQLite (both tiers).
- [ ] Decimal-precision config option (`NUMERIC` → `TEXT` exact vs `REAL` fast).
- [ ] Concurrency notes (single-writer; WAL); document limits vs PG.
- [ ] NuGet packaging (`redb.SQLite`, `redb.SQLite.Pro`).

---

## Open questions to resolve as we go

1. Free `get_object_json` in C: faithful row-recursion (chosen) — confirm JSON output byte-shape matches
   what the C# serializer expects (compare against PG `get_object_json` output on the same data).
2. Does the Pro tree/PVT path in `redb.Postgres.Pro` ever call server-side `pvt_*`? Any such call must be
   replaced by pure C# for SQLite. Audit `ProTreeQueryProvider.*` + `ProPostgresObjectStorageProvider`.
3. Keep `ConcatenateSql` MSBuild target for Free DDL, or hand-write a single `sqlite_init.sql`?
