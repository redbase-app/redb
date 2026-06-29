# redb.SQLite (Free) — native C extension

The Free tier is the analog of PostgreSQL Free: the heavy logic lives **inside the database** as
functions, except SQLite hosts them through a **loadable extension** (`.so`/`.dll`/`.dylib`) instead of
plpgsql. This is the only net-new piece (no equivalent exists in `redb.Postgres` — there it is plpgsql).

Loaded from C# via Microsoft.Data.Sqlite: `connection.EnableExtensions(true)` then
`SELECT load_extension(@path)` (or `sqlite3_load_extension`). WASM cannot do this → WASM uses Pro.

## Two kinds of functions to port

The PG `sql/` corpus splits into two fundamentally different kinds — they map to different C styles.

### 1. SQL-string generators → straightforward C (string building)

`pvt_build_query_sql` and the whole `v2-pvt/*` module, plus `search_objects_with_facets`, take a
`scheme_id` + JSON (filter/order/paging/mode) and **return SQL text**. In C: parse the JSON (SQLite's
own json1 or a small JSON lib), reproduce the shape logic, emit SQLite-dialect SQL (apply the swaps in
[DIALECT_NOTES.md](DIALECT_NOTES.md): `ANY`→`IN`, `DISTINCT ON`→`GROUP BY`/window, drop casts, `WITH
RECURSIVE` as-is). Deterministic string manipulation — the bulk of the corpus, and the "not that hard"
part. (These same generators already exist in C# as `ProSqlBuilder` for the Pro tier.)

### 2. `get_object_json` — data-driven materializer → direct C recursion

`redb_json_objects.sql` (`get_object_json` + `build_hierarchical_properties_optimized`) does NOT generate
SQL — it reads rows and assembles nested JSON. Port it **directly** (1:1 with the plpgsql), registered via
`sqlite3_create_function`:

```
get_object_json(object_id INTEGER, max_depth INTEGER) -> TEXT (json)
```

Implementation:
1. From the function context get the db handle (`sqlite3_context_db_handle`).
2. `SELECT * FROM _values WHERE _id_object = ?` — load **all** rows for the object once into a C array
   (mirrors PG loading into `_values[]`).
3. Read field metadata from `_scheme_metadata_cache` for the object's scheme.
4. Recurse in C over fields, building JSON (sqlite3_str API or a buffer):
   - scalar → typed column value;
   - Class field → recurse (max_depth NOT decreased);
   - array → json array; Dictionary → json object keyed by `_array_index`;
   - `_RObject` reference → nested `get_object_json(ref, max_depth-1)`;
   - ListItem → linked-object base fields.
5. Return the JSON text.

**Why direct C, not generate-and-cache SQL:** it is scheme-agnostic (driven by `_scheme_metadata_cache`
at runtime, exactly like the plpgsql original), so there is **no per-scheme SQL to cache and no cache to
invalidate when schemes change dynamically**. Simpler and faithful to PG. A bulk variant
`get_objects_json(ids, max_depth)` mirrors the PG batch loader used by `LazyPropsLoader`.

**Validation gate:** the JSON byte-shape must match what the C# serializer expects. Diff the C
extension's output against PG `get_object_json` on identical data before wiring it in.

## Supporting functions to port

- `redb_metadata_cache` → `_scheme_metadata_cache` table + `sync_metadata_cache_for_scheme` (shared
  dependency of both the materializer and the query generators).
- `redb_soft_delete`, `redb_structure_tree`, `redb_migrations`, `migrate_structure_type`,
  `migration_drop_deleted_objects`, permissions (`get_user_permissions_for_object`).

## Build & packaging (later)

- Single C source compiled per platform → `.so` (Linux), `.dll` (Windows), `.dylib` (macOS), plus mobile.
- Ship binaries in the `redb.SQLite` NuGet (runtimes/) and load at startup.
- The Free `SqliteDialect` methods then return calls to these extension functions (mirroring PG Free,
  where dialect methods return `get_object_json(...)` / `search_objects_with_facets(...)`).
