# redb.SQLite — dialect & translation rules

Target: **SQLite 3.44.0+** via Microsoft.Data.Sqlite. Rules below apply when adapting
`PostgreSqlDialect.cs` → `SqliteDialect.cs`, `ProPostgreSqlDialect.cs` → `ProSqliteDialect.cs`,
and the Pro `ProSqlBuilder`/`ExpressionToSqlCompiler` output.

## Parameters

- PG dialect strings use `$1, $2, …`. Microsoft.Data.Sqlite uses named params.
- Convert `$N` → `@pN` centrally in `SqliteRedbConnection` (the `IRedbContext` impl maps positional
  `params object[]` to `@p0, @p1, …`). Keep the dialect strings in `$N` form OR rewrite to `@pN` — pick
  one and apply consistently. Recommended: translate at the connection boundary so dialect text changes stay minimal.

## Types (column affinity)

| PG | SQLite | Note |
|---|---|---|
| `bigint` / `BIGSERIAL` | `INTEGER` (`INTEGER PRIMARY KEY` for rowid PK) | no sequences; see key generation. |
| `boolean` | `INTEGER` (0/1) | emit JSON booleans via `json('true'/'false')` or `CASE` in materializer. |
| `timestamptz` | `TEXT` (ISO-8601) — sorts lexicographically — or `REAL` (unix epoch) | choose TEXT for readability/ordering. |
| `numeric(38,18)` | `TEXT` (exact) **or** `REAL` (lossy) | **config option**; SQLite has no exact decimal. Main precision gap. |
| `double precision` | `REAL` | |
| `uuid` | `TEXT` | store canonical string. |
| `bytea` | `BLOB` | |
| `text`/`varchar` | `TEXT` | |
| `jsonb` | `TEXT` + json1 functions | no native jsonb type; use `json_object`/`json_group_array`. |

`PRAGMA foreign_keys = ON` is required for `ON DELETE CASCADE` to fire (off by default per-connection).

## SQL construct swaps (in generated/dialect SQL)

| PG | SQLite | Where it shows up |
|---|---|---|
| `x = ANY(ARRAY[…]::bigint[])` / `= ANY($1)` | `x IN (…)` or `x IN (SELECT value FROM json_each(@ids))` | batch loads, bulk selects, pvt CTE. |
| `unnest($1::bigint[])` | `json_each(@arr)` | batch `get_object_json`, id lists. |
| `SELECT DISTINCT ON (col) …` | `GROUP BY col` or `ROW_NUMBER() OVER(PARTITION BY col ORDER BY …)=1` subquery | `ProSqlBuilder.BuildSelectClause`. |
| `expr::type` casts | drop or `CAST(expr AS …)` | everywhere. |
| `jsonb_build_object` / `jsonb_agg` / `jsonb_object_agg` | `json_object` / `json_group_array` / `json_group_object` | materializer. |
| `ILIKE` | `LIKE` (ASCII case-insensitive by default) | string filters; escape `%`/`_`. |
| `~` regex | `LIKE`/`GLOB` (no PCRE) or registered C function | rare. |
| `WITH RECURSIVE` | identical | tree modes — works as-is. |
| window funcs, `FILTER (WHERE)`, `GROUP BY`/`HAVING`, `LIMIT/OFFSET` | identical | available in 3.44+. |
| string/math/date funcs (`lower`,`upper`,`trim`,`abs`,`round`; date parts) | SQLite equivs (`lower`,`upper`,`trim`,`abs`,`round`,`strftime`) | `ExpressionToSqlCompiler`. |

## Key generation (`SqliteKeyGenerator`)

PG: `nextval('global_identity')` (a sequence). SQLite has no sequences.
Approach: a single-row counter table allocated in a **hi-lo** fashion:

```
CREATE TABLE _identity (name TEXT PRIMARY KEY, value INTEGER NOT NULL);
-- batch allocate N ids atomically (3.35+ RETURNING):
UPDATE _identity SET value = value + @n WHERE name = 'global'
RETURNING value;   -- ids = [old+1 .. old+n]
```

Wrap in the connection's transaction; reuse the existing `RedbKeyGeneratorBase` caching (static, same as PG).

## Bulk operations (`SqliteBulkOperations`)

PG: COPY binary protocol. SQLite: **batched multi-row INSERT** inside one transaction.

- Build `INSERT INTO _values (…) VALUES (…),(…),…` with N rows per statement.
- **Chunk** so `rows * columns ≤ SQLITE_MAX_VARIABLE_NUMBER` (999 on older builds, 32766 on 3.32+).
  Compute chunk size from column count; default conservative (e.g. floor(900 / cols)).
- One transaction around the whole bulk; `PRAGMA synchronous`/WAL for throughput.

## `get_object_json`

NOT a SQL generator — a data-driven recursive materializer. Two homes:
- **Free**: ported to **C** in the extension (faithful row-recursion). See [C_EXTENSION.md](C_EXTENSION.md).
- **Pro**: not used — `ProPropsMaterializer` (C#, in `redb.Core.Pro`) reads rows and builds the graph.

A single static SQLite SELECT cannot cover all schemes (shape is per-scheme) nor arbitrary nesting
(aggregates are forbidden in a recursive CTE), so neither "one embedded SELECT" nor "generate+cache SQL"
is the path — direct code (C for Free, C# for Pro) is.
