# redb.SQLite — provider plan

SQLite backend for REDB. Greenfield, built by **porting the PostgreSQL provider file-by-file**
(`redb.Postgres` → `redb.SQLite`, `redb.Postgres.Pro` → `redb.SQLite.Pro`). The full PG tree was
copied verbatim via terminal so the structure is 1:1 and nothing is missing — porting is then a
mechanical, low-error adaptation rather than generation from scratch.

## Why two tiers

| Tier | Engine | Runs on | Native code |
|------|--------|---------|-------------|
| **redb.SQLite** (Free) | SQL functions inside the DB via a **native loadable extension** (`.so`/`.dll`/`.dylib`) — the direct analog of PG Free (in-DB plpgsql). | desktop, mobile, server | yes (C extension) |
| **redb.SQLite.Pro** | **C# query builder** (`ProSqlBuilder`) emitting inline SQL + **C# materializer** (`ProPropsMaterializer`, shared from `redb.Core.Pro`). | everywhere incl. **Blazor WASM** | none |

WASM cannot load native extensions → WASM users take Pro.

## Hard constraints

- **Minimum SQLite version: 3.44.0 (Nov 2023).** Decided. Gives `FILTER (WHERE …)`, window functions,
  `RETURNING`, json1 (`json_object`/`json_group_array`/`json_group_object`), `WITH RECURSIVE` — so the
  dialect stays maximally close to PG. Older versions are explicitly unsupported.
- Driver: **Microsoft.Data.Sqlite** (bundles a current SQLite).
- `PRAGMA foreign_keys = ON` required for `ON DELETE CASCADE`.

## What copies cleanly vs what is net-new

- **C# layer** (context, providers, query builders, dialect, DI): copies + mechanical adaptation
  (`Npgsql*`→`Sqlite*`, dialect strings, type mapping, `$1`→`@p1`). Low risk. See [PORTING_MAP.md](PORTING_MAP.md).
- **`sql/*.sql`** (plpgsql functions): copied as **reference/spec only** — they do not run in SQLite.
  Their logic is reimplemented in the **native C extension** (Free) or in C# (Pro). The schema DDL part
  of `redbPostgre.sql` is ported to SQLite DDL.
- **Native C extension** (Free): the ONE genuinely net-new piece — no equivalent exists in PG (there it
  is plpgsql in the DB). See [C_EXTENSION.md](C_EXTENSION.md).

## Doc index

- [ROADMAP.md](ROADMAP.md) — phased plan (Pro-first), with checklists.
- [PORTING_MAP.md](PORTING_MAP.md) — file-by-file: PG source → SQLite target → change type.
- [DIALECT_NOTES.md](DIALECT_NOTES.md) — SQL/type translation rules (params, types, `ANY`→`IN`,
  `DISTINCT ON`, json1, key generation, bulk insert, `get_object_json`).
- [C_EXTENSION.md](C_EXTENSION.md) — Free-tier native extension design (Phase 2).

## Status

Planning. Skeleton copied and added to `redb.sln`. The two new projects are **clones of the PG
provider** (still `namespace redb.Postgres`, still referencing Npgsql) until Phase 1 adaptation —
they compile as PG clones but are not yet SQLite. Do not expect SQLite behavior until Phase 1 lands.
