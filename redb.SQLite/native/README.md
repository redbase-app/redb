# REDB SQLite Free — native extension

A SQLite **loadable extension** that hosts the REDB server-side SQL functions
(`get_object_json`, `pvt_*`, permissions, soft-delete) inside the SQLite engine.
This is the SQLite analog of the PostgreSQL **Free** edition (where those live as
PL/pgSQL functions). Because it is loaded by the host process and resolves the
`sqlite3` API at load time, **the same binary works in any SQLite host**:
Microsoft.Data.Sqlite (.NET), Python's `sqlite3`, the `sqlite3` CLI, etc.

> Pure-C# **Pro** (`redb.SQLite.Pro`) needs none of this — it materializes in
> managed code and runs in Blazor WASM. This extension is the **Free** path and
> the foundation for non-.NET bindings (e.g. Python).

## Status

| Milestone | Scope | State |
|-----------|-------|-------|
| M1 | Build/load pipeline + `redb_version()` canary | **current** |
| M2 | `get_object_json` recursive materializer | pending |
| M3 | `pvt_*` SQL generators + `search_objects_with_facets` | pending |
| M4 | permissions (`get_user_permissions_for_object`, `v_user_permissions`) | pending |
| M5 | soft-delete (`mark_for_deletion`, `purge_trash`) | pending |

## Output

One file, base name `redb`, per platform/arch:

| Platform | File |
|----------|------|
| Windows x64 | `redb.dll` |
| Linux x64 / arm64 (glibc) | `redb.so` |
| macOS x64 / arm64 | `redb.dylib` |

The base name `redb` makes the default entry point `sqlite3_redb_init`, so hosts
load it with no explicit entry-point argument.

## Toolchain

- **CMake ≥ 3.16** and a **C99 compiler**.
  - Windows: Visual Studio Build Tools (Desktop C++ workload) **or** MSYS2 /
    MinGW-w64. Plus CMake.
  - Linux: `gcc` or `clang` + `cmake` (e.g. `apt install build-essential cmake`).
  - macOS: Xcode Command Line Tools + `cmake` (`brew install cmake`).
- Internet access on the **first** configure (CMake fetches the pinned SQLite
  amalgamation for `sqlite3.h` / `sqlite3ext.h`). Override the source with
  `-DSQLITE_AMALGAMATION_URL=...` to use a local mirror.

## Build

```sh
cd redb.SQLite/native
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --config Release
```

The artifact lands in `build/` (or `build/Release/` with MSVC multi-config).

### Cross-compiling Linux arm64 (example)

```sh
cmake -S . -B build-arm64 -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_C_COMPILER=aarch64-linux-gnu-gcc
cmake --build build-arm64
```

## Quick check

```sh
sqlite3
sqlite> .load ./build/redb
sqlite> SELECT redb_version();
0.1.0-m1
```

(`.load` requires a `sqlite3` CLI built with extension loading enabled, which is
the default.)

## Loading from .NET (Microsoft.Data.Sqlite)

```csharp
connection.EnableExtensions(true);
connection.LoadExtension("/path/to/redb");   // no extension/entry-point needed
```

Wiring this into `SqliteRedbConnection` (Free path only, behind config) is done
once the extension exposes real functions (M2+).
