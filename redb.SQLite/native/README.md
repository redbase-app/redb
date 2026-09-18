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

One file, base name `redbsqlite`, per platform/arch:

| Platform | File |
|----------|------|
| Windows x64 | `redbsqlite.dll` |
| Linux x64 / arm64 (glibc) | `redbsqlite.so` |
| macOS x64 / arm64 | `redbsqlite.dylib` |

> The base name is **`redbsqlite`** (specific), NOT the generic `redb` — the latter
> collides with the managed `redb.*` assemblies and the host's `redb.*` prune globs.
> The C init symbol stays **`sqlite3_redb_init`** (SQLite would otherwise derive
> `sqlite3_redbsqlite_init` from the file name), so hosts must load with an **explicit
> entry point**: `sqlite3_redb_init`.

## Toolchain

- **CMake ≥ 3.16** and a **C99 compiler**.
  - Windows: Visual Studio Build Tools (Desktop C++ workload) **or** MSYS2 /
    MinGW-w64. Plus CMake.
  - Linux: `gcc` or `clang` + `cmake` (e.g. `apt install build-essential cmake`). From a Windows host
    nothing is needed beyond Docker: the toolchain ships in the image built from [Dockerfile](Dockerfile).
  - macOS: Xcode Command Line Tools + `cmake` (`brew install cmake`).
- Internet access on the **first** configure (CMake fetches the pinned SQLite
  amalgamation for `sqlite3.h` / `sqlite3ext.h`). Override the source with
  `-DSQLITE_AMALGAMATION_URL=...` to use a local mirror.

## Build

One `redb_pvt.c` (and siblings) → one loadable module per platform/arch. There is **no CI for the native
extension** — it is rebuilt by hand whenever the C sources change, and the artifacts are git-ignored
(packed at publish time from the `build*/` directories below). The recipes below are the verified ones.

### Windows x64 → `build/redbsqlite.dll`

Uses the compiler + CMake + Ninja bundled with Visual Studio (no separate CMake install needed). Run
from **PowerShell** — piping a `vcvarsXX.bat` through Git Bash mangles the quoting.

```powershell
$vs     = "C:\Program Files\Microsoft Visual Studio\18\Insiders"   # adjust edition/year
$vcvars = "$vs\VC\Auxiliary\Build\vcvars64.bat"
$cmake  = "$vs\Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin\cmake.exe"
$nat    = "C:\Work\redb_code\csharp\redb\redb.SQLite\native"
cmd /c "`"$vcvars`" && `"$cmake`" -S `"$nat`" -B `"$nat\build`" -G Ninja -DCMAKE_BUILD_TYPE=Release"
cmd /c "`"$vcvars`" && `"$cmake`" --build `"$nat\build`" --clean-first"
```

Artifact: `build/redbsqlite.dll`.

### Linux x64 + arm64 → `build-linux-x64/` and `build-linux-arm64/` (Docker, from a Windows/any host)

x64 builds natively; arm64 cross-compiles. The toolchain lives in an image built from
[Dockerfile](Dockerfile) — build it once:

```sh
docker build -t redb-sqlite-native native/
```

Then every rebuild is seconds and needs no network:

```sh
docker run --rm -v "/abs/path/to/redb.SQLite/native":/src redb-sqlite-native bash -c '
  set -e
  cmake -S /src -B /src/build-linux-x64   -DCMAKE_BUILD_TYPE=Release && cmake --build /src/build-linux-x64
  cmake -S /src -B /src/build-linux-arm64 -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=aarch64-linux-gnu-gcc
  cmake --build /src/build-linux-arm64'
```

> On Windows Git Bash, prefix with `MSYS_NO_PATHCONV=1` and use a forward-slash absolute path for `-v`
> (`"C:/Work/.../redb.SQLite/native":/src`) so the mount path is not rewritten.

The recipe this replaced ran `debian:12` and `apt-get install` inside every build, then threw the
container away: ~190 MB of toolchain re-downloaded per rebuild, and a release that could not be built
while a Debian mirror was unreachable. It also configured into `/tmp` inside the container, so the
fetched SQLite amalgamation went with the container too.

**Mount at `/src` and keep the build dirs.** CMake records in `CMakeCache.txt` the path the cache was
created under; mounting the same folder elsewhere makes it refuse the cache outright (*"The current
CMakeCache.txt directory ... is different"*), and stale stamps under `_deps/` then fail the
amalgamation fetch. Caches made before 2026-09-17 came from a `/work/...` mount and had to be deleted
once — after that the dirs are reusable and hold the amalgamation, so nothing is downloaded again.
When a cache does have to be reset: `rm -rf build-linux-*/CMakeCache.txt build-linux-*/CMakeFiles build-linux-*/_deps`.

### macOS x64/arm64 → `.dylib`

Xcode Command Line Tools + `cmake` (`brew install cmake`), same `cmake -S . -B build && cmake --build build`
on a macOS host (no cross-build from Windows/Linux).

## Verify a rebuilt artifact

```sh
# 1. Right arch:
file build-linux-arm64/redbsqlite.so         # → ELF ... ARM aarch64

# 2. Carries the current SQL (spot-check a literal that only the new code emits):
grep -a "depth > 0) AND NOT EXISTS" build-linux-x64/redbsqlite.so

# 3. Loads + reports version (native-arch host only):
sqlite3 ":memory:" ".load ./build-linux-x64/redbsqlite.so sqlite3_redb_init" "SELECT redb_version();"
```

Run step 3 inside the build image — its `sqlite3` is compiled from the same pinned amalgamation as the
extension. The Debian package is 3.40.1 and the extension refuses to load below 3.44 (it needs
`sqlite3_get_clientdata`), so the distro CLI answers with the version guard, not with a verdict on the
artifact:

```sh
docker run --rm -v "/abs/path/to/redb.SQLite/native":/src redb-sqlite-native bash -c '
  file -b /src/build-linux-arm64/redbsqlite.so
  sqlite3 ":memory:" ".load /src/build-linux-x64/redbsqlite.so sqlite3_redb_init" "SELECT redb_version();"'
```

Also worth a look after a rebuild: `PVT_MODULE_VERSION` in `src/redb_pvt.c` must equal
`SqliteDialect.Query_PvtRequiredVersion()` — `RedbService` compares them at initialization, so a stale
artifact fails the Free path outright rather than misbehaving quietly.

The full functional check is the .NET Free-path integration suite
(`redb.Tests.Integration`, the `Sqlite*` fixtures), which loads the platform artifact.

## Quick check

```sh
sqlite3
sqlite> .load ./build/redbsqlite sqlite3_redb_init
sqlite> SELECT redb_version();
0.1.0-m1
```

(`.load` requires a `sqlite3` CLI built with extension loading enabled, which is
the default.)

## Loading from .NET (Microsoft.Data.Sqlite)

```csharp
connection.EnableExtensions(true);
connection.LoadExtension("/path/to/redbsqlite", "sqlite3_redb_init");
```

Wiring this into `SqliteRedbConnection` (Free path only, behind config) is done
once the extension exposes real functions (M2+).
