cmake_minimum_required(VERSION 3.16)
project(redb_sqlite_ext C)

# REDB SQLite Free loadable extension.
# Hosts the server-side SQL functions (get_object_json, pvt_*, permissions,
# soft-delete) inside the SQLite engine — the SQLite analog of PostgreSQL Free.
# Works in ANY SQLite host (Microsoft.Data.Sqlite, Python sqlite3, sqlite3 CLI),
# because the extension is loaded by the host process and resolves the sqlite3
# API symbols from it at load time — we do NOT link against libsqlite3.

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)

# ----------------------------------------------------------------------------
# SQLite amalgamation headers (sqlite3.h + sqlite3ext.h).
# A loadable extension only needs the HEADERS to compile against the public
# API + the SQLITE_EXTENSION_INIT machinery. We fetch a pinned amalgamation
# zip at configure time so the repo stays clean (no vendored 13k-line header).
# Override SQLITE_AMALGAMATION_URL to bump or to point at a local mirror.
# ----------------------------------------------------------------------------
include(FetchContent)
set(SQLITE_AMALGAMATION_URL
    "https://www.sqlite.org/2024/sqlite-amalgamation-3460100.zip"
    CACHE STRING "URL of the SQLite amalgamation zip (provides sqlite3.h + sqlite3ext.h)")
FetchContent_Declare(sqlite_amalgamation
    URL "${SQLITE_AMALGAMATION_URL}"
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE)
FetchContent_MakeAvailable(sqlite_amalgamation)

# ----------------------------------------------------------------------------
# The extension itself — built as a shared MODULE (dlopen/LoadLibrary target).
# Output base name is "redb" on every platform so the default entry-point
# derivation in sqlite3_load_extension() resolves to sqlite3_redb_init:
#   Windows : redb.dll
#   Linux   : redb.so      (PREFIX "" → not libredb.so)
#   macOS   : redb.dylib
# ----------------------------------------------------------------------------
add_library(redb MODULE
    src/redb_extension.c
    src/redb_pvt.c
    src/redb_pvt_analytics.c
)
target_include_directories(redb PRIVATE "${sqlite_amalgamation_SOURCE_DIR}")
set_target_properties(redb PROPERTIES
    PREFIX ""
    OUTPUT_NAME "redb")
if(APPLE)
    set_target_properties(redb PROPERTIES SUFFIX ".dylib")
endif()

# Warnings as a quality gate (extension code must be clean).
if(MSVC)
    target_compile_options(redb PRIVATE /W3)
else()
    target_compile_options(redb PRIVATE -Wall -Wextra)
endif()
