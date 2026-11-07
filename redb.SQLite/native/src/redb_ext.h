/*
** Shared internals for the REDB SQLite extension, split across translation
** units. redb_extension.c owns SQLITE_EXTENSION_INIT1 (defines sqlite3_api);
** every other TU uses SQLITE_EXTENSION_INIT3 (extern) and includes this header.
*/
#ifndef REDB_EXT_H
#define REDB_EXT_H

#include "sqlite3ext.h"

/* ---- Shared JSON1-backed helpers (defined in redb_extension.c) ---------- */

/* json_extract(json, path) as a sqlite3_malloc'd text (sqlite3_free), or NULL
** when the path is absent / SQL-null. Objects/arrays come back as JSON text. */
char *jsonGetText(sqlite3 *db, const char *json, const char *path);

/* json_extract(json, path) as int64; sets *found to 0 when absent/null. */
sqlite3_int64 jsonGetInt(sqlite3 *db, const char *json, const char *path, int *found);

/* json_type(json, path) as sqlite3_malloc'd text, or NULL when absent.
** One of: object array text integer real true false null. */
char *jsonTypeAt(sqlite3 *db, const char *json, const char *path);

/* Allocate the next id from the _global_identity counter. */
sqlite3_int64 nextId(sqlite3 *db);

/* ---- pvt engine (defined in redb_pvt.c) --------------------------------- */

/* Registers the v2-pvt SQL-generation functions. Returns an SQLite rc. */
int redbRegisterPvt(sqlite3 *db);

#endif /* REDB_EXT_H */
