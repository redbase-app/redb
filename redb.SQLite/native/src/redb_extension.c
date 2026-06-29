/*
** REDB SQLite Free — loadable extension.
**
** Hosts the REDB server-side SQL functions inside the SQLite engine, mirroring
** the PostgreSQL Free edition's PL/pgSQL functions. Loaded by the host process
** (Microsoft.Data.Sqlite, Python's sqlite3, the sqlite3 CLI, ...); the sqlite3
** API symbols are resolved from that host at load time, so this library is NOT
** linked against libsqlite3.
**
** Functions:
**   redb_version()                         -> TEXT   (build canary)
**   get_object_json(id [, max_depth=10])   -> TEXT   (JSON materializer)
**
** get_object_json is a C port of redb.SQLite/sql/redb_json_objects.sql
** (get_object_json + build_hierarchical_properties_optimized + build_listitem).
** Default entry point: file basename "redb" -> sqlite3_redb_init.
*/

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1

#include "redb_ext.h"
#include <string.h>

#ifndef REDB_EXT_VERSION
#define REDB_EXT_VERSION "1.0.0-m3-pvt-complete"
#endif

/* Collection-type marker IDs (mirror the PG function constants). */
#define REDB_COLL_ARRAY      (-9223372036854775668LL)
#define REDB_COLL_DICTIONARY (-9223372036854775667LL)

/* _values column projection used everywhere; indices below must match order. */
/* _DateTimeOffset is stored as REAL Julian day (UTC); emit it as ISO-8601 text
   (strftime consumes the Julian REAL directly) so the materializer deserializes a
   datetime, not a number. Column order/index (VC_DATETIME) is unchanged.
   NOTE: the '%%' are escaped — VCOLS is only ever spliced into sqlite3_mprintf()
   FORMAT strings, where mprintf collapses %%->% before SQLite parses. Do NOT use
   VCOLS in a raw sqlite3_prepare_v2() without un-escaping. */
#define VCOLS " _id,_String,_Long,_Guid,_Double,strftime('%%Y-%%m-%%dT%%H:%%M:%%fZ',_DateTimeOffset),_Boolean,_ByteArray,_Numeric,_ListItem,_Object,_array_parent_id,_array_index "
enum {
  VC_ID = 0, VC_STRING, VC_LONG, VC_GUID, VC_DOUBLE, VC_DATETIME, VC_BOOL,
  VC_BYTES, VC_NUMERIC, VC_LISTITEM, VC_OBJECT, VC_APARENT, VC_AINDEX
};

/* ------------------------------------------------------------------------- */
/* JSON helpers                                                              */
/* ------------------------------------------------------------------------- */

static const char B64[] =
  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

static void appendBase64(sqlite3_str *o, const unsigned char *d, int n){
  int i;
  for(i = 0; i + 2 < n; i += 3){
    sqlite3_str_appendchar(o, 1, B64[d[i] >> 2]);
    sqlite3_str_appendchar(o, 1, B64[((d[i] & 3) << 4) | (d[i+1] >> 4)]);
    sqlite3_str_appendchar(o, 1, B64[((d[i+1] & 15) << 2) | (d[i+2] >> 6)]);
    sqlite3_str_appendchar(o, 1, B64[d[i+2] & 63]);
  }
  if(i < n){
    int rem = n - i;
    sqlite3_str_appendchar(o, 1, B64[d[i] >> 2]);
    if(rem == 1){
      sqlite3_str_appendchar(o, 1, B64[(d[i] & 3) << 4]);
      sqlite3_str_append(o, "==", 2);
    }else{
      sqlite3_str_appendchar(o, 1, B64[((d[i] & 3) << 4) | (d[i+1] >> 4)]);
      sqlite3_str_appendchar(o, 1, B64[(d[i+1] & 15) << 2]);
      sqlite3_str_appendchar(o, 1, '=');
    }
  }
}

/* Append a JSON string literal (with surrounding quotes) for z[0..n). */
static void appendJsonString(sqlite3_str *o, const char *z, int n){
  int i;
  sqlite3_str_appendchar(o, 1, '"');
  for(i = 0; i < n; i++){
    unsigned char c = (unsigned char)z[i];
    switch(c){
      case '"':  sqlite3_str_append(o, "\\\"", 2); break;
      case '\\': sqlite3_str_append(o, "\\\\", 2); break;
      case '\b': sqlite3_str_append(o, "\\b", 2); break;
      case '\f': sqlite3_str_append(o, "\\f", 2); break;
      case '\n': sqlite3_str_append(o, "\\n", 2); break;
      case '\r': sqlite3_str_append(o, "\\r", 2); break;
      case '\t': sqlite3_str_append(o, "\\t", 2); break;
      default:
        if(c < 0x20) sqlite3_str_appendf(o, "\\u%04x", c);
        else         sqlite3_str_appendchar(o, 1, (char)c);
    }
  }
  sqlite3_str_appendchar(o, 1, '"');
}

/* Append a double as a round-trippable JSON number. */
static void appendDouble(sqlite3_str *o, double d){
  char buf[64];
  sqlite3_snprintf(sizeof(buf), buf, "%.17g", d);
  sqlite3_str_append(o, buf, (int)strlen(buf));
}

/* Append a single SQLite column value as JSON, by REDB db_type. */
static void appendTextCol(sqlite3_str *o, sqlite3_stmt *s, int col){
  const char *t = (const char*)sqlite3_column_text(s, col);
  int n = sqlite3_column_bytes(s, col);
  appendJsonString(o, t ? t : "", n);
}

/* ------------------------------------------------------------------------- */
/* Materializer                                                              */
/* ------------------------------------------------------------------------- */

/* Returns a sqlite3_malloc'd JSON string for the object, or NULL if missing.
** Caller frees with sqlite3_free. */
static char *redbObjectJson(sqlite3 *db, sqlite3_int64 id, int max_depth);

static void buildProps(sqlite3 *db, sqlite3_int64 obj_id, sqlite3_int64 scheme_id,
                       int has_pstruct, sqlite3_int64 pstruct_id,
                       const char *array_index,
                       int has_pval, sqlite3_int64 pval_id,
                       int max_depth, sqlite3_str *out);

/* Locate the single _values row for a (structure, context); positioned stmt or
** NULL. Caller finalizes. Context precedence mirrors the PG function. */
static sqlite3_stmt *findValueRow(sqlite3 *db, sqlite3_int64 struct_id, sqlite3_int64 obj_id,
                                  const char *array_index,
                                  int has_pval, sqlite3_int64 pval_id){
  /* MUST scope by _id_object: a structure is shared across all objects of the
     scheme, so without _id_object the LIMIT 1 returns some other object's value
     (the props of the first object of the scheme leak into every object). All of
     an object's values — top-level and nested — carry _id_object = the root id. */
  const char *cond = has_pval ? "_array_parent_id=?3"
                   : array_index ? "_array_index=?3"
                                 : "_array_index IS NULL";
  char *sql = sqlite3_mprintf(
      "SELECT" VCOLS "FROM _values WHERE _id_structure=?1 AND _id_object=?2 AND %s LIMIT 1", cond);
  sqlite3_stmt *st = 0;
  if(!sql) return 0;
  sqlite3_prepare_v2(db, sql, -1, &st, 0);
  sqlite3_free(sql);
  if(!st) return 0;
  sqlite3_bind_int64(st, 1, struct_id);
  sqlite3_bind_int64(st, 2, obj_id);
  if(has_pval)          sqlite3_bind_int64(st, 3, pval_id);
  else if(array_index)  sqlite3_bind_text(st, 3, array_index, -1, SQLITE_TRANSIENT);
  if(sqlite3_step(st) == SQLITE_ROW) return st;
  sqlite3_finalize(st);
  return 0;
}

/* Append a ListItem JSON object. Returns 1 if appended, 0 if not found. */
static int buildListItem(sqlite3 *db, sqlite3_int64 li_id, int max_depth,
                         sqlite3_str *out){
  sqlite3_stmt *st = 0;
  sqlite3_prepare_v2(db,
      "SELECT _id,_id_list,_value,_alias,_id_object FROM _list_items WHERE _id=?1",
      -1, &st, 0);
  if(!st) return 0;
  sqlite3_bind_int64(st, 1, li_id);
  if(sqlite3_step(st) != SQLITE_ROW){ sqlite3_finalize(st); return 0; }

  sqlite3_str_appendf(out, "{\"id\":%lld", (long long)sqlite3_column_int64(st, 0));
  sqlite3_str_appendf(out, ",\"idList\":%lld", (long long)sqlite3_column_int64(st, 1));
  sqlite3_str_appendall(out, ",\"value\":");
  if(sqlite3_column_type(st, 2) == SQLITE_NULL) sqlite3_str_append(out, "null", 4);
  else appendTextCol(out, st, 2);
  sqlite3_str_appendall(out, ",\"alias\":");
  if(sqlite3_column_type(st, 3) == SQLITE_NULL) sqlite3_str_append(out, "null", 4);
  else appendTextCol(out, st, 3);
  sqlite3_str_appendall(out, ",\"object\":");
  if(sqlite3_column_type(st, 4) == SQLITE_NULL){
    sqlite3_str_append(out, "null", 4);
  }else{
    int d = max_depth - 1; if(d < 0) d = 0;
    char *child = redbObjectJson(db, sqlite3_column_int64(st, 4), d);
    if(child){ sqlite3_str_append(out, child, (int)strlen(child)); sqlite3_free(child); }
    else sqlite3_str_append(out, "null", 4);
  }
  sqlite3_str_appendchar(out, 1, '}');
  sqlite3_finalize(st);
  return 1;
}

/* Emit a primitive value from a positioned _values row by db_type.
** Returns 1 if a non-null value was appended, 0 if the value is null. */
static int emitPrimitive(sqlite3 *db, sqlite3_stmt *r, const char *db_type,
                         const char *type_semantic, int max_depth,
                         sqlite3_str *out){
  /* ListItem stored in old schema as Long, or explicit ListItem db_type. */
  if(sqlite3_column_type(r, VC_LISTITEM) != SQLITE_NULL ||
     strcmp(db_type, "ListItem") == 0){
    if(sqlite3_column_type(r, VC_LISTITEM) != SQLITE_NULL)
      return buildListItem(db, sqlite3_column_int64(r, VC_LISTITEM), max_depth, out);
    return 0;
  }
  /* Object reference inside a primitive array. */
  if(strcmp(type_semantic, "_RObject") == 0){
    if(sqlite3_column_type(r, VC_OBJECT) == SQLITE_NULL) return 0;
    char *child = redbObjectJson(db, sqlite3_column_int64(r, VC_OBJECT), max_depth - 1);
    if(!child) return 0;
    sqlite3_str_append(out, child, (int)strlen(child));
    sqlite3_free(child);
    return 1;
  }
  if(strcmp(db_type, "String") == 0){
    if(sqlite3_column_type(r, VC_STRING) == SQLITE_NULL) return 0;
    appendTextCol(out, r, VC_STRING); return 1;
  }
  if(strcmp(db_type, "Long") == 0){
    if(sqlite3_column_type(r, VC_LONG) == SQLITE_NULL) return 0;
    sqlite3_str_appendf(out, "%lld", (long long)sqlite3_column_int64(r, VC_LONG)); return 1;
  }
  if(strcmp(db_type, "Guid") == 0){
    if(sqlite3_column_type(r, VC_GUID) == SQLITE_NULL) return 0;
    appendTextCol(out, r, VC_GUID); return 1;
  }
  if(strcmp(db_type, "Double") == 0){
    if(sqlite3_column_type(r, VC_DOUBLE) == SQLITE_NULL) return 0;
    appendDouble(out, sqlite3_column_double(r, VC_DOUBLE)); return 1;
  }
  if(strcmp(db_type, "Numeric") == 0){
    if(sqlite3_column_type(r, VC_NUMERIC) == SQLITE_NULL) return 0;
    appendDouble(out, sqlite3_column_double(r, VC_NUMERIC)); return 1;
  }
  if(strcmp(db_type, "DateTimeOffset") == 0){
    if(sqlite3_column_type(r, VC_DATETIME) == SQLITE_NULL) return 0;
    appendTextCol(out, r, VC_DATETIME); return 1;
  }
  if(strcmp(db_type, "Boolean") == 0){
    if(sqlite3_column_type(r, VC_BOOL) == SQLITE_NULL) return 0;
    sqlite3_str_append(out, sqlite3_column_int(r, VC_BOOL) ? "true" : "false",
                       sqlite3_column_int(r, VC_BOOL) ? 4 : 5);
    return 1;
  }
  if(strcmp(db_type, "ByteArray") == 0){
    if(sqlite3_column_type(r, VC_BYTES) == SQLITE_NULL) return 0;
    sqlite3_str_appendchar(out, 1, '"');
    appendBase64(out, (const unsigned char*)sqlite3_column_blob(r, VC_BYTES),
                 sqlite3_column_bytes(r, VC_BYTES));
    sqlite3_str_appendchar(out, 1, '"');
    return 1;
  }
  return 0;
}

/* Find the head (_array_index IS NULL) record id of an array/dictionary in the
** given parent context. Returns 1 + sets *out_id if found. */
static int findCollectionHead(sqlite3 *db, sqlite3_int64 struct_id, sqlite3_int64 obj_id,
                              int has_pval, sqlite3_int64 pval_id,
                              sqlite3_int64 *out_id){
  /* Scope by _id_object (see findValueRow) — otherwise the head of another object's
     collection (e.g. an empty one) is picked, yielding an empty/wrong array. */
  const char *cond = has_pval ? "_array_parent_id=?3" : "_array_parent_id IS NULL";
  char *sql = sqlite3_mprintf(
      "SELECT _id FROM _values WHERE _id_structure=?1 AND _id_object=?2 AND _array_index IS NULL AND %s LIMIT 1",
      cond);
  sqlite3_stmt *st = 0;
  int found = 0;
  if(!sql) return 0;
  sqlite3_prepare_v2(db, sql, -1, &st, 0);
  sqlite3_free(sql);
  if(!st) return 0;
  sqlite3_bind_int64(st, 1, struct_id);
  sqlite3_bind_int64(st, 2, obj_id);
  if(has_pval) sqlite3_bind_int64(st, 3, pval_id);
  if(sqlite3_step(st) == SQLITE_ROW){ *out_id = sqlite3_column_int64(st, 0); found = 1; }
  sqlite3_finalize(st);
  return found;
}

/* Build an array/dictionary collection value into out. Returns 1 if appended
** (head record exists), 0 if the property is null (no head record). */
static int buildCollection(sqlite3 *db, sqlite3_int64 obj_id, sqlite3_int64 scheme_id,
                           sqlite3_int64 struct_id, const char *db_type,
                           const char *type_semantic, int is_dict,
                           int has_pval, sqlite3_int64 pval_id,
                           int max_depth, sqlite3_str *out){
  sqlite3_int64 head_id;
  if(!findCollectionHead(db, struct_id, obj_id, has_pval, pval_id, &head_id)) return 0;

  char *sql = sqlite3_mprintf(
      "SELECT" VCOLS "FROM _values WHERE _id_structure=?1 AND _id_object=?3 AND _array_index IS NOT NULL "
      "AND _array_parent_id=?2 ORDER BY CAST(_array_index AS INTEGER), _array_index");
  sqlite3_stmt *st = 0;
  if(!sql) return 0;
  sqlite3_prepare_v2(db, sql, -1, &st, 0);
  sqlite3_free(sql);
  if(!st) return 0;
  sqlite3_bind_int64(st, 1, struct_id);
  sqlite3_bind_int64(st, 2, head_id);
  sqlite3_bind_int64(st, 3, obj_id);

  sqlite3_str_appendchar(out, 1, is_dict ? '{' : '[');
  int first = 1;
  while(sqlite3_step(st) == SQLITE_ROW){
    sqlite3_int64 elem_id = sqlite3_column_int64(st, VC_ID);
    if(!first) sqlite3_str_appendchar(out, 1, ',');
    first = 0;
    if(is_dict){
      appendTextCol(out, st, VC_AINDEX);   /* key */
      sqlite3_str_appendchar(out, 1, ':');
    }
    if(strcmp(type_semantic, "Object") == 0){
      /* Element is a Class field — recurse by parent_value_id. */
      buildProps(db, obj_id, scheme_id, 1, struct_id, 0, 1, elem_id, max_depth, out);
    }else if(strcmp(type_semantic, "_RObject") == 0){
      if(sqlite3_column_type(st, VC_OBJECT) == SQLITE_NULL){
        sqlite3_str_append(out, "null", 4);
      }else{
        char *child = redbObjectJson(db, sqlite3_column_int64(st, VC_OBJECT), max_depth - 1);
        if(child){ sqlite3_str_append(out, child, (int)strlen(child)); sqlite3_free(child); }
        else sqlite3_str_append(out, "null", 4);
      }
    }else{
      /* Primitive element — preserve nulls so positions/keys stay aligned. */
      if(!emitPrimitive(db, st, db_type, type_semantic, max_depth, out))
        sqlite3_str_append(out, "null", 4);
    }
  }
  sqlite3_finalize(st);
  sqlite3_str_appendchar(out, 1, is_dict ? '}' : ']');
  return 1;
}

/* Build a "{...}" properties object for one (object, parent-structure, element)
** context. Class fields recurse with the same max_depth; object references and
** ListItem.object decrement it. */
static void buildProps(sqlite3 *db, sqlite3_int64 obj_id, sqlite3_int64 scheme_id,
                       int has_pstruct, sqlite3_int64 pstruct_id,
                       const char *array_index,
                       int has_pval, sqlite3_int64 pval_id,
                       int max_depth, sqlite3_str *out){
  char *sql = sqlite3_mprintf(
      "SELECT _structure_id,_name,_collection_type,type_name,db_type,type_semantic "
      "FROM _scheme_metadata_cache WHERE _scheme_id=?1 AND %s "
      "ORDER BY _order,_structure_id",
      has_pstruct ? "_parent_structure_id=?2" : "_parent_structure_id IS NULL");
  sqlite3_stmt *cs = 0;
  sqlite3_str_appendchar(out, 1, '{');
  if(!sql){ sqlite3_str_appendchar(out, 1, '}'); return; }
  sqlite3_prepare_v2(db, sql, -1, &cs, 0);
  sqlite3_free(sql);
  if(!cs){ sqlite3_str_appendchar(out, 1, '}'); return; }
  sqlite3_bind_int64(cs, 1, scheme_id);
  if(has_pstruct) sqlite3_bind_int64(cs, 2, pstruct_id);

  int first = 1;
  while(sqlite3_step(cs) == SQLITE_ROW){
    sqlite3_int64 struct_id = sqlite3_column_int64(cs, 0);
    const char *name = (const char*)sqlite3_column_text(cs, 1);
    int name_n = sqlite3_column_bytes(cs, 1);
    sqlite3_int64 coll = sqlite3_column_type(cs, 2) == SQLITE_NULL
                         ? 0 : sqlite3_column_int64(cs, 2);
    const char *type_name = (const char*)sqlite3_column_text(cs, 3);
    const char *db_type   = (const char*)sqlite3_column_text(cs, 4);
    const char *type_sem  = (const char*)sqlite3_column_text(cs, 5);
    if(!type_name) type_name = "";
    if(!db_type)   db_type = "";
    if(!type_sem)  type_sem = "";

    int is_array = (coll == REDB_COLL_ARRAY);
    int is_dict  = (coll == REDB_COLL_DICTIONARY);

    sqlite3_str *fv = sqlite3_str_new(db);
    int got = 0;

    if(is_array || is_dict){
      got = buildCollection(db, obj_id, scheme_id, struct_id, db_type, type_sem,
                            is_dict, has_pval, pval_id, max_depth, fv);
    }else if(strcmp(type_name, "Object") == 0 && strcmp(type_sem, "_RObject") == 0){
      sqlite3_stmt *r = findValueRow(db, struct_id, obj_id, array_index, has_pval, pval_id);
      if(r){
        if(sqlite3_column_type(r, VC_OBJECT) != SQLITE_NULL){
          char *child = redbObjectJson(db, sqlite3_column_int64(r, VC_OBJECT), max_depth - 1);
          if(child){ sqlite3_str_append(fv, child, (int)strlen(child)); sqlite3_free(child); got = 1; }
        }
        sqlite3_finalize(r);
      }
    }else if(strcmp(type_sem, "Object") == 0){
      /* Class field with hierarchical children. */
      sqlite3_stmt *r = findValueRow(db, struct_id, obj_id, array_index, has_pval, pval_id);
      if(r){
        if(sqlite3_column_type(r, VC_GUID) != SQLITE_NULL){
          sqlite3_int64 row_id = sqlite3_column_int64(r, VC_ID);
          buildProps(db, obj_id, scheme_id, 1, struct_id, 0, 1, row_id, max_depth, fv);
          got = 1;
        }
        sqlite3_finalize(r);
      }
    }else{
      sqlite3_stmt *r = findValueRow(db, struct_id, obj_id, array_index, has_pval, pval_id);
      if(r){
        got = emitPrimitive(db, r, db_type, type_sem, max_depth, fv);
        sqlite3_finalize(r);
      }
    }

    if(got){
      if(!first) sqlite3_str_appendchar(out, 1, ',');
      first = 0;
      appendJsonString(out, name ? name : "", name_n);
      sqlite3_str_appendchar(out, 1, ':');
      sqlite3_str_append(out, sqlite3_str_value(fv), sqlite3_str_length(fv));
    }
    sqlite3_free(sqlite3_str_finish(fv));
  }
  sqlite3_finalize(cs);
  sqlite3_str_appendchar(out, 1, '}');
}

static char *redbObjectJson(sqlite3 *db, sqlite3_int64 id, int max_depth){
  sqlite3_stmt *st = 0;
  sqlite3_prepare_v2(db,
      "SELECT o._id,o._name,o._id_scheme,"
      "(SELECT _name FROM _schemes WHERE _id=o._id_scheme),"
      /* Dates are stored as REAL Julian day (UTC). strftime() consumes a Julian
         REAL directly and emits ISO-8601 'YYYY-MM-DDTHH:MM:SS.SSSZ' (UTC), which
         System.Text.Json's DateTimeOffset/DateTime converter accepts. NULL → NULL. */
      "o._id_parent,o._id_owner,o._id_who_change,"
      "strftime('%Y-%m-%dT%H:%M:%fZ',o._date_create),strftime('%Y-%m-%dT%H:%M:%fZ',o._date_modify),"
      "strftime('%Y-%m-%dT%H:%M:%fZ',o._date_begin),strftime('%Y-%m-%dT%H:%M:%fZ',o._date_complete),"
      "o._key,o._value_long,o._value_string,"
      "o._value_guid,o._note,o._value_bool,o._value_double,o._value_numeric,"
      "strftime('%Y-%m-%dT%H:%M:%fZ',o._value_datetime),o._value_bytes,o._hash "
      /* Soft-deleted objects (_id_scheme = -10, @@__deleted) are treated as
         non-existent: a nested _Object reference to a trashed object resolves
         to NULL (caller appends "null") instead of materializing the tombstone.
         The _values pointer stays intact, so soft-delete remains reversible. */
      "FROM _objects o WHERE o._id=?1 AND o._id_scheme<>-10", -1, &st, 0);
  if(!st) return 0;
  sqlite3_bind_int64(st, 1, id);
  if(sqlite3_step(st) != SQLITE_ROW){ sqlite3_finalize(st); return 0; }

  sqlite3_int64 scheme_id = sqlite3_column_int64(st, 2);
  sqlite3_str *out = sqlite3_str_new(db);

  /* Base fields — always all keys, null preserved (matches PG jsonb_build_object). */
  sqlite3_str_appendf(out, "{\"id\":%lld", (long long)sqlite3_column_int64(st, 0));
  sqlite3_str_append(out, ",\"name\":", 8);
  if(sqlite3_column_type(st, 1) == SQLITE_NULL) sqlite3_str_append(out, "null", 4); else appendTextCol(out, st, 1);
  sqlite3_str_appendf(out, ",\"scheme_id\":%lld", (long long)scheme_id);
  sqlite3_str_appendall(out, ",\"scheme_name\":");
  if(sqlite3_column_type(st, 3) == SQLITE_NULL) sqlite3_str_append(out, "null", 4); else appendTextCol(out, st, 3);
  sqlite3_str_append(out, ",\"parent_id\":", 13);
  if(sqlite3_column_type(st, 4) == SQLITE_NULL) sqlite3_str_append(out, "null", 4);
  else sqlite3_str_appendf(out, "%lld", (long long)sqlite3_column_int64(st, 4));
  sqlite3_str_appendf(out, ",\"owner_id\":%lld", (long long)sqlite3_column_int64(st, 5));
  sqlite3_str_appendf(out, ",\"who_change_id\":%lld", (long long)sqlite3_column_int64(st, 6));
  sqlite3_str_append(out, ",\"date_create\":", 15);
  if(sqlite3_column_type(st, 7) == SQLITE_NULL) sqlite3_str_append(out, "null", 4); else appendTextCol(out, st, 7);
  sqlite3_str_append(out, ",\"date_modify\":", 15);
  if(sqlite3_column_type(st, 8) == SQLITE_NULL) sqlite3_str_append(out, "null", 4); else appendTextCol(out, st, 8);
  sqlite3_str_append(out, ",\"date_begin\":", 14);
  if(sqlite3_column_type(st, 9) == SQLITE_NULL) sqlite3_str_append(out, "null", 4); else appendTextCol(out, st, 9);
  sqlite3_str_append(out, ",\"date_complete\":", 17);
  if(sqlite3_column_type(st, 10) == SQLITE_NULL) sqlite3_str_append(out, "null", 4); else appendTextCol(out, st, 10);
  sqlite3_str_append(out, ",\"key\":", 7);
  if(sqlite3_column_type(st, 11) == SQLITE_NULL) sqlite3_str_append(out, "null", 4);
  else sqlite3_str_appendf(out, "%lld", (long long)sqlite3_column_int64(st, 11));
  sqlite3_str_append(out, ",\"value_long\":", 14);
  if(sqlite3_column_type(st, 12) == SQLITE_NULL) sqlite3_str_append(out, "null", 4);
  else sqlite3_str_appendf(out, "%lld", (long long)sqlite3_column_int64(st, 12));
  sqlite3_str_append(out, ",\"value_string\":", 16);
  if(sqlite3_column_type(st, 13) == SQLITE_NULL) sqlite3_str_append(out, "null", 4); else appendTextCol(out, st, 13);
  sqlite3_str_append(out, ",\"value_guid\":", 14);
  if(sqlite3_column_type(st, 14) == SQLITE_NULL) sqlite3_str_append(out, "null", 4); else appendTextCol(out, st, 14);
  sqlite3_str_append(out, ",\"note\":", 8);
  if(sqlite3_column_type(st, 15) == SQLITE_NULL) sqlite3_str_append(out, "null", 4); else appendTextCol(out, st, 15);
  sqlite3_str_append(out, ",\"value_bool\":", 14);
  if(sqlite3_column_type(st, 16) == SQLITE_NULL) sqlite3_str_append(out, "null", 4);
  else sqlite3_str_append(out, sqlite3_column_int(st, 16) ? "true" : "false", sqlite3_column_int(st, 16) ? 4 : 5);
  sqlite3_str_append(out, ",\"value_double\":", 16);
  if(sqlite3_column_type(st, 17) == SQLITE_NULL) sqlite3_str_append(out, "null", 4); else appendDouble(out, sqlite3_column_double(st, 17));
  sqlite3_str_append(out, ",\"value_numeric\":", 17);
  if(sqlite3_column_type(st, 18) == SQLITE_NULL) sqlite3_str_append(out, "null", 4); else appendDouble(out, sqlite3_column_double(st, 18));
  sqlite3_str_append(out, ",\"value_datetime\":", 18);
  if(sqlite3_column_type(st, 19) == SQLITE_NULL) sqlite3_str_append(out, "null", 4); else appendTextCol(out, st, 19);
  sqlite3_str_append(out, ",\"value_bytes\":", 15);
  if(sqlite3_column_type(st, 20) == SQLITE_NULL){
    sqlite3_str_append(out, "null", 4);
  }else{
    sqlite3_str_appendchar(out, 1, '"');
    appendBase64(out, (const unsigned char*)sqlite3_column_blob(st, 20), sqlite3_column_bytes(st, 20));
    sqlite3_str_appendchar(out, 1, '"');
  }
  sqlite3_str_append(out, ",\"hash\":", 8);
  if(sqlite3_column_type(st, 21) == SQLITE_NULL) sqlite3_str_append(out, "null", 4); else appendTextCol(out, st, 21);
  sqlite3_finalize(st);

  if(max_depth > 0){
    /* properties: null when the object has no _values rows at all. */
    sqlite3_stmt *hv = 0;
    int has_values = 0;
    sqlite3_prepare_v2(db, "SELECT 1 FROM _values WHERE _id_object=?1 LIMIT 1", -1, &hv, 0);
    if(hv){
      sqlite3_bind_int64(hv, 1, id);
      has_values = (sqlite3_step(hv) == SQLITE_ROW);
      sqlite3_finalize(hv);
    }
    sqlite3_str_append(out, ",\"properties\":", 14);
    if(has_values) buildProps(db, id, scheme_id, 0, 0, 0, 0, 0, max_depth, out);
    else           sqlite3_str_append(out, "null", 4);
  }

  sqlite3_str_appendchar(out, 1, '}');
  return sqlite3_str_finish(out);
}

/* ------------------------------------------------------------------------- */
/* Writer: save_object_json (inverse of get_object_json)                     */
/* Port of redb.SQLite/sql/redb_save_json_objects.sql. JSON parsing reuses    */
/* SQLite's built-in JSON1 (json_extract / json_type / json_each).            */
/* Strategy: DeleteInsert. Mutually recursive with saveObject for _RObject.   */
/* ------------------------------------------------------------------------- */

static sqlite3_int64 saveObject(sqlite3 *db, const char *json, int *ok);
static void saveProps(sqlite3 *db, sqlite3_int64 obj_id, sqlite3_int64 scheme_id,
                      int has_pstruct, sqlite3_int64 pstruct_id,
                      const char *props_json,
                      int has_pval, sqlite3_int64 pval_id);
static void insertSingleObjectRef(sqlite3 *db, sqlite3_int64 struct_id, sqlite3_int64 obj_id,
                                  int has_pval, sqlite3_int64 pval, int has_obj,
                                  sqlite3_int64 object_id);

/* Allocate the next object/value id by bumping the _global_identity AUTOINCREMENT
   high-water mark in sqlite_sequence (same source as the C# keygen). */
sqlite3_int64 nextId(sqlite3 *db){
  sqlite3_stmt *st = 0;
  sqlite3_int64 id = 0;
  sqlite3_prepare_v2(db,
      "UPDATE sqlite_sequence SET seq=seq+1 WHERE name='_global_identity' RETURNING seq",
      -1, &st, 0);
  if(st){
    if(sqlite3_step(st) == SQLITE_ROW) id = sqlite3_column_int64(st, 0);
    sqlite3_finalize(st);
  }
  return id;
}

/* json_extract(json, path) as malloc'd text (sqlite3_free), or NULL if the
** path is absent / SQL-null. For objects/arrays this is their JSON text. */
char *jsonGetText(sqlite3 *db, const char *json, const char *path){
  sqlite3_stmt *st = 0;
  char *res = 0;
  sqlite3_prepare_v2(db, "SELECT json_extract(?1,?2)", -1, &st, 0);
  if(!st) return 0;
  sqlite3_bind_text(st, 1, json, -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(st, 2, path, -1, SQLITE_TRANSIENT);
  if(sqlite3_step(st) == SQLITE_ROW && sqlite3_column_type(st, 0) != SQLITE_NULL)
    res = sqlite3_mprintf("%s", (const char*)sqlite3_column_text(st, 0));
  sqlite3_finalize(st);
  return res;
}

/* json_extract(json, path) as int64. Sets *found. */
sqlite3_int64 jsonGetInt(sqlite3 *db, const char *json, const char *path, int *found){
  sqlite3_stmt *st = 0;
  sqlite3_int64 v = 0;
  *found = 0;
  sqlite3_prepare_v2(db, "SELECT json_extract(?1,?2)", -1, &st, 0);
  if(st){
    sqlite3_bind_text(st, 1, json, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, path, -1, SQLITE_TRANSIENT);
    if(sqlite3_step(st) == SQLITE_ROW && sqlite3_column_type(st, 0) != SQLITE_NULL){
      v = sqlite3_column_int64(st, 0); *found = 1;
    }
    sqlite3_finalize(st);
  }
  return v;
}

/* json_type(json, path) as malloc'd text (sqlite3_free), or NULL if absent.
** Returns one of: object array text integer real true false null. */
char *jsonTypeAt(sqlite3 *db, const char *json, const char *path){
  sqlite3_stmt *st = 0;
  char *res = 0;
  sqlite3_prepare_v2(db, "SELECT json_type(?1,?2)", -1, &st, 0);
  if(!st) return 0;
  sqlite3_bind_text(st, 1, json, -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(st, 2, path, -1, SQLITE_TRANSIENT);
  if(sqlite3_step(st) == SQLITE_ROW && sqlite3_column_type(st, 0) != SQLITE_NULL)
    res = sqlite3_mprintf("%s", (const char*)sqlite3_column_text(st, 0));
  sqlite3_finalize(st);
  return res;
}

static int b64val(int c){
  if(c >= 'A' && c <= 'Z') return c - 'A';
  if(c >= 'a' && c <= 'z') return c - 'a' + 26;
  if(c >= '0' && c <= '9') return c - '0' + 52;
  if(c == '+') return 62;
  if(c == '/') return 63;
  return -1;
}

/* Decode base64 text into a sqlite3_malloc'd buffer; *outlen set. NULL on empty. */
static unsigned char *b64decode(const char *in, int *outlen){
  int n = (int)strlen(in), i, bits = 0, acc = 0, o = 0;
  unsigned char *out;
  *outlen = 0;
  if(n == 0) return 0;
  out = (unsigned char*)sqlite3_malloc(n);     /* upper bound */
  if(!out) return 0;
  for(i = 0; i < n; i++){
    int v = b64val((unsigned char)in[i]);
    if(v < 0) continue;                          /* skip '=', whitespace, newlines */
    acc = (acc << 6) | v;
    bits += 6;
    if(bits >= 8){ bits -= 8; out[o++] = (unsigned char)((acc >> bits) & 0xFF); }
  }
  *outlen = o;
  return out;
}

/* Bind a base64 JSON string at (json,path) as a BLOB into stmt parameter idx,
** or bind NULL when absent. */
static void bindBase64Blob(sqlite3 *db, sqlite3_stmt *st, int idx,
                           const char *json, const char *path){
  char *b64 = jsonGetText(db, json, path);
  if(!b64){ sqlite3_bind_null(st, idx); return; }
  int len = 0;
  unsigned char *raw = b64decode(b64, &len);
  if(raw){ sqlite3_bind_blob(st, idx, raw, len, sqlite3_free); }
  else     sqlite3_bind_null(st, idx);
  sqlite3_free(b64);
}

/* Route a REDB db_type to its _values storage column. */
static const char *primColumn(const char *db_type){
  if(!strcmp(db_type, "String"))         return "_String";
  if(!strcmp(db_type, "Long"))           return "_Long";
  if(!strcmp(db_type, "Guid"))           return "_Guid";
  if(!strcmp(db_type, "Double"))         return "_Double";
  if(!strcmp(db_type, "Numeric"))        return "_Numeric";
  if(!strcmp(db_type, "DateTimeOffset")) return "_DateTimeOffset";
  if(!strcmp(db_type, "DateTime"))       return "_DateTimeOffset";
  if(!strcmp(db_type, "Boolean"))        return "_Boolean";
  if(!strcmp(db_type, "ByteArray"))      return "_ByteArray";
  if(!strcmp(db_type, "ListItem"))       return "_ListItem";
  return 0;
}

/* Insert one primitive _values row. The value comes from (src_json, src_path).
** array_index is text (NULL for scalar); has_pval gates _array_parent_id. */
static void insertPrimitive(sqlite3 *db, sqlite3_int64 struct_id, sqlite3_int64 obj_id,
                            const char *array_index, int has_pval, sqlite3_int64 pval,
                            const char *db_type, const char *jtype,
                            const char *src_json, const char *src_path){
  const char *col = primColumn(db_type);
  if(!col) return;
  sqlite3_int64 id = nextId(db);

  /* ByteArray: base64 text -> BLOB (decoded in C). */
  if(!strcmp(db_type, "ByteArray")){
    sqlite3_stmt *st = 0;
    sqlite3_prepare_v2(db,
        "INSERT INTO _values(_id,_id_structure,_id_object,_array_index,_array_parent_id,_ByteArray)"
        " VALUES(?1,?2,?3,?4,?5,?6)", -1, &st, 0);
    if(!st) return;
    sqlite3_bind_int64(st, 1, id);
    sqlite3_bind_int64(st, 2, struct_id);
    sqlite3_bind_int64(st, 3, obj_id);
    if(array_index) sqlite3_bind_text(st, 4, array_index, -1, SQLITE_TRANSIENT); else sqlite3_bind_null(st, 4);
    if(has_pval) sqlite3_bind_int64(st, 5, pval); else sqlite3_bind_null(st, 5);
    bindBase64Blob(db, st, 6, src_json, src_path);
    sqlite3_step(st);
    sqlite3_finalize(st);
    return;
  }

  /* ListItem: value is either {id,...} object or a bare numeric id. */
  if(!strcmp(db_type, "ListItem")){
    int found = 0;
    sqlite3_int64 li;
    if(jtype && !strcmp(jtype, "object")){
      char *idpath = sqlite3_mprintf("%s.id", src_path);
      li = jsonGetInt(db, src_json, idpath, &found);
      sqlite3_free(idpath);
    }else{
      li = jsonGetInt(db, src_json, src_path, &found);
    }
    if(!found) return;
    sqlite3_stmt *st = 0;
    sqlite3_prepare_v2(db,
        "INSERT INTO _values(_id,_id_structure,_id_object,_array_index,_array_parent_id,_ListItem)"
        " VALUES(?1,?2,?3,?4,?5,?6)", -1, &st, 0);
    if(!st) return;
    sqlite3_bind_int64(st, 1, id);
    sqlite3_bind_int64(st, 2, struct_id);
    sqlite3_bind_int64(st, 3, obj_id);
    if(array_index) sqlite3_bind_text(st, 4, array_index, -1, SQLITE_TRANSIENT); else sqlite3_bind_null(st, 4);
    if(has_pval) sqlite3_bind_int64(st, 5, pval); else sqlite3_bind_null(st, 5);
    sqlite3_bind_int64(st, 6, li);
    sqlite3_step(st);
    sqlite3_finalize(st);
    return;
  }

  /* Standard primitive: let SQLite extract+type via json_extract inline.
     Boolean comes back as integer 1/0 -> _Boolean; numerics as int/real.
     _DateTimeOffset stores REAL Julian day (UTC) -> wrap the extracted ISO
     string in julianday() (parses any offset/separator). */
  const char *valExpr = (col && !strcmp(col, "_DateTimeOffset"))
                          ? "julianday(json_extract(?6,?7))"
                          : "json_extract(?6,?7)";
  char *sql = sqlite3_mprintf(
      "INSERT INTO _values(_id,_id_structure,_id_object,_array_index,_array_parent_id,%s)"
      " VALUES(?1,?2,?3,?4,?5, %s)", col, valExpr);
  if(!sql) return;
  sqlite3_stmt *st = 0;
  sqlite3_prepare_v2(db, sql, -1, &st, 0);
  sqlite3_free(sql);
  if(!st) return;
  sqlite3_bind_int64(st, 1, id);
  sqlite3_bind_int64(st, 2, struct_id);
  sqlite3_bind_int64(st, 3, obj_id);
  if(array_index) sqlite3_bind_text(st, 4, array_index, -1, SQLITE_TRANSIENT); else sqlite3_bind_null(st, 4);
  if(has_pval) sqlite3_bind_int64(st, 5, pval); else sqlite3_bind_null(st, 5);
  sqlite3_bind_text(st, 6, src_json, -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(st, 7, src_path, -1, SQLITE_TRANSIENT);
  sqlite3_step(st);
  sqlite3_finalize(st);
}

/* Insert a collection head record (marker that the array/dict property exists)
** and return its id. */
static sqlite3_int64 insertCollectionHead(sqlite3 *db, sqlite3_int64 struct_id,
                                          sqlite3_int64 obj_id, int has_pval,
                                          sqlite3_int64 pval){
  sqlite3_int64 id = nextId(db);
  sqlite3_stmt *st = 0;
  sqlite3_prepare_v2(db,
      "INSERT INTO _values(_id,_id_structure,_id_object,_array_index,_array_parent_id)"
      " VALUES(?1,?2,?3,NULL,?4)", -1, &st, 0);
  if(!st) return id;
  sqlite3_bind_int64(st, 1, id);
  sqlite3_bind_int64(st, 2, struct_id);
  sqlite3_bind_int64(st, 3, obj_id);
  if(has_pval) sqlite3_bind_int64(st, 4, pval); else sqlite3_bind_null(st, 4);
  sqlite3_step(st);
  sqlite3_finalize(st);
  return id;
}

/* Insert an element row carrying just structural keys (array_index + parent),
** used for Class elements (children attach via _array_parent_id) and as the
** _Object carrier for _RObject elements. Returns the new row id. */
static sqlite3_int64 insertElementRow(sqlite3 *db, sqlite3_int64 struct_id,
                                      sqlite3_int64 obj_id, const char *array_index,
                                      sqlite3_int64 head_id, int has_object,
                                      sqlite3_int64 object_id){
  sqlite3_int64 id = nextId(db);
  sqlite3_stmt *st = 0;
  sqlite3_prepare_v2(db,
      "INSERT INTO _values(_id,_id_structure,_id_object,_Object,_array_index,_array_parent_id)"
      " VALUES(?1,?2,?3,?4,?5,?6)", -1, &st, 0);
  if(!st) return id;
  sqlite3_bind_int64(st, 1, id);
  sqlite3_bind_int64(st, 2, struct_id);
  sqlite3_bind_int64(st, 3, obj_id);
  if(has_object) sqlite3_bind_int64(st, 4, object_id); else sqlite3_bind_null(st, 4);
  if(array_index) sqlite3_bind_text(st, 5, array_index, -1, SQLITE_TRANSIENT); else sqlite3_bind_null(st, 5);
  sqlite3_bind_int64(st, 6, head_id);
  sqlite3_step(st);
  sqlite3_finalize(st);
  return id;
}

/* Resolve an _RObject element/field: a nested object -> recursive save (returns
** new id); a bare number -> that id; null -> not set. */
static int resolveRObject(sqlite3 *db, const char *elem_json, const char *jtype,
                          sqlite3_int64 *out_id){
  if(jtype && !strcmp(jtype, "object")){
    int ok = 0;
    sqlite3_int64 nid = saveObject(db, elem_json, &ok);
    if(ok){ *out_id = nid; return 1; }
    return 0;
  }
  if(jtype && (!strcmp(jtype, "integer") || !strcmp(jtype, "real"))){
    int found = 0;
    *out_id = jsonGetInt(db, elem_json, "$", &found);
    return found;
  }
  return 0;
}

/* Iterate the elements of a collection (array or dictionary) via json_each and
** write rows. For arrays json_each yields integer keys ("0","1",...); for
** dictionaries it yields the text keys — both map straight to _array_index. */
static void saveCollectionElements(sqlite3 *db, sqlite3_int64 obj_id, sqlite3_int64 scheme_id,
                                    sqlite3_int64 struct_id, const char *db_type,
                                    const char *type_semantic, sqlite3_int64 head_id,
                                    const char *coll_json){
  sqlite3_stmt *je = 0;
  sqlite3_prepare_v2(db, "SELECT key, type, value FROM json_each(?1)", -1, &je, 0);
  if(!je) return;
  sqlite3_bind_text(je, 1, coll_json, -1, SQLITE_TRANSIENT);

  while(sqlite3_step(je) == SQLITE_ROW){
    char *key   = sqlite3_mprintf("%s", (const char*)sqlite3_column_text(je, 0));
    char *etype = sqlite3_mprintf("%s", (const char*)sqlite3_column_text(je, 1));

    if(!strcmp(type_semantic, "Object")){
      /* Class element: structural row + recurse into its child properties. */
      char *elem = sqlite3_mprintf("%s", (const char*)sqlite3_column_text(je, 2));
      sqlite3_int64 elem_id = insertElementRow(db, struct_id, obj_id, key, head_id, 0, 0);
      if(etype && !strcmp(etype, "object"))
        saveProps(db, obj_id, scheme_id, 1, struct_id, elem, 1, elem_id);
      sqlite3_free(elem);
    }else if(!strcmp(type_semantic, "_RObject")){
      char *elem = sqlite3_mprintf("%s", (const char*)sqlite3_column_text(je, 2));
      sqlite3_int64 nid = 0;
      int has = resolveRObject(db, elem, etype, &nid);
      insertElementRow(db, struct_id, obj_id, key, head_id, has, nid);
      sqlite3_free(elem);
    }else{
      /* Primitive element: route value (the json_each element) into its column. */
      char *elem = sqlite3_mprintf("%s", (const char*)sqlite3_column_text(je, 2));
      insertPrimitive(db, struct_id, obj_id, key, 1, head_id, db_type, etype, elem, "$");
      sqlite3_free(elem);
    }
    sqlite3_free(key);
    sqlite3_free(etype);
  }
  sqlite3_finalize(je);
}

static void saveProps(sqlite3 *db, sqlite3_int64 obj_id, sqlite3_int64 scheme_id,
                      int has_pstruct, sqlite3_int64 pstruct_id,
                      const char *props_json,
                      int has_pval, sqlite3_int64 pval_id){
  char *sql = sqlite3_mprintf(
      "SELECT _structure_id,_name,_collection_type,type_name,db_type,type_semantic "
      "FROM _scheme_metadata_cache WHERE _scheme_id=?1 AND %s "
      "ORDER BY _order,_structure_id",
      has_pstruct ? "_parent_structure_id=?2" : "_parent_structure_id IS NULL");
  if(!sql) return;
  sqlite3_stmt *cs = 0;
  sqlite3_prepare_v2(db, sql, -1, &cs, 0);
  sqlite3_free(sql);
  if(!cs) return;
  sqlite3_bind_int64(cs, 1, scheme_id);
  if(has_pstruct) sqlite3_bind_int64(cs, 2, pstruct_id);

  while(sqlite3_step(cs) == SQLITE_ROW){
    sqlite3_int64 struct_id = sqlite3_column_int64(cs, 0);
    char *name      = sqlite3_mprintf("%s", (const char*)sqlite3_column_text(cs, 1));
    sqlite3_int64 coll = sqlite3_column_type(cs, 2) == SQLITE_NULL ? 0 : sqlite3_column_int64(cs, 2);
    char *type_name = sqlite3_mprintf("%s", sqlite3_column_text(cs, 3) ? (const char*)sqlite3_column_text(cs, 3) : "");
    char *db_type   = sqlite3_mprintf("%s", sqlite3_column_text(cs, 4) ? (const char*)sqlite3_column_text(cs, 4) : "");
    char *type_sem  = sqlite3_mprintf("%s", sqlite3_column_text(cs, 5) ? (const char*)sqlite3_column_text(cs, 5) : "");

    char *path  = sqlite3_mprintf("$.\"%s\"", name);
    char *jtype = jsonTypeAt(db, props_json, path);   /* NULL=absent */

    /* Skip absent or JSON-null fields. */
    if(jtype && strcmp(jtype, "null") != 0){
      int is_array = (coll == REDB_COLL_ARRAY);
      int is_dict  = (coll == REDB_COLL_DICTIONARY);

      if(is_array && !strcmp(jtype, "array")){
        char *coll_json = jsonGetText(db, props_json, path);
        sqlite3_int64 head = insertCollectionHead(db, struct_id, obj_id, has_pval, pval_id);
        if(coll_json){
          saveCollectionElements(db, obj_id, scheme_id, struct_id, db_type, type_sem, head, coll_json);
          sqlite3_free(coll_json);
        }
      }else if(is_dict && !strcmp(jtype, "object")){
        char *coll_json = jsonGetText(db, props_json, path);
        sqlite3_int64 head = insertCollectionHead(db, struct_id, obj_id, has_pval, pval_id);
        if(coll_json){
          saveCollectionElements(db, obj_id, scheme_id, struct_id, db_type, type_sem, head, coll_json);
          sqlite3_free(coll_json);
        }
      }else if(!strcmp(type_name, "Object") && !strcmp(type_sem, "_RObject")){
        /* Single object reference: nested object -> recursive save, else id ref. */
        char *field = jsonGetText(db, props_json, path);
        sqlite3_int64 nid = 0;
        int has = resolveRObject(db, field, jtype, &nid);
        insertSingleObjectRef(db, struct_id, obj_id, has_pval, pval_id, has, nid);
        if(field) sqlite3_free(field);
      }else if(!strcmp(type_sem, "Object")){
        /* Class field: marker row with non-null _Guid, then recurse. */
        if(!strcmp(jtype, "object")){
          char *field = jsonGetText(db, props_json, path);
          sqlite3_int64 id = nextId(db);
          sqlite3_stmt *st = 0;
          sqlite3_prepare_v2(db,
              "INSERT INTO _values(_id,_id_structure,_id_object,_Guid,_array_parent_id)"
              " VALUES(?1,?2,?3, lower(hex(randomblob(16))), ?4)", -1, &st, 0);
          if(st){
            sqlite3_bind_int64(st, 1, id);
            sqlite3_bind_int64(st, 2, struct_id);
            sqlite3_bind_int64(st, 3, obj_id);
            if(has_pval) sqlite3_bind_int64(st, 4, pval_id); else sqlite3_bind_null(st, 4);
            sqlite3_step(st);
            sqlite3_finalize(st);
          }
          if(field){ saveProps(db, obj_id, scheme_id, 1, struct_id, field, 1, id); sqlite3_free(field); }
        }
      }else{
        /* Primitive scalar field. */
        insertPrimitive(db, struct_id, obj_id, 0, has_pval, pval_id, db_type, jtype, props_json, path);
      }
    }

    sqlite3_free(jtype);
    sqlite3_free(path);
    sqlite3_free(name);
    sqlite3_free(type_name);
    sqlite3_free(db_type);
    sqlite3_free(type_sem);
  }
  sqlite3_finalize(cs);
}

/* Insert a single _RObject reference value (its own row, parent = parent value). */
static void insertSingleObjectRef(sqlite3 *db, sqlite3_int64 struct_id, sqlite3_int64 obj_id,
                                  int has_pval, sqlite3_int64 pval, int has_obj,
                                  sqlite3_int64 object_id){
  sqlite3_int64 id = nextId(db);
  sqlite3_stmt *st = 0;
  sqlite3_prepare_v2(db,
      "INSERT INTO _values(_id,_id_structure,_id_object,_Object,_array_parent_id)"
      " VALUES(?1,?2,?3,?4,?5)", -1, &st, 0);
  if(!st) return;
  sqlite3_bind_int64(st, 1, id);
  sqlite3_bind_int64(st, 2, struct_id);
  sqlite3_bind_int64(st, 3, obj_id);
  if(has_obj) sqlite3_bind_int64(st, 4, object_id); else sqlite3_bind_null(st, 4);
  if(has_pval) sqlite3_bind_int64(st, 5, pval); else sqlite3_bind_null(st, 5);
  sqlite3_step(st);
  sqlite3_finalize(st);
}

/* save_object_json entry: JSON -> _objects (upsert) + _values (delete-insert).
** Returns the object id; *ok=0 on validation failure. */
static sqlite3_int64 saveObject(sqlite3 *db, const char *json, int *ok){
  int found = 0;
  *ok = 0;
  sqlite3_int64 scheme_id = jsonGetInt(db, json, "$.scheme_id", &found);
  if(!found) return 0;                                  /* scheme_id required */

  sqlite3_int64 obj_id = jsonGetInt(db, json, "$.id", &found);
  if(!found || obj_id == 0) obj_id = nextId(db);

  int exists = 0;
  sqlite3_stmt *ck = 0;
  sqlite3_prepare_v2(db, "SELECT 1 FROM _objects WHERE _id=?1", -1, &ck, 0);
  if(ck){ sqlite3_bind_int64(ck, 1, obj_id); exists = (sqlite3_step(ck) == SQLITE_ROW); sqlite3_finalize(ck); }

  if(exists){
    sqlite3_stmt *st = 0;
    sqlite3_prepare_v2(db,
        "UPDATE _objects SET "
        "_id_scheme=?1,"
        "_id_parent=json_extract(?2,'$.parent_id'),"
        "_id_owner=COALESCE(json_extract(?2,'$.owner_id'),_id_owner),"
        "_id_who_change=COALESCE(json_extract(?2,'$.who_change_id'),_id_who_change),"
        "_name=json_extract(?2,'$.name'),"
        "_note=json_extract(?2,'$.note'),"
        "_key=json_extract(?2,'$.key'),"
        "_hash=json_extract(?2,'$.hash'),"
        "_date_modify=julianday('now'),"  /* REAL Julian day (UTC) */
        "_date_begin=julianday(json_extract(?2,'$.date_begin')),"
        "_date_complete=julianday(json_extract(?2,'$.date_complete')),"
        "_value_long=json_extract(?2,'$.value_long'),"
        "_value_string=json_extract(?2,'$.value_string'),"
        "_value_guid=json_extract(?2,'$.value_guid'),"
        "_value_bool=json_extract(?2,'$.value_bool'),"
        "_value_double=json_extract(?2,'$.value_double'),"
        "_value_numeric=json_extract(?2,'$.value_numeric'),"
        "_value_datetime=julianday(json_extract(?2,'$.value_datetime')),"
        "_value_bytes=?3 "
        "WHERE _id=?4", -1, &st, 0);
    if(st){
      sqlite3_bind_int64(st, 1, scheme_id);
      sqlite3_bind_text(st, 2, json, -1, SQLITE_TRANSIENT);
      bindBase64Blob(db, st, 3, json, "$.value_bytes");
      sqlite3_bind_int64(st, 4, obj_id);
      sqlite3_step(st);
      sqlite3_finalize(st);
    }
    sqlite3_stmt *del = 0;
    sqlite3_prepare_v2(db, "DELETE FROM _values WHERE _id_object=?1", -1, &del, 0);
    if(del){ sqlite3_bind_int64(del, 1, obj_id); sqlite3_step(del); sqlite3_finalize(del); }
  }else{
    sqlite3_stmt *st = 0;
    sqlite3_prepare_v2(db,
        "INSERT INTO _objects("
        "_id,_id_scheme,_id_parent,_id_owner,_id_who_change,_name,_note,_key,_hash,"
        "_date_create,_date_modify,_date_begin,_date_complete,"
        "_value_long,_value_string,_value_guid,_value_bool,_value_double,"
        "_value_numeric,_value_datetime,_value_bytes) VALUES("
        "?1,?4,json_extract(?2,'$.parent_id'),"
        "COALESCE(json_extract(?2,'$.owner_id'),1),"
        "COALESCE(json_extract(?2,'$.who_change_id'),1),"
        "json_extract(?2,'$.name'),json_extract(?2,'$.note'),json_extract(?2,'$.key'),"
        "json_extract(?2,'$.hash'),"
        "COALESCE(julianday(json_extract(?2,'$.date_create')),julianday('now')),"
        "julianday('now'),"
        "julianday(json_extract(?2,'$.date_begin')),julianday(json_extract(?2,'$.date_complete')),"
        "json_extract(?2,'$.value_long'),json_extract(?2,'$.value_string'),"
        "json_extract(?2,'$.value_guid'),json_extract(?2,'$.value_bool'),"
        "json_extract(?2,'$.value_double'),json_extract(?2,'$.value_numeric'),"
        "julianday(json_extract(?2,'$.value_datetime')),?3)", -1, &st, 0);
    if(st){
      sqlite3_bind_int64(st, 1, obj_id);
      sqlite3_bind_text(st, 2, json, -1, SQLITE_TRANSIENT);
      bindBase64Blob(db, st, 3, json, "$.value_bytes");
      sqlite3_bind_int64(st, 4, scheme_id);
      sqlite3_step(st);
      sqlite3_finalize(st);
    }
  }

  char *jtype = jsonTypeAt(db, json, "$.properties");
  if(jtype && !strcmp(jtype, "object")){
    char *props = jsonGetText(db, json, "$.properties");
    if(props){ saveProps(db, obj_id, scheme_id, 0, 0, props, 0, 0); sqlite3_free(props); }
  }
  sqlite3_free(jtype);

  *ok = 1;
  return obj_id;
}

/* ------------------------------------------------------------------------- */
/* SQL function bindings                                                     */
/* ------------------------------------------------------------------------- */

static void redbVersionFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  (void)argc; (void)argv;
  sqlite3_result_text(ctx, REDB_EXT_VERSION, -1, SQLITE_STATIC);
}

static void getObjectJsonFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  sqlite3_int64 id = sqlite3_value_int64(argv[0]);
  int max_depth = (argc > 1 && sqlite3_value_type(argv[1]) != SQLITE_NULL)
                  ? sqlite3_value_int(argv[1]) : 10;
  char *json = redbObjectJson(db, id, max_depth);
  if(!json){ sqlite3_result_null(ctx); return; }
  sqlite3_result_text(ctx, json, -1, sqlite3_free);
}

static void saveObjectJsonFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv){
  (void)argc;
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  const char *json = (const char*)sqlite3_value_text(argv[0]);
  if(!json){ sqlite3_result_error(ctx, "save_object_json: NULL input", -1); return; }
  int ok = 0;
  sqlite3_int64 id = saveObject(db, json, &ok);
  if(!ok){ sqlite3_result_error(ctx, "save_object_json: scheme_id is required", -1); return; }
  sqlite3_result_int64(ctx, id);
}

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_redb_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi){
  int rc;
  SQLITE_EXTENSION_INIT2(pApi);
  (void)pzErrMsg;

  rc = sqlite3_create_function(db, "redb_version", 0,
                               SQLITE_UTF8 | SQLITE_DETERMINISTIC, 0,
                               redbVersionFunc, 0, 0);
  if(rc != SQLITE_OK) return rc;

  rc = sqlite3_create_function(db, "get_object_json", 1, SQLITE_UTF8, 0,
                               getObjectJsonFunc, 0, 0);
  if(rc != SQLITE_OK) return rc;
  rc = sqlite3_create_function(db, "get_object_json", 2, SQLITE_UTF8, 0,
                               getObjectJsonFunc, 0, 0);
  if(rc != SQLITE_OK) return rc;

  rc = sqlite3_create_function(db, "save_object_json", 1, SQLITE_UTF8, 0,
                               saveObjectJsonFunc, 0, 0);
  if(rc != SQLITE_OK) return rc;

  /* v2-pvt SQL-generation engine (separate translation unit). */
  rc = redbRegisterPvt(db);
  if(rc != SQLITE_OK) return rc;

  return SQLITE_OK;
}
