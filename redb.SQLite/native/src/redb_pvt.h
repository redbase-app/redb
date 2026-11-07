/*
** Shared internals of the v2-pvt engine, split across translation units:
**   redb_pvt.c            — resolve/collect, expr engine, condition/where,
**                            column-expr, cte, split, order, query entry,
**                            aggregate/group-by; function registration.
**   redb_pvt_analytics.c  — window + projection orchestrators.
**
** These declarations expose the core builders so the analytics TU can reuse
** them. All take the live sqlite3* and return sqlite3_malloc'd SQL fragments
** (caller frees), or NULL on unsupported input. JSON args are parsed via JSON1.
*/
#ifndef REDB_PVT_H
#define REDB_PVT_H

#include "sqlite3ext.h"

/* small helpers */
int   jsonGetBool(sqlite3 *db, const char *json, const char *path);
void  pvtLc(char *dst, const char *src, int n);
int   pvtJsonArrayLen(sqlite3 *db, const char *container, const char *path);

/* expression engine (17) */
char *pvtScalarNode(sqlite3 *db, const char *container, const char *path, const char *fields, const char *prefix);
char *pvtBuildScalarExpr(sqlite3 *db, const char *node, const char *fields, const char *prefix);
char *pvtBuildBoolExpr(sqlite3 *db, const char *node, const char *fields, const char *prefix);

/* order / where / cte / split / shape helpers */
char *pvtCompileOrderCol(sqlite3 *db, const char *elem, const char *fields, const char *base_prefix);
char *pvtBuildOrderConditions(sqlite3 *db, const char *order, const char *fields, const char *base_prefix);
char *pvtBuildWhereFromJson(sqlite3 *db, const char *filter, const char *fields, const char *base_prefix);
char *pvtBuildCteSql(sqlite3 *db, sqlite3_int64 scheme, const char *fields,
                     const char *source_mode, const char *tree_ids_json,
                     int has_max_depth, int max_depth, int force_outer,
                     const char *extra_where, int narrow, int include_seed,
                     int polymorphic, const char *residual_where);
void  pvtSplitFilter(sqlite3 *db, const char *filter, const char *fields, char **out_push, char **out_resid);
int   pvtHasCheck(sqlite3 *db, const char *filter, int absence);
int   pvtHasPropFields(sqlite3 *db, const char *fields);
int   pvtHasNestedDict(sqlite3 *db, const char *fields);

/* general field collection (filter + order + group + aggs + having) */
char *pvtCollectAll(sqlite3 *db, sqlite3_int64 scheme, const char *filter,
                    const char *order, const char *group_by, const char *aggs,
                    const char *having);

/* analytics (redb_pvt_analytics.c) — window expr is referenced by the $over
** hook inside pvtBuildScalarExpr (redb_pvt.c). */
char *pvtBuildWindowExpr(sqlite3 *db, const char *node, const char *fields, const char *prefix);
int   redbRegisterPvtAnalytics(sqlite3 *db);

#endif /* REDB_PVT_H */
