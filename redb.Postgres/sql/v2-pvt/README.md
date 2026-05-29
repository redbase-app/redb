# v2-pvt — PVT-based search module for REDB free (PostgreSQL)

> **Назначение**: новый движок поиска объектов REDB, использующий PVT CTE-pattern (один pass по `_values` + PIVOT-агрегация в CTE) вместо EXISTS-pattern с N коррелированными подзапросами, как делает `redb_facets_search.sql`.
>
> Модуль — **самостоятельный**. Все необходимые helper-функции форкнуты сюда из legacy под префиксом `pvt_*`. Старые SQL-файлы НЕ модифицируются (они остаются жить отдельно для пользователей, фиксирующих старую версию пакета).

---

## Деплой

Файлы накатываются в порядке нумерации:

```
00_module_init.sql            -- system infra check + DROP-секция + pvt_module_version()
01_pvt_field_path.sql         -- pvt_normalize_base_field_name, pvt_parse_field_path
02_pvt_type_info.sql          -- pvt_get_listitem_field_type_info
03_pvt_structure_info.sql     -- pvt_find_structure_info
04_pvt_inner_condition.sql    -- pvt_build_inner_condition
05_pvt_single_facet.sql       -- pvt_build_single_facet_condition (used as fallback for complex ops)
06_pvt_hierarchical.sql       -- pvt_build_hierarchical_conditions
07_pvt_base_fields.sql        -- pvt_get_object_base_fields

10_pvt_field_collection.sql   -- pvt_collect_fields, pvt_has_null_check
11_pvt_column_expr.sql        -- pvt_build_column_expr (scalar/array/dict/listitem)
12_pvt_cte_builder.sql        -- pvt_build_cte_sql (flat | tree)
13_pvt_condition.sql          -- pvt_build_field_condition
14_pvt_where.sql              -- pvt_build_where_from_json (recursive $and/$or/$not)
15_pvt_order.sql              -- pvt_build_order_conditions

20_pvt_build_query_sql.sql    -- pvt_build_query_sql (ЕДИНСТВЕННАЯ public entry-point модуля)

99_smoke_tests.sql            -- manual SELECT smoke-tests (commented out)

deprecated/                   -- старые helper-функции (НЕ деплоятся), оставлены для истории:
  21_pvt_search_base.sql      -- pvt_search_objects_base / pvt_get_sql_preview_base
  22_pvt_search_full.sql      -- pvt_search_objects / pvt_get_sql_preview
```

> Файлы из `deprecated/` НЕ накатываются. Они оборачивали `pvt_build_query_sql` в `RETURN NEXT` / `get_object_json` — теперь это делает C#-сторона (см. ниже), а БД отдаёт только готовую SQL-строку. Любой клиент (C#, Python, Go) может попросить SQL и сам решить, выполнять, оборачивать или показывать пользователю как preview.

Пример деплоя локально:

```powershell
$files = Get-ChildItem 'redb.Postgres/sql/v2-pvt/' -Filter '*.sql' | Sort-Object Name
foreach ($f in $files) { psql -d redb -f $f.FullName }
```

---

## Зависимости (read-only)

Модуль ссылается **только** на системную инфраструктуру REDB:

| Объект | Где живёт |
|---|---|
| Таблицы `_objects`, `_values`, `_structures`, `_list_items`, `_scheme_metadata_cache` | core schema |
| `get_scheme_definition(bigint)` | `redb_metadata_cache.sql` |
| `get_object_json(bigint, integer)` | `redb_init.sql` — вызывается **из C#-обёртки**, не из самого pvt-модуля |

Ни одна `pvt_*` функция **не вызывает** unprefixed legacy-функцию (`_build_inner_condition`, `_parse_field_path`, и т.п.) — все они форкнуты в файлы 01..07.

---

## Включение в `redb_init.sql`

**Не делается автоматически.** Решение об активации модуля = отдельный шаг (после ревью и smoke-тестов). До тех пор модуль деплоится вручную.

---

## Замечание про legacy

- `redb_facets_search.sql`, `redb_lazy_loading_search.sql`, `redb_init.sql`, `redb_metadata_cache.sql` — **не редактируем**.
- При исправлении бага в legacy helper'е — зеркалить правку в `pvt_*` копии (см. маркер `-- Forked from ... on 2026-05-18` в шапке каждого forked-файла).

---

## Поток выполнения (free Postgres)

Движок намеренно разделён на **две фазы**, выполняемые двумя round-trip'ами:

```
┌─────────────────────────────────────────────────────────────────┐
│ Фаза 1 (build):                                                 │
│   C# → SELECT pvt_build_query_sql($1,$2::jsonb, ...)            │
│   PG ← '<inner SQL>' (строка вида WITH _pvt_cte AS (...) SELECT │
│         _id FROM _pvt_cte WHERE ... ORDER BY ... LIMIT ...)     │
├─────────────────────────────────────────────────────────────────┤
│ Фаза 2 (execute) — C# оборачивает inner-SQL под задачу:         │
│   * GetSqlPreview()  → возвращает inner-SQL как есть            │
│   * Execute (full)   → SELECT get_object_json(t._id, N)         │
│                          FROM (<inner SQL>) t                   │
│   * Count            → SELECT count(*) FROM (<inner SQL>) t     │
│                        (либо передаётся p_limit=NULL в фазу 1)  │
│   * Any/Exists       → SELECT EXISTS (<inner SQL LIMIT 1>)      │
│   * Projection       → JOIN _values поверх <inner SQL> (TASK-20)│
└─────────────────────────────────────────────────────────────────┘
```

Два round-trip оправданы: inner-SQL получается прозрачным и логируемым; одну и ту же фабрику дёргает любой клиент-язык (Python, Go) без необходимости тянуть C#.

---

## Версионирование

Функция `pvt_module_version()` возвращает semver-строку (например `'0.1.0'`). C# уровень проверяет совместимость при `InitializeAsync` (см. TASK-11/12). При несовпадении major или deployed minor < required — `throw InvalidOperationException`. Никаких runtime-fallback'ов на EXISTS-движок.
