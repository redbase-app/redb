# План миграции SQL скриптов PostgreSQL → MS SQL Server

## ⚠️ КРИТИЧЕСКОЕ ЗАМЕЧАНИЕ

**ВНИМАНИЕ!** Текущий проект `redb.MSSql` практически ПУСТОЙ:
- Есть только `redb.MSSql.csproj` и устаревший `sql/redbMSSQL.sql`
- **НЕТ** C# провайдеров (MSSqlObjectStorageProvider, MSSqlTreeProvider)
- **НЕТ** MSSqlDialect (ISqlDialect реализация)
- **НЕТ** MSSqlRedbContext, Connection, Transaction
- **НЕТ** RedbService для MSSQL

**ПОРЯДОК РАБОТЫ:**
1. **СНАЧАЛА** - обновить SQL схему (`redbMSSQL.sql`)
2. **ЗАТЕМ** - создать базовые C# компоненты для `redb.MSSql` (по образцу `redb.Postgres`)
3. **ПОТОМ** - создавать `redb.MSSql.Pro`

---

## 🎯 ЦЕЛЬ

Портировать все SQL скрипты из `redb.Postgres/sql` в `redb.MSSql/sql` с учетом:
1. Все изменения из миграций (004_add_dictionary_array_support.sql) должны быть **СРАЗУ** в основном файле
2. Синтаксические различия PostgreSQL vs MSSQL
3. Разделение на базовую (OpenSource) и Pro версию

### Что нужно для базовой версии redb.MSSql (до Pro!):

| Компонент | PostgreSQL эквивалент | Статус |
|-----------|----------------------|--------|
| `sql/redbMSSQL.sql` | `redbPostgre.sql` | ⚠️ УСТАРЕВШИЙ |
| `Sql/MSSqlDialect.cs` | `PostgreSqlDialect.cs` | ❌ НЕТ |
| `Data/MSSqlRedbContext.cs` | `NpgsqlRedbContext.cs` | ❌ НЕТ |
| `Data/MSSqlRedbConnection.cs` | `NpgsqlRedbConnection.cs` | ❌ НЕТ |
| `Data/MSSqlKeyGenerator.cs` | `NpgsqlKeyGenerator.cs` | ❌ НЕТ |
| `Providers/MSSqlObjectStorageProvider.cs` | `PostgresObjectStorageProvider.cs` | ❌ НЕТ |
| `Providers/MSSqlTreeProvider.cs` | `PostgresTreeProvider.cs` | ❌ НЕТ |
| `Providers/MSSqlSchemeSyncProvider.cs` | `PostgresSchemeSyncProvider.cs` | ❌ НЕТ |
| `Providers/MSSqlPermissionProvider.cs` | `PostgresPermissionProvider.cs` | ❌ НЕТ |
| `Providers/MSSqlUserProvider.cs` | `PostgresUserProvider.cs` | ❌ НЕТ |
| `Providers/MSSqlListProvider.cs` | `PostgresListProvider.cs` | ❌ НЕТ |
| `RedbService.cs` | `RedbService.cs` | ❌ НЕТ |
| `Extensions/ServiceCollectionExtensions.cs` | Аналог | ❌ НЕТ |

---

## 📂 СТРУКТУРА ИСХОДНЫХ SQL ФАЙЛОВ

### redb.Postgres/sql/

#### Базовые файлы (OpenSource)
1. **redbPostgre.sql** (1624 строки) - основная схема БД
   - Таблицы: _types, _schemes, _structures, _objects, _values, etc.
   - Функции: get_scheme_definition, get_object_json, etc.
   - Триггеры: ftr__objects__deleted_objects
   - SEQUENCE: global_identity

2. **004_add_dictionary_array_support.sql** (993 строки) - МИГРАЦИЯ
   - ❌ **НЕ ПОРТИРОВАТЬ как отдельный файл!**
   - ✅ **Изменения включить СРАЗУ в redbMSSQL.sql:**
     - `_schemes._type` column (Class/Array/Dictionary/JsonDocument/XDocument)
     - `_structures._collection_type` и `_key_type` (вместо `_is_array`)
     - `_values._array_index` как TEXT (вместо INT)
     - `_objects._value_*` columns (вместо `_code_*`)
     - Новые типы: Array, Dictionary, JsonDocument, XDocument

3. **redb_structure_tree.sql** (257 строк) - функции для работы с древовидными структурами
   - Рекурсивные CTE для иерархий
   - Функции навигации по дереву

4. **redb_lazy_loading_search.sql** (737 строк) - ленивая загрузка и поиск
   - Функции для lazy loading Props
   - Оптимизированные запросы поиска

5. **redb_json_objects.sql** (608 строк) - JSON сериализация объектов
   - get_object_json функция
   - Конвертация EAV в JSON

6. **redb_facets_search.sql** (3385 строк) - фасетный поиск
   - Продвинутый поиск с фасетами
   - Динамические фильтры

#### Pro файлы
7. **redb_metadata_cache.sql** (316 строк) - кеш метаданных схем
   - Таблица _scheme_metadata_cache
   - Функции синхронизации кеша
   - Триггеры автообновления

8. **redb_aggregation.sql** (603 строки) - агрегации (Pro)
   - Функции агрегации для Pro запросов
   - Групповые операции

9. **redb_grouping.sql** (585 строк) - группировки (Pro)
   - GroupBy реализация
   - Having фильтрация

10. **redb_window.sql** (476 строк) - оконные функции (Pro)
    - ROW_NUMBER, RANK, DENSE_RANK
    - LAG, LEAD, FIRST_VALUE, LAST_VALUE
    - PARTITION BY реализация

11. **redb_projection.sql** (1054 строки) - проекции (Pro)
    - PVT материализация
    - Селекция полей

#### Вспомогательные
12. **PostgreSqlDialect.cs** (790 строк) - C# диалект
    - Методы ISqlDialect
    - Не портируется как SQL, но используется как референс

13. **README.md** (1272 строки) - документация
14. **README_MAX_DEPTH.md** (187 строк) - ограничения глубины

---

## 📋 ПЛАН МИГРАЦИИ (по порядку выполнения)

### ЭТАП 1: Обновление основной схемы БД (3-4 дня)

#### Задача 1.1: Анализ различий redbPostgre.sql vs redbMSSQL.sql

**Файлы:**
- Источник: `redb.Postgres/sql/redbPostgre.sql`
- Текущая версия: `redb.MSSql/sql/redbMSSQL.sql`
- Миграция для включения: `redb.Postgres/sql/004_add_dictionary_array_support.sql`

**Действия:**
1. Сравнить структуру таблиц
2. Найти отсутствующие таблицы/колонки в MSSQL
3. Найти устаревшие колонки (_code_* вместо _value_*)
4. Проверить типы данных
5. Проверить индексы
6. Проверить constraints

**Ожидаемые отличия:**

| Что проверить | PostgreSQL | MSSQL (должно быть) |
|---------------|------------|---------------------|
| **_schemes._type** | ✅ Есть (после 004) | ❌ Отсутствует |
| **_structures._collection_type** | ✅ Есть (вместо _is_array) | ❌ Есть _is_array? |
| **_structures._key_type** | ✅ Есть | ❌ Отсутствует |
| **_values._array_index** | ✅ TEXT | ❌ INT? |
| **_objects._value_long** | ✅ Есть (был _code_int) | ❌ Есть _code_int? |
| **_objects._value_string** | ✅ TEXT (был _code_string) | ❌ VARCHAR(250)? |
| **_objects._value_guid** | ✅ Есть (был _code_guid) | ❌ Есть _code_guid? |
| **_objects._value_bool** | ✅ Есть (был _bool) | ❌ Есть _bool? |
| **_objects._value_double** | ✅ Есть (новый) | ❌ Отсутствует |
| **_objects._value_numeric** | ✅ Есть (новый) | ❌ Отсутствует |
| **_objects._value_datetime** | ✅ Есть (новый) | ❌ Отсутствует |
| **_objects._value_bytes** | ✅ Есть (новый) | ❌ Отсутствует |

**Новые типы в _types:**
```sql
-- Должны быть добавлены в redbMSSQL.sql
INSERT INTO _types (_id, _name, _db_type, _type) VALUES 
(-9223372036854775668, 'Array', 'Guid', 'Array'),
(-9223372036854775667, 'Dictionary', 'Guid', 'Dictionary'),
(-9223372036854775666, 'JsonDocument', 'Guid', 'JsonDocument'),
(-9223372036854775665, 'XDocument', 'Guid', 'XDocument');
```

---

#### Задача 1.2: Создание обновленного redbMSSQL.sql

**Файл:** `redb.MSSql/sql/redbMSSQL.sql` (заменить текущий)

**Включить изменения из 004_add_dictionary_array_support.sql:**

1. **Таблица _types** - добавить коллекции
```sql
-- Collection types
INSERT INTO _types (_id, _name, _db_type, _type) VALUES 
(-9223372036854775668, 'Array', 'Guid', 'Array');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES 
(-9223372036854775667, 'Dictionary', 'Guid', 'Dictionary');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES 
(-9223372036854775666, 'JsonDocument', 'Guid', 'JsonDocument');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES 
(-9223372036854775665, 'XDocument', 'Guid', 'XDocument');
```

2. **Таблица _schemes** - добавить _type
```sql
CREATE TABLE [dbo].[_schemes](
    [_id] BIGINT NOT NULL PRIMARY KEY,
    [_id_parent] BIGINT NULL,
    [_name] NVARCHAR(250) NOT NULL,
    [_alias] NVARCHAR(250) NULL,
    [_name_space] NVARCHAR(1000) NULL,
    [_structure_hash] UNIQUEIDENTIFIER NULL,
    [_type] BIGINT NOT NULL DEFAULT -9223372036854775675, -- Class by default
    CONSTRAINT [FK__schemes__type] FOREIGN KEY ([_type]) REFERENCES [_types]([_id])
);
```

3. **Таблица _structures** - заменить _is_array на _collection_type
```sql
CREATE TABLE [dbo].[_structures](
    [_id] BIGINT NOT NULL PRIMARY KEY,
    [_id_parent] BIGINT NULL,
    [_id_scheme] BIGINT NOT NULL,
    [_id_override] BIGINT NULL,
    [_id_type] BIGINT NOT NULL,
    [_id_list] BIGINT NULL,
    [_name] NVARCHAR(250) NOT NULL,
    [_alias] NVARCHAR(250) NULL,
    [_order] BIGINT NULL,
    [_readonly] BIT NULL,
    [_allow_not_null] BIT NULL,
    -- OLD: [_is_array] BIT NULL,  -- УДАЛИТЬ!
    -- NEW:
    [_collection_type] BIGINT NULL,  -- NULL = not collection, Array/Dictionary type ID
    [_key_type] BIGINT NULL,         -- For Dictionary key type
    [_is_compress] BIT NULL,
    [_store_null] BIT NULL,
    [_default_value] VARBINARY(MAX) NULL,
    [_default_editor] NVARCHAR(MAX) NULL,
    
    CONSTRAINT [FK__structures__collection_type] 
        FOREIGN KEY ([_collection_type]) REFERENCES [_types]([_id]),
    CONSTRAINT [FK__structures__key_type] 
        FOREIGN KEY ([_key_type]) REFERENCES [_types]([_id])
);
```

4. **Таблица _values** - _array_index как NVARCHAR
```sql
CREATE TABLE [dbo].[_values](
    [_id] BIGINT NOT NULL PRIMARY KEY,
    [_id_structure] BIGINT NOT NULL,
    [_id_object] BIGINT NOT NULL,
    [_String] NVARCHAR(850) NULL,
    [_Long] BIGINT NULL,
    [_Guid] UNIQUEIDENTIFIER NULL,
    [_Double] FLOAT NULL,
    [_Numeric] DECIMAL(38, 18) NULL,
    [_DateTime] DATETIME2 NULL,    -- Или DATETIMEOFFSET
    [_Boolean] BIT NULL,
    [_ByteArray] VARBINARY(MAX) NULL,
    [_Text] NVARCHAR(MAX) NULL,
    [_ListItem] BIGINT NULL,
    [_Object] BIGINT NULL,
    [_array_parent_id] BIGINT NULL,
    -- OLD: [_array_index] INT NULL,  -- ИЗМЕНИТЬ!
    -- NEW:
    [_array_index] NVARCHAR(500) NULL  -- TEXT for Dictionary keys!
);
```

5. **Таблица _objects** - переименовать _code_* в _value_*
```sql
CREATE TABLE [dbo].[_objects](
    [_id] BIGINT NOT NULL PRIMARY KEY,
    [_id_parent] BIGINT NULL,
    [_id_scheme] BIGINT NOT NULL,
    [_id_owner] BIGINT NOT NULL,
    [_id_who_change] BIGINT NOT NULL,
    [_date_create] DATETIME2 NOT NULL DEFAULT GETDATE(),
    [_date_modify] DATETIME2 NOT NULL DEFAULT GETDATE(),
    [_date_begin] DATETIME2 NULL,
    [_date_complete] DATETIME2 NULL,
    [_key] BIGINT NULL,
    
    -- OLD names (УДАЛИТЬ):
    -- [_code_int] BIGINT NULL,
    -- [_code_string] NVARCHAR(250) NULL,
    -- [_code_guid] UNIQUEIDENTIFIER NULL,
    -- [_bool] BIT NULL,
    
    -- NEW names для RedbPrimitive<T>:
    [_value_long] BIGINT NULL,              -- was _code_int
    [_value_string] NVARCHAR(MAX) NULL,     -- was _code_string (expanded!)
    [_value_guid] UNIQUEIDENTIFIER NULL,    -- was _code_guid
    [_value_bool] BIT NULL,                 -- was _bool
    [_value_double] FLOAT NULL,             -- NEW
    [_value_numeric] DECIMAL(38, 18) NULL,  -- NEW
    [_value_datetime] DATETIMEOFFSET NULL,  -- NEW
    [_value_bytes] VARBINARY(MAX) NULL,     -- NEW
    
    [_name] NVARCHAR(250) NULL,
    [_note] NVARCHAR(1000) NULL,
    [_hash] VARBINARY(32) NULL
);
```

6. **Индексы** - обновить для новых колонок
```sql
-- Удалить старые
DROP INDEX IF EXISTS IX__objects__code_int;
DROP INDEX IF EXISTS IX__objects__code_string;
DROP INDEX IF EXISTS IX__objects__code_guid;

-- Создать новые
CREATE INDEX IX__objects__value_long 
    ON _objects(_value_long) 
    WHERE _value_long IS NOT NULL;

CREATE INDEX IX__objects__value_string 
    ON _objects(_value_string) 
    WHERE _value_string IS NOT NULL;

CREATE INDEX IX__objects__value_guid 
    ON _objects(_value_guid) 
    WHERE _value_guid IS NOT NULL;

CREATE INDEX IX__objects__value_datetime 
    ON _objects(_value_datetime) 
    WHERE _value_datetime IS NOT NULL;

CREATE INDEX IX__objects__value_numeric 
    ON _objects(_value_numeric) 
    WHERE _value_numeric IS NOT NULL;

-- Для _structures
CREATE INDEX IX__structures__collection_type 
    ON _structures(_collection_type) 
    WHERE _collection_type IS NOT NULL;

CREATE INDEX IX__structures__not_collection 
    ON _structures(_id, _name, _id_scheme) 
    WHERE _collection_type IS NULL;

-- Для _values
CREATE INDEX IX__values__array_key 
    ON _values(_id_structure, _array_index) 
    WHERE _array_index IS NOT NULL;
```

7. **Функции и триггеры** - обновить для новых колонок

```sql
-- Обновить get_scheme_definition для _type, _collection_type, _key_type
-- Обновить ftr__objects__deleted_objects для _value_* колонок
```

---

#### Задача 1.3: Создание скрипта миграции (для существующих БД)

**Файл:** `redb.MSSql/sql/migrations/001_update_to_latest_schema.sql`

Для тех, кто уже использует старую схему MSSQL:

```sql
-- Миграция старой схемы redbMSSQL.sql к новой версии
-- (аналог 004_add_dictionary_array_support.sql для MSSQL)

-- 1. Добавить типы коллекций
-- 2. Добавить _schemes._type
-- 3. Заменить _structures._is_array на _collection_type
-- 4. Изменить _values._array_index INT -> NVARCHAR
-- 5. Переименовать _objects._code_* -> _value_*
-- 6. Пересоздать индексы
```

---

### ЭТАП 2: Портирование базовых SQL функций (2-3 дня)

#### Задача 2.1: redb_structure_tree.sql → redbMSSQL_structure_tree.sql

**Источник:** `redb.Postgres/sql/redb_structure_tree.sql`  
**Цель:** `redb.MSSql/sql/redbMSSQL_structure_tree.sql`

**Что портировать:**
- Функции навигации по древовидной структуре
- Рекурсивные CTE для иерархий
- Функции подсчета глубины

**Изменения для MSSQL:**
- `WITH RECURSIVE` → `WITH` (без RECURSIVE)
- `::type` → `CAST(... AS type)`
- `$1, $2` → `@p0, @p1`
- `CREATE OR REPLACE FUNCTION` → `CREATE PROCEDURE` или UDF

---

#### Задача 2.2: redb_json_objects.sql → redbMSSQL_json_objects.sql

**Источник:** `redb.Postgres/sql/redb_json_objects.sql`  
**Цель:** `redb.MSSql/sql/redbMSSQL_json_objects.sql`

**Что портировать:**
- Функция `get_object_json` - сериализация объекта в JSON
- Конвертация EAV в JSON структуру

**Изменения для MSSQL:**
- PostgreSQL `json_build_object()` → MSSQL `FOR JSON PATH`
- PostgreSQL `json_agg()` → MSSQL `FOR JSON AUTO`
- PostgreSQL JSONB операторы → MSSQL JSON функции

**Пример адаптации:**

```sql
-- PostgreSQL
SELECT json_build_object(
    'id', _id,
    'name', _name,
    'props', (SELECT json_object_agg(_name, _value) FROM props)
) FROM _objects;

-- MSSQL
SELECT 
    _id AS id,
    _name AS name,
    (SELECT * FROM props FOR JSON PATH) AS props
FROM _objects
FOR JSON PATH, WITHOUT_ARRAY_WRAPPER;
```

---

#### Задача 2.3: redb_lazy_loading_search.sql → redbMSSQL_lazy_loading.sql

**Источник:** `redb.Postgres/sql/redb_lazy_loading_search.sql`  
**Цель:** `redb.MSSql/sql/redbMSSQL_lazy_loading.sql`

**Что портировать:**
- Функции для lazy loading Props
- Оптимизированные запросы поиска
- Batch loading для множества объектов

**Изменения для MSSQL:**
- Array параметры → Table-Valued Parameters
- `ANY($1)` → `IN (SELECT Value FROM @idList)`

---

### ЭТАП 3: Портирование Pro SQL функций (4-5 дней)

#### Задача 3.1: redb_metadata_cache.sql → redbMSSQL_metadata_cache.sql (Pro)

**Источник:** `redb.Postgres/sql/redb_metadata_cache.sql`  
**Цель:** `redb.MSSql.Pro/sql/002_scheme_metadata_cache.sql` (уже создан в плане!)

**Статус:** ✅ Уже описан в `SQL_SCRIPTS_DETAILED.md`

**Проверить:**
- Триггеры на изменение _schemes._structure_hash
- Функции sync_metadata_cache_for_scheme
- Поддержка _collection_type, _key_type

---

#### Задача 3.2: redb_projection.sql → ProSqlBuilder.cs (Pro)

**Источник:** `redb.Postgres/sql/redb_projection.sql`  
**Цель:** Реализация в `redb.MSSql.Pro/Query/ProSqlBuilder.cs` + `PvtSqlGenerator.cs`

**Что портировать:**
- PVT материализация через FILTER → CASE WHEN
- Динамическая генерация SELECT с полями
- Маппинг типов для PVT колонок

**Особенность:** Не отдельный SQL файл, а часть C# кода Query провайдера!

---

#### Задача 3.3: redb_aggregation.sql → ProQueryProvider.Aggregation.cs (Pro)

**Источник:** `redb.Postgres/sql/redb_aggregation.sql`  
**Цель:** `redb.MSSql.Pro/Query/ProQueryProvider.Aggregation.cs`

**Что портировать:**
- COUNT, SUM, AVG, MIN, MAX
- DISTINCT агрегации
- Conditional aggregations

**SQL vs C#:**
- SQL функции остаются в файле (если есть хранимые процедуры)
- Основная логика в C# провайдере

---

#### Задача 3.4: redb_grouping.sql → ProQueryProvider.Grouping.cs (Pro)

**Источник:** `redb.Postgres/sql/redb_grouping.sql`  
**Цель:** `redb.MSSql.Pro/Query/ProQueryProvider.Grouping.cs`

**Что портировать:**
- GROUP BY реализация
- HAVING фильтрация
- Агрегации в группах

---

#### Задача 3.5: redb_window.sql → ProQueryProvider.Window.cs (Pro)

**Источник:** `redb.Postgres/sql/redb_window.sql`  
**Цель:** `redb.MSSql.Pro/Query/ProQueryProvider.Window.cs`

**Что портировать:**
- ROW_NUMBER, RANK, DENSE_RANK
- LAG, LEAD
- FIRST_VALUE, LAST_VALUE
- PARTITION BY, ORDER BY

**Совместимость:** Window Functions практически идентичны в PostgreSQL и MSSQL (SQL 2012+)!

---

### ЭТАП 4: Дополнительные функции (опционально, 2-3 дня)

#### Задача 4.1: redb_facets_search.sql → redbMSSQL_facets_search.sql

**Источник:** `redb.Postgres/sql/redb_facets_search.sql` (3385 строк!)  
**Цель:** `redb.MSSql/sql/redbMSSQL_facets_search.sql`

**Приоритет:** P3 (Optional) - сложный продвинутый поиск

**Что портировать:**
- Фасетный поиск с динамическими фильтрами
- Построение facet conditions
- Агрегация результатов поиска

**Сложность:** ВЫСОКАЯ - много динамического SQL и специфичных для PostgreSQL конструкций

---

## 📊 ИТОГОВАЯ ТАБЛИЦА ФАЙЛОВ

| PostgreSQL файл | MSSQL файл | Приоритет | Статус | Этап |
|----------------|------------|-----------|--------|------|
| **redbPostgre.sql** | redbMSSQL.sql (обновить) | P0 | 📝 TODO | 1 |
| **004_add_dictionary_array_support.sql** | ❌ Включить в redbMSSQL.sql | P0 | 📝 TODO | 1 |
| - | migrations/001_update_to_latest_schema.sql | P1 | 📝 TODO | 1 |
| **redb_structure_tree.sql** | redbMSSQL_structure_tree.sql | P0 | 📝 TODO | 2 |
| **redb_json_objects.sql** | redbMSSQL_json_objects.sql | P1 | 📝 TODO | 2 |
| **redb_lazy_loading_search.sql** | redbMSSQL_lazy_loading.sql | P0 | 📝 TODO | 2 |
| **redb_metadata_cache.sql** (Pro) | 002_scheme_metadata_cache.sql | P0 | ✅ Готов | 3 |
| **redb_projection.sql** (Pro) | ProSqlBuilder.cs + C# | P0 | 📝 TODO | 3 |
| **redb_aggregation.sql** (Pro) | ProQueryProvider.Aggregation.cs | P0 | 📝 TODO | 3 |
| **redb_grouping.sql** (Pro) | ProQueryProvider.Grouping.cs | P0 | 📝 TODO | 3 |
| **redb_window.sql** (Pro) | ProQueryProvider.Window.cs | P1 | 📝 TODO | 3 |
| **redb_facets_search.sql** | redbMSSQL_facets_search.sql | P3 | ⏭️ Later | 4 |

---

## 🚀 ПОРЯДОК ВЫПОЛНЕНИЯ

### Шаг 1: Анализ текущей схемы (сегодня)
```bash
# Открыть файлы для сравнения
code redb.Postgres/sql/redbPostgre.sql
code redb.MSSql/sql/redbMSSQL.sql
code redb.Postgres/sql/004_add_dictionary_array_support.sql
```

**Создать документ:** `redb.MSSql/doc/SCHEMA_COMPARISON.md`
- Таблица различий PostgreSQL vs MSSQL
- Список отсутствующих колонок
- Список устаревших колонок

### Шаг 2: Обновление redbMSSQL.sql (день 1-2)
- Включить все изменения из 004_add_dictionary_array_support.sql
- Обновить типы данных
- Обновить индексы
- Обновить функции/триггеры

### Шаг 3: Создание миграции (день 2)
- Скрипт миграции для существующих БД
- Тестирование на тестовой БД

### Шаг 4: Портирование базовых функций (день 3-4)
- redb_structure_tree.sql
- redb_json_objects.sql
- redb_lazy_loading_search.sql

### Шаг 5: Портирование Pro функций (день 5-8)
- Metadata cache (уже готов)
- Projection → C# код
- Aggregation → C# код
- Grouping → C# код
- Window → C# код

### Шаг 6: Тестирование (день 9-10)
- Unit тесты для всех функций
- Интеграционные тесты
- Проверка производительности

---

## ✅ КРИТЕРИИ ГОТОВНОСТИ

### Этап 1: Основная схема
- [ ] redbMSSQL.sql содержит все таблицы из redbPostgre.sql
- [ ] Все колонки из 004_add_dictionary_array_support.sql включены
- [ ] _value_* колонки вместо _code_*
- [ ] _collection_type вместо _is_array
- [ ] _array_index как NVARCHAR
- [ ] Все индексы созданы
- [ ] Все FK constraints созданы
- [ ] Функции и триггеры обновлены

### Этап 2: Базовые функции
- [ ] Древовидные запросы работают
- [ ] JSON сериализация работает
- [ ] Lazy loading работает

### Этап 3: Pro функции
- [ ] Metadata cache синхронизируется
- [ ] PVT материализация работает
- [ ] Агрегации работают
- [ ] Группировки работают
- [ ] Window functions работают

---

## 📞 СЛЕДУЮЩИЙ ШАГ

**НАЧАТЬ С:** Создание `SCHEMA_COMPARISON.md` - детальное сравнение схем

**Команда:**
```bash
# Создать документ для анализа
code redb.MSSql/doc/SCHEMA_COMPARISON.md
```

Готовы начать анализ? 🚀

