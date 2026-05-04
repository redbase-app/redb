# План реализации redb.MSSql.Pro

## ⚠️ КРИТИЧЕСКОЕ ЗАМЕЧАНИЕ

**СТОП!** Перед созданием Pro версии необходимо:

1. ✅ **Обновить SQL схему** `redb.MSSql/sql/redbMSSQL.sql`
2. ❌ **Создать базовый redb.MSSql** (C# код!) - **СЕЙЧАС ПУСТОЙ!**
3. ⏭️ Только потом создавать redb.MSSql.Pro

**Смотри:** `IMPLEMENTATION_ROADMAP.md` → **ФАЗА 0** (обязательная!)

**Оценка времени:**
- Базовый redb.MSSql: 5-7 дней
- Pro версия: 10-14 дней
- **ИТОГО: 15-21 день**

---

## 🎯 ЦЕЛЬ ПРОЕКТА

Создать Pro версию для MS SQL Server по аналогии с `redb.Postgres.Pro`, включающую:
- **PVT материализацию** вместо функций get_object_json
- **Расширенные запросы**: Aggregation, Grouping, Window functions
- **Систему миграций данных**
- **Параллельную обработку** деревьев значений

---

## 📋 ЭТАП 0: БАЗОВЫЙ redb.MSSql (ОБЯЗАТЕЛЬНО!)

Перед Pro версией нужен работающий базовый redb.MSSql:

| Компонент | Файл | Приоритет |
|-----------|------|-----------|
| SQL схема | `sql/redbMSSQL.sql` | P0 |
| Диалект | `Sql/MSSqlDialect.cs` | P0 |
| Контекст | `Data/MSSqlRedbContext.cs` | P0 |
| Подключение | `Data/MSSqlRedbConnection.cs` | P0 |
| Генератор ID | `Data/MSSqlKeyGenerator.cs` | P0 |
| Провайдер объектов | `Providers/MSSqlObjectStorageProvider.cs` | P0 |
| Провайдер дерева | `Providers/MSSqlTreeProvider.cs` | P0 |
| Провайдер схем | `Providers/MSSqlSchemeSyncProvider.cs` | P0 |
| Сервис | `RedbService.cs` | P0 |
| DI Extensions | `Extensions/ServiceCollectionExtensions.cs` | P0 |

**Подробный план:** См. `IMPLEMENTATION_ROADMAP.md` → ФАЗА 0

---

## 📋 ЭТАП 1: SQL СКРИПТЫ И СХЕМА БД (Pro)

### 1.1. Анализ текущей схемы MSSQL

**Файл:** `redb.MSSql/sql/redbMSSQL.sql`

**⚠️ ПРОБЛЕМА:** Текущий файл УСТАРЕВШИЙ! Нет:
- `_schemes._type` (тип схемы)
- `_structures._collection_type` (вместо _is_array)
- `_structures._key_type` (для Dictionary)
- `_values._array_index` как TEXT
- `_objects._value_*` (вместо _code_*)

**Текущие таблицы:**
- `_objects` - основная таблица объектов
- `_values` - EAV значения
- `_schemes` - схемы типов
- `_structures` - структура полей
- `_types` - типы данных
- `_lists` / `_list_items` - справочники
- `_users` / `_roles` / `_users_roles` - безопасность
- `_permissions` - права доступа
- `_links` - связи
- `_functions` - функции
- `_dependencies` - зависимости схем
- `_deleted_objects` - корзина

**Отсутствующие таблицы (для Pro):**
- `_migrations` - история миграций данных
- `_scheme_metadata_cache` - кеш метаданных схем (опционально)

---

### 1.2. SQL скрипт: Таблица миграций

**Файл:** `redb.MSSql.Pro/sql/001_migrations_table.sql`

#### Описание

Таблица `_migrations` хранит историю всех выполненных миграций данных:
- ComputedFrom - вычисляемые поля
- TypeChange - изменение типов
- DefaultValue - установка значений по умолчанию
- Transform - трансформация данных

#### SQL скрипт

```sql
-- =====================================================
-- REDB Pro: Таблица истории миграций
-- =====================================================

-- Проверка существования таблицы
IF NOT EXISTS (SELECT * FROM sys.objects 
               WHERE object_id = OBJECT_ID(N'[dbo].[_migrations]') 
               AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[_migrations](
        [_id] BIGINT PRIMARY KEY,
        
        -- Уникальный ID миграции "OrderProps_TotalPrice_v1"
        [_migration_id] NVARCHAR(500) NOT NULL,
        
        -- Ссылка на схему
        [_scheme_id] BIGINT NOT NULL,
        CONSTRAINT [FK__migrations__schemes] 
            FOREIGN KEY ([_scheme_id]) 
            REFERENCES [dbo].[_schemes]([_id]) 
            ON DELETE CASCADE,
        
        -- Ссылка на структуру (NULL = вся схема)
        [_structure_id] BIGINT NULL,
        CONSTRAINT [FK__migrations__structures] 
            FOREIGN KEY ([_structure_id]) 
            REFERENCES [dbo].[_structures]([_id]) 
            ON DELETE SET NULL,
        
        -- Имя свойства (для логов)
        [_property_name] NVARCHAR(500) NULL,
        
        -- MD5 от Expression для детекции изменений
        [_expression_hash] NVARCHAR(32) NULL,
        
        -- Тип миграции: ComputedFrom, TypeChange, DefaultValue, Transform
        [_migration_type] NVARCHAR(100) NOT NULL,
        
        -- Когда применена
        [_applied_at] DATETIME2 NOT NULL DEFAULT GETDATE(),
        
        -- Кто применил (user/system)
        [_applied_by] NVARCHAR(250) NULL,
        
        -- SQL который был выполнен (для аудита)
        [_sql_executed] NVARCHAR(MAX) NULL,
        
        -- Сколько записей затронуто
        [_affected_rows] INT NULL,
        
        -- Время выполнения в миллисекундах
        [_duration_ms] INT NULL,
        
        -- Это был dry-run?
        [_dry_run] BIT NOT NULL DEFAULT 0,
        
        -- Уникальность миграции в рамках схемы
        CONSTRAINT [UQ__migrations_scheme] 
            UNIQUE([_scheme_id], [_migration_id])
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
    
    -- Индексы для быстрого поиска
    CREATE INDEX [IX__migrations_scheme] 
        ON [dbo].[_migrations]([_scheme_id])
    
    CREATE INDEX [IX__migrations_applied] 
        ON [dbo].[_migrations]([_applied_at] DESC)
    
    -- Расширенные свойства (комментарии)
    EXEC sp_addextendedproperty 
        @name = N'MS_Description', 
        @value = N'История применённых миграций данных (Pro feature)', 
        @level0type = N'SCHEMA', @level0name = N'dbo',
        @level1type = N'TABLE',  @level1name = N'_migrations'
    
    EXEC sp_addextendedproperty 
        @name = N'MS_Description', 
        @value = N'Уникальный ID миграции в формате SchemeType_PropertyName_vN', 
        @level0type = N'SCHEMA', @level0name = N'dbo',
        @level1type = N'TABLE',  @level1name = N'_migrations',
        @level2type = N'COLUMN', @level2name = N'_migration_id'
    
    EXEC sp_addextendedproperty 
        @name = N'MS_Description', 
        @value = N'MD5 хеш Expression для детекции изменений', 
        @level0type = N'SCHEMA', @level0name = N'dbo',
        @level1type = N'TABLE',  @level1name = N'_migrations',
        @level2type = N'COLUMN', @level2name = N'_expression_hash'
    
    EXEC sp_addextendedproperty 
        @name = N'MS_Description', 
        @value = N'SQL запрос для аудита и отладки', 
        @level0type = N'SCHEMA', @level0name = N'dbo',
        @level1type = N'TABLE',  @level1name = N'_migrations',
        @level2type = N'COLUMN', @level2name = N'_sql_executed'
    
    PRINT 'Таблица _migrations успешно создана'
END
ELSE
BEGIN
    PRINT 'Таблица _migrations уже существует'
END
GO
```

#### Ключевые отличия от PostgreSQL

| PostgreSQL | MS SQL Server | Комментарий |
|------------|---------------|-------------|
| `BIGSERIAL` | `BIGINT` | ID генерируется через sequence или IDENTITY |
| `TEXT` | `NVARCHAR(MAX)` или `NVARCHAR(n)` | Ограничение 500 символов для индексируемых |
| `TIMESTAMPTZ` | `DATETIME2` | Или `DATETIMEOFFSET` для часовых поясов |
| `BOOLEAN` | `BIT` | Значения 0/1 вместо true/false |
| `CREATE TABLE IF NOT EXISTS` | `IF NOT EXISTS (SELECT...)` | Проверка через sys.objects |
| `COMMENT ON TABLE` | `sp_addextendedproperty` | Расширенные свойства |
| `CREATE INDEX IF NOT EXISTS` | Проверка через sys.indexes | Или `CREATE INDEX` без проверки |

---

### 1.3. SQL скрипт: Кеш метаданных схем (опционально)

**Файл:** `redb.MSSql.Pro/sql/002_scheme_metadata_cache.sql`

#### Описание

Таблица `_scheme_metadata_cache` - денормализованный кеш для быстрого доступа к метаданным полей схем.

**Преимущества:**
- Быстрый поиск полей по имени без JOIN
- Предварительно вычисленные типы для PVT
- Поддержка вложенных Dictionary полей

#### SQL скрипт

```sql
-- =====================================================
-- REDB Pro: Кеш метаданных схем для быстрого PVT
-- =====================================================

IF NOT EXISTS (SELECT * FROM sys.objects 
               WHERE object_id = OBJECT_ID(N'[dbo].[_scheme_metadata_cache]') 
               AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[_scheme_metadata_cache](
        [_id] BIGINT PRIMARY KEY,
        
        -- Схема
        [_scheme_id] BIGINT NOT NULL,
        CONSTRAINT [FK__scheme_metadata_cache__schemes] 
            FOREIGN KEY ([_scheme_id]) 
            REFERENCES [dbo].[_schemes]([_id]) 
            ON DELETE CASCADE,
        
        -- Структура (поле)
        [_structure_id] BIGINT NOT NULL,
        CONSTRAINT [FK__scheme_metadata_cache__structures] 
            FOREIGN KEY ([_structure_id]) 
            REFERENCES [dbo].[_structures]([_id]) 
            ON DELETE CASCADE,
        
        -- Родительская структура (для вложенных полей)
        [_parent_structure_id] BIGINT NULL,
        
        -- Имя поля (полный путь для вложенных: "Address.City")
        [_name] NVARCHAR(500) NOT NULL,
        
        -- Тип данных из _types._db_type
        [db_type] NVARCHAR(250) NOT NULL,
        
        -- Тип коллекции: 0=None, 1=Array, 2=Dictionary
        [_collection_type] INT NOT NULL DEFAULT 0,
        
        -- Ключ словаря (для Dictionary элементов)
        [_dict_key] NVARCHAR(250) NULL,
        
        -- Порядок сортировки
        [_order] INT NULL,
        
        -- Признак вычисляемого поля
        [_is_computed] BIT NOT NULL DEFAULT 0,
        
        -- Дата последнего обновления кеша
        [_cache_updated] DATETIME2 NOT NULL DEFAULT GETDATE()
    ) ON [PRIMARY]
    
    -- Индексы для быстрого поиска
    CREATE INDEX [IX__scheme_metadata_cache_scheme] 
        ON [dbo].[_scheme_metadata_cache]([_scheme_id])
    
    CREATE INDEX [IX__scheme_metadata_cache_lookup] 
        ON [dbo].[_scheme_metadata_cache]([_scheme_id], [_name])
    
    CREATE INDEX [IX__scheme_metadata_cache_parent] 
        ON [dbo].[_scheme_metadata_cache]([_parent_structure_id])
    
    -- Комментарии
    EXEC sp_addextendedproperty 
        @name = N'MS_Description', 
        @value = N'Денормализованный кеш метаданных схем для PVT (Pro feature)', 
        @level0type = N'SCHEMA', @level0name = N'dbo',
        @level1type = N'TABLE',  @level1name = N'_scheme_metadata_cache'
    
    PRINT 'Таблица _scheme_metadata_cache успешно создана'
END
ELSE
BEGIN
    PRINT 'Таблица _scheme_metadata_cache уже существует'
END
GO
```

---

### 1.4. SQL скрипт: Хранимые процедуры для PVT (опционально)

**Файл:** `redb.MSSql.Pro/sql/003_pvt_procedures.sql`

#### Описание

Хранимые процедуры для оптимизации PVT запросов и материализации объектов.

#### Пример процедуры

```sql
-- =====================================================
-- Процедура для PVT материализации одного объекта
-- =====================================================

IF EXISTS (SELECT * FROM sys.objects 
           WHERE object_id = OBJECT_ID(N'[dbo].[sp_GetObjectWithPvt]') 
           AND type in (N'P', N'PC'))
    DROP PROCEDURE [dbo].[sp_GetObjectWithPvt]
GO

CREATE PROCEDURE [dbo].[sp_GetObjectWithPvt]
    @ObjectId BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    
    -- 1. Базовые поля объекта
    SELECT 
        _id AS Id,
        _name AS Name,
        _id_scheme AS IdScheme,
        _id_parent AS IdParent,
        _id_owner AS IdOwner,
        _id_who_change AS IdWhoChange,
        _date_create AS DateCreate,
        _date_modify AS DateModify,
        _date_begin AS DateBegin,
        _date_complete AS DateComplete,
        _key AS [Key],
        _value_long AS ValueLong,      -- ИСПРАВЛЕНО: было _code_int
        _value_string AS ValueString,  -- ИСПРАВЛЕНО: было _code_string
        _value_guid AS ValueGuid,      -- ИСПРАВЛЕНО: было _code_guid
        _value_bool AS ValueBool,      -- ДОБАВЛЕНО
        _value_double AS ValueDouble,  -- ДОБАВЛЕНО
        _value_numeric AS ValueNumeric,-- ДОБАВЛЕНО
        _value_datetime AS ValueDatetime, -- ДОБАВЛЕНО
        _value_bytes AS ValueBytes,    -- ДОБАВЛЕНО
        _note AS Note,
        _hash AS [Hash]
    FROM [dbo].[_objects]
    WHERE _id = @ObjectId;
    
    -- 2. Все значения для PVT
    SELECT 
        v._id,
        v._id_object,
        v._id_structure,
        s._name AS PropertyName,
        t._db_type AS DbType,
        v._String,
        v._Long,
        v._Double,
        v._Guid,
        v._DateTime,
        v._Boolean,
        v._ByteArray,
        v._Text
    FROM [dbo].[_values] v
    INNER JOIN [dbo].[_structures] s ON v._id_structure = s._id
    INNER JOIN [dbo].[_types] t ON s._id_type = t._id
    WHERE v._id_object = @ObjectId
    ORDER BY s._order;
END
GO
```

---

### 1.5. Сравнение типов данных PostgreSQL vs MSSQL

| Назначение | PostgreSQL | MS SQL Server | Примечание |
|------------|------------|---------------|------------|
| **Числа** |
| Автоинкремент | `BIGSERIAL` | `BIGINT IDENTITY(1,1)` | Или SEQUENCE |
| Целое 64-bit | `BIGINT` | `BIGINT` | Одинаково |
| Целое 32-bit | `INTEGER` | `INT` | Одинаково |
| Вещественное | `DOUBLE PRECISION` | `FLOAT` | Одинаково |
| Decimal | `NUMERIC(p,s)` | `DECIMAL(p,s)` | Одинаково |
| **Строки** |
| Переменная длина | `TEXT` | `NVARCHAR(MAX)` | MAX до 2GB |
| Фиксированная | `VARCHAR(n)` | `NVARCHAR(n)` | N для Unicode |
| **Дата/Время** |
| Дата и время | `TIMESTAMP` | `DATETIME2` | Точность до 100ns |
| С часовым поясом | `TIMESTAMPTZ` | `DATETIMEOFFSET` | Включает offset |
| Только дата | `DATE` | `DATE` | Одинаково |
| Только время | `TIME` | `TIME` | Одинаково |
| **Логические** |
| Булево | `BOOLEAN` | `BIT` | 0/1 вместо true/false |
| **Бинарные** |
| Бинарные данные | `BYTEA` | `VARBINARY(MAX)` | MAX до 2GB |
| **Специальные** |
| UUID | `UUID` | `UNIQUEIDENTIFIER` | GUID |
| JSON | `JSON`, `JSONB` | `NVARCHAR(MAX)` | С функциями JSON (2016+) |
| Массивы | `type[]` | Нет нативной поддержки | Использовать таблицы или JSON |

---

### 1.6. Сравнение SQL синтаксиса

#### Параметры запросов

```sql
-- PostgreSQL
SELECT * FROM _objects WHERE _id = $1 AND _scheme_id = $2

-- MS SQL Server
SELECT * FROM _objects WHERE _id = @p0 AND _scheme_id = @p1
```

#### LIMIT / TOP

```sql
-- PostgreSQL
SELECT * FROM _objects LIMIT 10

-- MS SQL Server (до 2012)
SELECT TOP 10 * FROM _objects

-- MS SQL Server (2012+)
SELECT * FROM _objects 
ORDER BY _id
OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY
```

#### OFFSET с LIMIT

```sql
-- PostgreSQL
SELECT * FROM _objects ORDER BY _id LIMIT 10 OFFSET 20

-- MS SQL Server (2012+)
SELECT * FROM _objects 
ORDER BY _id
OFFSET 20 ROWS FETCH NEXT 10 ROWS ONLY
```

#### Проверка существования

```sql
-- PostgreSQL
CREATE TABLE IF NOT EXISTS _migrations (...)

-- MS SQL Server
IF NOT EXISTS (SELECT * FROM sys.objects 
               WHERE object_id = OBJECT_ID(N'[dbo].[_migrations]') 
               AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[_migrations] (...)
END
```

#### Массивы в условиях

```sql
-- PostgreSQL
SELECT * FROM _objects WHERE _id = ANY($1::bigint[])

-- MS SQL Server (простой вариант)
SELECT * FROM _objects WHERE _id IN (SELECT value FROM STRING_SPLIT(@p0, ','))

-- MS SQL Server (производительный вариант - TVP)
CREATE TYPE [dbo].[BigIntListType] AS TABLE ([Value] BIGINT)
GO

-- В запросе
SELECT * FROM _objects WHERE _id IN (SELECT Value FROM @idList)
```

#### Конкатенация строк

```sql
-- PostgreSQL
SELECT _name || ' - ' || _note FROM _objects

-- MS SQL Server (старый способ)
SELECT _name + ' - ' + _note FROM _objects

-- MS SQL Server (новый способ)
SELECT CONCAT(_name, ' - ', _note) FROM _objects
```

#### Текущая дата/время

```sql
-- PostgreSQL
NOW()
CURRENT_TIMESTAMP

-- MS SQL Server
GETDATE()
SYSDATETIME()  -- Более точное
```

#### RETURNING (возврат вставленных значений)

```sql
-- PostgreSQL
INSERT INTO _objects (...) VALUES (...) RETURNING _id

-- MS SQL Server
INSERT INTO _objects (...) 
OUTPUT INSERTED._id
VALUES (...)
```

#### UPSERT (INSERT OR UPDATE)

```sql
-- PostgreSQL
INSERT INTO _objects (...) VALUES (...)
ON CONFLICT (_id) DO UPDATE SET ...

-- MS SQL Server
MERGE INTO _objects AS target
USING (VALUES (...)) AS source (...)
ON target._id = source._id
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT (...) VALUES (...);
```

#### Комментарии

```sql
-- PostgreSQL
COMMENT ON TABLE _migrations IS 'История миграций'

-- MS SQL Server
EXEC sp_addextendedproperty 
    @name = N'MS_Description', 
    @value = N'История миграций', 
    @level0type = N'SCHEMA', @level0name = N'dbo',
    @level1type = N'TABLE',  @level1name = N'_migrations'
```

---

### 1.7. Оптимизация работы с массивами в MSSQL

#### Проблема
PostgreSQL нативно поддерживает массивы: `ANY($1::bigint[])`. В MSSQL нет прямой поддержки.

#### Решение 1: STRING_SPLIT (простое, для малых объемов)

```sql
-- C# код
var ids = new[] { 1L, 2L, 3L, 4L };
var idsString = string.Join(",", ids);
var sql = "SELECT * FROM _objects WHERE _id IN (SELECT value FROM STRING_SPLIT(@p0, ','))";
await context.QueryAsync<object>(sql, idsString);
```

**Минусы:**
- Преобразование в строку и обратно
- Ограничение на длину строки
- Медленнее для больших списков

#### Решение 2: Table-Valued Parameters (производительное)

```sql
-- 1. Создать пользовательский тип
CREATE TYPE [dbo].[BigIntListType] AS TABLE ([Value] BIGINT)
GO

-- 2. Использовать в запросе
CREATE PROCEDURE sp_GetObjectsByIds
    @IdList [dbo].[BigIntListType] READONLY
AS
BEGIN
    SELECT * FROM _objects WHERE _id IN (SELECT Value FROM @IdList)
END
GO
```

```csharp
// C# код с Dapper
var ids = new[] { 1L, 2L, 3L, 4L };
var table = new DataTable();
table.Columns.Add("Value", typeof(long));
foreach (var id in ids)
    table.Rows.Add(id);

var param = new SqlParameter("@IdList", SqlDbType.Structured)
{
    TypeName = "[dbo].[BigIntListType]",
    Value = table
};

var results = await connection.QueryAsync<RedbObject>(
    "sp_GetObjectsByIds", 
    new { IdList = table.AsTableValuedParameter("[dbo].[BigIntListType]") },
    commandType: CommandType.StoredProcedure);
```

#### Решение 3: Временные таблицы (универсальное)

```csharp
// C# код
await using var transaction = await context.BeginTransactionAsync();

// Создать временную таблицу
await context.ExecuteAsync(@"
    CREATE TABLE #TempIds (_id BIGINT PRIMARY KEY)
");

// Вставить значения батчами
var ids = new[] { 1L, 2L, 3L, ... };
foreach (var batch in ids.Chunk(1000))
{
    var sql = $"INSERT INTO #TempIds VALUES {string.Join(",", batch.Select(id => $"({id})"))}";
    await context.ExecuteAsync(sql);
}

// Использовать в запросе
var results = await context.QueryAsync<RedbObject>(@"
    SELECT * FROM _objects WHERE _id IN (SELECT _id FROM #TempIds)
");

await transaction.CommitAsync();
// Временная таблица автоматически удалится
```

#### Рекомендация для redb.MSSql.Pro

Использовать **гибридный подход**:
- **< 100 элементов**: `STRING_SPLIT`
- **100-10000 элементов**: Table-Valued Parameters
- **> 10000 элементов**: Временные таблицы

---

### 1.8. JSON поддержка в MSSQL

#### Возможности (SQL Server 2016+)

```sql
-- Проверка валидности JSON
SELECT ISJSON(N'{"name":"John","age":30}')  -- Возвращает 1

-- Извлечение значения
SELECT JSON_VALUE(N'{"name":"John","age":30}', '$.name')  -- 'John'

-- Извлечение объекта/массива
SELECT JSON_QUERY(N'{"address":{"city":"Moscow"}}', '$.address')

-- Изменение JSON
DECLARE @json NVARCHAR(MAX) = N'{"name":"John","age":30}'
SET @json = JSON_MODIFY(@json, '$.age', 31)

-- Конвертация таблицы в JSON
SELECT _id, _name 
FROM _objects 
FOR JSON PATH

-- Разбор JSON в таблицу
SELECT * FROM OPENJSON(N'[{"id":1,"name":"A"},{"id":2,"name":"B"}]')
WITH (id INT '$.id', name NVARCHAR(50) '$.name')
```

#### Применение в redb

```sql
-- Хранение Props как JSON (альтернатива EAV)
ALTER TABLE _objects ADD _props_json NVARCHAR(MAX)

-- Поиск по JSON
SELECT * FROM _objects 
WHERE JSON_VALUE(_props_json, '$.Price') > 100

-- Индекс на JSON поле (computed column)
ALTER TABLE _objects ADD _price AS CAST(JSON_VALUE(_props_json, '$.Price') AS DECIMAL(18,2))
CREATE INDEX IX_objects_price ON _objects(_price)
```

---

## 📋 ИТОГОВЫЙ СПИСОК SQL ФАЙЛОВ

### Обязательные

1. **001_migrations_table.sql** - Таблица истории миграций ✅
2. **002_scheme_metadata_cache.sql** - Кеш метаданных (опционально, но рекомендуется)
3. **003_tvp_types.sql** - Table-Valued Parameter типы для массивов

### Дополнительные (опционально)

4. **004_pvt_procedures.sql** - Хранимые процедуры для PVT
5. **005_json_support.sql** - JSON индексы и функции
6. **006_performance_indexes.sql** - Дополнительные индексы для Pro функций
7. **007_maintenance.sql** - Процедуры обслуживания (rebuild indexes, update stats)

---

## 🚀 СЛЕДУЮЩИЕ ШАГИ

1. ✅ **SQL скрипты** - текущий этап
2. ⏭️ **ProMSSqlDialect** - реализация ISqlDialectPro
3. ⏭️ **Провайдеры** - ProMSSqlObjectStorageProvider, ProMSSqlTreeProvider
4. ⏭️ **Query билдеры** - ProSqlBuilder, ProQueryProvider
5. ⏭️ **Сервисы** - ProRedbService, Extensions

---

## 📚 ПОЛЕЗНЫЕ ССЫЛКИ

- [SQL Server 2019 Documentation](https://docs.microsoft.com/en-us/sql/sql-server/)
- [JSON in SQL Server](https://docs.microsoft.com/en-us/sql/relational-databases/json/)
- [Table-Valued Parameters](https://docs.microsoft.com/en-us/sql/relational-databases/tables/use-table-valued-parameters-database-engine)
- [Window Functions](https://docs.microsoft.com/en-us/sql/t-sql/queries/select-over-clause-transact-sql)


