# Детальные SQL скрипты для redb.MSSql.Pro

## 📋 СОДЕРЖАНИЕ

1. [Таблица миграций](#1-таблица-миграций)
2. [Кеш метаданных схем](#2-кеш-метаданных-схем)
3. [Table-Valued Parameter типы](#3-table-valued-parameter-типы)
4. [Хранимые процедуры PVT](#4-хранимые-процедуры-pvt)
5. [JSON поддержка](#5-json-поддержка)
6. [Индексы производительности](#6-индексы-производительности)
7. [Процедуры обслуживания](#7-процедуры-обслуживания)

---

## 1. ТАБЛИЦА МИГРАЦИЙ

**Файл:** `redb.MSSql.Pro/sql/001_migrations_table.sql`

### Назначение

Хранение истории всех выполненных миграций данных для отслеживания изменений и предотвращения повторного применения.

### Полный SQL скрипт

```sql
-- =====================================================
-- REDB Pro: Таблица истории миграций
-- =====================================================
USE [redb]
GO

-- Проверка существования таблицы
IF NOT EXISTS (SELECT * FROM sys.objects 
               WHERE object_id = OBJECT_ID(N'[dbo].[_migrations]') 
               AND type in (N'U'))
BEGIN
    PRINT 'Создание таблицы _migrations...'
    
    CREATE TABLE [dbo].[_migrations](
        -- Первичный ключ
        [_id] BIGINT NOT NULL,
        CONSTRAINT [PK__migrations] PRIMARY KEY CLUSTERED ([_id] ASC)
            WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, 
                  IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, 
                  ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF),
        
        -- Уникальный ID миграции (например: "OrderProps_TotalPrice_v1")
        [_migration_id] NVARCHAR(500) NOT NULL,
        
        -- Ссылка на схему
        [_scheme_id] BIGINT NOT NULL,
        
        -- Ссылка на структуру (NULL = миграция для всей схемы)
        [_structure_id] BIGINT NULL,
        
        -- Имя свойства (для читаемости логов)
        [_property_name] NVARCHAR(500) NULL,
        
        -- MD5 хеш от Expression (для детекции изменений в коде миграции)
        [_expression_hash] NVARCHAR(32) NULL,
        
        -- Тип миграции: 
        -- - ComputedFrom: вычисление на основе других полей
        -- - TypeChange: изменение типа данных
        -- - DefaultValue: установка значений по умолчанию
        -- - Transform: произвольная трансформация
        -- - Split: разделение поля
        -- - Merge: объединение полей
        [_migration_type] NVARCHAR(100) NOT NULL,
        
        -- Когда применена миграция
        [_applied_at] DATETIME2(7) NOT NULL 
            CONSTRAINT [DF__migrations__applied_at] DEFAULT (SYSDATETIME()),
        
        -- Кто применил (user ID или 'system')
        [_applied_by] NVARCHAR(250) NULL,
        
        -- SQL который был выполнен (для аудита и отладки)
        [_sql_executed] NVARCHAR(MAX) NULL,
        
        -- Сколько записей было затронуто
        [_affected_rows] INT NULL,
        
        -- Время выполнения в миллисекундах
        [_duration_ms] INT NULL,
        
        -- Был ли это пробный запуск (dry-run)
        [_dry_run] BIT NOT NULL 
            CONSTRAINT [DF__migrations__dry_run] DEFAULT (0),
        
        -- Статус миграции: 
        -- 0=Success, 1=Failed, 2=Partial, 3=Rollback
        [_status] TINYINT NOT NULL DEFAULT (0),
        
        -- Сообщение об ошибке (если была)
        [_error_message] NVARCHAR(MAX) NULL,
        
        -- Уникальность: одна миграция один раз на схему
        CONSTRAINT [UQ__migrations_scheme] 
            UNIQUE NONCLUSTERED ([_scheme_id], [_migration_id])
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
    
    PRINT 'Таблица _migrations создана успешно'
END
ELSE
BEGIN
    PRINT 'Таблица _migrations уже существует'
END
GO

-- =====================================================
-- Внешние ключи
-- =====================================================
IF NOT EXISTS (SELECT * FROM sys.foreign_keys 
               WHERE object_id = OBJECT_ID(N'[dbo].[FK__migrations__schemes]'))
BEGIN
    ALTER TABLE [dbo].[_migrations] 
    ADD CONSTRAINT [FK__migrations__schemes] 
        FOREIGN KEY ([_scheme_id]) 
        REFERENCES [dbo].[_schemes]([_id]) 
        ON DELETE CASCADE
    
    PRINT 'FK _migrations -> _schemes создан'
END
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys 
               WHERE object_id = OBJECT_ID(N'[dbo].[FK__migrations__structures]'))
BEGIN
    ALTER TABLE [dbo].[_migrations] 
    ADD CONSTRAINT [FK__migrations__structures] 
        FOREIGN KEY ([_structure_id]) 
        REFERENCES [dbo].[_structures]([_id]) 
        ON DELETE SET NULL
    
    PRINT 'FK _migrations -> _structures создан'
END
GO

-- =====================================================
-- Индексы для оптимизации запросов
-- =====================================================

-- Индекс по scheme_id (частые запросы миграций для схемы)
IF NOT EXISTS (SELECT * FROM sys.indexes 
               WHERE object_id = OBJECT_ID(N'[dbo].[_migrations]') 
               AND name = N'IX__migrations_scheme')
BEGIN
    CREATE NONCLUSTERED INDEX [IX__migrations_scheme] 
    ON [dbo].[_migrations]([_scheme_id])
    INCLUDE ([_migration_id], [_applied_at], [_dry_run])
    
    PRINT 'Индекс IX__migrations_scheme создан'
END
GO

-- Индекс по дате применения (для аудита и просмотра истории)
IF NOT EXISTS (SELECT * FROM sys.indexes 
               WHERE object_id = OBJECT_ID(N'[dbo].[_migrations]') 
               AND name = N'IX__migrations_applied')
BEGIN
    CREATE NONCLUSTERED INDEX [IX__migrations_applied] 
    ON [dbo].[_migrations]([_applied_at] DESC)
    INCLUDE ([_scheme_id], [_migration_id], [_migration_type], [_affected_rows])
    
    PRINT 'Индекс IX__migrations_applied создан'
END
GO

-- Индекс по типу миграции (для аналитики)
IF NOT EXISTS (SELECT * FROM sys.indexes 
               WHERE object_id = OBJECT_ID(N'[dbo].[_migrations]') 
               AND name = N'IX__migrations_type')
BEGIN
    CREATE NONCLUSTERED INDEX [IX__migrations_type] 
    ON [dbo].[_migrations]([_migration_type])
    INCLUDE ([_scheme_id], [_applied_at])
    
    PRINT 'Индекс IX__migrations_type создан'
END
GO

-- =====================================================
-- Расширенные свойства (комментарии)
-- =====================================================

-- Описание таблицы
IF NOT EXISTS (SELECT * FROM sys.extended_properties 
               WHERE major_id = OBJECT_ID(N'[dbo].[_migrations]') 
               AND minor_id = 0 AND name = N'MS_Description')
BEGIN
    EXEC sp_addextendedproperty 
        @name = N'MS_Description', 
        @value = N'История применённых миграций данных (Pro feature). Отслеживает все изменения структуры и данных с возможностью аудита и отката.', 
        @level0type = N'SCHEMA', @level0name = N'dbo',
        @level1type = N'TABLE',  @level1name = N'_migrations'
END
GO

-- Описания полей
EXEC sp_addextendedproperty 
    @name = N'MS_Description', 
    @value = N'Уникальный ID миграции в формате: SchemeTypeName_PropertyName_vN (например: Order_TotalPrice_v1)', 
    @level0type = N'SCHEMA', @level0name = N'dbo',
    @level1type = N'TABLE',  @level1name = N'_migrations',
    @level2type = N'COLUMN', @level2name = N'_migration_id'
GO

EXEC sp_addextendedproperty 
    @name = N'MS_Description', 
    @value = N'MD5 хеш от Expression для автоматической детекции изменений в коде миграции', 
    @level0type = N'SCHEMA', @level0name = N'dbo',
    @level1type = N'TABLE',  @level1name = N'_migrations',
    @level2type = N'COLUMN', @level2name = N'_expression_hash'
GO

EXEC sp_addextendedproperty 
    @name = N'MS_Description', 
    @value = N'Выполненный SQL запрос (сохраняется для аудита, отладки и возможности анализа производительности)', 
    @level0type = N'SCHEMA', @level0name = N'dbo',
    @level1type = N'TABLE',  @level1name = N'_migrations',
    @level2type = N'COLUMN', @level2name = N'_sql_executed'
GO

PRINT '========================================='
PRINT 'Скрипт 001_migrations_table.sql выполнен успешно!'
PRINT '========================================='
GO
```

### Примеры использования

```sql
-- Проверка, применена ли миграция
SELECT TOP 1 _id 
FROM _migrations 
WHERE _scheme_id = @schemeId 
  AND _migration_id = @migrationId 
  AND _dry_run = 0

-- Получение хеша предыдущей версии миграции
SELECT TOP 1 _expression_hash 
FROM _migrations 
WHERE _scheme_id = @schemeId 
  AND _migration_id = @migrationId 
  AND _dry_run = 0

-- Запись новой миграции
INSERT INTO _migrations 
    (_id, _migration_id, _scheme_id, _property_name, _expression_hash, 
     _migration_type, _applied_at, _applied_by, _sql_executed, 
     _affected_rows, _duration_ms, _dry_run, _status)
VALUES 
    (@id, @migrationId, @schemeId, @propertyName, @hash, 
     @type, SYSDATETIME(), @userId, @sql, 
     @affectedRows, @durationMs, 0, 0)

-- История миграций для схемы
SELECT 
    _migration_id,
    _property_name,
    _migration_type,
    _applied_at,
    _applied_by,
    _affected_rows,
    _duration_ms,
    CASE _status 
        WHEN 0 THEN 'Success'
        WHEN 1 THEN 'Failed'
        WHEN 2 THEN 'Partial'
        WHEN 3 THEN 'Rollback'
    END AS Status
FROM _migrations
WHERE _scheme_id = @schemeId 
  AND _dry_run = 0
ORDER BY _applied_at DESC

-- Статистика миграций
SELECT 
    _migration_type,
    COUNT(*) AS TotalCount,
    SUM(_affected_rows) AS TotalRowsAffected,
    AVG(_duration_ms) AS AvgDurationMs,
    MAX(_duration_ms) AS MaxDurationMs
FROM _migrations
WHERE _dry_run = 0 AND _status = 0
GROUP BY _migration_type
ORDER BY TotalCount DESC
```

---

## 2. КЕШ МЕТАДАННЫХ СХЕМ

**Файл:** `redb.MSSql.Pro/sql/002_scheme_metadata_cache.sql`

### Назначение

Денормализованный кеш метаданных полей схем для быстрого PVT и резолва полей без множественных JOIN.

### Преимущества

1. **Производительность**: O(1) поиск поля по имени вместо JOIN через 3-4 таблицы
2. **PVT оптимизация**: Предвычисленные типы для быстрой материализации
3. **Вложенные поля**: Поддержка Dictionary и Array с быстрым поиском
4. **Кеширование**: Обновляется только при изменении схемы

### Полный SQL скрипт

```sql
-- =====================================================
-- REDB Pro: Кеш метаданных схем
-- =====================================================
USE [redb]
GO

IF NOT EXISTS (SELECT * FROM sys.objects 
               WHERE object_id = OBJECT_ID(N'[dbo].[_scheme_metadata_cache]') 
               AND type in (N'U'))
BEGIN
    PRINT 'Создание таблицы _scheme_metadata_cache...'
    
    CREATE TABLE [dbo].[_scheme_metadata_cache](
        -- Первичный ключ
        [_id] BIGINT NOT NULL,
        CONSTRAINT [PK__scheme_metadata_cache] PRIMARY KEY CLUSTERED ([_id] ASC),
        
        -- Схема
        [_scheme_id] BIGINT NOT NULL,
        
        -- Структура (поле)
        [_structure_id] BIGINT NOT NULL,
        
        -- Родительская структура (для вложенных полей: Address.City)
        [_parent_structure_id] BIGINT NULL,
        
        -- Полное имя поля
        -- Для обычных: "Price"
        -- Для вложенных: "Address.City"
        -- Для Dictionary: "Phones[mobile]"
        [_name] NVARCHAR(500) NOT NULL,
        
        -- Короткое имя (без пути)
        [_short_name] NVARCHAR(250) NOT NULL,
        
        -- Тип данных из _types._db_type (String, Long, Double, etc.)
        [db_type] NVARCHAR(250) NOT NULL,
        
        -- Тип коллекции:
        -- 0 = None (простое поле)
        -- 1 = Array (массив)
        -- 2 = Dictionary (словарь)
        [_collection_type] INT NOT NULL DEFAULT (0),
        
        -- Ключ словаря (для Dictionary элементов)
        -- Пример: "mobile" в "Phones[mobile]"
        [_dict_key] NVARCHAR(250) NULL,
        
        -- Порядок сортировки полей
        [_order] INT NULL,
        
        -- Признак вычисляемого поля (ComputedFrom)
        [_is_computed] BIT NOT NULL DEFAULT (0),
        
        -- Только для чтения
        [_is_readonly] BIT NOT NULL DEFAULT (0),
        
        -- Обязательное поле
        [_is_required] BIT NOT NULL DEFAULT (0),
        
        -- Индекс массива (для элементов Array)
        [_array_index] INT NULL,
        
        -- Путь до поля (для быстрой навигации)
        -- Пример: "_structures[1234] > _structures[5678]"
        [_structure_path] NVARCHAR(1000) NULL,
        
        -- Глубина вложенности (0 = root level)
        [_depth] INT NOT NULL DEFAULT (0),
        
        -- Дата создания кеша
        [_cache_created] DATETIME2(7) NOT NULL DEFAULT (SYSDATETIME()),
        
        -- Дата последнего обновления
        [_cache_updated] DATETIME2(7) NOT NULL DEFAULT (SYSDATETIME()),
        
        -- Хеш структуры для валидации кеша
        [_structure_hash] NVARCHAR(32) NULL
        
    ) ON [PRIMARY]
    
    PRINT 'Таблица _scheme_metadata_cache создана успешно'
END
ELSE
BEGIN
    PRINT 'Таблица _scheme_metadata_cache уже существует'
END
GO

-- =====================================================
-- Внешние ключи
-- =====================================================

IF NOT EXISTS (SELECT * FROM sys.foreign_keys 
               WHERE object_id = OBJECT_ID(N'[dbo].[FK__scheme_metadata_cache__schemes]'))
BEGIN
    ALTER TABLE [dbo].[_scheme_metadata_cache] 
    ADD CONSTRAINT [FK__scheme_metadata_cache__schemes] 
        FOREIGN KEY ([_scheme_id]) 
        REFERENCES [dbo].[_schemes]([_id]) 
        ON DELETE CASCADE
    
    PRINT 'FK _scheme_metadata_cache -> _schemes создан'
END
GO

IF NOT EXISTS (SELECT * FROM sys.foreign_keys 
               WHERE object_id = OBJECT_ID(N'[dbo].[FK__scheme_metadata_cache__structures]'))
BEGIN
    ALTER TABLE [dbo].[_scheme_metadata_cache] 
    ADD CONSTRAINT [FK__scheme_metadata_cache__structures] 
        FOREIGN KEY ([_structure_id]) 
        REFERENCES [dbo].[_structures]([_id]) 
        ON DELETE CASCADE
    
    PRINT 'FK _scheme_metadata_cache -> _structures создан'
END
GO

-- =====================================================
-- Индексы для быстрого поиска
-- =====================================================

-- Основной индекс для поиска полей по имени
IF NOT EXISTS (SELECT * FROM sys.indexes 
               WHERE object_id = OBJECT_ID(N'[dbo].[_scheme_metadata_cache]') 
               AND name = N'IX__scheme_metadata_cache_lookup')
BEGIN
    CREATE NONCLUSTERED INDEX [IX__scheme_metadata_cache_lookup] 
    ON [dbo].[_scheme_metadata_cache]([_scheme_id], [_name])
    INCLUDE ([_structure_id], [db_type], [_collection_type], [_parent_structure_id])
    
    PRINT 'Индекс IX__scheme_metadata_cache_lookup создан'
END
GO

-- Индекс для поиска по схеме
IF NOT EXISTS (SELECT * FROM sys.indexes 
               WHERE object_id = OBJECT_ID(N'[dbo].[_scheme_metadata_cache]') 
               AND name = N'IX__scheme_metadata_cache_scheme')
BEGIN
    CREATE NONCLUSTERED INDEX [IX__scheme_metadata_cache_scheme] 
    ON [dbo].[_scheme_metadata_cache]([_scheme_id])
    INCLUDE ([_structure_id], [_name], [db_type], [_order])
    
    PRINT 'Индекс IX__scheme_metadata_cache_scheme создан'
END
GO

-- Индекс для поиска дочерних полей
IF NOT EXISTS (SELECT * FROM sys.indexes 
               WHERE object_id = OBJECT_ID(N'[dbo].[_scheme_metadata_cache]') 
               AND name = N'IX__scheme_metadata_cache_parent')
BEGIN
    CREATE NONCLUSTERED INDEX [IX__scheme_metadata_cache_parent] 
    ON [dbo].[_scheme_metadata_cache]([_parent_structure_id])
    WHERE [_parent_structure_id] IS NOT NULL
    
    PRINT 'Индекс IX__scheme_metadata_cache_parent создан'
END
GO

-- Индекс для поиска по структуре
IF NOT EXISTS (SELECT * FROM sys.indexes 
               WHERE object_id = OBJECT_ID(N'[dbo].[_scheme_metadata_cache]') 
               AND name = N'IX__scheme_metadata_cache_structure')
BEGIN
    CREATE NONCLUSTERED INDEX [IX__scheme_metadata_cache_structure] 
    ON [dbo].[_scheme_metadata_cache]([_structure_id])
    
    PRINT 'Индекс IX__scheme_metadata_cache_structure создан'
END
GO

-- =====================================================
-- Комментарии
-- =====================================================

EXEC sp_addextendedproperty 
    @name = N'MS_Description', 
    @value = N'Денормализованный кеш метаданных схем для быстрого PVT и резолва полей (Pro feature)', 
    @level0type = N'SCHEMA', @level0name = N'dbo',
    @level1type = N'TABLE',  @level1name = N'_scheme_metadata_cache'
GO

EXEC sp_addextendedproperty 
    @name = N'MS_Description', 
    @value = N'Полное имя поля с путём: Price, Address.City, Phones[mobile]', 
    @level0type = N'SCHEMA', @level0name = N'dbo',
    @level1type = N'TABLE',  @level1name = N'_scheme_metadata_cache',
    @level2type = N'COLUMN', @level2name = N'_name'
GO

PRINT '========================================='
PRINT 'Скрипт 002_scheme_metadata_cache.sql выполнен успешно!'
PRINT '========================================='
GO
```

### Примеры использования

```sql
-- Поиск корневого поля по имени
SELECT _structure_id, db_type, _collection_type, _name 
FROM _scheme_metadata_cache 
WHERE _scheme_id = @schemeId 
  AND _name = @fieldName 
  AND _parent_structure_id IS NULL

-- Поиск вложенного поля
SELECT _structure_id, db_type, _collection_type, _name 
FROM _scheme_metadata_cache 
WHERE _scheme_id = @schemeId 
  AND _name = @fieldName 
  AND _parent_structure_id = @parentStructureId

-- Получение всех полей схемы с сортировкой
SELECT 
    _structure_id,
    _name,
    _short_name,
    db_type,
    CASE _collection_type 
        WHEN 0 THEN 'Simple'
        WHEN 1 THEN 'Array'
        WHEN 2 THEN 'Dictionary'
    END AS CollectionType,
    _is_computed,
    _is_required,
    _depth
FROM _scheme_metadata_cache
WHERE _scheme_id = @schemeId
ORDER BY _depth, _order

-- Поиск Dictionary элементов
SELECT * 
FROM _scheme_metadata_cache
WHERE _scheme_id = @schemeId
  AND _collection_type = 2
  AND _dict_key IS NOT NULL

-- Rebuild кеша для схемы (после изменения структуры)
DELETE FROM _scheme_metadata_cache WHERE _scheme_id = @schemeId

INSERT INTO _scheme_metadata_cache
    (_id, _scheme_id, _structure_id, _parent_structure_id, 
     _name, _short_name, db_type, _collection_type, _order, _depth)
SELECT 
    NEXT VALUE FOR dbo.global_identity,
    s._id_scheme,
    s._id,
    s._id_parent,
    -- Построение полного имени с учётом иерархии
    dbo.fn_GetStructureFullName(s._id),
    s._name,
    t._db_type,
    -- CollectionType: 0=None, 1=Array, 2=List, 3=Dictionary
    ISNULL(s._collection_type, 0),  -- ИСПРАВЛЕНО: было _is_array
    s._order,
    dbo.fn_GetStructureDepth(s._id)
FROM _structures s
INNER JOIN _types t ON s._id_type = t._id
WHERE s._id_scheme = @schemeId
```

---

## 3. TABLE-VALUED PARAMETER ТИПЫ

**Файл:** `redb.MSSql.Pro/sql/003_tvp_types.sql`

### Назначение

Создание пользовательских типов для эффективной передачи массивов параметров в SQL запросы.

### Полный SQL скрипт

```sql
-- =====================================================
-- REDB Pro: Table-Valued Parameter Types
-- =====================================================
USE [redb]
GO

-- =====================================================
-- 1. BigInt List (для массивов ID)
-- =====================================================
IF NOT EXISTS (SELECT * FROM sys.types 
               WHERE is_table_type = 1 
               AND name = 'BigIntListType')
BEGIN
    CREATE TYPE [dbo].[BigIntListType] AS TABLE
    (
        [Value] BIGINT NOT NULL,
        PRIMARY KEY ([Value])
    )
    
    PRINT 'Тип BigIntListType создан'
END
ELSE
BEGIN
    PRINT 'Тип BigIntListType уже существует'
END
GO

-- =====================================================
-- 2. String List (для массивов строк)
-- =====================================================
IF NOT EXISTS (SELECT * FROM sys.types 
               WHERE is_table_type = 1 
               AND name = 'StringListType')
BEGIN
    CREATE TYPE [dbo].[StringListType] AS TABLE
    (
        [Value] NVARCHAR(500) NOT NULL,
        PRIMARY KEY ([Value])
    )
    
    PRINT 'Тип StringListType создан'
END
ELSE
BEGIN
    PRINT 'Тип StringListType уже существует'
END
GO

-- =====================================================
-- 3. Guid List (для массивов GUID)
-- =====================================================
IF NOT EXISTS (SELECT * FROM sys.types 
               WHERE is_table_type = 1 
               AND name = 'GuidListType')
BEGIN
    CREATE TYPE [dbo].[GuidListType] AS TABLE
    (
        [Value] UNIQUEIDENTIFIER NOT NULL,
        PRIMARY KEY ([Value])
    )
    
    PRINT 'Тип GuidListType создан'
END
ELSE
BEGIN
    PRINT 'Тип GuidListType уже существует'
END
GO

-- =====================================================
-- 4. Structure Field Pair (для PVT запросов)
-- =====================================================
IF NOT EXISTS (SELECT * FROM sys.types 
               WHERE is_table_type = 1 
               AND name = 'StructureFieldPairType')
BEGIN
    CREATE TYPE [dbo].[StructureFieldPairType] AS TABLE
    (
        [StructureId] BIGINT NOT NULL,
        [FieldName] NVARCHAR(500) NOT NULL,
        [DbType] NVARCHAR(250) NOT NULL,
        PRIMARY KEY ([StructureId])
    )
    
    PRINT 'Тип StructureFieldPairType создан'
END
ELSE
BEGIN
    PRINT 'Тип StructureFieldPairType уже существует'
END
GO

-- =====================================================
-- 5. Object ID with Scheme (для batch операций)
-- =====================================================
IF NOT EXISTS (SELECT * FROM sys.types 
               WHERE is_table_type = 1 
               AND name = 'ObjectSchemeType')
BEGIN
    CREATE TYPE [dbo].[ObjectSchemeType] AS TABLE
    (
        [ObjectId] BIGINT NOT NULL,
        [SchemeId] BIGINT NOT NULL,
        PRIMARY KEY ([ObjectId])
    )
    
    PRINT 'Тип ObjectSchemeType создан'
END
ELSE
BEGIN
    PRINT 'Тип ObjectSchemeType уже существует'
END
GO

PRINT '========================================='
PRINT 'Скрипт 003_tvp_types.sql выполнен успешно!'
PRINT 'Создано 5 Table-Valued Parameter типов'
PRINT '========================================='
GO
```

### Примеры использования в C#

```csharp
using Microsoft.Data.SqlClient;
using System.Data;

// 1. BigIntListType - список ID объектов
public async Task<List<RedbObject>> GetObjectsByIds(long[] objectIds)
{
    var table = new DataTable();
    table.Columns.Add("Value", typeof(long));
    foreach (var id in objectIds)
        table.Rows.Add(id);

    var param = new SqlParameter("@ObjectIds", SqlDbType.Structured)
    {
        TypeName = "[dbo].[BigIntListType]",
        Value = table
    };

    var sql = @"
        SELECT * FROM _objects 
        WHERE _id IN (SELECT Value FROM @ObjectIds)";

    return await _context.QueryAsync<RedbObject>(sql, param);
}

// 2. StringListType - список имён
public async Task<List<Scheme>> GetSchemesByNames(string[] names)
{
    var table = new DataTable();
    table.Columns.Add("Value", typeof(string));
    foreach (var name in names)
        table.Rows.Add(name);

    var param = new SqlParameter("@Names", SqlDbType.Structured)
    {
        TypeName = "[dbo].[StringListType]",
        Value = table
    };

    var sql = @"
        SELECT * FROM _schemes 
        WHERE _name IN (SELECT Value FROM @Names)";

    return await _context.QueryAsync<Scheme>(sql, param);
}

// 3. StructureFieldPairType - для PVT запросов
public async Task<Dictionary<string, object>> GetObjectProps(
    long objectId, 
    List<(long structureId, string fieldName, string dbType)> fields)
{
    var table = new DataTable();
    table.Columns.Add("StructureId", typeof(long));
    table.Columns.Add("FieldName", typeof(string));
    table.Columns.Add("DbType", typeof(string));
    
    foreach (var (structureId, fieldName, dbType) in fields)
        table.Rows.Add(structureId, fieldName, dbType);

    var param = new SqlParameter("@Fields", SqlDbType.Structured)
    {
        TypeName = "[dbo].[StructureFieldPairType]",
        Value = table
    };

    var sql = @"
        SELECT 
            f.FieldName,
            CASE f.DbType
                WHEN 'String' THEN v._String
                WHEN 'Long' THEN CAST(v._Long AS NVARCHAR(50))
                WHEN 'Double' THEN CAST(v._Double AS NVARCHAR(50))
                -- ... другие типы
            END AS Value
        FROM @Fields f
        LEFT JOIN _values v ON v._id_structure = f.StructureId 
                            AND v._id_object = @ObjectId";

    var results = await _context.QueryAsync<(string FieldName, string Value)>(
        sql, new { ObjectId = objectId }, param);
    
    return results.ToDictionary(r => r.FieldName, r => (object)r.Value);
}
```

---

## 4. ХРАНИМЫЕ ПРОЦЕДУРЫ PVT

**Файл:** `redb.MSSql.Pro/sql/004_pvt_procedures.sql`

### Назначение

Оптимизированные хранимые процедуры для PVT материализации объектов.

### Полный SQL скрипт

```sql
-- =====================================================
-- REDB Pro: Хранимые процедуры для PVT материализации
-- =====================================================
USE [redb]
GO

-- =====================================================
-- 1. Получение объекта с базовыми полями
-- =====================================================
IF EXISTS (SELECT * FROM sys.objects 
           WHERE object_id = OBJECT_ID(N'[dbo].[sp_GetObjectBase]') 
           AND type in (N'P', N'PC'))
    DROP PROCEDURE [dbo].[sp_GetObjectBase]
GO

CREATE PROCEDURE [dbo].[sp_GetObjectBase]
    @ObjectId BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    
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
END
GO

-- =====================================================
-- 2. Получение значений для PVT (один объект)
-- =====================================================
IF EXISTS (SELECT * FROM sys.objects 
           WHERE object_id = OBJECT_ID(N'[dbo].[sp_GetObjectValues]') 
           AND type in (N'P', N'PC'))
    DROP PROCEDURE [dbo].[sp_GetObjectValues]
GO

CREATE PROCEDURE [dbo].[sp_GetObjectValues]
    @ObjectId BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT 
        v._id,
        v._id_object,
        v._id_structure,
        v._String,
        v._Long,
        v._Double,
        v._Guid,
        v._DateTime,
        v._Boolean,
        v._ByteArray,
        v._Text
    FROM [dbo].[_values] v
    WHERE v._id_object = @ObjectId
    ORDER BY v._id_structure;
END
GO

-- =====================================================
-- 3. Batch получение значений (множество объектов)
-- =====================================================
IF EXISTS (SELECT * FROM sys.objects 
           WHERE object_id = OBJECT_ID(N'[dbo].[sp_GetObjectValuesBatch]') 
           AND type in (N'P', N'PC'))
    DROP PROCEDURE [dbo].[sp_GetObjectValuesBatch]
GO

CREATE PROCEDURE [dbo].[sp_GetObjectValuesBatch]
    @ObjectIds [dbo].[BigIntListType] READONLY
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT 
        v._id,
        v._id_object,
        v._id_structure,
        v._String,
        v._Long,
        v._Double,
        v._Guid,
        v._DateTime,
        v._Boolean,
        v._ByteArray,
        v._Text
    FROM [dbo].[_values] v
    INNER JOIN @ObjectIds ids ON v._id_object = ids.Value
    ORDER BY v._id_object, v._id_structure;
END
GO

-- =====================================================
-- 4. Получение list items batch
-- =====================================================
IF EXISTS (SELECT * FROM sys.objects 
           WHERE object_id = OBJECT_ID(N'[dbo].[sp_GetListItemsBatch]') 
           AND type in (N'P', N'PC'))
    DROP PROCEDURE [dbo].[sp_GetListItemsBatch]
GO

CREATE PROCEDURE [dbo].[sp_GetListItemsBatch]
    @ItemIds [dbo].[BigIntListType] READONLY
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT 
        li._id,
        li._id_list,
        li._value,
        li._id_object
    FROM [dbo].[_list_items] li
    INNER JOIN @ItemIds ids ON li._id = ids.Value;
END
GO

-- =====================================================
-- 5. Получение nested objects batch
-- =====================================================
IF EXISTS (SELECT * FROM sys.objects 
           WHERE object_id = OBJECT_ID(N'[dbo].[sp_GetObjectsBatch]') 
           AND type in (N'P', N'PC'))
    DROP PROCEDURE [dbo].[sp_GetObjectsBatch]
GO

CREATE PROCEDURE [dbo].[sp_GetObjectsBatch]
    @ObjectIds [dbo].[BigIntListType] READONLY
AS
BEGIN
    SET NOCOUNT ON;
    
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
        _value_long AS ValueLong,      -- ИСПРАВЛЕНО
        _value_string AS ValueString,  -- ИСПРАВЛЕНО
        _value_guid AS ValueGuid,      -- ИСПРАВЛЕНО
        _value_bool AS ValueBool,      -- ДОБАВЛЕНО
        _value_double AS ValueDouble,  -- ДОБАВЛЕНО
        _value_numeric AS ValueNumeric,-- ДОБАВЛЕНО
        _value_datetime AS ValueDatetime, -- ДОБАВЛЕНО
        _value_bytes AS ValueBytes,    -- ДОБАВЛЕНО
        _note AS Note,
        _hash AS [Hash]
    FROM [dbo].[_objects]
    WHERE _id IN (SELECT Value FROM @ObjectIds);
END
GO

-- =====================================================
-- 6. Полная PVT материализация объекта
-- =====================================================
IF EXISTS (SELECT * FROM sys.objects 
           WHERE object_id = OBJECT_ID(N'[dbo].[sp_MaterializeObject]') 
           AND type in (N'P', N'PC'))
    DROP PROCEDURE [dbo].[sp_MaterializeObject]
GO

CREATE PROCEDURE [dbo].[sp_MaterializeObject]
    @ObjectId BIGINT,
    @SchemeId BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Базовые поля
    EXEC sp_GetObjectBase @ObjectId;
    
    -- Значения EAV
    EXEC sp_GetObjectValues @ObjectId;
    
    -- Метаданные схемы для маппинга
    SELECT 
        _structure_id,
        _name,
        db_type,
        _collection_type
    FROM _scheme_metadata_cache
    WHERE _scheme_id = @SchemeId
    ORDER BY _order;
END
GO

PRINT '========================================='
PRINT 'Скрипт 004_pvt_procedures.sql выполнен успешно!'
PRINT 'Создано 6 хранимых процедур'
PRINT '========================================='
GO
```

---

## СЛЕДУЮЩИЕ ФАЙЛЫ

- `005_json_support.sql` - JSON индексы и функции
- `006_performance_indexes.sql` - Дополнительные индексы
- `007_maintenance.sql` - Процедуры обслуживания

Готов создать остальные SQL скрипты по вашему указанию!

