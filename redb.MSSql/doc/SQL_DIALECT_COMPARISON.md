# Сравнение SQL диалектов: PostgreSQL vs MS SQL Server

## 📋 СОДЕРЖАНИЕ

1. [Базовый синтаксис](#1-базовый-синтаксис)
2. [Параметры и плейсхолдеры](#2-параметры-и-плейсхолдеры)
3. [Типы данных](#3-типы-данных)
4. [Функции и операторы](#4-функции-и-операторы)
5. [Window Functions](#5-window-functions)
6. [Common Table Expressions (CTE)](#6-common-table-expressions-cte)
7. [PVT операции](#7-pvt-операции)
8. [Массивы и списки](#8-массивы-и-списки)
9. [JSON операции](#9-json-операции)
10. [Транзакции и изоляция](#10-транзакции-и-изоляция)

---

## 1. БАЗОВЫЙ СИНТАКСИС

### Создание таблиц

#### PostgreSQL
```sql
CREATE TABLE IF NOT EXISTS _migrations (
    _id BIGSERIAL PRIMARY KEY,
    _migration_id TEXT NOT NULL,
    _applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE _migrations IS 'История миграций';
```

#### MS SQL Server
```sql
IF NOT EXISTS (SELECT * FROM sys.objects 
               WHERE object_id = OBJECT_ID(N'[dbo].[_migrations]'))
BEGIN
    CREATE TABLE [dbo].[_migrations] (
        [_id] BIGINT IDENTITY(1,1) PRIMARY KEY,
        [_migration_id] NVARCHAR(500) NOT NULL,
        [_applied_at] DATETIME2 NOT NULL DEFAULT GETDATE()
    );
END

EXEC sp_addextendedproperty 
    @name = N'MS_Description', 
    @value = N'История миграций',
    @level0type = N'SCHEMA', @level0name = N'dbo',
    @level1type = N'TABLE', @level1name = N'_migrations';
```

### LIMIT и OFFSET

#### PostgreSQL
```sql
-- Простой LIMIT
SELECT * FROM _objects 
ORDER BY _id 
LIMIT 10;

-- LIMIT с OFFSET
SELECT * FROM _objects 
ORDER BY _id 
LIMIT 10 OFFSET 20;
```

#### MS SQL Server
```sql
-- TOP (до SQL Server 2012)
SELECT TOP 10 * FROM _objects 
ORDER BY _id;

-- OFFSET/FETCH (SQL Server 2012+, рекомендуется)
SELECT * FROM _objects 
ORDER BY _id
OFFSET 20 ROWS FETCH NEXT 10 ROWS ONLY;

-- Без ORDER BY нельзя использовать OFFSET/FETCH!
-- Нужен минимум ORDER BY (SELECT 0) для обхода
```

### RETURNING vs OUTPUT

#### PostgreSQL
```sql
INSERT INTO _objects (_id, _name) 
VALUES (1, 'Test')
RETURNING _id, _name;

UPDATE _objects 
SET _name = 'Updated'
WHERE _id = 1
RETURNING _id, _name;

DELETE FROM _objects 
WHERE _id = 1
RETURNING _id;
```

#### MS SQL Server
```sql
-- OUTPUT после INSERT
INSERT INTO _objects (_id, _name) 
OUTPUT INSERTED._id, INSERTED._name
VALUES (1, 'Test');

-- OUTPUT после UPDATE
UPDATE _objects 
SET _name = 'Updated'
OUTPUT INSERTED._id, INSERTED._name, DELETED._name AS old_name
WHERE _id = 1;

-- OUTPUT после DELETE
DELETE FROM _objects 
OUTPUT DELETED._id
WHERE _id = 1;

-- Можно сохранять в таблицу
DECLARE @Output TABLE (_id BIGINT, _name NVARCHAR(250));

INSERT INTO _objects (_id, _name) 
OUTPUT INSERTED._id, INSERTED._name INTO @Output
VALUES (1, 'Test');

SELECT * FROM @Output;
```

---

## 2. ПАРАМЕТРЫ И ПЛЕЙСХОЛДЕРЫ

### Позиционные vs Именованные

#### PostgreSQL (позиционные $1, $2, $3)
```sql
-- SQL запрос
SELECT * FROM _objects 
WHERE _id = $1 AND _scheme_id = $2

-- C# код (Npgsql)
var sql = "SELECT * FROM _objects WHERE _id = $1 AND _scheme_id = $2";
var result = await connection.QueryAsync<RedbObject>(sql, objectId, schemeId);
```

#### MS SQL Server (именованные @param)
```sql
-- SQL запрос
SELECT * FROM _objects 
WHERE _id = @p0 AND _scheme_id = @p1

-- C# код (Microsoft.Data.SqlClient)
var sql = "SELECT * FROM _objects WHERE _id = @p0 AND _scheme_id = @p1";
var result = await connection.QueryAsync<RedbObject>(sql, 
    new { p0 = objectId, p1 = schemeId });

-- Альтернатива с именованными параметрами
var sql = "SELECT * FROM _objects WHERE _id = @ObjectId AND _scheme_id = @SchemeId";
var result = await connection.QueryAsync<RedbObject>(sql, 
    new { ObjectId = objectId, SchemeId = schemeId });
```

### IN clause с массивами

#### PostgreSQL
```sql
-- Массив параметров
SELECT * FROM _objects 
WHERE _id = ANY($1::bigint[])

-- C# код
var ids = new long[] { 1, 2, 3, 4, 5 };
var sql = "SELECT * FROM _objects WHERE _id = ANY($1::bigint[])";
var result = await connection.QueryAsync<RedbObject>(sql, ids);
```

#### MS SQL Server
```sql
-- Вариант 1: STRING_SPLIT (простой, но медленный для больших списков)
SELECT * FROM _objects 
WHERE _id IN (SELECT value FROM STRING_SPLIT(@ids, ','))

-- C# код
var ids = new long[] { 1, 2, 3, 4, 5 };
var idsString = string.Join(",", ids);
var sql = "SELECT * FROM _objects WHERE _id IN (SELECT value FROM STRING_SPLIT(@ids, ','))";
var result = await connection.QueryAsync<RedbObject>(sql, new { ids = idsString });

-- Вариант 2: Table-Valued Parameters (производительный)
SELECT * FROM _objects 
WHERE _id IN (SELECT Value FROM @idList)

-- C# код
var table = new DataTable();
table.Columns.Add("Value", typeof(long));
foreach (var id in ids) table.Rows.Add(id);

var param = new SqlParameter("@idList", SqlDbType.Structured)
{
    TypeName = "[dbo].[BigIntListType]",
    Value = table
};
var sql = "SELECT * FROM _objects WHERE _id IN (SELECT Value FROM @idList)";
var result = await connection.QueryAsync<RedbObject>(sql, param);
```

---

## 3. ТИПЫ ДАННЫХ

### Маппинг типов

| Тип данных | PostgreSQL | MS SQL Server | .NET Type |
|------------|------------|---------------|-----------|
| **Целые числа** |
| Auto-increment | `BIGSERIAL` | `BIGINT IDENTITY(1,1)` | `long` |
| 64-bit int | `BIGINT` | `BIGINT` | `long` |
| 32-bit int | `INTEGER` | `INT` | `int` |
| 16-bit int | `SMALLINT` | `SMALLINT` | `short` |
| 8-bit int | `SMALLINT` | `TINYINT` | `byte` |
| **Вещественные** |
| Double | `DOUBLE PRECISION` | `FLOAT(53)` | `double` |
| Float | `REAL` | `REAL` | `float` |
| Decimal | `NUMERIC(p,s)` | `DECIMAL(p,s)` | `decimal` |
| **Строки** |
| Unlimited | `TEXT` | `NVARCHAR(MAX)` | `string` |
| Variable | `VARCHAR(n)` | `VARCHAR(n)` | `string` |
| Unicode | `VARCHAR(n)` | `NVARCHAR(n)` | `string` |
| Fixed | `CHAR(n)` | `CHAR(n)` | `string` |
| **Дата/Время** |
| Timestamp | `TIMESTAMP` | `DATETIME2` | `DateTime` |
| With timezone | `TIMESTAMPTZ` | `DATETIMEOFFSET` | `DateTimeOffset` |
| Date only | `DATE` | `DATE` | `DateOnly` (.NET 6+) |
| Time only | `TIME` | `TIME` | `TimeOnly` (.NET 6+) |
| Legacy | `TIMESTAMP` | `DATETIME` | `DateTime` |
| **Логические** |
| Boolean | `BOOLEAN` | `BIT` | `bool` |
| **Бинарные** |
| Binary | `BYTEA` | `VARBINARY(MAX)` | `byte[]` |
| Fixed binary | - | `BINARY(n)` | `byte[]` |
| **Специальные** |
| UUID | `UUID` | `UNIQUEIDENTIFIER` | `Guid` |
| JSON | `JSON`, `JSONB` | `NVARCHAR(MAX)` | `string` / JsonDocument |
| XML | `XML` | `XML` | `XmlDocument` |
| **Массивы** |
| Array | `type[]` | ❌ Нет | - |

### Примеры преобразований

```sql
-- PostgreSQL: Boolean
CREATE TABLE test (flag BOOLEAN);
INSERT INTO test VALUES (TRUE), (FALSE);
SELECT * FROM test WHERE flag = TRUE;

-- MS SQL Server: Bit
CREATE TABLE test (flag BIT);
INSERT INTO test VALUES (1), (0);
SELECT * FROM test WHERE flag = 1;

-- PostgreSQL: UUID
CREATE TABLE test (id UUID);
INSERT INTO test VALUES (gen_random_uuid());

-- MS SQL Server: UNIQUEIDENTIFIER
CREATE TABLE test (id UNIQUEIDENTIFIER);
INSERT INTO test VALUES (NEWID());

-- PostgreSQL: Массивы
CREATE TABLE test (tags TEXT[]);
INSERT INTO test VALUES (ARRAY['tag1', 'tag2']);
SELECT * FROM test WHERE 'tag1' = ANY(tags);

-- MS SQL Server: JSON вместо массивов
CREATE TABLE test (tags NVARCHAR(MAX));
INSERT INTO test VALUES (N'["tag1","tag2"]');
SELECT * FROM test WHERE tags LIKE '%tag1%';
-- Или с JSON функциями (SQL 2016+):
SELECT * FROM test 
WHERE EXISTS (
    SELECT * FROM OPENJSON(tags) 
    WHERE value = 'tag1'
);
```

---

## 4. ФУНКЦИИ И ОПЕРАТОРЫ

### Строковые функции

| Операция | PostgreSQL | MS SQL Server |
|----------|------------|---------------|
| Конкатенация | `str1 \|\| str2` | `str1 + str2` или `CONCAT(str1, str2)` |
| Длина | `LENGTH(str)` | `LEN(str)` или `DATALENGTH(str)` |
| Подстрока | `SUBSTRING(str, start, len)` | `SUBSTRING(str, start, len)` |
| Верхний регистр | `UPPER(str)` | `UPPER(str)` |
| Нижний регистр | `LOWER(str)` | `LOWER(str)` |
| Trim | `TRIM(str)` | `TRIM(str)` (SQL 2017+) или `LTRIM(RTRIM(str))` |
| Replace | `REPLACE(str, old, new)` | `REPLACE(str, old, new)` |
| Position | `POSITION(substr IN str)` | `CHARINDEX(substr, str)` |

### Примеры

```sql
-- PostgreSQL
SELECT 'Hello' || ' ' || 'World';  -- 'Hello World'
SELECT LENGTH('Test');              -- 4
SELECT POSITION('lo' IN 'Hello');   -- 4

-- MS SQL Server
SELECT 'Hello' + ' ' + 'World';     -- 'Hello World'
SELECT CONCAT('Hello', ' ', 'World'); -- 'Hello World' (безопаснее с NULL)
SELECT LEN('Test');                 -- 4
SELECT CHARINDEX('lo', 'Hello');    -- 4
```

### Математические функции

| Операция | PostgreSQL | MS SQL Server |
|----------|------------|---------------|
| Округление | `ROUND(n, d)` | `ROUND(n, d)` |
| Потолок | `CEIL(n)` | `CEILING(n)` |
| Пол | `FLOOR(n)` | `FLOOR(n)` |
| Абсолют | `ABS(n)` | `ABS(n)` |
| Степень | `POWER(b, e)` | `POWER(b, e)` |
| Квадратный корень | `SQRT(n)` | `SQRT(n)` |
| Модуль | `MOD(n, m)` | `n % m` |

### Дата/Время функции

| Операция | PostgreSQL | MS SQL Server |
|----------|------------|---------------|
| Текущая дата/время | `NOW()`, `CURRENT_TIMESTAMP` | `GETDATE()`, `SYSDATETIME()` |
| Только дата | `CURRENT_DATE` | `CAST(GETDATE() AS DATE)` |
| Только время | `CURRENT_TIME` | `CAST(GETDATE() AS TIME)` |
| Добавить интервал | `date + INTERVAL '1 day'` | `DATEADD(day, 1, date)` |
| Разница | `date1 - date2` | `DATEDIFF(day, date1, date2)` |
| Извлечь часть | `EXTRACT(YEAR FROM date)` | `DATEPART(year, date)` или `YEAR(date)` |

### Примеры

```sql
-- PostgreSQL
SELECT NOW();                               -- 2024-12-26 15:30:45.123456+00
SELECT NOW() + INTERVAL '1 day';            -- Завтра
SELECT NOW() - INTERVAL '1 hour';           -- Час назад
SELECT EXTRACT(YEAR FROM NOW());            -- 2024
SELECT AGE(TIMESTAMP '2024-01-01');         -- Интервал от даты до сейчас

-- MS SQL Server
SELECT GETDATE();                           -- 2024-12-26 15:30:45.123
SELECT SYSDATETIME();                       -- 2024-12-26 15:30:45.1234567 (точнее)
SELECT DATEADD(day, 1, GETDATE());          -- Завтра
SELECT DATEADD(hour, -1, GETDATE());        -- Час назад
SELECT YEAR(GETDATE());                     -- 2024
SELECT DATEDIFF(day, '2024-01-01', GETDATE()); -- Дней от начала года
```

### Агрегатные функции

Большинство одинаковы:

```sql
-- Оба диалекта
COUNT(*), COUNT(col), COUNT(DISTINCT col)
SUM(col), AVG(col), MIN(col), MAX(col)

-- PostgreSQL: дополнительные
ARRAY_AGG(col)          -- Агрегация в массив
STRING_AGG(col, sep)    -- Конкатенация строк

-- MS SQL Server: эквиваленты
STRING_AGG(col, sep)    -- SQL Server 2017+ (аналогично PostgreSQL)
-- До 2017: FOR XML PATH или STUFF
```

---

## 5. WINDOW FUNCTIONS

### Синтаксис (практически идентичен)

```sql
-- PostgreSQL
SELECT 
    _id,
    _name,
    ROW_NUMBER() OVER (PARTITION BY _id_scheme ORDER BY _date_create) AS rn,
    RANK() OVER (ORDER BY _id) AS rnk,
    LAG(_name) OVER (ORDER BY _id) AS prev_name,
    LEAD(_name) OVER (ORDER BY _id) AS next_name,
    SUM(_value_long) OVER (PARTITION BY _id_scheme) AS total  -- NOTE: _value_long (not _code_int!)
FROM _objects;

-- MS SQL Server (идентично!)
SELECT 
    _id,
    _name,
    ROW_NUMBER() OVER (PARTITION BY _id_scheme ORDER BY _date_create) AS rn,
    RANK() OVER (ORDER BY _id) AS rnk,
    LAG(_name) OVER (ORDER BY _id) AS prev_name,
    LEAD(_name) OVER (ORDER BY _id) AS next_name,
    SUM(_value_long) OVER (PARTITION BY _id_scheme) AS total  -- NOTE: _value_long (not _code_int!)
FROM _objects;
```

### Поддерживаемые функции

| Функция | PostgreSQL | MS SQL Server | Описание |
|---------|------------|---------------|----------|
| `ROW_NUMBER()` | ✅ | ✅ | Порядковый номер строки |
| `RANK()` | ✅ | ✅ | Ранг с пропусками |
| `DENSE_RANK()` | ✅ | ✅ | Ранг без пропусков |
| `NTILE(n)` | ✅ | ✅ | Разбиение на N групп |
| `LAG()` | ✅ | ✅ SQL 2012+ | Предыдущее значение |
| `LEAD()` | ✅ | ✅ SQL 2012+ | Следующее значение |
| `FIRST_VALUE()` | ✅ | ✅ SQL 2012+ | Первое значение в окне |
| `LAST_VALUE()` | ✅ | ✅ SQL 2012+ | Последнее значение в окне |
| `NTH_VALUE()` | ✅ | ❌ | N-ное значение |

**Важно:** В MS SQL Server полная поддержка Window Functions появилась в SQL Server 2012.

---

## 6. COMMON TABLE EXPRESSIONS (CTE)

Синтаксис практически идентичен:

```sql
-- PostgreSQL
WITH cte AS (
    SELECT _id, _name 
    FROM _objects 
    WHERE _id_scheme = 100
)
SELECT * FROM cte;

-- MS SQL Server (идентично!)
WITH cte AS (
    SELECT _id, _name 
    FROM _objects 
    WHERE _id_scheme = 100
)
SELECT * FROM cte;
```

### Рекурсивные CTE

```sql
-- PostgreSQL
WITH RECURSIVE tree AS (
    -- Anchor
    SELECT _id, _id_parent, _name, 1 AS level
    FROM _objects
    WHERE _id_parent IS NULL
    
    UNION ALL
    
    -- Recursive
    SELECT o._id, o._id_parent, o._name, t.level + 1
    FROM _objects o
    INNER JOIN tree t ON o._id_parent = t._id
)
SELECT * FROM tree;

-- MS SQL Server (без RECURSIVE ключевого слова!)
WITH tree AS (
    -- Anchor
    SELECT _id, _id_parent, _name, 1 AS level
    FROM _objects
    WHERE _id_parent IS NULL
    
    UNION ALL
    
    -- Recursive
    SELECT o._id, o._id_parent, o._name, t.level + 1
    FROM _objects o
    INNER JOIN tree t ON o._id_parent = t._id
)
SELECT * FROM tree;
```

**Отличие:** В PostgreSQL используется `WITH RECURSIVE`, в MS SQL - просто `WITH`.

---

## 7. PVT ОПЕРАЦИИ

### PostgreSQL: crosstab (расширение tablefunc)

```sql
-- Установка расширения
CREATE EXTENSION IF NOT EXISTS tablefunc;

-- PVT с crosstab
SELECT * FROM crosstab(
    'SELECT _id_object, _id_structure, _String 
     FROM _values 
     ORDER BY 1, 2',
    'SELECT DISTINCT _id_structure FROM _values ORDER BY 1'
) AS ct (
    object_id BIGINT,
    field1 TEXT,
    field2 TEXT,
    field3 TEXT
);

-- Альтернатива: FILTER (стандартный SQL)
SELECT 
    _id_object,
    MAX(_String) FILTER (WHERE _id_structure = 1) AS field1,
    MAX(_String) FILTER (WHERE _id_structure = 2) AS field2,
    MAX(_String) FILTER (WHERE _id_structure = 3) AS field3
FROM _values
GROUP BY _id_object;
```

### MS SQL Server: PVT

```sql
-- Статический PVT (колонки известны заранее)
SELECT 
    _id_object,
    [1] AS field1,
    [2] AS field2,
    [3] AS field3
FROM (
    SELECT _id_object, _id_structure, _String
    FROM _values
) AS SourceTable
PVT (
    MAX(_String)
    FOR _id_structure IN ([1], [2], [3])
) AS PvtTable;

-- Динамический PVT (колонки определяются в runtime)
DECLARE @columns NVARCHAR(MAX), @sql NVARCHAR(MAX);

SELECT @columns = STRING_AGG(QUOTENAME(_id_structure), ',')
FROM (SELECT DISTINCT _id_structure FROM _values) AS structures;

SET @sql = N'
SELECT _id_object, ' + @columns + '
FROM (
    SELECT _id_object, _id_structure, _String
    FROM _values
) AS SourceTable
PVT (
    MAX(_String)
    FOR _id_structure IN (' + @columns + ')
) AS PvtTable';

EXEC sp_executesql @sql;
```

### Сравнение для redb

#### PostgreSQL Pro
```sql
-- Использует FILTER для динамического PVT
SELECT 
    o._id,
    o._name,
    MAX(v._String) FILTER (WHERE v._id_structure = $1) AS Price,
    MAX(v._Long) FILTER (WHERE v._id_structure = $2) AS Quantity,
    MAX(v._Double) FILTER (WHERE v._id_structure = $3) AS Total
FROM _objects o
LEFT JOIN _values v ON v._id_object = o._id
WHERE o._id_scheme = $4
GROUP BY o._id, o._name;
```

#### MS SQL Server Pro (предлагаемое решение)
```sql
-- Вариант 1: CASE WHEN (универсальный, работает везде)
SELECT 
    o._id,
    o._name,
    MAX(CASE WHEN v._id_structure = @p0 THEN v._String END) AS Price,
    MAX(CASE WHEN v._id_structure = @p1 THEN v._Long END) AS Quantity,
    MAX(CASE WHEN v._id_structure = @p2 THEN v._Double END) AS Total
FROM _objects o
LEFT JOIN _values v ON v._id_object = o._id
WHERE o._id_scheme = @p3
GROUP BY o._id, o._name;

-- Вариант 2: PVT (более читаемый для статических полей)
SELECT 
    _id,
    _name,
    [Price],
    [Quantity],
    [Total]
FROM (
    SELECT 
        o._id,
        o._name,
        s._alias AS FieldName,
        CASE t._db_type
            WHEN 'String' THEN v._String
            WHEN 'Long' THEN CAST(v._Long AS NVARCHAR(50))
            WHEN 'Double' THEN CAST(v._Double AS NVARCHAR(50))
        END AS Value
    FROM _objects o
    LEFT JOIN _values v ON v._id_object = o._id
    LEFT JOIN _structures s ON v._id_structure = s._id
    LEFT JOIN _types t ON s._id_type = t._id
    WHERE o._id_scheme = @p0
) AS SourceTable
PVT (
    MAX(Value)
    FOR FieldName IN ([Price], [Quantity], [Total])
) AS PvtTable;
```

**Рекомендация для redb.MSSql.Pro:** Использовать `CASE WHEN` для динамического PVT, так как он:
- Работает во всех версиях SQL Server
- Не требует динамического SQL
- Типобезопасен
- Производителен

---

## 8. МАССИВЫ И СПИСКИ

### PostgreSQL: Нативная поддержка массивов

```sql
-- Создание таблицы с массивом
CREATE TABLE test (
    id BIGINT,
    tags TEXT[]
);

-- Вставка
INSERT INTO test VALUES (1, ARRAY['tag1', 'tag2', 'tag3']);
INSERT INTO test VALUES (2, '{tag4,tag5}'::TEXT[]);

-- Поиск
SELECT * FROM test WHERE 'tag1' = ANY(tags);
SELECT * FROM test WHERE tags @> ARRAY['tag1'];
SELECT * FROM test WHERE tags && ARRAY['tag1', 'tag2']; -- Пересечение

-- Разворачивание массива
SELECT id, unnest(tags) AS tag FROM test;

-- Агрегация в массив
SELECT ARRAY_AGG(_name) FROM _objects;
```

### MS SQL Server: Эмуляция через JSON или таблицы

```sql
-- Вариант 1: JSON (SQL Server 2016+)
CREATE TABLE test (
    id BIGINT,
    tags NVARCHAR(MAX) -- Хранится как JSON: ["tag1","tag2","tag3"]
);

-- Вставка
INSERT INTO test VALUES (1, N'["tag1","tag2","tag3"]');

-- Поиск
SELECT * FROM test 
WHERE EXISTS (
    SELECT * FROM OPENJSON(tags) 
    WHERE value = 'tag1'
);

-- Разворачивание JSON
SELECT t.id, j.value AS tag
FROM test t
CROSS APPLY OPENJSON(t.tags) j;

-- Агрегация в JSON
SELECT id, (
    SELECT STRING_AGG(value, ',')
    FROM (VALUES ('tag1'), ('tag2'), ('tag3')) AS t(value)
) AS tags_string
FROM test;

-- Или с FOR JSON (SQL 2016+)
SELECT id, (
    SELECT value 
    FROM (VALUES ('tag1'), ('tag2'), ('tag3')) AS t(value)
    FOR JSON PATH
) AS tags_json
FROM test;

-- Вариант 2: Связанная таблица (традиционный подход)
CREATE TABLE test (
    id BIGINT PRIMARY KEY
);

CREATE TABLE test_tags (
    test_id BIGINT,
    tag NVARCHAR(250),
    FOREIGN KEY (test_id) REFERENCES test(id)
);

-- Вставка
INSERT INTO test VALUES (1);
INSERT INTO test_tags VALUES (1, 'tag1'), (1, 'tag2'), (1, 'tag3');

-- Поиск
SELECT DISTINCT t.* 
FROM test t
INNER JOIN test_tags tt ON t.id = tt.test_id
WHERE tt.tag = 'tag1';

-- Агрегация
SELECT 
    t.id,
    STRING_AGG(tt.tag, ',') AS tags
FROM test t
LEFT JOIN test_tags tt ON t.id = tt.test_id
GROUP BY t.id;
```

### Рекомендация для redb.MSSql.Pro

Для совместимости с PostgreSQL версией:

1. **Простые списки**: JSON (SQL 2016+)
2. **Производительные запросы**: Связанные таблицы
3. **Передача параметров**: Table-Valued Parameters

---

## 9. JSON ОПЕРАЦИИ

### PostgreSQL: JSONB

```sql
-- Создание
CREATE TABLE test (data JSONB);

-- Вставка
INSERT INTO test VALUES ('{"name":"John","age":30,"address":{"city":"Moscow"}}');

-- Извлечение значения
SELECT data->'name' AS name FROM test;              -- Результат: "John" (JSON)
SELECT data->>'name' AS name FROM test;             -- Результат: John (TEXT)
SELECT data->'address'->>'city' AS city FROM test;  -- Результат: Moscow

-- Проверка существования ключа
SELECT * FROM test WHERE data ? 'name';
SELECT * FROM test WHERE data->'address' ? 'city';

-- Изменение
UPDATE test SET data = data || '{"age":31}';
UPDATE test SET data = jsonb_set(data, '{address,city}', '"SPb"');

-- Индексы
CREATE INDEX idx_data_name ON test ((data->>'name'));
CREATE INDEX idx_data_gin ON test USING gin (data);

-- Поиск
SELECT * FROM test WHERE data @> '{"name":"John"}';
```

### MS SQL Server: JSON функции (SQL 2016+)

```sql
-- Создание
CREATE TABLE test (data NVARCHAR(MAX));

-- Проверка валидности
ALTER TABLE test ADD CONSTRAINT chk_json CHECK (ISJSON(data) = 1);

-- Вставка
INSERT INTO test VALUES (N'{"name":"John","age":30,"address":{"city":"Moscow"}}');

-- Извлечение значения
SELECT JSON_VALUE(data, '$.name') AS name FROM test;              -- John
SELECT JSON_VALUE(data, '$.address.city') AS city FROM test;      -- Moscow
SELECT JSON_QUERY(data, '$.address') AS address FROM test;        -- {"city":"Moscow"}

-- Проверка существования (нет встроенной функции, используем JSON_VALUE IS NOT NULL)
SELECT * FROM test WHERE JSON_VALUE(data, '$.name') IS NOT NULL;

-- Изменение
UPDATE test SET data = JSON_MODIFY(data, '$.age', 31);
UPDATE test SET data = JSON_MODIFY(data, '$.address.city', 'SPb');

-- Индексы (через computed column)
ALTER TABLE test ADD name AS JSON_VALUE(data, '$.name');
CREATE INDEX idx_data_name ON test(name);

-- Или full-text index для поиска
CREATE FULLTEXT INDEX ON test(data) KEY INDEX PK_test;

-- Разбор JSON в таблицу
SELECT * 
FROM test t
CROSS APPLY OPENJSON(t.data) 
WITH (
    name NVARCHAR(50) '$.name',
    age INT '$.age',
    city NVARCHAR(50) '$.address.city'
);

-- Формирование JSON из таблицы
SELECT _id, _name, _value_long  -- NOTE: _value_long (not _code_int!)
FROM _objects
FOR JSON PATH;

-- Результат:
-- [{"_id":1,"_name":"Test","_value_long":100}]

-- С корневым элементом
SELECT _id, _name, _value_long
FROM _objects
FOR JSON PATH, ROOT('objects');

-- Результат:
-- {"objects":[{"_id":1,"_name":"Test","_value_long":100}]}
```

### Сравнение производительности

| Операция | PostgreSQL JSONB | MS SQL JSON | Примечание |
|----------|------------------|-------------|------------|
| Хранение | Бинарный формат | Текст | JSONB эффективнее |
| Индексация | GIN, GiST | Computed columns | JSONB удобнее |
| Поиск по ключу | Очень быстро | Медленно без индекса | |
| Изменение | В месте | Полная перезапись | |
| Валидация | Автоматическая | Нужен CONSTRAINT | |

---

## 10. ТРАНЗАКЦИИ И ИЗОЛЯЦИЯ

### Уровни изоляции

| Уровень | PostgreSQL | MS SQL Server | Описание |
|---------|------------|---------------|-----------|
| READ UNCOMMITTED | ✅ | ✅ | Грязное чтение разрешено |
| READ COMMITTED | ✅ (default) | ✅ (default) | Грязное чтение запрещено |
| REPEATABLE READ | ✅ | ✅ | Phantom reads возможны |
| SERIALIZABLE | ✅ | ✅ | Полная изоляция |
| SNAPSHOT | ❌ | ✅ | Версионность строк |

### PostgreSQL

```sql
-- Начало транзакции
BEGIN;

-- Установка уровня изоляции
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;

-- Savepoint
SAVEPOINT sp1;

-- Откат к savepoint
ROLLBACK TO SAVEPOINT sp1;

-- Commit
COMMIT;

-- Rollback
ROLLBACK;
```

### MS SQL Server

```sql
-- Начало транзакции
BEGIN TRANSACTION;

-- Установка уровня изоляции
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;

-- Savepoint
SAVE TRANSACTION sp1;

-- Откат к savepoint
ROLLBACK TRANSACTION sp1;

-- Commit
COMMIT TRANSACTION;

-- Rollback
ROLLBACK TRANSACTION;

-- SNAPSHOT isolation (нужно включить на уровне БД)
ALTER DATABASE redb SET ALLOW_SNAPSHOT_ISOLATION ON;
ALTER DATABASE redb SET READ_COMMITTED_SNAPSHOT ON;

BEGIN TRANSACTION;
SET TRANSACTION ISOLATION LEVEL SNAPSHOT;
-- ...
COMMIT;
```

### Блокировки

```sql
-- PostgreSQL: Явные блокировки
SELECT * FROM _objects WHERE _id = 1 FOR UPDATE;
SELECT * FROM _objects WHERE _id = 1 FOR SHARE;

-- MS SQL Server: Table hints
SELECT * FROM _objects WITH (UPDLOCK) WHERE _id = 1;
SELECT * FROM _objects WITH (HOLDLOCK) WHERE _id = 1;
SELECT * FROM _objects WITH (NOLOCK) WHERE _id = 1;  -- Dirty read
SELECT * FROM _objects WITH (ROWLOCK) WHERE _id = 1;
SELECT * FROM _objects WITH (PAGLOCK) WHERE _id = 1;
SELECT * FROM _objects WITH (TABLOCK) WHERE _id = 1;
```

---

## 📊 ИТОГОВАЯ ТАБЛИЦА СОВМЕСТИМОСТИ

| Фича | PostgreSQL | MS SQL Server | Совместимость |
|------|------------|---------------|---------------|
| Базовый SQL | ✅ | ✅ | 95% |
| Window Functions | ✅ | ✅ (2012+) | 90% |
| CTE | ✅ | ✅ | 95% |
| Рекурсивные CTE | ✅ RECURSIVE | ✅ | 100% |
| JSON | ✅ JSONB | ✅ (2016+) | 70% |
| Массивы | ✅ Native | ❌ Эмуляция | 0% |
| PVT | ✅ crosstab/FILTER | ✅ PVT | 80% |
| Full-text search | ✅ tsvector | ✅ | 60% |
| RETURNING | ✅ | ✅ OUTPUT | 90% |
| UPSERT | ✅ ON CONFLICT | ✅ MERGE | 85% |

---

## 🎯 ВЫВОДЫ ДЛЯ redb.MSSql.Pro

### Легко портируется
- Базовые CRUD операции
- Window Functions
- CTE (рекурсивные и обычные)
- Агрегации и группировки

### Требует адаптации
- Параметры ($1 → @p0)
- LIMIT/OFFSET → TOP/OFFSET FETCH
- Массивы → TVP или JSON
- FILTER → CASE WHEN
- JSONB → JSON функции

### Потенциальные проблемы
- Отсутствие массивов (нужны TVP)
- Другая реализация JSON
- Другой синтаксис PVT
- Производительность JSON операций

### Рекомендации
1. Использовать **CASE WHEN** вместо FILTER для PVT
2. Использовать **Table-Valued Parameters** для массивов
3. Использовать **JSON** (SQL 2016+) для сложных структур
4. Использовать **computed columns** для индексов на JSON
5. Тщательно тестировать производительность

---

**Следующий шаг:** Реализация ProMSSqlDialect с учётом всех различий!

