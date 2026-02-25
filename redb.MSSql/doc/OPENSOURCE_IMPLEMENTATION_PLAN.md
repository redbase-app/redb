# План реализации redb.MSSql (OpenSource)

## 🎯 Цель

Создать OpenSource провайдер MSSQL для redb, аналогичный `redb.Postgres`.

**Принцип:** Вся бизнес-логика в `redb.Core` (базовые классы). В `redb.MSSql` только:
- Platform-specific реализации интерфейсов
- SQL диалект для MSSQL
- Тонкие наследники, передающие `MsSqlDialect` в базовые классы

---

## ✅ Что уже готово в redb.MSSql/sql/

**44 stored procedures и functions уже реализованы!**

| Файл | Строк | Содержимое |
|------|-------|------------|
| `redbMSSQL.sql` | 699 | Основная схема БД, таблицы, sequence |
| `redb_json_objects.sql` | 701 | `get_object_json`, `build_hierarchical_properties_optimized` |
| `redb_facets_search.sql` | 2006 | `search_objects_with_facets`, `search_tree_objects_with_facets`, facets |
| `redb_lazy_loading_search.sql` | 750 | `search_objects_with_facets_base`, `get_object_base_fields` |
| `redb_structure_tree.sql` | 372 | `get_scheme_structure_tree`, `get_structure_children` |
| `redb_metadata_cache.sql` | 394 | `sync_metadata_cache_for_scheme`, warmup |
| `redb_projection.sql` | 651 | `search_objects_with_projection_by_ids/paths` |
| `redb_aggregation.sql` | 461 | `aggregate_field`, `aggregate_batch` |
| `redb_grouping.sql` | 585 | `aggregate_grouped`, `aggregate_array_grouped` |
| `redb_window.sql` | 538 | `query_with_window`, `resolve_field_path` |
| **Итого** | **~6700** | **SQL база полностью готова!** |

---

## 📋 Структура плана (обновлённая)

| Этап | Описание | Сложность | Оценка |
|------|----------|-----------|--------|
| **0** | Рефакторинг Core (SimplePasswordHasher, RedbServiceBase) | Средняя | 4-6 ч |
| **1** | SQL + Подготовка (redb_permissions.sql, csproj) | Средняя | 3-4 ч |
| **2** | Data Layer (Connection, Transaction, Bulk, KeyGen) | Высокая | 8-12 ч |
| **3** | MsSqlDialect (168 методов ISqlDialect) | Средняя | 8-12 ч |
| **4** | Providers (10 тонких наследников) + Query (3 класса) | Низкая | 3-5 ч |
| **5** | Extensions + RedbService | Низкая | 2-3 ч |
| **6** | Тестирование и отладка | Средняя | 8-12 ч |
| **Итого** | | | **36-54 ч** |

**Примечания:**
- SQL скрипты на 95% готовы — нужно только `redb_permissions.sql`
- MsSqlDialect: SQL готов, нужно правильно вызывать stored procedures
- Этап 0 делается один раз и улучшает архитектуру для всех провайдеров

---

## 📂 Целевая структура redb.MSSql

```
redb.MSSql/
├── redb.MSSql.csproj          ✅ Есть
│
├── sql/                             ✅ Есть (SQL скрипты)
│   ├── redbMSSQL.sql               ✅ Основная схема
│   ├── redb_json_objects.sql       ✅ get_object_json
│   ├── redb_structure_tree.sql     
│   ├── redb_lazy_loading_search.sql
│   └── ...
│
├── Data/                            ❌ СОЗДАТЬ
│   ├── SqlRedbContext.cs
│   ├── SqlRedbConnection.cs
│   ├── SqlRedbTransaction.cs
│   ├── SqlKeyGenerator.cs
│   └── SqlBulkOperations.cs
│
├── Sql/                             ❌ СОЗДАТЬ
│   └── MsSqlDialect.cs
│
├── Providers/                       ❌ СОЗДАТЬ
│   ├── MssqlObjectStorageProvider.cs
│   ├── MssqlTreeProvider.cs
│   ├── MssqlRoleProvider.cs
│   ├── MssqlUserProvider.cs
│   ├── MssqlPermissionProvider.cs
│   ├── MssqlListProvider.cs
│   ├── MssqlSchemeSyncProvider.cs
│   ├── MssqlValidationProvider.cs
│   ├── MssqlQueryableProvider.cs
│   └── LazyPropsLoader.cs
│
├── Query/                           ❌ СОЗДАТЬ
│   ├── MssqlQueryProvider.cs
│   ├── MssqlTreeQueryProvider.cs
│   └── MssqlTreeQueryable.cs
│
├── Extensions/                      ❌ СОЗДАТЬ
│   ├── ServiceCollectionExtensions.cs
│   └── MssqlOptionsExtensions.cs
│
├── Security/                        ❌ СОЗДАТЬ
│   └── SimplePasswordHasher.cs     (копия из Postgres или ссылка на Core)
│
└── RedbService.cs                   ❌ СОЗДАТЬ
```

---

## Этап 0: Подготовка

### 0.1 Обновить csproj

**Пример:** `redb.Postgres/redb.Postgres.csproj`

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>net9.0</TargetFramework>
    <ImplicitUsings>enable</ImplicitUsings>
    <Nullable>enable</Nullable>
    <LangVersion>12</LangVersion>
  </PropertyGroup>

  <ItemGroup>
    <ProjectReference Include="..\redb.Core\redb.Core.csproj" />
  </ItemGroup>

  <ItemGroup>
    <PackageReference Include="Microsoft.Data.SqlClient" Version="5.2.2" />
    <PackageReference Include="Microsoft.Extensions.Logging.Abstractions" Version="9.0.0" />
  </ItemGroup>
</Project>
```

### 0.2 Создать структуру папок

```
Data/
Sql/
Providers/
Query/
Extensions/
Security/
```

---

## Этап 1: Data Layer

### Интерфейсы из Core

| Интерфейс | Файл в Core | Описание |
|-----------|-------------|----------|
| `IRedbContext` | `redb.Core/Data/IRedbContext.cs` | Фабрика контекста |
| `IRedbConnection` | `redb.Core/Data/IRedbConnection.cs` | Подключение к БД |
| `IRedbTransaction` | `redb.Core/Data/IRedbTransaction.cs` | Транзакция |
| `IKeyGenerator` | `redb.Core/Data/IKeyGenerator.cs` | Генератор ID |
| `IBulkOperations` | `redb.Core/Data/IBulkOperations.cs` | Массовые операции |

### 1.1 SqlRedbContext.cs

**Пример:** `redb.Postgres/Data/NpgsqlRedbContext.cs` (69 строк)

**Назначение:** Фабрика для создания `IRedbConnection`.

**Ключевые отличия MSSQL:**
- Использовать `SqlConnection` вместо `NpgsqlConnection`

### 1.2 SqlRedbConnection.cs

**Пример:** `redb.Postgres/Data/NpgsqlRedbConnection.cs` (511 строк)

**Назначение:** Реализация `IRedbConnection` для выполнения SQL.

**Ключевые методы:**
```csharp
Task<List<T>> QueryAsync<T>(string sql, params object[] parameters);
Task<T?> QueryFirstOrDefaultAsync<T>(string sql, params object[] parameters);
Task<T?> ExecuteScalarAsync<T>(string sql, params object[] parameters);
Task<int> ExecuteAsync(string sql, params object[] parameters);
Task<IRedbTransaction> BeginTransactionAsync();
Task ExecuteAtomicAsync(Func<Task> operations);
```

**Критические отличия MSSQL:**

| Аспект | PostgreSQL | MSSQL |
|--------|------------|-------|
| Параметры | `$1, $2, $3` | `@p0, @p1, @p2` |
| Подключение | `NpgsqlConnection` | `SqlConnection` |
| Команда | `NpgsqlCommand` | `SqlCommand` |
| Параметр | `cmd.Parameters.AddWithValue($"${i}", value)` | `cmd.Parameters.AddWithValue($"@p{i}", value)` |

**Критический код (преобразование параметров):**

```csharp
// PostgreSQL: $1, $2, $3
// MSSQL: @p0, @p1, @p2

private string ConvertParameters(string sql)
{
    // Заменить $N на @pN-1
    return Regex.Replace(sql, @"\$(\d+)", m => 
        $"@p{int.Parse(m.Groups[1].Value) - 1}");
}
```

### 1.3 SqlRedbTransaction.cs

**Пример:** `redb.Postgres/Data/NpgsqlRedbTransaction.cs` (104 строки)

**Назначение:** Обёртка над `SqlTransaction`.

**Копировать логику, заменить:**
- `NpgsqlTransaction` → `SqlTransaction`
- `NpgsqlConnection` → `SqlConnection`

### 1.4 SqlKeyGenerator.cs

**Пример:** `redb.Postgres/Data/NpgsqlKeyGenerator.cs` (68 строк)

**Назначение:** Генерация ID из sequence `global_identity`.

**Критические отличия:**

```sql
-- PostgreSQL
SELECT nextval('global_identity')

-- MSSQL  
SELECT NEXT VALUE FOR global_identity
```

### 1.5 SqlBulkOperations.cs

**Пример:** `redb.Postgres/Data/NpgsqlBulkOperations.cs` (266 строк)

**Назначение:** Массовая вставка через `SqlBulkCopy`.

**Критические отличия:**

| Аспект | PostgreSQL | MSSQL |
|--------|------------|-------|
| Протокол | `BeginBinaryImport` (COPY) | `SqlBulkCopy` |
| API | Стриминг по строкам | DataTable целиком |
| NULL | `WriteNullAsync()` | `DBNull.Value` |

**Критический код:**

```csharp
public async Task BulkInsertObjectsAsync(IEnumerable<RedbObjectRow> objects)
{
    var dt = CreateObjectsDataTable();
    foreach (var obj in objects)
        AddObjectRow(dt, obj);
    
    using var bulk = new SqlBulkCopy(_connection)
    {
        DestinationTableName = "_objects",
        BatchSize = 5000,
        BulkCopyTimeout = 600
    };
    
    foreach (DataColumn col in dt.Columns)
        bulk.ColumnMappings.Add(col.ColumnName, col.ColumnName);
    
    await bulk.WriteToServerAsync(dt);
}

private DataTable CreateObjectsDataTable()
{
    var dt = new DataTable();
    dt.Columns.Add("_id", typeof(long));
    dt.Columns.Add("_id_parent", typeof(long));
    dt.Columns.Add("_id_scheme", typeof(long));
    // ... все колонки
    return dt;
}
```

---

## Этап 2: MsSqlDialect

### Интерфейс

**Файл:** `redb.Core/Query/ISqlDialect.cs` (~168 методов, 982 строки)

### Реализация

**Пример:** `redb.Postgres/sql/PostgreSqlDialect.cs` (790 строк)

**Создать:** `redb.MSSql/Sql/MsSqlDialect.cs`

### Ключевые преобразования

| Категория | PostgreSQL | MSSQL |
|-----------|------------|-------|
| Параметры | `$1, $2` | `@p0, @p1` |
| Пагинация | `LIMIT 10 OFFSET 20` | `OFFSET 20 ROWS FETCH NEXT 10 ROWS ONLY` |
| Массив contains | `= ANY($1)` | `IN (SELECT value FROM OPENJSON(@p0))` |
| Идентификатор | `"name"` | `[name]` |
| Boolean | `true/false` | `1/0` |
| Конкатенация | `\|\|` | `+` |
| COALESCE | `COALESCE(a, b)` | `ISNULL(a, b)` или `COALESCE` |

### Методы по группам

#### Базовые (обязательные первыми)

```csharp
public string ProviderName => "MSSql";

public string FormatParameter(int index) => $"@p{index - 1}";

public string QuoteIdentifier(string name) => $"[{name}]";

public string FormatPagination(int? limit, int? offset)
{
    if (!limit.HasValue && !offset.HasValue) return "";
    var off = offset ?? 0;
    var lim = limit ?? 1000;
    return $"OFFSET {off} ROWS FETCH NEXT {lim} ROWS ONLY";
}

public string FormatArrayContains(string column, string paramName)
    => $"{column} IN (SELECT CAST(value AS BIGINT) FROM OPENJSON({paramName}))";
```

#### Roles SQL (~15 методов)

```csharp
public string Roles_SelectById() =>
    "SELECT _id AS Id, _name AS Name, _id_configuration AS IdConfiguration FROM _roles WHERE _id = @p0";

public string Roles_Insert() =>
    "INSERT INTO _roles (_id, _name) VALUES (@p0, @p1)";
// ... остальные по аналогии с PostgreSqlDialect
```

#### Users SQL (~20 методов)

По аналогии с `PostgreSqlDialect.cs`, заменяя `$N` на `@pN-1`.

#### Permissions SQL (~20 методов)

**ВНИМАНИЕ:** `Permissions_GetEffectiveForObject()` в PostgreSQL вызывает функцию:
```sql
SELECT * FROM get_user_permissions_for_object($1, $2)
```

В MSSQL нужно либо:
1. Создать аналогичную stored procedure
2. Переписать как CTE/JOIN запрос

**Рекомендация:** Создать stored procedure в `redb.MSSql/sql/`.

#### Schemes SQL (~15 методов)

По аналогии.

#### Structures SQL (~10 методов)

По аналогии.

#### Tree SQL (~15 методов)

**Готовые stored procedures в MSSQL:**

```csharp
// Вызов get_object_json (redb_json_objects.sql)
public string Tree_GetObjectJson() =>
    "EXEC get_object_json @object_id = @p0, @max_depth = @p1";

// Вызов get_scheme_structure_tree (redb_structure_tree.sql)
public string Schemes_GetStructureTree() =>
    "EXEC get_scheme_structure_tree @scheme_id = @p0";
```

#### ObjectStorage SQL (~25 методов)

По аналогии с PostgreSqlDialect, заменяя `$N` на `@pN-1`.

#### Lists SQL (~10 методов)

По аналогии.

#### ListItems SQL (~10 методов)

По аналогии.

#### Query SQL (~25 методов)

**✅ Все stored procedures уже есть!**

```csharp
// Вызов search_objects_with_facets (redb_facets_search.sql)
public string Query_SearchObjectsFunction() => "search_objects_with_facets";

// Вызов search_tree_objects_with_facets (redb_facets_search.sql)
public string Query_SearchTreeObjectsFunction() => "search_tree_objects_with_facets";

// Вызов search_objects_with_facets_base (redb_lazy_loading_search.sql)
public string Query_SearchObjectsBaseFunction() => "search_objects_with_facets_base";

// Вызов aggregate_field (redb_aggregation.sql)
public string Query_AggregateFieldSql() =>
    "EXEC aggregate_field @scheme_id = @p0, @structure_path = @p1, @function = @p2, @filters = @p3";

// Пример шаблона для search
public string Query_SearchTemplate() =>
    "EXEC {0} @scheme_id = @p0, @filters = @p1, @limit = @p2, @offset = @p3, @order_by = @p4, @user_id = @p5";
```

**Примечание:** Синтаксис вызова SP в MSSQL: `EXEC proc_name @param = value`

---

## Этап 3: Providers

### Принцип

Все провайдеры — тонкие наследники базовых классов из Core.
Единственное, что делают: передают `MsSqlDialect` в конструктор.

### 3.1 MssqlObjectStorageProvider.cs

**Пример:** `redb.Postgres/Providers/PostgresObjectStorageProvider.cs` (44 строки)

```csharp
public class MssqlObjectStorageProvider : ObjectStorageProviderBase
{
    public MssqlObjectStorageProvider(
        IRedbContext context,
        IRedbObjectSerializer serializer,
        IPermissionProvider permissionProvider,
        IRedbSecurityContext securityContext,
        ISchemeSyncProvider schemeSync,
        RedbServiceConfiguration configuration,
        IListProvider? listProvider = null,
        ILogger? logger = null)
        : base(context, serializer, permissionProvider, securityContext, 
               schemeSync, configuration, new MsSqlDialect(), listProvider, logger)
    {
    }
    
    protected override ILazyPropsLoader CreateLazyPropsLoader()
    {
        return new LazyPropsLoader(Context, SchemeSyncProvider, Serializer, 
                                   Configuration, ListProvider, Logger);
    }
}
```

### 3.2 Остальные провайдеры

| Файл | Пример из Postgres | Строк |
|------|-------------------|-------|
| `MssqlRoleProvider.cs` | `PostgresRoleProvider.cs` | ~38 |
| `MssqlUserProvider.cs` | `PostgresUserProvider.cs` | ~54 |
| `MssqlTreeProvider.cs` | `PostgresTreeProvider.cs` | ~43 |
| `MssqlPermissionProvider.cs` | `PostgresPermissionProvider.cs` | ~40 |
| `MssqlListProvider.cs` | `PostgresListProvider.cs` | ~40 |
| `MssqlSchemeSyncProvider.cs` | `PostgresSchemeSyncProvider.cs` | ~40 |
| `MssqlValidationProvider.cs` | `PostgresValidationProvider.cs` | ~40 |
| `MssqlQueryableProvider.cs` | `PostgresQueryableProvider.cs` | ~40 |

**Шаблон для всех:**
1. Скопировать файл из `redb.Postgres/Providers/`
2. Заменить `Postgres` → `Mssql`
3. Заменить `PostgreSqlDialect` → `MsSqlDialect`
4. Заменить namespace

### 3.3 LazyPropsLoader.cs

**Пример:** `redb.Postgres/Providers/LazyPropsLoader.cs` (296 строк)

Скопировать и заменить:
- `PostgreSqlDialect` → `MsSqlDialect`
- Проверить SQL запросы через `ISqlDialect`

---

## Этап 4: Query

### 4.1 MssqlQueryProvider.cs

**Пример:** `redb.Postgres/Query/PostgresQueryProvider.cs` (38 строк)

```csharp
public class MssqlQueryProvider : QueryProviderBase
{
    public MssqlQueryProvider(
        IRedbContext context,
        IRedbObjectSerializer serializer,
        ILazyPropsLoader? lazyPropsLoader = null,
        RedbServiceConfiguration? configuration = null,
        ILogger? logger = null,
        ISchemeSyncProvider? schemeSync = null)
        : base(context, serializer, new MsSqlDialect(), lazyPropsLoader, 
               configuration, logger, schemeSync)
    {
    }
    
    protected override IFilterExpressionParser CreateFilterParser()
    {
        return new FilterExpressionParser();
    }
}
```

### 4.2 MssqlTreeQueryProvider.cs

**Пример:** `redb.Postgres/Query/PostgresTreeQueryProvider.cs`

Аналогично — копия с заменой диалекта.

### 4.3 MssqlTreeQueryable.cs

**Пример:** `redb.Postgres/Query/PostgresTreeQueryable.cs`

Аналогично.

---

## Этап 5: Extensions + RedbService

### 5.1 ServiceCollectionExtensions.cs

**Пример:** `redb.Postgres/Extensions/ServiceCollectionExtensions.cs`

Регистрация всех сервисов в DI.

### 5.2 MssqlOptionsExtensions.cs

**Пример:** `redb.Postgres/Extensions/PostgresOptionsExtensions.cs`

### 5.3 RedbService.cs

**Пример:** `redb.Postgres/RedbService.cs` (925 строк)

**ВНИМАНИЕ:** Это большой файл. Нужно проверить, можно ли вынести общую логику в Core.

**Вопрос к пользователю:** 
- `RedbService.cs` в Postgres содержит много логики. 
- Можно ли создать `RedbServiceBase` в Core и наследоваться от него?
- Или это специфично для каждого провайдера?

---

## Этап 6: Тестирование

### Проверить

1. **Connection:** Подключение к MSSQL
2. **KeyGenerator:** Генерация ID из sequence
3. **CRUD:** Создание/чтение/обновление/удаление объектов
4. **Bulk:** Массовая вставка
5. **Query:** Поиск и фильтрация
6. **Tree:** Работа с иерархией

---

## ✅ Решения по архитектуре

### Решение 1: RedbService.cs → RedbServiceBase в Core ✅

**Задача:** Вынести общую логику из `redb.Postgres/RedbService.cs` (925 строк) в базовый класс.

**Действия:**
1. Создать `redb.Core/RedbServiceBase.cs` — абстрактный класс с общей логикой
2. Изменить `redb.Postgres/RedbService.cs` — наследовать от `RedbServiceBase`
3. Создать `redb.MSSql/RedbService.cs` — наследовать от `RedbServiceBase`

**Пример структуры:**
```csharp
// redb.Core/RedbServiceBase.cs
public abstract class RedbServiceBase : IRedbService
{
    protected abstract ISqlDialect CreateDialect();
    protected abstract IRedbContext CreateContext();
    // ... общая логика
}

// redb.Postgres/RedbService.cs
public class RedbService : RedbServiceBase
{
    protected override ISqlDialect CreateDialect() => new PostgreSqlDialect();
    protected override IRedbContext CreateContext() => new NpgsqlRedbContext(...);
}

// redb.MSSql/RedbService.cs
public class RedbService : RedbServiceBase
{
    protected override ISqlDialect CreateDialect() => new MsSqlDialect();
    protected override IRedbContext CreateContext() => new SqlRedbContext(...);
}
```

---

### Решение 2: SimplePasswordHasher → Core ✅

**Задача:** Вынести `SimplePasswordHasher` в Core, удалить из Postgres.

**Действия:**
1. Переместить `redb.Postgres/Security/SimplePasswordHasher.cs` → `redb.Core/Security/SimplePasswordHasher.cs`
2. Изменить namespace на `redb.Core.Security`
3. Удалить файл из `redb.Postgres/Security/`
4. Обновить `using` в `redb.Postgres/Providers/PostgresUserProvider.cs`

---

### Решение 3: get_user_permissions_for_object → redb_permissions.sql ✅

**Задача:** Портировать функцию permissions из PostgreSQL в MSSQL.

**Источник:** `redb.Postgres/sql/redbPostgre.sql` строки 1049-1180

**Цель:** `redb.MSSql/sql/redb_permissions.sql` (новый файл)

**Особенности MSSQL при портировании:**
- `RETURNS TABLE` → `RETURNS @result TABLE (...)`
- `$$` → `BEGIN...END`
- `COALESCE` работает одинаково
- `BOOLEAN` → `BIT`
- `SETOF` → table-valued function
- Рекурсивный CTE синтаксис аналогичен

---

## ✅ SQL функции — статус

| PostgreSQL функция | MSSQL аналог | Файл | Статус |
|-------------------|--------------|------|--------|
| `get_object_json` | `get_object_json` | `redb_json_objects.sql` | ✅ |
| `search_objects_with_facets` | `search_objects_with_facets` | `redb_facets_search.sql` | ✅ |
| `search_tree_objects_with_facets` | `search_tree_objects_with_facets` | `redb_facets_search.sql` | ✅ |
| `search_objects_with_facets_base` | `search_objects_with_facets_base` | `redb_lazy_loading_search.sql` | ✅ |
| `get_scheme_structure_tree` | `get_scheme_structure_tree` | `redb_structure_tree.sql` | ✅ |
| `sync_metadata_cache_for_scheme` | `sync_metadata_cache_for_scheme` | `redb_metadata_cache.sql` | ✅ |
| `get_user_permissions_for_object` | `get_user_permissions_for_object` | `redb_permissions.sql` | ❌ Создать |

**Вывод:** SQL база готова на 95%, нужна только функция permissions.

---

## 📊 Сводная таблица файлов

| Файл | Строк (оценка) | Сложность | Пример из Postgres |
|------|----------------|-----------|-------------------|
| **Data/** | | | |
| SqlRedbContext.cs | ~70 | Низкая | NpgsqlRedbContext.cs |
| SqlRedbConnection.cs | ~500 | Высокая | NpgsqlRedbConnection.cs |
| SqlRedbTransaction.cs | ~100 | Низкая | NpgsqlRedbTransaction.cs |
| SqlKeyGenerator.cs | ~70 | Низкая | NpgsqlKeyGenerator.cs |
| SqlBulkOperations.cs | ~300 | Средняя | NpgsqlBulkOperations.cs |
| **Sql/** | | | |
| MsSqlDialect.cs | ~800 | Высокая | PostgreSqlDialect.cs |
| **Providers/** | | | |
| MssqlObjectStorageProvider.cs | ~50 | Низкая | PostgresObjectStorageProvider.cs |
| MssqlTreeProvider.cs | ~45 | Низкая | PostgresTreeProvider.cs |
| MssqlRoleProvider.cs | ~40 | Низкая | PostgresRoleProvider.cs |
| MssqlUserProvider.cs | ~55 | Низкая | PostgresUserProvider.cs |
| MssqlPermissionProvider.cs | ~40 | Низкая | PostgresPermissionProvider.cs |
| MssqlListProvider.cs | ~40 | Низкая | PostgresListProvider.cs |
| MssqlSchemeSyncProvider.cs | ~40 | Низкая | PostgresSchemeSyncProvider.cs |
| MssqlValidationProvider.cs | ~40 | Низкая | PostgresValidationProvider.cs |
| MssqlQueryableProvider.cs | ~40 | Низкая | PostgresQueryableProvider.cs |
| LazyPropsLoader.cs | ~300 | Средняя | LazyPropsLoader.cs |
| **Query/** | | | |
| MssqlQueryProvider.cs | ~40 | Низкая | PostgresQueryProvider.cs |
| MssqlTreeQueryProvider.cs | ~50 | Низкая | PostgresTreeQueryProvider.cs |
| MssqlTreeQueryable.cs | ~50 | Низкая | PostgresTreeQueryable.cs |
| **Extensions/** | | | |
| ServiceCollectionExtensions.cs | ~100 | Низкая | ServiceCollectionExtensions.cs |
| MssqlOptionsExtensions.cs | ~100 | Низкая | PostgresOptionsExtensions.cs |
| **Security/** | | | |
| SimplePasswordHasher.cs | ~50 | Низкая | SimplePasswordHasher.cs |
| **Root** | | | |
| RedbService.cs | ~900 | Средняя | RedbService.cs |
| **Итого** | **~2900** | | |

---

## 🚀 Порядок работы

### Этап 0: Рефакторинг Core (перед MSSQL)

**Цель:** Подготовить Core для поддержки нескольких провайдеров.

#### 0.1 SimplePasswordHasher → Core
1. [ ] Переместить `redb.Postgres/Security/SimplePasswordHasher.cs` → `redb.Core/Security/SimplePasswordHasher.cs`
2. [ ] Изменить namespace: `redb.Postgres.Security` → `redb.Core.Security`
3. [ ] Удалить `redb.Postgres/Security/SimplePasswordHasher.cs`
4. [ ] Обновить `using` в `redb.Postgres/Providers/PostgresUserProvider.cs`
5. [ ] Проверить компиляцию `redb.Postgres`

#### 0.2 RedbServiceBase → Core
6. [ ] Создать `redb.Core/RedbServiceBase.cs` — вынести общую логику из `redb.Postgres/RedbService.cs`
7. [ ] Изменить `redb.Postgres/RedbService.cs` — наследовать от `RedbServiceBase`
8. [ ] Проверить компиляцию и работу `redb.Postgres`

---

### Этап 1: SQL + Подготовка MSSQL

#### День 1: SQL + csproj
1. [ ] Обновить `redb.MSSql.csproj` (добавить Microsoft.Data.SqlClient)
2. [ ] Создать структуру папок: `Data/`, `Sql/`, `Providers/`, `Query/`, `Extensions/`
3. [ ] **Создать `redb.MSSql/sql/redb_permissions.sql`** — портировать `get_user_permissions_for_object`
   - Источник: `redb.Postgres/sql/redbPostgre.sql` строки 1049-1180
   - Учесть особенности MSSQL:
     - `RETURNS TABLE` → `RETURNS @result TABLE (...)`
     - `BOOLEAN` → `BIT`
     - `$$` → `BEGIN...END`
4. [ ] Проверить SQL на тестовой БД MSSQL

---

### Этап 2: Data Layer

#### День 2: Data Layer (часть 1)
5. [ ] `SqlRedbTransaction.cs` — пример: `NpgsqlRedbTransaction.cs`
6. [ ] `SqlRedbContext.cs` — пример: `NpgsqlRedbContext.cs`
7. [ ] `SqlKeyGenerator.cs` — `SELECT NEXT VALUE FOR global_identity`

#### День 3: Data Layer (часть 2)
8. [ ] `SqlRedbConnection.cs` (основной, ~500 строк)
   - Преобразование параметров `$N` → `@pN-1`
   - `SqlConnection`, `SqlCommand`, `SqlParameter`
9. [ ] `SqlBulkOperations.cs` — `SqlBulkCopy` + DataTable

---

### Этап 3: MsSqlDialect

#### День 4-5: MsSqlDialect (168 методов)
10. [ ] `MsSqlDialect.cs` — базовые методы:
    - `FormatParameter(int index)` → `@p{index-1}`
    - `QuoteIdentifier(string name)` → `[name]`
    - `FormatPagination(limit, offset)` → `OFFSET...FETCH`
    - `FormatArrayContains(column, param)` → `IN (SELECT ... FROM OPENJSON)`
11. [ ] Roles SQL (~15 методов) — замена `$N` → `@pN-1`
12. [ ] Users SQL (~20 методов)
13. [ ] Permissions SQL (~20 методов) — включая вызов `get_user_permissions_for_object`
14. [ ] Schemes, Structures, Types SQL (~25 методов)
15. [ ] Tree, ObjectStorage SQL (~40 методов) — вызовы готовых SP
16. [ ] Query SQL (~25 методов) — вызовы `search_objects_with_facets` и др.
17. [ ] Lists, ListItems SQL (~20 методов)

---

### Этап 4: Providers + Query

#### День 6: Providers
18. [ ] `MssqlObjectStorageProvider.cs` — наследник `ObjectStorageProviderBase`
19. [ ] `MssqlTreeProvider.cs`
20. [ ] `MssqlRoleProvider.cs`
21. [ ] `MssqlUserProvider.cs`
22. [ ] `MssqlPermissionProvider.cs`
23. [ ] `MssqlListProvider.cs`
24. [ ] `MssqlSchemeSyncProvider.cs`
25. [ ] `MssqlValidationProvider.cs`
26. [ ] `MssqlQueryableProvider.cs`
27. [ ] `LazyPropsLoader.cs`

#### День 6 (продолжение): Query
28. [ ] `MssqlQueryProvider.cs`
29. [ ] `MssqlTreeQueryProvider.cs`
30. [ ] `MssqlTreeQueryable.cs`

---

### Этап 5: Extensions + Service

#### День 7: Интеграция
31. [ ] `ServiceCollectionExtensions.cs` — DI регистрация
32. [ ] `MssqlOptionsExtensions.cs`
33. [ ] `RedbService.cs` — наследник `RedbServiceBase`

---

### Этап 6: Тестирование

#### День 8: Тестирование
34. [ ] Компиляция проекта без ошибок
35. [ ] Подключение к MSSQL
36. [ ] Генерация ID из sequence
37. [ ] CRUD операции (Create, Read, Update, Delete)
38. [ ] Bulk insert
39. [ ] Query с Where/OrderBy
40. [ ] Tree операции
41. [ ] Permissions
42. [ ] Исправление ошибок

---

## ✅ Чеклист готовности

### Этап 0: Core (перед MSSQL)
- [ ] `SimplePasswordHasher` перемещён в `redb.Core/Security/`
- [ ] `RedbServiceBase` создан в `redb.Core/`
- [ ] `redb.Postgres/RedbService.cs` наследует от `RedbServiceBase`
- [ ] `redb.Postgres` компилируется и работает

### SQL
- [ ] `redb_permissions.sql` создан с `get_user_permissions_for_object`
- [ ] Все скрипты из `redb.MSSql/sql/` развёрнуты на тестовой БД
- [ ] `get_user_permissions_for_object` работает корректно

### Data Layer
- [ ] `SqlRedbContext` создаёт подключения
- [ ] `SqlRedbConnection` выполняет запросы
- [ ] `SqlKeyGenerator` генерирует ID из sequence
- [ ] `SqlBulkOperations` массово вставляет данные

### MsSqlDialect
- [ ] Все 168 методов ISqlDialect реализованы
- [ ] Параметры корректно преобразуются ($N → @pN-1)
- [ ] Stored procedures вызываются корректно

### Providers
- [ ] Все 10 провайдеров компилируются
- [ ] `LazyPropsLoader` загружает Props

### Функциональность
- [ ] Подключение к MSSQL работает
- [ ] CRUD операции работают (Create, Read, Update, Delete)
- [ ] Query с Where/OrderBy работает
- [ ] Tree операции работают (GetChildren, Move, Delete)
- [ ] Permissions работают
- [ ] DI регистрация работает

---

## 📚 Справочные файлы

| Что смотреть | Где |
|--------------|-----|
| Интерфейсы | `redb.Core/Data/`, `redb.Core/Providers/`, `redb.Core/Query/ISqlDialect.cs` |
| Пример реализации | `redb.Postgres/` (полная структура) |
| Готовые SQL скрипты | `redb.MSSql/sql/` (44 stored procedures) |
| PostgreSqlDialect | `redb.Postgres/sql/PostgreSqlDialect.cs` (790 строк, 168 методов) |
| Базовые классы | `redb.Core/Providers/Base/` (вся бизнес-логика) |

---

**Дата создания:** 28 декабря 2024  
**Обновлено:** 28 декабря 2024  
**Версия:** 1.1  
**Статус:** Готов к реализации (SQL база на 95% готова)


