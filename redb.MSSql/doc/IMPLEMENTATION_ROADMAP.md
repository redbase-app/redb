# Пошаговый план реализации redb.MSSql.Pro

## ⚠️ КРИТИЧЕСКОЕ ЗАМЕЧАНИЕ

**ПРОБЛЕМА:** Планировали сразу делать Pro версию, но **БАЗОВЫЙ redb.MSSql ПУСТОЙ!**

Текущее состояние `redb.MSSql`:
- ✅ `redb.MSSql.csproj` - файл проекта существует
- ⚠️ `sql/redbMSSQL.sql` - УСТАРЕВШИЙ (нет _value_*, _collection_type)
- ❌ **ВСЕ ОСТАЛЬНОЕ ОТСУТСТВУЕТ!**

**ПРАВИЛЬНЫЙ ПОРЯДОК:**
1. **ФАЗА 0:** Обновить SQL схему + создать базовый redb.MSSql (5-7 дней)
2. **ФАЗА 1-6:** Затем создавать redb.MSSql.Pro (10-14 дней)

**ИТОГО: 15-21 дней (а не 10-16!)**

---

## 🎯 ЦЕЛЬ

Создать полнофункциональную Pro версию для MS SQL Server с поддержкой всех возможностей PostgreSQL Pro версии.

---

## 📅 ПЛАН РАБОТЫ (15-21 день, включая ФАЗУ 0)

### 🔴 ФАЗА 0: Базовый redb.MSSql (5-7 дней) ← ОБЯЗАТЕЛЬНО СНАЧАЛА!

#### День 0-1: SQL Схема
- [ ] Обновить `sql/redbMSSQL.sql` до актуальной версии
- [ ] Включить все изменения из 004_add_dictionary_array_support.sql
- [ ] Создать SEQUENCE для global_identity
- [ ] Протестировать создание схемы на SQL Server

#### День 2-3: Data Layer
- [ ] Создать `Data/MSSqlRedbContext.cs`
- [ ] Создать `Data/MSSqlRedbConnection.cs`
- [ ] Создать `Data/MSSqlRedbTransaction.cs`
- [ ] Создать `Data/MSSqlKeyGenerator.cs`
- [ ] Создать `Data/MSSqlBulkOperations.cs`

#### День 4: SQL Диалект
- [ ] Создать `Sql/MSSqlDialect.cs` (ISqlDialect)
- [ ] Реализовать все методы базового интерфейса
- [ ] Адаптировать параметры ($1 → @p0)
- [ ] Адаптировать LIMIT → OFFSET/FETCH

#### День 5-6: Провайдеры
- [ ] Создать `Providers/MSSqlObjectStorageProvider.cs`
- [ ] Создать `Providers/MSSqlTreeProvider.cs`
- [ ] Создать `Providers/MSSqlSchemeSyncProvider.cs`
- [ ] Создать `Providers/MSSqlPermissionProvider.cs`
- [ ] Создать `Providers/MSSqlUserProvider.cs`
- [ ] Создать `Providers/MSSqlRoleProvider.cs`
- [ ] Создать `Providers/MSSqlListProvider.cs`
- [ ] Создать `Providers/MSSqlQueryableProvider.cs`
- [ ] Создать `Providers/LazyPropsLoader.cs`

#### День 7: Сервис и Extensions
- [ ] Создать `RedbService.cs`
- [ ] Создать `Extensions/ServiceCollectionExtensions.cs`
- [ ] Создать `Extensions/MSSqlOptionsExtensions.cs`
- [ ] Базовое тестирование CRUD

---

### 🔵 ФАЗА 1: Pro SQL Инфраструктура (2-3 дня) - ПОСЛЕ ФАЗЫ 0!

#### День 8: Pro SQL Скрипты
- [ ] Создать структуру проекта `redb.MSSql.Pro`
- [ ] Создать папку `sql/`
- [ ] Реализовать `001_migrations_table.sql`
- [ ] Реализовать `002_scheme_metadata_cache.sql`
- [ ] Реализовать `003_tvp_types.sql`
- [ ] Тестирование SQL скриптов на реальной БД

#### День 9: Дополнительные Pro SQL компоненты
- [ ] Реализовать `004_pvt_procedures.sql`
- [ ] Реализовать `005_performance_indexes.sql`
- [ ] Реализовать `006_maintenance.sql`
- [ ] Документация SQL скриптов
- [ ] Создать `Sql/ProMSSqlDialect.cs`
- [ ] Реализовать интерфейс `ISqlDialectPro`
- [ ] Реализовать все методы резолва полей
- [ ] Реализовать все методы миграций
- [ ] Реализовать все методы материализации
- [ ] Unit тесты для диалекта

---

### 🟢 ФАЗА 2: Pro Провайдеры (3-4 дня)

#### День 10: ProMSSqlObjectStorageProvider
- [ ] Создать `Providers/ProMSSqlObjectStorageProvider.cs`
- [ ] Наследовать от `ProObjectStorageProviderBase`
- [ ] Реализовать `CreateLazyPropsLoader()`
- [ ] Адаптировать SQL запросы под MSSQL
- [ ] Тесты сохранения объектов
- [ ] Тесты параллельного сравнения деревьев

#### День 11: ProMSSqlTreeProvider
- [ ] Создать `Providers/ProMSSqlTreeProvider.cs`
- [ ] Наследовать от `ProTreeProviderBase`
- [ ] Реализовать `LoadChildrenBySchemeBaseAsync()`
- [ ] Реализовать `LoadChildrenBaseAsync()`
- [ ] Реализовать `LoadObjectByIdAsync()`
- [ ] Реализовать `CreateProLazyPropsLoader()`
- [ ] Тесты загрузки детей (typed)
- [ ] Тесты полиморфной загрузки

#### День 12: ProQueryableProvider
- [ ] Создать `Providers/ProQueryableProvider.cs`
- [ ] Интеграция с `ProQueryProvider`
- [ ] Интеграция с `ProTreeQueryProvider`
- [ ] Тесты базовых запросов
- [ ] Тесты IQueryable интеграции

---

### 🟡 ФАЗА 3: Query Провайдеры (4-5 дней)

#### День 13: ProSqlBuilder
- [ ] Создать `Query/ProSqlBuilder.cs`
- [ ] Наследовать от `ProSqlBuilderBase`
- [ ] Реализовать `CompileFilterToSql()` для MSSQL
- [ ] Адаптировать операторы:
  - [ ] Строковые (`||` → `+`)
  - [ ] Дата/время (`NOW()` → `GETDATE()`)
  - [ ] LIKE escaping
- [ ] Реализовать PVT генерацию через CASE WHEN
- [ ] Unit тесты для всех операторов

#### День 14: ProQueryProvider - Базовые запросы
- [ ] Создать `Query/ProQueryProvider.cs`
- [ ] Реализовать базовый `ExecuteQueryAsync()`
- [ ] Реализовать `Where()` с фильтрацией
- [ ] Реализовать `OrderBy()` / `OrderByDescending()`
- [ ] Реализовать `Skip()` / `Take()` через OFFSET/FETCH
- [ ] Интеграционные тесты

#### День 15: ProQueryProvider - Агрегации
- [ ] Создать `Query/ProQueryProvider.Aggregation.cs`
- [ ] Реализовать `Count()`, `Sum()`, `Avg()`, `Min()`, `Max()`
- [ ] Реализовать `Select()` для проекций
- [ ] Реализовать `SelectMany()` для вложенных коллекций
- [ ] Тесты всех агрегатных функций

#### День 16: ProQueryProvider - Группировки
- [ ] Создать `Query/ProQueryProvider.Grouping.cs`
- [ ] Реализовать `GroupBy()` с одним полем
- [ ] Реализовать `GroupBy()` с множеством полей
- [ ] Реализовать агрегации в группах
- [ ] Реализовать `Having()` фильтрацию
- [ ] Тесты группировок

#### День 17: ProQueryProvider - Window Functions
- [ ] Создать `Query/ProQueryProvider.Window.cs`
- [ ] Реализовать `RowNumber()`
- [ ] Реализовать `Rank()` / `DenseRank()`
- [ ] Реализовать `Lead()` / `Lag()`
- [ ] Реализовать `FirstValue()` / `LastValue()`
- [ ] Реализовать PARTITION BY
- [ ] Тесты оконных функций

---

### 🟣 ФАЗА 4: Tree Query Провайдеры (2-3 дня)

#### День 18: ProTreeQueryProvider - Базовые запросы
- [ ] Создать `Query/ProTreeQueryProvider.cs`
- [ ] Создать `Query/ProTreeQueryProvider.Execute.cs`
- [ ] Реализовать `GetChildren()` с PVT
- [ ] Реализовать `GetDescendants()` с рекурсивным CTE
- [ ] Реализовать `GetAncestors()`
- [ ] Тесты древовидных запросов

#### День 19: ProTreeQueryProvider - Расширенные функции
- [ ] Создать `Query/ProTreeQueryProvider.Aggregation.cs`
- [ ] Создать `Query/ProTreeQueryProvider.Grouping.cs`
- [ ] Создать `Query/ProTreeQueryProvider.Window.cs`
- [ ] Создать `Query/ProTreeQueryProvider.Delegates.cs`
- [ ] Реализовать агрегации на деревьях
- [ ] Реализовать фильтрацию на уровнях
- [ ] Тесты сложных сценариев

---

### 🔴 ФАЗА 5: Вспомогательные компоненты (1-2 дня)

#### День 20: Сервисы и Extensions
- [ ] Создать `Services/ProRedbService.cs`
- [ ] Реализовать конструктор с DI
- [ ] Заменить провайдеры на Pro версии
- [ ] Создать `Extensions/ProServiceCollectionExtensions.cs`
- [ ] Реализовать `AddRedbMSSqlPro()`
- [ ] Создать `Extensions/MSSqlProOptionsExtensions.cs`
- [ ] Документация по регистрации

#### День 20 (продолжение): Дополнительные классы
- [ ] Создать `Query/ExpressionToSqlCompiler.cs`
- [ ] Создать `Query/PvtSqlGenerator.cs`
- [ ] Реализовать компиляцию Expression Tree в SQL
- [ ] Реализовать динамическую генерацию PVT
- [ ] Unit тесты

---

### ⚪ ФАЗА 6: Тестирование и Оптимизация (1-2 дня)

#### День 21: Интеграционное тестирование
- [ ] Создать тестовый проект `redb.MSSql.Pro.Tests`
- [ ] Настроить TestContainers для SQL Server
- [ ] Тесты CRUD операций
- [ ] Тесты сложных запросов
- [ ] Тесты миграций данных
- [ ] Тесты производительности

#### День 21 (продолжение): Оптимизация и багфиксинг
- [ ] Профилирование производительности
- [ ] Оптимизация SQL запросов
- [ ] Оптимизация PVT материализации
- [ ] Исправление найденных багов
- [ ] Code review

#### Финализация: Документация и примеры
- [ ] README.md для проекта
- [ ] Примеры использования
- [ ] Migration guide с PostgreSQL Pro
- [ ] API документация
- [ ] Release notes

---

## 📂 СТРУКТУРА ПРОЕКТА

```
redb.MSSql.Pro/
├── redb.MSSql.Pro.csproj              ← Файл проекта
│
├── sql/                               ← SQL скрипты
│   ├── 001_migrations_table.sql
│   ├── 002_scheme_metadata_cache.sql
│   ├── 003_tvp_types.sql
│   ├── 004_pvt_procedures.sql
│   ├── 005_performance_indexes.sql
│   └── 006_maintenance.sql
│
├── Sql/                               ← SQL диалект
│   └── ProMSSqlDialect.cs
│
├── Query/                             ← Query провайдеры
│   ├── ProSqlBuilder.cs
│   ├── ProQueryProvider.cs
│   ├── ProQueryProvider.Aggregation.cs
│   ├── ProQueryProvider.Grouping.cs
│   ├── ProQueryProvider.Window.cs
│   ├── ProQueryProvider.AggregateBatch.cs
│   ├── ProTreeQueryProvider.cs
│   ├── ProTreeQueryProvider.Execute.cs
│   ├── ProTreeQueryProvider.Aggregation.cs
│   ├── ProTreeQueryProvider.Grouping.cs
│   ├── ProTreeQueryProvider.Window.cs
│   ├── ProTreeQueryProvider.Delegates.cs
│   ├── ExpressionToSqlCompiler.cs
│   └── PvtSqlGenerator.cs
│
├── Providers/                         ← Провайдеры данных
│   ├── ProMSSqlObjectStorageProvider.cs
│   ├── ProMSSqlTreeProvider.cs
│   └── ProQueryableProvider.cs
│
├── Services/                          ← Сервисы
│   └── ProRedbService.cs
│
├── Extensions/                        ← DI Extensions
│   ├── ProServiceCollectionExtensions.cs
│   └── MSSqlProOptionsExtensions.cs
│
└── README.md                          ← Документация
```

---

## 🔧 ТЕХНИЧЕСКИЕ ТРЕБОВАНИЯ

### Зависимости

```xml
<ItemGroup>
  <!-- Core проекты -->
  <ProjectReference Include="..\redb.Core\redb.Core.csproj" />
  <ProjectReference Include="..\redb.Core.Pro\redb.Core.Pro.csproj" />
  <ProjectReference Include="..\redb.MSSql\redb.MSSql.csproj" />
  
  <!-- NuGet пакеты -->
  <PackageReference Include="Microsoft.EntityFrameworkCore" Version="9.0.8" />
  <PackageReference Include="Microsoft.EntityFrameworkCore.SqlServer" Version="9.0.8" />
  <PackageReference Include="Microsoft.Extensions.Caching.Memory" Version="9.0.8" />
  <PackageReference Include="Microsoft.Data.SqlClient" Version="5.2.0" />
</ItemGroup>
```

### Минимальная версия SQL Server

**Рекомендуется: SQL Server 2016 или выше**

Причины:
- JSON функции (2016+)
- STRING_AGG (2017+, опционально)
- Window Functions полная поддержка (2012+)
- OFFSET/FETCH (2012+)

**Минимальная поддержка: SQL Server 2012**

Ограничения:
- Нет JSON функций (нужны обходные пути)
- Нет STRING_AGG (использовать FOR XML PATH)

---

## ✅ КРИТЕРИИ ГОТОВНОСТИ

### Фаза 1: SQL Инфраструктура
- [ ] Все SQL скрипты выполняются без ошибок
- [ ] Созданы все необходимые таблицы
- [ ] Созданы все TVP типы
- [ ] Созданы индексы
- [ ] ProMSSqlDialect реализует ISqlDialectPro
- [ ] Все методы диалекта возвращают валидный SQL

### Фаза 2: Базовые Провайдеры
- [ ] ProMSSqlObjectStorageProvider работает
- [ ] ProMSSqlTreeProvider работает
- [ ] Параллельное сравнение деревьев функционирует
- [ ] PVT материализация работает
- [ ] Все базовые тесты проходят

### Фаза 3: Query Провайдеры
- [ ] ProQueryProvider реализован полностью
- [ ] Все типы запросов работают (Where, OrderBy, GroupBy, etc.)
- [ ] Агрегации работают корректно
- [ ] Window functions работают
- [ ] Производительность приемлема

### Фаза 4: Tree Query Провайдеры
- [ ] ProTreeQueryProvider реализован
- [ ] Древовидные запросы работают
- [ ] Рекурсивные CTE работают
- [ ] Агрегации на деревьях работают

### Фаза 5: Вспомогательные компоненты
- [ ] ProRedbService регистрируется в DI
- [ ] Extensions работают корректно
- [ ] Компиляция Expression Tree в SQL работает
- [ ] Документация написана

### Фаза 6: Тестирование
- [ ] Все unit тесты проходят (>90% покрытие)
- [ ] Все интеграционные тесты проходят
- [ ] Нет критических багов
- [ ] Производительность соответствует ожиданиям
- [ ] Документация полная

---

## 🎯 КЛЮЧЕВЫЕ ЗАДАЧИ ПО ПРИОРИТЕТУ

### P0 - Критические (должны быть в первой версии)
1. SQL скрипты миграций
2. ProMSSqlDialect
3. ProMSSqlObjectStorageProvider (базовый CRUD)
4. ProMSSqlTreeProvider (базовая навигация)
5. ProSqlBuilder (базовые запросы)
6. ProQueryProvider (Where, OrderBy, Skip, Take)
7. ProRedbService (регистрация)

### P1 - Важные (вторая итерация)
8. ProQueryProvider.Aggregation (Count, Sum, Avg, Min, Max)
9. ProQueryProvider.Grouping (GroupBy, Having)
10. ProTreeQueryProvider (GetChildren, GetDescendants)
11. PVT материализация через CASE WHEN
12. Интеграционные тесты

### P2 - Желательные (третья итерация)
13. ProQueryProvider.Window (ROW_NUMBER, RANK, LAG, LEAD)
14. ProTreeQueryProvider расширенные функции
15. Хранимые процедуры PVT
16. Оптимизация производительности
17. Полная документация

### P3 - Опциональные (будущие версии)
18. Scheme metadata cache
19. Дополнительные индексы
20. Процедуры обслуживания
21. Advanced JSON support
22. Полнотекстовый поиск

---

## 🚀 QUICK START

### Шаг 1: Создание проекта

```bash
cd c:\Work\redb_code\csharp\redb
dotnet new classlib -n redb.MSSql.Pro -f net9.0
cd redb.MSSql.Pro
```

### Шаг 2: Добавление в solution

```bash
cd ..
dotnet sln redb.sln add redb.MSSql.Pro/redb.MSSql.Pro.csproj
```

### Шаг 3: Добавление зависимостей

```bash
cd redb.MSSql.Pro
dotnet add reference ../redb.Core/redb.Core.csproj
dotnet add reference ../redb.Core.Pro/redb.Core.Pro.csproj
dotnet add reference ../redb.MSSql/redb.MSSql.csproj

dotnet add package Microsoft.EntityFrameworkCore --version 9.0.8
dotnet add package Microsoft.EntityFrameworkCore.SqlServer --version 9.0.8
dotnet add package Microsoft.Extensions.Caching.Memory --version 9.0.8
```

### Шаг 4: Создание структуры папок

```bash
mkdir sql
mkdir Sql
mkdir Query
mkdir Providers
mkdir Services
mkdir Extensions
```

### Шаг 5: Начало работы

Начинаем с SQL скриптов в папке `sql/`!

---

## 📊 МЕТРИКИ УСПЕХА

### Производительность
- PVT материализация: < 100ms для 100 объектов
- Batch загрузка: < 500ms для 1000 объектов
- Агрегации: < 200ms для 10000 записей
- Window functions: < 300ms для 10000 записей

### Качество кода
- Unit test coverage: > 90%
- Integration test coverage: > 80%
- Code review: все компоненты
- Documentation coverage: 100%

### Совместимость
- Все примеры из PostgreSQL Pro работают
- Все тесты из PostgreSQL Pro адаптированы
- API совместимость: 95%+

---

## 📞 КОНТРОЛЬНЫЕ ТОЧКИ

### После Фазы 1 (День 3)
- ✅ SQL скрипты работают
- ✅ Диалект реализован
- 🎯 **Готовность: 20%**

### После Фазы 2 (День 6)
- ✅ Базовые провайдеры работают
- ✅ CRUD операции функционируют
- 🎯 **Готовность: 40%**

### После Фазы 3 (День 11)
- ✅ Все Query провайдеры реализованы
- ✅ Агрегации, группировки, window functions работают
- 🎯 **Готовность: 70%**

### После Фазы 4 (День 13)
- ✅ Tree провайдеры реализованы
- ✅ Древовидные запросы работают
- 🎯 **Готовность: 85%**

### После Фазы 5 (День 15)
- ✅ Все компоненты реализованы
- ✅ DI регистрация работает
- 🎯 **Готовность: 95%**

### После Фазы 6 (День 18)
- ✅ Все тесты проходят
- ✅ Документация готова
- 🎯 **Готовность: 100%** 🎉

---

## 🎓 ОБУЧЕНИЕ И РЕСУРСЫ

### Необходимые знания
1. C# 12, .NET 9
2. MS SQL Server (T-SQL)
3. Entity Framework Core
4. LINQ и Expression Trees
5. Dependency Injection

### Полезные ресурсы
- redb.Postgres.Pro - референсная реализация
- [SQL Server Documentation](https://docs.microsoft.com/sql/)
- [EF Core Documentation](https://docs.microsoft.com/ef/core/)
- [SQL Comparison Guide](./SQL_DIALECT_COMPARISON.md)

---

## ✨ НАЧИНАЕМ!

**Текущая задача:** Реализация SQL скриптов в папке `sql/`

**Следующий файл для создания:** 
```
redb.MSSql.Pro/sql/001_migrations_table.sql
```

Готовы начать? 🚀

