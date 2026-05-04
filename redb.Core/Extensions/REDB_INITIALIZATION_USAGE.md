# 🚀 Использование RedbServiceInitializationExtensions

## 📖 Описание

Единая точка входа для инициализации REDB системы при старте приложения.

### ✨ Что делает `InitializeAsync()`?

1. **Автоматическая синхронизация схем** - находит все типы с `[RedbScheme]` атрибутом и синхронизирует их схемы
2. **Инициализация RedbObjectFactory** - настраивает фабрику для создания объектов
3. **Инициализация AutomaticTypeRegistry** - регистрирует типы для полиморфных операций

## 🎯 Использование

### ✅ Вариант 1: Автоматическая инициализация (рекомендуется)

Сканирует все загруженные сборки:

```csharp
// В Program.cs / Startup.cs
var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();

// ✅ ВСЁ В ОДНОМ ВЫЗОВЕ!
await redb.InitializeAsync();
```

### ✅ Вариант 2: Указание конкретных сборок

Для оптимизации производительности указывайте только нужные сборки:

```csharp
// Инициализация только из текущей сборки
await redb.InitializeAsync(Assembly.GetExecutingAssembly());

// Инициализация из нескольких сборок
await redb.InitializeAsync(
    typeof(MyModel).Assembly,
    typeof(AnotherModel).Assembly
);
```

### ✅ Вариант 3: Только синхронизация схем

Если нужно только синхронизировать схемы:

```csharp
// Автоматическая синхронизация всех схем
await redb.AutoSyncSchemesAsync();

// Синхронизация из конкретных сборок
await redb.AutoSyncSchemesAsync(typeof(MyModel).Assembly);
```

## 📋 Примеры в реальных проектах

### ASP.NET Core (Program.cs)

```csharp
var builder = WebApplication.CreateBuilder(args);

// Регистрация сервисов
builder.Services.AddDbContext<RedbContext>(options => 
    options.UseNpgsql(connectionString));
builder.Services.AddScoped<IRedbService, RedbService>();

var app = builder.Build();

// 🚀 Инициализация REDB при старте
using (var scope = app.Services.CreateScope())
{
    var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
    await redb.InitializeAsync();
}

app.Run();
```

### Console Application

```csharp
static async Task Main(string[] args)
{
    var services = new ServiceCollection();
    
    // Настройка DI
    services.AddDbContext<RedbContext>(options => 
        options.UseNpgsql(connectionString));
    services.AddScoped<IRedbService, RedbService>();
    
    var provider = services.BuildServiceProvider();
    
    // 🚀 Инициализация REDB
    var redb = provider.GetRequiredService<IRedbService>();
    await redb.InitializeAsync();
    
    // Теперь можно работать с REDB
    var employee = await RedbObjectFactory.CreateAsync(new EmployeeProps 
    { 
        Name = "Иван Иванов" 
    });
    
    await redb.SaveAsync(employee);
}
```

### С явным указанием сборок (для больших проектов)

```csharp
// Для оптимизации - указываем только сборки с моделями
await redb.InitializeAsync(
    typeof(Company).Assembly,    // Модели компаний
    typeof(Employee).Assembly,   // Модели сотрудников
    typeof(Project).Assembly     // Модели проектов
);
```

## 🔧 Особенности реализации

### 1. Параллельная синхронизация
Все схемы синхронизируются параллельно для максимальной производительности:
```csharp
await Task.WhenAll(tasks); // Все схемы одновременно
```

### 2. Обработка ошибок
Проблемные сборки автоматически игнорируются:
```csharp
catch (ReflectionTypeLoadException ex)
{
    // Возвращаем типы которые удалось загрузить
    return ex.Types.Where(t => t != null)!;
}
```

### 3. Универсальность
Работает с любой реализацией IRedbService:
- PostgreSQL (redb.Postgres)
- MS SQL Server (redb.MSSql)
- SQLite (redb.Core.SQLite)

### 4. Поддержка .NET Framework и .NET 5+
```csharp
#if NET5_0_OR_GREATER
    return AssemblyLoadContext.Default.Assemblies;
#else
    return AppDomain.CurrentDomain.GetAssemblies();
#endif
```

## ⚡ Производительность

### До (3 отдельных вызова):
```csharp
await redb.AutoSyncSchemesAsync();              // ~500ms
RedbObjectFactory.Initialize(redb);             // ~10ms
await treeProvider.InitializeTypeRegistryAsync(); // ~200ms
// Итого: ~710ms последовательно
```

### После (1 вызов с параллельностью):
```csharp
await redb.InitializeAsync();
// Итого: ~500ms (схемы синхронизируются параллельно!)
```

## 🎯 Преимущества

✅ **Простота** - 1 вызов вместо 3-4 строк кода  
✅ **Производительность** - параллельная синхронизация схем  
✅ **Универсальность** - работает с любой БД  
✅ **Гибкость** - можно указать конкретные сборки  
✅ **Надежность** - автоматическая обработка ошибок  
✅ **Совместимость** - .NET Framework и .NET 5+/9

## 📚 См. также

- `RedbObjectFactory` - фабрика для создания объектов
- `AutomaticTypeRegistry` - реестр типов для полиморфизма
- `RedbSchemeAttribute` - атрибут для пометки схем
- `ISchemeSyncProvider.SyncSchemeAsync<T>()` - синхронизация отдельной схемы

