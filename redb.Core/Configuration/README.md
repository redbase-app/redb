# 🔧 RedbService Configuration via IConfiguration

Complete integration of the RedbService configuration system with Microsoft.Extensions.Configuration to support `appsettings.json`, environment variables, and other configuration sources.

## 🚀 Quick Start

### 1. Basic setup in Program.cs

```csharp
var builder = WebApplication.CreateBuilder(args);

// Automatic loading from appsettings.json
builder.Services.AddRedbServiceConfiguration(builder.Configuration);

var app = builder.Build();
```

### 2. Basic appsettings.json

```json
{
  "RedbService": {
    "DefaultCheckPermissionsOnLoad": false,
    "DefaultCheckPermissionsOnSave": false,
    "DefaultCheckPermissionsOnDelete": true,
    "DefaultLoadDepth": 10,
    "EnableSchemaMetadataCache": true
  }
}
```

## 📋 Configuration Methods

### 1. Using predefined profiles

```json
{
  "RedbService": {
    "Profile": "Development",
    "Overrides": {
      "DefaultLoadDepth": 15,
      "EnableSchemaMetadataCache": false
    }
  }
}
```

**Available profiles:**
- `Development` - for development
- `Production` - for production
- `HighPerformance` - for high performance
- `BulkOperations` - for bulk operations
- `Debug` - for debugging
- `IntegrationTesting` - for integration testing
- `DataMigration` - for data migration

### 2. Full configuration

```json
{
  "RedbService": {
    "IdResetStrategy": "AutoCreateNewOnSave",
    "MissingObjectStrategy": "AutoSwitchToInsert",
    "DefaultCheckPermissionsOnLoad": false,
    "DefaultCheckPermissionsOnSave": false,
    "DefaultCheckPermissionsOnDelete": true,
    "DefaultLoadDepth": 10,
    "DefaultMaxTreeDepth": 50,
    "EnableSchemaMetadataCache": true,
    "SchemaMetadataCacheLifetimeMinutes": 30,
    "EnableSchemaValidation": true,
    "EnableDataValidation": true,
    "AutoSetModifyDate": true,
    "AutoRecomputeHash": true,
    "DefaultSecurityPriority": "Context",
    "SystemUserId": 0,
    "JsonOptions": {
      "WriteIndented": false,
      "UseUnsafeRelaxedJsonEscaping": true
    }
  }
}
```

### 3. Environment variables

```bash
# Override via environment variables
REDBSERVICE__DEFAULTCHECKPERMISSIONSONLOAD=true
REDBSERVICE__DEFAULTLOADDEPTH=5
REDBSERVICE__ENABLESCHEMAMETADATACACHE=false
REDBSERVICE__JSONOPTIONS__WRITEINDENTED=true
```

## 🛠️ Registration methods in DI

### 1. Basic registration

```csharp
// Load from appsettings.json
services.AddRedbServiceConfiguration(configuration);

// With validation
services.AddValidatedRedbServiceConfiguration(configuration);
```

### 2. Combined setup

```csharp
// appsettings.json + programmatic setup
services.AddRedbServiceConfiguration(configuration, builder =>
{
    builder.WithLoadDepth(5)
           .WithStrictSecurity();
});
```

### 3. Predefined profiles

```csharp
// Using a profile
services.AddRedbServiceConfiguration("Production");

// Profile + additional setup
services.AddRedbServiceConfiguration("Development", builder =>
{
    builder.WithoutCache()
           .WithPrettyJson();
});
```

### 4. Programmatic configuration

```csharp
// Via builder only
services.AddRedbServiceConfiguration(builder =>
{
    builder.ForProduction()
           .WithLoadDepth(3)
           .WithMetadataCache(enabled: true, lifetimeMinutes: 120);
});
```

### 5. Change monitoring

```csharp
// Hot-reload configuration
services.AddRedbServiceConfigurationMonitoring(configuration);

// Usage in a service
public class MyService
{
    private readonly IRedbServiceConfigurationMonitor _configMonitor;
    
    public MyService(IRedbServiceConfigurationMonitor configMonitor)
    {
        _configMonitor = configMonitor;
        _configMonitor.ConfigurationChanged += OnConfigChanged;
    }
    
    private void OnConfigChanged(RedbServiceConfiguration newConfig)
    {
        // React to configuration changes
    }
}
```

## 🔍 Configuration validation

### 1. Automatic validation

```csharp
// Validation during registration
services.AddValidatedRedbServiceConfiguration(configuration, throwOnValidationError: true);
```

### 2. Validation for specific scenarios

```csharp
// Validation for production
services.AddSingleton<IValidateOptions<RedbServiceConfiguration>>(
    new ScenarioBasedConfigurationValidator(ConfigurationScenario.Production));
```

### 3. Validation with auto-fix

```csharp
// Automatic correction of critical errors
services.AddSingleton<IValidateOptions<RedbServiceConfiguration>>(
    new RedbServiceConfigurationValidatorWithAutoFix(autoFixCriticalErrors: true));
```

## 🌍 Configuration for different environments

### appsettings.json (base)
```json
{
  "RedbService": {
    "Profile": "Development"
  }
}
```

### appsettings.Production.json
```json
{
  "RedbService": {
    "Profile": "Production",
    "Overrides": {
      "DefaultLoadDepth": 3,
      "SchemaMetadataCacheLifetimeMinutes": 120
    }
  }
}
```

### appsettings.Development.json
```json
{
  "RedbService": {
    "Profile": "Development",
    "Overrides": {
      "EnableSchemaMetadataCache": false,
      "JsonOptions": {
        "WriteIndented": true
      }
    }
  }
}
```

## 📊 Usage in code

### 1. Via IOptions

```csharp
public class MyService
{
    private readonly RedbServiceConfiguration _config;
    
    public MyService(IOptions<RedbServiceConfiguration> options)
    {
        _config = options.Value;
    }
}
```

### 2. Via IOptionsMonitor (with hot-reload)

```csharp
public class MyService
{
    private readonly IOptionsMonitor<RedbServiceConfiguration> _configMonitor;
    
    public MyService(IOptionsMonitor<RedbServiceConfiguration> configMonitor)
    {
        _configMonitor = configMonitor;
    }
    
    public void DoSomething()
    {
        var currentConfig = _configMonitor.CurrentValue;
        // Use current configuration
    }
}
```

### 3. Direct injection

```csharp
public class MyService
{
    private readonly RedbServiceConfiguration _config;
    
    public MyService(RedbServiceConfiguration config)
    {
        _config = config;
    }
}
```

## ⚙️ Configuration settings

### Object processing strategies

| Parameter | Values | Description |
|----------|----------|----------|
| `IdResetStrategy` | `Manual`, `AutoResetOnDelete`, `AutoCreateNewOnSave` | ID reset strategy after deletion |
| `MissingObjectStrategy` | `ThrowException`, `AutoSwitchToInsert`, `ReturnNull` | Handling non-existent objects |

### Security settings

| Parameter | Type | Description |
|----------|-----|----------|
| `DefaultCheckPermissionsOnLoad` | `bool` | Check permissions on load |
| `DefaultCheckPermissionsOnSave` | `bool` | Check permissions on save |
| `DefaultCheckPermissionsOnDelete` | `bool` | Check permissions on delete |
| `DefaultSecurityPriority` | `System`, `Explicit`, `Context` | Security context priority |
| `SystemUserId` | `long` | System user ID |

### Performance settings

| Parameter | Type | Description |
|----------|-----|----------|
| `DefaultLoadDepth` | `int` | Object loading depth |
| `DefaultMaxTreeDepth` | `int` | Maximum tree depth |
| `EnableSchemaMetadataCache` | `bool` | Enable metadata caching |
| `SchemaMetadataCacheLifetimeMinutes` | `int` | Cache lifetime in minutes |

### Validation settings

| Parameter | Type | Description |
|----------|-----|----------|
| `EnableSchemaValidation` | `bool` | Schema validation |
| `EnableDataValidation` | `bool` | Data validation |

### JSON settings

| Parameter | Type | Description |
|----------|-----|----------|
| `JsonOptions.WriteIndented` | `bool` | Formatted JSON |
| `JsonOptions.UseUnsafeRelaxedJsonEscaping` | `bool` | Simplified JSON escaping |

## 🎯 Examples for different scenarios

See the `appsettings.examples.json` file for detailed configuration examples for various usage scenarios.

## 🚨 Solving the deleted objects problem

```json
{
  "RedbService": {
    "IdResetStrategy": "AutoResetOnDelete",
    "MissingObjectStrategy": "AutoSwitchToInsert"
  }
}
```

This configuration automatically solves the problem when attempting to resave an object after it has been deleted from the database causes an error.

## 🔧 Debugging configuration

```csharp
// Get description of current configuration
var description = configuration.GetRedbServiceConfigurationDescription();
Console.WriteLine($"RedbService configuration: {description}");

// Check validity
var config = configuration.GetRedbServiceConfiguration();
var validation = ConfigurationValidator.Validate(config);
if (!validation.IsValid)
{
    foreach (var message in validation.GetAllMessages())
    {
        Console.WriteLine(message);
    }
}
```

Integration with `IConfiguration` makes RedbService much more flexible and convenient for enterprise use! 🚀