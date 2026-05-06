using redb.Core.Data;
using redb.Core.Extensions;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Core.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace redb.Core.Providers.Base
{
    /// <summary>
    /// Base class for validation provider.
    /// Contains common validation logic for schema and type checking.
    /// </summary>
    public abstract class ValidationProviderBase : IValidationProvider
    {
        protected readonly IRedbContext Context;
        protected readonly ISqlDialect Sql;
        protected readonly ILogger? Logger;
        
        private List<SupportedType>? _supportedTypesCache;
        private Dictionary<Type, string>? _csharpToRedbTypeCache;

        protected ValidationProviderBase(
            IRedbContext context, 
            ISqlDialect sql,
            ILogger? logger = null)
        {
            Context = context ?? throw new ArgumentNullException(nameof(context));
            Sql = sql ?? throw new ArgumentNullException(nameof(sql));
            Logger = logger;
        }

        public async Task<List<SupportedType>> GetSupportedTypesAsync()
        {
            if (_supportedTypesCache != null)
                return _supportedTypesCache;

            var dbTypes = await Context.QueryAsync<RedbType>(Sql.Validation_SelectAllTypes());
            
            _supportedTypesCache = dbTypes.Select(t => new SupportedType
            {
                Id = t.Id,
                Name = t.Name,
                DbType = t.DbType ?? "String",
                DotNetType = t.Type1 ?? "string",
                SupportsArrays = t.Name != "Object" && t.Name != "ListItem",
                SupportsNullability = t.Name != "Object",
                Description = GetTypeDescription(t.Name)
            }).ToList();

            return _supportedTypesCache;
        }

        public async Task<ValidationIssue?> ValidateTypeAsync(Type csharpType, string propertyName)
        {
            var supportedTypes = await GetSupportedTypesAsync();
            var underlyingType = Nullable.GetUnderlyingType(csharpType) ?? csharpType;
            
            var mappedTypeName = await MapCSharpTypeToRedbTypeAsync(underlyingType);
            var supportedType = supportedTypes.FirstOrDefault(t => t.Name == mappedTypeName);
            
            if (supportedType == null)
            {
                return new ValidationIssue
                {
                    Severity = ValidationSeverity.Error,
                    PropertyName = propertyName,
                    Message = $"Type '{csharpType.Name}' is not supported in REDB",
                    SuggestedFix = $"Use one of supported types: {string.Join(", ", supportedTypes.Where(t => t.SupportsNullability).Select(t => t.DotNetType))}"
                };
            }

            if (underlyingType == typeof(decimal))
            {
                return new ValidationIssue
                {
                    Severity = ValidationSeverity.Warning,
                    PropertyName = propertyName,
                    Message = "Type 'decimal' will be converted to 'double', possible precision loss",
                    SuggestedFix = "Consider using 'double' directly or store as 'string' for precise calculations"
                };
            }

            if (underlyingType == typeof(float))
            {
                return new ValidationIssue
                {
                    Severity = ValidationSeverity.Info,
                    PropertyName = propertyName,
                    Message = "Type 'float' will be converted to 'double'",
                    SuggestedFix = "Consider using 'double' directly"
                };
            }

            return null;
        }

        public async Task<SchemaValidationResult> ValidateSchemaAsync<TProps>(string schemeName, bool strictDeleteExtra = true) where TProps : class
        {
            var result = new SchemaValidationResult { IsValid = true };
            var properties = typeof(TProps).GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => !p.ShouldIgnoreForRedb())
                .ToArray();
            var nullabilityContext = new NullabilityInfoContext();

            foreach (var property in properties)
            {
                var typeIssue = await ValidateTypeAsync(property.PropertyType, property.Name);
                if (typeIssue != null)
                {
                    result.Issues.Add(typeIssue);
                    if (typeIssue.Severity == ValidationSeverity.Error)
                        result.IsValid = false;
                }

                var nullabilityInfo = nullabilityContext.Create(property);
                var isArray = IsArrayType(property.PropertyType);
                var baseType = isArray ? GetArrayElementType(property.PropertyType) : property.PropertyType;
                var isRequired = nullabilityInfo.WriteState != NullabilityState.Nullable && 
                                Nullable.GetUnderlyingType(baseType) == null;

                var constraintIssue = ValidatePropertyConstraints(property.PropertyType, property.Name, isRequired, isArray);
                if (constraintIssue != null)
                {
                    result.Issues.Add(constraintIssue);
                    if (constraintIssue.Severity == ValidationSeverity.Error)
                        result.IsValid = false;
                }
            }

            var existingScheme = await Context.QueryFirstOrDefaultAsync<RedbScheme>(
                Sql.Validation_SelectSchemeByName(), schemeName);
            
            if (existingScheme != null)
            {
                result.ChangeReport = await AnalyzeSchemaChangesAsync<TProps>(existingScheme.Id);
                if (result.ChangeReport.HasBreakingChanges && strictDeleteExtra)
                {
                    result.Issues.Add(new ValidationIssue
                    {
                        Severity = ValidationSeverity.Warning,
                        PropertyName = "Schema",
                        Message = "Breaking schema changes detected with strictDeleteExtra=true",
                        SuggestedFix = "Check change report or set strictDeleteExtra=false"
                    });
                }
            }

            return result;
        }

        public async Task<SchemaValidationResult> ValidateSchemaAsync<TProps>(IRedbScheme scheme, bool strictDeleteExtra = true) where TProps : class
        {
            return await ValidateSchemaAsync<TProps>(scheme.Name, strictDeleteExtra);
        }

        public async Task<SchemaChangeReport> AnalyzeSchemaChangesAsync<TProps>(IRedbScheme scheme) where TProps : class
        {
            return await AnalyzeSchemaChangesAsync<TProps>(scheme.Id);
        }

        public async Task<SchemaChangeReport> AnalyzeSchemaChangesAsync<TProps>(long schemeId) where TProps : class
        {
            var report = new SchemaChangeReport();
            var properties = typeof(TProps).GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => !p.ShouldIgnoreForRedb())
                .ToArray();
            var nullabilityContext = new NullabilityInfoContext();

            var existingStructures = await Context.QueryAsync<RedbStructure>(
                Sql.Validation_SelectStructuresBySchemeId(), schemeId);
            
            var allTypes = await Context.QueryAsync<RedbType>(Sql.Validation_SelectAllTypes());
            var typesDict = allTypes.ToDictionary(t => t.Id);

            var existingNames = existingStructures.Select(s => s.Name).ToHashSet();
            var newNames = properties.Select(p => p.Name).ToHashSet();

            // Added properties
            foreach (var property in properties.Where(p => !existingNames.Contains(p.Name)))
            {
                report.Changes.Add(new StructureChange
                {
                    Type = ChangeType.Added,
                    PropertyName = property.Name,
                    NewValue = $"{property.PropertyType.Name} ({await MapCSharpTypeToRedbTypeAsync(property.PropertyType)})",
                    IsBreaking = false
                });
            }

            // Removed properties
            foreach (var structure in existingStructures.Where(s => !newNames.Contains(s.Name)))
            {
                var typeName = typesDict.TryGetValue(structure.IdType, out var t) ? t.Name : "Unknown";
                report.Changes.Add(new StructureChange
                {
                    Type = ChangeType.Removed,
                    PropertyName = structure.Name,
                    OldValue = typeName,
                    IsBreaking = true
                });
                report.HasBreakingChanges = true;
            }

            // Modified properties
            foreach (var property in properties.Where(p => existingNames.Contains(p.Name)))
            {
                var existingStructure = existingStructures.First(s => s.Name == property.Name);
                var structureTypeName = typesDict.TryGetValue(existingStructure.IdType, out var st) ? st.Name : "Unknown";
                var nullabilityInfo = nullabilityContext.Create(property);
                var isArray = IsArrayType(property.PropertyType);
                var baseType = isArray ? GetArrayElementType(property.PropertyType) : property.PropertyType;
                var isRequired = nullabilityInfo.WriteState != NullabilityState.Nullable && 
                                Nullable.GetUnderlyingType(baseType) == null;
                var newTypeName = await MapCSharpTypeToRedbTypeAsync(baseType);

                // Type change check
                if (structureTypeName != newTypeName)
                {
                    var change = new StructureChange
                    {
                        Type = ChangeType.TypeChanged,
                        PropertyName = property.Name,
                        OldValue = structureTypeName,
                        NewValue = newTypeName,
                        IsBreaking = !AreTypesCompatible(structureTypeName, newTypeName)
                    };
                    report.Changes.Add(change);
                    if (change.IsBreaking)
                        report.HasBreakingChanges = true;
                }

                // Nullability change check
                if (existingStructure.AllowNotNull != isRequired)
                {
                    var change = new StructureChange
                    {
                        Type = ChangeType.NullabilityChanged,
                        PropertyName = property.Name,
                        OldValue = existingStructure.AllowNotNull == true ? "required" : "optional",
                        NewValue = isRequired ? "required" : "optional",
                        IsBreaking = isRequired && existingStructure.AllowNotNull != true
                    };
                    report.Changes.Add(change);
                    if (change.IsBreaking)
                        report.HasBreakingChanges = true;
                }

                // Array change check
                var wasArray = existingStructure.CollectionType != null;
                if (wasArray != isArray)
                {
                    var change = new StructureChange
                    {
                        Type = ChangeType.ArrayChanged,
                        PropertyName = property.Name,
                        OldValue = wasArray ? "array" : "single",
                        NewValue = isArray ? "array" : "single",
                        IsBreaking = true
                    };
                    report.Changes.Add(change);
                    report.HasBreakingChanges = true;
                }
            }

            return report;
        }

        public ValidationIssue? ValidatePropertyConstraints(Type propertyType, string propertyName, bool isRequired, bool isArray)
        {
            if (isArray)
            {
                var elementType = GetArrayElementType(propertyType);
                if (typeof(RedbObject<>).IsAssignableFrom(elementType))
                {
                    return new ValidationIssue
                    {
                        Severity = ValidationSeverity.Warning,
                        PropertyName = propertyName,
                        Message = "Object arrays require special attention during serialization",
                        SuggestedFix = "Ensure objects in array have correct IDs"
                    };
                }
            }

            if (isRequired && propertyType.IsClass && propertyType != typeof(string))
            {
                return new ValidationIssue
                {
                    Severity = ValidationSeverity.Warning,
                    PropertyName = propertyName,
                    Message = "Required reference types (except string) may cause deserialization issues",
                    SuggestedFix = "Consider making the field nullable or provide a default value"
                };
            }

            return null;
        }

        #region Helper Methods

        private async Task<string> MapCSharpTypeToRedbTypeAsync(Type type)
        {
            var underlyingType = Nullable.GetUnderlyingType(type) ?? type;
            
            if (_csharpToRedbTypeCache == null)
            {
                await InitializeCSharpToRedbTypeMappingAsync();
            }

            if (_csharpToRedbTypeCache!.TryGetValue(underlyingType, out var exactMatch))
                return exactMatch;

            if (underlyingType.IsGenericType && underlyingType.GetGenericTypeDefinition() == typeof(RedbObject<>))
                return "Object";

            return "String";
        }

        private async Task InitializeCSharpToRedbTypeMappingAsync()
        {
            var allTypes = await Context.QueryAsync<RedbType>(Sql.Validation_SelectAllTypes());
            _csharpToRedbTypeCache = new Dictionary<Type, string>();

            // Sort by ID to ensure base types (String, Long, etc.) are processed first
            foreach (var dbType in allTypes.OrderBy(t => t.Id))
            {
                var dotNetTypeName = dbType.Type1;
                if (string.IsNullOrEmpty(dotNetTypeName))
                    continue;

                var csharpType = MapStringToType(dotNetTypeName);
                // Don't overwrite base type mapping with derived types
                if (csharpType != null && !_csharpToRedbTypeCache.ContainsKey(csharpType))
                {
                    _csharpToRedbTypeCache[csharpType] = dbType.Name;
                }
            }
        }

        private static Type? MapStringToType(string typeName) => typeName switch
        {
            "string" => typeof(string),
            "int" => typeof(int),
            "long" => typeof(long),
            "short" => typeof(short),
            "byte" => typeof(byte),
            "double" => typeof(double),
            "float" => typeof(float),
            "decimal" => typeof(decimal),
            "Numeric" => typeof(decimal),
            "boolean" => typeof(bool),
            "DateTime" => typeof(DateTimeOffset),
            "DateTimeOffset" => typeof(DateTimeOffset),
            "Guid" => typeof(Guid),
            "byte[]" => typeof(byte[]),
            "char" => typeof(char),
            "TimeSpan" => typeof(TimeSpan),
#if NET6_0_OR_GREATER
            "DateOnly" => typeof(DateOnly),
            "TimeOnly" => typeof(TimeOnly),
#endif
            "RedbObjectRow" => typeof(RedbObject<>),
            "_RListItem" => null,
            "Enum" => typeof(Enum),
            _ => null
        };

        private static bool IsArrayType(Type type) =>
            type.IsArray || 
            (type.IsGenericType && 
             (type.GetGenericTypeDefinition() == typeof(List<>) ||
              type.GetGenericTypeDefinition() == typeof(IList<>) ||
              type.GetGenericTypeDefinition() == typeof(ICollection<>) ||
              type.GetGenericTypeDefinition() == typeof(IEnumerable<>)));

        private static Type GetArrayElementType(Type arrayType)
        {
            if (arrayType.IsArray)
                return arrayType.GetElementType()!;
            
            if (arrayType.IsGenericType)
                return arrayType.GetGenericArguments()[0];
            
            return typeof(object);
        }

        private static bool AreTypesCompatible(string oldType, string newType)
        {
            var compatibleMappings = new Dictionary<string, HashSet<string>>
            {
                ["String"] = ["String"],
                ["Long"] = ["Long", "Double"],
                ["Double"] = ["Double"],
                ["Boolean"] = ["Boolean"],
                ["DateTime"] = ["DateTime", "String"],
                ["DateTimeOffset"] = ["DateTimeOffset", "String"],
                ["DateOnly"] = ["DateOnly", "String"],
                ["TimeOnly"] = ["TimeOnly", "String"],
                ["TimeSpan"] = ["TimeSpan", "String"],
                ["Guid"] = ["Guid", "String"],
                ["ByteArray"] = ["ByteArray", "String"],
                ["Object"] = ["Object", "Long"]
            };

            return compatibleMappings.TryGetValue(oldType, out var compatible) && 
                   compatible.Contains(newType);
        }

        private static string GetTypeDescription(string typeName) => typeName switch
        {
            "String" => "String values (text up to 850 chars in _String)",
            "Long" => "Integer numbers (int, long)",
            "Double" => "Floating point numbers (double, float, decimal)",
            "Boolean" => "Boolean values (true/false)",
            "DateTime" => "Date and time",
            "DateTimeOffset" => "Date and time with time zone",
            "Guid" => "Unique identifiers",
            "ByteArray" => "Binary data",
            "Object" => "References to other REDB objects",
            "ListItem" => "List items",
            "Text" => "Long text values (deprecated, use String)",
            _ => "Unknown type"
        };

        #endregion
    }
}

