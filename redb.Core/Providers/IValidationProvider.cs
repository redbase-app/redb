using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using redb.Core.Models.Contracts;

namespace redb.Core.Providers
{
    /// <summary>
    /// Schema validation result.
    /// </summary>
    public class SchemaValidationResult
    {
        public bool IsValid { get; set; }
        public List<ValidationIssue> Issues { get; set; } = new();
        public SchemaChangeReport? ChangeReport { get; set; }
    }

    /// <summary>
    /// Validation issue.
    /// </summary>
    public class ValidationIssue
    {
        public ValidationSeverity Severity { get; set; }
        public string PropertyName { get; set; } = string.Empty;
        public string Message { get; set; } = string.Empty;
        public string? SuggestedFix { get; set; }
    }

    /// <summary>
    /// Validation issue severity level.
    /// </summary>
    public enum ValidationSeverity
    {
        Info,
        Warning,
        Error
    }

    /// <summary>
    /// Schema change report.
    /// </summary>
    public class SchemaChangeReport
    {
        public List<StructureChange> Changes { get; set; } = new();
        public bool HasBreakingChanges { get; set; }
    }

    /// <summary>
    /// Structure change.
    /// </summary>
    public class StructureChange
    {
        public ChangeType Type { get; set; }
        public string PropertyName { get; set; } = string.Empty;
        public string? OldValue { get; set; }
        public string? NewValue { get; set; }
        public bool IsBreaking { get; set; }
    }

    /// <summary>
    /// Change type.
    /// </summary>
    public enum ChangeType
    {
        Added,
        Removed,
        Modified,
        TypeChanged,
        NullabilityChanged,
        ArrayChanged
    }

    /// <summary>
    /// Supported type information.
    /// </summary>
    public class SupportedType
    {
        public string Name { get; set; } = string.Empty;
        public string DbType { get; set; } = string.Empty;
        public string DotNetType { get; set; } = string.Empty;
        public long Id { get; set; }
        public bool SupportsArrays { get; set; } = true;
        public bool SupportsNullability { get; set; } = true;
        public string? Description { get; set; }
    }

    /// <summary>
    /// Provider for schema and type validation.
    /// </summary>
    public interface IValidationProvider
    {
        /// <summary>
        /// Get all supported types.
        /// </summary>
        Task<List<SupportedType>> GetSupportedTypesAsync();

        /// <summary>
        /// Validate C# type correspondence with REDB supported types.
        /// </summary>
        Task<ValidationIssue?> ValidateTypeAsync(Type csharpType, string propertyName);

        /// <summary>
        /// Validate schema before synchronization.
        /// </summary>
        Task<SchemaValidationResult> ValidateSchemaAsync<TProps>(string schemeName, bool strictDeleteExtra = true) where TProps : class;
        
        /// <summary>
        /// Validate schema before synchronization (with contract).
        /// </summary>
        Task<SchemaValidationResult> ValidateSchemaAsync<TProps>(IRedbScheme scheme, bool strictDeleteExtra = true) where TProps : class;

        /// <summary>
        /// Check schema change compatibility.
        /// </summary>
        Task<SchemaChangeReport> AnalyzeSchemaChangesAsync<TProps>(IRedbScheme scheme) where TProps : class;

        /// <summary>
        /// Validate property constraints and arrays.
        /// </summary>
        ValidationIssue? ValidatePropertyConstraints(Type propertyType, string propertyName, bool isRequired, bool isArray);
    }
}
