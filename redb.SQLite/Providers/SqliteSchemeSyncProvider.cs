using System.Text.Json;
using redb.Core.Data;
using redb.Core.Models.Configuration;
using redb.Core.Providers;
using redb.Core.Providers.Base;
using redb.SQLite.Sql;
using Microsoft.Extensions.Logging;

namespace redb.SQLite.Providers;

/// <summary>
/// SQLite implementation of scheme synchronization provider.
/// Inherits all logic from SchemeSyncProviderBase, provides SQLite-specific SQL via SqliteDialect.
/// </summary>
public class SqliteSchemeSyncProvider : SchemeSyncProviderBase
{
    /// <summary>
    /// Creates SQLite scheme sync provider with default SqliteDialect.
    /// </summary>
    public SqliteSchemeSyncProvider(
        IRedbContext context, 
        RedbServiceConfiguration? configuration = null,
        string? cacheDomain = null,
        ILogger? logger = null)
        : base(context, new SqliteDialect(), configuration, cacheDomain, logger)
    {
    }

    /// <summary>
    /// Gets structure tree as JsonElement (for API compatibility).
    /// </summary>
    public new async Task<JsonElement> GetStructureTreeJsonAsync(long schemeId)
    {
        var result = await base.GetStructureTreeJsonAsync(schemeId);
        
        if (string.IsNullOrEmpty(result))
            return JsonSerializer.SerializeToElement("[]");
        
        return JsonSerializer.SerializeToElement(result);
    }
}
