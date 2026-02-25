using System.Text.Json;
using redb.Core.Data;
using redb.Core.Models.Configuration;
using redb.Core.Providers;
using redb.Core.Providers.Base;
using redb.Postgres.Sql;
using Microsoft.Extensions.Logging;

namespace redb.Postgres.Providers;

/// <summary>
/// PostgreSQL implementation of scheme synchronization provider.
/// Inherits all logic from SchemeSyncProviderBase, provides PostgreSQL-specific SQL via PostgreSqlDialect.
/// </summary>
public class PostgresSchemeSyncProvider : SchemeSyncProviderBase
{
    /// <summary>
    /// Creates PostgreSQL scheme sync provider with default PostgreSqlDialect.
    /// </summary>
    public PostgresSchemeSyncProvider(
        IRedbContext context, 
        RedbServiceConfiguration? configuration = null,
        string? cacheDomain = null,
        ILogger? logger = null)
        : base(context, new PostgreSqlDialect(), configuration, cacheDomain, logger)
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
