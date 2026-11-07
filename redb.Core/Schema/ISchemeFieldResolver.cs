using redb.Core.Query.Models;

namespace redb.Core.Schema;

/// <summary>
/// Resolves field paths to FieldInfo with structure_id and db_type.
/// Supports: simple (Name), nested (Address.City), dictionary (PhoneBook[home]), 
/// nested dictionary (AddressBook[work].City), ListItem properties (Status.Value).
/// </summary>
public interface ISchemeFieldResolver
{
    /// <summary>
    /// Resolve single field path to FieldInfo.
    /// </summary>
    /// <param name="schemeId">ID of the scheme</param>
    /// <param name="fieldPath">
    /// Field path. Examples:
    /// - "Name" - simple field
    /// - "Address.City" - nested field
    /// - "PhoneBook[home]" - dictionary access
    /// - "AddressBook[work].City" - nested dictionary field
    /// - "Status.Value" - ListItem property
    /// - "Roles[].Value" - array of ListItem property
    /// </param>
    /// <returns>FieldInfo or null if field not found</returns>
    Task<FieldInfo?> ResolveAsync(long schemeId, string fieldPath);
    
    /// <summary>
    /// Batch resolve multiple field paths. Uses internal caching.
    /// More efficient than multiple ResolveAsync calls.
    /// </summary>
    /// <param name="schemeId">ID of the scheme</param>
    /// <param name="fieldPaths">Collection of field paths to resolve</param>
    /// <returns>Dictionary mapping field path to FieldInfo (only resolved fields included)</returns>
    Task<Dictionary<string, FieldInfo>> ResolveManyAsync(long schemeId, IEnumerable<string> fieldPaths);
    
    /// <summary>
    /// Clear internal cache. Useful for testing or after schema changes.
    /// </summary>
    void ClearCache();
}

