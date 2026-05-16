using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using redb.Core.Models.Entities;

namespace redb.Core.Utils;

/// <summary>
/// Utility for computing MD5 hash of scheme structure.
/// Used for change detection and cache invalidation.
/// </summary>
public static class SchemeHashCalculator
{
    /// <summary>
    /// Computes MD5 hash of scheme from all its structures.
    /// Hash aggregates critical fields of all structures: Id, Name, IdType, IdParent, IsArray, AllowNotNull, StoreNull.
    /// </summary>
    /// <param name="structures">List of scheme structures.</param>
    /// <returns>MD5 hash as Guid.</returns>
    public static Guid ComputeSchemeStructureHash(List<RedbStructure> structures)
    {
        if (structures == null || !structures.Any())
            return Guid.Empty;
        
        // Sort for stable hash (independent of load order)
        var sorted = structures
            .OrderBy(s => s.IdParent ?? 0)
            .ThenBy(s => s.Order ?? 0)
            .ThenBy(s => s.Id)
            .ToList();
        
        var sb = new StringBuilder();
        foreach (var s in sorted)
        {
            // Include only critical fields (that affect data)
            sb.Append($"{s.Id}|");
            sb.Append($"{s.Name}|");
            sb.Append($"{s.IdType}|");
            sb.Append($"{s.IdParent}|");
            sb.Append($"{s.CollectionType}|");
            sb.Append($"{s.KeyType}|");
            sb.Append($"{s.AllowNotNull}|");
            sb.Append($"{s.StoreNull};");
        }
        
        // Compute MD5 and convert to Guid
        using var md5 = MD5.Create();
        var hash = md5.ComputeHash(Encoding.UTF8.GetBytes(sb.ToString()));
        return new Guid(hash);
    }
}
