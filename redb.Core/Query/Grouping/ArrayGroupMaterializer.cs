using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text.Json;
using redb.Core.Query.Aggregation;
using redb.Core.Query.Utils;
using redb.Core.Query.Window;

namespace redb.Core.Query.Grouping;

/// <summary>
/// Builds the results of a grouping from the JSON rows a provider returns. Strict: an answer that is not an array,
/// or a row without a column the selector reads, is an error and never an empty list or a default value.
/// </summary>
internal static class ArrayGroupMaterializer
{
    /// <summary>The root array of the answer, or an <see cref="InvalidOperationException"/> naming the provider.</summary>
    public static JsonElement RequireArray(JsonDocument? json, string provider, string api)
    {
        if (json == null)
            throw new InvalidOperationException($"{api}: {provider} returned no answer for the grouping.");
        if (json.RootElement.ValueKind != JsonValueKind.Array)
            throw new InvalidOperationException(
                $"{api}: {provider} returned {json.RootElement.ValueKind} instead of an array of group rows.");
        return json.RootElement;
    }

    /// <summary>
    /// One result per row. A member that reads g.Key takes the key's alias, an Agg.* or Win.* member takes its own
    /// name, a member that does not use the group is computed once.
    /// </summary>
    public static List<TResult> Materialize<TResult>(
        JsonDocument? json, LambdaExpression selector, IReadOnlyList<GroupFieldRequest> groupFields, string provider, string api)
    {
        var root = RequireArray(json, provider, api);
        var members = GroupSelectorMembers.Extract(selector, api);
        var groupParam = selector.Parameters[0];

        var plan = new (bool Client, object? ClientValue, string[] Columns, Type Type, Type? CompositeKey)[members.Count];
        for (int i = 0; i < members.Count; i++)
        {
            var (name, type, raw) = members[i];
            var expr = GroupSelectorMembers.StripConvert(raw);
            var serverCall = expr is MethodCallExpression mc &&
                             (mc.Method.DeclaringType == typeof(Agg) || mc.Method.DeclaringType == typeof(Win));
            if (!serverCall && !GroupSelectorMembers.ReferencesParameter(expr, groupParam))
            {
                plan[i] = (true, Expression.Lambda(raw).Compile().DynamicInvoke(), Array.Empty<string>(), type, null);
                continue;
            }

            if (GroupSelectorMembers.IsKeyAccess(expr, groupParam))
            {
                var keyMember = (MemberExpression)expr;
                if (keyMember.Member.Name == "Key" && keyMember.Expression == groupParam)
                {
                    // g.Key: one key reads its column; a composite key is rebuilt from all of them.
                    plan[i] = groupFields.Count == 1
                        ? (false, null, new[] { groupFields[0].Alias }, type, null)
                        : (false, null, groupFields.Select(g => g.Alias).ToArray(), type, type);
                    continue;
                }

                // g.Key.X of a composite key (g.Key.X.Id reads the same column), or g.Key.Id of a single list item key.
                var underKey = keyMember;
                while (underKey.Expression is MemberExpression parent && !(parent.Member.Name == "Key" && parent.Expression == groupParam))
                    underKey = parent;
                var column = groupFields.Count > 1
                    ? groupFields.FirstOrDefault(g => g.Alias == underKey.Member.Name)?.Alias
                    : groupFields[0].Alias;
                if (column == null)
                    throw new NotSupportedException($"{api}: member '{name}' reads '{raw}', which is not a key of the grouping.");
                plan[i] = (false, null, new[] { column }, type, null);
                continue;
            }

            plan[i] = (false, null, new[] { name }, type, null);
        }

        var results = new List<TResult>();
        foreach (var row in root.EnumerateArray())
        {
            if (row.ValueKind != JsonValueKind.Object)
                throw new InvalidOperationException($"{api}: {provider} returned a group row that is {row.ValueKind}, not an object.");

            var values = new object?[members.Count];
            for (int i = 0; i < members.Count; i++)
            {
                var p = plan[i];
                if (p.Client) { values[i] = p.ClientValue; continue; }

                if (p.CompositeKey != null)
                {
                    var ctor = p.CompositeKey.GetConstructors().Single();
                    var parameters = ctor.GetParameters();
                    var args = new object?[parameters.Length];
                    for (int k = 0; k < parameters.Length; k++)
                        args[k] = JsonValueConverter.Convert(Column(row, p.Columns[k], provider, api), parameters[k].ParameterType);
                    values[i] = ctor.Invoke(args);
                    continue;
                }

                values[i] = JsonValueConverter.Convert(Column(row, p.Columns[0], provider, api), p.Type);
            }
            results.Add(GroupSelectorMembers.Construct<TResult>(selector, values));
        }

        return results;
    }

    private static JsonElement Column(JsonElement row, string name, string provider, string api)
    {
        if (row.TryGetProperty(name, out var exact))
            return exact;
        // Providers fold the case of aliases differently.
        foreach (var p in row.EnumerateObject())
            if (string.Equals(p.Name, name, StringComparison.OrdinalIgnoreCase))
                return p.Value;
        throw new InvalidOperationException(
            $"{api}: {provider} returned a group row without the column '{name}' " +
            $"(columns: {string.Join(", ", row.EnumerateObject().Select(o => o.Name))}).");
    }
}
