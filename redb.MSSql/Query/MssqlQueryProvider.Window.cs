using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using redb.Core.Query.QueryExpressions;
using redb.Core.Query.Window;

namespace redb.MSSql.Query;

/// <summary>
/// PVT-backed window functions for free MSSql. Generates the SELECT SQL
/// via the server-side compiler <c>dbo.pvt_build_window_sql</c> and wraps
/// it with <c>FOR JSON PATH, INCLUDE_NULL_VALUES</c> to materialize a JSON
/// array in one round-trip.
///
/// Window calls are expressed as scalar <c>$over</c> nodes inside the
/// <c>p_select</c> array; all funcs share the same partition_by / order_by
/// / frame supplied through the public API. The JSON shape is identical to
/// the PostgreSQL dialect so <c>BuildPvtWindowSelectJson</c> logic is shared.
/// </summary>
public partial class MssqlQueryProvider
{
    /// <inheritdoc />
    public override Task<JsonDocument?> ExecuteWindowQueryAsync(
        long schemeId,
        IEnumerable<WindowFieldRequest> selectFields,
        IEnumerable<WindowFuncRequest> windowFuncs,
        IEnumerable<WindowFieldRequest> partitionBy,
        IEnumerable<WindowOrderRequest> orderBy,
        string? filterJson = null,
        string? frameJson = null,
        int? take = null,
        int? skip = null)
    {
        return ExecuteWindowQueryInternalAsync(schemeId, selectFields, windowFuncs,
            partitionBy, orderBy, filterJson, frameJson, take, skip);
    }

    /// <inheritdoc />
    public override Task<JsonDocument?> ExecuteWindowQueryAsync(
        long schemeId,
        IEnumerable<WindowFieldRequest> selectFields,
        IEnumerable<WindowFuncRequest> windowFuncs,
        IEnumerable<WindowFieldRequest> partitionBy,
        IEnumerable<WindowOrderRequest> orderBy,
        FilterExpression? filter,
        string? frameJson = null,
        int? take = null,
        int? skip = null)
    {
        var filterJson = filter is null ? null : _facetBuilder.BuildFacetFilters(filter);
        return ExecuteWindowQueryInternalAsync(schemeId, selectFields, windowFuncs,
            partitionBy, orderBy, filterJson, frameJson, take, skip);
    }

    private async Task<JsonDocument?> ExecuteWindowQueryInternalAsync(
        long schemeId,
        IEnumerable<WindowFieldRequest> selectFields,
        IEnumerable<WindowFuncRequest> windowFuncs,
        IEnumerable<WindowFieldRequest> partitionBy,
        IEnumerable<WindowOrderRequest> orderBy,
        string? filterJson,
        string? frameJson,
        int? take,
        int? skip)
    {
        var innerSql = await BuildWindowInnerSqlAsync(schemeId, selectFields, windowFuncs,
            partitionBy, orderBy, filterJson, frameJson, take, skip);

        var wrapped = "SELECT (SELECT * FROM (" + innerSql
            + ") _win_rows FOR JSON PATH, INCLUDE_NULL_VALUES)";

        var jsonArray = await _context.ExecuteScalarAsync<string>(wrapped);
        if (string.IsNullOrEmpty(jsonArray))
            return JsonDocument.Parse("[]");
        return JsonDocument.Parse(jsonArray);
    }

    /// <inheritdoc />
    public override Task<string> GetWindowSqlPreviewAsync(
        long schemeId,
        IEnumerable<WindowFieldRequest> selectFields,
        IEnumerable<WindowFuncRequest> windowFuncs,
        IEnumerable<WindowFieldRequest> partitionBy,
        IEnumerable<WindowOrderRequest> orderBy,
        string? filterJson = null,
        string? frameJson = null,
        int? take = null,
        int? skip = null)
    {
        return BuildWindowInnerSqlAsync(schemeId, selectFields, windowFuncs,
            partitionBy, orderBy, filterJson, frameJson, take, skip);
    }

    private async Task<string> BuildWindowInnerSqlAsync(
        long schemeId,
        IEnumerable<WindowFieldRequest> selectFields,
        IEnumerable<WindowFuncRequest> windowFuncs,
        IEnumerable<WindowFieldRequest> partitionBy,
        IEnumerable<WindowOrderRequest> orderBy,
        string? filterJson,
        string? frameJson,
        int? take,
        int? skip)
    {
        var selectList = selectFields?.ToList() ?? new List<WindowFieldRequest>();
        var funcList = windowFuncs?.ToList() ?? new List<WindowFuncRequest>();
        if (funcList.Count == 0)
            throw new ArgumentException(
                "windowFuncs must contain at least one entry.", nameof(windowFuncs));

        var partitionList = partitionBy?.ToList() ?? new List<WindowFieldRequest>();
        var orderList = orderBy?.ToList() ?? new List<WindowOrderRequest>();

        JsonElement? frameNode = null;
        if (!string.IsNullOrWhiteSpace(frameJson) && frameJson != "null")
        {
            using var frameDoc = JsonDocument.Parse(frameJson);
            frameNode = ConvertLegacyFrameJson(frameDoc.RootElement);
        }

        var selectJson = BuildPvtWindowSelectJson(selectList, funcList,
            partitionList, orderList, frameNode);

        object filterParam = string.IsNullOrEmpty(filterJson)
            || filterJson == "{}" || filterJson == "null"
            ? DBNull.Value
            : (object)filterJson!;
        object limitParam  = take.HasValue ? (object)take.Value : DBNull.Value;
        object offsetParam = skip ?? 0;

        // pvt_build_window_sql(@scheme_id, @filter, @select, @order, @limit, @offset, @source_mode)
        // $1=filter, $2=selectJson, $3=limit, $4=offset
        var invocation =
            "SELECT dbo.pvt_build_window_sql("
            + schemeId.ToString(CultureInfo.InvariantCulture)
            + ", $1, $2, NULL, $3, $4, N'flat')";

        _logger?.LogDebug(
            "PVT Window Build (MSSql): SchemeId={SchemeId}, Select={Select}, Filter={Filter}, Take={Take}, Skip={Skip}",
            schemeId, selectJson, filterJson ?? "null", take, skip);

        var innerSql = await _context.ExecuteScalarAsync<string>(
            invocation, filterParam, selectJson, limitParam, offsetParam);

        if (string.IsNullOrWhiteSpace(innerSql))
            throw new InvalidOperationException(
                "dbo.pvt_build_window_sql returned an empty SQL string for scheme " + schemeId + ".");
        return innerSql!;
    }

    /// <summary>
    /// Builds the <c>p_select</c> JSON array: plain field projections followed
    /// by one <c>$over</c> entry per window function. partition_by / order_by /
    /// frame are replicated into every <c>$over</c> node.
    /// The shape is identical to the PostgreSQL dialect so the UDF on either
    /// side can consume the same JSON.
    /// </summary>
    private static string BuildPvtWindowSelectJson(
        IList<WindowFieldRequest> selectFields,
        IList<WindowFuncRequest> windowFuncs,
        IList<WindowFieldRequest> partitionBy,
        IList<WindowOrderRequest> orderBy,
        JsonElement? frameNode)
    {
        var entries = new List<Dictionary<string, object>>(
            selectFields.Count + windowFuncs.Count);

        foreach (var f in selectFields)
        {
            var entry = new Dictionary<string, object>
            {
                ["field"] = NormalizePvtFieldName(f.FieldPath, f.IsBaseField)
            };
            if (!string.IsNullOrEmpty(f.Alias))
                entry["alias"] = f.Alias!;
            entries.Add(entry);
        }

        var partitionEntries = partitionBy
            .Select(p => new Dictionary<string, object>
            {
                ["field"] = NormalizePvtFieldName(p.FieldPath, p.IsBaseField)
            })
            .ToList();

        var orderEntries = orderBy
            .Select(o => new Dictionary<string, object>
            {
                ["field"] = NormalizePvtFieldName(o.FieldPath, o.IsBaseField),
                ["dir"]   = o.Descending ? "desc" : "asc"
            })
            .ToList();

        foreach (var f in windowFuncs)
        {
            var over = new Dictionary<string, object>
            {
                ["func"] = (f.Func ?? string.Empty).ToLowerInvariant()
            };

            var args = BuildWindowArgs(f);
            if (args is not null)
                over["args"] = args;
            if (partitionEntries.Count > 0)
                over["partition_by"] = partitionEntries;
            if (orderEntries.Count > 0)
                over["order_by"] = orderEntries;
            if (frameNode.HasValue)
                over["frame"] = frameNode.Value;

            var entry = new Dictionary<string, object>
            {
                ["alias"] = string.IsNullOrEmpty(f.Alias)
                    ? (f.Func ?? string.Empty).ToLowerInvariant()
                    : f.Alias!,
                ["$expr"] = new Dictionary<string, object> { ["$over"] = over }
            };
            entries.Add(entry);
        }

        var options = new JsonSerializerOptions
        {
            Converters = { new WindowJsonElementConverter() }
        };
        return JsonSerializer.Serialize(entries, options);
    }

    private static List<object>? BuildWindowArgs(WindowFuncRequest f)
    {
        var func = (f.Func ?? string.Empty).ToUpperInvariant();
        if (func == "NTILE" && f.Buckets.HasValue)
        {
            return new List<object>
            {
                new Dictionary<string, object> { ["$const"] = f.Buckets.Value }
            };
        }
        if (!string.IsNullOrEmpty(f.FieldPath))
        {
            return new List<object>
            {
                new Dictionary<string, object>
                {
                    ["$field"] = NormalizePvtFieldName(f.FieldPath, f.IsBaseField)
                }
            };
        }
        // COUNT(*) shorthand
        if (func == "COUNT")
            return new List<object> { "*" };
        return null;
    }

    /// <summary>
    /// Converts the legacy <c>SerializeFrame()</c> JSON shape
    /// (kind + offset) to the PVT frame shape consumed by
    /// <c>dbo.pvt_compile_frame_bound</c>. Inputs already in PVT
    /// shape pass through unchanged.
    /// </summary>
    private static JsonElement ConvertLegacyFrameJson(JsonElement frame)
    {
        if (frame.ValueKind != JsonValueKind.Object)
            return frame.Clone();

        var node = new Dictionary<string, object>();
        if (frame.TryGetProperty("type", out var typeEl) && typeEl.ValueKind == JsonValueKind.String)
            node["type"] = typeEl.GetString()!.ToLowerInvariant();
        if (frame.TryGetProperty("start", out var startEl))
            node["start"] = ConvertLegacyFrameBound(startEl);
        if (frame.TryGetProperty("end", out var endEl))
            node["end"] = ConvertLegacyFrameBound(endEl);
        if (frame.TryGetProperty("exclude", out var excEl) && excEl.ValueKind == JsonValueKind.String)
            node["exclude"] = excEl.GetString()!.ToLowerInvariant();

        var json = JsonSerializer.Serialize(node);
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.Clone();
    }

    private static object ConvertLegacyFrameBound(JsonElement bound)
    {
        if (bound.ValueKind == JsonValueKind.String)
            return bound.GetString()!.ToLowerInvariant();
        if (bound.ValueKind != JsonValueKind.Object)
            return bound.Clone();

        // Already PVT shape?
        if (bound.TryGetProperty("preceding", out _) || bound.TryGetProperty("following", out _))
            return bound.Clone();

        if (!bound.TryGetProperty("kind", out var kindEl) || kindEl.ValueKind != JsonValueKind.String)
            return bound.Clone();

        var kind = (kindEl.GetString() ?? string.Empty).ToUpperInvariant();
        int? offset = null;
        if (bound.TryGetProperty("offset", out var offEl) && offEl.ValueKind == JsonValueKind.Number)
            offset = offEl.GetInt32();

        return kind switch
        {
            "UNBOUNDEDPRECEDING" or "UNBOUNDED_PRECEDING" => "unbounded_preceding",
            "CURRENTROW"         or "CURRENT_ROW"         => "current_row",
            "UNBOUNDEDFOLLOWING" or "UNBOUNDED_FOLLOWING" => "unbounded_following",
            "PRECEDING" => new Dictionary<string, object> { ["preceding"] = offset ?? 0 },
            "FOLLOWING" => new Dictionary<string, object> { ["following"] = offset ?? 0 },
            _ => (object)bound.Clone()
        };
    }

    /// <summary>
    /// Passes already-parsed <see cref="JsonElement"/> values through
    /// serialization verbatim so the embedded frame sub-tree survives intact.
    /// </summary>
    private sealed class WindowJsonElementConverter
        : System.Text.Json.Serialization.JsonConverter<JsonElement>
    {
        public override JsonElement Read(
            ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            => JsonDocument.ParseValue(ref reader).RootElement.Clone();

        public override void Write(
            Utf8JsonWriter writer, JsonElement value, JsonSerializerOptions options)
            => value.WriteTo(writer);
    }
}
