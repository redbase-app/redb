using System.Globalization;
using redb.Route.Abstractions;

namespace RedbWorker.Module;

/// <summary>
/// The module's settings, read from the properties of its route context. Both hosts fill them the same
/// way: RedbWorker.Module.config.json first, then the <c>Tsak:Contexts:{context}:Override</c> section
/// of the host configuration, which wins. An environment variable such as
/// <c>Tsak__Contexts__redbworker__Override__Folders__Inbox</c> works in both.
/// </summary>
public sealed record ModuleSettings(string Inbox, string Outbox, string Archive, string Error)
{
    public static ModuleSettings FromContext(IRouteContext context)
    {
        var folders = context.GetProperty<IDictionary<string, object?>>("Folders")
            ?? throw Missing(context, "Folders");

        return new(
            Inbox: FullPath(Value(context, folders, "Inbox")),
            Outbox: FullPath(Value(context, folders, "Outbox")),
            Archive: FullPath(Value(context, folders, "Archive")),
            Error: FullPath(Value(context, folders, "Error")));
    }

    /// <summary>Creates the folders that do not exist yet.</summary>
    public void EnsureFolders()
    {
        foreach (var folder in new[] { Inbox, Outbox, Archive, Error })
            Directory.CreateDirectory(folder);
    }

    // Relative paths resolve against the current directory of the process.
    private static string FullPath(string path) => Path.GetFullPath(path).Replace('\\', '/');

    private static string Value(IRouteContext context, IDictionary<string, object?> section, string key) =>
        section.TryGetValue(key, out var value) && Convert.ToString(value, CultureInfo.InvariantCulture) is { Length: > 0 } text
            ? text
            : throw Missing(context, $"Folders:{key}");

    private static InvalidOperationException Missing(IRouteContext context, string key) => new(
        $"{key} is not configured for context '{context.ContextId}': set it in RedbWorker.Module.config.json " +
        $"or in Tsak:Contexts:{context.ContextId}:Override.");
}
