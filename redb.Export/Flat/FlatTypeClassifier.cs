using System.Reflection;
using redb.Core.Attributes;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Export.Flat.Model;

namespace redb.Export.Flat;

/// <summary>
/// Classifies Props members the way scheme synchronisation does. The rules are a copy of the private
/// helpers of <c>redb.Core/Providers/Base/SchemeSyncProviderBase.cs</c> (<c>IsArrayType</c>,
/// <c>IsDictionaryType</c>, <c>IsBusinessClass</c>, <c>MapCSharpTypeToRedbTypeAsync</c>) and of the
/// value writer <c>ObjectStorageProviderBase.SetSimpleValueByType</c>: the flat model has to see a
/// field exactly as the structure tree and the stored rows do. When those rules change, this class
/// changes with them.
/// </summary>
internal static class FlatTypeClassifier
{
    /// <summary>Properties scheme sync turns into structures, in declaration order.</summary>
    public static IEnumerable<PropertyInfo> StoredProperties(Type type) =>
        type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            // Save skips indexers (it looks properties up by name and rejects index parameters),
            // so an indexer never has rows.
            .Where(p => p.GetIndexParameters().Length == 0)
            .Where(p => p.GetCustomAttribute<RedbIgnoreAttribute>() == null);

    public static bool IsDictionary(Type type)
    {
        if (!type.IsGenericType)
            return false;

        var definition = type.GetGenericTypeDefinition();
        return definition == typeof(Dictionary<,>) || definition == typeof(IDictionary<,>);
    }

    /// <summary>Array in the scheme-sync sense. <c>byte[]</c> is a scalar.</summary>
    public static bool IsArray(Type type)
    {
        if (type == typeof(byte[]) || IsDictionary(type))
            return false;

        return IsArrayShape(type);
    }

    public static Type ArrayElementType(Type type) =>
        type.IsArray ? type.GetElementType()! : type.GetGenericArguments()[0];

    public static (Type Key, Type Value) DictionaryTypes(Type type)
    {
        var arguments = type.GetGenericArguments();
        return (arguments[0], arguments[1]);
    }

    public static bool IsListItem(Type type) =>
        type == typeof(IRedbListItem) || type == typeof(RedbListItem);

    /// <summary>
    /// True for <c>RedbObject&lt;T&gt;</c>. Any other object-shaped type is refused: scheme sync
    /// rejects the bare shapes itself and would reflect a derived generic one as a plain class.
    /// </summary>
    public static bool IsObjectReference(Type type, string memberDescription)
    {
        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(RedbObject<>))
            return true;

        if (typeof(IRedbObject).IsAssignableFrom(type))
            throw new NotSupportedException(
                $"{memberDescription} has type '{type.Name}'. A reference in a flat model must be " +
                "declared as RedbObject<TProps>.");

        return false;
    }

    public static bool IsBusinessClass(Type type)
    {
        if (type.IsPrimitive || type == typeof(string) || type == typeof(decimal))
            return false;

        if (type == typeof(DateTime) || type == typeof(DateTimeOffset) ||
            type == typeof(DateOnly) || type == typeof(TimeOnly) ||
            type == typeof(Guid) || type == typeof(TimeSpan) || type == typeof(byte[]))
            return false;

        if (Nullable.GetUnderlyingType(type) != null)
            return false;

        if (type.IsArray || IsArrayShape(type) || IsDictionary(type))
            return false;

        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(RedbObject<>))
            return false;

        if (IsListItem(type) || type.IsEnum)
            return false;

        if (type.Namespace?.StartsWith("System") == true)
            return false;

        return type.IsClass;
    }

    /// <summary>
    /// The <c>_values</c> column a scalar of <paramref name="type"/> is written to. A type scheme
    /// sync has no mapping for (an enum included) is stored as text.
    /// </summary>
    public static FlatValueSource ScalarSource(Type type)
    {
        var underlying = Nullable.GetUnderlyingType(type) ?? type;

        if (underlying == typeof(long) || underlying == typeof(int) ||
            underlying == typeof(short) || underlying == typeof(byte))
            return FlatValueSource.Long;
        if (underlying == typeof(double) || underlying == typeof(float))
            return FlatValueSource.Double;
        if (underlying == typeof(decimal))
            return FlatValueSource.Numeric;
        if (underlying == typeof(bool))
            return FlatValueSource.Boolean;
        if (underlying == typeof(Guid))
            return FlatValueSource.Guid;
        if (underlying == typeof(DateTime) || underlying == typeof(DateTimeOffset) || underlying == typeof(DateOnly))
            return FlatValueSource.DateTimeOffset;
        if (underlying == typeof(byte[]))
            return FlatValueSource.ByteArray;

        return FlatValueSource.String;
    }

    public static bool IsValueTuple(Type type) =>
        type.IsValueType && type.IsGenericType &&
        type.GetGenericTypeDefinition().FullName?.StartsWith("System.ValueTuple`", StringComparison.Ordinal) == true;

    private static bool IsArrayShape(Type type)
    {
        if (type.IsArray)
            return true;
        if (!type.IsGenericType)
            return false;

        var definition = type.GetGenericTypeDefinition();
        return definition == typeof(List<>) || definition == typeof(IList<>) ||
               definition == typeof(ICollection<>) || definition == typeof(IEnumerable<>);
    }
}
