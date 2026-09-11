using System;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization;
using redb.Core.Models.Entities;

namespace redb.Core.Serialization;

/// <summary>
/// V4 (L.4, LAZY plan §4.3): serializing a <c>RedbObject&lt;T&gt;</c> must never trigger lazy loading.
/// The default object converter walks the public <c>Props</c> getter — for an unloaded reference stub
/// that getter IS a load, so serializing a parent would pull the whole lazy graph (the classic ORM
/// trap). This factory takes over WRITING: base fields are written by hand, and <c>properties</c> is
/// emitted only when the object is actually loaded (via <c>GetPropsDirectly</c>, which never loads).
/// Nested references inside the Props re-enter this factory, so a stub anywhere in the graph
/// serializes as its base fields — the same shape the JSON builders emit at the depth boundary.
///
/// <para>
/// READING is a pass-through: the payload is deserialized with the same options minus this factory,
/// i.e. the exact pre-V4 default path - property mapping, the date/enum/tuple converters, the Props
/// setter - with one rule on top: an object that arrives without Props (no <c>properties</c> key,
/// or <c>"properties": null</c>, the shape a foreign serializer gives a loader-less stub) is NOT
/// loaded, so a parent save leaves it alone as the reference it is.
/// </para>
///
/// <para>
/// <c>TreeRedbObject&lt;T&gt;</c> deliberately keeps the default path: this converter would flatten it
/// to a plain <c>RedbObject&lt;T&gt;</c> on read. Tree relations are not references (LAZY plan §4.12).
/// </para>
/// </summary>
public sealed class RedbObjectStubWriteConverterFactory : JsonConverterFactory
{
    public override bool CanConvert(Type typeToConvert)
        => typeToConvert.IsGenericType
           && typeToConvert.GetGenericTypeDefinition() == typeof(RedbObject<>);

    public override JsonConverter CreateConverter(Type typeToConvert, JsonSerializerOptions options)
    {
        var propsType = typeToConvert.GetGenericArguments()[0];
        return (JsonConverter)Activator.CreateInstance(
            typeof(StubWriteConverter<>).MakeGenericType(propsType))!;
    }

    /// <summary>One stripped copy per options instance (in practice: the static Options).</summary>
    private static readonly ConditionalWeakTable<JsonSerializerOptions, JsonSerializerOptions> InnerCache = new();

    internal static JsonSerializerOptions WithoutFactory(JsonSerializerOptions options)
    {
        return InnerCache.GetValue(options, static o =>
        {
            var inner = new JsonSerializerOptions(o);
            for (var i = inner.Converters.Count - 1; i >= 0; i--)
                if (inner.Converters[i] is RedbObjectStubWriteConverterFactory)
                    inner.Converters.RemoveAt(i);
            return inner;
        });
    }

    private sealed class StubWriteConverter<TProps> : JsonConverter<RedbObject<TProps>>
        where TProps : class, new()
    {
        public override RedbObject<TProps>? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            var obj = JsonSerializer.Deserialize<RedbObject<TProps>>(ref reader, WithoutFactory(options));

            // "properties": null - or no properties at all - is NOT a loaded object with nothing in it,
            // it is an object whose Props were not carried: a stub written by a foreign serializer
            // (the getter of a loader-less stub answers null), or a stub written by this one. The
            // setter marks any assignment as loaded, and a nested object marked loaded-with-nothing is
            // saved as such by the parent: its values deleted, nothing written (the wipe 9fa79c45
            // closed for our own stubs). Null Props after reading therefore mean "not loaded"; for a
            // root that changes nothing - a root is saved whatever its loaded flag says (review).
            // The subtree was read with the default converter, so nested objects never pass through
            // here: one walk from the root applies the rule to every level.
            Utils.LazyReferenceInstaller.MarkUnloadedWherePropsAreNull(obj);

            return obj;
        }

        public override void Write(Utf8JsonWriter writer, RedbObject<TProps>? value, JsonSerializerOptions options)
        {
            if (value == null)
            {
                writer.WriteNullValue();
                return;
            }

            writer.WriteStartObject();
            writer.WriteNumber("id", value.id);
            if (value.parent_id.HasValue) writer.WriteNumber("parent_id", value.parent_id.Value);
            writer.WriteNumber("scheme_id", value.scheme_id);
            if (value.owner_id != 0) writer.WriteNumber("owner_id", value.owner_id);
            if (value.who_change_id != 0) writer.WriteNumber("who_change_id", value.who_change_id);
            if (value.date_create != default) writer.WriteString("date_create", value.date_create);
            if (value.date_modify != default) writer.WriteString("date_modify", value.date_modify);
            if (value.date_begin.HasValue) writer.WriteString("date_begin", value.date_begin.Value);
            if (value.date_complete.HasValue) writer.WriteString("date_complete", value.date_complete.Value);
            if (value.key.HasValue) writer.WriteNumber("key", value.key.Value);
            if (value.value_long.HasValue) writer.WriteNumber("value_long", value.value_long.Value);
            if (value.value_string != null) writer.WriteString("value_string", value.value_string);
            if (value.value_guid.HasValue) writer.WriteString("value_guid", value.value_guid.Value);
            if (value.value_bool.HasValue) writer.WriteBoolean("value_bool", value.value_bool.Value);
            if (value.value_double.HasValue) writer.WriteNumber("value_double", value.value_double.Value);
            if (value.value_numeric.HasValue) writer.WriteNumber("value_numeric", value.value_numeric.Value);
            if (value.value_datetime.HasValue) writer.WriteString("value_datetime", value.value_datetime.Value);
            if (value.value_bytes != null) writer.WriteBase64String("value_bytes", value.value_bytes);
            if (value.value_unique != null) writer.WriteString("value_unique", value.value_unique);
            if (value.name != null) writer.WriteString("name", value.name);
            if (value.note != null) writer.WriteString("note", value.note);
            if (value.hash.HasValue) writer.WriteString("hash", value.hash.Value);

            // GetPropsDirectly never loads: an unloaded stub stops here — base fields only.
            var props = value.IsPropsLoaded ? value.GetPropsDirectly() : null;
            if (props != null)
            {
                writer.WritePropertyName("properties");
                // Full options on purpose: nested RedbObject<T> re-enter this factory.
                JsonSerializer.Serialize(writer, props, options);
            }

            writer.WriteEndObject();
        }
    }
}
