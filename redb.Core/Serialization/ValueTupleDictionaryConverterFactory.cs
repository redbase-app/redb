using System;
using System.Collections;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using redb.Core.Utils;

namespace redb.Core.Serialization
{
    /// <summary>
    /// Factory for the JsonConverters of Dictionary&lt;K, V&gt; and IDictionary&lt;K, V&gt;. A read lists the entries
    /// by key (<see cref="RedbDictionaryOrder"/>), not in the order of the JSON the modules built, which differed
    /// between providers. Complex keys (ValueTuple, classes) are stored as Base64-encoded JSON strings; simple keys
    /// are read and written by the converter of the key type, as System.Text.Json does on its own.
    /// </summary>
    public class ValueTupleDictionaryConverterFactory : JsonConverterFactory
    {
        public override bool CanConvert(Type typeToConvert)
        {
            if (!typeToConvert.IsGenericType)
                return false;

            var genericDef = typeToConvert.GetGenericTypeDefinition();
            return genericDef == typeof(Dictionary<,>) || genericDef == typeof(IDictionary<,>);
        }

        public override JsonConverter? CreateConverter(Type typeToConvert, JsonSerializerOptions options)
        {
            var args = typeToConvert.GetGenericArguments();
            var keyType = args[0];
            var valueType = args[1];

            Type converterType;
            if (RedbKeySerializer.IsComplexKey(keyType))
                converterType = typeof(ValueTupleDictionaryConverter<,>).MakeGenericType(keyType, valueType);
            else if (typeToConvert.GetGenericTypeDefinition() == typeof(Dictionary<,>))
                converterType = typeof(KeyOrderedDictionaryConverter<,>).MakeGenericType(keyType, valueType);
            else
                converterType = typeof(KeyOrderedDictionaryInterfaceConverter<,>).MakeGenericType(keyType, valueType);
            return (JsonConverter?)Activator.CreateInstance(converterType);
        }
    }

    /// <summary>
    /// JsonConverter for Dictionary with complex keys (ValueTuple, classes).
    /// Keys are serialized to Base64-encoded JSON strings.
    /// </summary>
    public class ValueTupleDictionaryConverter<TKey, TValue> : JsonConverter<Dictionary<TKey, TValue>>
        where TKey : notnull
    {
        public override Dictionary<TKey, TValue>? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.Null)
                return null;

            if (reader.TokenType != JsonTokenType.StartObject)
                throw new JsonException("Expected StartObject token");

            var entries = new List<KeyValuePair<TKey, TValue>>();

            while (reader.Read())
            {
                if (reader.TokenType == JsonTokenType.EndObject)
                    return KeyOrderedDictionaryConverter<TKey, TValue>.ToDictionary(entries);

                if (reader.TokenType != JsonTokenType.PropertyName)
                    throw new JsonException("Expected PropertyName token");

                // Key is Base64-encoded JSON
                var keyString = reader.GetString()!;
                var key = (TKey)RedbKeySerializer.DeserializeObject(keyString, typeof(TKey))!;

                // Read value
                reader.Read();
                var value = JsonSerializer.Deserialize<TValue>(ref reader, options);

                entries.Add(new KeyValuePair<TKey, TValue>(key, value!));
            }

            throw new JsonException("Unexpected end of JSON");
        }

        public override void Write(Utf8JsonWriter writer, Dictionary<TKey, TValue>? value, JsonSerializerOptions options)
        {
            if (value == null)
            {
                writer.WriteNullValue();
                return;
            }

            writer.WriteStartObject();

            foreach (var kvp in value)
            {
                // Key → Base64-encoded JSON
                var keyString = RedbKeySerializer.SerializeObject(kvp.Key, typeof(TKey));
                writer.WritePropertyName(keyString);

                // Write value
                JsonSerializer.Serialize(writer, kvp.Value, options);
            }

            writer.WriteEndObject();
        }
    }

    /// <summary>
    /// JsonConverter for Dictionary with a simple key: the key is read and written by the converter of its type, as
    /// System.Text.Json does without a converter; a read lists the entries by key.
    /// </summary>
    public class KeyOrderedDictionaryConverter<TKey, TValue> : JsonConverter<Dictionary<TKey, TValue>>
        where TKey : notnull
    {
        public override Dictionary<TKey, TValue>? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            => ReadEntries(ref reader, options);

        public override void Write(Utf8JsonWriter writer, Dictionary<TKey, TValue>? value, JsonSerializerOptions options)
            => WriteEntries(writer, value, options);

        internal static Dictionary<TKey, TValue>? ReadEntries(ref Utf8JsonReader reader, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.Null)
                return null;
            if (reader.TokenType != JsonTokenType.StartObject)
                throw new JsonException("Expected StartObject token");

            var keyConverter = (JsonConverter<TKey>)options.GetConverter(typeof(TKey));
            var entries = new List<KeyValuePair<TKey, TValue>>();
            while (reader.Read())
            {
                if (reader.TokenType == JsonTokenType.EndObject)
                    return ToDictionary(entries);
                if (reader.TokenType != JsonTokenType.PropertyName)
                    throw new JsonException("Expected PropertyName token");

                var key = keyConverter.ReadAsPropertyName(ref reader, typeof(TKey), options);
                reader.Read();
                var value = JsonSerializer.Deserialize<TValue>(ref reader, options);
                entries.Add(new KeyValuePair<TKey, TValue>(key, value!));
            }
            throw new JsonException("Unexpected end of JSON");
        }

        internal static void WriteEntries(Utf8JsonWriter writer, IDictionary<TKey, TValue>? value, JsonSerializerOptions options)
        {
            if (value == null)
            {
                writer.WriteNullValue();
                return;
            }

            var keyConverter = (JsonConverter<TKey>)options.GetConverter(typeof(TKey));
            writer.WriteStartObject();
            foreach (var kvp in value)
            {
                keyConverter.WriteAsPropertyName(writer, kvp.Key, options);
                JsonSerializer.Serialize(writer, kvp.Value, options);
            }
            writer.WriteEndObject();
        }

        /// <summary>The entries by key; a repeated key keeps its last value, as System.Text.Json reads one.</summary>
        internal static Dictionary<TKey, TValue> ToDictionary(List<KeyValuePair<TKey, TValue>> entries)
        {
            var last = new Dictionary<TKey, TValue>(entries.Count);
            foreach (var entry in entries)
                last[entry.Key] = entry.Value;
            if (last.Count != entries.Count)
                entries = new List<KeyValuePair<TKey, TValue>>(last);

            var dict = new Dictionary<TKey, TValue>(entries.Count);
            foreach (var entry in RedbDictionaryOrder.Sort(entries))
                dict[entry.Key] = entry.Value;
            return dict;
        }
    }

    /// <summary>The same for a property declared as IDictionary with a simple key; it reads a Dictionary.</summary>
    public class KeyOrderedDictionaryInterfaceConverter<TKey, TValue> : JsonConverter<IDictionary<TKey, TValue>>
        where TKey : notnull
    {
        public override IDictionary<TKey, TValue>? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            => KeyOrderedDictionaryConverter<TKey, TValue>.ReadEntries(ref reader, options);

        public override void Write(Utf8JsonWriter writer, IDictionary<TKey, TValue>? value, JsonSerializerOptions options)
            => KeyOrderedDictionaryConverter<TKey, TValue>.WriteEntries(writer, value, options);
    }
}
