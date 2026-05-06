using System;
using System.Collections;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using redb.Core.Utils;

namespace redb.Core.Serialization
{
    /// <summary>
    /// Factory for creating JsonConverters for Dictionary with ValueTuple keys.
    /// Handles serialization/deserialization of Dictionary&lt;(T1, T2), TValue&gt; and similar types.
    /// Keys are stored as Base64-encoded JSON strings.
    /// </summary>
    public class ValueTupleDictionaryConverterFactory : JsonConverterFactory
    {
        public override bool CanConvert(Type typeToConvert)
        {
            if (!typeToConvert.IsGenericType)
                return false;
                
            var genericDef = typeToConvert.GetGenericTypeDefinition();
            if (genericDef != typeof(Dictionary<,>) && genericDef != typeof(IDictionary<,>))
                return false;
                
            // Check if key is ValueTuple
            var keyType = typeToConvert.GetGenericArguments()[0];
            return RedbKeySerializer.IsComplexKey(keyType);
        }

        public override JsonConverter? CreateConverter(Type typeToConvert, JsonSerializerOptions options)
        {
            var args = typeToConvert.GetGenericArguments();
            var keyType = args[0];
            var valueType = args[1];
            
            var converterType = typeof(ValueTupleDictionaryConverter<,>).MakeGenericType(keyType, valueType);
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
                
            var dict = new Dictionary<TKey, TValue>();
            
            while (reader.Read())
            {
                if (reader.TokenType == JsonTokenType.EndObject)
                    return dict;
                    
                if (reader.TokenType != JsonTokenType.PropertyName)
                    throw new JsonException("Expected PropertyName token");
                    
                // Key is Base64-encoded JSON
                var keyString = reader.GetString()!;
                var key = (TKey)RedbKeySerializer.DeserializeObject(keyString, typeof(TKey))!;
                
                // Read value
                reader.Read();
                var value = JsonSerializer.Deserialize<TValue>(ref reader, options);
                
                dict[key] = value!;
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
}

