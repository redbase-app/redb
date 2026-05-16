using System;
using System.Text.Json;
using System.Text.Json.Serialization;
using redb.Core.Models.Entities;
using redb.Core.Models.Contracts;
using redb.Core.Utils;
using redb.Core.Caching;

namespace redb.Core.Serialization
{
    // Implementation using System.Text.Json
    // Requirement: C# property names match JSON names (snake_case), so no custom NamingPolicy
    public class SystemTextJsonRedbSerializer : IRedbObjectSerializer
    {
        /// <summary>
        /// Static type resolver for polymorphic deserialization.
        /// Set via SetTypeResolver during service initialization.
        /// </summary>
        private static Func<long, Type?>? _typeResolver;
        
        /// <summary>
        /// Set the type resolver function for polymorphic deserialization.
        /// Should be called once during service initialization.
        /// </summary>
        public static void SetTypeResolver(Func<long, Type?> resolver)
        {
            _typeResolver = resolver ?? throw new ArgumentNullException(nameof(resolver));
        }
        
        /// <summary>
        /// Resolve CLR type by scheme ID using configured resolver.
        /// </summary>
        internal static Type? ResolveType(long schemeId)
        {
            return _typeResolver?.Invoke(schemeId);
        }
        
        /// <summary>
        /// Public serialization options for use in other components
        /// (e.g., for polymorphic TreeRedbObject deserialization)
        /// </summary>
        public static readonly JsonSerializerOptions Options = new()
        {
            PropertyNameCaseInsensitive = true,
            ReadCommentHandling = JsonCommentHandling.Skip,
            AllowTrailingCommas = true,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingDefault, // ✅ Ignore empty arrays/null/default when serializing
            Converters = { 
                new NullToDefaultConverterFactory(), // ✅ null → default(T) for value types (int, long, etc)
                new ValueTupleDictionaryConverterFactory(), // ✅ Dictionary with ValueTuple/Class keys (Base64)
                new PolymorphicRedbObjectConverter(), // ✅ Polymorphic IRedbObject deserialization based on scheme_id
                new PostgresDateTimeOffsetConverter(), // ✅ DateTimeOffset from PostgreSQL timestamptz
                new PostgresNullableDateTimeOffsetConverter(), // ✅ Nullable DateTimeOffset from PostgreSQL
                new PostgresInfinityDateTimeConverter(), // ✅ FIX: PostgreSQL "-infinity" support
                new PostgresInfinityNullableDateTimeConverter(), // ✅ FIX: PostgreSQL "-infinity" support for nullable DateTime
                new JsonStringEnumConverter(), // Enum as strings support
                new FlexibleTimeSpanConverter(), // TimeSpan from strings support
                new FlexibleNullableTimeSpanConverter(), // nullable TimeSpan support
#if NET6_0_OR_GREATER
                new FlexibleDateOnlyConverter(), // DateOnly from DateTime strings support
                new FlexibleNullableDateOnlyConverter(), // nullable DateOnly support
                new FlexibleTimeOnlyConverter(), // TimeOnly from TimeSpan strings support
                new FlexibleNullableTimeOnlyConverter() // nullable TimeOnly support
#endif
            }
        };

        public RedbObject<TProps> Deserialize<TProps>(string json) where TProps : class, new()
        {
            var obj = JsonSerializer.Deserialize<RedbObject<TProps>>(json, Options);
            if (obj == null)
            {
                throw new InvalidOperationException("Failed to deserialize get_object_json payload to RedbObject<TProps>.");
            }
            return obj;
        }

        public IRedbObject DeserializeDynamic(string json, Type propsType)
        {
            // Create generic type RedbObject&lt;propsType&gt; via reflection
            var redbObjectType = typeof(RedbObject<>).MakeGenericType(propsType);
            
            // Deserialize JSON to this type
            var deserializedObj = JsonSerializer.Deserialize(json, redbObjectType, Options);
            
            if (deserializedObj == null)
            {
                throw new InvalidOperationException($"Failed to deserialize get_object_json payload to RedbObject<{propsType.Name}>.");
            }
            
            // Return as IRedbObject
            return (IRedbObject)deserializedObj;
        }

        public RedbObject? DeserializeRedbDynamic(string json, Type propsType)
        {
            // Simply deserialize to RedbObject&lt;TProps&gt;
            // Conversion to TreeRedbObject will happen in PostgresTreeQueryProvider
            var redbObjectType = typeof(RedbObject<>).MakeGenericType(propsType);
            var redbObj = JsonSerializer.Deserialize(json, redbObjectType, Options) as RedbObject;
            return redbObj;
        }
    }

    /// <summary>
    /// ✅ Polymorphic converter for IRedbObject
    /// Automatically determines type based on scheme_id from JSON and deserializes to RedbObject&lt;TProps&gt;
    /// Used for nested objects (e.g., RedbListItem.Object)
    /// </summary>
    public class PolymorphicRedbObjectConverter : JsonConverter<IRedbObject>
    {
        public override IRedbObject? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            // 1. Read JSON to JsonElement for analysis
            using var jsonDoc = JsonDocument.ParseValue(ref reader);
            var jsonElement = jsonDoc.RootElement;
            
            // 2. Extract scheme_id to determine type
            if (!jsonElement.TryGetProperty("scheme_id", out var schemeIdProp))
            {
                // No scheme_id → cannot determine type
                return null;
            }
            
            var schemeId = schemeIdProp.GetInt64();
            
            // 3. Get real C# type via type resolver
            var propsType = SystemTextJsonRedbSerializer.ResolveType(schemeId);
            if (propsType == null)
            {
                // Type not registered → skip
                return null;
            }
            
            // 4. Create RedbObject&lt;TProps&gt; dynamically via reflection
            var redbObjectType = typeof(RedbObject<>).MakeGenericType(propsType);
            
            // 5. Deserialize JSON to concrete type
            // ⚠️ CRITICAL: Use new JsonSerializerOptions WITHOUT this converter
            // Otherwise infinite recursion occurs with nested IRedbObject
            var optionsWithoutThisConverter = new JsonSerializerOptions(options);
            optionsWithoutThisConverter.Converters.Clear();
            foreach (var converter in options.Converters)
            {
                if (converter is not PolymorphicRedbObjectConverter)
                {
                    optionsWithoutThisConverter.Converters.Add(converter);
                }
            }
            
            var obj = JsonSerializer.Deserialize(jsonElement.GetRawText(), redbObjectType, optionsWithoutThisConverter);
            
            return obj as IRedbObject;
        }
        
        public override void Write(Utf8JsonWriter writer, IRedbObject? value, JsonSerializerOptions options)
        {
            if (value == null)
            {
                writer.WriteNullValue();
                return;
            }
            
            // ⚠️ IRedbObject serialization may lead to circular references
            // For safety return null (navigation properties are not serialized)
            writer.WriteNullValue();
        }
    }

#if NET6_0_OR_GREATER
    /// <summary>
    /// Flexible converter for DateOnly - supports DateTime strings
    /// </summary>
    public class FlexibleDateOnlyConverter : JsonConverter<DateOnly>
    {
        public override DateOnly Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.String)
            {
                var dateString = reader.GetString();
                if (DateTime.TryParse(dateString, out var dateTime))
                {
                    return DateOnly.FromDateTime(dateTime);
                }
            }
            throw new JsonException($"Unable to convert '{reader.GetString()}' to DateOnly.");
        }

        public override void Write(Utf8JsonWriter writer, DateOnly value, JsonSerializerOptions options)
        {
            writer.WriteStringValue(value.ToString("yyyy-MM-dd"));
        }
    }

    /// <summary>
    /// Flexible converter for nullable DateOnly
    /// </summary>
    public class FlexibleNullableDateOnlyConverter : JsonConverter<DateOnly?>
    {
        public override DateOnly? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.Null)
                return null;
                
            if (reader.TokenType == JsonTokenType.String)
            {
                var dateString = reader.GetString();
                if (string.IsNullOrEmpty(dateString))
                    return null;
                    
                if (DateTime.TryParse(dateString, out var dateTime))
                {
                    return DateOnly.FromDateTime(dateTime);
                }
            }
            throw new JsonException($"Unable to convert '{reader.GetString()}' to DateOnly?.");
        }

        public override void Write(Utf8JsonWriter writer, DateOnly? value, JsonSerializerOptions options)
        {
            if (value.HasValue)
                writer.WriteStringValue(value.Value.ToString("yyyy-MM-dd"));
            else
                writer.WriteNullValue();
        }
    }

    /// <summary>
    /// Flexible converter for TimeOnly - supports TimeSpan strings
    /// </summary>
    public class FlexibleTimeOnlyConverter : JsonConverter<TimeOnly>
    {
        public override TimeOnly Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.String)
            {
                var timeString = reader.GetString();
                if (TimeSpan.TryParse(timeString, out var timeSpan))
                {
                    return TimeOnly.FromTimeSpan(timeSpan);
                }
            }
            throw new JsonException($"Unable to convert '{reader.GetString()}' to TimeOnly.");
        }

        public override void Write(Utf8JsonWriter writer, TimeOnly value, JsonSerializerOptions options)
        {
            writer.WriteStringValue(value.ToString("HH:mm:ss"));
        }
    }

    /// <summary>
    /// Flexible converter for nullable TimeOnly
    /// </summary>
    public class FlexibleNullableTimeOnlyConverter : JsonConverter<TimeOnly?>
    {
        public override TimeOnly? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.Null)
                return null;
                
            if (reader.TokenType == JsonTokenType.String)
            {
                var timeString = reader.GetString();
                if (string.IsNullOrEmpty(timeString))
                    return null;
                    
                if (TimeSpan.TryParse(timeString, out var timeSpan))
                {
                    return TimeOnly.FromTimeSpan(timeSpan);
                }
            }
            throw new JsonException($"Unable to convert '{reader.GetString()}' to TimeOnly?.");
        }

        public override void Write(Utf8JsonWriter writer, TimeOnly? value, JsonSerializerOptions options)
        {
            if (value.HasValue)
                writer.WriteStringValue(value.Value.ToString("HH:mm:ss"));
            else
                writer.WriteNullValue();
        }
    }
#endif

    /// <summary>
    /// Flexible converter for TimeSpan - supports strings
    /// </summary>
    public class FlexibleTimeSpanConverter : JsonConverter<TimeSpan>
    {
        public override TimeSpan Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.String)
            {
                var timeString = reader.GetString();
                if (TimeSpan.TryParse(timeString, out var timeSpan))
                {
                    return timeSpan;
                }
            }
            throw new JsonException($"Unable to convert '{reader.GetString()}' to TimeSpan.");
        }

        public override void Write(Utf8JsonWriter writer, TimeSpan value, JsonSerializerOptions options)
        {
            writer.WriteStringValue(value.ToString(@"hh\:mm\:ss"));
        }
    }

    /// <summary>
    /// Flexible converter for nullable TimeSpan
    /// </summary>
    public class FlexibleNullableTimeSpanConverter : JsonConverter<TimeSpan?>
    {
        public override TimeSpan? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.Null)
                return null;
                
            if (reader.TokenType == JsonTokenType.String)
            {
                var timeString = reader.GetString();
                if (string.IsNullOrEmpty(timeString))
                    return null;
                    
                if (TimeSpan.TryParse(timeString, out var timeSpan))
                {
                    return timeSpan;
                }
            }
            throw new JsonException($"Unable to convert '{reader.GetString()}' to TimeSpan?.");
        }

        public override void Write(Utf8JsonWriter writer, TimeSpan? value, JsonSerializerOptions options)
        {
            if (value.HasValue)
                writer.WriteStringValue(value.Value.ToString(@"hh\:mm\:ss"));
            else
                writer.WriteNullValue();
        }
    }

    /// <summary>
    /// ✅ DateTimeOffset converter for PostgreSQL timestamptz.
    /// </summary>
    public class PostgresDateTimeOffsetConverter : JsonConverter<DateTimeOffset>
    {
        public override DateTimeOffset Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.String)
            {
                var stringValue = reader.GetString();
                
                // PostgreSQL infinity support
                if (stringValue == "-infinity")
                    return DateTimeOffset.MinValue;
                if (stringValue == "infinity")
                    return DateTimeOffset.MaxValue;
                
                // Parse standard ISO8601 with timezone
                if (DateTimeOffset.TryParse(stringValue, out var result))
                {
                    return result.ToUniversalTime();
                }
            }
            
            if (reader.TokenType == JsonTokenType.Null)
                throw new JsonException("Cannot convert null to DateTimeOffset");
            
            return JsonSerializer.Deserialize<DateTimeOffset>(ref reader, options);
        }

        public override void Write(Utf8JsonWriter writer, DateTimeOffset value, JsonSerializerOptions options)
        {
            if (value == DateTimeOffset.MinValue)
                writer.WriteStringValue("-infinity");
            else if (value == DateTimeOffset.MaxValue)
                writer.WriteStringValue("infinity");
            else
                writer.WriteStringValue(value.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffffffzzz"));
        }
    }

    /// <summary>
    /// ✅ Nullable DateTimeOffset converter for PostgreSQL timestamptz.
    /// </summary>
    public class PostgresNullableDateTimeOffsetConverter : JsonConverter<DateTimeOffset?>
    {
        public override DateTimeOffset? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.Null)
                return null;
            
            if (reader.TokenType == JsonTokenType.String)
            {
                var stringValue = reader.GetString();
                
                if (stringValue == "-infinity")
                    return DateTimeOffset.MinValue;
                if (stringValue == "infinity")
                    return DateTimeOffset.MaxValue;
                
                if (DateTimeOffset.TryParse(stringValue, out var result))
                {
                    return result.ToUniversalTime();
                }
            }
            
            return JsonSerializer.Deserialize<DateTimeOffset?>(ref reader, options);
        }

        public override void Write(Utf8JsonWriter writer, DateTimeOffset? value, JsonSerializerOptions options)
        {
            if (value == null)
                writer.WriteNullValue();
            else if (value == DateTimeOffset.MinValue)
                writer.WriteStringValue("-infinity");
            else if (value == DateTimeOffset.MaxValue)
                writer.WriteStringValue("infinity");
            else
                writer.WriteStringValue(value.Value.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffffffzzz"));
        }
    }

    /// <summary>
    /// ✅ FIX FOR "-infinity" PROBLEM: DateTime converter for handling PostgreSQL "-infinity"
    /// Solves problem: The JSON value could not be converted to System.DateTime
    /// </summary>
    public class PostgresInfinityDateTimeConverter : JsonConverter<DateTime>
    {
        public override DateTime Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.String)
            {
                var stringValue = reader.GetString();
                
                // ✅ HANDLE PostgreSQL "-infinity"
                if (stringValue == "-infinity")
                {
                    return DateTime.MinValue;
                }
                
                // ✅ HANDLE PostgreSQL "infinity"  
                if (stringValue == "infinity")
                {
                    return DateTime.MaxValue;
                }

                // Regular DateTime deserialization
                if (DateTimeOffset.TryParse(stringValue, out var dateTimeOffset))
                {
                    return Utils.DateTimeConverter.DenormalizeFromStorage(dateTimeOffset);
                }
                
                throw new JsonException($"Cannot parse '{stringValue}' as DateTimeOffset.");
            }
            
           
            if (reader.TokenType == JsonTokenType.Null)
            {
                throw new JsonException("Cannot convert null to DateTime");
            }
            
            // Try standard deserialization
            var result = JsonSerializer.Deserialize<DateTime>(ref reader, options);
            return result;
        }

        public override void Write(Utf8JsonWriter writer, DateTime value, JsonSerializerOptions options)
        {
            // ✅ REVERSE CONVERSION: DateTime.MinValue → "-infinity" for PostgreSQL
            if (value == DateTime.MinValue)
            {
                writer.WriteStringValue("-infinity");
            }
            else if (value == DateTime.MaxValue)
            {
                writer.WriteStringValue("infinity");
            }
            else
            {
                // Standard serialization
                writer.WriteStringValue(value.ToString("yyyy-MM-ddTHH:mm:ss.fffffffK"));
            }
        }
    }

    /// <summary>
    /// ✅ FIX FOR "-infinity" PROBLEM: Nullable DateTime converter for handling PostgreSQL "-infinity"
    /// </summary>
    public class PostgresInfinityNullableDateTimeConverter : JsonConverter<DateTime?>
    {
        public override DateTime? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.Null)
            {
                return null;
            }
            
            if (reader.TokenType == JsonTokenType.String)
            {
                var stringValue = reader.GetString();
                
                // ✅ HANDLE PostgreSQL "-infinity"
                if (stringValue == "-infinity")
                {
                    return DateTime.MinValue;
                }
                
                // ✅ HANDLE PostgreSQL "infinity"  
                if (stringValue == "infinity")
                {
                    return DateTime.MaxValue;
                }
                
                // Standard DateTime deserialization
                if (DateTimeOffset.TryParse(stringValue, out var dateTime))
                {
                    // ✅ FIXED: Return UTC instead of Local
                    return Utils.DateTimeConverter.DenormalizeFromStorage(dateTime);
                }
            }
            
            // Try standard deserialization
            return JsonSerializer.Deserialize<DateTime?>(ref reader, options);
        }

        public override void Write(Utf8JsonWriter writer, DateTime? value, JsonSerializerOptions options)
        {
            if (!value.HasValue)
            {
                writer.WriteNullValue();
                return;
            }
            
            // ✅ REVERSE CONVERSION: DateTime.MinValue → "-infinity" for PostgreSQL
            if (value.Value == DateTime.MinValue)
            {
                writer.WriteStringValue("-infinity");
            }
            else if (value.Value == DateTime.MaxValue)
            {
                writer.WriteStringValue("infinity");
            }
            else
            {
                // Standard serialization
                writer.WriteStringValue(value.Value.ToString("yyyy-MM-ddTHH:mm:ss.fffffffK"));
            }
        }
    }

    /// <summary>
    /// Factory that creates converters to handle JSON null → default(T) for non-nullable value types.
    /// Fixes MSSQL deserialization where null values are explicitly included in JSON.
    /// </summary>
    public class NullToDefaultConverterFactory : JsonConverterFactory
    {
        private static readonly HashSet<Type> SupportedTypes = new()
        {
            typeof(int), typeof(long), typeof(short), typeof(byte),
            typeof(uint), typeof(ulong), typeof(ushort), typeof(sbyte),
            typeof(float), typeof(double), typeof(decimal),
            typeof(bool), typeof(char)
        };

        public override bool CanConvert(Type typeToConvert)
        {
            return SupportedTypes.Contains(typeToConvert);
        }

        public override JsonConverter? CreateConverter(Type typeToConvert, JsonSerializerOptions options)
        {
            var converterType = typeof(NullToDefaultConverter<>).MakeGenericType(typeToConvert);
            return (JsonConverter?)Activator.CreateInstance(converterType);
        }
    }

    /// <summary>
    /// Converter that returns default(T) when JSON contains null for a non-nullable value type.
    /// </summary>
    public class NullToDefaultConverter<T> : JsonConverter<T> where T : struct
    {
        public override T Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.Null)
            {
                return default;
            }

            // Handle numeric types
            if (typeof(T) == typeof(int)) return (T)(object)reader.GetInt32();
            if (typeof(T) == typeof(long)) return (T)(object)reader.GetInt64();
            if (typeof(T) == typeof(short)) return (T)(object)reader.GetInt16();
            if (typeof(T) == typeof(byte)) return (T)(object)reader.GetByte();
            if (typeof(T) == typeof(uint)) return (T)(object)reader.GetUInt32();
            if (typeof(T) == typeof(ulong)) return (T)(object)reader.GetUInt64();
            if (typeof(T) == typeof(ushort)) return (T)(object)reader.GetUInt16();
            if (typeof(T) == typeof(sbyte)) return (T)(object)reader.GetSByte();
            if (typeof(T) == typeof(float)) return (T)(object)reader.GetSingle();
            if (typeof(T) == typeof(double)) return (T)(object)reader.GetDouble();
            if (typeof(T) == typeof(decimal)) return (T)(object)reader.GetDecimal();
            if (typeof(T) == typeof(bool)) return (T)(object)reader.GetBoolean();
            if (typeof(T) == typeof(char))
            {
                var str = reader.GetString();
                return (T)(object)(str?.Length > 0 ? str[0] : default(char));
            }

            throw new JsonException($"Unsupported type {typeof(T).Name}");
        }

        public override void Write(Utf8JsonWriter writer, T value, JsonSerializerOptions options)
        {
            JsonSerializer.Serialize(writer, value, options);
        }
    }
}
