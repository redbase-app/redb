using System;
using System.Text.Json;
using System.Text.Json.Serialization;
using redb.Core.Models.Configuration;

namespace redb.Core.Configuration
{
    /// <summary>
    /// JsonConverter for ObjectIdResetStrategy
    /// </summary>
    public class ObjectIdResetStrategyJsonConverter : JsonConverter<ObjectIdResetStrategy>
    {
        public override ObjectIdResetStrategy Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            var value = reader.GetString();
            return value?.ToLowerInvariant() switch
            {
                "manual" => ObjectIdResetStrategy.Manual,
                "autoresetondelete" => ObjectIdResetStrategy.AutoResetOnDelete,
                "autocreatenewonsave" => ObjectIdResetStrategy.AutoCreateNewOnSave,
                _ => throw new JsonException($"Unknown ObjectIdResetStrategy value: {value}")
            };
        }

        public override void Write(Utf8JsonWriter writer, ObjectIdResetStrategy value, JsonSerializerOptions options)
        {
            var stringValue = value switch
            {
                ObjectIdResetStrategy.Manual => "Manual",
                ObjectIdResetStrategy.AutoResetOnDelete => "AutoResetOnDelete",
                ObjectIdResetStrategy.AutoCreateNewOnSave => "AutoCreateNewOnSave",
                _ => throw new JsonException($"Unknown ObjectIdResetStrategy value: {value}")
            };
            writer.WriteStringValue(stringValue);
        }
    }

    /// <summary>
    /// JsonConverter for MissingObjectStrategy
    /// </summary>
    public class MissingObjectStrategyJsonConverter : JsonConverter<MissingObjectStrategy>
    {
        public override MissingObjectStrategy Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            var value = reader.GetString();
            return value?.ToLowerInvariant() switch
            {
                "throwexception" => MissingObjectStrategy.ThrowException,
                "autoswitchtoinsert" => MissingObjectStrategy.AutoSwitchToInsert,
                "returnnull" => MissingObjectStrategy.ReturnNull,
                _ => throw new JsonException($"Unknown MissingObjectStrategy value: {value}")
            };
        }

        public override void Write(Utf8JsonWriter writer, MissingObjectStrategy value, JsonSerializerOptions options)
        {
            var stringValue = value switch
            {
                MissingObjectStrategy.ThrowException => "ThrowException",
                MissingObjectStrategy.AutoSwitchToInsert => "AutoSwitchToInsert",
                MissingObjectStrategy.ReturnNull => "ReturnNull",
                _ => throw new JsonException($"Unknown MissingObjectStrategy value: {value}")
            };
            writer.WriteStringValue(stringValue);
        }
    }

    /// <summary>
    /// JsonConverter for PropsSaveStrategy
    /// </summary>
    public class PropsSaveStrategyJsonConverter : JsonConverter<PropsSaveStrategy>
    {
        public override PropsSaveStrategy Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            var value = reader.GetString();
            return value?.ToLowerInvariant() switch
            {
                "deleteinsert" => PropsSaveStrategy.DeleteInsert,
                "changetracking" => PropsSaveStrategy.ChangeTracking,
                _ => throw new JsonException($"Unknown PropsSaveStrategy value: {value}")
            };
        }

        public override void Write(Utf8JsonWriter writer, PropsSaveStrategy value, JsonSerializerOptions options)
        {
            var stringValue = value switch
            {
                PropsSaveStrategy.DeleteInsert => "DeleteInsert",
                PropsSaveStrategy.ChangeTracking => "ChangeTracking",
                _ => throw new JsonException($"Unknown PropsSaveStrategy value: {value}")
            };
            writer.WriteStringValue(stringValue);
        }
    }

    // SecurityContextPriorityJsonConverter removed - priorities are no longer used

    /// <summary>
    /// JsonSerializer settings for RedbService configuration
    /// </summary>
    public static class RedbConfigurationJsonOptions
    {
        /// <summary>
        /// Get JsonSerializer settings with all converters support
        /// </summary>
        public static JsonSerializerOptions GetJsonSerializerOptions()
        {
            var options = new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true,
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                WriteIndented = true,
                AllowTrailingCommas = true,
                ReadCommentHandling = JsonCommentHandling.Skip
            };

            // Add converters for enums
            options.Converters.Add(new ObjectIdResetStrategyJsonConverter());
            options.Converters.Add(new MissingObjectStrategyJsonConverter());
            options.Converters.Add(new PropsSaveStrategyJsonConverter());
            // options.Converters.Add(new SecurityContextPriorityJsonConverter()); // Removed
            options.Converters.Add(new JsonStringEnumConverter(JsonNamingPolicy.CamelCase));

            return options;
        }
    }
}
