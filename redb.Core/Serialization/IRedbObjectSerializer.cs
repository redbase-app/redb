using System;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;

namespace redb.Core.Serialization
{
    // Abstraction for JSON object serialization from DB to typed wrapper
    public interface IRedbObjectSerializer
    {
        RedbObject<TProps> Deserialize<TProps>(string json) where TProps : class, new();
        
        /// <summary>
        /// Dynamic JSON deserialization to typed object based on runtime type
        /// </summary>
        /// <param name="json">JSON string</param>
        /// <param name="propsType">Type of properties for deserialization</param>
        /// <returns>Deserialized object as interface</returns>
        IRedbObject DeserializeDynamic(string json, Type propsType);
        
        /// <summary>
        /// Polymorphic JSON deserialization to RedbObject based on runtime type
        /// Used for tree structures with different node types  
        /// (conversion to TreeRedbObject happens at provider level)
        /// </summary>
        /// <param name="json">JSON string from v_objects_json</param>
        /// <param name="propsType">Type of properties for deserialization</param>
        /// <returns>Deserialized RedbObject or null</returns>
        RedbObject? DeserializeRedbDynamic(string json, Type propsType);
    }
}
