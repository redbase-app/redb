//using redb.Core.Utils;
//using System;
//using System.Text.Json.Serialization;

//namespace redb.Core.Models.Entities;

///// <summary>
///// RedbObject with primitive value stored directly in _objects._value_* columns
///// No _values records created. For simple scalar schemas like RedbPrimitive<long>, RedbPrimitive<string>
///// Value is stored in appropriate _value_* column based on type
///// </summary>
///// <typeparam name="T">Primitive type (long, string, Guid, bool, double, decimal, DateTimeOffset, byte[])</typeparam>
//public class RedbPrimitive<T> : RedbObject
//{
//    /// <summary>
//    /// Primitive value stored in _objects._value_* column (which column depends on type T)
//    /// </summary>
//    [JsonPropertyName("value")]
//    public T? Value 
//    { 
//        get => GetPrimitiveValue();
//        set => SetPrimitiveValue(value);
//    }
    
//    private T? GetPrimitiveValue()
//    {
//        var type = typeof(T);
        
//        if (type == typeof(long) || type == typeof(int) || type == typeof(short) || type == typeof(byte))
//            return ValueLong.HasValue ? (T)(object)ValueLong.Value : default;
        
//        if (type == typeof(string))
//            return (T)(object?)ValueString;
        
//        if (type == typeof(Guid))
//            return ValueGuid.HasValue ? (T)(object)ValueGuid.Value : default;
        
//        if (type == typeof(bool))
//            return ValueBool.HasValue ? (T)(object)ValueBool.Value : default;
        
//        if (type == typeof(double) || type == typeof(float))
//            return ValueDouble.HasValue ? (T)(object)ValueDouble.Value : default;
        
//        if (type == typeof(decimal))
//            return ValueNumeric.HasValue ? (T)(object)ValueNumeric.Value : default;
        
//        if (type == typeof(DateTimeOffset) || type == typeof(DateTime))
//            return ValueDatetime.HasValue ? (T)(object)ValueDatetime.Value : default;
        
//        if (type == typeof(byte[]))
//            return (T)(object?)ValueBytes;
        
//        throw new NotSupportedException($"Type {typeof(T).Name} is not supported for RedbPrimitive");
//    }
    
//    private void SetPrimitiveValue(T? value)
//    {
//        if (value == null)
//        {
//            // Clear all value columns
//            ValueLong = null;
//            ValueString = null;
//            ValueGuid = null;
//            ValueBool = null;
//            ValueDouble = null;
//            ValueNumeric = null;
//            ValueDatetime = null;
//            ValueBytes = null;
//            return;
//        }
        
//        switch (value)
//        {
//            case long v:
//                ValueLong = v;
//                break;
//            case int v:
//                ValueLong = v;
//                break;
//            case short v:
//                ValueLong = v;
//                break;
//            case byte v:
//                ValueLong = v;
//                break;
//            case string v:
//                ValueString = v;
//                break;
//            case Guid v:
//                ValueGuid = v;
//                break;
//            case bool v:
//                ValueBool = v;
//                break;
//            case double v:
//                ValueDouble = v;
//                break;
//            case float v:
//                ValueDouble = v;
//                break;
//            case decimal v:
//                ValueNumeric = v;
//                break;
//            case DateTimeOffset v:
//                ValueDatetime = v;
//                break;
//            case DateTime v:
//                ValueDatetime = v;
//                break;
//            case byte[] v:
//                ValueBytes = v;
//                break;
//            default:
//                throw new NotSupportedException($"Type {typeof(T).Name} is not supported for RedbPrimitive");
//        }
//    }

//    /// <summary>
//    /// Recompute hash based on primitive value and store in hash field
//    /// </summary>
//    public override void RecomputeHash()
//    {
//        hash = ComputeHash();
//    }

//    /// <summary>
//    /// Compute MD5 hash based on primitive value without changing hash field
//    /// </summary>
//    public override Guid ComputeHash()
//    {
//        var value = Value;
//        if (value == null)
//            return Guid.Empty;
        
//        var bytes = System.Text.Encoding.UTF8.GetBytes(value.ToString() ?? string.Empty);
//        using var md5 = System.Security.Cryptography.MD5.Create();
//        var hashBytes = md5.ComputeHash(bytes);
//        return new Guid(hashBytes);
//    }
//}

