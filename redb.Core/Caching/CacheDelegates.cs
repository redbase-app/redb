using System;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using redb.Core.Models.Contracts;
using redb.Core.Utils;

namespace redb.Core.Caching
{
    /// <summary>
    /// Typed cache calls for an object type met at run time. A batch load and a graph write used to run
    /// <c>GetMethod</c> + <c>MakeGenericMethod</c> + <c>Invoke</c> on every object; here the call is built once per type
    /// and kept in a <see cref="ConditionalWeakTable{TKey,TValue}"/>: a type unloaded with its collectible load context
    /// takes its delegate with it, so Tsak hot reload releases the assembly.
    /// </summary>
    internal static class CacheDelegates
    {
        private static readonly ConditionalWeakTable<Type, Action<IRedbObjectCache, IRedbObject>> _set = new();
        private static readonly ConditionalWeakTable<Type, Action<GlobalPropsCache, IRedbObject>> _setGraph = new();
        private static readonly ConditionalWeakTable<Type, Func<GlobalPropsCache, long, Guid, IRedbObject?>> _get = new();

        /// <summary>Write the object by its own id (the Set of the cache interface). A non-generic RedbObject carries
        /// no Props and was never an entry of its own.</summary>
        public static void Set(IRedbObjectCache cache, IRedbObject obj)
        {
            if (RedbObjectTypes.GenericOf(obj.GetType()) == null) return;
            _set.GetValue(obj.GetType(), CreateSet)(cache, obj);
        }

        /// <summary>Write the object and every loaded object under it in one walk.</summary>
        public static void SetGraph(GlobalPropsCache cache, IRedbObject obj)
        {
            if (RedbObjectTypes.GenericOf(obj.GetType()) == null) return;
            _setGraph.GetValue(obj.GetType(), CreateSetGraph)(cache, obj);
        }

        /// <summary>Read the object by id and hash; null when the cache has no current entry.</summary>
        public static IRedbObject? Get(GlobalPropsCache cache, Type propsType, long objectId, Guid hash)
            => _get.GetValue(propsType, CreateGet)(cache, objectId, hash);

        private static Action<IRedbObjectCache, IRedbObject> CreateSet(Type objectType)
        {
            var redbType = RedbObjectTypes.GenericOf(objectType)!;
            var set = typeof(IRedbObjectCache).GetMethod(nameof(IRedbObjectCache.Set))!
                .MakeGenericMethod(redbType.GetGenericArguments()[0]);

            var cache = Expression.Parameter(typeof(IRedbObjectCache), "cache");
            var obj = Expression.Parameter(typeof(IRedbObject), "obj");
            var call = Expression.Call(cache, set, Expression.Convert(obj, redbType));
            return Expression.Lambda<Action<IRedbObjectCache, IRedbObject>>(call, cache, obj).Compile();
        }

        private static Action<GlobalPropsCache, IRedbObject> CreateSetGraph(Type objectType)
        {
            var redbType = RedbObjectTypes.GenericOf(objectType)!;
            var setGraph = typeof(GlobalPropsCache).GetMethod(nameof(GlobalPropsCache.SetGraph))!
                .MakeGenericMethod(redbType.GetGenericArguments()[0]);

            var cache = Expression.Parameter(typeof(GlobalPropsCache), "cache");
            var obj = Expression.Parameter(typeof(IRedbObject), "obj");
            var call = Expression.Call(cache, setGraph, Expression.Convert(obj, redbType));
            return Expression.Lambda<Action<GlobalPropsCache, IRedbObject>>(call, cache, obj).Compile();
        }

        private static Func<GlobalPropsCache, long, Guid, IRedbObject?> CreateGet(Type propsType)
        {
            var get = typeof(GlobalPropsCache).GetMethod(nameof(GlobalPropsCache.Get))!
                .MakeGenericMethod(propsType);

            var cache = Expression.Parameter(typeof(GlobalPropsCache), "cache");
            var objectId = Expression.Parameter(typeof(long), "objectId");
            var hash = Expression.Parameter(typeof(Guid), "hash");
            var call = Expression.Call(cache, get, objectId, hash);
            var body = Expression.Convert(call, typeof(IRedbObject));
            return Expression.Lambda<Func<GlobalPropsCache, long, Guid, IRedbObject?>>(body, cache, objectId, hash)
                .Compile();
        }
    }
}
