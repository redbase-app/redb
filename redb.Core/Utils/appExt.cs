using System.Collections;
using System.Linq.Expressions;

namespace redb.Core.Utils
{
    public static class appExt
    {
        public static void forEach<T>(this IEnumerable d, Action<T> a)
        {
            foreach (object? i in d) { a((T)i); }
        }

#pragma warning disable CS8600, CS8602, CS8603, CS8604
        public static void forEach<K, V>(this IDictionary source, Action<K, V?> action) => 
            source.forEach<DictionaryEntry>(i=>action((K)i.Key,(V?)i.Value));
        
        /// <summary>
        /// Filter IQueryable by property name dynamically.
        /// Uses expression trees instead of EF.Property.
        /// </summary>
        public static IQueryable<T> Filter<T>(this IQueryable<T> query, string propertyName, object propertyValue)
        {
            var parameter = Expression.Parameter(typeof(T), "e");
            var property = Expression.Property(parameter, propertyName);
            var value = Expression.Constant(propertyValue);
            var equals = Expression.Equal(property, Expression.Convert(value, property.Type));
            var lambda = Expression.Lambda<Func<T, bool>>(equals, parameter);
            return query.Where(lambda);
        }
#pragma warning restore CS8600, CS8602, CS8603, CS8604
    }
}
