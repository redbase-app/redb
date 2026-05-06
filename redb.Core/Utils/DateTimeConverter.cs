using System;

namespace redb.Core.Utils
{
    /// <summary>
    /// Centralized DateTime conversion logic.
    /// RULE: DateTime is ALWAYS treated as UTC in REDB system.
    /// 
    /// CONTRACT:
    /// - On save: any DateTime → UTC (via NormalizeForStorage)
    /// - On read: DateTimeOffset from DB → DateTime with Kind=Utc (via DenormalizeFromStorage)
    /// - On search: any DateTime → UTC (via NormalizeForStorage)
    /// - On JSON: DateTimeOffset → DateTime with Kind=Utc (via DenormalizeFromStorage)
    /// 
    /// IMPORTANT: DateTimeOffset remains as is (preserves timezone information)
    /// </summary>
    public static class DateTimeConverter
    {
        /// <summary>
        /// Normalize DateTime for saving to DB.
        /// DateTime.Unspecified and DateTime.Local are converted to UTC.
        /// 
        /// Examples:
        /// - new DateTime(2025, 11, 16) [Unspecified] → 2025-11-16 00:00:00 UTC
        /// - DateTime.Now [Local MSK 14:00] → UTC 11:00
        /// - DateTime.UtcNow [Utc] → no changes
        /// </summary>
        /// <param name="dateTime">Original DateTime value</param>
        /// <returns>DateTime with Kind=Utc</returns>
        public static DateTime NormalizeForStorage(DateTime dateTime)
        {
            return DateTime.SpecifyKind(dateTime, DateTimeKind.Utc);
            //return dateTime.Kind switch
            //{
            //    DateTimeKind.Utc => dateTime,
            //    DateTimeKind.Local => dateTime.ToUniversalTime(),
            //    // Treat Unspecified as UTC (NOT as Local!)
            //    DateTimeKind.Unspecified => DateTime.SpecifyKind(dateTime, DateTimeKind.Utc),
            //    _ => throw new ArgumentException($"Unknown DateTimeKind: {dateTime.Kind}")
            //};
        }
        
        /// <summary>
        /// Denormalize DateTime from DB for returning to user.
        /// Returns DateTime with Kind = Utc.
        /// 
        /// Example:
        /// - DB: 2025-11-16 00:00:00+00 → DateTime(2025, 11, 16, 0, 0, 0, DateTimeKind.Utc)
        /// </summary>
        /// <param name="dateTimeOffset">DateTimeOffset from DB (timestamptz)</param>
        /// <returns>DateTime with Kind=Utc</returns>
        public static DateTime DenormalizeFromStorage(DateTimeOffset dateTimeOffset)
        {
            return DateTime.SpecifyKind(dateTimeOffset.UtcDateTime, DateTimeKind.Utc);
        }
        
        /// <summary>
        /// Parse DateTime from string (JSON/SQL) as UTC.
        /// 
        /// Example:
        /// - "2025-11-16T12:00:00Z" → DateTime(2025, 11, 16, 12, 0, 0, DateTimeKind.Utc)
        /// - "2025-11-16T12:00:00+03:00" → DateTime(2025, 11, 16, 9, 0, 0, DateTimeKind.Utc)
        /// </summary>
        /// <param name="value">String with date/time</param>
        /// <returns>DateTime with Kind=Utc</returns>
        /// <exception cref="FormatException">If string cannot be parsed</exception>
        public static DateTime ParseAsUtc(string value)
        {
            if (DateTimeOffset.TryParse(value, out var dto))
            {
                return DateTime.SpecifyKind(dto.UtcDateTime, DateTimeKind.Utc);
            }
            throw new FormatException($"Cannot parse '{value}' as DateTime");
        }
    }
}

