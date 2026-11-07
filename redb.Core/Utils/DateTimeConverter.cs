using System;

namespace redb.Core.Utils
{
    /// <summary>
    /// Centralized DateTime conversion logic.
    ///
    /// RULE: in REDB a <see cref="DateTime"/> carries NO time zone. The clock reading is the
    /// datum and is preserved end to end: 14:00 written is 14:00 read, on any machine, in any
    /// zone, through any read path. Kind is set to Utc purely so the value has one unambiguous
    /// representation on the wire and in the column; it is a label, not a conversion.
    ///
    /// CONTRACT (all three points use the SAME transform, which is why they agree):
    /// - On save:   DateTime → same reading, Kind=Utc (NormalizeForStorage)
    /// - On search: DateTime → same reading, Kind=Utc (NormalizeForStorage)
    /// - On read:   DateTimeOffset from DB → same reading, Kind=Utc (DenormalizeFromStorage)
    ///
    /// IMPORTANT: <see cref="DateTimeOffset"/> is a different contract and is NOT touched. It is
    /// the native .NET type and keeps native semantics: it carries a real instant with an offset.
    /// Base object fields (_date_create, _date_modify, _date_begin, _date_complete) are
    /// DateTimeOffset and are therefore instants, matching the DB-side now() defaults.
    /// Use DateTime for zone-less business data, DateTimeOffset when the moment matters.
    /// </summary>
    public static class DateTimeConverter
    {
        /// <summary>
        /// Normalize a <see cref="DateTime"/> for storage: relabel the clock reading as UTC
        /// WITHOUT shifting it. This is deliberate, not an oversight.
        ///
        /// <para>
        /// In REDB a <see cref="DateTime"/> carries no zone. The number on the clock is the datum,
        /// and it must survive unchanged: 14:00 written on any machine is 14:00 read on any other.
        /// Converting Local to UTC here would make the stored value depend on where the writer
        /// happened to run, which is exactly what this type is defined not to do. Code that needs
        /// a zone-aware instant uses <see cref="DateTimeOffset"/>, which keeps native .NET
        /// semantics throughout REDB.
        /// </para>
        ///
        /// Examples:
        /// - new DateTime(2025, 11, 16) [Unspecified] → 2025-11-16 00:00:00, Kind=Utc
        /// - DateTime.Now [Local MSK 14:00] → 14:00, Kind=Utc (the reading, not the instant)
        /// - DateTime.UtcNow [Utc] → unchanged
        /// </summary>
        /// <param name="dateTime">Original DateTime value</param>
        /// <returns>The same clock reading, with Kind=Utc</returns>
        public static DateTime NormalizeForStorage(DateTime dateTime)
        {
            return DateTime.SpecifyKind(dateTime, DateTimeKind.Utc);
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

