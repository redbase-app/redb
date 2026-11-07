using System;

namespace redb.Core.Exceptions;

/// <summary>
/// <c>_objects._value_string</c> is an identifier column (owner decision 2026-09-02): 450
/// characters, the MSSQL index-key width, enforced in C# so the text-typed PostgreSQL and SQLite
/// columns hold the same contract. Long text belongs in <c>_note</c> or in a Props field.
/// </summary>
public class RedbValueStringTooLongException : InvalidOperationException
{
    /// <summary>Id of the object whose value was rejected (0 for a new object).</summary>
    public long ObjectId { get; }

    /// <summary>The rejected value's length.</summary>
    public int Length { get; }

    public RedbValueStringTooLongException(long objectId, int length)
        : base($"object id={objectId}: _value_string is {length} characters, the limit is 450. " +
               "The column is for identifiers and external keys; long text belongs in _note or in a Props field.")
    {
        ObjectId = objectId;
        Length = length;
    }
}
