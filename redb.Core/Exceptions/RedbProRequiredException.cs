namespace redb.Core.Exceptions;

/// <summary>
/// Thrown when a Pro-only feature is invoked on a Free edition instance.
/// </summary>
public class RedbProRequiredException : Exception
{
    public string Feature { get; }

    public RedbProRequiredException(string feature)
        : base($"Feature '{feature}' requires redb Pro edition.")
    {
        Feature = feature;
    }
}
