namespace redb.Examples.Ef;

/// <summary>
/// Values written to <c>objects._id_scheme</c> by the EF twins. An EF application has no redb schemes, but
/// the column needs a value; a fixed one per Props type keeps rows of different types apart in the shared
/// header table. A header without Props (a plain object) gets <see cref="None"/>.
/// </summary>
internal static class EfSchemeIds
{
    public const long None = 0;
    public const long Employee = 1;
    public const long ProjectMetrics = 2;
    public const long Department = 3;
    public const long City = 4;
    public const long Person = 5;
}
