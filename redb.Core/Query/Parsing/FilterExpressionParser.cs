using System.Linq.Expressions;
using redb.Core.Exceptions;

namespace redb.Core.Query.Parsing;

/// <summary>
/// OpenSource implementation of FilterExpressionParser.
/// Inherits all parsing logic from BaseFilterExpressionParser.
/// Checks for Pro-only features and throws if detected.
/// </summary>
public class FilterExpressionParser : BaseFilterExpressionParser
{
    /// <summary>
    /// OpenSource: check for Pro-only features and throw if detected.
    /// Pro version overrides this method to allow all features.
    /// </summary>
    protected override void CheckProOnlyFeatures(Expression body, string context)
    {
        RedbProRequiredException.ThrowIfProRequired(body, context);
    }
}

