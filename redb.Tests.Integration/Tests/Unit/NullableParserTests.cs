using System.Linq.Expressions;
using redb.Core.Models.Contracts;
using redb.Core.Query.Parsing;
using redb.Core.Query.QueryExpressions;

namespace redb.Tests.Integration.Tests.Unit;

/// <summary>
/// Unit tests for Nullable&lt;T&gt;.Value / .HasValue handling in expression parser.
/// No database required — tests pure expression tree parsing.
/// </summary>
public class NullableParserTests
{
    private readonly FilterExpressionParser _parser = new();

    [Fact]
    public void ParentId_Value_Equal_ShouldParseTo_ParentId_BaseField()
    {
        Expression<Func<IRedbObject, bool>> expr = o => o.ParentId.Value == 42;
        var result = _parser.ParseRedbFilter(expr);

        var comparison = Assert.IsType<ComparisonExpression>(result);
        Assert.Equal("ParentId", comparison.Property.Name);
        Assert.True(comparison.Property.IsBaseField);
        Assert.Equal(ComparisonOperator.Equal, comparison.Operator);
        Assert.Equal(42L, comparison.Value);
    }

    [Fact]
    public void ParentId_HasValue_ShouldParseTo_IsNotNull()
    {
        Expression<Func<IRedbObject, bool>> expr = o => o.ParentId.HasValue;
        var result = _parser.ParseRedbFilter(expr);

        var nullCheck = Assert.IsType<NullCheckExpression>(result);
        Assert.Equal("ParentId", nullCheck.Property.Name);
        Assert.True(nullCheck.Property.IsBaseField);
        Assert.False(nullCheck.IsNull); // HasValue = true → IS NOT NULL
    }

    [Fact]
    public void Not_ParentId_HasValue_ShouldParseTo_IsNull()
    {
        Expression<Func<IRedbObject, bool>> expr = o => !o.ParentId.HasValue;
        var result = _parser.ParseRedbFilter(expr);

        var nullCheck = Assert.IsType<NullCheckExpression>(result);
        Assert.Equal("ParentId", nullCheck.Property.Name);
        Assert.True(nullCheck.Property.IsBaseField);
        Assert.True(nullCheck.IsNull); // !HasValue → IS NULL
    }

    [Fact]
    public void ParentId_EqualNull_ShouldParseTo_IsNull()
    {
        Expression<Func<IRedbObject, bool>> expr = o => o.ParentId == null;
        var result = _parser.ParseRedbFilter(expr);

        var nullCheck = Assert.IsType<NullCheckExpression>(result);
        Assert.Equal("ParentId", nullCheck.Property.Name);
        Assert.True(nullCheck.Property.IsBaseField);
        Assert.True(nullCheck.IsNull);
    }

    [Fact]
    public void ValueLong_Value_GreaterThan_ShouldParseTo_ValueLong_BaseField()
    {
        Expression<Func<IRedbObject, bool>> expr = o => o.ValueLong.Value > 50;
        var result = _parser.ParseRedbFilter(expr);

        var comparison = Assert.IsType<ComparisonExpression>(result);
        Assert.Equal("ValueLong", comparison.Property.Name);
        Assert.True(comparison.Property.IsBaseField);
        Assert.Equal(ComparisonOperator.GreaterThan, comparison.Operator);
    }

    [Fact]
    public void Key_HasValue_ShouldParseTo_IsNotNull()
    {
        Expression<Func<IRedbObject, bool>> expr = o => o.Key.HasValue;
        var result = _parser.ParseRedbFilter(expr);

        var nullCheck = Assert.IsType<NullCheckExpression>(result);
        Assert.Equal("Key", nullCheck.Property.Name);
        Assert.True(nullCheck.Property.IsBaseField);
        Assert.False(nullCheck.IsNull);
    }

    [Fact]
    public void HasValue_And_Value_Equal_ShouldParseToLogical()
    {
        long target = 42;
        Expression<Func<IRedbObject, bool>> expr = o => o.ParentId.HasValue && o.ParentId.Value == target;
        var result = _parser.ParseRedbFilter(expr);

        var logical = Assert.IsType<LogicalExpression>(result);
        Assert.Equal(LogicalOperator.And, logical.Operator);
        Assert.Equal(2, logical.Operands.Count);

        var nullCheck = Assert.IsType<NullCheckExpression>(logical.Operands[0]);
        Assert.Equal("ParentId", nullCheck.Property.Name);
        Assert.False(nullCheck.IsNull);

        var comparison = Assert.IsType<ComparisonExpression>(logical.Operands[1]);
        Assert.Equal("ParentId", comparison.Property.Name);
        Assert.Equal(ComparisonOperator.Equal, comparison.Operator);
    }
}
