using redb.Core.Data;

namespace redb.Tests.Integration.Tests.Unit;

/// <summary>
/// The teardown wait budget follows the connection's own command timeout (owner decision,
/// 2026-09-11: "30 seconds is sometimes just not enough - take it from the connection
/// parameter, default otherwise"): a command may legally run right up to its timeout, so
/// teardown waits that long plus slack; an infinite command timeout still gets a finite,
/// generous budget - disposal must end.
/// </summary>
public class CommandGateBudgetTests
{
    [Theory]
    [InlineData(600, 605)]
    [InlineData(30, 35)]
    [InlineData(1, 6)]
    public void Budget_FollowsTheCommandTimeout_PlusSlack(int timeoutSeconds, int expectedSeconds)
        => CommandGate.BudgetFrom(timeoutSeconds).Should().Be(TimeSpan.FromSeconds(expectedSeconds));

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void InfiniteCommandTimeout_StillGetsAFiniteBudget(int timeoutSeconds)
        => CommandGate.BudgetFrom(timeoutSeconds).Should().Be(TimeSpan.FromMinutes(10));
}
