using System.Diagnostics;
using AwesomeAssertions;
using redb.Core.Diagnostics;
using Xunit;

namespace redb.Tests.Integration.Tests.Diagnostics;

/// <summary>
/// The <c>redb.Core</c> ActivitySource: one span per database command, carrying the operation name and
/// the database system only. The source is inert without a subscriber, so nothing is emitted by default.
/// Asserted through an <see cref="ActivityListener"/>, the way redb.Route reads its own spans.
/// </summary>
public class RedbTelemetryTests
{
    private static (ActivityListener Listener, List<Activity> Spans) Listen()
    {
        var spans = new List<Activity>();
        var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == RedbActivitySource.SourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = spans.Add,
        };
        ActivitySource.AddActivityListener(listener);
        return (listener, spans);
    }

    [Fact]
    public void WithNoSubscriber_TheSourceIsOff()
    {
        RedbActivitySource.SourceName.Should().Be("redb.Core");
        RedbTelemetry.Enabled.Should().BeFalse("nothing subscribes, so a command is run straight through");
    }

    [Fact]
    public async Task AQuery_OpensOneSpan_CarryingOnlyTheOperation()
    {
        var (listener, spans) = Listen();
        try
        {
            var rows = await RedbTelemetry.RunAsync(
                RedbTelemetry.Query, () => Task.FromResult(new List<int> { 1, 2, 3 }));

            rows.Should().Equal(1, 2, 3);

            var span = spans.Should().ContainSingle().Which;
            span.DisplayName.Should().Be("redb.query");
            span.GetTagItem("db.system").Should().Be("redb");
            span.GetTagItem("db.operation").Should().Be("query");
            span.GetTagItem("db.statement").Should().BeNull("the statement text is never captured");
        }
        finally
        {
            listener.Dispose();
        }
    }

    [Fact]
    public async Task AFailingCommand_MarksTheSpanAnError_AndRethrows()
    {
        var (listener, spans) = Listen();
        try
        {
            var act = () => RedbTelemetry.RunAsync<int>(
                RedbTelemetry.Execute, () => Task.FromException<int>(new InvalidOperationException("boom")));

            (await act.Should().ThrowAsync<InvalidOperationException>()).WithMessage("boom");

            spans.Should().ContainSingle().Which.Status.Should().Be(ActivityStatusCode.Error);
        }
        finally
        {
            listener.Dispose();
        }
    }

    [Fact]
    public async Task ACancelledCommand_DoesNotMarkTheSpanAnError()
    {
        var (listener, spans) = Listen();
        try
        {
            var act = () => RedbTelemetry.RunAsync<int>(
                RedbTelemetry.Query, () => Task.FromException<int>(new OperationCanceledException()));

            await act.Should().ThrowAsync<OperationCanceledException>();

            spans.Should().ContainSingle().Which.Status.Should().NotBe(ActivityStatusCode.Error);
        }
        finally
        {
            listener.Dispose();
        }
    }
}
