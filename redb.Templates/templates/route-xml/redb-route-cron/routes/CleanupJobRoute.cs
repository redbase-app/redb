using redb.Route.Core;

namespace RouteModule.Routes;

/// <summary>
/// A sibling of routes/schedules.route.xml in C#: a cleanup job on a cron schedule of its own, on the same Quartz
/// scheduler and in the same time zone. The job works out in C# which day it cleans up.
/// </summary>
public sealed class CleanupJobRoute : RouteBuilder
{
    protected override void Configure()
    {
        // At second 15 of every minute by default; a real cleanup would be "0 30 2 * * ?", at half past two at night.
        // The time zone, stateful and misfireInstruction as in the XML report job.
        From("cron://cs-modulectx-cleanup?schedule={{cleanup.schedule:15 * * * * ?}}&timeZone={{report.timeZone:UTC}}&stateful=true&misfireInstruction=CronDoNothing")
            .RouteId("cs-modulectx-cleanup")
            .Description("Runs the cleanup job on a cron schedule")
            .Process(exchange =>
            {
                // The day whose leftovers the job removes: everything older than yesterday, in UTC.
                var cutoff = DateOnly.FromDateTime(DateTime.UtcNow).AddDays(-1);
                exchange.In.Body = cutoff.ToString("yyyy-MM-dd");
            })
            .Log("cleanup job: removing what is older than ${body}");
    }
}
