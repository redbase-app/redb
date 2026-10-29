Schedules (`redb.Route.Quartz`), `routes/schedules.route.xml`: `cron://modulectx-report?schedule=...&amp;timeZone=...`
runs a job on a Quartz cron expression (six or seven fields: seconds minutes hours day-of-month month day-of-week
[year], one of the two day fields `?`); `qtimer://modulectx-heartbeat?period=...` is an interval on the same
scheduler. Both have `stateful=true` (no overlapping runs).

Facts to keep: the job key is the group and the URI path (`redb.modulectx-report`), so it is stable across restarts
and nodes, and one path has one consumer; `misfireInstruction` decides what happens to runs missed while the worker
was down (`CronDoNothing`, `CronFireOnceNow`); the scheduler and its store (in memory, or a shared database for a
cluster) belong to the worker, not to the module; `timer://` is not Quartz and fires on every node.

Usually changed: the schedule, the time zone, the work of the job.

`routes/CleanupJobRoute.cs` is a C# job on its own cron name and schedule (`cleanup.schedule`), same time zone key
and misfire rule; the work is C# code in a `Process` step. Every job needs its own name after `cron://`.
