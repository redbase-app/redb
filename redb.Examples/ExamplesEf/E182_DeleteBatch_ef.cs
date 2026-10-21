using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Core.Models.Entities;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;
using Biz = redb.Examples.Models;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E182_DeleteBatch"/>: five employees saved, loaded as whole objects, deleted one by
/// one (the cascade removes their Props rows) and looked up again.
/// </summary>
[ExampleMeta("E182_ef", "EF: Delete - Batch", "Core", ExampleTier.Free, 1, "EF", "Delete")]
public class E182_DeleteBatch_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();

        var batchId = DateTime.UtcNow.Ticks;
        var headers = Enumerable.Range(0, 5).Select(i => EmployeeEfMapping.ToEntity(new RedbObject<Biz.EmployeeProps>
        {
            name = $"BatchDelete_{batchId}_{i}",
            Props = new Biz.EmployeeProps
            {
                FirstName = $"Temp{i}",
                LastName = "Employee",
                Age = 20 + i,
                Salary = 40000m + i * 1000,
                Department = "Test"
            }
        })).ToList();
        await using (var db = await contexts.CreateDbContextAsync())
        {
            db.objects.AddRange(headers);
            await db.SaveChangesAsync();
        }
        var ids = headers.Select(h => h._id).ToList();

        int countBefore;
        await using (var db = await contexts.CreateDbContextAsync())
            countBefore = (await db.EmployeeProps.WithGraph().Where(e => ids.Contains(e._id)).ToBusinessListAsync()).Count;

        await using (var db = await contexts.CreateDbContextAsync())
        {
            foreach (var id in ids)
                await db.objects.Where(o => o._id == id).ExecuteDeleteAsync();
        }

        int countAfter;
        await using (var db = await contexts.CreateDbContextAsync())
            countAfter = (await db.EmployeeProps.WithGraph().Where(e => ids.Contains(e._id)).ToBusinessListAsync()).Count;

        sw.Stop();

        return Ok("E182_ef", "EF: Delete - Batch", ExampleTier.Free, sw.ElapsedMilliseconds, ids.Count,
            [$"Created: {ids.Count} objects",
             $"Before delete: {countBefore}",
             $"After delete: {countAfter}"]);
    }
}
