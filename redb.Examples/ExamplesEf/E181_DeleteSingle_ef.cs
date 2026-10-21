using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Core.Models.Entities;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;
using Biz = redb.Examples.Models;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E181_DeleteSingle"/>: create an employee, load it, delete it, load it again.
/// <para>
/// The delete removes the header row in <c>objects</c>; the Props row and every nested row go with it
/// because the flat schema declares ON DELETE CASCADE, as redb does for its own rows. Without that in the
/// schema, an EF application has to configure the cascade itself (<c>OnDelete(DeleteBehavior.Cascade)</c>)
/// or load and remove the whole graph.
/// </para>
/// </summary>
[ExampleMeta("E181_ef", "EF: Delete - Single Object", "CRUD", ExampleTier.Free, 181, "EF", "Delete")]
public class E181_DeleteSingle_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var header = EmployeeEfMapping.ToEntity(new RedbObject<Biz.EmployeeProps>
        {
            name = $"ToDelete_{DateTime.UtcNow.Ticks}",
            Props = new Biz.EmployeeProps
            {
                FirstName = "Temporary",
                LastName = "Employee",
                Age = 25,
                Salary = 50000m,
                Department = "Test"
            }
        });
        db.objects.Add(header);
        await db.SaveChangesAsync();
        var id = header._id;

        // Verify it exists: loaded as a whole object, as redb's LoadAsync does.
        var before = await db.EmployeeProps.WithGraph().FirstOrDefaultAsync(e => e._id == id);
        var existsBefore = before != null && EmployeeEfGraph.ToBusiness(before) != null;

        db.objects.Remove(header);
        await db.SaveChangesAsync();

        var after = await db.EmployeeProps.WithGraph().FirstOrDefaultAsync(e => e._id == id);
        var existsAfter = after != null;

        sw.Stop();

        if (!existsBefore || existsAfter)
        {
            return Fail("E181_ef", "EF: Delete - Single Object", ExampleTier.Free, sw.ElapsedMilliseconds,
                $"Delete verification failed. Before: {existsBefore}, After: {existsAfter}");
        }

        return Ok("E181_ef", "EF: Delete - Single Object", ExampleTier.Free, sw.ElapsedMilliseconds, 1,
            [$"Created object ID: {id}",
             $"Existed before delete: {existsBefore}",
             $"Existed after delete: {existsAfter}"]);
    }
}
