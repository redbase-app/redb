using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Paginate employees using Skip and Take.
/// Get page 2 with 10 items per page.
/// </summary>
[ExampleMeta("E082", "Skip/Take - Pagination", "Query",
    ExampleTier.Free, 1, "Skip", "Take", "Pagination")]
public class E082_SkipTake : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        const int pageSize = 10;
        const int page = 2; // 0-based would skip 10

        var query = redb.Query<EmployeeProps>()
            .OrderBy(e => e.LastName)
            .Skip((page - 1) * pageSize)
            .Take(pageSize);

        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var result = await query.ToListAsync();
        sw.Stop();

        return Ok("E082", "Skip/Take - Pagination", ExampleTier.Free, sw.ElapsedMilliseconds, result.Count,
            [$"Page {page}, size {pageSize}", $"Skip {(page - 1) * pageSize}, Take {pageSize}"]);
    }
}
