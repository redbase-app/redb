using redb.Core;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// The contract for a <c>RedbObject&lt;T&gt;</c> reference at the depth boundary of <c>get_object_json</c>:
/// it is a <b>stub</b> — the referenced object's base fields (<c>id</c>, <c>scheme_id</c>, <c>hash</c>, …)
/// with no <c>properties</c> — and never <c>null</c>. Identical on the three Free JSON builders.
///
/// <para>
/// Before V4 the builders disagreed: PostgreSQL and the SQLite extension emitted the stub, MSSQL
/// emitted <c>null</c> (a <c>@max_depth &gt; 0</c> guard in front of the recursive call). The same
/// employee loaded with the same depth had a project on two providers and none on the third.
/// </para>
///
/// <para>
/// The stub is also what lazy references are built on (LAZY_REFERENCES_PLAN §2): it carries the id
/// to load by and the hash to look the cache up with. So this is a contract test, not a regression
/// test — it pins the shape every later piece relies on.
/// </para>
/// </summary>
public abstract class ReferenceStubTestsBase : IAsyncLifetime
{
    protected readonly IRedbService Redb;

    // The database is shared with whole-scheme tests: every object a test saves is deleted after it,
    // the employees first, then the projects they reference.
    private readonly List<long> _employees = new();
    private readonly List<long> _projects = new();

    protected ReferenceStubTestsBase(IRedbService redb) => Redb = redb;

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        if (_employees.Count > 0) await Redb.DeleteAsync(_employees);
        if (_projects.Count > 0) await Redb.DeleteAsync(_projects);
    }

    // A complete employee, so that a whole-scheme aggregate never meets a zero salary or an empty department.
    private static EmployeeProps StubEmployee(string firstName, string lastName, long projectId) => new()
    {
        FirstName = firstName, LastName = lastName, Age = 30, Position = "dev",
        Salary = 1000m, Department = "Stub", HireDate = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc),
        CurrentProject = new RedbObject<ProjectMetricsProps> { id = projectId }
    };

    private async Task<(long employeeId, long projectId)> SaveEmployeeWithProjectAsync(string tag)
    {
        var project = new RedbObject<ProjectMetricsProps>
        {
            name = $"stub-project-{tag}",
            Props = new ProjectMetricsProps { ProjectId = 7, TasksTotal = 10, TasksCompleted = 3 }
        };
        var projectId = await Redb.SaveAsync(project);
        _projects.Add(projectId);

        var employee = new RedbObject<EmployeeProps>
        {
            name = $"stub-employee-{tag}",
            Props = StubEmployee("Stub", tag, projectId)
        };
        var employeeId = await Redb.SaveAsync(employee);
        _employees.Add(employeeId);
        return (employeeId, projectId);
    }

    /// <summary>
    /// Depth 1: the root has its properties, the reference under it sits exactly at the boundary.
    /// </summary>
    [Fact]
    public async Task AtDepthBoundary_ReferenceIsAStub_NotNull()
    {
        var (employeeId, projectId) = await SaveEmployeeWithProjectAsync("boundary");

        var loaded = await Redb.LoadAsync<EmployeeProps>(employeeId, depth: 1);

        loaded.Should().NotBeNull();
        loaded!.Props.Should().NotBeNull("the root is never a stub");

        var stub = loaded.Props.CurrentProject;
        stub.Should().NotBeNull("a reference at the depth boundary is a stub, never null — on every provider");
        stub!.id.Should().Be(projectId);
        stub.scheme_id.Should().BeGreaterThan(0, "the stub carries the scheme to materialise by");
        stub.hash.Should().NotBeNull("the stub carries the hash the props cache is keyed by");
        stub.GetPropsDirectly().Should().BeNull("a stub has no properties, that is what makes it a stub");
    }

    /// <summary>Depth 2: the same reference is a full object. The boundary is the only thing that differs.</summary>
    [Fact]
    public async Task BelowDepthBoundary_ReferenceIsMaterialised()
    {
        var (employeeId, projectId) = await SaveEmployeeWithProjectAsync("full");

        var loaded = await Redb.LoadAsync<EmployeeProps>(employeeId, depth: 2);

        var reference = loaded!.Props.CurrentProject;
        reference.Should().NotBeNull();
        reference!.id.Should().Be(projectId);
        reference.Props.Should().NotBeNull();
        reference.Props.TasksTotal.Should().Be(10);
    }

    /// <summary>
    /// The stub's hash equals the referenced object's stored hash. LAZY §4.1 hangs on this: the
    /// parent's hash will use it instead of walking into the reference.
    /// </summary>
    [Fact]
    public async Task StubHash_EqualsStoredHashOfReferencedObject()
    {
        var (employeeId, projectId) = await SaveEmployeeWithProjectAsync("hash");

        var stub = (await Redb.LoadAsync<EmployeeProps>(employeeId, depth: 1))!.Props.CurrentProject!;
        var full = await Redb.LoadAsync<ProjectMetricsProps>(projectId, depth: 1);

        stub.hash.Should().Be(full!.hash);
    }
    /// <summary>
    /// Saving a parent whose reference is by id only must write the reference and leave the
    /// referenced object alone. Before the fix the stub was collected as an object to save and the
    /// parent's save rewrote the project as an empty object: values deleted, name reset to
    /// <c>Object_&lt;id&gt;</c>, hash cleared — silently, on every provider.
    /// </summary>
    [Fact]
    public async Task SavingParent_WithReferenceById_LeavesReferencedObjectIntact()
    {
        var (_, projectId) = await SaveEmployeeWithProjectAsync("intact");

        var project = await Redb.LoadAsync<ProjectMetricsProps>(projectId, depth: 1);

        project.Should().NotBeNull();
        project!.name.Should().Be("stub-project-intact");
        project.hash.Should().NotBeNull();
        project.Props.TasksTotal.Should().Be(10);
        project.Props.TasksCompleted.Should().Be(3);
    }

    /// <summary>
    /// The batch save shares the collector with the single save; same contract. Two parents name the
    /// same project: the reference is not deduplicated away, each parent gets its own reference row.
    /// </summary>
    [Fact]
    public async Task SavingParents_InBatch_WithReferenceById_LeavesReferencedObjectIntact()
    {
        var project = new RedbObject<ProjectMetricsProps>
        {
            name = "stub-project-batch",
            Props = new ProjectMetricsProps { ProjectId = 8, TasksTotal = 20, TasksCompleted = 5 }
        };
        var projectId = await Redb.SaveAsync(project);
        _projects.Add(projectId);

        var employees = Enumerable.Range(0, 2).Select(i => new RedbObject<EmployeeProps>
        {
            name = $"stub-employee-batch-{i}",
            Props = StubEmployee("Batch", i.ToString(), projectId)
        }).Cast<IRedbObject>().ToList();
        var ids = await Redb.SaveAsync(employees);
        _employees.AddRange(ids);
        ids.Should().HaveCount(2);

        var loaded = await Redb.LoadAsync<ProjectMetricsProps>(projectId, depth: 1);
        loaded!.name.Should().Be("stub-project-batch");
        loaded.Props.TasksTotal.Should().Be(20);

        foreach (var id in ids)
            (await Redb.LoadAsync<EmployeeProps>(id, depth: 1))!.Props.CurrentProject!.id.Should().Be(projectId);
    }
}
