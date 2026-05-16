using redb.Core.Attributes;
using redb.Core.Models.Entities;

namespace redb.Tests.Integration.Models;

// ───────────────────────────────────────────────
// Simple model for basic CRUD tests
// ───────────────────────────────────────────────

[RedbScheme("TestSimple")]
public class SimpleProps
{
    public string Title { get; set; } = string.Empty;
    public int Count { get; set; }
    public decimal Price { get; set; }
    public DateTime CreatedAt { get; set; }
    public bool IsActive { get; set; }
    public Guid Code { get; set; }
    public string? Description { get; set; }
    public long? OptionalNumber { get; set; }
}

// ───────────────────────────────────────────────
// Employee model (main test model, matching Examples)
// ───────────────────────────────────────────────

public class TestAddress
{
    public string City { get; set; } = string.Empty;
    public string Street { get; set; } = string.Empty;
    public TestBuildingInfo? Building { get; set; }
}

public class TestBuildingInfo
{
    public int Floor { get; set; }
    public string Name { get; set; } = string.Empty;
    public string[]? Amenities { get; set; }
}

public class TestContact
{
    public string Type { get; set; } = string.Empty;
    public string Value { get; set; } = string.Empty;
    public bool IsVerified { get; set; }
    public TestContactDetail[]? Metadata { get; set; }
}

public class TestContactDetail
{
    public string Label { get; set; } = string.Empty;
    public string Value { get; set; } = string.Empty;
}

public class TestTag
{
    public string Name { get; set; } = string.Empty;
    public int Priority { get; set; }
}

public class TestDepartment
{
    public string Name { get; set; } = string.Empty;
    public int HeadCount { get; set; }
    public string[] Projects { get; set; } = [];
    public TestTag[] Leaders { get; set; } = [];
    public Dictionary<string, int>? BudgetByYear { get; set; }
}

[RedbScheme("TestEmployee")]
public class EmployeeProps
{
    public string FirstName { get; set; } = string.Empty;
    public string LastName { get; set; } = string.Empty;
    public int Age { get; set; }
    public DateTime HireDate { get; set; }
    public string Position { get; set; } = string.Empty;
    public decimal Salary { get; set; }
    public string Department { get; set; } = string.Empty;
    public string? EmployeeCode { get; set; }
    public bool IsRemote { get; set; }
    public double? Rating { get; set; }

    // Arrays
    public string[]? Skills { get; set; }
    public int[]? SkillLevels { get; set; }

    // Nested objects
    public TestAddress? HomeAddress { get; set; }
    public TestAddress? WorkAddress { get; set; }

    // Array of business objects
    public TestContact[]? Contacts { get; set; }

    // RedbObject reference
    public RedbObject<ProjectMetricsProps>? CurrentProject { get; set; }

    // Dictionaries
    public Dictionary<string, string>? PhoneDirectory { get; set; }
    public Dictionary<string, TestAddress>? OfficeLocations { get; set; }
    public Dictionary<int, decimal>? BonusByYear { get; set; }
    public Dictionary<string, TestDepartment>? DepartmentHistory { get; set; }
    public Dictionary<(int Year, string Quarter), string>? PerformanceReviews { get; set; }
}

[RedbScheme("TestProjectMetrics")]
public class ProjectMetricsProps
{
    public long ProjectId { get; set; }
    public long? TasksCompleted { get; set; }
    public long? TasksTotal { get; set; }
    public double? Budget { get; set; }
    public long? TeamSize { get; set; }
    public string[]? Technologies { get; set; }
}

// ───────────────────────────────────────────────
// Tree node model
// ───────────────────────────────────────────────

[RedbScheme("TestTreeNode")]
public class TreeNodeProps
{
    public string Name { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public bool IsActive { get; set; } = true;
    public decimal Budget { get; set; }
    public string Code { get; set; } = string.Empty;
}

// ───────────────────────────────────────────────
// Polymorphic tree models (3 different scheme types in one tree)
// ───────────────────────────────────────────────

[RedbScheme("TestOrgRoot")]
public class OrgRootProps
{
    public string OrgName { get; set; } = string.Empty;
    public string Country { get; set; } = string.Empty;
    public int FoundedYear { get; set; }
}

[RedbScheme("TestDivision")]
public class DivisionProps
{
    public string DivisionName { get; set; } = string.Empty;
    public string Head { get; set; } = string.Empty;
    public decimal AnnualBudget { get; set; }
}

[RedbScheme("TestTeam")]
public class TeamProps
{
    public string TeamName { get; set; } = string.Empty;
    public int MemberCount { get; set; }
    public string[] Technologies { get; set; } = [];
}

// ───────────────────────────────────────────────
// List-related models
// ───────────────────────────────────────────────

[RedbScheme("TestPerson")]
public class PersonProps
{
    public string Name { get; set; } = string.Empty;
    public int Age { get; set; }
    public string Email { get; set; } = string.Empty;
    public RedbListItem? Status { get; set; }
    public List<RedbListItem>? Roles { get; set; }
}

[RedbScheme("TestCity")]
public class CityProps
{
    public string Name { get; set; } = string.Empty;
    public int Population { get; set; }
    public string Region { get; set; } = string.Empty;
    public bool IsCapital { get; set; }
}
