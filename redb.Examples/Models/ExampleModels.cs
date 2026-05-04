using redb.Core.Models.Entities;
using redb.Core.Attributes;

namespace redb.Examples.Models;

/// <summary>
/// Tag with priority for categorization.
/// </summary>
public class Tag
{
    public string Name { get; set; } = string.Empty;
    public int Priority { get; set; }
    public string? Description { get; set; }
}

/// <summary>
/// Project metrics for analytics.
/// </summary>
[RedbScheme("ProjectMetrics")]
public class ProjectMetricsProps
{
    public long ProjectId { get; set; }
    public long? TasksCompleted { get; set; }
    public long? TasksTotal { get; set; }
    public long? BugsFixed { get; set; }
    public double? Budget { get; set; }
    public long? TeamSize { get; set; }
    public Tag[]? Tags { get; set; }
    public string[]? Technologies { get; set; }
}

/// <summary>
/// Address with city, street and building details.
/// </summary>
public class Address
{
    public string City { get; set; } = string.Empty;
    public string Street { get; set; } = string.Empty;
    public BuildingInfo? Building { get; set; }
}

/// <summary>
/// Building information with floor and amenities.
/// </summary>
public class BuildingInfo
{
    public int Floor { get; set; }
    public string Name { get; set; } = string.Empty;
    public string[]? Amenities { get; set; }
    public int[]? AccessCodes { get; set; }
    public string[]? ParkingSpots { get; set; }
    public int[]? ElevatorFloors { get; set; }
}

/// <summary>
/// Contact detail key-value pair.
/// </summary>
public class ContactDetail
{
    public string Label { get; set; } = string.Empty;
    public string Value { get; set; } = string.Empty;
}

/// <summary>
/// Contact information (email, phone, etc).
/// </summary>
public class Contact
{
    public string Type { get; set; } = string.Empty;
    public string Value { get; set; } = string.Empty;
    public bool IsVerified { get; set; }
    public int[]? NotificationHours { get; set; }
    public ContactDetail[]? Metadata { get; set; }
}

/// <summary>
/// Department info with tags, metrics and nested budget data.
/// </summary>
public class Department
{
    public string Name { get; set; } = string.Empty;
    public int HeadCount { get; set; }
    public string[] Projects { get; set; } = [];
    public Tag[] Leaders { get; set; } = [];
    public Dictionary<string, int>? BudgetByYear { get; set; }
}

/// <summary>
/// Employee props - main model for examples.
/// Demonstrates all supported types:
/// - Simple types (int, string, DateTime, long, decimal)
/// - Arrays (string[], int[])
/// - Business classes (Address with nested BuildingInfo)
/// - Array of business classes (Contact[])
/// - RedbObject references (CurrentProject, PastProjects[])
/// - Dictionary types (various key and value types)
/// </summary>
[RedbScheme("Employee")]
public class EmployeeProps
{
    // Basic info
    public string FirstName { get; set; } = string.Empty;
    public string LastName { get; set; } = string.Empty;
    public int Age { get; set; }
    public DateTime HireDate { get; set; }
    public string Position { get; set; } = string.Empty;
    public decimal Salary { get; set; }
    public string Department { get; set; } = string.Empty;
    public string? EmployeeCode { get; set; }

    // Skills and certifications (arrays)
    public string[]? Skills { get; set; }
    public int[]? SkillLevels { get; set; }
    public string[]? Certifications { get; set; }
    public int[]? CertificationYears { get; set; }

    // Addresses (business classes)
    public Address? HomeAddress { get; set; }
    public Address? WorkAddress { get; set; }
    public Address? EmergencyAddress { get; set; }

    // Contacts (array of business classes)
    public Contact[]? Contacts { get; set; }

    // Project references (RedbObject)
    public RedbObject<ProjectMetricsProps>? CurrentProject { get; set; }
    public RedbObject<ProjectMetricsProps>[]? PastProjects { get; set; }

    // Phone directory: extension -> phone number
    public Dictionary<string, string>? PhoneDirectory { get; set; }

    // Office locations by city
    public Dictionary<string, Address>? OfficeLocations { get; set; }

    // Bonus history by year
    public Dictionary<int, decimal>? BonusByYear { get; set; }

    // Department details with complex nested data (Pro)
    public Dictionary<string, Department>? DepartmentHistory { get; set; }

    // Composite key: (year, quarter) -> performance score (Pro)
    public Dictionary<(int Year, string Quarter), string>? PerformanceReviews { get; set; }

    // Project metrics by code (Pro)
    public Dictionary<string, RedbObject<ProjectMetricsProps>>? ProjectMetrics { get; set; }
}

/// <summary>
/// Department props for tree examples (corporate hierarchy).
/// Used with TreeRedbObject for organizational structure.
/// Similar to CategoryTestProps in ConsoleTest.
/// </summary>
[RedbScheme("Department")]
public class DepartmentProps
{
    /// <summary>Department name (e.g. "IT Department Moscow").</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>Description (e.g. "Software development team").</summary>
    public string Description { get; set; } = string.Empty;

    /// <summary>Is department active?</summary>
    public bool IsActive { get; set; } = true;

    /// <summary>Budget in USD.</summary>
    public decimal Budget { get; set; }

    /// <summary>Department code (e.g. "IT-MSK-DEV").</summary>
    public string Code { get; set; } = string.Empty;
}

/// <summary>
/// City props for list examples (linked objects).
/// Used as target object for RedbListItem.Object reference.
/// </summary>
[RedbScheme("City")]
public class CityProps
{
    /// <summary>City name.</summary>
    public string Name { get; set; } = string.Empty;
    
    /// <summary>Population count.</summary>
    public int Population { get; set; }
    
    /// <summary>Region or district.</summary>
    public string Region { get; set; } = string.Empty;
    
    /// <summary>Is capital city?</summary>
    public bool IsCapital { get; set; }
    
    /// <summary>GPS coordinates [lat, lon].</summary>
    public double[] Coordinates { get; set; } = [];
}

/// <summary>
/// Person props demonstrating ListItem fields.
/// Shows single ListItem and List of ListItems usage.
/// </summary>
[RedbScheme("Person")]
public class PersonProps
{
    /// <summary>Person name.</summary>
    public string Name { get; set; } = string.Empty;
    
    /// <summary>Age in years.</summary>
    public int Age { get; set; }
    
    /// <summary>Email address.</summary>
    public string Email { get; set; } = string.Empty;
    
    /// <summary>Single ListItem field (e.g. status from dictionary).</summary>
    public RedbListItem? Status { get; set; }
    
    /// <summary>Array of ListItems (e.g. roles from dictionary).</summary>
    public List<RedbListItem>? Roles { get; set; }
}
