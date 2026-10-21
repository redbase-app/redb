using redb.Core.Models.Entities;
using redb.Examples.Ef.Entities;
using Biz = redb.Examples.Models;

namespace redb.Examples.Ef;

/// <summary>
/// Turns the business class <see cref="Biz.EmployeeProps"/> into the EF entity graph of the flat tables.
/// <para>
/// This is the code an EF application writes by hand to get its business class in and out of the
/// database: one entity per flat table, a list per array and dictionary, a reference per nested class.
/// redb does the same from the Props class alone. The mapping is part of every EF measurement.
/// </para>
/// </summary>
internal static class EmployeeEfMapping
{
    /// <summary>
    /// The header row of a new employee with its Props attached. Keys are left unset: the database hands
    /// them out from the flat sequence, and EF copies the header key into the Props row.
    /// </summary>
    public static objects ToEntity(RedbObject<Biz.EmployeeProps> source)
    {
        var props = source.Props ?? throw new ArgumentException("An employee without Props.", nameof(source));
        var now = DateTime.UtcNow;

        return new objects
        {
            _id_scheme = EfSchemeIds.Employee,
            // The system user, as redb writes it for objects saved without a user (owner_id 0).
            _id_owner = 0,
            _id_who_change = 0,
            _date_create = now,
            _date_modify = now,
            _name = source.Name,
            EmployeeProps = ToEntity(props)
        };
    }

    private static EmployeeProps ToEntity(Biz.EmployeeProps p) => new()
    {
        FirstName = p.FirstName,
        LastName = p.LastName,
        Age = p.Age,
        // The reading redb stores: the clock value marked UTC (DateTimeConverter.NormalizeForStorage).
        HireDate = DateTime.SpecifyKind(p.HireDate, DateTimeKind.Utc),
        Position = p.Position,
        Salary = p.Salary,
        Department = p.Department,
        EmployeeCode = p.EmployeeCode,
        CurrentProject = p.CurrentProject?.Id,

        EmployeeProps_Skills = Items(p.Skills, (i, v) => new EmployeeProps_Skills { _idx = i, _value = v }),
        EmployeeProps_SkillLevels = Items(p.SkillLevels, (i, v) => new EmployeeProps_SkillLevels { _idx = i, _value = v }),
        EmployeeProps_Certifications = Items(p.Certifications, (i, v) => new EmployeeProps_Certifications { _idx = i, _value = v }),
        EmployeeProps_CertificationYears = Items(p.CertificationYears, (i, v) => new EmployeeProps_CertificationYears { _idx = i, _value = v }),

        EmployeeProps_HomeAddress = HomeAddress(p.HomeAddress),
        EmployeeProps_WorkAddress = WorkAddress(p.WorkAddress),
        EmployeeProps_EmergencyAddress = EmergencyAddress(p.EmergencyAddress),

        EmployeeProps_Contacts = Items(p.Contacts, (i, c) => new EmployeeProps_Contacts
        {
            _idx = i,
            Type = c.Type,
            Value = c.Value,
            IsVerified = c.IsVerified,
            EmployeeProps_Contacts_NotificationHours = Items(c.NotificationHours,
                (j, v) => new EmployeeProps_Contacts_NotificationHours { _idx = j, _value = v }),
            EmployeeProps_Contacts_Metadata = Items(c.Metadata,
                (j, m) => new EmployeeProps_Contacts_Metadata { _idx = j, Label = m.Label, Value = m.Value })
        }),

        EmployeeProps_PastProjects = Items(p.PastProjects, (i, o) => new EmployeeProps_PastProjects { _idx = i, _value = o?.Id }),

        EmployeeProps_PhoneDirectory = Entries(p.PhoneDirectory, (k, v) => new EmployeeProps_PhoneDirectory { _key = k, _value = v }),
        EmployeeProps_BonusByYear = Entries(p.BonusByYear, (k, v) => new EmployeeProps_BonusByYear { _key = k, _value = v }),
        EmployeeProps_ProjectMetrics = Entries(p.ProjectMetrics, (k, o) => new EmployeeProps_ProjectMetrics { _key = k, _value = o?.Id }),
        EmployeeProps_PerformanceReviews = Entries(p.PerformanceReviews, (k, v) => new EmployeeProps_PerformanceReviews
        {
            _key_Item1 = k.Year,
            _key_Item2 = k.Quarter,
            _value = v
        }),

        EmployeeProps_OfficeLocations = Entries(p.OfficeLocations, (k, a) => new EmployeeProps_OfficeLocations
        {
            _key = k,
            City = a?.City,
            Street = a?.Street,
            EmployeeProps_OfficeLocations_Building = OfficeBuilding(a?.Building)
        }),

        EmployeeProps_DepartmentHistory = Entries(p.DepartmentHistory, (k, d) => new EmployeeProps_DepartmentHistory
        {
            _key = k,
            Name = d?.Name,
            HeadCount = d?.HeadCount,
            EmployeeProps_DepartmentHistory_Projects = Items(d?.Projects,
                (i, v) => new EmployeeProps_DepartmentHistory_Projects { _idx = i, _value = v }),
            EmployeeProps_DepartmentHistory_Leaders = Items(d?.Leaders,
                (i, t) => new EmployeeProps_DepartmentHistory_Leaders { _idx = i, Name = t.Name, Priority = t.Priority, Description = t.Description }),
            EmployeeProps_DepartmentHistory_BudgetByYear = Entries(d?.BudgetByYear,
                (y, v) => new EmployeeProps_DepartmentHistory_BudgetByYear { _key = y, _value = v })
        })
    };

    // Every place an Address appears is a table of its own in the flat model, so each one needs its own
    // copy of the same code.

    private static EmployeeProps_HomeAddress? HomeAddress(Biz.Address? a) => a == null ? null : new()
    {
        City = a.City,
        Street = a.Street,
        EmployeeProps_HomeAddress_Building = a.Building is not { } b ? null : new()
        {
            Floor = b.Floor,
            Name = b.Name,
            EmployeeProps_HomeAddress_Building_Amenities = Items(b.Amenities, (i, v) => new EmployeeProps_HomeAddress_Building_Amenities { _idx = i, _value = v }),
            EmployeeProps_HomeAddress_Building_AccessCodes = Items(b.AccessCodes, (i, v) => new EmployeeProps_HomeAddress_Building_AccessCodes { _idx = i, _value = v }),
            EmployeeProps_HomeAddress_Building_ParkingSpots = Items(b.ParkingSpots, (i, v) => new EmployeeProps_HomeAddress_Building_ParkingSpots { _idx = i, _value = v }),
            EmployeeProps_HomeAddress_Building_ElevatorFloors = Items(b.ElevatorFloors, (i, v) => new EmployeeProps_HomeAddress_Building_ElevatorFloors { _idx = i, _value = v })
        }
    };

    private static EmployeeProps_WorkAddress? WorkAddress(Biz.Address? a) => a == null ? null : new()
    {
        City = a.City,
        Street = a.Street,
        EmployeeProps_WorkAddress_Building = a.Building is not { } b ? null : new()
        {
            Floor = b.Floor,
            Name = b.Name,
            EmployeeProps_WorkAddress_Building_Amenities = Items(b.Amenities, (i, v) => new EmployeeProps_WorkAddress_Building_Amenities { _idx = i, _value = v }),
            EmployeeProps_WorkAddress_Building_AccessCodes = Items(b.AccessCodes, (i, v) => new EmployeeProps_WorkAddress_Building_AccessCodes { _idx = i, _value = v }),
            EmployeeProps_WorkAddress_Building_ParkingSpots = Items(b.ParkingSpots, (i, v) => new EmployeeProps_WorkAddress_Building_ParkingSpots { _idx = i, _value = v }),
            EmployeeProps_WorkAddress_Building_ElevatorFloors = Items(b.ElevatorFloors, (i, v) => new EmployeeProps_WorkAddress_Building_ElevatorFloors { _idx = i, _value = v })
        }
    };

    private static EmployeeProps_EmergencyAddress? EmergencyAddress(Biz.Address? a) => a == null ? null : new()
    {
        City = a.City,
        Street = a.Street,
        EmployeeProps_EmergencyAddress_Building = a.Building is not { } b ? null : new()
        {
            Floor = b.Floor,
            Name = b.Name,
            EmployeeProps_EmergencyAddress_Building_Amenities = Items(b.Amenities, (i, v) => new EmployeeProps_EmergencyAddress_Building_Amenities { _idx = i, _value = v }),
            EmployeeProps_EmergencyAddress_Building_AccessCodes = Items(b.AccessCodes, (i, v) => new EmployeeProps_EmergencyAddress_Building_AccessCodes { _idx = i, _value = v }),
            EmployeeProps_EmergencyAddress_Building_ParkingSpots = Items(b.ParkingSpots, (i, v) => new EmployeeProps_EmergencyAddress_Building_ParkingSpots { _idx = i, _value = v }),
            EmployeeProps_EmergencyAddress_Building_ElevatorFloors = Items(b.ElevatorFloors, (i, v) => new EmployeeProps_EmergencyAddress_Building_ElevatorFloors { _idx = i, _value = v })
        }
    };

    private static EmployeeProps_OfficeLocations_Building? OfficeBuilding(Biz.BuildingInfo? b) => b == null ? null : new()
    {
        Floor = b.Floor,
        Name = b.Name,
        EmployeeProps_OfficeLocations_Building_Amenities = Items(b.Amenities, (i, v) => new EmployeeProps_OfficeLocations_Building_Amenities { _idx = i, _value = v }),
        EmployeeProps_OfficeLocations_Building_AccessCodes = Items(b.AccessCodes, (i, v) => new EmployeeProps_OfficeLocations_Building_AccessCodes { _idx = i, _value = v }),
        EmployeeProps_OfficeLocations_Building_ParkingSpots = Items(b.ParkingSpots, (i, v) => new EmployeeProps_OfficeLocations_Building_ParkingSpots { _idx = i, _value = v }),
        EmployeeProps_OfficeLocations_Building_ElevatorFloors = Items(b.ElevatorFloors, (i, v) => new EmployeeProps_OfficeLocations_Building_ElevatorFloors { _idx = i, _value = v })
    };

    /// <summary>One row per array element, its position in <c>_idx</c>; no rows for a null array.</summary>
    private static List<T> Items<TValue, T>(IEnumerable<TValue>? values, Func<int, TValue, T> make) =>
        values?.Select((value, index) => make(index, value)).ToList() ?? [];

    /// <summary>One row per dictionary entry; no rows for a null dictionary.</summary>
    private static List<T> Entries<TKey, TValue, T>(IDictionary<TKey, TValue>? values, Func<TKey, TValue, T> make)
        where TKey : notnull =>
        values?.Select(entry => make(entry.Key, entry.Value)).ToList() ?? [];
}
