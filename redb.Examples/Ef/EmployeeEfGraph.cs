using Microsoft.EntityFrameworkCore;
using redb.Core.Models.Entities;
using redb.Examples.Ef.Entities;
using Biz = redb.Examples.Models;

namespace redb.Examples.Ef;

/// <summary>
/// Reading an employee through EF Core as redb hands it out: the whole object, header and every nested
/// class, array and dictionary, turned into <see cref="RedbObject{TProps}"/> of the business class.
/// </summary>
internal static class EmployeeEfGraph
{
    /// <summary>
    /// The query with every part of the employee included. Split into one SQL statement per collection:
    /// a single statement would multiply the rows of independent collections (skills times contacts
    /// times offices and so on).
    /// </summary>
    public static IQueryable<EmployeeProps> WithGraph(this IQueryable<EmployeeProps> query) => query
        .Include(e => e.objects)
        .Include(e => e.EmployeeProps_Skills)
        .Include(e => e.EmployeeProps_SkillLevels)
        .Include(e => e.EmployeeProps_Certifications)
        .Include(e => e.EmployeeProps_CertificationYears)
        .Include(e => e.EmployeeProps_PastProjects)
        .Include(e => e.EmployeeProps_HomeAddress!).ThenInclude(a => a.EmployeeProps_HomeAddress_Building!).ThenInclude(b => b.EmployeeProps_HomeAddress_Building_Amenities)
        .Include(e => e.EmployeeProps_HomeAddress!).ThenInclude(a => a.EmployeeProps_HomeAddress_Building!).ThenInclude(b => b.EmployeeProps_HomeAddress_Building_AccessCodes)
        .Include(e => e.EmployeeProps_HomeAddress!).ThenInclude(a => a.EmployeeProps_HomeAddress_Building!).ThenInclude(b => b.EmployeeProps_HomeAddress_Building_ParkingSpots)
        .Include(e => e.EmployeeProps_HomeAddress!).ThenInclude(a => a.EmployeeProps_HomeAddress_Building!).ThenInclude(b => b.EmployeeProps_HomeAddress_Building_ElevatorFloors)
        .Include(e => e.EmployeeProps_WorkAddress!).ThenInclude(a => a.EmployeeProps_WorkAddress_Building!).ThenInclude(b => b.EmployeeProps_WorkAddress_Building_Amenities)
        .Include(e => e.EmployeeProps_WorkAddress!).ThenInclude(a => a.EmployeeProps_WorkAddress_Building!).ThenInclude(b => b.EmployeeProps_WorkAddress_Building_AccessCodes)
        .Include(e => e.EmployeeProps_WorkAddress!).ThenInclude(a => a.EmployeeProps_WorkAddress_Building!).ThenInclude(b => b.EmployeeProps_WorkAddress_Building_ParkingSpots)
        .Include(e => e.EmployeeProps_WorkAddress!).ThenInclude(a => a.EmployeeProps_WorkAddress_Building!).ThenInclude(b => b.EmployeeProps_WorkAddress_Building_ElevatorFloors)
        .Include(e => e.EmployeeProps_EmergencyAddress!).ThenInclude(a => a.EmployeeProps_EmergencyAddress_Building!).ThenInclude(b => b.EmployeeProps_EmergencyAddress_Building_Amenities)
        .Include(e => e.EmployeeProps_EmergencyAddress!).ThenInclude(a => a.EmployeeProps_EmergencyAddress_Building!).ThenInclude(b => b.EmployeeProps_EmergencyAddress_Building_AccessCodes)
        .Include(e => e.EmployeeProps_EmergencyAddress!).ThenInclude(a => a.EmployeeProps_EmergencyAddress_Building!).ThenInclude(b => b.EmployeeProps_EmergencyAddress_Building_ParkingSpots)
        .Include(e => e.EmployeeProps_EmergencyAddress!).ThenInclude(a => a.EmployeeProps_EmergencyAddress_Building!).ThenInclude(b => b.EmployeeProps_EmergencyAddress_Building_ElevatorFloors)
        .Include(e => e.EmployeeProps_Contacts).ThenInclude(c => c.EmployeeProps_Contacts_NotificationHours)
        .Include(e => e.EmployeeProps_Contacts).ThenInclude(c => c.EmployeeProps_Contacts_Metadata)
        .Include(e => e.EmployeeProps_PhoneDirectory)
        .Include(e => e.EmployeeProps_BonusByYear)
        .Include(e => e.EmployeeProps_ProjectMetrics)
        .Include(e => e.EmployeeProps_PerformanceReviews)
        .Include(e => e.EmployeeProps_OfficeLocations).ThenInclude(o => o.EmployeeProps_OfficeLocations_Building!).ThenInclude(b => b.EmployeeProps_OfficeLocations_Building_Amenities)
        .Include(e => e.EmployeeProps_OfficeLocations).ThenInclude(o => o.EmployeeProps_OfficeLocations_Building!).ThenInclude(b => b.EmployeeProps_OfficeLocations_Building_AccessCodes)
        .Include(e => e.EmployeeProps_OfficeLocations).ThenInclude(o => o.EmployeeProps_OfficeLocations_Building!).ThenInclude(b => b.EmployeeProps_OfficeLocations_Building_ParkingSpots)
        .Include(e => e.EmployeeProps_OfficeLocations).ThenInclude(o => o.EmployeeProps_OfficeLocations_Building!).ThenInclude(b => b.EmployeeProps_OfficeLocations_Building_ElevatorFloors)
        .Include(e => e.EmployeeProps_DepartmentHistory).ThenInclude(d => d.EmployeeProps_DepartmentHistory_Projects)
        .Include(e => e.EmployeeProps_DepartmentHistory).ThenInclude(d => d.EmployeeProps_DepartmentHistory_Leaders)
        .Include(e => e.EmployeeProps_DepartmentHistory).ThenInclude(d => d.EmployeeProps_DepartmentHistory_BudgetByYear)
        .AsSplitQuery();

    /// <summary>Runs the query and turns every employee into the business object.</summary>
    public static async Task<List<RedbObject<Biz.EmployeeProps>>> ToBusinessListAsync(this IQueryable<EmployeeProps> query) =>
        (await query.ToListAsync()).Select(ToBusiness).ToList();

    /// <summary>
    /// The business object of a loaded employee, as redb returns it. References to other objects
    /// (<c>RedbObject&lt;ProjectMetricsProps&gt;</c>) carry the target id only: the seeded data has none,
    /// and loading their targets would be one more include chain.
    /// </summary>
    public static RedbObject<Biz.EmployeeProps> ToBusiness(EmployeeProps e)
    {
        var h = e.objects;
        return new RedbObject<Biz.EmployeeProps>
        {
            id = h._id,
            parent_id = h._id_parent,
            scheme_id = h._id_scheme,
            owner_id = h._id_owner,
            who_change_id = h._id_who_change,
            date_create = h._date_create,
            date_modify = h._date_modify,
            date_begin = h._date_begin,
            date_complete = h._date_complete,
            key = h._key,
            name = h._name,
            note = h._note,
            value_long = h._value_long,
            value_string = h._value_string,
            value_guid = h._value_guid,
            value_bool = h._value_bool,
            value_double = h._value_double,
            value_numeric = h._value_numeric,
            value_datetime = h._value_datetime,
            value_bytes = h._value_bytes,
            Props = new Biz.EmployeeProps
            {
                FirstName = e.FirstName ?? string.Empty,
                LastName = e.LastName ?? string.Empty,
                Age = e.Age ?? 0,
                HireDate = e.HireDate ?? default,
                Position = e.Position ?? string.Empty,
                Salary = e.Salary ?? 0m,
                Department = e.Department ?? string.Empty,
                EmployeeCode = e.EmployeeCode,
                Skills = Elements(e.EmployeeProps_Skills, x => x._idx, x => x._value!),
                SkillLevels = Elements(e.EmployeeProps_SkillLevels, x => x._idx, x => x._value ?? 0),
                Certifications = Elements(e.EmployeeProps_Certifications, x => x._idx, x => x._value!),
                CertificationYears = Elements(e.EmployeeProps_CertificationYears, x => x._idx, x => x._value ?? 0),
                HomeAddress = e.EmployeeProps_HomeAddress is not { } home ? null : Address(home.City, home.Street,
                    home.EmployeeProps_HomeAddress_Building is not { } b ? null : Building(b.Floor, b.Name,
                        Elements(b.EmployeeProps_HomeAddress_Building_Amenities, x => x._idx, x => x._value!),
                        Elements(b.EmployeeProps_HomeAddress_Building_AccessCodes, x => x._idx, x => x._value ?? 0),
                        Elements(b.EmployeeProps_HomeAddress_Building_ParkingSpots, x => x._idx, x => x._value!),
                        Elements(b.EmployeeProps_HomeAddress_Building_ElevatorFloors, x => x._idx, x => x._value ?? 0))),
                WorkAddress = e.EmployeeProps_WorkAddress is not { } work ? null : Address(work.City, work.Street,
                    work.EmployeeProps_WorkAddress_Building is not { } wb ? null : Building(wb.Floor, wb.Name,
                        Elements(wb.EmployeeProps_WorkAddress_Building_Amenities, x => x._idx, x => x._value!),
                        Elements(wb.EmployeeProps_WorkAddress_Building_AccessCodes, x => x._idx, x => x._value ?? 0),
                        Elements(wb.EmployeeProps_WorkAddress_Building_ParkingSpots, x => x._idx, x => x._value!),
                        Elements(wb.EmployeeProps_WorkAddress_Building_ElevatorFloors, x => x._idx, x => x._value ?? 0))),
                EmergencyAddress = e.EmployeeProps_EmergencyAddress is not { } emergency ? null : Address(emergency.City, emergency.Street,
                    emergency.EmployeeProps_EmergencyAddress_Building is not { } eb ? null : Building(eb.Floor, eb.Name,
                        Elements(eb.EmployeeProps_EmergencyAddress_Building_Amenities, x => x._idx, x => x._value!),
                        Elements(eb.EmployeeProps_EmergencyAddress_Building_AccessCodes, x => x._idx, x => x._value ?? 0),
                        Elements(eb.EmployeeProps_EmergencyAddress_Building_ParkingSpots, x => x._idx, x => x._value!),
                        Elements(eb.EmployeeProps_EmergencyAddress_Building_ElevatorFloors, x => x._idx, x => x._value ?? 0))),
                Contacts = Elements(e.EmployeeProps_Contacts, x => x._idx, c => new Biz.Contact
                {
                    Type = c.Type ?? string.Empty,
                    Value = c.Value ?? string.Empty,
                    IsVerified = c.IsVerified ?? false,
                    NotificationHours = Elements(c.EmployeeProps_Contacts_NotificationHours, x => x._idx, x => x._value ?? 0),
                    Metadata = Elements(c.EmployeeProps_Contacts_Metadata, x => x._idx,
                        m => new Biz.ContactDetail { Label = m.Label ?? string.Empty, Value = m.Value ?? string.Empty })
                }),
                CurrentProject = e.CurrentProject is { } projectId ? new RedbObject<Biz.ProjectMetricsProps> { id = projectId } : null,
                PastProjects = Elements(e.EmployeeProps_PastProjects, x => x._idx,
                    x => new RedbObject<Biz.ProjectMetricsProps> { id = x._value ?? 0 }),
                PhoneDirectory = Entries(e.EmployeeProps_PhoneDirectory, x => x._key, x => x._value!),
                BonusByYear = Entries(e.EmployeeProps_BonusByYear, x => x._key, x => x._value ?? 0m),
                ProjectMetrics = Entries(e.EmployeeProps_ProjectMetrics, x => x._key,
                    x => new RedbObject<Biz.ProjectMetricsProps> { id = x._value ?? 0 }),
                PerformanceReviews = Entries(e.EmployeeProps_PerformanceReviews,
                    x => (x._key_Item1, x._key_Item2), x => x._value!),
                OfficeLocations = Entries(e.EmployeeProps_OfficeLocations, x => x._key, o => Address(o.City, o.Street,
                    o.EmployeeProps_OfficeLocations_Building is not { } ob ? null : Building(ob.Floor, ob.Name,
                        Elements(ob.EmployeeProps_OfficeLocations_Building_Amenities, x => x._idx, x => x._value!),
                        Elements(ob.EmployeeProps_OfficeLocations_Building_AccessCodes, x => x._idx, x => x._value ?? 0),
                        Elements(ob.EmployeeProps_OfficeLocations_Building_ParkingSpots, x => x._idx, x => x._value!),
                        Elements(ob.EmployeeProps_OfficeLocations_Building_ElevatorFloors, x => x._idx, x => x._value ?? 0)))),
                DepartmentHistory = Entries(e.EmployeeProps_DepartmentHistory, x => x._key, d => new Biz.Department
                {
                    Name = d.Name ?? string.Empty,
                    HeadCount = d.HeadCount ?? 0,
                    Projects = Elements(d.EmployeeProps_DepartmentHistory_Projects, x => x._idx, x => x._value!) ?? [],
                    Leaders = Elements(d.EmployeeProps_DepartmentHistory_Leaders, x => x._idx,
                        t => new Biz.Tag { Name = t.Name ?? string.Empty, Priority = t.Priority ?? 0, Description = t.Description }) ?? [],
                    BudgetByYear = Entries(d.EmployeeProps_DepartmentHistory_BudgetByYear, x => x._key, x => x._value ?? 0)
                })
            }
        };
    }

    private static Biz.Address Address(string? city, string? street, Biz.BuildingInfo? building) => new()
    {
        City = city ?? string.Empty,
        Street = street ?? string.Empty,
        Building = building
    };

    private static Biz.BuildingInfo Building(int? floor, string? name, string[]? amenities, int[]? accessCodes,
        string[]? parkingSpots, int[]? elevatorFloors) => new()
    {
        Floor = floor ?? 0,
        Name = name ?? string.Empty,
        Amenities = amenities,
        AccessCodes = accessCodes,
        ParkingSpots = parkingSpots,
        ElevatorFloors = elevatorFloors
    };

    /// <summary>
    /// The array of a collection table in element order. No rows read as null: the flat model cannot tell
    /// an empty array from a missing one, and redb leaves a missing array null.
    /// </summary>
    private static TValue[]? Elements<TRow, TValue>(ICollection<TRow> rows, Func<TRow, int> index, Func<TRow, TValue> value) =>
        rows.Count == 0 ? null : rows.OrderBy(index).Select(value).ToArray();

    /// <summary>The dictionary of a collection table; no rows read as null, as with arrays.</summary>
    private static Dictionary<TKey, TValue>? Entries<TRow, TKey, TValue>(ICollection<TRow> rows, Func<TRow, TKey> key, Func<TRow, TValue> value)
        where TKey : notnull =>
        rows.Count == 0 ? null : rows.ToDictionary(key, value);
}
