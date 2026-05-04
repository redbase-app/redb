using System.Diagnostics;
using redb.Core;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Example: Bulk insert of complex employee records.
/// 
/// <para><b>Classic ORM (EF Core) would require ~28 tables:</b></para>
/// <list type="number">
///   <item>Employees - main table</item>
///   <item>EmployeeSkills - string[] Skills (FK)</item>
///   <item>EmployeeSkillLevels - int[] SkillLevels (FK)</item>
///   <item>EmployeeCertifications - string[] (FK)</item>
///   <item>EmployeeCertificationYears - int[] (FK)</item>
///   <item>Addresses - for Home/Work/Emergency (FK)</item>
///   <item>BuildingInfos - nested in Address (FK)</item>
///   <item>BuildingAmenities - string[] in BuildingInfo (FK)</item>
///   <item>BuildingAccessCodes - int[] in BuildingInfo (FK)</item>
///   <item>BuildingParkingSpots - string[] (FK)</item>
///   <item>BuildingElevatorFloors - int[] (FK)</item>
///   <item>Contacts - Contact[] (FK)</item>
///   <item>ContactNotificationHours - int[] in Contact (FK)</item>
///   <item>ContactMetadata - ContactDetail[] in Contact (FK)</item>
///   <item>ProjectMetrics - RedbObject reference</item>
///   <item>ProjectTags - Tag[] in ProjectMetrics (FK)</item>
///   <item>ProjectTechnologies - string[] in ProjectMetrics (FK)</item>
///   <item>EmployeePastProjects - many-to-many (2 FK)</item>
///   <item>PhoneDirectory - Dictionary (FK)</item>
///   <item>OfficeLocations - Dictionary + Address (2 FK)</item>
///   <item>BonusByYear - Dictionary (FK)</item>
///   <item>DepartmentHistory - Dictionary (FK)</item>
///   <item>Departments - nested class</item>
///   <item>DepartmentProjects - string[] (FK)</item>
///   <item>DepartmentLeaders - Tag[] (FK)</item>
///   <item>DepartmentBudgetByYear - nested Dictionary (FK)</item>
///   <item>PerformanceReviews - tuple key Dictionary (FK)</item>
///   <item>ProjectMetricsDict - Dictionary with RedbObject (2 FK)</item>
/// </list>
/// 
/// <para><b>INSERTs per employee:</b> ~40-60 records across 28 tables with FK ordering.</para>
/// <para><b>For 100 employees:</b> 4000-6000 INSERTs!</para>
/// 
/// <para><b>REDB:</b> 2 tables (_objects + _values), ~3000 records, single BulkInsert via COPY.</para>
/// </summary>
[ExampleMeta("E000", "Bulk Insert - Complex Objects", "Setup",
    ExampleTier.Free, 1, "BulkInsert", "AddNewObjectsAsync", "COPY")]
public class E000_BulkInsert : ExampleBase
{
    private static readonly string[] Positions = ["Developer", "Designer", "Manager", "Analyst", "Tester"];
    private static readonly string[] Departments = ["Engineering", "Design", "Sales", "Marketing", "Support"];
    private static readonly string[] Cities = ["New York", "London", "Berlin", "Tokyo", "Sydney"];
    private static readonly string[] Skills = ["C#", "JavaScript", "Python", "SQL", "React", "Azure", "Docker"];

    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();
        const int count = 100;

        var employees = CreateEmployees(count);

        // Bulk insert using COPY protocol (Pro feature)
        var savedIds = await redb.SaveAsync(employees); 

        sw.Stop();

        // Classic ORM: ~25 tables, ~5000 INSERTs with FK ordering
        // REDB: 2 tables, ~3000 values, single BulkInsert
        return Ok("E000", "Bulk Insert - Complex Objects", ExampleTier.Free, sw.ElapsedMilliseconds, count,
        [
            $"Inserted: {savedIds.Count} complex objects (vs ~5000 INSERTs in EF)",
            $"Rate: {savedIds.Count * 1000 / Math.Max(sw.ElapsedMilliseconds, 1)} obj/sec | 2 tables vs ~25"
        ]);
    }

    /// <summary>
    /// Creates employee records with meaningful data for queries.
    /// </summary>
    private static List<RedbObject<EmployeeProps>> CreateEmployees(int count)
    {
        var result = new List<RedbObject<EmployeeProps>>();
        var baseDate = DateTime.Today.AddYears(-5);

        for (int i = 0; i < count; i++)
        {
            var position = Positions[i % Positions.Length];
            var department = Departments[i % Departments.Length];
            var city = Cities[i % Cities.Length];

            // Age groups: 25-34, 35-44, 45-54, 55-64
            var age = 25 + (i % 4) * 10 + (i % 10);

            // Salary based on position and seniority
            var baseSalary = position switch
            {
                "Manager" => 90000m,
                "Developer" => 80000m,
                "Analyst" => 70000m,
                "Designer" => 65000m,
                _ => 55000m
            };
            var salary = baseSalary + (i % 20) * 1000;

            // Hire dates spread over 5 years
            var hireDate = baseDate.AddDays(i * 30);

            var emp = new RedbObject<EmployeeProps>
            {
                name = $"{position} - {department} #{i + 1}",
                Props = new EmployeeProps
                {
                    FirstName = $"John{i}",
                    LastName = $"Smith{i}",
                    Age = age,
                    Position = position,
                    Department = department,
                    Salary = salary,
                    HireDate = hireDate,
                    EmployeeCode = $"EMP-{i + 1:D4}",

                    // Skills based on position
                    Skills = GetSkillsForPosition(position, i),
                    SkillLevels = [3 + i % 3, 2 + i % 4, 4 + i % 2],

                    Certifications = i % 3 == 0 ? ["AWS", "Azure"] : i % 2 == 0 ? ["PMP"] : null,
                    CertificationYears = i % 3 == 0 ? [2022, 2023] : i % 2 == 0 ? [2021] : null,

                    // Home address
                    HomeAddress = new Address
                    {
                        City = city,
                        Street = $"{100 + i} Main Street",
                        Building = new BuildingInfo
                        {
                            Floor = 1 + i % 30,
                            Name = $"Tower {(char)('A' + i % 5)}",
                            Amenities = ["Parking", "Gym", "Cafe"],
                            AccessCodes = [1000 + i, 2000 + i]
                        }
                    },

                    // Work address
                    WorkAddress = new Address
                    {
                        City = city,
                        Street = $"{1 + i % 10} Corporate Plaza",
                        Building = new BuildingInfo
                        {
                            Floor = 5 + i % 20,
                            Name = "HQ Building",
                            Amenities = ["Conference Rooms", "Cafeteria"],
                            AccessCodes = [9000 + i]
                        }
                    },

                    // Contacts
                    Contacts =
                    [
                        new Contact
                        {
                            Type = "email",
                            Value = $"john{i}@company.com",
                            IsVerified = i % 2 == 0,
                            NotificationHours = [9, 12, 17],
                            Metadata = [new ContactDetail { Label = "domain", Value = "company.com" }]
                        },
                        new Contact
                        {
                            Type = "phone",
                            Value = $"+1-555-{1000 + i:D4}",
                            IsVerified = i % 3 == 0,
                            NotificationHours = [10, 15],
                            Metadata = [new ContactDetail { Label = "carrier", Value = "Verizon" }]
                        }
                    ],

                    // Phone directory
                    PhoneDirectory = new Dictionary<string, string>
                    {
                        ["desk"] = $"x{3000 + i}",
                        ["mobile"] = $"+1-555-{2000 + i:D4}"
                    },

                    // Bonus history
                    BonusByYear = new Dictionary<int, decimal>
                    {
                        [2022] = 5000m + i * 100,
                        [2023] = 6000m + i * 150,
                        [2024] = 7000m + i * 200
                    },

                    // Office locations (Dict<string, Address>)
                    OfficeLocations = new Dictionary<string, Address>
                    {
                        ["HQ"] = new Address
                        {
                            City = i % 5 == 0 ? "New York" : city,
                            Street = "1 Corporate Drive",
                            Building = new BuildingInfo { Floor = 1, Name = "Main HQ" }
                        }
                    },

                    // Performance reviews (Dict<(int,string), string>)
                    PerformanceReviews = new Dictionary<(int Year, string Quarter), string>
                    {
                        [(2024, "Q1")] = i % 3 == 0 ? "Excellent" : i % 2 == 0 ? "Good" : "Satisfactory",
                        [(2024, "Q2")] = i % 4 == 0 ? "Excellent" : "Good"
                    }
                }
            };

            result.Add(emp);
        }

        return result;
    }

    private static string[] GetSkillsForPosition(string position, int index)
    {
        var skills = position switch
        {
            "Developer" => new[] { "C#", "SQL", "Azure", "Docker" },
            "Designer" => new[] { "Figma", "Photoshop", "CSS" },
            "Analyst" => new[] { "SQL", "Excel", "Python" },
            "Manager" => new[] { "Leadership", "Agile", "Communication" },
            _ => new[] { "Communication", "Teamwork" }
        };
        var take = position == "Developer" ? 2 + index % 3 : 2 + index % 2;
        return skills.Take(Math.Min(take, skills.Length)).ToArray();
    }
}
