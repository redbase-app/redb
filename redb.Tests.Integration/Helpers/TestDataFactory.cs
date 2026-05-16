using redb.Core;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Helpers;

public static class TestDataFactory
{
    private static readonly string[] Positions = ["Developer", "Senior Developer", "Manager", "Analyst", "Tester"];
    private static readonly string[] Departments = ["Engineering", "Design", "Sales", "Marketing", "Support"];
    private static readonly string[] Cities = ["New York", "London", "Berlin", "Tokyo", "Sydney"];
    private static readonly string[] Streets = ["Main St", "Oak Ave", "Pine Rd", "Elm St", "Cedar Blvd"];
    private static readonly string[][] SkillSets =
    [
        ["C#", "SQL", "Azure"],
        ["JavaScript", "React", "Node.js"],
        ["Python", "Django", "PostgreSQL"],
        ["Java", "Spring", "Docker"],
        ["TypeScript", "Angular", "AWS"]
    ];

    public static RedbObject<SimpleProps> CreateSimple(string title = "Test Item", decimal price = 99.99m) => new()
    {
        name = title,
        Props = new SimpleProps
        {
            Title = title,
            Count = 10,
            Price = price,
            CreatedAt = new DateTime(2025, 6, 15, 10, 30, 0, DateTimeKind.Utc),
            IsActive = true,
            Code = Guid.NewGuid(),
            Description = "Test description"
        }
    };

    public static RedbObject<EmployeeProps> CreateEmployee(
        int index,
        string? firstName = null,
        string? position = null,
        decimal? salary = null,
        string? department = null,
        string? city = null)
    {
        var pos = position ?? Positions[index % Positions.Length];
        var dept = department ?? Departments[index % Departments.Length];
        var c = city ?? Cities[index % Cities.Length];
        var sal = salary ?? 50000m + index * 5000m;
        var fn = firstName ?? $"Employee{index:D3}";

        return new RedbObject<EmployeeProps>
        {
            name = $"{fn} {pos}",
            Props = new EmployeeProps
            {
                FirstName = fn,
                LastName = $"Last{index:D3}",
                Age = 25 + (index % 30),
                HireDate = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddMonths(index),
                Position = pos,
                Salary = sal,
                Department = dept,
                EmployeeCode = index % 3 == 0 ? null : $"EMP-{index:D4}",
                IsRemote = index % 2 == 0,
                Rating = index % 4 == 0 ? null : 3.0 + (index % 5) * 0.5,
                Skills = SkillSets[index % SkillSets.Length],
                SkillLevels = [5 - (index % 3), 4, 3 + (index % 2)],
                HomeAddress = new TestAddress
                {
                    City = c,
                    Street = Streets[index % Streets.Length],
                    Building = new TestBuildingInfo
                    {
                        Floor = 1 + (index % 20),
                        Name = $"Building {index % 5 + 1}",
                        Amenities = ["Parking", "Gym"]
                    }
                },
                WorkAddress = new TestAddress
                {
                    City = c,
                    Street = "Office Park 1"
                },
                Contacts =
                [
                    new TestContact
                    {
                        Type = "email",
                        Value = $"emp{index}@test.com",
                        IsVerified = true,
                        Metadata = [new TestContactDetail { Label = "work", Value = "primary" }]
                    },
                    new TestContact
                    {
                        Type = "phone",
                        Value = $"+1-555-{index:D4}",
                        IsVerified = false
                    }
                ],
                PhoneDirectory = new Dictionary<string, string>
                {
                    ["desk"] = $"555-{1000 + index}",
                    ["mobile"] = $"555-{2000 + index}"
                },
                OfficeLocations = new Dictionary<string, TestAddress>
                {
                    ["HQ"] = new() { City = "New York", Street = "5th Ave" },
                    ["Branch"] = new() { City = c, Street = "Local St" }
                },
                BonusByYear = new Dictionary<int, decimal>
                {
                    [2023] = 5000m + index * 100m,
                    [2024] = 6000m + index * 150m
                },
                DepartmentHistory = new Dictionary<string, TestDepartment>
                {
                    [dept] = new()
                    {
                        Name = dept,
                        HeadCount = 10 + index,
                        Projects = [$"Proj-{index}"],
                        Leaders = [new TestTag { Name = "Lead", Priority = 1 }]
                    }
                },
                PerformanceReviews = new Dictionary<(int Year, string Quarter), string>
                {
                    [(2024, "Q1")] = index % 3 == 0 ? "Excellent" : "Good",
                    [(2024, "Q2")] = "Meets expectations"
                }
            }
        };
    }

    public static async Task<List<long>> SeedEmployees(IRedbService redb, int count = 20)
    {
        var employees = Enumerable.Range(0, count).Select(i => CreateEmployee(i)).ToList();
        var ids = new List<long>();
        foreach (var emp in employees)
        {
            emp.id = await redb.SaveAsync(emp);
            ids.Add(emp.id);
        }
        return ids;
    }

    public static TreeRedbObject<TreeNodeProps> CreateTreeNode(
        string name, string code, decimal budget = 100000m) => new()
    {
        name = name,
        Props = new TreeNodeProps
        {
            Name = name,
            Description = $"{name} department",
            IsActive = true,
            Budget = budget,
            Code = code
        }
    };
}
