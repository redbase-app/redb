using System;
using System.Collections.Generic;

namespace redb.Examples.Ef.Entities;

public partial class EmployeeProps
{
    public long _id { get; set; }

    public string? FirstName { get; set; }

    public string? LastName { get; set; }

    public int? Age { get; set; }

    public DateTime? HireDate { get; set; }

    public string? Position { get; set; }

    public decimal? Salary { get; set; }

    public string? Department { get; set; }

    public string? EmployeeCode { get; set; }

    public long? CurrentProject { get; set; }

    public virtual ProjectMetricsProps? CurrentProjectNavigation { get; set; }

    public virtual ICollection<EmployeeProps_BonusByYear> EmployeeProps_BonusByYear { get; set; } = new List<EmployeeProps_BonusByYear>();

    public virtual ICollection<EmployeeProps_CertificationYears> EmployeeProps_CertificationYears { get; set; } = new List<EmployeeProps_CertificationYears>();

    public virtual ICollection<EmployeeProps_Certifications> EmployeeProps_Certifications { get; set; } = new List<EmployeeProps_Certifications>();

    public virtual ICollection<EmployeeProps_Contacts> EmployeeProps_Contacts { get; set; } = new List<EmployeeProps_Contacts>();

    public virtual ICollection<EmployeeProps_DepartmentHistory> EmployeeProps_DepartmentHistory { get; set; } = new List<EmployeeProps_DepartmentHistory>();

    public virtual EmployeeProps_EmergencyAddress? EmployeeProps_EmergencyAddress { get; set; }

    public virtual EmployeeProps_HomeAddress? EmployeeProps_HomeAddress { get; set; }

    public virtual ICollection<EmployeeProps_OfficeLocations> EmployeeProps_OfficeLocations { get; set; } = new List<EmployeeProps_OfficeLocations>();

    public virtual ICollection<EmployeeProps_PastProjects> EmployeeProps_PastProjects { get; set; } = new List<EmployeeProps_PastProjects>();

    public virtual ICollection<EmployeeProps_PerformanceReviews> EmployeeProps_PerformanceReviews { get; set; } = new List<EmployeeProps_PerformanceReviews>();

    public virtual ICollection<EmployeeProps_PhoneDirectory> EmployeeProps_PhoneDirectory { get; set; } = new List<EmployeeProps_PhoneDirectory>();

    public virtual ICollection<EmployeeProps_ProjectMetrics> EmployeeProps_ProjectMetrics { get; set; } = new List<EmployeeProps_ProjectMetrics>();

    public virtual ICollection<EmployeeProps_SkillLevels> EmployeeProps_SkillLevels { get; set; } = new List<EmployeeProps_SkillLevels>();

    public virtual ICollection<EmployeeProps_Skills> EmployeeProps_Skills { get; set; } = new List<EmployeeProps_Skills>();

    public virtual EmployeeProps_WorkAddress? EmployeeProps_WorkAddress { get; set; }

    public virtual objects objects { get; set; } = null!;
}
