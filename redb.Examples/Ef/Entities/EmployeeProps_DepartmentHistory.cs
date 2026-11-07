using System;
using System.Collections.Generic;

namespace redb.Examples.Ef.Entities;

public partial class EmployeeProps_DepartmentHistory
{
    public long _id { get; set; }

    public long _owner_id { get; set; }

    public string _key { get; set; } = null!;

    public string? Name { get; set; }

    public int? HeadCount { get; set; }

    public virtual ICollection<EmployeeProps_DepartmentHistory_BudgetByYear> EmployeeProps_DepartmentHistory_BudgetByYear { get; set; } = new List<EmployeeProps_DepartmentHistory_BudgetByYear>();

    public virtual ICollection<EmployeeProps_DepartmentHistory_Leaders> EmployeeProps_DepartmentHistory_Leaders { get; set; } = new List<EmployeeProps_DepartmentHistory_Leaders>();

    public virtual ICollection<EmployeeProps_DepartmentHistory_Projects> EmployeeProps_DepartmentHistory_Projects { get; set; } = new List<EmployeeProps_DepartmentHistory_Projects>();

    public virtual EmployeeProps _owner { get; set; } = null!;
}
