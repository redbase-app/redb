using System;
using System.Collections.Generic;

namespace redb.Examples.Ef.Entities;

public partial class EmployeeProps_DepartmentHistory_BudgetByYear
{
    public long _id { get; set; }

    public long _owner_id { get; set; }

    public string _key { get; set; } = null!;

    public int? _value { get; set; }

    public virtual EmployeeProps_DepartmentHistory _owner { get; set; } = null!;
}
