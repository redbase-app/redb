using System;
using System.Collections.Generic;

namespace redb.Examples.Ef.Entities;

public partial class EmployeeProps_Contacts_Metadata
{
    public long _id { get; set; }

    public long _owner_id { get; set; }

    public int _idx { get; set; }

    public string? Label { get; set; }

    public string? Value { get; set; }

    public virtual EmployeeProps_Contacts _owner { get; set; } = null!;
}
