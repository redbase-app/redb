using System;
using System.Collections.Generic;

namespace redb.Examples.Ef.Entities;

public partial class EmployeeProps_Contacts
{
    public long _id { get; set; }

    public long _owner_id { get; set; }

    public int _idx { get; set; }

    public string? Type { get; set; }

    public string? Value { get; set; }

    public bool? IsVerified { get; set; }

    public virtual ICollection<EmployeeProps_Contacts_Metadata> EmployeeProps_Contacts_Metadata { get; set; } = new List<EmployeeProps_Contacts_Metadata>();

    public virtual ICollection<EmployeeProps_Contacts_NotificationHours> EmployeeProps_Contacts_NotificationHours { get; set; } = new List<EmployeeProps_Contacts_NotificationHours>();

    public virtual EmployeeProps _owner { get; set; } = null!;
}
