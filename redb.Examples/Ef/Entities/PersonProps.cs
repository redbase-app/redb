using System;
using System.Collections.Generic;

namespace redb.Examples.Ef.Entities;

public partial class PersonProps
{
    public long _id { get; set; }

    public string? Name { get; set; }

    public int? Age { get; set; }

    public string? Email { get; set; }

    public long? Status { get; set; }

    public virtual ICollection<PersonProps_Roles> PersonProps_Roles { get; set; } = new List<PersonProps_Roles>();

    public virtual list_items? StatusNavigation { get; set; }

    public virtual objects objects { get; set; } = null!;
}
