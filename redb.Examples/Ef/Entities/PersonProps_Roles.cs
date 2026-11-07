using System;
using System.Collections.Generic;

namespace redb.Examples.Ef.Entities;

public partial class PersonProps_Roles
{
    public long _id { get; set; }

    public long _owner_id { get; set; }

    public int _idx { get; set; }

    public long? _value { get; set; }

    public virtual PersonProps _owner { get; set; } = null!;

    public virtual list_items? _valueNavigation { get; set; }
}
