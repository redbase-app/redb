using System;
using System.Collections.Generic;

namespace redb.Examples.Ef.Entities;

public partial class lists
{
    public long _id { get; set; }

    public string _name { get; set; } = null!;

    public string? _alias { get; set; }

    public virtual ICollection<list_items> list_items { get; set; } = new List<list_items>();
}
