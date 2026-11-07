using System;
using System.Collections.Generic;

namespace redb.Examples.Ef.Entities;

public partial class list_items
{
    public long _id { get; set; }

    public long _id_list { get; set; }

    public string? _value { get; set; }

    public string? _alias { get; set; }

    public long? _id_object { get; set; }

    public virtual ICollection<PersonProps> PersonProps { get; set; } = new List<PersonProps>();

    public virtual ICollection<PersonProps_Roles> PersonProps_Roles { get; set; } = new List<PersonProps_Roles>();

    public virtual lists _id_listNavigation { get; set; } = null!;

    public virtual objects? _id_objectNavigation { get; set; }
}
