using System;
using System.Collections.Generic;

namespace redb.Examples.Ef.Entities;

public partial class DepartmentProps
{
    public long _id { get; set; }

    public string? Name { get; set; }

    public string? Description { get; set; }

    public bool? IsActive { get; set; }

    public decimal? Budget { get; set; }

    public string? Code { get; set; }

    public virtual objects objects { get; set; } = null!;
}
