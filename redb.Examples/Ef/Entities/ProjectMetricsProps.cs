using System;
using System.Collections.Generic;

namespace redb.Examples.Ef.Entities;

public partial class ProjectMetricsProps
{
    public long _id { get; set; }

    public long? ProjectId { get; set; }

    public long? TasksCompleted { get; set; }

    public long? TasksTotal { get; set; }

    public long? BugsFixed { get; set; }

    public double? Budget { get; set; }

    public long? TeamSize { get; set; }

    public virtual ICollection<EmployeeProps> EmployeeProps { get; set; } = new List<EmployeeProps>();

    public virtual ICollection<EmployeeProps_PastProjects> EmployeeProps_PastProjects { get; set; } = new List<EmployeeProps_PastProjects>();

    public virtual ICollection<EmployeeProps_ProjectMetrics> EmployeeProps_ProjectMetrics { get; set; } = new List<EmployeeProps_ProjectMetrics>();

    public virtual ICollection<ProjectMetricsProps_Tags> ProjectMetricsProps_Tags { get; set; } = new List<ProjectMetricsProps_Tags>();

    public virtual ICollection<ProjectMetricsProps_Technologies> ProjectMetricsProps_Technologies { get; set; } = new List<ProjectMetricsProps_Technologies>();

    public virtual objects objects { get; set; } = null!;
}
