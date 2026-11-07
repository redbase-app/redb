using System;
using System.Collections.Generic;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef.Entities;

namespace redb.Examples.Ef;

public partial class FlatContext : DbContext
{
    public FlatContext(DbContextOptions<FlatContext> options)
        : base(options)
    {
    }

    public virtual DbSet<CityProps> CityProps { get; set; }

    public virtual DbSet<CityProps_Coordinates> CityProps_Coordinates { get; set; }

    public virtual DbSet<DepartmentProps> DepartmentProps { get; set; }

    public virtual DbSet<EmployeeProps> EmployeeProps { get; set; }

    public virtual DbSet<EmployeeProps_BonusByYear> EmployeeProps_BonusByYear { get; set; }

    public virtual DbSet<EmployeeProps_CertificationYears> EmployeeProps_CertificationYears { get; set; }

    public virtual DbSet<EmployeeProps_Certifications> EmployeeProps_Certifications { get; set; }

    public virtual DbSet<EmployeeProps_Contacts> EmployeeProps_Contacts { get; set; }

    public virtual DbSet<EmployeeProps_Contacts_Metadata> EmployeeProps_Contacts_Metadata { get; set; }

    public virtual DbSet<EmployeeProps_Contacts_NotificationHours> EmployeeProps_Contacts_NotificationHours { get; set; }

    public virtual DbSet<EmployeeProps_DepartmentHistory> EmployeeProps_DepartmentHistory { get; set; }

    public virtual DbSet<EmployeeProps_DepartmentHistory_BudgetByYear> EmployeeProps_DepartmentHistory_BudgetByYear { get; set; }

    public virtual DbSet<EmployeeProps_DepartmentHistory_Leaders> EmployeeProps_DepartmentHistory_Leaders { get; set; }

    public virtual DbSet<EmployeeProps_DepartmentHistory_Projects> EmployeeProps_DepartmentHistory_Projects { get; set; }

    public virtual DbSet<EmployeeProps_EmergencyAddress> EmployeeProps_EmergencyAddress { get; set; }

    public virtual DbSet<EmployeeProps_EmergencyAddress_Building> EmployeeProps_EmergencyAddress_Building { get; set; }

    public virtual DbSet<EmployeeProps_EmergencyAddress_Building_AccessCodes> EmployeeProps_EmergencyAddress_Building_AccessCodes { get; set; }

    public virtual DbSet<EmployeeProps_EmergencyAddress_Building_Amenities> EmployeeProps_EmergencyAddress_Building_Amenities { get; set; }

    public virtual DbSet<EmployeeProps_EmergencyAddress_Building_ElevatorFloors> EmployeeProps_EmergencyAddress_Building_ElevatorFloors { get; set; }

    public virtual DbSet<EmployeeProps_EmergencyAddress_Building_ParkingSpots> EmployeeProps_EmergencyAddress_Building_ParkingSpots { get; set; }

    public virtual DbSet<EmployeeProps_HomeAddress> EmployeeProps_HomeAddress { get; set; }

    public virtual DbSet<EmployeeProps_HomeAddress_Building> EmployeeProps_HomeAddress_Building { get; set; }

    public virtual DbSet<EmployeeProps_HomeAddress_Building_AccessCodes> EmployeeProps_HomeAddress_Building_AccessCodes { get; set; }

    public virtual DbSet<EmployeeProps_HomeAddress_Building_Amenities> EmployeeProps_HomeAddress_Building_Amenities { get; set; }

    public virtual DbSet<EmployeeProps_HomeAddress_Building_ElevatorFloors> EmployeeProps_HomeAddress_Building_ElevatorFloors { get; set; }

    public virtual DbSet<EmployeeProps_HomeAddress_Building_ParkingSpots> EmployeeProps_HomeAddress_Building_ParkingSpots { get; set; }

    public virtual DbSet<EmployeeProps_OfficeLocations> EmployeeProps_OfficeLocations { get; set; }

    public virtual DbSet<EmployeeProps_OfficeLocations_Building> EmployeeProps_OfficeLocations_Building { get; set; }

    public virtual DbSet<EmployeeProps_OfficeLocations_Building_AccessCodes> EmployeeProps_OfficeLocations_Building_AccessCodes { get; set; }

    public virtual DbSet<EmployeeProps_OfficeLocations_Building_Amenities> EmployeeProps_OfficeLocations_Building_Amenities { get; set; }

    public virtual DbSet<EmployeeProps_OfficeLocations_Building_ElevatorFloors> EmployeeProps_OfficeLocations_Building_ElevatorFloors { get; set; }

    public virtual DbSet<EmployeeProps_OfficeLocations_Building_ParkingSpots> EmployeeProps_OfficeLocations_Building_ParkingSpots { get; set; }

    public virtual DbSet<EmployeeProps_PastProjects> EmployeeProps_PastProjects { get; set; }

    public virtual DbSet<EmployeeProps_PerformanceReviews> EmployeeProps_PerformanceReviews { get; set; }

    public virtual DbSet<EmployeeProps_PhoneDirectory> EmployeeProps_PhoneDirectory { get; set; }

    public virtual DbSet<EmployeeProps_ProjectMetrics> EmployeeProps_ProjectMetrics { get; set; }

    public virtual DbSet<EmployeeProps_SkillLevels> EmployeeProps_SkillLevels { get; set; }

    public virtual DbSet<EmployeeProps_Skills> EmployeeProps_Skills { get; set; }

    public virtual DbSet<EmployeeProps_WorkAddress> EmployeeProps_WorkAddress { get; set; }

    public virtual DbSet<EmployeeProps_WorkAddress_Building> EmployeeProps_WorkAddress_Building { get; set; }

    public virtual DbSet<EmployeeProps_WorkAddress_Building_AccessCodes> EmployeeProps_WorkAddress_Building_AccessCodes { get; set; }

    public virtual DbSet<EmployeeProps_WorkAddress_Building_Amenities> EmployeeProps_WorkAddress_Building_Amenities { get; set; }

    public virtual DbSet<EmployeeProps_WorkAddress_Building_ElevatorFloors> EmployeeProps_WorkAddress_Building_ElevatorFloors { get; set; }

    public virtual DbSet<EmployeeProps_WorkAddress_Building_ParkingSpots> EmployeeProps_WorkAddress_Building_ParkingSpots { get; set; }

    public virtual DbSet<ExampleResultProps> ExampleResultProps { get; set; }

    public virtual DbSet<ExampleResultProps_Results> ExampleResultProps_Results { get; set; }

    public virtual DbSet<PersonProps> PersonProps { get; set; }

    public virtual DbSet<PersonProps_Roles> PersonProps_Roles { get; set; }

    public virtual DbSet<ProjectMetricsProps> ProjectMetricsProps { get; set; }

    public virtual DbSet<ProjectMetricsProps_Tags> ProjectMetricsProps_Tags { get; set; }

    public virtual DbSet<ProjectMetricsProps_Technologies> ProjectMetricsProps_Technologies { get; set; }

    public virtual DbSet<list_items> list_items { get; set; }

    public virtual DbSet<lists> lists { get; set; }

    public virtual DbSet<objects> objects { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<CityProps>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.Property(e => e._id).ValueGeneratedNever();

            entity.HasOne(d => d.objects).WithOne(p => p.CityProps)
                .HasForeignKey<CityProps>(d => d._id)
                .HasConstraintName("FK_CityProps__id");
        });

        modelBuilder.Entity<CityProps_Coordinates>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("CityProps.Coordinates");

            entity.HasIndex(e => new { e._owner_id, e._idx }, "UQ_CityProps.Coordinates").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._owner).WithMany(p => p.CityProps_Coordinates)
                .HasForeignKey(d => d._owner_id)
                .HasConstraintName("FK_CityProps.Coordinates__owner_id");
        });

        modelBuilder.Entity<DepartmentProps>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.Property(e => e._id).ValueGeneratedNever();
            entity.Property(e => e.Budget).HasPrecision(38, 18);

            entity.HasOne(d => d.objects).WithOne(p => p.DepartmentProps)
                .HasForeignKey<DepartmentProps>(d => d._id)
                .HasConstraintName("FK_DepartmentProps__id");
        });

        modelBuilder.Entity<EmployeeProps>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.Property(e => e._id).ValueGeneratedNever();
            entity.Property(e => e.Salary).HasPrecision(38, 18);

            entity.HasOne(d => d.CurrentProjectNavigation).WithMany(p => p.EmployeeProps)
                .HasForeignKey(d => d.CurrentProject)
                .HasConstraintName("FK_EmployeeProps_CurrentProject");

            entity.HasOne(d => d.objects).WithOne(p => p.EmployeeProps)
                .HasForeignKey<EmployeeProps>(d => d._id)
                .HasConstraintName("FK_EmployeeProps__id");
        });

        modelBuilder.Entity<EmployeeProps_BonusByYear>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("EmployeeProps.BonusByYear");

            entity.HasIndex(e => new { e._owner_id, e._key }, "UQ_EmployeeProps.BonusByYear").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");
            entity.Property(e => e._value).HasPrecision(38, 18);

            entity.HasOne(d => d._owner).WithMany(p => p.EmployeeProps_BonusByYear)
                .HasForeignKey(d => d._owner_id)
                .HasConstraintName("FK_EmployeeProps.BonusByYear__owner_id");
        });

        modelBuilder.Entity<EmployeeProps_CertificationYears>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("EmployeeProps.CertificationYears");

            entity.HasIndex(e => new { e._owner_id, e._idx }, "UQ_EmployeeProps.CertificationYears").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._owner).WithMany(p => p.EmployeeProps_CertificationYears)
                .HasForeignKey(d => d._owner_id)
                .HasConstraintName("FK_EmployeeProps.CertificationYears__owner_id");
        });

        modelBuilder.Entity<EmployeeProps_Certifications>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("EmployeeProps.Certifications");

            entity.HasIndex(e => new { e._owner_id, e._idx }, "UQ_EmployeeProps.Certifications").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._owner).WithMany(p => p.EmployeeProps_Certifications)
                .HasForeignKey(d => d._owner_id)
                .HasConstraintName("FK_EmployeeProps.Certifications__owner_id");
        });

        modelBuilder.Entity<EmployeeProps_Contacts>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("EmployeeProps.Contacts");

            entity.HasIndex(e => new { e._owner_id, e._idx }, "UQ_EmployeeProps.Contacts").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._owner).WithMany(p => p.EmployeeProps_Contacts)
                .HasForeignKey(d => d._owner_id)
                .HasConstraintName("FK_EmployeeProps.Contacts__owner_id");
        });

        modelBuilder.Entity<EmployeeProps_Contacts_Metadata>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("EmployeeProps.Contacts.Metadata");

            entity.HasIndex(e => new { e._owner_id, e._idx }, "UQ_EmployeeProps.Contacts.Metadata").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._owner).WithMany(p => p.EmployeeProps_Contacts_Metadata)
                .HasForeignKey(d => d._owner_id)
                .HasConstraintName("FK_EmployeeProps.Contacts.Metadata__owner_id");
        });

        modelBuilder.Entity<EmployeeProps_Contacts_NotificationHours>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("EmployeeProps.Contacts.NotificationHours");

            entity.HasIndex(e => new { e._owner_id, e._idx }, "UQ_EmployeeProps.Contacts.NotificationHours").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._owner).WithMany(p => p.EmployeeProps_Contacts_NotificationHours)
                .HasForeignKey(d => d._owner_id)
                .HasConstraintName("FK_EmployeeProps.Contacts.NotificationHours__owner_id");
        });

        modelBuilder.Entity<EmployeeProps_DepartmentHistory>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("EmployeeProps.DepartmentHistory");

            entity.HasIndex(e => new { e._owner_id, e._key }, "UQ_EmployeeProps.DepartmentHistory").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._owner).WithMany(p => p.EmployeeProps_DepartmentHistory)
                .HasForeignKey(d => d._owner_id)
                .HasConstraintName("FK_EmployeeProps.DepartmentHistory__owner_id");
        });

        modelBuilder.Entity<EmployeeProps_DepartmentHistory_BudgetByYear>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("EmployeeProps.DepartmentHistory.BudgetByYear");

            entity.HasIndex(e => new { e._owner_id, e._key }, "UQ_EmployeeProps.DepartmentHistory.BudgetByYear").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._owner).WithMany(p => p.EmployeeProps_DepartmentHistory_BudgetByYear)
                .HasForeignKey(d => d._owner_id)
                .HasConstraintName("FK_EmployeeProps.DepartmentHistory.BudgetByYear__owner_id");
        });

        modelBuilder.Entity<EmployeeProps_DepartmentHistory_Leaders>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("EmployeeProps.DepartmentHistory.Leaders");

            entity.HasIndex(e => new { e._owner_id, e._idx }, "UQ_EmployeeProps.DepartmentHistory.Leaders").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._owner).WithMany(p => p.EmployeeProps_DepartmentHistory_Leaders)
                .HasForeignKey(d => d._owner_id)
                .HasConstraintName("FK_EmployeeProps.DepartmentHistory.Leaders__owner_id");
        });

        modelBuilder.Entity<EmployeeProps_DepartmentHistory_Projects>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("EmployeeProps.DepartmentHistory.Projects");

            entity.HasIndex(e => new { e._owner_id, e._idx }, "UQ_EmployeeProps.DepartmentHistory.Projects").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._owner).WithMany(p => p.EmployeeProps_DepartmentHistory_Projects)
                .HasForeignKey(d => d._owner_id)
                .HasConstraintName("FK_EmployeeProps.DepartmentHistory.Projects__owner_id");
        });

        modelBuilder.Entity<EmployeeProps_EmergencyAddress>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("EmployeeProps.EmergencyAddress");

            entity.HasIndex(e => e._owner_id, "UQ_EmployeeProps.EmergencyAddress").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._owner).WithOne(p => p.EmployeeProps_EmergencyAddress)
                .HasForeignKey<EmployeeProps_EmergencyAddress>(d => d._owner_id)
                .HasConstraintName("FK_EmployeeProps.EmergencyAddress__owner_id");
        });

        modelBuilder.Entity<EmployeeProps_EmergencyAddress_Building>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("EmployeeProps.EmergencyAddress.Building");

            entity.HasIndex(e => e._owner_id, "UQ_EmployeeProps.EmergencyAddress.Building").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._owner).WithOne(p => p.EmployeeProps_EmergencyAddress_Building)
                .HasForeignKey<EmployeeProps_EmergencyAddress_Building>(d => d._owner_id)
                .HasConstraintName("FK_EmployeeProps.EmergencyAddress.Building__owner_id");
        });

        modelBuilder.Entity<EmployeeProps_EmergencyAddress_Building_AccessCodes>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("EmployeeProps.EmergencyAddress.Building.AccessCodes");

            entity.HasIndex(e => new { e._owner_id, e._idx }, "UQ_EmployeeProps.EmergencyAddress.Building.AccessCodes").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._owner).WithMany(p => p.EmployeeProps_EmergencyAddress_Building_AccessCodes)
                .HasForeignKey(d => d._owner_id)
                .HasConstraintName("FK_EmployeeProps.EmergencyAddress.Building.AccessCodes~20cad047");
        });

        modelBuilder.Entity<EmployeeProps_EmergencyAddress_Building_Amenities>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("EmployeeProps.EmergencyAddress.Building.Amenities");

            entity.HasIndex(e => new { e._owner_id, e._idx }, "UQ_EmployeeProps.EmergencyAddress.Building.Amenities").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._owner).WithMany(p => p.EmployeeProps_EmergencyAddress_Building_Amenities)
                .HasForeignKey(d => d._owner_id)
                .HasConstraintName("FK_EmployeeProps.EmergencyAddress.Building.Amenities__owner_id");
        });

        modelBuilder.Entity<EmployeeProps_EmergencyAddress_Building_ElevatorFloors>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("EmployeeProps.EmergencyAddress.Building.ElevatorFloors");

            entity.HasIndex(e => new { e._owner_id, e._idx }, "UQ_EmployeeProps.EmergencyAddress.Building.ElevatorFloors").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._owner).WithMany(p => p.EmployeeProps_EmergencyAddress_Building_ElevatorFloors)
                .HasForeignKey(d => d._owner_id)
                .HasConstraintName("FK_EmployeeProps.EmergencyAddress.Building.ElevatorFlo~50707330");
        });

        modelBuilder.Entity<EmployeeProps_EmergencyAddress_Building_ParkingSpots>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("EmployeeProps.EmergencyAddress.Building.ParkingSpots");

            entity.HasIndex(e => new { e._owner_id, e._idx }, "UQ_EmployeeProps.EmergencyAddress.Building.ParkingSpots").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._owner).WithMany(p => p.EmployeeProps_EmergencyAddress_Building_ParkingSpots)
                .HasForeignKey(d => d._owner_id)
                .HasConstraintName("FK_EmployeeProps.EmergencyAddress.Building.ParkingSpot~7c1178eb");
        });

        modelBuilder.Entity<EmployeeProps_HomeAddress>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("EmployeeProps.HomeAddress");

            entity.HasIndex(e => e._owner_id, "UQ_EmployeeProps.HomeAddress").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._owner).WithOne(p => p.EmployeeProps_HomeAddress)
                .HasForeignKey<EmployeeProps_HomeAddress>(d => d._owner_id)
                .HasConstraintName("FK_EmployeeProps.HomeAddress__owner_id");
        });

        modelBuilder.Entity<EmployeeProps_HomeAddress_Building>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("EmployeeProps.HomeAddress.Building");

            entity.HasIndex(e => e._owner_id, "UQ_EmployeeProps.HomeAddress.Building").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._owner).WithOne(p => p.EmployeeProps_HomeAddress_Building)
                .HasForeignKey<EmployeeProps_HomeAddress_Building>(d => d._owner_id)
                .HasConstraintName("FK_EmployeeProps.HomeAddress.Building__owner_id");
        });

        modelBuilder.Entity<EmployeeProps_HomeAddress_Building_AccessCodes>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("EmployeeProps.HomeAddress.Building.AccessCodes");

            entity.HasIndex(e => new { e._owner_id, e._idx }, "UQ_EmployeeProps.HomeAddress.Building.AccessCodes").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._owner).WithMany(p => p.EmployeeProps_HomeAddress_Building_AccessCodes)
                .HasForeignKey(d => d._owner_id)
                .HasConstraintName("FK_EmployeeProps.HomeAddress.Building.AccessCodes__owner_id");
        });

        modelBuilder.Entity<EmployeeProps_HomeAddress_Building_Amenities>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("EmployeeProps.HomeAddress.Building.Amenities");

            entity.HasIndex(e => new { e._owner_id, e._idx }, "UQ_EmployeeProps.HomeAddress.Building.Amenities").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._owner).WithMany(p => p.EmployeeProps_HomeAddress_Building_Amenities)
                .HasForeignKey(d => d._owner_id)
                .HasConstraintName("FK_EmployeeProps.HomeAddress.Building.Amenities__owner_id");
        });

        modelBuilder.Entity<EmployeeProps_HomeAddress_Building_ElevatorFloors>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("EmployeeProps.HomeAddress.Building.ElevatorFloors");

            entity.HasIndex(e => new { e._owner_id, e._idx }, "UQ_EmployeeProps.HomeAddress.Building.ElevatorFloors").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._owner).WithMany(p => p.EmployeeProps_HomeAddress_Building_ElevatorFloors)
                .HasForeignKey(d => d._owner_id)
                .HasConstraintName("FK_EmployeeProps.HomeAddress.Building.ElevatorFloors__owner_id");
        });

        modelBuilder.Entity<EmployeeProps_HomeAddress_Building_ParkingSpots>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("EmployeeProps.HomeAddress.Building.ParkingSpots");

            entity.HasIndex(e => new { e._owner_id, e._idx }, "UQ_EmployeeProps.HomeAddress.Building.ParkingSpots").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._owner).WithMany(p => p.EmployeeProps_HomeAddress_Building_ParkingSpots)
                .HasForeignKey(d => d._owner_id)
                .HasConstraintName("FK_EmployeeProps.HomeAddress.Building.ParkingSpots__owner_id");
        });

        modelBuilder.Entity<EmployeeProps_OfficeLocations>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("EmployeeProps.OfficeLocations");

            entity.HasIndex(e => new { e._owner_id, e._key }, "UQ_EmployeeProps.OfficeLocations").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._owner).WithMany(p => p.EmployeeProps_OfficeLocations)
                .HasForeignKey(d => d._owner_id)
                .HasConstraintName("FK_EmployeeProps.OfficeLocations__owner_id");
        });

        modelBuilder.Entity<EmployeeProps_OfficeLocations_Building>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("EmployeeProps.OfficeLocations.Building");

            entity.HasIndex(e => e._owner_id, "UQ_EmployeeProps.OfficeLocations.Building").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._owner).WithOne(p => p.EmployeeProps_OfficeLocations_Building)
                .HasForeignKey<EmployeeProps_OfficeLocations_Building>(d => d._owner_id)
                .HasConstraintName("FK_EmployeeProps.OfficeLocations.Building__owner_id");
        });

        modelBuilder.Entity<EmployeeProps_OfficeLocations_Building_AccessCodes>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("EmployeeProps.OfficeLocations.Building.AccessCodes");

            entity.HasIndex(e => new { e._owner_id, e._idx }, "UQ_EmployeeProps.OfficeLocations.Building.AccessCodes").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._owner).WithMany(p => p.EmployeeProps_OfficeLocations_Building_AccessCodes)
                .HasForeignKey(d => d._owner_id)
                .HasConstraintName("FK_EmployeeProps.OfficeLocations.Building.AccessCodes__owner_id");
        });

        modelBuilder.Entity<EmployeeProps_OfficeLocations_Building_Amenities>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("EmployeeProps.OfficeLocations.Building.Amenities");

            entity.HasIndex(e => new { e._owner_id, e._idx }, "UQ_EmployeeProps.OfficeLocations.Building.Amenities").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._owner).WithMany(p => p.EmployeeProps_OfficeLocations_Building_Amenities)
                .HasForeignKey(d => d._owner_id)
                .HasConstraintName("FK_EmployeeProps.OfficeLocations.Building.Amenities__owner_id");
        });

        modelBuilder.Entity<EmployeeProps_OfficeLocations_Building_ElevatorFloors>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("EmployeeProps.OfficeLocations.Building.ElevatorFloors");

            entity.HasIndex(e => new { e._owner_id, e._idx }, "UQ_EmployeeProps.OfficeLocations.Building.ElevatorFloors").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._owner).WithMany(p => p.EmployeeProps_OfficeLocations_Building_ElevatorFloors)
                .HasForeignKey(d => d._owner_id)
                .HasConstraintName("FK_EmployeeProps.OfficeLocations.Building.ElevatorFloo~6445e391");
        });

        modelBuilder.Entity<EmployeeProps_OfficeLocations_Building_ParkingSpots>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("EmployeeProps.OfficeLocations.Building.ParkingSpots");

            entity.HasIndex(e => new { e._owner_id, e._idx }, "UQ_EmployeeProps.OfficeLocations.Building.ParkingSpots").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._owner).WithMany(p => p.EmployeeProps_OfficeLocations_Building_ParkingSpots)
                .HasForeignKey(d => d._owner_id)
                .HasConstraintName("FK_EmployeeProps.OfficeLocations.Building.ParkingSpots~439995bd");
        });

        modelBuilder.Entity<EmployeeProps_PastProjects>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("EmployeeProps.PastProjects");

            entity.HasIndex(e => new { e._owner_id, e._idx }, "UQ_EmployeeProps.PastProjects").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._owner).WithMany(p => p.EmployeeProps_PastProjects)
                .HasForeignKey(d => d._owner_id)
                .HasConstraintName("FK_EmployeeProps.PastProjects__owner_id");

            entity.HasOne(d => d._valueNavigation).WithMany(p => p.EmployeeProps_PastProjects)
                .HasForeignKey(d => d._value)
                .HasConstraintName("FK_EmployeeProps.PastProjects__value");
        });

        modelBuilder.Entity<EmployeeProps_PerformanceReviews>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("EmployeeProps.PerformanceReviews");

            entity.HasIndex(e => new { e._owner_id, e._key_Item1, e._key_Item2 }, "UQ_EmployeeProps.PerformanceReviews").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._owner).WithMany(p => p.EmployeeProps_PerformanceReviews)
                .HasForeignKey(d => d._owner_id)
                .HasConstraintName("FK_EmployeeProps.PerformanceReviews__owner_id");
        });

        modelBuilder.Entity<EmployeeProps_PhoneDirectory>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("EmployeeProps.PhoneDirectory");

            entity.HasIndex(e => new { e._owner_id, e._key }, "UQ_EmployeeProps.PhoneDirectory").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._owner).WithMany(p => p.EmployeeProps_PhoneDirectory)
                .HasForeignKey(d => d._owner_id)
                .HasConstraintName("FK_EmployeeProps.PhoneDirectory__owner_id");
        });

        modelBuilder.Entity<EmployeeProps_ProjectMetrics>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("EmployeeProps.ProjectMetrics");

            entity.HasIndex(e => new { e._owner_id, e._key }, "UQ_EmployeeProps.ProjectMetrics").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._owner).WithMany(p => p.EmployeeProps_ProjectMetrics)
                .HasForeignKey(d => d._owner_id)
                .HasConstraintName("FK_EmployeeProps.ProjectMetrics__owner_id");

            entity.HasOne(d => d._valueNavigation).WithMany(p => p.EmployeeProps_ProjectMetrics)
                .HasForeignKey(d => d._value)
                .HasConstraintName("FK_EmployeeProps.ProjectMetrics__value");
        });

        modelBuilder.Entity<EmployeeProps_SkillLevels>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("EmployeeProps.SkillLevels");

            entity.HasIndex(e => new { e._owner_id, e._idx }, "UQ_EmployeeProps.SkillLevels").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._owner).WithMany(p => p.EmployeeProps_SkillLevels)
                .HasForeignKey(d => d._owner_id)
                .HasConstraintName("FK_EmployeeProps.SkillLevels__owner_id");
        });

        modelBuilder.Entity<EmployeeProps_Skills>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("EmployeeProps.Skills");

            entity.HasIndex(e => new { e._owner_id, e._idx }, "UQ_EmployeeProps.Skills").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._owner).WithMany(p => p.EmployeeProps_Skills)
                .HasForeignKey(d => d._owner_id)
                .HasConstraintName("FK_EmployeeProps.Skills__owner_id");
        });

        modelBuilder.Entity<EmployeeProps_WorkAddress>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("EmployeeProps.WorkAddress");

            entity.HasIndex(e => e._owner_id, "UQ_EmployeeProps.WorkAddress").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._owner).WithOne(p => p.EmployeeProps_WorkAddress)
                .HasForeignKey<EmployeeProps_WorkAddress>(d => d._owner_id)
                .HasConstraintName("FK_EmployeeProps.WorkAddress__owner_id");
        });

        modelBuilder.Entity<EmployeeProps_WorkAddress_Building>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("EmployeeProps.WorkAddress.Building");

            entity.HasIndex(e => e._owner_id, "UQ_EmployeeProps.WorkAddress.Building").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._owner).WithOne(p => p.EmployeeProps_WorkAddress_Building)
                .HasForeignKey<EmployeeProps_WorkAddress_Building>(d => d._owner_id)
                .HasConstraintName("FK_EmployeeProps.WorkAddress.Building__owner_id");
        });

        modelBuilder.Entity<EmployeeProps_WorkAddress_Building_AccessCodes>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("EmployeeProps.WorkAddress.Building.AccessCodes");

            entity.HasIndex(e => new { e._owner_id, e._idx }, "UQ_EmployeeProps.WorkAddress.Building.AccessCodes").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._owner).WithMany(p => p.EmployeeProps_WorkAddress_Building_AccessCodes)
                .HasForeignKey(d => d._owner_id)
                .HasConstraintName("FK_EmployeeProps.WorkAddress.Building.AccessCodes__owner_id");
        });

        modelBuilder.Entity<EmployeeProps_WorkAddress_Building_Amenities>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("EmployeeProps.WorkAddress.Building.Amenities");

            entity.HasIndex(e => new { e._owner_id, e._idx }, "UQ_EmployeeProps.WorkAddress.Building.Amenities").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._owner).WithMany(p => p.EmployeeProps_WorkAddress_Building_Amenities)
                .HasForeignKey(d => d._owner_id)
                .HasConstraintName("FK_EmployeeProps.WorkAddress.Building.Amenities__owner_id");
        });

        modelBuilder.Entity<EmployeeProps_WorkAddress_Building_ElevatorFloors>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("EmployeeProps.WorkAddress.Building.ElevatorFloors");

            entity.HasIndex(e => new { e._owner_id, e._idx }, "UQ_EmployeeProps.WorkAddress.Building.ElevatorFloors").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._owner).WithMany(p => p.EmployeeProps_WorkAddress_Building_ElevatorFloors)
                .HasForeignKey(d => d._owner_id)
                .HasConstraintName("FK_EmployeeProps.WorkAddress.Building.ElevatorFloors__owner_id");
        });

        modelBuilder.Entity<EmployeeProps_WorkAddress_Building_ParkingSpots>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("EmployeeProps.WorkAddress.Building.ParkingSpots");

            entity.HasIndex(e => new { e._owner_id, e._idx }, "UQ_EmployeeProps.WorkAddress.Building.ParkingSpots").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._owner).WithMany(p => p.EmployeeProps_WorkAddress_Building_ParkingSpots)
                .HasForeignKey(d => d._owner_id)
                .HasConstraintName("FK_EmployeeProps.WorkAddress.Building.ParkingSpots__owner_id");
        });

        modelBuilder.Entity<ExampleResultProps>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.Property(e => e._id).ValueGeneratedNever();

            entity.HasOne(d => d.objects).WithOne(p => p.ExampleResultProps)
                .HasForeignKey<ExampleResultProps>(d => d._id)
                .HasConstraintName("FK_ExampleResultProps__id");
        });

        modelBuilder.Entity<ExampleResultProps_Results>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("ExampleResultProps.Results");

            entity.HasIndex(e => new { e._owner_id, e._key }, "UQ_ExampleResultProps.Results").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._owner).WithMany(p => p.ExampleResultProps_Results)
                .HasForeignKey(d => d._owner_id)
                .HasConstraintName("FK_ExampleResultProps.Results__owner_id");
        });

        modelBuilder.Entity<PersonProps>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.Property(e => e._id).ValueGeneratedNever();

            entity.HasOne(d => d.StatusNavigation).WithMany(p => p.PersonProps)
                .HasForeignKey(d => d.Status)
                .HasConstraintName("FK_PersonProps_Status");

            entity.HasOne(d => d.objects).WithOne(p => p.PersonProps)
                .HasForeignKey<PersonProps>(d => d._id)
                .HasConstraintName("FK_PersonProps__id");
        });

        modelBuilder.Entity<PersonProps_Roles>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("PersonProps.Roles");

            entity.HasIndex(e => new { e._owner_id, e._idx }, "UQ_PersonProps.Roles").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._owner).WithMany(p => p.PersonProps_Roles)
                .HasForeignKey(d => d._owner_id)
                .HasConstraintName("FK_PersonProps.Roles__owner_id");

            entity.HasOne(d => d._valueNavigation).WithMany(p => p.PersonProps_Roles)
                .HasForeignKey(d => d._value)
                .HasConstraintName("FK_PersonProps.Roles__value");
        });

        modelBuilder.Entity<ProjectMetricsProps>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.Property(e => e._id).ValueGeneratedNever();

            entity.HasOne(d => d.objects).WithOne(p => p.ProjectMetricsProps)
                .HasForeignKey<ProjectMetricsProps>(d => d._id)
                .HasConstraintName("FK_ProjectMetricsProps__id");
        });

        modelBuilder.Entity<ProjectMetricsProps_Tags>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("ProjectMetricsProps.Tags");

            entity.HasIndex(e => new { e._owner_id, e._idx }, "UQ_ProjectMetricsProps.Tags").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._owner).WithMany(p => p.ProjectMetricsProps_Tags)
                .HasForeignKey(d => d._owner_id)
                .HasConstraintName("FK_ProjectMetricsProps.Tags__owner_id");
        });

        modelBuilder.Entity<ProjectMetricsProps_Technologies>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.ToTable("ProjectMetricsProps.Technologies");

            entity.HasIndex(e => new { e._owner_id, e._idx }, "UQ_ProjectMetricsProps.Technologies").IsUnique();

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._owner).WithMany(p => p.ProjectMetricsProps_Technologies)
                .HasForeignKey(d => d._owner_id)
                .HasConstraintName("FK_ProjectMetricsProps.Technologies__owner_id");
        });

        modelBuilder.Entity<list_items>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");

            entity.HasOne(d => d._id_listNavigation).WithMany(p => p.list_items)
                .HasForeignKey(d => d._id_list)
                .HasConstraintName("FK_list_items__id_list");

            entity.HasOne(d => d._id_objectNavigation).WithMany(p => p.list_items)
                .HasForeignKey(d => d._id_object)
                .OnDelete(DeleteBehavior.SetNull)
                .HasConstraintName("FK_list_items__id_object");
        });

        modelBuilder.Entity<lists>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");
        });

        modelBuilder.Entity<objects>(entity =>
        {
            entity.HasKey(e => e._id);

            entity.Property(e => e._id).HasDefaultValueSql("nextval('flat_identity'::regclass)");
            entity.Property(e => e._value_numeric).HasPrecision(38, 18);

            entity.HasOne(d => d._id_parentNavigation).WithMany(p => p.Inverse_id_parentNavigation)
                .HasForeignKey(d => d._id_parent)
                .OnDelete(DeleteBehavior.Cascade)
                .HasConstraintName("FK_objects__id_parent");
        });
        modelBuilder.HasSequence("flat_identity");

        OnModelCreatingPartial(modelBuilder);
    }

    partial void OnModelCreatingPartial(ModelBuilder modelBuilder);
}
