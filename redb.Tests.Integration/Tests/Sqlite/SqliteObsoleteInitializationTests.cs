using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.Loader;
using redb.Core;
using redb.Core.Attributes;
using redb.Core.Extensions;
using redb.Tests.Integration.Fixtures;

namespace redb.Tests.Integration.Tests.Sqlite;

/// <summary>
/// The obsolete AutoSyncSchemesAsync extension (review INIT-1) carried its own copy of the start-up sequence:
/// parallel on one instance, without the up-front validation of scheme names, and with every synchronization
/// error swallowed in a bare catch. A scheme with a name that must not boot passed through it without a word.
/// The scheme lives in an assembly built here, so no other test ever scans it.
/// </summary>
[Collection("Sqlite")]
public class SqliteObsoleteInitializationTests
{
    private readonly IRedbService _redb;

    public SqliteObsoleteInitializationTests(SqliteFixture fixture) => _redb = fixture.Redb;

    /// <summary>
    /// Built in its own collectible load context: an InitializeAsync() without arguments scans every assembly of
    /// the default context, and a bad scheme left there fails every later initialization in the process.
    /// </summary>
    private static Assembly AssemblyWithABadSchemeName(AssemblyLoadContext context)
    {
        using var _ = context.EnterContextualReflection();
        var assembly = AssemblyBuilder.DefineDynamicAssembly(new AssemblyName("redb.Tests.BadSchemeName"), AssemblyBuilderAccess.RunAndCollect);
        var type = assembly.DefineDynamicModule("m").DefineType("BadSchemeNameProps", TypeAttributes.Public | TypeAttributes.Class);
        type.SetCustomAttribute(new CustomAttributeBuilder(
            typeof(RedbSchemeAttribute).GetConstructor(Type.EmptyTypes)!, [],
            [typeof(RedbSchemeAttribute).GetProperty(nameof(RedbSchemeAttribute.Name))!], ["bad name!"]));
        type.DefineDefaultConstructor(MethodAttributes.Public);
        type.CreateType();
        return assembly;
    }

    [Fact]
    public async Task AutoSyncSchemesAsync_RefusesASchemeThatMustNotBoot()
    {
        var context = new AssemblyLoadContext("redb.Tests.BadSchemeName", isCollectible: true);
        try
        {
            var assembly = AssemblyWithABadSchemeName(context);
            AssemblyLoadContext.Default.Assemblies.Should().NotContain(assembly, "precondition: no other test can scan it");

#pragma warning disable CS0618 // the obsolete entry point is what this test is about
            var sync = async () => await RedbServiceInitializationExtensions.AutoSyncSchemesAsync(_redb, assembly);
#pragma warning restore CS0618

            (await sync.Should().ThrowAsync<Exception>()).Which.ToString().Should().Contain("bad name!");
        }
        finally
        {
            context.Unload();
        }
    }
}
