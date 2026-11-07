using System.Diagnostics;
using redb.Core;
using redb.Core.Exceptions;
using redb.Core.Models.Entities;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Unique business keys (V4 <c>[RedbUnique]</c>): the value of a marked field is unique within
/// its scheme, enforced by the DATABASE over the hash of the canonical value - not by a
/// pre-check that races. The contract shown here:
///
/// 1. A duplicate save fails with a typed <see cref="RedbUniqueViolationException"/>.
/// 2. <c>GetByUniqueAsync</c> finds an object by its key value (server-side, hash lookup).
/// 3. NULL never participates: any number of objects may leave the key empty.
/// 4. Soft delete releases the key - the trash does not squat on business identifiers.
///
/// redb carries TWO unique-key mechanisms, both shown here:
/// - <c>[RedbUnique]</c> on a Props field - per-FIELD uniqueness, any scalar type, hash in
///   <c>_values._unique</c>;
/// - <c>RedbObject.value_unique</c> - a per-OBJECT string key on the object row itself
///   (scheme-wide, up to 440 chars, no Props field needed) - the natural slot for an external
///   identifier of the whole object.
/// </summary>
[ExampleMeta("E003", "Unique Keys - [RedbUnique] and value_unique", "CRUD",
    ExampleTier.Free, 3, "Unique", "RedbUnique", "ValueUnique", "GetByUniqueAsync", "SoftDelete",
    RelatedApis = ["IRedbService.GetByUniqueAsync", "IRedbService.SaveAsync"])]
public class E003_UniqueKeys : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        await redb.SyncSchemeAsync<ProductProps>();

        // Unique per run so the example is rerunnable on a shared database.
        var sku = $"SKU-{Guid.NewGuid():N}"[..20];
        var sw = Stopwatch.StartNew();

        // 1. The first holder of the key wins.
        var first = new RedbObject<ProductProps>
        {
            name = "unique-demo-first",
            Props = new ProductProps { Sku = sku, Title = "First holder", Price = 9.99m }
        };
        var firstId = await redb.SaveAsync(first);

        // 2. A duplicate is rejected by the database with a typed exception.
        string rejected;
        try
        {
            await redb.SaveAsync(new RedbObject<ProductProps>
            {
                name = "unique-demo-duplicate",
                Props = new ProductProps { Sku = sku, Title = "Impostor", Price = 0.99m }
            });
            rejected = "NOT rejected - this line must never print";
        }
        catch (RedbUniqueViolationException ex)
        {
            rejected = $"rejected with {nameof(RedbUniqueViolationException)} (property: {ex.PropertyName ?? "Sku"})";
        }

        // 3. Lookup by key value - one server-side hash probe, no scan.
        var found = await redb.GetByUniqueAsync<ProductProps>(p => p.Sku, sku);

        // 4. NULL keys do not collide: both keyless products save fine.
        await redb.SaveAsync(new RedbObject<ProductProps> { name = "no-key-a", Props = new ProductProps { Title = "Draft A" } });
        await redb.SaveAsync(new RedbObject<ProductProps> { name = "no-key-b", Props = new ProductProps { Title = "Draft B" } });

        // 5. Soft delete releases the key for the next claimant.
        await redb.DeleteAsync(firstId);
        var successor = new RedbObject<ProductProps>
        {
            name = "unique-demo-successor",
            Props = new ProductProps { Sku = sku, Title = "Successor", Price = 19.99m }
        };
        var successorId = await redb.SaveAsync(successor);

        // 6. The OBJECT-level key: value_unique lives on the object row itself - no Props
        //    field involved - and is unique across the whole scheme.
        var objectKey = $"EXT-{Guid.NewGuid():N}"[..20];
        var holder = new RedbObject<ProductProps>
        {
            name = "object-key-holder",
            value_unique = objectKey,
            Props = new ProductProps { Title = "Holder of the object key" }
        };
        await redb.SaveAsync(holder);

        string objectRejected;
        try
        {
            await redb.SaveAsync(new RedbObject<ProductProps>
            {
                name = "object-key-impostor",
                value_unique = objectKey,
                Props = new ProductProps { Title = "Impostor" }
            });
            objectRejected = "NOT rejected - this line must never print";
        }
        catch (RedbUniqueViolationException ex)
        {
            // ex.Kind tells WHICH mechanism fired: ObjectKey here vs Property for [RedbUnique].
            objectRejected = $"rejected with {nameof(RedbUniqueViolationException)} (Kind: {ex.Kind})";
        }

        sw.Stop();

        return Ok("E003", "Unique Keys - [RedbUnique] and value_unique", ExampleTier.Free, sw.ElapsedMilliseconds, 5,
            [
                $"Field key [RedbUnique]: {sku}",
                $"Duplicate: {rejected}",
                $"GetByUniqueAsync found: #{found?.Id} ({found?.Props.Title})",
                "Two keyless (NULL) products saved side by side",
                $"Soft delete released the key: successor #{successorId} took it",
                $"Object key (value_unique): {objectKey}, duplicate {objectRejected}"
            ]);
    }
}
