using redb.Core;
using redb.Core.Models.Users;
using redb.Core.Security;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Pins the default password hasher of a bare redb deployment (external security report,
/// 2026-09-08): every provider's stock CreateUserProvider factory used the short UserProvider
/// constructor, which hard-wired SimplePasswordHasher (salted SHA256) - BcryptPasswordHasher
/// existed in the tree but nothing reached it. The default is bcrypt now; legacy SHA256+salt
/// hashes keep validating (BcryptPasswordHasher recognizes both formats), so existing
/// databases migrate naturally on the next password change.
/// </summary>
public abstract class PasswordHasherDefaultTestsBase
{
    protected readonly IRedbService Redb;

    protected PasswordHasherDefaultTestsBase(IRedbService redb) => Redb = redb;

    private static string FreshLogin() => "hash_" + Guid.NewGuid().ToString("N")[..12];

    [Fact]
    public async Task DefaultHasher_WritesBcrypt()
    {
        var login = FreshLogin();
        var user = await Redb.UserProvider.CreateUserAsync(
            new CreateUserRequest { Login = login, Name = login, Password = "correct-horse" });

        var stored = await Redb.Context.ExecuteScalarAsync<string>(
            $"SELECT _password FROM _users WHERE _id = {user.Id}");

        stored.Should().StartWith("$2",
            "a bare redb deployment must hash passwords with bcrypt by default, not salted SHA256");
    }

    [Fact]
    public async Task LegacySha256Hash_StillValidates()
    {
        // An existing database carries SimplePasswordHasher-era hashes ("base64(salt):base64(hash)").
        // The bcrypt default must keep accepting them - login survives the upgrade, and the hash
        // migrates to bcrypt whenever the user changes the password.
        var login = FreshLogin();
        var user = await Redb.UserProvider.CreateUserAsync(
            new CreateUserRequest { Login = login, Name = login, Password = "placeholder" });

        var legacy = new SimplePasswordHasher().HashPassword("old-password");
        await Redb.Context.ExecuteAsync(
            $"UPDATE _users SET _password = '{legacy}' WHERE _id = {user.Id}");

        var validated = await Redb.UserProvider.ValidateUserAsync(login, "old-password");
        validated.Should().NotBeNull("a legacy SHA256+salt hash must still authenticate");

        var rejected = await Redb.UserProvider.ValidateUserAsync(login, "wrong-password");
        rejected.Should().BeNull("the legacy path must still reject a wrong password");
    }
}
