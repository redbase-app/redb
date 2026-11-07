using System.Security.Cryptography;
using System.Text;

namespace RedbBff.Web.Services;

/// <summary>
/// The users who can sign in, from <c>Web:Users</c> in configuration. A stand-in that keeps the template
/// small: a real application keeps its users in a store of its own, for example redb.Identity.
/// </summary>
public sealed class WebUsers(IConfiguration configuration)
{
    /// <summary>The user's role when the login and password match, otherwise null.</summary>
    public string? Check(string login, string password)
    {
        var user = configuration.GetSection($"Web:Users:{login}");
        var expected = user["Password"];
        if (string.IsNullOrEmpty(login) || expected is null)
            return null;

        // Constant-time comparison, so the answer time does not tell how much of the password was right.
        return CryptographicOperations.FixedTimeEquals(Encoding.UTF8.GetBytes(expected), Encoding.UTF8.GetBytes(password))
            ? user["Role"] ?? throw new InvalidOperationException($"Web:Users:{login}:Role is not set.")
            : null;
    }
}
