using System.Globalization;
using redb.Identity.Contracts.Common;
using redb.Identity.Contracts.Routes;
using redb.Identity.Contracts.Users;
using redb.Route.Controllers.Attributes;
using RedbIdentityApp.Contracts;
using RedbIdentityApp.Module.Identity;

namespace RedbIdentityApp.Module.Controllers;

/// <summary>
/// Administration: reads Identity's management API in process. Open only to a token that holds the
/// administrative scope, which Identity issues to users of its <c>admin</c> role alone (see IdentitySetup).
/// The management call runs as the module's service client, not as the user: the user's token proves
/// the right, the module does the work.
/// </summary>
[Route("/api/admin")]
public sealed class AdminController : ApiController
{
    public const int PageSize = 20;

    /// <summary><c>GET /api/admin/users?page=</c>: Identity's users, a page at a time.</summary>
    [HttpGet("users")]
    public async Task<object> Users([FromQuery("page")] string? page)
    {
        if (RequireScope(Settings.AdminScope) is { } denied)
            return denied;

        var number = Math.Max(1, int.TryParse(page, CultureInfo.InvariantCulture, out var p) ? p : 1);
        IdentityAnswer answer;
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            answer = await Service<IdentityManagement>().CallAsync(IdentityEndpoints.ManageUsers, "list",
                new ListRequest { Offset = (number - 1) * PageSize, Count = PageSize }, timeout.Token);
        }
        catch (IdentityUnavailableException)
        {
            Exchange.Out ??= Exchange.In.Clone();
            Exchange.Out.Headers["Retry-After"] = IdentityGate.RetryAfterSeconds.ToString(CultureInfo.InvariantCulture);
            return Status(503, new ApiError { Message = "The sign-in service is not available. Try again shortly." });
        }
        catch (IdentityRefusedException)
        {
            return Status(502, new ApiError { Message = "Identity refused the module's management client; see the module's log." });
        }

        if (answer.IsError)
            return Status(502, new ApiError { Message = $"Identity answered {answer.Error}: {answer.ErrorDescription}" });

        var users = answer.Body as PagedResult<UserResponse>
            ?? throw new InvalidOperationException($"Unexpected answer of {IdentityEndpoints.ManageUsers}: {answer.Body?.GetType().Name}.");
        return new Page<UserSummaryDto>
        {
            Total = users.Total,
            Items = users.Items.Select(u => new UserSummaryDto
            {
                Id = u.Id,
                Login = u.Login ?? "",
                Name = u.DisplayName,
                Email = u.Email,
                Enabled = !string.Equals(u.Status, "disabled", StringComparison.OrdinalIgnoreCase),
            }).ToList(),
        };
    }
}
