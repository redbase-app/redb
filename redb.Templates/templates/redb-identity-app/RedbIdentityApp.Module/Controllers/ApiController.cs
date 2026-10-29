using System.ComponentModel.DataAnnotations;
using System.Security.Claims;
using redb.Core;
using redb.Route.Abstractions;
using redb.Route.Controllers;
using redb.Route.Http;
using redb.Route.RedbCore.Extensions;
using RedbIdentityApp.Contracts;
using RedbIdentityApp.Module.Identity;

namespace RedbIdentityApp.Module.Controllers;

/// <summary>
/// What the controllers of the module share. A controller is created for each request, without DI: what
/// it needs comes from <c>Context</c> (the route context, whose registry holds the module's services)
/// and <c>Exchange</c> (the request, whose principal is the checked bearer token).
/// </summary>
public abstract class ApiController : RedbController
{
    /// <summary>The redb service of this request, scoped to the exchange. The empty name is the worker's default redb.</summary>
    protected IRedbService Redb => Context.GetRedbService("", Exchange);

    /// <summary>The caller: what Identity said about the token (<see cref="BearerTokenValidator"/>).</summary>
    protected ClaimsPrincipal Caller => ExchangePrincipal.Get(Exchange)
        ?? throw new InvalidOperationException("The API route runs without a bearer check.");

    /// <summary>The token's subject: a stable, public id of the user (or of the client, for a service token).</summary>
    protected string Subject => Caller.FindFirst(BearerTokenValidator.SubjectClaim)!.Value;

    /// <summary>True for a user's token; a client-credentials token has no user.</summary>
    protected bool IsUser => Caller.HasClaim(c => c.Type == BearerTokenValidator.UserIdClaim);

    protected ModuleSettings Settings => Service<ModuleSettings>();

    protected T Service<T>() where T : class =>
        Context.GetFromRegistry<T>(typeof(T).FullName!) ?? throw new InvalidOperationException($"{typeof(T).Name} is not registered.");

    /// <summary>
    /// Null when the token holds <paramref name="scope"/>; otherwise sets 403 with
    /// <c>error="insufficient_scope"</c> and returns the body to return.
    /// </summary>
    protected ApiError? RequireScope(string scope)
    {
        if (Caller.HasClaim(BearerTokenValidator.ScopeClaim, scope))
            return null;
        IdentityGate.InsufficientScope(Exchange, Context.ContextId, scope);
        return new ApiError { Message = $"The token lacks the scope {scope}." };
    }

    /// <summary>Sets the status code of the answer; the returned object is still the body.</summary>
    protected T Status<T>(int code, T body)
    {
        Exchange.Out ??= Exchange.In.Clone();
        Exchange.Out.Headers[HttpHeaders.ResponseCode] = code;
        return body;
    }

    protected ApiError NotFound(string message) => Status(404, new ApiError { Message = message });

    /// <summary>The web server validates the form, but the module cannot trust that: the same attributes are checked here.</summary>
    protected ApiError? Invalid(object dto)
    {
        var errors = new List<ValidationResult>();
        return Validator.TryValidateObject(dto, new ValidationContext(dto), errors, validateAllProperties: true)
            ? null
            : Status(400, new ApiError { Message = string.Join(" ", errors.Select(r => r.ErrorMessage)) });
    }
}
