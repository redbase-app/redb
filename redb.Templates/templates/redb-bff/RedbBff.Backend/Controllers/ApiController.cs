using System.ComponentModel.DataAnnotations;
using redb.Core;
using redb.Route.Controllers;
using redb.Route.Http;
using redb.Route.RedbCore.Extensions;
using RedbBff.Models;

namespace RedbBff.Backend.Controllers;

/// <summary>
/// What the controllers of this backend share. A controller is created for each request, without DI:
/// what it needs comes from <c>Context</c> (the route context) and <c>Exchange</c> (the request).
/// </summary>
public abstract class ApiController : RedbController
{
    /// <summary>
    /// The redb service of this request: scoped to the exchange and disposed with it, so parallel requests
    /// never share a connection. The empty name is the host's default redb.
    /// </summary>
    protected IRedbService Redb => Context.GetRedbService("", Exchange);

    /// <summary>Sets the status code of the answer; the returned object is still the body.</summary>
    protected T Status<T>(int code, T body)
    {
        Exchange.Out ??= Exchange.In.Clone();
        Exchange.Out.Headers[HttpHeaders.ResponseCode] = code;
        return body;
    }

    protected ApiError NotFound(string message) => Status(404, new ApiError { Message = message });

    /// <summary>The web server validates the form, but the backend cannot trust that: the same attributes are checked here.</summary>
    protected ApiError? Invalid(object dto)
    {
        var errors = new List<ValidationResult>();
        return Validator.TryValidateObject(dto, new ValidationContext(dto), errors, validateAllProperties: true)
            ? null
            : Status(400, new ApiError { Message = string.Join(" ", errors.Select(r => r.ErrorMessage)) });
    }
}
