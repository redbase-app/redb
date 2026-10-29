namespace RedbIdentityApp.Web.Auth;

/// <summary>The <c>Identity</c> section of appsettings.json: where users sign in, and as which client.</summary>
public sealed class WebIdentityOptions
{
    /// <summary>Identity's issuer, an https address. The browser and this server both reach it there.</summary>
    public string Authority { get; set; } = "";

    /// <summary>This server's client; the module creates it at Identity (Identity:WebClientId of the module).</summary>
    public string ClientId { get; set; } = "";

    /// <summary>The client's secret, the same value as the module's Identity:WebClientSecret. Environment only.</summary>
    public string ClientSecret { get; set; } = "";

    /// <summary>The scope that lets the access token call the module's API.</summary>
    public string ApiScope { get; set; } = "";

    /// <summary>The administrative scope; Identity grants it to administrators only and drops it for others.</summary>
    public string AdminScope { get; set; } = "";

    /// <summary>
    /// Development only: accept any certificate from Identity, for the self-signed one of the
    /// redb-identity-full image on your machine. Refused outside the Development environment; a container
    /// trusts that certificate through its CA store instead (Dockerfile).
    /// </summary>
    public bool AcceptAnyServerCertificate { get; set; }

    public void Validate(IHostEnvironment environment)
    {
        if (!Uri.TryCreate(Authority, UriKind.Absolute, out var authority) || authority.Scheme != Uri.UriSchemeHttps)
            throw new InvalidOperationException("Identity:Authority must be the https address of redb.Identity.");
        if (string.IsNullOrWhiteSpace(ClientId) || string.IsNullOrWhiteSpace(ApiScope) || string.IsNullOrWhiteSpace(AdminScope))
            throw new InvalidOperationException("Identity:ClientId, Identity:ApiScope and Identity:AdminScope are required.");
        if (ClientSecret.Length < 32)
            throw new InvalidOperationException("Identity:ClientSecret is missing or shorter than 32 characters: set Identity__ClientSecret.");
        if (AcceptAnyServerCertificate && !environment.IsDevelopment())
            throw new InvalidOperationException("Identity:AcceptAnyServerCertificate is for the Development environment only.");
    }
}
