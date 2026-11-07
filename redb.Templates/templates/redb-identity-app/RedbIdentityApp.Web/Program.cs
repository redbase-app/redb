using System.Security.Claims;
using Microsoft.AspNetCore.Antiforgery;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Authentication.OpenIdConnect;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.IdentityModel.Protocols.OpenIdConnect;
using RedbIdentityApp.Web.Auth;
using RedbIdentityApp.Web.Components;
using RedbIdentityApp.Web.Services;

var builder = WebApplication.CreateBuilder(args);

var identity = builder.Configuration.GetSection("Identity").Get<WebIdentityOptions>()
    ?? throw new InvalidOperationException("The Identity section is missing in appsettings.json.");
identity.Validate(builder.Environment);
builder.Services.Configure<WebIdentityOptions>(builder.Configuration.GetSection("Identity"));

// --- DataProtection: the session cookie and the stored tokens are encrypted with it ---
// A key ring in a directory survives a restart; in a container the directory is a volume (deploy/).
var dataProtection = builder.Services.AddDataProtection().SetApplicationName("RedbIdentityApp");
if (builder.Configuration["Web:DataProtectionKeysPath"] is { Length: > 0 } keysPath)
    dataProtection.PersistKeysToFileSystem(new DirectoryInfo(keysPath));

// --- The tokens stay on this server (Auth/UserTokenStore.cs) ---
builder.Services.AddDistributedMemoryCache();
builder.Services.AddSingleton<UserTokenStore>();
builder.Services.AddSingleton<AccessTokens>();

// The HttpClient that talks to Identity: the OpenID Connect handler (metadata, code exchange, user info)
// and AccessTokens (refresh, revocation). Identity's certificate is checked like any other; only the
// Development flag accepts the self-signed one of a local image.
builder.Services.AddHttpClient(AccessTokens.HttpClientName)
    .ConfigurePrimaryHttpMessageHandler(() => identity.AcceptAnyServerCertificate
        ? new HttpClientHandler { ServerCertificateCustomValidationCallback = HttpClientHandler.DangerousAcceptAnyServerCertificateValidator }
        : new HttpClientHandler());

// --- Sign-in: OpenID Connect at redb.Identity, then this server's session cookie ---
builder.Services.AddAuthentication(options =>
    {
        options.DefaultScheme = CookieAuthenticationDefaults.AuthenticationScheme;
        options.DefaultChallengeScheme = OpenIdConnectDefaults.AuthenticationScheme;
    })
    .AddCookie(options =>
    {
        options.Cookie.Name = "RedbIdentityApp";
        options.Cookie.HttpOnly = true;
        // Lax, not Strict: the browser comes back from Identity with a top-level redirect.
        options.Cookie.SameSite = SameSiteMode.Lax;
        // Secure when the request is HTTPS. Behind a TLS-terminating proxy, add UseForwardedHeaders so the
        // request scheme is the real one.
        options.Cookie.SecurePolicy = CookieSecurePolicy.SameAsRequest;
        options.ExpireTimeSpan = UserTokenStore.SessionLifetime;
        options.SlidingExpiration = true;
        options.LoginPath = "/account/login";
        options.AccessDeniedPath = "/";
        // A cookie whose tokens are gone (a restart with the in-memory store, a revoked session) is no
        // session: the user goes through Identity again, usually without a password.
        options.Events.OnValidatePrincipal = async context =>
        {
            var key = context.Principal?.FindFirst(UserTokenStore.SessionClaim)?.Value;
            var store = context.HttpContext.RequestServices.GetRequiredService<UserTokenStore>();
            if (key is null || await store.GetAsync(key, context.HttpContext.RequestAborted) is null)
            {
                context.RejectPrincipal();
                await context.HttpContext.SignOutAsync(CookieAuthenticationDefaults.AuthenticationScheme);
            }
        };
    })
    .AddOpenIdConnect(options =>
    {
        options.Authority = identity.Authority;
        options.ClientId = identity.ClientId;
        options.ClientSecret = identity.ClientSecret;
        options.RequireHttpsMetadata = true;

        // Authorization code with PKCE. The answer comes back in the query of a top-level GET, so the
        // correlation and nonce cookies can be SameSite=Lax and also work over plain http on localhost.
        options.ResponseType = OpenIdConnectResponseType.Code;
        options.UsePkce = true;
        options.ResponseMode = OpenIdConnectResponseMode.Query;
        options.CorrelationCookie.SameSite = SameSiteMode.Lax;
        options.CorrelationCookie.SecurePolicy = CookieSecurePolicy.SameAsRequest;
        options.NonceCookie.SameSite = SameSiteMode.Lax;
        options.NonceCookie.SecurePolicy = CookieSecurePolicy.SameAsRequest;

        options.Scope.Clear();
        foreach (var scope in new[] { "openid", "profile", "email", "offline_access", identity.ApiScope, identity.AdminScope })
            options.Scope.Add(scope);

        // The tokens go to UserTokenStore, not into the cookie.
        options.SaveTokens = false;
        options.GetClaimsFromUserInfoEndpoint = true;
        options.MapInboundClaims = false;
        options.TokenValidationParameters.NameClaimType = "name";
        options.TokenValidationParameters.RoleClaimType = "roles";
        options.SignedOutCallbackPath = "/signout-callback-oidc";

        options.Events.OnTokenValidated = async context =>
        {
            var response = context.TokenEndpointResponse
                ?? throw new InvalidOperationException("Identity's token endpoint gave no answer.");
            var tokens = new UserTokens(
                AccessToken: response.AccessToken,
                RefreshToken: response.RefreshToken,
                IdToken: response.IdToken,
                ExpiresAt: DateTimeOffset.UtcNow.AddSeconds(int.TryParse(response.ExpiresIn, out var seconds) ? seconds : 300),
                Scopes: response.Scope?.Split(' ', StringSplitOptions.RemoveEmptyEntries) ?? []);
            var store = context.HttpContext.RequestServices.GetRequiredService<UserTokenStore>();
            var key = await store.CreateAsync(tokens, context.HttpContext.RequestAborted);

            // The cookie gets the session key and the granted scopes (to show or hide pages); the module
            // checks the token itself on every call, whatever the page shows.
            var user = (ClaimsIdentity)context.Principal!.Identity!;
            user.AddClaim(new Claim(UserTokenStore.SessionClaim, key));
            foreach (var scope in tokens.Scopes)
                user.AddClaim(new Claim("scope", scope));
        };

        // Sign-out at Identity names the session that ends (OIDC RP-Initiated Logout).
        options.Events.OnRedirectToIdentityProviderForSignOut = context =>
        {
            if (context.HttpContext.Items["id_token_hint"] is string idToken)
                context.ProtocolMessage.IdTokenHint = idToken;
            return Task.CompletedTask;
        };
    });

// The handler's backchannel goes through the same HttpClient as the refresh.
builder.Services.AddOptions<OpenIdConnectOptions>(OpenIdConnectDefaults.AuthenticationScheme)
    .Configure<IHttpClientFactory>((options, clients) => options.Backchannel = clients.CreateClient(AccessTokens.HttpClientName));

builder.Services.AddAuthorizationBuilder()
    // Only a page decision: the module refuses an administrative call without the scope on its own.
    .AddPolicy("admin", policy => policy.RequireClaim("scope", identity.AdminScope));
builder.Services.AddCascadingAuthenticationState();

builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();

// --- The module's API ---
var moduleUrl = builder.Configuration["Module:BaseUrl"]
    ?? throw new InvalidOperationException("Module:BaseUrl is missing in appsettings.json.");
builder.Services.AddHttpClient<ModuleClient>(http => http.BaseAddress = new Uri(moduleUrl));

var app = builder.Build();

if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error", createScopeForErrors: true);
}

app.UseAuthentication();
app.UseAuthorization();
app.UseAntiforgery();
// UseStaticFiles rather than MapStaticAssets: the same code for .NET 8, 9 and 10.
app.UseStaticFiles();

// --- Sign-in and sign-out ---
// Both are plain requests: a challenge redirects and a sign-out clears the cookie, which an interactive
// circuit cannot do once its response has started.
app.MapGet("/account/login", (string? returnUrl) =>
    Results.Challenge(new AuthenticationProperties { RedirectUri = LocalPath(returnUrl) },
        [OpenIdConnectDefaults.AuthenticationScheme]));

app.MapPost("/account/logout", async (HttpContext http, IAntiforgery antiforgery, AccessTokens tokens) =>
{
    // The sign-out form carries an antiforgery token: another site cannot sign the user out.
    if (!await antiforgery.IsRequestValidAsync(http))
    {
        http.Response.Redirect("/");
        return;
    }

    if (http.User.FindFirst(UserTokenStore.SessionClaim)?.Value is { } key)
        http.Items["id_token_hint"] = await tokens.EndSessionAsync(key, http.RequestAborted);

    await http.SignOutAsync(CookieAuthenticationDefaults.AuthenticationScheme);
    await http.SignOutAsync(OpenIdConnectDefaults.AuthenticationScheme, new AuthenticationProperties { RedirectUri = "/" });
}).DisableAntiforgery();   // checked above, so a stale form goes home instead of failing

app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();

app.Run();

// Only ever a local path, never an address from the query.
static string LocalPath(string? returnUrl) =>
    returnUrl is { Length: > 0 } && returnUrl.StartsWith('/') && !returnUrl.StartsWith("//") && !returnUrl.StartsWith("/\\")
        ? returnUrl
        : "/";
